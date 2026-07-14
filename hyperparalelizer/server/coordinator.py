# (Pessoa 4) Nó "Barriga": orquestra tarefas e middleware

# servidor STATELESS
# O coordenador não mantém estado dinâmico interno. 
# Todas as informações críticas de rede, tarefas e nós residem e são gerenciadas na GlobalTable.

import asyncio
import itertools
import math
import queue
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.network import send_once
from core.pubsub import TOPIC_GLOBAL_BEST_SCORE
from hyperparalelizer.global_table import GlobalTable
from utils.protocol import PubSubPublish, TrainingTask

TASK_TIMEOUT = 30.0  # segundos até uma tarefa ser considerada perdida


@dataclass
class Peer:
    ip: str
    port: int
    id_node: str = ""          # preenchido pelo GlobalTable após add_node
    hyperparameters: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=dict)
    received_dataset: bool = False
    received_model: bool = False


class Coordinator:
    def __init__(self, dataset, model, global_table: GlobalTable,
                 model_type: str = "generic",
                 model_config: Optional[Dict[str, Any]] = None,
                 pubsub_queue: Optional[queue.Queue] = None):
        # Configurações estáticas/inputs (não mudam ao longo do ciclo de vida)
        self.dataset = dataset
        self.model = model
        self.model_type = model_type
        self.model_config = model_config or {}

        # Dependências externas
        self.GlobalTable = global_table
        self._pubsub_queue = pubsub_queue  # fila Middleware → PubSubClient
        
        # Referência ao peer pupilo (se existir)
        self.pupil_peer: Optional[Dict[str, Any]] = None

        # Garante a inicialização de atributos que a GlobalTable espera em métodos de snapshot,
        # mas que não foram declarados em seu construtor original.
        if not hasattr(self.GlobalTable, 'fragments'):
            self.GlobalTable.fragments = {}
        if not hasattr(self.GlobalTable, 'system_state'):
            self.GlobalTable.system_state = "HASHING"

    # ENDPOINT: ADICIONA NOVO PEER                                      
    def add_peer(self, peer: Peer) -> str:
        # Ao entrar na rede, guardamos a própria instância de Peer como metadata na GlobalTable
        node_id = self.GlobalTable.add_node(
            peer.ip, 
            peer.port, 
            peer
        )
        peer.id_node = node_id

        # Recupera fragmentos e nós de forma thread-safe para fazer o round-robin
        with self.GlobalTable.lock:
            dataset_fragments = list(self.GlobalTable.fragments.keys())
            all_nodes = list(self.GlobalTable.nodes.values())
            # Determina a posição do peer na lista de nós ativos
            peer_index = next((i for i, node in enumerate(all_nodes) if node["node_id"] == node_id), 0)

        # Associa um fragmento de dataset ao peer (round-robin)
        if dataset_fragments:
            frag_index = peer_index % len(dataset_fragments)
            fragment_name = dataset_fragments[frag_index]
            self.GlobalTable.add_fragment_location(fragment_name, node_id)

        # Atribui hiperparâmetros pendentes da fila
        self.assign_hyperparameters_to_peer(peer)

        return node_id
    
    # GRID SEARCH                                              
    def generate_grid_search(self, hyperparameters: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        with self.GlobalTable.lock:
            dataset_fragments = list(self.GlobalTable.fragments.keys())

        if not dataset_fragments:
            raise RuntimeError(
                "Execute fragment_dataset() antes de generate_grid_search()."
            )

        keys = list(hyperparameters.keys())
        values = list(hyperparameters.values())

        combinations = [
            dict(zip(keys, combo))
            for combo in itertools.product(*values)
        ]
        
        # Cada combinação vira uma TrainingTask populada diretamente na GlobalTable
        with self.GlobalTable.lock:
            self.GlobalTable.task_pool = [
                TrainingTask(
                    task_id=str(uuid.uuid4()),
                    id_node_origem="server",
                    dataset_fragmentos=dataset_fragments,
                    parametros=combo,
                    model_type=self.model_type,
                    model_config=dict(self.model_config),
                )
                for combo in combinations
            ]

        return combinations

    # ATRIBUIÇÃO DE HIPERPARÂMETROS                                        
    def assign_hyperparameters_to_peer(self, peer: Peer) -> Optional[TrainingTask]:
        with self.GlobalTable.lock:
            if not self.GlobalTable.task_pool:
                return None
            
            task = self.GlobalTable.task_pool.pop(0)
            self.GlobalTable.assigned_tasks[task.task_id] = {
                "peer": peer,
                "timestamp": time.time(),
                "task": task,
            }
            
        peer.hyperparameters = task.parametros
        return task

    # FRAGMENTAÇÃO DO DATASET                                              
    def fragment_dataset(self, n_fragments: Optional[int] = None) -> List[str]:
        """
        Divide self.dataset em n_fragments partes e registra cada fragmento
        na GlobalTable associado ao peer correspondente (se já houver peers)
        """
        with self.GlobalTable.lock:
            all_nodes = list(self.GlobalTable.nodes.values())
            
        current_peers_count = len(all_nodes)
            
        if n_fragments is None:
            n_fragments = max(1, current_peers_count)

        if n_fragments <= 0:
            raise ValueError("n_fragments deve ser maior que zero.")

        total = len(self.dataset)
        if total == 0:
            raise ValueError("dataset está vazio.")

        fragment_size = math.ceil(total / n_fragments)
        fragment_names: List[str] = []
        
        # O tamanho dos fragmentos é calculado de forma aproximadamente uniforme
        for i in range(n_fragments):
            frag_data = self.dataset[i * fragment_size: (i + 1) * fragment_size]
            frag_name = f"fragment_{i:04d}"
            fragment_names.append(frag_name)
            
            # Armazena os dados do fragmento diretamente na tabela global de forma thread-safe
            with self.GlobalTable.lock:
                self.GlobalTable.fragments[frag_name] = frag_data

            # Associa o fragmento ao peer já registrado, se houver
            if i < current_peers_count:
                self.GlobalTable.add_fragment_location(frag_name, all_nodes[i]["node_id"])

        with self.GlobalTable.lock:
            self.GlobalTable.system_state = "DATASET_DISTRIBUTION"

        return fragment_names

    # VERIFICAÇÃO DE STATUS / TIMEOUT                                      
    def check_task_status(self) -> List[str]:
        now = time.time()
        timed_out_ids: List[str] = []

        with self.GlobalTable.lock:
            for task_id, entry in list(self.GlobalTable.assigned_tasks.items()):
                if now - entry["timestamp"] > TASK_TIMEOUT:
                    timed_out_ids.append(task_id)

            for task_id in timed_out_ids:
                task_info = self.GlobalTable.assigned_tasks.pop(task_id)
                self.GlobalTable.task_pool.append(task_info["task"])  # Devolve à fila central da GlobalTable

        return timed_out_ids

    # DISTRIBUIÇÃO                                                         
    def distribute_dataset(self):
        with self.GlobalTable.lock:
            self.GlobalTable.system_state = "DATASET_DISTRIBUTION"

    def distribute_model_and_hyperparameters(self) -> List[tuple]:
        with self.GlobalTable.lock:
            busy_ids = {entry["peer"].id_node for entry in self.GlobalTable.assigned_tasks.values()}
            all_nodes = list(self.GlobalTable.nodes.values())
            
            # Extrai os peers ociosos diretamente do metadata armazenado na tabela
            idle_peers = [node["metadata"] for node in all_nodes if node["node_id"] not in busy_ids]

        dispatched: List[tuple] = []
        for peer in idle_peers:
            task = self.assign_hyperparameters_to_peer(peer)
            if task is None:
                break  # Fila de tarefas vazia
            dispatched.append((peer, task))

        if dispatched:
            with self.GlobalTable.lock:
                self.GlobalTable.system_state = "MODEL_DISTRIBUTION"

        return dispatched
    
    def receive_task_result(
        self,
        task_id: str,
        metrics: Dict[str, Any],
    ) -> Optional[tuple]:
        is_new_best = False

        with self.GlobalTable.lock:
            task_info = self.GlobalTable.assigned_tasks.pop(task_id, None)
            
        if task_info is None:
            return None

        peer = task_info["peer"]
        task = task_info["task"]
        peer.metrics = metrics

        new_score = float(metrics.get("f1_score", 0.0))
        
        # Obtém o score do melhor modelo atual usando o método thread-safe da GlobalTable
        current_best_model = self.GlobalTable.get_best_model()
        current_best_score = current_best_model["f1_score"] if current_best_model else -1.0

        if new_score > current_best_score:
            is_new_best = True
            # Salva o melhor modelo de forma centralizada
            self.GlobalTable.set_best_model({
                "task_id": task_id,
                "peer_id": peer.id_node,
                "hyperparameters": task.parametros,
                "metrics": dict(metrics),
                "f1_score": new_score,
            })
            
        if is_new_best and self._pubsub_queue is not None:
            publish_msg = PubSubPublish(
                id_node="server",
                topic=TOPIC_GLOBAL_BEST_SCORE,
                payload={
                    "task_id": task_id,
                    "id_node": peer.id_node,
                    "f1_score": metrics.get("f1_score", 0.0),
                    "accuracy": metrics.get("accuracy", 0.0),
                    "precision": metrics.get("precision", 0.0),
                    "recall": metrics.get("recall", 0.0),
                    "roc_auc": metrics.get("roc_auc", 0.0),
                },
            )
            self._pubsub_queue.put_nowait(publish_msg.to_dict())

        return peer, task

    def replicate_state_to_pupil(self):
        pupil_peer = getattr(self, 'pupil_peer', None)
        if not isinstance(pupil_peer, dict):
            return

        ip = pupil_peer.get('ip')
        port = pupil_peer.get('port')
        if ip is None or port is None:
            return

        # Pega o snapshot completo diretamente da GlobalTable (stateless)
        msg = {
            "type": "SyncState",
            "id_node": "server",
            "global_table_snapshot": self.GlobalTable.get_snapshot()
        }

        asyncio.create_task(send_once(ip, port, msg, expect_reply=False))