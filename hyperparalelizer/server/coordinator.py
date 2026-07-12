#(Pessoa 4) Nó "Barriga": orquestra tarefas e middleware

# servidor com ESTADOS
# armazenar informações do estado da rede, o histórico de requisições e monitorar o desempenho dos modelos recebidos.

import asyncio
import itertools
import math
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
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


class State(Enum):
    HASHING = 0              # apos iniciar com o dataset, para distribuir
    DATASET_DISTRIBUTION = 1 # apos dar Hash, para distribuir dataset
    MODEL_DISTRIBUTION = 2   # Apos separar modelos e hiperparametros, para distribuir
    MODEL_COMPARE = 3        # apos receber metricas, para comparar modelos e requisita-lo
    FINISHED = 4


class Coordinator:
    def __init__(self, dataset, model, GlobalTable: GlobalTable,
                 model_type: str = "generic",
                 model_config: Optional[Dict[str, Any]] = None,
                 pubsub_queue: Optional[queue.Queue] = None):
        self.state = State.HASHING
        self.dataset = dataset
        self.model = model
        self.model_type = model_type
        self.model_config = model_config or {}

        self.GlobalTable = GlobalTable
        self.peers: List[Peer] = []

        self.task_pool: List[TrainingTask] = []
        # task_id -> {"peer": Peer, "timestamp": float, "task": TrainingTask}
        self.assigned_tasks: Dict[str, dict] = {}

        self.dataset_fragments: List[str] = []  # nomes dos fragmentos gerados
        self._fragments_data: Dict[str, Any] = {}  # nome -> dados do fragmento

        # best_model: {"task_id", "peer_id", "hyperparameters", "metrics", "f1_score"}
        self.best_model: Optional[Dict[str, Any]] = None
        self._pubsub_queue = pubsub_queue  # fila Middleware → PubSubClient
        self._lock = threading.Lock()
        
        # Referência ao peer pupilo (se existir)
        self.pupil_peer: Optional[Dict[str, Any]] = None

    # ENDPOINT: ADICIONA NOVO PEER                                      
    def add_peer(self, peer: Peer) -> str:
        # Ao entrar na rede, cada peer recebe um ID único baseado no hash SHA-256(IP:Porta), gerado pela GlobalTable.
        node_id = self.GlobalTable.add_node(
            peer.ip, 
            peer.port, 
            {
            "received_dataset": peer.received_dataset,
            "received_model": peer.received_model,
            }
        )
        peer.id_node = node_id

        with self._lock:
            self.peers.append(peer)
            peer_index = len(self.peers) - 1 

        # Associa um fragmento de dataset ao peer (round-robin)
        if self.dataset_fragments:
            frag_index = peer_index % len(self.dataset_fragments)
            fragment_name = self.dataset_fragments[frag_index]
            self.GlobalTable.add_fragment_location(fragment_name, node_id)

        # Atribui hiperparâmetros pendentes da fila
        self.assign_hyperparameters_to_peer(peer)

        return node_id
    
    # GRID SEARCH                                               
    # Gera todas as combinações possíveis de hiperparâmetros através do produto cartesiano.           
    # Entrada: {"lr": [0.01, 0.001], "n_estimators": [10, 50]}
    #  Saída  : [{"lr": 0.01, "n_estimators": 10}, ...]
    def generate_grid_search(self, hyperparameters: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
       
        if not self.dataset_fragments:
            raise RuntimeError(
                "Execute fragment_dataset() antes de generate_grid_search()."
            )

        keys = list(hyperparameters.keys())
        values = list(hyperparameters.values())

        combinations = [
            dict(zip(keys, combo))
            for combo in itertools.product(*values)
        ]
        # Cada combinação vira uma TrainingTask
        with self._lock:
            self.task_pool = [
                TrainingTask(
                    task_id=str(uuid.uuid4()),
                    id_node_origem="server",
                    dataset_fragmentos=list(self.dataset_fragments),
                    parametros=combo,
                    model_type=self.model_type,
                    model_config=dict(self.model_config),
                )
                for combo in combinations
            ]

        return combinations

    # ATRIBUIÇÃO DE HIPERPARÂMETROS                                        
    def assign_hyperparameters_to_peer(self, peer: Peer) -> Optional[TrainingTask]:
        # a próxima tarefa da fila é atribuída ao peer. None se a fila estiver vazia
        with self._lock:
            if not self.task_pool:
                return None
            task = self.task_pool.pop(0)
            self.assigned_tasks[task.task_id] = {
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
        if n_fragments is None:
            n_fragments = max(1, len(self.peers))

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
            self._fragments_data[frag_name] = frag_data

            # Associa o fragmento ao peer já registrado, se houver
            if i < len(self.peers):
                self.GlobalTable.add_fragment_location(frag_name, self.peers[i].id_node)

        with self._lock:
            self.dataset_fragments = fragment_names

        self.state = State.DATASET_DISTRIBUTION
        return fragment_names

    # VERIFICAÇÃO DE STATUS / TIMEOUT                                      
    def check_task_status(self) -> List[str]:
        
        #Verifica as tarefas em andamento. Tasks que ultrapassaram TASK_TIMEOUT são removidas de assigned_tasks e devolvidas à task_pool.

        now = time.time()
        timed_out_ids: List[str] = []

        with self._lock:
            for task_id, entry in list(self.assigned_tasks.items()):
                if now - entry["timestamp"] > TASK_TIMEOUT:
                    timed_out_ids.append(task_id)

            for task_id in timed_out_ids:
                task = self.assigned_tasks.pop(task_id)["task"]
                self.task_pool.append(task)  # devolve à fila para reatribuir

        return timed_out_ids

    # DISTRIBUIÇÃO                                                         
    def distribute_dataset(self):
        # Lógica de envio dos fragmentos delegada ao ServerMessenger
        self.state = State.DATASET_DISTRIBUTION

    def distribute_model_and_hyperparameters(self) -> List[tuple]:
        # peer é ocupado enquanto possuir pelo menos uma tarefa registrada em assigned_tasks.
        with self._lock:
            busy_ids = {entry["peer"].id_node for entry in self.assigned_tasks.values()}
        # só peers livres recebem novas tarefas
        idle_peers = [p for p in self.peers if p.id_node not in busy_ids]

        dispatched: List[tuple] = []
        for peer in idle_peers:
            task = self.assign_hyperparameters_to_peer(peer)
            if task is None:
                break  # fila vazia
            dispatched.append((peer, task))

        if dispatched:
            self.state = State.MODEL_DISTRIBUTION

        return dispatched
    
    def receive_task_result(
        self,
        task_id: str,
        metrics: Dict[str, Any],
    ) -> Optional[tuple]:
        """
         Quando uma tarefa termina:
        
         1. Remove da lista de tarefas em execução.
         2. Libera o peer para receber novas tarefas.
         3. Armazena as métricas produzidas.
         4. Compara com o melhor modelo global atual.
         5. Caso seja um novo melhor resultado, publica no tópico global_best_score para notificar a rede.
        """
        is_new_best = False

        with self._lock:
            task_info = self.assigned_tasks.pop(task_id, None)
            if task_info is None:
                return None

            peer = task_info["peer"]
            task = task_info["task"]
            peer.metrics = metrics

            new_score = float(metrics.get("f1_score", 0.0))
            current_best = self.best_model["f1_score"] if self.best_model else -1.0

            if new_score > current_best:
                is_new_best = True
                self.best_model = {
                    "task_id": task_id,
                    "peer_id": peer.id_node,
                    "hyperparameters": task.parametros,
                    "metrics": dict(metrics),
                    "f1_score": new_score,
                }
        # encontrou um modelo melhor? publica no tópico global_best_score para notificar a rede
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

        msg = {
            "type": "SyncState",
            "id_node": "server",
            "global_table_snapshot": self.GlobalTable.nodes,
            "task_queue_snapshot": self.task_pool,
            "best_model_metrics": self.best_model,
        }

        asyncio.create_task(send_once(ip, port, msg, expect_reply=False))
