#(Pessoa 4) Nó "Barriga": orquestra tarefas e middleware

# servidor com ESTADOS
# armazenar informações do estado da rede, o histórico de requisições e monitorar o desempenho dos modelos recebidos.

import itertools
import math
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from hyperparalelizer.server.dht_global import DHT
from utils.protocol import TrainingTask

TASK_TIMEOUT = 30.0  # segundos até uma tarefa ser considerada perdida


@dataclass
class Peer:
    ip: str
    port: int
    id_node: str = ""          # preenchido pelo DHT após add_node
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
    def __init__(self, dataset, model, dht: DHT,
                 model_type: str = "generic",
                 model_config: Optional[Dict[str, Any]] = None):
        self.state = State.HASHING
        self.dataset = dataset
        self.model = model
        self.model_type = model_type
        self.model_config = model_config or {}

        self.dht: DHT = dht
        self.peers: List[Peer] = []

        self.task_pool: List[TrainingTask] = []
        # task_id -> {"peer": Peer, "timestamp": float, "task": TrainingTask}
        self.assigned_tasks: Dict[str, dict] = {}

        self.dataset_fragments: List[str] = []  # nomes dos fragmentos gerados
        self._fragments_data: Dict[str, Any] = {}  # nome -> dados do fragmento

        self.best_model = None
        self._lock = threading.Lock()

    # ENDPOINT: ADICIONA NOVO PEER                                      
    def add_peer(self, peer: Peer) -> str:
        # Ao entrar na rede, cada peer recebe um ID único baseado no hash SHA-256(IP:Porta), gerado pela DHT.
        node_id = self.dht.add_node(peer.ip, peer.port, {
            "received_dataset": peer.received_dataset,
            "received_model": peer.received_model,
        })
        peer.id_node = node_id

        with self._lock:
            self.peers.append(peer)
            peer_index = len(self.peers) - 1 

        # Associa um fragmento de dataset ao peer (round-robin)
        if self.dataset_fragments:
            frag_index = peer_index % len(self.dataset_fragments)
            fragment_name = self.dataset_fragments[frag_index]
            self.dht.add_fragment_location(fragment_name, node_id)

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
        na DHT associado ao peer correspondente (se já houver peers)
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
                self.dht.add_fragment_location(frag_name, self.peers[i].id_node)

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
    
    """
    Recebe o resultado de uma tarefa concluída.
   
    Responsabilidades:

    1. Remove a tarefa da lista de execução.
    2. Libera o peer para novos trabalhos.
    3. Armazena as métricas retornadas.
    4. Atualiza o melhor modelo global.
    """
    def receive_task_result(
        self,
        task_id: str,
        metrics: Dict[str, Any]
    ):
        with self._lock:
            task_info = self.assigned_tasks.pop(task_id, None)

            if task_info is None:
                return None

            peer = task_info["peer"]
            task = task_info["task"]

            peer.metrics = metrics

        return peer, task