#(Pessoa 4) Nó "Barriga": orquestra tarefas e middleware


# servidor com ESTADOS
# armazenar informações do estado da rede, o histórico de requisições e monitorar o desempenho dos modelos recebidos.

# GRID SEARCH: "divide and conquer": conforme são adicionados peers, o coordenador envia um hyperparametro no meio de dois outros já testados/assigned.


from enum import Enum
from dataclasses import dataclass

# tarefas_para_processar = queue.Queue()

@dataclass
class Peer:
    id_node: int
    hyperparameters: dict
    metrics: dict
    received_dataset: bool = False
    received_model: bool = False

class State(Enum):
    HASHING = 0 # apos iniciar com o dataset, para distribuir
    DATASET_DISTRIBUTION = 1 # apos dar Hash, para distribuir dataset
    MODEL_DISTRIBUTION = 2 # Apos separar modelos e hiperparametros, para distribuir
    MODEL_COMPARE = 3 # apos receber metricas, para comparar modelos e requisita-lo
    FINISHED = 4

class Coordinator:
    def __init__(self, dataset, model):
        # Inicialização do coordenador
        self.state = State.HASHING
        self.dataset = dataset
        self.model = model

        self.peers = []

        self.task_pool = []  # Fila de tarefas para os peers

        self.best_model = None
        pass
    
    # ENDPOINT: ADICIONA NOVO PEER
    def add_peer(self, peer):
        # atualizar a DHT com metadados
            # assign fragmento dataset
            # assign hyperparametros
        self.assign_hyperparameters_to_peer(peer)

        pass

    def generate_grid_search(self, hyperparameters):
        # Gera combinações de hiperparâmetros para distribuição
        # calcula o produto cartesiano de todas as combinações possíveis de hiperparâmetros

        pass

    def assign_hyperparameters_to_peer(self):
        # GRID SEARCH: "divide and conquer": conforme são adicionados peers, o coordenador envia um hyperparametro no meio de dois outros já testados/assigned.
        pass

    def fragment_dataset(self):
        # Fragmenta o dataset para distribuição entre os peers
        # chama DHT
        pass

    def check_task_status(self):
        # VAI PRO MESSENGER
        # Verifica o status das tarefas enviadas
        # caso der ruim em um peer (timeout), volta a tarefa para a pool
        pass

    def distribute_dataset(self):
        pass
    
    def distribute_model_and_hyperparameters(self):
        # Distribui o modelo e as combinações de hiperparâmetros para os peers
        # gera as mensagens Training Task contendo os parâmetros a serem testados. Conforme descobre nós ociosos via DHT/Heartbeat, ele retira uma combinação da fila e envia para o nó.
        pass

    




