#(Pessoa 4) Nó "Barriga": orquestra tarefas e middleware


# servidor com ESTADOS
# armazenar informações do estado da rede, o histórico de requisições e monitorar o desempenho dos modelos recebidos.

# GRID SEARCH: "divide and conquer": conforme são adicionados peers, o coordenador envia um hyperparametro no meio de dois outros já testados/assigned.


from enum import Enum
from dataclasses import dataclass
import queue
import pandas as pd

class Coordinator:
    def __init__(self, dataset: pd.DataFrame, 
                 model_type: str, 
                 msg_in: queue.Queue[dict], 
                 msg_out: queue.Queue[dict], 
                 DEBUG=False):

        self.DEBUG = DEBUG  # ativa prints para debugar

        self.dataset = dataset
        self.model_type = model_type

        self.peers = []
        self.task_pool = []  # Fila de tarefas para os peers

        self.best_model = None

        # Queues de Mensageria entre server_messenger/pubsub
        self.msg_in = msg_in
        self.msg_out = msg_out
    
        
    def listen_for_queue(self):
        msg = self.msg_in.get() # RECEBE MENSAGEM DO PUBSUB
        if self.DEBUG: print("msg_in: " + msg)

        # TRATAMENTO DAS MENSAGENS (TO DO)
            # JoinNetwork (novo peer):
                # registra, envia endereços do dataset e primeira task
        
            # TaskResult (resultado de treinamento):
                # salva resultado daquela combinação de hiperparametros em tabela
                # compara e atualiza best_model

        queue.done()

        # caso der ruim em um peer (timeout), volta a tarefa de treinamento para a pool
        pass

    def send_to_queue(self):
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
        # Fragmenta o dataset conforme o tamanho

        pass



    def distribute_dataset(self):
        pass
    
    def distribute_model_and_hyperparameters(self):
        # Distribui o modelo e as combinações de hiperparâmetros para os peers
        # gera as mensagens Training Task contendo os parâmetros a serem testados. Conforme descobre nós ociosos via DHT/Heartbeat, ele retira uma combinação da fila e envia para o nó.
        pass

    




