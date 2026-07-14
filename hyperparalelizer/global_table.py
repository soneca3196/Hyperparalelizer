# "container" thread-safe para variaveis de dicionários e listas críticos

import hashlib
import threading
from enum import Enum

class ServerState(Enum):
    HASHING = 0
    OPEN = 1

class GlobalTable:
    def __init__(self, snapshot=None):
        self.lock = threading.Lock() # garante que duas threads nao modifiquem a GlobalTable ao mesmo tempo

        if snapshot is not None:
            self.overwrite_from_snapshot(snapshot)
            return
        
        self.nodes = {} # tabela para mapear: hash(IP+Porta) -> Metadados do Nó
        #self.server_state = ServerState('HASHING')
        self.fragments_dataset = {} # tabela para mapear: hash(Nome do Fragmento) -> lista de IPs/IDs
        self.best_model = None

        self.task_pool = []
        self.assigned_tasks = {}      # task_id -> {node_id, task, timestamp}
        
        
    def set_best_model(self, model_data):
        with self.lock:
            self.best_model = model_data

    def get_best_model(self):
        with self.lock:
            return self.best_model


    def get_snapshot(self):
        """Retorna uma cópia limpa de todo o estado para enviar ao pupilo."""
        with self.lock:
            return {
                "nodes": self.nodes.copy(),
                "fragments": self.fragments.copy(),
                "system_state": self.system_state,
                "best_model": self.best_model.copy() if self.best_model else None,
                "task_pool": list(self.task_pool),
                "assigned_tasks": self.assigned_tasks.copy()
            }
        
    def overwrite_from_snapshot(self, snapshot):
        """Usado pelo pupilo para assumir o estado se o mestre cair."""
        with self.lock:
            self.nodes = snapshot.get("nodes", {})
            self.fragments = snapshot.get("fragments", {})
            self.system_state = snapshot.get("system_state", "HASHING")
            self.best_model = snapshot.get("best_model", None)
            self.task_pool = snapshot.get("task_pool", [])
            self.assigned_tasks = snapshot.get("assigned_tasks", {})

    # gera o hash sha256 dado o ip + porta ou nome do fragmento
    def generate_hash(self, key_string):
        return hashlib.sha256(key_string.encode('utf-8')).hexdigest()

    # adiciona um no a GlobalTable, armazenando seu IP, porta e metadados
    def add_node(self, ip, port, metadata):
        ''' Adiciona node a table, e retorna o node_id para administração interna do coordinator'''
        node_id = self.generate_hash(f"{ip}:{port}")
        
        with self.lock: # bloqueia para outras threads enquanto escreve
            self.nodes[node_id] = {
                "node_id": node_id,
                "ip": ip,
                "port": port,
                "metadata": metadata # dados Peer
            }
        return node_id

    # endpoint para obter os metadados de um no dado seu ID
    def get_node(self, node_id): 
        with self.lock: # Bloqueia para leitura segura
            return self.nodes.get(node_id, None)

    # endpoint para listar todos os nós ativos (para o coordenador)
    def get_all_nodes(self):
        with self.lock:
            return list(self.nodes.values())

    # endpoint para remover um nó (quando o KeepAlive falhar)
    def remove_node(self, node_id):
        with self.lock:
            # 1. Remove da tabela de nós
            if node_id in self.nodes:
                del self.nodes[node_id]
            
            # 2. Remove o nó de todos os fragmentos que ele possuía
            for frag_id in self.fragments_dataset:
                if node_id in self.fragments_dataset[frag_id]:
                    self.fragments_dataset[frag_id].remove(node_id)

    # adiciona a localização de um fragmento (nome do fragmento) associada a um nó (node_id)
    def add_fragment_location(self, fragment_name, node_id):
        frag_id = self.generate_hash(fragment_name)
        
        with self.lock:
            if frag_id not in self.fragments_dataset:
                self.fragments_dataset[frag_id] = []
            
            # evita de dois nos terem o mesmo fragmento repetido na lista
            if node_id not in self.fragments_dataset[frag_id]:
                self.fragments_dataset[frag_id].append(node_id)
            
    # endpoint para obter as localizações de um fragmento dado seu nome
    def get_fragment_locations(self, fragment_name):
        frag_id = self.generate_hash(fragment_name)
        with self.lock:
            return self.fragments_dataset.get(frag_id, [])