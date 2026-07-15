# "container" thread-safe para variaveis de dicionários e listas críticos

import hashlib
import threading
from enum import Enum
from dataclasses import asdict

class ServerState(Enum):
    HASHING = 0
    OPEN = 1
    DATASET_DISTRIBUTION = 2
    MODEL_DISTRIBUTION = 3

class GlobalTable:
    def __init__(self, snapshot=None):
        self.lock = threading.Lock() # garante que duas threads nao modifiquem a GlobalTable ao mesmo tempo
        
        self.nodes = {} # tabela para mapear: hash(IP+Porta) -> Metadados do Nó
        self.fragments_payloads = {} # Conteudo Real do Fragmento
        self.fragments_locations = {} # Localizacao do Fragmentos na rede de peers
        self._system_state = ServerState.HASHING
        self.best_model = None
        self.task_pool = []
        self.assigned_tasks = {} # task_id -> {node_id, task, timestamp}
        
        if snapshot is not None:
            self.overwrite_from_snapshot(snapshot)
            return

    @property
    def system_state(self) -> ServerState:
        return self._system_state

    @system_state.setter
    def system_state(self, value):
        if isinstance(value, ServerState):
            self._system_state = value
        elif isinstance(value, str):
            try:
                self._system_state = ServerState[value]
            except KeyError:
                raise ValueError(
                    f"system_state inválido: {value!r}. "
                    f"Esperado um dos: {[s.name for s in ServerState]}"
                )
        else:
            raise TypeError(
                f"system_state deve ser ServerState ou str, recebeu {type(value)}"
            )

    def set_best_model(self, model_data):
        with self.lock:
            self.best_model = model_data

    def get_best_model(self):
        with self.lock:
            return self.best_model


    def get_snapshot(self, include_fragment_payloads: bool = True):
        """Retorna uma cópia limpa de todo o estado para enviar ao pupilo
        """
        with self.lock:
            # Serializa os nós (Convertendo Peer em dicionário)
            nodes_serialized = {}
            for k, v in self.nodes.items():
                node_copy = v.copy()
                if hasattr(node_copy.get("metadata"), "__dataclass_fields__"):
                    node_copy["metadata"] = asdict(node_copy["metadata"])
                nodes_serialized[k] = node_copy


            # Serializa a fila de tarefas (Convertendo TrainningTask em dicionário)
            task_pool_serialized = [
                t.to_dict() if hasattr(t, "to_dict") else t 
                for t in self.task_pool
            ]

            # Serializa as tarefas atribuídas 
            assigned_tasks_serialized = {}
            for k, v in self.assigned_tasks.items():
                task_info = v.copy()
                if hasattr(task_info.get("peer"), "__dataclass_fields__"):
                    task_info["peer"] = asdict(task_info["peer"])
                if hasattr(task_info.get("task"), "to_dict"):
                    task_info["task"] = task_info["task"].to_dict()
                assigned_tasks_serialized[k] = task_info

            return {
                "nodes": nodes_serialized,
                "fragments_payloads": (
                    self.fragments_payloads.copy() if include_fragment_payloads else {}
                ),
                "fragments_locations": {k: list(v) for k, v in self.fragments_locations.items()},
                
                "system_state": self._system_state.name,
                "best_model": self.best_model.copy() if self.best_model else None,
                "task_pool": task_pool_serialized,
                "assigned_tasks": assigned_tasks_serialized.copy()
            }
        
    def overwrite_from_snapshot(self, snapshot):
        """Usado pelo pupilo para assumir o estado se o mestre cair."""
        from hyperparalelizer.server.coordinator import Peer
        from utils.protocol import from_dict
        
        # Reconstruir nos (dicionario para Peer)
        with self.lock:
            self.nodes = {}
            for k, v in snapshot.get("nodes", {}).items():
                node_data = v.copy()
                if isinstance(node_data.get("metadata"), dict):
                    node_data["metadata"] = Peer(**node_data["metadata"])
                self.nodes[k] = node_data

            self.fragments_payloads = snapshot.get("fragments_payloads", {})
            self.fragments_locations = snapshot.get("fragments_locations", {})
            self.system_state = snapshot.get("system_state", "HASHING")
            self.best_model = snapshot.get("best_model", None)

            # Reconstruir task_pool (dicionario para TrainningTask)
            self.task_pool = []
            for t in snapshot.get("task_pool", []):
                if isinstance(t, dict) and "type" in t:
                    self.task_pool.append(from_dict(t))
                else:
                    self.task_pool.append(t)

            # Reconstruir assigned_tasks (dicionario para Peer e TrainningTask)
            self.assigned_tasks = {}
            for k, v in snapshot.get("assigned_tasks", {}).items():
                task_info = v.copy()
                if isinstance(task_info.get("peer"), dict):
                    task_info["peer"] = Peer(**task_info["peer"])
                if isinstance(task_info.get("task"), dict) and "type" in task_info["task"]:
                    task_info["task"] = from_dict(task_info["task"])
                self.assigned_tasks[k] = task_info


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
            
            # 2. Remove o nó de todas as localizações de fragmentos
            for fragment_name in self.fragments_locations:
                if node_id in self.fragments_locations[fragment_name]:
                    self.fragments_locations[fragment_name].remove(node_id)

    # adiciona a localização de um fragmento (nome do fragmento) associada a um nó (node_id)
    def add_fragment_location(self, fragment_name, node_id):
        
        with self.lock:
            if fragment_name not in self.fragments_locations:
                self.fragments_locations[fragment_name] = []
            
            # evita de dois nos terem o mesmo fragmento repetido na lista
            if node_id not in self.fragments_locations[fragment_name]:
                self.fragments_locations[fragment_name].append(node_id)
            
    # endpoint para obter as localizações de um fragmento dado seu nome
    def get_fragment_locations(self, fragment_name):
        with self.lock:
            return self.fragments_locations.get(fragment_name, [])