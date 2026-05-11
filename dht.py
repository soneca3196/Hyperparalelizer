import hashlib
import threading

class DHT:
    def __init__(self):
        # tabela para mapear: hash(IP+Porta) -> Metadados do Nó
        self.nodes = {}
        # tabela para mapear: hash(Nome do Fragmento) -> lista de IPs/IDs
        self.fragments_dataset = {}
        
        # garanti que duas threads nao modifiquem a DHT ao mesmo tempo
        self.lock = threading.Lock()

    # gera o hash sha256 dado o ip + porta ou nome do fragmento
    def generate_hash(self, key_string):
        return hashlib.sha256(key_string.encode('utf-8')).hexdigest()

    # adiciona um no a DHT, armazenando seu IP, porta e metadados
    def add_node(self, ip, port, metadata):
        node_id = self.generate_hash(f"{ip}:{port}")
        
        with self.lock: # bloqueia para outras threads enquanto escreve
            self.nodes[node_id] = {
                "ip": ip,
                "port": port,
                "metadata": metadata # ex: {"ram": "8GB", "cpu_usage": "20%"}
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
        

# Teste do DHT
# if __name__ == "__main__":
#     import time

#     print("--- INICIANDO TESTES DA DHT (HYPERPARALELIZER) ---\n")
    
#     # instanciando a DHT
#     dht = DHT()
    
#     # testando a adição de nós
#     print("Adicionando nós na rede...")
#     id_barriga = dht.add_node("192.168.0.10", 5000, {"role": "Coordenador (Barriga)", "ram": "16GB"})
#     id_madruga1 = dht.add_node("192.168.0.11", 5001, {"role": "Treinador (Madruga 1)", "ram": "8GB"})
#     id_madruga2 = dht.add_node("192.168.0.12", 5001, {"role": "Treinador (Madruga 2)", "ram": "4GB"})
    
#     print(f" -> ID Barriga: {id_barriga}")
#     print(f" -> ID Madruga 1: {id_madruga1}")
#     print(f" -> ID Madruga 2: {id_madruga2}\n")

#     # listagem de todos os nós ativos
#     print("Listando todos os nós ativos:")
#     todos_nos = dht.get_all_nodes()
#     for no in todos_nos:
#         print(f" -> IP: {no['ip']} | Role: {no['metadata']['role']}")
#     print("")

#     # mapeamento de fragmentos do dataset
#     print("Distribuindo fragmentos do dataset...")
#     # 1 pega o fragmento 1 e 2
#     dht.add_fragment_location("frag_treino_01.csv", id_madruga1)
#     dht.add_fragment_location("frag_treino_02.csv", id_madruga1)
    
#     # 2 pega o fragmento 2 (redundância) e 3
#     dht.add_fragment_location("frag_treino_02.csv", id_madruga2)
#     dht.add_fragment_location("frag_treino_03.csv", id_madruga2)
    
#     # verifica onde está o fragmento 2
#     locais_frag_2 = dht.get_fragment_locations("frag_treino_02.csv")
#     print(f" -> O 'frag_treino_02.csv' está salvo nos nós (IDs): {locais_frag_2}\n")

#     # testando a remoção de um nó (caso falha de KeepAlive)
#     print("Simulando queda do Madruga 1 (KeepAlive falhou)...")
#     dht.remove_node(id_madruga1)
    
#     # verificando se ele sumiu da lista de nós
#     nos_restantes = dht.get_all_nodes()
#     print(f" -> Quantidade de nós ativos agora: {len(nos_restantes)}")
    
#     # verificando se a DHT limpou os fragmentos que pertenciam a ele
#     locais_frag_2_atualizado = dht.get_fragment_locations("frag_treino_02.csv")
#     print(f" -> O 'frag_treino_02.csv' agora está salvo apenas em: {locais_frag_2_atualizado}")
    
#     print("\n--- TESTES FINALIZADOS ---")