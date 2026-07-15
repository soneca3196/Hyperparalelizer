from peer.data_thread import DataThread
from peer.peer_messenger import PeerMessenger
from peer.trainer import TrainerNode


# Criar instancias peer
dataThread = DataThread(own_ip, own_port, server_ip, server_port)
reply = dataThread.join_network()
node_id = reply.get("node_id")

# não sei se vai usar esses de baixo
fragment_id = reply.get("fragment_id")
known_peers = reply.get("peers") or []
initial_task = reply.get("task")

pm = PeerMessenger(node_id, server_ip, server_port)
