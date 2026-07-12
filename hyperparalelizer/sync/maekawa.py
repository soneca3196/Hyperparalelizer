import asyncio
from typing import List, Dict, Any
from hyperparalelizer.sync.lamport import LamportClock
from core.network import send_once

class MaekawaMutex:
    def __init__(self, node_id: str, quorum: List[Dict[str, Any]]):
        self.node_id = node_id
        self.quorum = quorum  # Lista de dicionários com 'ip' e 'port' dos membros do quórum
        self.clock = LamportClock()
        
        # Estados: RELEASED, WANTED, HELD
        self.state = "RELEASED"
        self.voted = False
        self.request_queue = [] # Fila de prioridade: (timestamp, node_id)
        
        self.grants_received = 0
        self.grant_event = asyncio.Event()


    async def request_access(self):
        """Peer invoca isso antes de tentar atualizar o best_model."""
        self.state = "WANTED"
        self.grants_received = 0
        self.grant_event.clear()
        
        req_time = self.clock.increment()
        msg = {"type": "MaekawaRequest", "id_node": self.node_id, "timestamp": req_time}
        
        # Envia Request para todo o quórum
        for peer in self.quorum:
            asyncio.create_task(send_once(peer['ip'], peer['port'], msg, expect_reply=False))
        
        # Aguarda a maioria/todos do quórum concederem
        await self.grant_event.wait()
        self.state = "HELD"


    async def release_access(self):
        """Peer invoca isso após atualizar o best_model."""
        self.state = "RELEASED"
        msg = {"type": "MaekawaRelease", "id_node": self.node_id}
        
        for peer in self.quorum:
            asyncio.create_task(send_once(peer['ip'], peer['port'], msg, expect_reply=False))


    def _get_peer_address(self, target_id: str):
        """Busca o IP e Porta do nó dentro do quórum conhecido."""
        for peer in self.quorum:
            if peer['id_node'] == target_id:
                return peer['ip'], peer['port']
        return None, None


    # Handlers para anexar no servidor P2P (P2PNode.register_handler)
    async def handle_request(self, msg: dict, writer: asyncio.StreamWriter):
        req_time = msg["timestamp"]
        req_node = msg["id_node"]
        self.clock.update(req_time)
        
        if self.state == "HELD" or self.voted:
            self.request_queue.append((req_time, req_node))
            self.request_queue.sort() 
        else:
            self.voted = True
            grant_msg = {"type": "MaekawaGrant", "id_node": self.node_id}
            
            # Busca o IP e Porta 
            ip_do_no, porta_do_no = self._get_peer_address(req_node)
            if ip_do_no:
                await send_once(ip_do_no, porta_do_no, grant_msg, expect_reply=False)


    async def handle_grant(self, msg: dict, writer: asyncio.StreamWriter):
        self.grants_received += 1
        if self.grants_received >= len(self.quorum):
            self.grant_event.set()


    async def handle_release(self, msg: dict, writer: asyncio.StreamWriter):
        if self.request_queue:
            next_req = self.request_queue.pop(0)
            target_node_id = next_req[1] # Pega o ID do nó que estava na fila
            
            grant_msg = {"type": "MaekawaGrant", "id_node": self.node_id}
            
            ip_do_no, porta_do_no = self._get_peer_address(target_node_id)
            if ip_do_no:
                await send_once(ip_do_no, porta_do_no, grant_msg, expect_reply=False)
            
            self.voted = True
        else:
            self.voted = False