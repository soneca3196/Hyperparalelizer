import asyncio
from typing import List, Dict, Any
from hyperparalelizer.sync.lamport import LamportClock
from core.network import P2PNode, send_once, send_message
from utils.protocol import MSG_MAEKAWA_GRANT, MSG_MAEKAWA_RELEASE, MSG_MAEKAWA_REQUEST


class MaekawaTimeoutError(Exception):
    """Levantada quando request_access() não consegue obter o quórum
    completo de grants dentro do tempo/tentativas configurados."""
    pass


class MaekawaMutex:
    # Configuração padrão de timeout/retry para aquisição do lock
    DEFAULT_GRANT_TIMEOUT = 10.0   # segundos por tentativa
    DEFAULT_MAX_RETRIES = 3        # tentativas antes de desistir

    def __init__(self, node_id: str, quorum: List[Dict[str, Any]]):
        self.node_id = node_id
        self.quorum = quorum  # Lista de dicionários com 'ip' e 'port' dos membros do quórum
        self.clock = LamportClock()
        
        # Estados: RELEASED, WANTED, HELD
        self.state = "RELEASED"
        self.voted = False
        self.request_queue = [] # Fila de prioridade: (timestamp, node_id)
        
        self.granted_by = set()
        self.grant_event = asyncio.Event()


    async def request_access(
        self,
        timeout: float = DEFAULT_GRANT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        """Peer invoca isso antes de tentar atualizar o best_model
        """
        if not self.quorum:
            self.state = "HELD"
            return

        self.state = "WANTED"
        self.granted_by = set()
        self.grant_event.clear()

        req_time = self.clock.increment()
        msg = {"type": "MaekawaRequest", "id_node": self.node_id, "timestamp": req_time}

        pending = list(self.quorum)

        for attempt in range(1, max_retries + 1):
            # Envia (ou reenvia) Request apenas para quem ainda não concedeu
            for peer in pending:
                if peer['id_node'] not in self.granted_by:
                    asyncio.create_task(
                        send_once(peer['ip'], peer['port'], msg, expect_reply=False)
                    )

            try:
                await asyncio.wait_for(self.grant_event.wait(), timeout=timeout)
                self.state = "HELD"
                return
            except asyncio.TimeoutError:
                missing = [p['id_node'] for p in pending if p['id_node'] not in self.granted_by]
                if attempt < max_retries:
                    continue  # tenta de novo, só para quem falta

        # Esgotou as tentativas: desiste e libera quem já tinha votado
        self.state = "RELEASED"
        release_msg = {"type": "MaekawaRelease", "id_node": self.node_id}
        for peer in pending:
            if peer['id_node'] in self.granted_by:
                asyncio.create_task(
                    send_once(peer['ip'], peer['port'], release_msg, expect_reply=False)
                )

        raise MaekawaTimeoutError(
            f"Timeout aguardando quórum Maekawa: {len(self.granted_by)}/{len(self.quorum)} "
            f"grants recebidos após {max_retries} tentativas. Faltando: {missing}"
        )


    def register_handlers(self, p2p_node: P2PNode) -> None:
        p2p_node.register_handler(MSG_MAEKAWA_REQUEST, self.handle_request)
        p2p_node.register_handler(MSG_MAEKAWA_GRANT, self.handle_grant)
        p2p_node.register_handler(MSG_MAEKAWA_RELEASE, self.handle_release)

    async def release_access(self):
        """Peer invoca isso após atualizar o best_model."""
        if not self.quorum:
            self.state = "RELEASED"
            return

        self.state = "RELEASED"
        msg = {"type": "MaekawaRelease", "id_node": self.node_id}
        
        for peer in self.quorum:
            asyncio.create_task(send_once(peer['ip'], peer['port'], msg, expect_reply=False))

    def replace_quorum(self, peers) -> None:
        """Atualiza o quórum dinamicamente (usado via MembershipUpdate)."""
        self.quorum = [
            p for p in peers
            if isinstance(p, dict) and p.get("id_node") != self.node_id
        ]


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
            grant_msg = {"type": MSG_MAEKAWA_GRANT, "id_node": self.node_id}
            
            # Busca o IP e Porta 
            ip_do_no, porta_do_no = self._get_peer_address(req_node)
            if ip_do_no and porta_do_no:
                await send_once(ip_do_no, porta_do_no, grant_msg, expect_reply=False)
            else:
                await send_message(writer, grant_msg)


    async def handle_grant(self, msg: dict, writer: asyncio.StreamWriter):
        sender_id = msg.get("id_node")
        if sender_id:
            self.granted_by.add(sender_id)

        if len(self.granted_by) >= len(self.quorum):
            self.grant_event.set()

        await send_message(writer, {"type": "Ack", "ref_type": MSG_MAEKAWA_GRANT, "ref_id": sender_id})


    async def handle_release(self, msg: dict, writer: asyncio.StreamWriter):
        if self.request_queue:
            next_req = self.request_queue.pop(0)
            target_node_id = next_req[1] # Pega o ID do nó que estava na fila
            
            grant_msg = {"type": MSG_MAEKAWA_GRANT, "id_node": self.node_id}
            
            ip_do_no, porta_do_no = self._get_peer_address(target_node_id)

            if ip_do_no and porta_do_no:
                await send_once(ip_do_no, porta_do_no, grant_msg, expect_reply=False)
            
            self.voted = True
        else:
            self.voted = False

        await send_message(writer, {"type": "Ack", "ref_type": MSG_MAEKAWA_RELEASE, "ref_id": msg.get("id_node")})