import asyncio
from typing import Any, Dict, Optional
from core.network import P2PNode, send_once, send_message
from utils.logger import get_logger
from utils.protocol import MSG_BULLY_ALIVE, MSG_BULLY_COORDINATOR, MSG_BULLY_ELECTION

log = get_logger("sync/bully")

class BullyElection:
    def __init__(self, my_id: str, globaltable_peers: Dict[str, Any], promote_callback):
        self.my_id = my_id
        self.peers = globaltable_peers  # Dicionário de peers ativos vindos da réplica da GlobalTable
        self.promote_callback = promote_callback # Função a rodar se este nó vencer
        self.election_in_progress = False

        self.current_coordinator_id: Optional[str] = None
        self.current_coordinator_ip: Optional[str] = None
        self.current_coordinator_port: Optional[int] = None

    async def detect_timeout_and_start(self):
        """Invocado quando o KeepAlive falha com o coordenador."""
        print("[Bully] Coordenador caiu. Iniciando eleição...")
        self.election_in_progress = True
        
        # Filtra peers com ID maior que o meu (comparação léxica do hash)
        higher_peers = {k: v for k, v in self.peers.items() if k > self.my_id}
        
        if not higher_peers:
            # Eu tenho o maior ID, ganhei
            await self._announce_victory()
            return

        # Envia Election para IDs maiores
        msg = {"type": "BullyElection", "id_node": self.my_id}
        responses = []
        for peer_id, info in higher_peers.items():
            resp = await send_once(info['ip'], info['port'], msg, expect_reply=True, timeout=2.0)
            if resp and resp.get("type") == "BullyAlive":
                responses.append(resp)

        # Se ninguém maior respondeu, eu ganho
        if not responses:
            await self._announce_victory()
        else:
            # Alguém maior assumirá, aguarda o anúncio
            pass

    async def _announce_victory(self):
        self.election_in_progress = False
        msg = {"type": "BullyCoordinator", "id_node": self.my_id}
        # Notifica todos os peers
        for peer_id, info in self.peers.items():
            if peer_id != self.my_id:
                asyncio.create_task(send_once(info['ip'], info['port'], msg, expect_reply=False))
        
        # Invoca callback para transformar este nó no servidor
        self.promote_callback()

    # Handlers para P2PNode
    def register_handlers(self, p2p_node: P2PNode) -> None:
        p2p_node.register_handler(MSG_BULLY_ELECTION, self.handle_election)
        p2p_node.register_handler(MSG_BULLY_COORDINATOR, self.handle_coordinator)
        p2p_node.register_handler(MSG_BULLY_ALIVE, self.handle_alive)

    async def handle_election(self, msg: dict, writer: asyncio.StreamWriter):
        sender_id = msg.get("id_node")
        # Normaliza e valida sender_id antes de comparar (pode ser None)
        if sender_id is None:
            return
        # Compare como strings (IDs são hashes/strings lexicográficos)
        try:
            if str(self.my_id) > str(sender_id):
                alive_msg = {"type": "BullyAlive", "id_node": self.my_id}
                await send_message(writer, alive_msg)
                if not self.election_in_progress:
                    asyncio.create_task(self.detect_timeout_and_start())
        except Exception:
            # Em caso de erro inesperado, não interrompe o nó
            log.warning("Exception caught in handle_election loop")
            return

    async def handle_coordinator(self, msg: dict, writer: asyncio.StreamWriter):
        new_coord = msg.get("id_node")
        print(f"[Bully] Novo coordenador estabelecido: {new_coord}")
        self.election_in_progress = False

        self.current_coordinator_id = new_coord

        info = self.peers.get(new_coord) if new_coord is not None else None
        if info is not None:
            self.current_coordinator_ip = info.get("ip")
            self.current_coordinator_port = info.get("port")
        else:
            self.current_coordinator_ip = None
            self.current_coordinator_port = None

    async def handle_alive(self, msg: dict, writer: asyncio.StreamWriter):
        sender_id = msg.get("id_node")
        if sender_id:
            log.debug(f"[Bully] heartbeat recebido de {sender_id}")
        await send_message(writer, {"type": "Ack", "ref_type": MSG_BULLY_ALIVE, "ref_id": sender_id})
