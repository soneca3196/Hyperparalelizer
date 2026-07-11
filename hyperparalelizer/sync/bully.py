import asyncio
from typing import Dict, Any
from core.network import send_once

class BullyElection:
    def __init__(self, my_id: str, globaltable_peers: Dict[str, Any], promote_callback):
        self.my_id = my_id
        self.peers = globaltable_peers  # Dicionário de peers ativos vindos da réplica da GlobalTable
        self.promote_callback = promote_callback # Função a rodar se este nó vencer
        self.election_in_progress = False

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
    async def handle_election(self, msg: dict, writer: asyncio.StreamWriter):
        sender_id = msg.get("id_node")
        if self.my_id > sender_id:
            alive_msg = {"type": "BullyAlive", "id_node": self.my_id}
            await send_once(writer.get_extra_info('peername')[0], msg.get("port", 5000), alive_msg, expect_reply=False)
            if not self.election_in_progress:
                asyncio.create_task(self.detect_timeout_and_start())

    async def handle_coordinator(self, msg: dict, writer: asyncio.StreamWriter):
        new_coord = msg.get("id_node")
        print(f"[Bully] Novo coordenador estabelecido: {new_coord}")
        self.election_in_progress = False
        # Atualiza o endereço do middleware na rede P2P