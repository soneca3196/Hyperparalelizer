"""Lado servidor da troca de fragmentos"""

import asyncio
import os
from typing import Any, Dict

from utils.logger import get_logger
from utils.protocol import MSG_REQUEST_FRAGMENT, FragmentData, FragmentNotFound
from core.network import send_message, P2PNode

log = get_logger("peer_peer_protocol")


async def handle_request_fragment(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    storage_dir: str,
) -> None:
    """Devolve o fragmento se existir, senão FragmentNotFound"""
    fragment_id = msg.get("fragment_id", "")
    requester = msg.get("id_node", "")
    path = os.path.join(storage_dir, f"{fragment_id}.bin")

    if not os.path.exists(path):
        log.debug(f"peer-peer: não possuo '{fragment_id}'")
        reply = FragmentNotFound(id_node=requester, fragment_id=fragment_id).to_dict()
        await send_message(writer, reply)
        return

    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError as exc:
        log.error(f"peer-peer: erro lendo '{fragment_id}' - {exc}")
        reply = FragmentNotFound(id_node=requester, fragment_id=fragment_id).to_dict()
        await send_message(writer, reply)
        return

    log.info(f"peer-peer: enviando '{fragment_id}' ({len(data)} bytes) para {requester[:8] if requester else '?'}..")
    reply = FragmentData(id_node=requester, fragment_id=fragment_id, data=data).to_dict()
    await send_message(writer, reply)


def register_peer_peer_handlers(p2p_node: P2PNode, storage_dir: str) -> None:
    """Registra RequestFragment. Chamar depois de instanciar o P2PNode e antes do start()"""

    async def _handler(msg: Dict[str, Any], writer: asyncio.StreamWriter) -> None:
        await handle_request_fragment(msg, writer, storage_dir)

    p2p_node.register_handler(MSG_REQUEST_FRAGMENT, _handler)
    log.debug("peer-peer_protocol: handler de RequestFragment registrado")