"""Lado cliente da troca de fragmentos: pede a outro peer ou, em último
caso, ao servidor (backup), e salva os bytes recebidos em disco"""

import os
from typing import Optional

from utils.logger import get_logger
from utils.protocol import (
    RequestFragment,
    RequestFragmentBackup,
    MSG_FRAGMENT_DATA,
    MSG_FRAGMENT_NOT_FOUND,
    MSG_FRAGMENT_BACKUP,
)
from core.network import send_once

log = get_logger("download_worker")

DOWNLOAD_TIMEOUT = 30.0


def _save_fragment(storage_dir: str, fragment_id: str, data: bytes) -> str:
    os.makedirs(storage_dir, exist_ok=True)
    path = os.path.join(storage_dir, f"{fragment_id}.bin")
    with open(path, "wb") as f:
        f.write(data)
    return path


async def fetch_fragment(
    peer_ip: str,
    peer_port: int,
    fragment_id: str,
    node_id: str,
    storage_dir: str,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> bool:
    """Pede um fragmento a um peer via conexão TCP temporária.

    Retorna False se sem resposta, sem o fragmento, ou resposta inválida,
    o chamador deve tentar outro peer ou cair para o servidor.
    """
    msg = RequestFragment(id_node=node_id, fragment_id=fragment_id).to_dict()
    reply = await send_once(peer_ip, peer_port, msg, expect_reply=True, timeout=timeout)

    if reply is None:
        log.warning(f"download_worker: sem resposta de {peer_ip}:{peer_port}")
        return False

    if reply.get("type") == MSG_FRAGMENT_NOT_FOUND:
        log.debug(f"download_worker: {peer_ip}:{peer_port} não possui '{fragment_id}'")
        return False

    if reply.get("type") != MSG_FRAGMENT_DATA:
        log.warning(f"download_worker: resposta inesperada de {peer_ip}:{peer_port}")
        return False

    data = reply.get("data")
    if not isinstance(data, (bytes, bytearray)):
        log.warning(f"download_worker: payload de '{fragment_id}' inválido")
        return False

    _save_fragment(storage_dir, fragment_id, bytes(data))
    log.info(f"download_worker: '{fragment_id}' salvo ({len(data)} bytes) via {peer_ip}:{peer_port}")
    return True


async def fetch_fragment_from_backup(
    server_ip: str,
    server_port: int,
    fragment_id: str,
    node_id: str,
    storage_dir: str,
    timeout: float = DOWNLOAD_TIMEOUT,
) -> bool:
    """Pede o fragmento pro servidor, quando nenhum peer tem"""
    msg = RequestFragmentBackup(id_node=node_id, fragment_id=fragment_id).to_dict()
    reply = await send_once(server_ip, server_port, msg, expect_reply=True, timeout=timeout)

    if reply is None or reply.get("type") != MSG_FRAGMENT_BACKUP:
        log.error(f"download_worker: servidor não forneceu backup de '{fragment_id}'")
        return False

    data = reply.get("data")
    if not isinstance(data, (bytes, bytearray)):
        log.error(f"download_worker: payload de backup de '{fragment_id}' inválido")
        return False

    _save_fragment(storage_dir, fragment_id, bytes(data))
    log.info(f"download_worker: '{fragment_id}' recuperado via backup ({len(data)} bytes)")
    return True