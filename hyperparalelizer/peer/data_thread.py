"""Abre conexão com o servidor, obtém o fragmento atribuído e a lista de
peers, e garante que o dataset esteja montado localmente antes do treino"""

import os
from typing import Any, Dict, List, Optional

from utils.logger import get_logger
from utils.protocol import JoinNetwork, DatasetReady, MSG_JOIN_ACK
from core.network import send_once
from hyperparalelizer.peer.download_worker import fetch_fragment, fetch_fragment_from_backup
from hyperparalelizer.peer.peer_inner_protocol import (
    InternalEventBus,
    FragmentAcquired,
    FragmentAssemblyFailed,
)

log = get_logger("data_thread")

DEFAULT_STORAGE_DIR = "data/fragments"


class DataThread:
    """Monta os fragmentos de dataset de um peer"""

    def __init__(
        self,
        ip: str,
        listen_port: int,
        server_ip: str,
        server_port: int,
        storage_dir: str = DEFAULT_STORAGE_DIR,
        event_bus: Optional[InternalEventBus] = None,
    ):
        self.ip = ip
        self.listen_port = listen_port
        self.server_ip = server_ip
        self.server_port = server_port
        self.storage_dir = storage_dir
        self.event_bus = event_bus  # opcional (peer_inner_protocol.py)

        # preenche depois do join_network()
        self.fragment_id: Optional[str] = None
        self.known_peers: List[Dict[str, Any]] = []
        self.initial_task: Optional[Dict[str, Any]] = None

        os.makedirs(self.storage_dir, exist_ok=True)

    # Entrada na rede

    async def join_network(
        self,
        memoria_total_mb: float = 0.0,
        memoria_disponivel_mb: float = 0.0,
        timeout: float = 60.0,
    ) -> Optional[Dict[str, Any]]:
        """Envia JoinNetwork e recebe fragmento + peers conhecidos"""
        msg = JoinNetwork(
            ip=self.ip,
            porta=self.listen_port,
            memoria_total_mb=memoria_total_mb,
            memoria_disponivel_mb=memoria_disponivel_mb,
        ).to_dict()

        reply = await send_once(
            self.server_ip, self.server_port, msg, expect_reply=True, timeout=timeout
        )

#        if reply is None or reply.get("type") != MSG_JOIN_ACK:
#            log.error("DataThread: falha ao entrar na rede")
#            return None
#        self.fragment_id = reply.get("fragment_id")
#        self.known_peers = reply.get("peers", []) or []
#        self.initial_task = reply.get("task")

        if reply is None:
            log.error("DataThread: servidor não respondeu ao JoinNetwork")
            return None

        if reply.get("type") != MSG_JOIN_ACK:
            log.error("DataThread: resposta inesperada no join: "f"{reply.get('type')!r}")
            return None

        node_id = reply.get("node_id")

        if not isinstance(node_id, str) or not node_id:
            log.error("DataThread: JoinAck sem node_id válido")
            return None

        self.node_id = node_id
        self.fragment_id = reply.get("fragment_id")
        self.known_peers = reply.get("peers") or []
        self.initial_task = reply.get("task")

        log.info(
            f"DataThread [{self.node_id[:8]}...]: "
            f"entrada confirmada, "
            f"fragmento={self.fragment_id}, "
            f"peers={len(self.known_peers)}, "
            f"task={self.initial_task.get('task_id') if self.initial_task else None}"
        )

        return reply


    def update_known_peers(self, peers: List[Dict[str, Any]]) -> None:
        """Atualiza a lista de peers conhecidos"""
        self.known_peers = peers or []

    # Montagem do dataset

    def fragment_path(self, fragment_id: Optional[str] = None) -> str:
        fid = fragment_id or self.fragment_id
        return os.path.join(self.storage_dir, f"{fid}.bin")

    def has_fragment_locally(self, fragment_id: Optional[str] = None) -> bool:
        return os.path.exists(self.fragment_path(fragment_id))

    async def assemble_dataset(self) -> Optional[str]:
        """Monta o fragmento desse nó"""
        if self.fragment_id is None:
            log.error("DataThread: nenhum fragmento, chame join_network()")
            return None

        return await self.assemble_dataset_for(self.fragment_id)

    async def assemble_dataset_for(self, fragment_id: str) -> Optional[str]:
        """Garante o fragmento: disco -> peers -> servidor"""
        if self.has_fragment_locally(fragment_id):
            log.debug(f"DataThread: '{fragment_id}' já local")
            self._emit_fragment_acquired(fragment_id, source="local")
            return self.fragment_path(fragment_id)

        for peer in self.known_peers:
            if peer.get("id_node") == self.node_id:
                continue

            ip = peer.get("ip")
            port = peer.get("port")
            if not ip or not port:
                continue

            ok = await fetch_fragment(
                ip, port, fragment_id, self.node_id, self.storage_dir
            )
            if ok:
                log.info(f"DataThread: '{fragment_id}' obtido via peer {ip}:{port}")
                self._emit_fragment_acquired(fragment_id, source="peer")
                return self.fragment_path(fragment_id)

        log.warning(f"DataThread: nenhum peer possui '{fragment_id}'")

        ok = await fetch_fragment_from_backup(
            self.server_ip, self.server_port, fragment_id, self.node_id, self.storage_dir
        )
        if ok:
            log.info(f"DataThread: '{fragment_id}' recuperado via backup do servidor")
            self._emit_fragment_acquired(fragment_id, source="server_backup")
            return self.fragment_path(fragment_id)

        log.error(f"DataThread: não foi possível obter '{fragment_id}'")
        self._emit_fragment_failed(fragment_id, reason="no_source_available")
        return None

    async def assemble_many(self, fragment_ids: List[str]) -> bool:
        """Monta uma lista de fragmentos; False no primeiro que falhar"""
        for fragment_id in fragment_ids:
            if await self.assemble_dataset_for(fragment_id) is None:
                return False
        return True

    # Eventos internos

    def _emit_fragment_acquired(self, fragment_id: str, source: str) -> None:
        if self.event_bus is None:
            return
        self.event_bus.publish(
            FragmentAcquired(
                fragment_id=fragment_id, node_id=self.node_id, source=source
            )
        )

    def _emit_fragment_failed(self, fragment_id: str, reason: str) -> None:
        if self.event_bus is None:
            return
        self.event_bus.publish(
            FragmentAssemblyFailed(
                fragment_id=fragment_id, node_id=self.node_id, reason=reason
            )
        )

    # Notificação ao servidor

    async def notify_dataset_ready(self, fragment_id: Optional[str] = None) -> bool:
        """Avisa o servidor que o dataset já está montado, para entrar em trenio"""
        fid = fragment_id or self.fragment_id
        if fid is None:
            log.warning("DataThread: fragment_id is None; cannot notify server.")
            return False

        msg = DatasetReady(id_node=self.node_id, fragment_id=fid).to_dict()

        reply = await send_once(
            self.server_ip, self.server_port, msg, expect_reply=True, timeout=10.0
        )

        ok = reply is not None and reply.get("type") == "Ack"
        if ok:
            log.info(f"DataThread: DatasetReady confirmado para '{fid}'")
        else:
            log.warning(f"DataThread: falha ao notificar DatasetReady para '{fid}'")
        return ok