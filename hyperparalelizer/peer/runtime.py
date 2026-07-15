import asyncio
import pickle
from pathlib import Path
from typing import Any, Optional, Type

from core.network import P2PNode
from hyperparalelizer.peer.data_thread import DataThread
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from hyperparalelizer.peer.trainer import TrainerNode


class PeerRuntime:
    """inicialização e ciclo de vida do peer"""

    def __init__(
        self,
        node_id: str,
        host: str,
        listen_port: int,
        server_ip: str,
        server_port: int,
        data_thread_cls: Optional[Type[DataThread]] = None,
        messenger_cls: Optional[Type[PeerMessenger]] = None,
        trainer_cls: Optional[Type[TrainerNode]] = None,
        storage_dir: str = "data/fragments",
    ):
        self.node_id = node_id
        self.host = host
        self.listen_port = listen_port
        self.server_ip = server_ip
        self.server_port = server_port
        self.storage_dir = storage_dir

        self.data_thread_cls = data_thread_cls or DataThread
        self.messenger_cls = messenger_cls or PeerMessenger
        self.trainer_cls = trainer_cls or TrainerNode

        self.data_thread = None
        self.messenger = None
        self.trainer = None
        self.p2p_node = None

    async def bootstrap(self) -> None:
        self.data_thread = self.data_thread_cls(
            ip=self.host,
            listen_port=self.listen_port,
            server_ip=self.server_ip,
            server_port=self.server_port,
            storage_dir=self.storage_dir,
        )

        reply = await self.data_thread.join_network()
        if reply is None:
            raise RuntimeError("PeerRuntime: join_network falhou")

        self.messenger = self.messenger_cls(
            node_id=self.node_id or str(reply.get("node_id", "peer")),
            server_ip=self.server_ip,
            server_port=self.server_port,
        )

        self.p2p_node = P2PNode(host=self.host, port=self.listen_port, node_id=self.node_id)
        self.messenger.register_handlers(self.p2p_node)

        if self.trainer_cls is not None and self.trainer_cls is not object:
            self.trainer = self.trainer_cls(
                node_id=self.node_id,
                messenger=self.messenger,
                data_thread=self.data_thread,
                dataset_loader=None,
                maekawa_mutex=None,
            )
            self.messenger.attach_trainer(self.trainer)
        else:
            self.trainer = None
            if hasattr(self.messenger, "attach_trainer"):
                self.messenger.attach_trainer(self.trainer)

        self.messenger.start()

        self.p2p_node_task = asyncio.create_task(self.p2p_node.start())
        await asyncio.sleep(0.1)
