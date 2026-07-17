import asyncio
import contextlib
import pickle
from pathlib import Path
from typing import Any, Optional, Type

from core.network import P2PNode, send_once
from hyperparalelizer.ml.dataset_loader import DatasetLoader
from hyperparalelizer.peer.data_thread import DataThread
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from hyperparalelizer.peer.peer_outer_protocol import register_peer_peer_handlers
from hyperparalelizer.peer.trainer import TrainerNode
from hyperparalelizer.sync.bully import BullyElection
from hyperparalelizer.sync.maekawa import MaekawaMutex, NoOpMutex
from utils.protocol import Ack, KeepAlive, MSG_BULLY_COORDINATOR, MSG_PUBSUB_NOTIFY, MSG_SYNC_STATE


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
        dataset_loader_cls: Optional[Type[DatasetLoader]] = None,
        maekawa_mutex_cls: Optional[Type[Any]] = None,
        bully_cls: Optional[Type[Any]] = None,
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
        self.dataset_loader_cls = dataset_loader_cls or DatasetLoader
        self.maekawa_mutex_cls = maekawa_mutex_cls
        self.bully_cls = bully_cls

        self.data_thread = None
        self.messenger = None
        self.trainer = None
        self.p2p_node = None
        self.maekawa_mutex = None
        self.bully = None
        self.p2p_node_task = None
        self.heartbeat_task = None
        self._background_tasks: list[asyncio.Task] = []

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

        resolved_node_id = str(reply.get("node_id") or self.node_id or "peer")
        self.node_id = resolved_node_id

        if hasattr(self.data_thread, "node_id"):
            self.data_thread.node_id = self.node_id

        self.messenger = self.messenger_cls(
            node_id=self.node_id,
            server_ip=self.server_ip,
            server_port=self.server_port,
        )

        self.p2p_node = P2PNode(host=self.host, port=self.listen_port, node_id=self.node_id)
        self.messenger.register_handlers(self.p2p_node)
        register_peer_peer_handlers(self.p2p_node, storage_dir=self.storage_dir)

        if self.trainer_cls is not None and self.trainer_cls is not object:
            dataset_loader = self.dataset_loader_cls(self.storage_dir)
            self.maekawa_mutex = self._build_maekawa_mutex()
            self.trainer = self.trainer_cls(
                node_id=self.node_id,
                messenger=self.messenger,
                data_thread=self.data_thread,
                dataset_loader=dataset_loader,
                maekawa_mutex=self.maekawa_mutex,
            )
            self.messenger.attach_trainer(self.trainer)
        else:
            self.trainer = None
            if hasattr(self.messenger, "attach_trainer"):
                self.messenger.attach_trainer(self.trainer)

        self._register_runtime_handlers()

        self.messenger.start()

        self.p2p_node_task = asyncio.create_task(self.p2p_node.start())
        self._background_tasks.append(self.p2p_node_task)
        self.heartbeat_task = asyncio.create_task(self._server_heartbeat_loop())
        self._background_tasks.append(self.heartbeat_task)

        initial_task = getattr(self.data_thread, "initial_task", None)
        if initial_task is not None and self.trainer is not None:
            asyncio.create_task(self.trainer.handle_training_task(initial_task))

        await asyncio.sleep(0.1)

    def _build_maekawa_mutex(self) -> Any:
        if self.maekawa_mutex_cls is None:
            return NoOpMutex()

        try:
            return self.maekawa_mutex_cls(node_id=self.node_id, quorum=[])
        except TypeError:
            return self.maekawa_mutex_cls()

    def _register_runtime_handlers(self) -> None:
        if self.p2p_node is None:
            return

        self.bully = self._build_bully()
        if self.bully is not None and hasattr(self.bully, "register_handlers"):
            self.bully.register_handlers(self.p2p_node)

        if self.maekawa_mutex is not None and hasattr(self.maekawa_mutex, "register_handlers"):
            self.maekawa_mutex.register_handlers(self.p2p_node)

        async def _handle_sync_state(msg, writer):
            if self.trainer is not None and hasattr(self.trainer, "handle_sync_state"):
                self.trainer.handle_sync_state(msg)

        async def _handle_pubsub_notify(msg, writer):
            return None

        self.p2p_node.register_handler(MSG_SYNC_STATE, _handle_sync_state)
        self.p2p_node.register_handler(MSG_PUBSUB_NOTIFY, _handle_pubsub_notify)

    def _build_bully(self) -> Optional[Any]:
        if self.bully_cls is None:
            return None

        try:
            return self.bully_cls(
                my_id=self.node_id,
                globaltable_peers={},
                promote_callback=lambda: None,
            )
        except TypeError:
            return self.bully_cls()

    async def _server_heartbeat_loop(self) -> None:
        while True:
            try:
                await send_once(
                    self.server_ip,
                    self.server_port,
                    KeepAlive(id_node=self.node_id).to_dict(),
                    expect_reply=True,
                    timeout=2.0,
                )
            except Exception:
                pass
            await asyncio.sleep(5.0)

    async def run(self) -> None:
        await self.bootstrap()
        await asyncio.Event().wait()

    async def stop(self) -> None:
        for task in list(self._background_tasks):
            if task is not None and not task.done():
                task.cancel()

        await asyncio.gather(*[task for task in self._background_tasks if task is not None], return_exceptions=True)
        self._background_tasks.clear()

        if self.p2p_node_task is not None and not self.p2p_node_task.done():
            self.p2p_node_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.p2p_node_task

        if self.p2p_node is not None:
            with contextlib.suppress(Exception):
                await self.p2p_node.stop()

        if self.messenger is not None:
            self.messenger.stop()