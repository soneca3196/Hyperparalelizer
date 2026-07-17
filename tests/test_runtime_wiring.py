import asyncio
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.network import P2PNode
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from hyperparalelizer.peer.runtime import PeerRuntime
from hyperparalelizer.server.server_messenger import ServerMessenger
from hyperparalelizer.server.server_peer_protocol import register_all_handlers
from hyperparalelizer.server.coordinator import Coordinator, Peer
from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.sync.bully import BullyElection
from hyperparalelizer.sync.maekawa import MaekawaMutex
from utils.protocol import MSG_BULLY_ELECTION, MSG_BULLY_COORDINATOR, MSG_MAEKAWA_REQUEST, MSG_MAEKAWA_GRANT, MSG_MAEKAWA_RELEASE, TrainingTask


def test_bully_registration_and_maekawa_registration():
    bully = BullyElection(my_id="node-2", globaltable_peers={}, promote_callback=lambda: None)
    mutex = MaekawaMutex(node_id="node-2", quorum=[])

    p2p_node = P2PNode(host="127.0.0.1", port=0, node_id="node-2")
    bully.register_handlers(p2p_node)
    mutex.register_handlers(p2p_node)

    assert MSG_BULLY_ELECTION in p2p_node._handlers
    assert MSG_BULLY_COORDINATOR in p2p_node._handlers
    assert MSG_MAEKAWA_REQUEST in p2p_node._handlers
    assert MSG_MAEKAWA_GRANT in p2p_node._handlers
    assert MSG_MAEKAWA_RELEASE in p2p_node._handlers


def test_server_message_handlers_can_be_registered():
    coordinator = Coordinator(dataset=([0, 1], [0, 1]), model=None, global_table=GlobalTable())
    messenger = ServerMessenger(coordinator=coordinator, host="127.0.0.1", port=0)

    register_all_handlers(messenger, coordinator)

    assert messenger._handlers


def test_peer_messenger_can_start_without_running_loop():
    messenger = PeerMessenger(node_id="peer-test", server_ip="127.0.0.1", server_port=9000)

    try:
        messenger.start()
        assert messenger._outbound_thread is not None
    finally:
        messenger.stop()


def test_scheduler_loop_dispatches_pending_tasks_and_requeues_timeouts():
    async def run_test():
        table = GlobalTable()
        coordinator = Coordinator(dataset=([0, 1], [0, 1]), model=None, global_table=table)

        peer = Peer(ip="127.0.0.1", port=9002)
        node_id = table.add_node(peer.ip, peer.port, peer)
        peer.id_node = node_id
        table.set_node_ready(node_id, True)

        task = TrainingTask(
            task_id="task-3",
            id_node_origem="server",
            dataset_fragmentos=["fragment_0000"],
            parametros={"C": 0.2},
            model_type="generic",
        )
        table.task_pool = [task]

        async def fake_send_once(*args, **kwargs):
            return {"type": "Ack"}

        with patch("hyperparalelizer.server.coordinator.send_once", side_effect=fake_send_once):
            await coordinator.run_scheduler_loop(interval=0.0, max_iterations=1)

        with table.lock:
            assert task.task_id in table.assigned_tasks
            assert task not in table.task_pool

        with table.lock:
            table.assigned_tasks[task.task_id]["timestamp"] = 0.0

        coordinator.check_task_status()

        with table.lock:
            assert task in table.task_pool
            assert task.task_id not in table.assigned_tasks

    asyncio.run(run_test())


def test_global_table_persistence_round_trip(tmp_path):
    table = GlobalTable()
    table.add_node("127.0.0.1", 9003, Peer(ip="127.0.0.1", port=9003))
    table.fragments_payloads["fragment_0000"] = b"payload"
    table.task_pool.append(TrainingTask(task_id="task-4", id_node_origem="server", dataset_fragmentos=["fragment_0000"], parametros={"C": 1.0}, model_type="generic"))
    table.best_model = {"task_id": "task-4", "f1_score": 0.9}

    path = tmp_path / "state.pkl"
    table.persist_state(path)
    restored = GlobalTable.load_state(path)

    assert restored.nodes
    assert restored.fragments_payloads["fragment_0000"] == b"payload"
    assert restored.best_model["task_id"] == "task-4"


def test_peer_runtime_bootstrap_wires_components():
    class DummyThread:
        def __init__(self, *args, **kwargs):
            self.joined = False
            self.node_id = None

        async def join_network(self, *args, **kwargs):
            self.joined = True
            return {"node_id": "peer-boot"}

    class DummyMessenger:
        def __init__(self, *args, **kwargs):
            self.started = False
            self.attached = False

        def attach_trainer(self, trainer):
            self.attached = True

        def register_handlers(self, p2p_node):
            self.registered = p2p_node

        def start(self):
            self.started = True

        def stop(self):
            self.started = False

    runtime = PeerRuntime(
        node_id="peer-boot",
        host="127.0.0.1",
        listen_port=0,
        server_ip="127.0.0.1",
        server_port=9004,
        data_thread_cls=DummyThread,
        messenger_cls=DummyMessenger,
        trainer_cls=object,
    )

    async def run_bootstrap():
        await runtime.bootstrap()

    asyncio.run(run_bootstrap())

    assert runtime.data_thread.joined is True
    assert runtime.messenger.attached is True
    assert runtime.messenger.started is True


def test_peer_runtime_propagates_node_id_and_initial_dependencies():
    class DummyThread:
        def __init__(self, *args, **kwargs):
            self.node_id = None
            self.initial_task = None

        async def join_network(self, *args, **kwargs):
            return {"node_id": "peer-boot"}

    class DummyMessenger:
        def __init__(self, *args, **kwargs):
            self.attached = None
            self.registered = None

        def attach_trainer(self, trainer):
            self.attached = trainer

        def register_handlers(self, p2p_node):
            self.registered = p2p_node

        def start(self):
            return None

        def stop(self):
            return None

    class DummyTrainer:
        def __init__(self, node_id, messenger, data_thread, dataset_loader, maekawa_mutex, **kwargs):
            self.node_id = node_id
            self.messenger = messenger
            self.data_thread = data_thread
            self.dataset_loader = dataset_loader
            self.maekawa_mutex = maekawa_mutex

    runtime = PeerRuntime(
        node_id="old-node-id",
        host="127.0.0.1",
        listen_port=0,
        server_ip="127.0.0.1",
        server_port=9005,
        data_thread_cls=DummyThread,
        messenger_cls=DummyMessenger,
        trainer_cls=DummyTrainer,
    )

    async def run_bootstrap():
        await runtime.bootstrap()

    asyncio.run(run_bootstrap())

    assert runtime.node_id == "peer-boot"
    assert runtime.data_thread.node_id == "peer-boot"
    assert runtime.trainer.node_id == "peer-boot"
    assert runtime.trainer.dataset_loader is not None
    assert runtime.trainer.maekawa_mutex is not None
    assert runtime.p2p_node.node_id == "peer-boot"


def test_peer_runtime_registers_maekawa_and_bully_handlers():
    class DummyThread:
        def __init__(self, *args, **kwargs):
            self.node_id = None
            self.initial_task = None

        async def join_network(self, *args, **kwargs):
            return {"node_id": "peer-handlers"}

    class DummyMessenger:
        def __init__(self, *args, **kwargs):
            self.registered = None

        def attach_trainer(self, trainer):
            return None

        def register_handlers(self, p2p_node):
            self.registered = p2p_node

        def start(self):
            return None

        def stop(self):
            return None

    class DummyTrainer:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    runtime = PeerRuntime(
        node_id="old-node-id",
        host="127.0.0.1",
        listen_port=0,
        server_ip="127.0.0.1",
        server_port=9006,
        data_thread_cls=DummyThread,
        messenger_cls=DummyMessenger,
        trainer_cls=DummyTrainer,
        maekawa_mutex_cls=MaekawaMutex,
        bully_cls=BullyElection,
    )

    async def run_bootstrap():
        await runtime.bootstrap()

    asyncio.run(run_bootstrap())

    assert runtime.maekawa_mutex is not None
    assert runtime.bully is not None
    assert MSG_MAEKAWA_REQUEST in runtime.p2p_node._handlers
    assert MSG_BULLY_ELECTION in runtime.p2p_node._handlers