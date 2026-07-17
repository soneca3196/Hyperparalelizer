import asyncio
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.network import send_once
from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.server.coordinator import Coordinator, Peer
from utils.protocol import TrainingTask


class DummyWriter:
    def write(self, data):
        pass

    async def drain(self):
        pass

    def close(self):
        pass

    async def wait_closed(self):
        pass


class DummyReader:
    async def readexactly(self, n):
        return b"{}"


def test_send_once_retries_then_succeeds():
    async def run_test():
        attempts = {"count": 0}

        async def fake_open_connection(*args, **kwargs):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise OSError("transient")
            return DummyReader(), DummyWriter()

        async def fake_send_message(writer, data):
            return None

        async def fake_recv_message(reader):
            return {"ok": True}

        with patch("core.network.asyncio.open_connection", side_effect=fake_open_connection):
            with patch("core.network.send_message", side_effect=fake_send_message):
                with patch("core.network.recv_message", side_effect=fake_recv_message):
                    response = await send_once(
                        "127.0.0.1",
                        9000,
                        {"type": "ping"},
                        expect_reply=True,
                        timeout=0.1,
                        max_retries=3,
                        retry_delay=0,
                    )

        assert response == {"ok": True}
        assert attempts["count"] == 3

    asyncio.run(run_test())


def test_receive_task_result_failed_requeues_task_and_clears_assignment():
    table = GlobalTable()
    coordinator = Coordinator(dataset=([0, 1], [0, 1]), model=None, global_table=table)

    peer = Peer(ip="127.0.0.1", port=9001)
    node_id = table.add_node(peer.ip, peer.port, peer)
    peer.id_node = node_id

    task = TrainingTask(
        task_id="task-2",
        id_node_origem="server",
        dataset_fragmentos=["fragment_0000"],
        parametros={"C": 1.0},
        model_type="generic",
    )

    with table.lock:
        table.assigned_tasks[task.task_id] = {
            "peer": peer,
            "timestamp": 0.0,
            "task": task,
        }

    result = coordinator.receive_task_result(
        task.task_id,
        {"status": "failed", "error": "boom"},
    )

    assert result == (peer, task)

    with table.lock:
        assert task in table.task_pool
        assert task.task_id not in table.assigned_tasks
        assert peer.hyperparameters == {}


def test_handle_peer_failure_requeues_task_and_removes_peer():
    table = GlobalTable()
    coordinator = Coordinator(dataset=([0, 1], [0, 1]), model=None, global_table=table)

    peer = Peer(ip="127.0.0.1", port=9000)
    node_id = table.add_node(peer.ip, peer.port, peer)
    peer.id_node = node_id

    table.add_fragment_location("fragment_0000", node_id)

    task = TrainingTask(
        task_id="task-1",
        id_node_origem="server",
        dataset_fragmentos=["fragment_0000"],
        parametros={"C": 1.0},
        model_type="generic",
    )

    with table.lock:
        table.assigned_tasks[task.task_id] = {
            "peer": peer,
            "timestamp": 0.0,
            "task": task,
        }

    coordinator.handle_peer_failure(node_id)

    with table.lock:
        assert task in table.task_pool
        assert node_id not in table.nodes
        assert node_id not in table.fragments_locations.get("fragment_0000", [])
