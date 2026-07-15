import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.network import P2PNode
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from hyperparalelizer.server.server_messenger import ServerMessenger
from hyperparalelizer.server.server_peer_protocol import register_all_handlers
from hyperparalelizer.server.coordinator import Coordinator, Peer
from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.sync.bully import BullyElection
from hyperparalelizer.sync.maekawa import MaekawaMutex
from utils.protocol import MSG_BULLY_ELECTION, MSG_BULLY_COORDINATOR, MSG_MAEKAWA_REQUEST, MSG_MAEKAWA_GRANT, MSG_MAEKAWA_RELEASE


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
