from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


def short_id(value: str, size: int = 8) -> str:
    return value[:size] + ("…" if len(value) > size else "")


@dataclass
class PeerHealth:
    last_seen: float
    misses: int = 0
    declared_dead: bool = False


class ServerHeartbeatTracker:
    """Mantém o último contato observado de cada peer.

    O protocolo de KeepAlive responde ao peer, mas não persiste last_seen no
    Coordinator/GlobalTable. Este tracker vive no lado do servidor, ao lado
    do Coordinator, e é consultado pelo monitor de falhas para decidir quando
    um peer deve ser considerado morto (handle_peer_failure).
    """

    def __init__(self) -> None:
        self._peers: Dict[str, PeerHealth] = {}

    def touch(self, node_id: str) -> None:
        if not node_id:
            return
        current = self._peers.get(node_id)
        restored = current is not None and current.misses > 0
        self._peers[node_id] = PeerHealth(last_seen=time.monotonic())
        if restored:
            print(f"[HEARTBEAT] Peer {short_id(node_id)} voltou a responder")

    def remove(self, node_id: str) -> None:
        self._peers.pop(node_id, None)

    def items(self) -> List[Tuple[str, PeerHealth]]:
        return list(self._peers.items())