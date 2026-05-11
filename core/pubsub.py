"""
pubsub.py - Simple Publish/Subscribe system.

Main topic:
    global_best_score

Nodes can:
    - subscribe to topics
    - publish messages
    - receive notifications from the broker

Maekawa integration:
    Mutual exclusion must happen BEFORE publishing.
    PubSub only distributes the update.

Lamport integration:
    Publish messages carry a lamport_clock value.
"""

import asyncio
from typing import Any, Awaitable, Callable, Dict, List, Set

from utils.logger import get_logger
from utils.protocol import (
    MSG_PUBSUB_NOTIFY,
    MSG_PUBSUB_PUBLISH,
    MSG_PUBSUB_SUBSCRIBE,
    Ack,
    PubSubNotify,
    PubSubPublish,
    PubSubSubscribe,
)
from core.network import send_message, send_once

log = get_logger("pubsub")

# Local callback type
NotifyCallback = Callable[[str, Dict[str, Any], int], Awaitable[None]]


# ── Broker ────────────────────────────────────────────────────────────────

class PubSubBroker:
    """Central Pub/Sub broker."""

    def __init__(self, node_id: str):
        self.node_id = node_id

        # topic -> set(id_node, ip, port)
        self._subscriptions: Dict[str, Set[tuple]] = {}

    async def handle_subscribe(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handles subscriptions."""
        topic = msg.get("topic", "")
        id_node = msg.get("id_node", "")

        peer = writer.get_extra_info("peername")
        ip = peer[0] if peer else "unknown"
        porta = peer[1] if peer else 0

        if topic not in self._subscriptions:
            self._subscriptions[topic] = set()

        self._subscriptions[topic].add((id_node, ip, porta))

        log.info(f"Broker: {id_node} subscribed to '{topic}'")

        ack = Ack(
            ref_type=MSG_PUBSUB_SUBSCRIBE,
            ref_id=id_node,
        ).to_dict()

        await send_message(writer, ack)

    async def handle_publish(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handles publish events."""
        topic = msg.get("topic", "")
        payload = msg.get("payload", {})
        lamport = msg.get("lamport_clock", 0)
        id_node = msg.get("id_node", "")

        log.info(
            f"Broker: publish on '{topic}' by {id_node} (L={lamport})"
        )

        # Confirm reception first
        ack = Ack(
            ref_type=MSG_PUBSUB_PUBLISH,
            ref_id=id_node,
        ).to_dict()

        await send_message(writer, ack)

        subscribers = self._subscriptions.get(topic, set())

        if not subscribers:
            log.debug(f"Broker: no subscribers for '{topic}'")
            return

        notify = PubSubNotify(
            topic=topic,
            payload=payload,
            lamport_clock=lamport,
        ).to_dict()

        tasks = [
            send_once(
                ip,
                porta,
                notify,
                expect_reply=False,
                timeout=5.0,
            )
            for (nid, ip, porta) in subscribers
            if nid != id_node
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        # Remove dead subscribers
        dead = []

        sub_list = [
            s for s in subscribers
            if s[0] != id_node
        ]

        for sub, result in zip(sub_list, results):
            if isinstance(result, Exception):
                log.warning(
                    f"Broker: failed notifying {sub[0]}, removing"
                )
                dead.append(sub)

        for sub in dead:
            self._subscriptions[topic].discard(sub)

    def get_subscribers(self, topic: str) -> List[tuple]:
        """Returns subscribers for a topic."""
        return list(self._subscriptions.get(topic, set()))


# ── Client ────────────────────────────────────────────────────────────────

class PubSubClient:
    """Pub/Sub client."""

    def __init__(
        self,
        node_id: str,
        broker_ip: str,
        broker_port: int,
    ):
        self.node_id = node_id
        self.broker_ip = broker_ip
        self.broker_port = broker_port

        # topic -> callbacks
        self._callbacks: Dict[str, List[NotifyCallback]] = {}

    async def subscribe(
        self,
        topic: str,
        callback: NotifyCallback,
    ) -> bool:
        """Subscribes to a topic."""
        if topic not in self._callbacks:
            self._callbacks[topic] = []

        self._callbacks[topic].append(callback)

        msg = PubSubSubscribe(
            id_node=self.node_id,
            topic=topic,
        ).to_dict()

        reply = await send_once(
            self.broker_ip,
            self.broker_port,
            msg,
            expect_reply=True,
            timeout=5.0,
        )

        ok = (
            reply is not None
            and reply.get("type") == "Ack"
        )

        if ok:
            log.info(f"[{self.node_id}] subscribed to '{topic}'")
        else:
            log.warning(f"[{self.node_id}] subscribe failed")

        return ok

    async def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        lamport: int = 0,
    ) -> bool:
        """Publishes a message."""
        msg = PubSubPublish(
            id_node=self.node_id,
            topic=topic,
            payload=payload,
            lamport_clock=lamport,
        ).to_dict()

        reply = await send_once(
            self.broker_ip,
            self.broker_port,
            msg,
            expect_reply=True,
            timeout=5.0,
        )

        ok = (
            reply is not None
            and reply.get("type") == "Ack"
        )

        if ok:
            log.info(
                f"[{self.node_id}] published to '{topic}' (L={lamport})"
            )
        else:
            log.warning(
                f"[{self.node_id}] publish failed"
            )

        return ok

    async def handle_notify(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handles incoming notifications."""
        topic = msg.get("topic", "")
        payload = msg.get("payload", {})
        lamport = msg.get("lamport_clock", 0)

        log.debug(
            f"[{self.node_id}] notify '{topic}' "
            f"(L={lamport}): {payload}"
        )

        callbacks = self._callbacks.get(topic, [])

        for cb in callbacks:
            try:
                await cb(topic, payload, lamport)

            except Exception as exc:
                log.error(
                    f"Callback error for '{topic}': {exc}"
                )


# ── Default topics ────────────────────────────────────────────────────────

TOPIC_GLOBAL_BEST_SCORE = "global_best_score"

"""
Expected payload:

{
    "task_id": str,
    "id_node": str,
    "f1_score": float,
    "accuracy": float,
    "precision": float,
    "recall": float,
    "roc_auc": float,
}
"""