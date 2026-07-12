"""
pubsub.py - Sistema Publish/Subscribe do Hyperparalelizer.

Comunicação com o Middleware via duas filas thread-safe (queue.Queue):
    inbound_queue  (Queue): PubSubClient → Middleware
    outbound_queue (Queue): Middleware → PubSubClient
"""

import asyncio
import queue
import threading
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from utils.logger import get_logger
from utils.protocol import (
    MSG_PUBSUB_NOTIFY,
    MSG_PUBSUB_PUBLISH,
    MSG_PUBSUB_SUBSCRIBE,
    MSG_PUBSUB_UNSUBSCRIBE,
    Ack,
    PubSubNotify,
    PubSubPublish,
    PubSubSubscribe,
    PubSubUnsubscribe,
)
from core.network import send_message, send_once

log = get_logger("pubsub")

# Callback: callback(topic, payload, lamport_clock)
NotifyCallback = Callable[[str, Dict[str, Any], int], Awaitable[None]]


# Tópicos

TOPIC_GLOBAL_BEST_SCORE = "global_best_score"

"""
Payload esperado:
{
    "task_id":   str,
    "id_node":   str,
    "f1_score":  float,
    "accuracy":  float,
    "precision": float,
    "recall":    float,
    "roc_auc":   float,
}
"""


# Broker


class PubSubBroker:
    """Broker Pub/Sub."""

    def __init__(self, node_id: str):
        self.node_id = node_id

        # topic -> set(id_node, ip, listen_port)
        self._subscriptions: Dict[str, Set[tuple]] = {}

    # Handlers

    async def handle_subscribe(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Processa subscribe."""
        topic = msg.get("topic", "")
        id_node = msg.get("id_node", "")

        peer = writer.get_extra_info("peername")
        ip = peer[0] if peer else "unknown"

        listen_port = msg.get("listen_port")

        if not listen_port:
            log.warning(
                f"Broker: subscribe from {id_node} without listen_port"
            )
            return

        if topic not in self._subscriptions:
            self._subscriptions[topic] = set()

        self._subscriptions[topic].add((id_node, ip, listen_port))

        log.info(
            f"Broker: {id_node[:8]}… subscribed to '{topic}' "
            f"({ip}:{listen_port})"
        )

        ack = Ack(
            ref_type=MSG_PUBSUB_SUBSCRIBE,
            ref_id=id_node,
        ).to_dict()

        await send_message(writer, ack)

    async def handle_unsubscribe(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Processa unsubscribe."""
        topic = msg.get("topic", "")
        id_node = msg.get("id_node", "")

        if topic == "*":
            self.remove_subscriber(id_node)

            log.info(
                f"Broker: {id_node[:8]}… unsubscribed from all topics"
            )

        elif topic in self._subscriptions:
            self._subscriptions[topic] = {
                s for s in self._subscriptions[topic]
                if s[0] != id_node
            }

            log.info(
                f"Broker: {id_node[:8]}… unsubscribed from '{topic}'"
            )

        else:
            log.debug(
                f"Broker: unsubscribe from unknown topic "
                f"'{topic}' by {id_node[:8]}…"
            )

        ack = Ack(
            ref_type=MSG_PUBSUB_UNSUBSCRIBE,
            ref_id=id_node,
        ).to_dict()

        await send_message(writer, ack)

    async def handle_publish(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Processa publish."""
        topic = msg.get("topic", "")
        payload = msg.get("payload", {})
        lamport = msg.get("lamport_clock", 0)
        id_node = msg.get("id_node", "")

        log.info(
            f"Broker: publish on '{topic}' by "
            f"{id_node[:8]}… (L={lamport})"
        )

        ack = Ack(
            ref_type=MSG_PUBSUB_PUBLISH,
            ref_id=id_node,
        ).to_dict()

        await send_message(writer, ack)

        subscribers = self._subscriptions.get(topic, set())

        if not subscribers:
            log.debug(
                f"Broker: no subscribers for '{topic}'"
            )
            return

        notify = PubSubNotify(
            topic=topic,
            payload=payload,
            lamport_clock=lamport,
        ).to_dict()

        targets = [
            (nid, ip, port)
            for (nid, ip, port) in subscribers
            if nid != id_node
        ]

        tasks = [
            send_once(
                ip,
                port,
                notify,
                expect_reply=False,
                timeout=5.0,
            )
            for (nid, ip, port) in targets
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )

        dead: List[tuple] = []

        for sub, result in zip(targets, results):
            if isinstance(result, Exception):
                log.warning(
                    f"Broker: failed to notify "
                    f"{sub[0][:8]}…, removing"
                )

                dead.append(sub)

        for sub in dead:
            self._subscriptions[topic].discard(sub)

    # Utilitários

    def get_subscribers(self, topic: str) -> List[tuple]:
        """Retorna subscribers."""
        return list(self._subscriptions.get(topic, set()))

    def remove_subscriber(self, node_id: str) -> None:
        """Remove um nó de todos os tópicos."""
        for topic in self._subscriptions:
            self._subscriptions[topic] = {
                s for s in self._subscriptions[topic]
                if s[0] != node_id
            }

        log.info(
            f"Broker: {node_id[:8]}… removed from all topics"
        )


# Cliente


class PubSubClient:
    """Cliente Pub/Sub
    - inbound_queue  (Queue): notificações recebidas → Middleware
    - outbound_queue (Queue): pedidos de publish do Middleware → rede

    """

    def __init__(
        self,
        node_id: str,
        broker_ip: str,
        broker_port: int,
        listen_port: int,
        inbound_queue: Optional[queue.Queue] = None,
        outbound_queue: Optional[queue.Queue] = None,
    ):
        self.node_id = node_id
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.listen_port = listen_port

        # topic -> callbacks (usados quando não há inbound_queue)
        self._callbacks: Dict[str, List[NotifyCallback]] = {}

        # Filas compartilhadas com o Middleware
        self._inbound_queue: Optional[queue.Queue] = inbound_queue
        self._outbound_queue: Optional[queue.Queue] = outbound_queue

        self._outbound_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # Gestão das filas

    def attach_queues(
        self,
        inbound_queue: queue.Queue,
        outbound_queue: queue.Queue,
    ) -> None:
        """Associa as filas compartilhadas com o Middleware
        """
        self._inbound_queue = inbound_queue
        self._outbound_queue = outbound_queue
        log.debug("PubSubClient: filas do Middleware associadas")

    def start_outbound_listener(self, loop: asyncio.AbstractEventLoop) -> None:
        """Inicia a thread que consome a outbound_queue e envia publishes
        """
        outbound_queue = self._outbound_queue
        if outbound_queue is None:
            log.warning(
                "PubSubClient: start_outbound_listener chamado "
                "sem outbound_queue configurada"
            )
            return

        def _worker() -> None:
            log.info("PubSubClient: outbound listener thread iniciada")

            while not self._stop_event.is_set():
                try:
                    item = outbound_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                topic = item.get("topic", "")
                payload = item.get("payload", {})
                lamport = item.get("lamport", 0)

                if not topic:
                    log.warning(
                        "PubSubClient: item na outbound_queue "
                        "sem 'topic', ignorando"
                    )
                    outbound_queue.task_done()
                    continue

                future = asyncio.run_coroutine_threadsafe(
                    self.publish(topic, payload, lamport),
                    loop,
                )

                try:
                    ok = future.result(timeout=10.0)
                    if not ok:
                        log.warning(
                            f"PubSubClient: publish em '{topic}' "
                            f"retornou False"
                        )
                except Exception as exc:
                    log.error(
                        f"PubSubClient: erro ao publicar em "
                        f"'{topic}' — {exc}"
                    )
                finally:
                    outbound_queue.task_done()

            log.info("PubSubClient: outbound listener thread encerrada")

        self._stop_event.clear()
        self._outbound_thread = threading.Thread(
            target=_worker,
            name="pubsub-outbound",
            daemon=True,
        )
        self._outbound_thread.start()

    def stop_outbound_listener(self) -> None:
        """Sinaliza parada da thread outbound"""
        self._stop_event.set()
        if self._outbound_thread and self._outbound_thread.is_alive():
            self._outbound_thread.join(timeout=5.0)

    # API (asyncio)

    async def subscribe(
        self,
        topic: str,
        callback: Optional[NotifyCallback] = None,
    ) -> bool:
        """Inscreve o nó em um tópico.
        """
        if self._inbound_queue is None and callback is None:
            raise ValueError(
                "subscribe requer callback quando não há "
                "inbound_queue configurada"
            )

        if callback is not None:
            if topic not in self._callbacks:
                self._callbacks[topic] = []
            self._callbacks[topic].append(callback)

        msg = PubSubSubscribe(
            id_node=self.node_id,
            topic=topic,
            listen_port=self.listen_port,
        ).to_dict()

        reply = await send_once(
            self.broker_ip,
            self.broker_port,
            msg,
            expect_reply=True,
            timeout=5.0,
        )

        ok = reply is not None and reply.get("type") == "Ack"

        if ok:
            log.info(
                f"[{self.node_id[:8]}…] subscribed to '{topic}'"
            )
        else:
            log.warning(
                f"[{self.node_id[:8]}…] subscribe failed "
                f"for '{topic}'"
            )

        return ok

    async def unsubscribe(self, topic: str) -> bool:
        """Remove inscrição de um tópico."""
        self._callbacks.pop(topic, None)

        msg = PubSubUnsubscribe(
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

        ok = reply is not None and reply.get("type") == "Ack"

        if ok:
            log.info(
                f"[{self.node_id[:8]}…] unsubscribed from '{topic}'"
            )
        else:
            log.warning(
                f"[{self.node_id[:8]}…] unsubscribe failed "
                f"for '{topic}'"
            )

        return ok

    async def unsubscribe_all(self) -> bool:
        """Remove inscrição de todos os tópicos."""
        self._callbacks.clear()

        msg = PubSubUnsubscribe(
            id_node=self.node_id,
            topic="*",
        ).to_dict()

        reply = await send_once(
            self.broker_ip,
            self.broker_port,
            msg,
            expect_reply=True,
            timeout=5.0,
        )

        ok = reply is not None and reply.get("type") == "Ack"

        if ok:
            log.info(
                f"[{self.node_id[:8]}…] unsubscribed from all topics"
            )
        else:
            log.warning(
                f"[{self.node_id[:8]}…] unsubscribe_all failed"
            )

        return ok

    async def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        lamport: int = 0,
    ) -> bool:
        """Publica em um tópico."""
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

        ok = reply is not None and reply.get("type") == "Ack"

        if ok:
            log.info(
                f"[{self.node_id[:8]}…] published to "
                f"'{topic}' (L={lamport})"
            )
        else:
            log.warning(
                f"[{self.node_id[:8]}…] publish failed "
                f"for '{topic}'"
            )

        return ok

    # Handler

    async def handle_notify(
        self,
        msg: Dict[str, Any],
        writer: asyncio.StreamWriter,
    ) -> None:
        """Processa notificações recebidas do broker.

        1. Se houver inbound_queue, coloca o evento nela para o Middleware.
        2. Também dispara callbacks locais registrados (se houver).
        """
        topic = msg.get("topic", "")
        payload = msg.get("payload", {})
        lamport = msg.get("lamport_clock", 0)

        log.debug(
            f"[{self.node_id[:8]}…] notified on "
            f"'{topic}' (L={lamport}): {payload}"
        )

        # 1. Entrega para o Middleware via inbound_queue
        if self._inbound_queue is not None:
            evento = {
                "tipo":    topic,
                "peer_id": payload.get("id_node", ""),
                "dados":   payload,
                "lamport": lamport,
            }
            self._inbound_queue.put(evento)
            log.debug(
                f"[{self.node_id[:8]}…] evento '{topic}' "
                f"colocado na inbound_queue"
            )

        # 2. Callbacks locais (retrocompatível)
        for cb in self._callbacks.get(topic, []):
            try:
                await cb(topic, payload, lamport)
            except Exception as exc:
                log.error(
                    f"Callback error for '{topic}': {exc}"
                )
