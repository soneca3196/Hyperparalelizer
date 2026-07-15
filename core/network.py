"""
network.py - Camada TCP assíncrona do Hyperparalelizer.
"""

import asyncio
import time
from typing import Any, Callable, Coroutine, Dict, Optional

from utils.logger import get_logger
from utils.serializer import HEADER_SIZE, decode_body, decode_header, encode

log = get_logger("network")

# Timeout padrão para conexões temporárias
DEFAULT_TIMEOUT = 10.0

# Intervalo entre heartbeats
KEEPALIVE_INTERVAL = 5.0

# Máximo de heartbeats perdidos
KEEPALIVE_MISSED_LIMIT = 3


# I/O


async def send_message(writer: asyncio.StreamWriter, data: Dict[str, Any]) -> None:
    """Envia um frame serializado."""
    frame = encode(data)
    writer.write(frame)
    await writer.drain()


async def recv_message(reader: asyncio.StreamReader) -> Dict[str, Any]:
    """Recebe um frame serializado."""
    raw_header = await reader.readexactly(HEADER_SIZE)
    payload_size = decode_header(raw_header)
    raw_body = await reader.readexactly(payload_size)
    return decode_body(raw_body)


# Conexão temporária


async def send_once(
    ip: str,
    port: int,
    data: Dict[str, Any],
    *,
    expect_reply: bool = True,
    timeout: float = DEFAULT_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    """Abre conexão, envia mensagem e fecha."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ip, port),
            timeout=timeout,
        )

    except (OSError, asyncio.TimeoutError) as exc:
        log.warning(f"send_once: failed to connect to {ip}:{port} — {exc}")
        return None

    try:
        await send_message(writer, data)

        if not expect_reply:
            return None

        reply = await asyncio.wait_for(
            recv_message(reader),
            timeout=timeout,
        )

        return reply

    except asyncio.TimeoutError:
        log.warning(
            f"send_once: timeout waiting for reply from {ip}:{port}"
        )
        return None

    except Exception as exc:
        log.error(
            f"send_once: error communicating with {ip}:{port} — {exc}"
        )
        return None

    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# Conexão persistente


class PersistentConnection:
    """Conexão TCP reutilizável."""

    def __init__(self, ip: str, port: int, timeout: float = DEFAULT_TIMEOUT):
        self.ip = ip
        self.port = port
        self.timeout = timeout

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    @property
    def is_open(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def connect(self) -> None:
        """Abre a conexão."""
        if self.is_open:
            return

        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.ip, self.port),
            timeout=self.timeout,
        )

        log.info(
            f"PersistentConnection: connected to {self.ip}:{self.port}"
        )

    async def send(self, data: Dict[str, Any]) -> None:
        """Envia um frame."""
        if not self.is_open:
            raise RuntimeError(
                "Connection is not open. Call connect() first."
            )

        writer = self._writer
        if writer is None:
            raise RuntimeError("Connection is not open. Call connect() first.")

        await send_message(writer, data)

    async def recv(self) -> Dict[str, Any]:
        """Recebe um frame."""
        reader = self._reader
        if not self.is_open or reader is None:
            raise RuntimeError("Connection is not open.")

        return await asyncio.wait_for(
            recv_message(reader),
            timeout=self.timeout,
        )

    async def close(self) -> None:
        """Fecha a conexão."""
        if self._writer:
            try:
                self._writer.close()
                await self._writer.wait_closed()

            except Exception:
                pass

            finally:
                self._reader = None
                self._writer = None

        log.info(
            f"PersistentConnection: closed {self.ip}:{self.port}"
        )


# Callback: on_node_dead(node_id)
NodeDeadCallback = Callable[[str], Coroutine[Any, Any, None]]


# KeepAlive


class KeepAliveManager:
    """Gerencia heartbeats."""

    def __init__(
        self,
        node_id: str,
        on_dead: Optional[NodeDeadCallback] = None,
        interval: float = KEEPALIVE_INTERVAL,
        missed_limit: int = KEEPALIVE_MISSED_LIMIT,
    ):
        self.node_id = node_id
        self.on_dead = on_dead
        self.interval = interval
        self.missed_limit = missed_limit

        # peer_id -> estado
        self._peers: Dict[str, Dict[str, Any]] = {}

        self._lock = asyncio.Lock()
        self._running = False

    async def register(self, peer_id: str, ip: str, port: int) -> None:
        """Adiciona um peer."""
        async with self._lock:
            self._peers[peer_id] = {
                "ip": ip,
                "port": port,
                "missed": 0,
                "last_seen": time.monotonic(),
            }

        log.debug(
            f"KeepAlive: registered {peer_id[:8]}… ({ip}:{port})"
        )

    async def unregister(self, peer_id: str) -> None:
        """Remove um peer."""
        async with self._lock:
            self._peers.pop(peer_id, None)

    async def record_heartbeat(self, peer_id: str) -> None:
        """Atualiza heartbeat."""
        async with self._lock:
            if peer_id in self._peers:
                self._peers[peer_id]["missed"] = 0
                self._peers[peer_id]["last_seen"] = time.monotonic()

    async def run(self) -> None:
        """Loop principal."""
        self._running = True

        log.info(
            f"KeepAlive: started (interval={self.interval}s)"
        )

        while self._running:
            await asyncio.sleep(self.interval)
            await self._probe_all()

    async def stop(self) -> None:
        self._running = False

    async def _probe_one(self, peer_id: str, info: Dict[str, Any]) -> Optional[str]:
        """Retorna peer_id se o peer deve ser considerado morto, ou None se não"""
        from utils.protocol import KeepAlive

        msg = KeepAlive(
            id_node=self.node_id,
        ).to_dict()

        reply = await send_once(
            info["ip"],
            info["port"],
            msg,
            expect_reply=True,
            timeout=self.interval * 0.8,
        )

        async with self._lock:
            if peer_id not in self._peers:
                return None

            if reply is not None:
                self._peers[peer_id]["missed"] = 0
                self._peers[peer_id]["last_seen"] = time.monotonic()
                return None

            self._peers[peer_id]["missed"] += 1
            missed = self._peers[peer_id]["missed"]

            log.warning(
                f"KeepAlive: {peer_id[:8]}… did not respond "
                f"({missed}/{self.missed_limit})"
            )

            if missed >= self.missed_limit:
                return peer_id
            return None

    async def _probe_all(self) -> None:
        """Envia heartbeats
        """
        async with self._lock:
            peers_snapshot = dict(self._peers)

        results = await asyncio.gather(
            *(
                self._probe_one(peer_id, info)
                for peer_id, info in peers_snapshot.items()
            ),
            return_exceptions=True,
        )

        dead = []
        for peer_id, result in zip(peers_snapshot.keys(), results):
            if isinstance(result, Exception):
                log.error(f"KeepAlive: erro ao sondar {peer_id[:8]}… — {result}")
                continue
            if result is not None:
                dead.append(result)

        for peer_id in dead:
            async with self._lock:
                self._peers.pop(peer_id, None)

            log.error(
                f"KeepAlive: node {peer_id[:8]}… declared DEAD"
            )

            if self.on_dead:
                try:
                    await self.on_dead(peer_id)

                except Exception as exc:
                    log.error(
                        f"KeepAlive: on_dead callback failed — {exc}"
                    )


# Handler: handler(msg, writer)
MessageHandler = Callable[
    [Dict[str, Any], asyncio.StreamWriter],
    Coroutine[Any, Any, None],
]


# Servidor P2P


class P2PNode:
    """Servidor TCP assíncrono."""

    def __init__(self, host: str, port: int, node_id: str):
        self.host = host
        self.port = port
        self.node_id = node_id

        self._handlers: Dict[str, MessageHandler] = {}
        self._server: Optional[asyncio.AbstractServer] = None

        self.keepalive: Optional[KeepAliveManager] = None

        # Cliente pub/sub opcional
        self._pubsub_client: Optional[Any] = None

    def attach_pubsub_client(self, client: Any) -> None:
        """Associa um cliente pub/sub."""
        self._pubsub_client = client

    def register_handler(
        self,
        msg_type: str,
        handler: MessageHandler,
    ) -> None:
        """Registra um handler."""
        self._handlers[msg_type] = handler

        log.debug(
            f"P2PNode: registered handler for '{msg_type}'"
        )

    async def start(self) -> None:
        """Inicia o servidor."""
        self._server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
        )

        addr = self._server.sockets[0].getsockname()

        log.info(
            f"P2PNode [{self.node_id[:8]}…] listening on {addr}"
        )

        # Registra KeepAlive automaticamente
        if self.keepalive:
            self._register_keepalive_handler()
            asyncio.create_task(self.keepalive.run())

        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Encerra o servidor."""
        # Remove subscriptions antes do shutdown
        if self._pubsub_client is not None:
            try:
                await self._pubsub_client.unsubscribe_all()

            except Exception as exc:
                log.warning(
                    f"P2PNode: unsubscribe_all failed on shutdown — {exc}"
                )

        if self._server:
            self._server.close()
            await self._server.wait_closed()

        if self.keepalive:
            await self.keepalive.stop()

        log.info(
            f"P2PNode [{self.node_id[:8]}…] stopped"
        )

    # Interno

    def _register_keepalive_handler(self) -> None:
        """Registra handler de KeepAlive."""
        from utils.protocol import Ack, MSG_KEEP_ALIVE

        keepalive_ref = self.keepalive
        if keepalive_ref is None:
            raise RuntimeError("keepalive broke")

        async def _handle_keepalive(
            msg: Dict[str, Any],
            writer: asyncio.StreamWriter,
        ) -> None:
            sender_id = msg.get("id_node", "")

            if sender_id:
                await keepalive_ref.record_heartbeat(sender_id)

                log.debug(
                    f"KeepAlive: heartbeat recorded from "
                    f"{sender_id[:8]}…"
                )

            ack = Ack(
                ref_type=MSG_KEEP_ALIVE,
                ref_id=sender_id,
            ).to_dict()

            await send_message(writer, ack)

        if MSG_KEEP_ALIVE not in self._handlers:
            self._handlers[MSG_KEEP_ALIVE] = _handle_keepalive

            log.debug(
                "P2PNode: auto-registered KeepAlive handler"
            )

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Processa uma conexão."""
        peer = writer.get_extra_info("peername")

        try:
            while True:
                try:
                    msg = await asyncio.wait_for(
                        recv_message(reader),
                        timeout=DEFAULT_TIMEOUT,
                    )

                except asyncio.TimeoutError:
                    break

                except asyncio.IncompleteReadError:
                    break

                msg_type = msg.get("type", "")
                handler = self._handlers.get(msg_type)

                if handler is None:
                    log.warning(
                        f"P2PNode: unknown type '{msg_type}' "
                        f"from {peer}"
                    )

                    err = {
                        "type": "Error",
                        "code": "UNKNOWN_TYPE",
                        "detail": msg_type,
                    }

                    await send_message(writer, err)
                    continue

                try:
                    await handler(msg, writer)

                except Exception as exc:
                    log.error(
                        f"P2PNode: handler '{msg_type}' failed — {exc}"
                    )

                    err = {
                        "type": "Error",
                        "code": "HANDLER_ERROR",
                        "detail": str(exc),
                    }

                    try:
                        await send_message(writer, err)

                    except Exception:
                        pass

        finally:
            try:
                writer.close()
                await writer.wait_closed()

            except Exception:
                pass