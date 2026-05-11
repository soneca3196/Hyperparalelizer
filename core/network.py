"""
network.py - Async TCP communication layer.

Responsibilities:
    1. Temporary connections
    2. Persistent connections
    3. KeepAlive / heartbeat
    4. Async TCP server with handlers

Design:
    - Pure asyncio
    - Length-prefixed framing
    - Coroutine-based handlers
"""

import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple

from utils.logger import get_logger
from utils.protocol import MSG_KEEP_ALIVE, KeepAlive
from utils.serializer import (
    HEADER_SIZE,
    decode_body,
    decode_header,
    encode,
)

log = get_logger("network")

# Handler signature
HandlerFn = Callable[
    [Dict[str, Any], asyncio.StreamWriter],
    Awaitable[None],
]

# Default timings
KEEPALIVE_TIMEOUT_S = 30.0
KEEPALIVE_INTERVAL_S = 10.0


# ── Frame I/O ──────────────────────────────────────────────────────────────

async def send_message(
    writer: asyncio.StreamWriter,
    data: Dict[str, Any],
) -> None:
    """Encodes and sends a message."""
    frame = encode(data)

    writer.write(frame)
    await writer.drain()


async def recv_message(
    reader: asyncio.StreamReader,
) -> Optional[Dict[str, Any]]:
    """Reads a full frame."""
    raw_header = await reader.readexactly(HEADER_SIZE)

    if not raw_header:
        return None

    size = decode_header(raw_header)

    raw_body = await reader.readexactly(size)

    return decode_body(raw_body)


# ── Temporary connections ─────────────────────────────────────────────────

async def send_once(
    host: str,
    port: int,
    data: Dict[str, Any],
    expect_reply: bool = False,
    timeout: float = 5.0,
) -> Optional[Dict[str, Any]]:
    """
    Opens a temporary connection, sends data,
    optionally waits for a reply, then closes.
    """
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout,
        )

        await send_message(writer, data)

        reply = None

        if expect_reply:
            reply = await asyncio.wait_for(
                recv_message(reader),
                timeout=timeout,
            )

        writer.close()
        await writer.wait_closed()

        return reply

    except (
        asyncio.TimeoutError,
        ConnectionRefusedError,
        OSError,
    ) as exc:
        log.warning(
            f"send_once failed for {host}:{port} - {exc}"
        )
        return None


# ── Persistent connections ────────────────────────────────────────────────

class PersistentConnection:
    """Keeps a TCP connection open."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        self._lock = asyncio.Lock()

    @property
    def is_open(self) -> bool:
        return (
            self._writer is not None
            and not self._writer.is_closing()
        )

    async def connect(
        self,
        timeout: float = 10.0,
    ) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port),
            timeout=timeout,
        )

        log.info(
            f"Persistent connection opened "
            f"to {self.host}:{self.port}"
        )

    async def send(
        self,
        data: Dict[str, Any],
    ) -> None:
        async with self._lock:
            if not self.is_open:
                raise ConnectionError("Connection is closed")

            await send_message(self._writer, data)

    async def recv(
        self,
        timeout: float = 60.0,
    ) -> Optional[Dict[str, Any]]:
        return await asyncio.wait_for(
            recv_message(self._reader),
            timeout=timeout,
        )

    async def close(self) -> None:
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()

            log.info(
                f"Persistent connection closed "
                f"with {self.host}:{self.port}"
            )

            self._writer = None
            self._reader = None


# ── TCP server ────────────────────────────────────────────────────────────

class TCPServer:
    """Async TCP server."""

    def __init__(
        self,
        host: str,
        port: int,
        node_id: str,
    ):
        self.host = host
        self.port = port
        self.node_id = node_id

        self._handlers: Dict[str, HandlerFn] = {}
        self._server: Optional[asyncio.AbstractServer] = None

    def register_handler(
        self,
        msg_type: str,
        fn: HandlerFn,
    ) -> None:
        """Registers a message handler."""
        self._handlers[msg_type] = fn

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
        )

        log.info(
            f"[{self.node_id}] listening on "
            f"{self.host}:{self.port}"
        )

        async with self._server:
            await self._server.serve_forever()

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")

        log.debug(f"New connection from {peer}")

        try:
            while True:
                try:
                    msg = await recv_message(reader)

                except asyncio.IncompleteReadError:
                    break

                if msg is None:
                    break

                msg_type = msg.get("type", "")

                handler = self._handlers.get(msg_type)

                if handler:
                    asyncio.create_task(
                        handler(msg, writer)
                    )
                else:
                    log.warning(
                        f"No handler for '{msg_type}' "
                        f"from {peer}"
                    )

        except Exception as exc:
            log.error(
                f"Connection error with {peer}: {exc}"
            )

        finally:
            writer.close()

            log.debug(
                f"Connection closed with {peer}"
            )


# ── KeepAlive ─────────────────────────────────────────────────────────────

class KeepAliveManager:
    """Heartbeat manager."""

    def __init__(
        self,
        node_id: str,
        timeout_s: float = KEEPALIVE_TIMEOUT_S,
        interval_s: float = KEEPALIVE_INTERVAL_S,
        on_node_down: Optional[
            Callable[[str], None]
        ] = None,
    ):
        self.node_id = node_id

        self.timeout_s = timeout_s
        self.interval_s = interval_s

        self.on_node_down = (
            on_node_down
            or (lambda nid: None)
        )

        # id_node -> (ip, port)
        self._peers: Dict[str, Tuple[str, int]] = {}

        # id_node -> last heartbeat time
        self._last_seen: Dict[str, float] = {}

    def add_peer(
        self,
        id_node: str,
        ip: str,
        port: int,
    ) -> None:
        self._peers[id_node] = (ip, port)

        self._last_seen[id_node] = time.monotonic()

        log.debug(
            f"KeepAlive: added peer "
            f"{id_node} ({ip}:{port})"
        )

    def remove_peer(
        self,
        id_node: str,
    ) -> None:
        self._peers.pop(id_node, None)
        self._last_seen.pop(id_node, None)

    def record_heartbeat(
        self,
        id_node: str,
    ) -> None:
        """Updates last seen timestamp."""
        self._last_seen[id_node] = time.monotonic()

        log.debug(
            f"KeepAlive: heartbeat from {id_node}"
        )

    async def run(self) -> None:
        """Main heartbeat loop."""
        log.info(
            f"[{self.node_id}] "
            f"KeepAliveManager started"
        )

        while True:
            await asyncio.sleep(self.interval_s)

            await self._send_heartbeats()

            self._check_timeouts()

    async def _send_heartbeats(self) -> None:
        msg = KeepAlive(
            id_node=self.node_id
        ).to_dict()

        tasks = [
            send_once(
                ip,
                port,
                msg,
                expect_reply=False,
                timeout=3.0,
            )
            for _, (ip, port) in self._peers.items()
        ]

        if tasks:
            await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

    def _check_timeouts(self) -> None:
        now = time.monotonic()

        dead_nodes = [
            nid
            for nid, last in self._last_seen.items()
            if (now - last) > self.timeout_s
        ]

        for nid in dead_nodes:
            log.warning(
                f"[{self.node_id}] "
                f"Node {nid} timed out"
            )

            self.remove_peer(nid)

            self.on_node_down(nid)