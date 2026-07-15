"""Mensageiro interno do peer: roteia mensagens de rede para o dentro do peer
e prepara uma fila para envio ao servidor"""

import asyncio
import queue
import threading
from typing import Any, Dict, Optional

from utils.logger import get_logger
from utils.protocol import MSG_TRAINING_TASK, MSG_REQUEST_BEST, Ack
from core.network import P2PNode, send_once, send_message

log = get_logger("peer_messenger")


class PeerMessenger:
    """Ponte entre a rede e o peer"""

    def __init__(
        self,
        node_id: str,
        server_ip: str,
        server_port: int,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.node_id = node_id
        self.server_ip = server_ip
        self.server_port = server_port
        self.loop = loop
        self._ensure_loop()

        self.trainer = None  # setado via attach_trainer()

        self._outbound_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._outbound_thread: Optional[threading.Thread] = None
        self._loop_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # Ligação com outros componentes

    def attach_trainer(self, trainer) -> None:
        self.trainer = trainer

    def _ensure_loop(self) -> None:
        if self.loop is not None:
            return

        try:
            self.loop = asyncio.get_running_loop()
            return
        except RuntimeError:
            pass

        try:
            self.loop = asyncio.get_event_loop_policy().get_event_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def register_handlers(self, p2p_node: P2PNode) -> None:
        """Registra mensagens vindas do servidor"""
        p2p_node.register_handler(MSG_TRAINING_TASK, self._handle_training_task)
        p2p_node.register_handler(MSG_REQUEST_BEST, self._handle_request_best_model)
        log.debug("PeerMessenger: handlers de rede registrados")

    async def _handle_training_task(
        self, msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        # confirma recebimento sem esperar o treino terminar
        ack = Ack(ref_type=MSG_TRAINING_TASK, ref_id=msg.get("task_id", "")).to_dict()
        await send_message(writer, ack)

        if self.trainer is None:
            log.error("PeerMessenger: TrainingTask recebida sem trainer")
            return

        asyncio.create_task(self.trainer.handle_training_task(msg))

    async def _handle_request_best_model(
        self, msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        ack = Ack(ref_type=MSG_REQUEST_BEST, ref_id=msg.get("id_node", "")).to_dict()
        await send_message(writer, ack)

        if self.trainer is None:
            return

        asyncio.create_task(self.trainer.handle_request_best_model())

    def send(self, message: Dict[str, Any]) -> None:
        """Enfileira mensagem para envio ao servidor"""
        self._outbound_queue.put(message)

    def start(self) -> None:
        """Inicia a thread que consome a fila"""
        self._ensure_loop()
        if self.loop is None:
            raise RuntimeError("PeerMessenger requires an event loop")

        if not self.loop.is_running():
            self._loop_thread = threading.Thread(
                target=self.loop.run_forever,
                name="peer-messenger-loop",
                daemon=True,
            )
            self._loop_thread.start()

        self._stop_event.clear()
        self._outbound_thread = threading.Thread(
            target=self._outbound_worker,
            name="peer-messenger-outbound",
            daemon=True,
        )
        self._outbound_thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._outbound_thread and self._outbound_thread.is_alive():
            self._outbound_thread.join(timeout=5.0)

        if self._loop_thread and self._loop_thread.is_alive():
            if self.loop is not None and not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.loop.stop)
            self._loop_thread.join(timeout=5.0)

        if self.loop is not None and not self.loop.is_closed():
            self.loop.close()

        self.loop = None

    def _outbound_worker(self) -> None:
        log.info("PeerMessenger: outbound worker iniciado")

        while not self._stop_event.is_set():
            try:
                message = self._outbound_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            future = asyncio.run_coroutine_threadsafe(
                self._send_async(message), self.loop
            )

            try:
                future.result(timeout=15.0)
            except Exception as exc:
                log.error(f"PeerMessenger: falha ao enviar '{message.get('type')}' - {exc}")
            finally:
                self._outbound_queue.task_done()

        log.info("PeerMessenger: outbound worker encerrado")

    async def _send_async(self, message: Dict[str, Any]) -> None:
        reply = await send_once(
            self.server_ip, self.server_port, message, expect_reply=True, timeout=10.0
        )

        if reply is None or reply.get("type") == "Error":
            log.warning(f"PeerMessenger: envio de '{message.get('type')}' falhou")
        else:
            log.debug(f"PeerMessenger: '{message.get('type')}' confirmado pelo servidor")