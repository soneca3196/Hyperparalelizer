"""Mensageiro interno do peer: roteia mensagens de rede para o dentro do peer
e prepara uma fila para envio ao servidor"""

import asyncio
import contextlib
import os
import pickle
import queue
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

from utils.logger import get_logger
from utils.protocol import (
    MSG_ERROR,
    MSG_REQUEST_BEST,
    MSG_TASK_RESULT,
    MSG_TRAINING_TASK,
    Ack,
    ErrorMsg,
    validate_ack,
)
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
        self._owns_loop = False
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
            self._owns_loop = False
            return

        try:
            self.loop = asyncio.get_running_loop()
            self._owns_loop = False
            return
        except RuntimeError:
            pass

        try:
            self.loop = asyncio.get_event_loop_policy().get_event_loop()
            self._owns_loop = False
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self._owns_loop = True

    def register_handlers(self, p2p_node: P2PNode) -> None:
        """Registra mensagens vindas do servidor"""
        p2p_node.register_handler(MSG_TRAINING_TASK, self._handle_training_task)
        p2p_node.register_handler(MSG_REQUEST_BEST, self._handle_request_best_model)
        log.debug("PeerMessenger: handlers de rede registrados")

    async def _handle_training_task(
        self, msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        task_id = str(msg.get("task_id") or "")

        if self.trainer is None:
            log.error("PeerMessenger: TrainingTask recebida sem trainer")
            await send_message(
                writer, ErrorMsg(code="NO_TRAINER", detail=task_id).to_dict()
            )
            return

        task_run_id = str(msg.get("run_id") or "")
        trainer_run_id = str(getattr(self.trainer, "run_id", "") or "")
        if trainer_run_id and task_run_id and task_run_id != trainer_run_id:
            log.warning(
                f"PeerMessenger: task '{task_id}' rejeitada (run_id divergente)"
            )
            await send_message(
                writer, ErrorMsg(code="RUN_MISMATCH", detail=task_id).to_dict()
            )
            return

        accepted = await self.trainer.try_submit_task(msg)
        if not accepted:
            log.warning(
                f"PeerMessenger: task '{task_id}' rejeitada (peer ocupado ou duplicada)"
            )
            await send_message(
                writer, ErrorMsg(code="PEER_BUSY", detail=task_id).to_dict()
            )
            return

        ack = Ack(ref_type=MSG_TRAINING_TASK, ref_id=task_id).to_dict()
        await send_message(writer, ack)

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

        if self._owns_loop and self.loop is not None and not self.loop.is_closed():
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

def _safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)


def _short_id(value: str, size: int = 8) -> str:
    return value[:size] + ("…" if len(value) > size else "")


def _atomic_pickle_dump(value: Any, target: Path, lock: Optional[threading.Lock] = None) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(target.suffix + ".tmp")

    def _write() -> None:
        with temp.open("wb") as handle:
            pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)
            handle.flush()
            os.fsync(handle.fileno())
        temp.replace(target)

    if lock is None:
        _write()
    else:
        with lock:
            _write()


class ReliablePeerMessenger(PeerMessenger):
    """PeerMessenger com spool em disco e retry para TaskResult.

    O PeerMessenger padrão não garante que um TaskResult chegue ao servidor
    (o worker de saída tenta uma vez e segue em frente). Esta variante mantém
    TaskResult em disco até receber Ack com ref_type/ref_id corretos,
    reenviando com backoff exponencial enquanto não houver confirmação.
    Mensagens não críticas continuam apenas em memória.
    """

    def __init__(
        self,
        node_id: str,
        server_ip: str,
        server_port: int,
        spool_dir: Path,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        super().__init__(
            node_id=node_id,
            server_ip=server_ip,
            server_port=server_port,
            loop=loop,
        )
        self.spool_dir = spool_dir
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self._spool_lock = threading.Lock()

    def send(self, message: Dict[str, Any]) -> None:
        envelope: Dict[str, Any] = {"message": message, "spool_path": None}
        if message.get("type") == MSG_TASK_RESULT:
            task_id = str(message.get("task_id") or uuid.uuid4())
            spool_path = self.spool_dir / f"task_result_{_safe_filename(task_id)}.pkl"
            _atomic_pickle_dump(message, spool_path, lock=self._spool_lock)
            envelope["spool_path"] = str(spool_path)
        self._outbound_queue.put(envelope)

    def restore_pending_results(self) -> int:
        restored = 0
        for path in sorted(self.spool_dir.glob("task_result_*.pkl")):
            try:
                with path.open("rb") as handle:
                    message = pickle.load(handle)
                if not isinstance(message, dict) or message.get("type") != MSG_TASK_RESULT:
                    raise ValueError("spool não contém TaskResult")
            except Exception as exc:
                quarantine = path.with_suffix(path.suffix + ".corrupt")
                path.replace(quarantine)
                print(f"[WARNING] Spool corrompido movido para {quarantine}: {exc}")
                continue
            self._outbound_queue.put(
                {"message": message, "spool_path": str(path)}
            )
            restored += 1
        if restored:
            print(f"[RECOVERY] {restored} resultado(s) pendente(s) restaurado(s)")
        return restored

    def _outbound_worker(self) -> None:
        print("[MESSENGER] Worker de saída confiável iniciado")
        while not self._stop_event.is_set():
            try:
                envelope = self._outbound_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            message = envelope.get("message", envelope)
            spool_raw = envelope.get("spool_path")
            spool_path = Path(spool_raw) if spool_raw else None
            attempt = 0
            delivered = False

            try:
                while not self._stop_event.is_set() and not delivered:
                    attempt += 1
                    future = asyncio.run_coroutine_threadsafe(
                        self._send_checked(message), self.loop
                    )
                    try:
                        delivered = bool(future.result(timeout=20.0))
                    except Exception as exc:
                        print(
                            f"[WARNING] Falha ao enviar {message.get('type')} "
                            f"(tentativa {attempt}): {exc}"
                        )
                        delivered = False

                    if not delivered and not self._stop_event.is_set():
                        delay = min(30.0, 1.0 * (2 ** min(attempt - 1, 5)))
                        print(
                            f"[RETRY] {message.get('type')} preservado; "
                            f"nova tentativa em {delay:.0f}s"
                        )
                        self._stop_event.wait(delay)

                if delivered and spool_path is not None:
                    with contextlib.suppress(FileNotFoundError):
                        spool_path.unlink()
                    print(
                        f"[DELIVERY] TaskResult {_short_id(str(message.get('task_id')))} "
                        "confirmado e removido do spool"
                    )
            finally:
                self._outbound_queue.task_done()

        print("[MESSENGER] Worker de saída confiável encerrado")

    async def _send_checked(self, message: Dict[str, Any]) -> bool:
        reply = await send_once(
            self.server_ip,
            self.server_port,
            message,
            expect_reply=True,
            timeout=10.0,
        )
        if message.get("type") == MSG_TASK_RESULT:
            return validate_ack(
                reply,
                expected_ref_type=MSG_TASK_RESULT,
                expected_ref_id=str(message.get("task_id") or ""),
            )
        return reply is not None and reply.get("type") != MSG_ERROR