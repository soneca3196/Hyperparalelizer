from __future__ import annotations

"""hyperparalelizer.main - bootstrap e orquestração do Hyperparalelizer.

Integra servidor coordenador, peers de treinamento, fragmentação de dataset,
Maekawa, Bully, Peer Pupilo e Pub/Sub conforme a especificação do Grupo 12.

Uso:
    python -m main_beta4 server --host 127.0.0.1 --port 9000 --fragments 10 --stop-when-complete
    python -m main_beta4 peer --host 127.0.0.1 --port 9101 --server-host 127.0.0.1 --server-port 9000 --reset-storage
    python -m main_beta4 peer --host 127.0.0.1 --port 9102 --server-host 127.0.0.1 --server-port 9000 --reset-storage
"""

import argparse
import asyncio
import contextlib
import hashlib
import os
import pickle
import queue
import shutil
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
from sklearn.datasets import load_breast_cancer

from core.network import P2PNode, send_message, send_once
from hyperparalelizer.global_table import GlobalTable
from hyperparalelizer.ml.dataset_loader import DatasetLoader
from hyperparalelizer.peer.data_thread import DataThread
from hyperparalelizer.peer.peer_inner_protocol import (
    EVT_BEST_MODEL_UPDATED,
    EVT_FRAGMENT_ACQUIRED,
    EVT_FRAGMENT_ASSEMBLY_FAILED,
    EVT_TRAINING_FAILED,
    EVT_TRAINING_FINISHED,
    EVT_TRAINING_STARTED,
    InternalEventBus,
)
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from hyperparalelizer.peer.peer_outer_protocol import register_peer_peer_handlers
from hyperparalelizer.peer.trainer import TrainerNode
from hyperparalelizer.server.coordinator import Coordinator
from hyperparalelizer.server.server_messenger import ServerMessenger
from hyperparalelizer.server.server_peer_protocol import (
    handle_keep_alive,
    register_all_handlers,
)
from hyperparalelizer.sync.bully import BullyElection
from hyperparalelizer.sync.maekawa import MaekawaMutex, MaekawaTimeoutError
from utils.protocol import (
    Ack,
    DatasetReady,
    ErrorMsg,
    KeepAlive,
    MSG_ACK,
    MSG_ERROR,
    MSG_JOIN_ACK,
    MSG_KEEP_ALIVE,
    MSG_MEMBERSHIP_UPDATE,
    MSG_PEER_READY,
    MSG_PUBSUB_NOTIFY,
    MSG_PUBSUB_PUBLISH,
    MSG_SYNC_STATE,
    MSG_TASK_RESULT,
    MSG_TRAINING_TASK,
    PeerReady,
    PubSubNotify,
)


DATASET_NAME = "sklearn.load_breast_cancer"
EXPECTED_SAMPLE_COUNT = 569
DEFAULT_MODEL_TYPE = "random_forest"
DEFAULT_GRID: Dict[str, List[Any]] = {
    "n_estimators": [1, 2, 3, 4, 5, 18, 19, 20, 30],
    "max_depth": [3, 5, 10, 20, 50, 60, 70, 80, 90],
    "random_state": [42],
    "n_jobs": [1],
}
SEPARATOR = "=" * 60


class BootstrapError(RuntimeError):
    """Erro de configuração ou validação que impede inicialização segura."""


class DatasetValidationError(RuntimeError):
    """Dataset montado não corresponde à execução esperada."""


@dataclass(frozen=True)
class DatasetIdentity:
    name: str
    sample_count: int
    feature_count: int
    sha256: str

    @property
    def short_id(self) -> str:
        return self.sha256[:16]


@dataclass(frozen=True)
class ValidatedJoin:
    node_id: str
    fragment_id: str
    peers: List[Dict[str, Any]]
    run_id: str
    initial_task: Optional[Dict[str, Any]]


@dataclass
class ServerProgress:
    total_tasks: int
    started_at: float = field(default_factory=time.monotonic)
    completed_task_ids: Set[str] = field(default_factory=set)
    failed_task_ids: Set[str] = field(default_factory=set)
    retry_counts: Dict[str, int] = field(default_factory=dict)

    def register_success(self, task_id: str) -> None:
        if task_id:
            self.completed_task_ids.add(task_id)

    def register_retry(self, task_id: str) -> int:
        if not task_id:
            return 0
        value = self.retry_counts.get(task_id, 0) + 1
        self.retry_counts[task_id] = value
        return value

    def register_failure_or_retry(self, task_id: str, max_retries: int) -> tuple[bool, int]:
        retries = self.register_retry(task_id)
        if retries > max_retries:
            if task_id:
                self.failed_task_ids.add(task_id)
            return False, retries
        return True, retries

    @property
    def completed(self) -> int:
        return len(self.completed_task_ids)

    @property
    def failed(self) -> int:
        return len(self.failed_task_ids)


@dataclass
class PeerHealth:
    last_seen: float
    misses: int = 0
    declared_dead: bool = False


class ServerHeartbeatTracker:
    """Mantém o último contato observado de cada peer.

    O protocolo atual responde KeepAlive, mas não persiste last_seen no
    Coordinator/GlobalTable. Este tracker fica na camada de runtime e chama o
    método de recuperação já existente no Coordinator.
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


class NoOpMutex:
    """Mutex permitido somente quando --disable-maekawa é explícito."""

    state = "RELEASED"

    async def request_access(self) -> None:
        self.state = "HELD"

    async def release_access(self) -> None:
        self.state = "RELEASED"



class ValidatingDatasetLoader(DatasetLoader):
    """DatasetLoader com validação de amostras e hash da execução atual."""

    def __init__(self, storage_dir: str, expected: DatasetIdentity):
        super().__init__(storage_dir)
        self.expected = expected
        self._validated_keys: Set[Tuple[str, ...]] = set()

    def load(self, fragment_ids: List[str]) -> Tuple[np.ndarray, np.ndarray]:
        key = tuple(fragment_ids)
        missing = [
            fragment_id
            for fragment_id in fragment_ids
            if not Path(self._fragment_path(fragment_id)).is_file()
        ]
        if missing:
            raise DatasetValidationError(
                "[VALIDATION ERROR] Fragmentos ausentes: " + ", ".join(missing)
            )

        try:
            X, y = super().load(fragment_ids)
        except (FileNotFoundError, ValueError, pickle.UnpicklingError) as exc:
            raise DatasetValidationError(
                f"[VALIDATION ERROR] Fragmento ausente ou corrompido: {exc}"
            ) from exc

        if len(X) != self.expected.sample_count:
            raise DatasetValidationError(
                "[VALIDATION ERROR] Dataset incompatível.\n"
                f"Esperado: {self.expected.sample_count} amostras\n"
                f"Carregado: {len(X)} amostras\n"
                "Possível causa: fragmentos de uma execução anterior."
            )

        actual = build_dataset_identity(self.expected.name, X, y)
        if actual.sha256 != self.expected.sha256:
            raise DatasetValidationError(
                "[VALIDATION ERROR] Hash do dataset incompatível.\n"
                f"Dataset esperado: {self.expected.short_id}\n"
                f"Dataset carregado: {actual.short_id}\n"
                "Possível causa: fragmentos antigos, duplicados ou de outro dataset."
            )

        if key not in self._validated_keys:
            self._validated_keys.add(key)
            print(
                f"[VALIDATION] {len(fragment_ids)}/{len(fragment_ids)} fragmentos "
                f"disponíveis | {len(X)} amostras | dataset_id={actual.short_id}"
            )
        return X, y


class ValidatingDataThread(DataThread):
    """DataThread que valida o dataset e confirma cada fragmento uma vez."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.dataset_loader: Optional[ValidatingDatasetLoader] = None
        self._notified_fragments: Set[str] = set()
        self.last_validation_error: Optional[str] = None

    def attach_dataset_loader(self, loader: ValidatingDatasetLoader) -> None:
        self.dataset_loader = loader

    async def assemble_many(self, fragment_ids: List[str]) -> bool:
        ok = await super().assemble_many(fragment_ids)
        if not ok:
            return False

        if self.dataset_loader is None:
            self.last_validation_error = "DatasetLoader de validação não conectado"
            print(f"[VALIDATION ERROR] {self.last_validation_error}")
            return False

        try:
            # Executa antes do treino. O Trainer fará uma segunda leitura barata,
            # mas a chave validada evita repetir os logs.
            await asyncio.to_thread(self.dataset_loader.load, fragment_ids)
        except DatasetValidationError as exc:
            self.last_validation_error = str(exc)
            print(str(exc))
            return False

        for fragment_id in fragment_ids:
            notified = await self.notify_dataset_ready(fragment_id)
            if not notified:
                return False
        return True

    async def notify_dataset_ready(self, fragment_id: Optional[str] = None) -> bool:
        fid = fragment_id or self.fragment_id
        if not isinstance(fid, str) or not fid:
            print("[VALIDATION ERROR] DatasetReady sem fragment_id válido")
            return False
        if fid in self._notified_fragments:
            return True

        reply = await send_once(
            self.server_ip,
            self.server_port,
            DatasetReady(id_node=self.node_id, fragment_id=fid).to_dict(),
            expect_reply=True,
            timeout=10.0,
        )
        valid = validate_ack(reply, expected_ref_type="DatasetReady", expected_ref_id=fid)
        if not valid:
            print(f"[WARNING] DatasetReady não confirmado para '{fid}'")
            return False

        self._notified_fragments.add(fid)
        print(f"[DATASET READY] Posse confirmada uma vez para '{fid}'")
        return True


class ReliablePeerMessenger(PeerMessenger):
    """PeerMessenger com spool em disco e retry para TaskResult.

    O PeerMessenger atual chama task_done() mesmo quando o servidor não confirma
    o resultado. Esta extensão mantém TaskResult em disco até receber Ack com
    ref_type/ref_id corretos. Mensagens não críticas continuam apenas em memória.
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
            spool_path = self.spool_dir / f"task_result_{safe_filename(task_id)}.pkl"
            atomic_pickle_dump(message, spool_path, lock=self._spool_lock)
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
                        f"[DELIVERY] TaskResult {short_id(str(message.get('task_id')))} "
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




@dataclass
class ServerRuntime:
    coordinator: Coordinator
    messenger: ServerMessenger
    progress: ServerProgress
    heartbeat: ServerHeartbeatTracker
    run_id: str
    dataset_identity: DatasetIdentity
    args: argparse.Namespace
    pubsub_queue: Optional[queue.Queue]
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    complete_event: asyncio.Event = field(default_factory=asyncio.Event)
    job_started: asyncio.Event = field(default_factory=asyncio.Event)
    tasks: List[asyncio.Task[Any]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Funções puras e validações
# ---------------------------------------------------------------------------


def short_id(value: str, size: int = 8) -> str:
    return value[:size] + ("…" if len(value) > size else "")


def safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in value)


def build_dataset_identity(name: str, X: np.ndarray, y: np.ndarray) -> DatasetIdentity:
    X_arr = np.ascontiguousarray(np.asarray(X))
    y_arr = np.ascontiguousarray(np.asarray(y))
    digest = hashlib.sha256()
    digest.update(name.encode("utf-8"))
    digest.update(str(X_arr.shape).encode("ascii"))
    digest.update(str(X_arr.dtype).encode("ascii"))
    digest.update(X_arr.tobytes(order="C"))
    digest.update(str(y_arr.shape).encode("ascii"))
    digest.update(str(y_arr.dtype).encode("ascii"))
    digest.update(y_arr.tobytes(order="C"))
    return DatasetIdentity(
        name=name,
        sample_count=len(X_arr),
        feature_count=X_arr.shape[1] if X_arr.ndim > 1 else 1,
        sha256=digest.hexdigest(),
    )


def load_reference_dataset() -> Tuple[np.ndarray, np.ndarray, DatasetIdentity]:
    dataset = load_breast_cancer()
    X = dataset.data
    y = dataset.target
    identity = build_dataset_identity(DATASET_NAME, X, y)
    if identity.sample_count != EXPECTED_SAMPLE_COUNT:
        raise BootstrapError(
            f"Dataset de referência inesperado: {identity.sample_count} amostras"
        )
    return X, y, identity


def grid_size(grid: Dict[str, Sequence[Any]]) -> int:
    size = 1
    for values in grid.values():
        size *= len(values)
    return size


def normalize_peers(
    peers: Iterable[Dict[str, Any]],
    exclude_node_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for peer in peers:
        if not isinstance(peer, dict):
            continue
        node_id = peer.get("id_node") or peer.get("node_id")
        ip = peer.get("ip")
        port = peer.get("port")
        if (
            not isinstance(node_id, str)
            or not node_id
            or node_id == exclude_node_id
            or node_id in seen
            or not isinstance(ip, str)
            or not ip
            or not isinstance(port, int)
            or not (1 <= port <= 65535)
        ):
            continue
        normalized.append({"id_node": node_id, "ip": ip, "port": port})
        seen.add(node_id)
    return normalized


def validate_training_task(task: Any) -> Optional[Dict[str, Any]]:
    if task is None:
        return None
    if not isinstance(task, dict):
        raise BootstrapError("JoinAck.task deve ser dict ou None")
    if task.get("type") != MSG_TRAINING_TASK:
        raise BootstrapError(
            f"Tipo da tarefa inicial inválido: {task.get('type')!r}"
        )
    task_id = task.get("task_id")
    fragments = task.get("dataset_fragmentos")
    params = task.get("parametros")
    model_type = task.get("model_type")
    if not isinstance(task_id, str) or not task_id:
        raise BootstrapError("Tarefa inicial sem task_id válido")
    if (
        not isinstance(fragments, list)
        or not fragments
        or any(not isinstance(item, str) or not item for item in fragments)
    ):
        raise BootstrapError("Tarefa inicial com dataset_fragmentos inválido")
    if not isinstance(params, dict):
        raise BootstrapError("Tarefa inicial com parametros inválido")
    if not isinstance(model_type, str) or not model_type:
        raise BootstrapError("Tarefa inicial com model_type inválido")
    return task


def validate_join_reply(reply: Any) -> ValidatedJoin:
    if reply is None:
        raise BootstrapError("Servidor não respondeu ao JoinNetwork")
    if not isinstance(reply, dict):
        raise BootstrapError("Resposta do JoinNetwork não é um dicionário")
    if reply.get("type") == MSG_ERROR:
        raise BootstrapError(
            f"Servidor recusou JoinNetwork: {reply.get('code') or reply.get('reason')} - "
            f"{reply.get('detail', '')}"
        )
    if reply.get("type") != MSG_JOIN_ACK:
        raise BootstrapError(
            f"Resposta inesperada ao JoinNetwork: {reply.get('type')!r}"
        )
    run_id = reply.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise BootstrapError("JoinAck sem run_id válido")

    node_id = reply.get("node_id")
    fragment_id = reply.get("fragment_id")
    peers = reply.get("peers")
    if not isinstance(node_id, str) or not node_id:
        raise BootstrapError("JoinAck sem node_id válido")
    if not isinstance(fragment_id, str) or not fragment_id:
        raise BootstrapError("JoinAck sem fragment_id válido")
    if not isinstance(peers, list):
        raise BootstrapError("JoinAck.peers deve ser uma lista")

    normalized = normalize_peers(peers, exclude_node_id=node_id)
    if len(normalized) != len(peers):
        raise BootstrapError("JoinAck contém peer(s) com estrutura inválida")

    initial_task = validate_training_task(reply.get("task"))
    return ValidatedJoin(
        node_id=node_id,
        fragment_id=fragment_id,
        peers=normalized,
        run_id=run_id,
        initial_task=initial_task,
    )


def validate_ack(
    reply: Any,
    expected_ref_type: str,
    expected_ref_id: str,
) -> bool:
    return (
        isinstance(reply, dict)
        and reply.get("type") == MSG_ACK
        and reply.get("ref_type") == expected_ref_type
        and str(reply.get("ref_id") or "") == str(expected_ref_id)
    )


def resolve_storage_dir(args: argparse.Namespace) -> Path:
    if args.storage_dir:
        return Path(args.storage_dir).expanduser()
    return Path("data") / f"peer_{args.port}" / "fragments"


def assert_safe_storage_path(storage_dir: Path) -> Path:
    raw = str(storage_dir).strip()
    if not raw:
        raise BootstrapError("Caminho de storage vazio não é permitido")

    candidate = storage_dir.expanduser().resolve()
    data_root = Path("data").resolve()
    project_root = Path.cwd().resolve()
    home = Path.home().resolve()
    filesystem_root = Path(candidate.anchor).resolve()

    forbidden = {data_root, project_root, home, filesystem_root}
    if candidate in forbidden:
        raise BootstrapError(f"Recusa de limpeza em caminho perigoso: {candidate}")

    try:
        candidate.relative_to(data_root)
    except ValueError as exc:
        raise BootstrapError(
            f"Storage deve permanecer dentro de '{data_root}': {candidate}"
        ) from exc

    if candidate.name.lower() != "fragments":
        raise BootstrapError(
            "Por segurança, o diretório removível deve terminar em 'fragments': "
            f"{candidate}"
        )
    return candidate


def prepare_storage(storage_dir: Path, reset: bool) -> Path:
    safe_path = assert_safe_storage_path(storage_dir)
    if reset and safe_path.exists():
        file_count = sum(1 for path in safe_path.rglob("*") if path.is_file())
        shutil.rmtree(safe_path)
        print(
            f"[STORAGE] Fragmentos antigos removidos de {safe_path} "
            f"({file_count} arquivo(s))"
        )
    safe_path.mkdir(parents=True, exist_ok=True)
    if reset:
        print("[STORAGE] Diretório de armazenamento recriado")
    else:
        print(f"[STORAGE] Diretório preparado: {safe_path}")
    return safe_path


def atomic_pickle_dump(
    value: Any,
    target: Path,
    lock: Optional[threading.Lock] = None,
) -> None:
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


async def wait_for_listener(host: str, port: int, timeout: float = 10.0) -> None:
    """Confirma que start() abriu a porta; evita depender de sleep arbitrário."""
    connect_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    deadline = time.monotonic() + timeout
    last_error: Optional[BaseException] = None
    while time.monotonic() < deadline:
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(connect_host, port),
                timeout=min(1.0, max(0.05, deadline - time.monotonic())),
            )
            writer.close()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(writer.wait_closed(), timeout=1.0)
            return
        except (ConnectionRefusedError, OSError, asyncio.TimeoutError) as exc:
            last_error = exc
            await asyncio.sleep(0.05)
    raise BootstrapError(
        f"Serviço não começou a escutar em {connect_host}:{port}: {last_error}"
    )


def create_task(
    registry: List[asyncio.Task[Any]],
    coroutine: Any,
    name: str,
) -> asyncio.Task[Any]:
    task = asyncio.create_task(coroutine, name=name)
    registry.append(task)

    def _report(done: asyncio.Task[Any]) -> None:
        if done.cancelled():
            return
        exc = done.exception()
        if exc is not None:
            print(f"[BACKGROUND ERROR] {done.get_name()}: {exc}")

    task.add_done_callback(_report)
    return task


async def cancel_tasks(tasks: Iterable[asyncio.Task[Any]]) -> None:
    pending = [task for task in tasks if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


# ---------------------------------------------------------------------------
# Runtime do servidor
# ---------------------------------------------------------------------------




def server_progress_snapshot(runtime: ServerRuntime) -> Tuple[int, int, int, int]:
    table = runtime.coordinator.GlobalTable
    with table.lock:
        waiting = len(table.task_pool)
        running = len(table.assigned_tasks)
    return runtime.progress.completed, running, waiting, runtime.progress.failed


def print_progress(runtime: ServerRuntime) -> None:
    completed, running, waiting, failed = server_progress_snapshot(runtime)
    print(
        f"[PROGRESS] {completed}/{runtime.progress.total_tasks} concluídas | "
        f"{running} executando | {waiting} aguardando | {failed} falhas"
    )


def is_formally_complete(runtime: ServerRuntime) -> bool:
    completed, running, waiting, failed = server_progress_snapshot(runtime)
    return (
        runtime.job_started.is_set()
        and waiting == 0
        and running == 0
        and completed + failed == runtime.progress.total_tasks
    )




async def status_consumer(runtime: ServerRuntime) -> None:
    while not runtime.stop_event.is_set():
        event = await runtime.messenger.status_queue.get()
        try:
            event_name = event.get("event")
            node_id = str(
                event.get("node_id")
                or event.get("peer_id")
                or event.get("requester_id")
                or ""
            )
            if node_id:
                runtime.heartbeat.touch(node_id)

            if event_name == "peer_joined":
                print(
                    f"[JOIN] Peer {short_id(node_id)} em "
                    f"{event.get('ip')}:{event.get('port')} | "
                    f"fragmento={event.get('fragment_id')} | "
                    f"task={event.get('task_id')}"
                )
                if not runtime.args.disable_pupil:
                    changed = await runtime.coordinator.pupil_manager.reconcile()
                    pupil = runtime.coordinator.pupil_peer
                    if changed and pupil is not None:
                        print(
                            f"[PUPIL] Peer de backup atual: "
                            f"{short_id(str(pupil.get('id_node')))} "
                            f"(epoch={runtime.coordinator.pupil_manager.epoch})"
                        )

            elif event_name == "task_result":
                task_id = str(event.get("task_id") or "")
                if event.get("status") == "success":
                    runtime.progress.register_success(task_id)
                else:
                    accepted_retry, retry = runtime.progress.register_failure_or_retry(
                        task_id, runtime.args.max_task_retries
                    )
                    if accepted_retry:
                        print(
                            f"[RETRY] Tarefa {short_id(task_id)} reenfileirada "
                            f"(tentativa registrada: {retry})"
                        )
                    else:
                        print(
                            f"[PROGRESSO] Tarefa {short_id(task_id)} falhou "
                            f"definitivamente após {runtime.args.max_task_retries} tentativas."
                        )
                        
                print_progress(runtime)

            elif event_name == "dataset_ready":
                print(
                    f"[DATASET] {short_id(node_id)} possui "
                    f"{event.get('fragment_id')}"
                )

            elif event_name == "fragment_backup_served":
                print(
                    f"[FRAGMENT] Backup de {event.get('fragment_id')} enviado "
                    f"a {short_id(node_id)}"
                )
        finally:
            runtime.messenger.status_queue.task_done()


async def scheduler_service(runtime: ServerRuntime, interval: float = 1.0) -> None:
    await runtime.job_started.wait()
    while not runtime.stop_event.is_set():
        timed_out = runtime.coordinator.check_task_status()
        for task_id in timed_out:
            retry = runtime.progress.register_retry(task_id)
            print(
                f"[TIMEOUT] Tarefa {short_id(task_id)} excedeu "
                f"{runtime.coordinator.task_timeout:.1f}s e voltou à fila "
                f"(retry {retry})"
            )

        await runtime.coordinator.dispatch_all_idle()

        if is_formally_complete(runtime):
            runtime.complete_event.set()
            if runtime.args.stop_when_complete:
                runtime.stop_event.set()
                return
        await asyncio.sleep(interval)


async def peer_failure_monitor(runtime: ServerRuntime) -> None:
    interval = runtime.args.heartbeat_interval
    timeout = runtime.args.heartbeat_timeout
    max_failures = runtime.args.heartbeat_failures

    while not runtime.stop_event.is_set():
        await asyncio.sleep(interval)
        now = time.monotonic()
        for node_id, health in runtime.heartbeat.items():
            if health.declared_dead:
                continue
            if now - health.last_seen <= timeout:
                health.misses = 0
                continue

            health.misses += 1
            if health.misses < max_failures:
                print(
                    f"[WARNING] Peer {short_id(node_id)} não respondeu "
                    f"({health.misses}/{max_failures})"
                )
                continue

            health.declared_dead = True
            with runtime.coordinator.GlobalTable.lock:
                before_pending = len(runtime.coordinator.GlobalTable.task_pool)
                before_assigned = len(runtime.coordinator.GlobalTable.assigned_tasks)
            print(f"[ERROR] Peer {short_id(node_id)} considerado morto")
            runtime.coordinator.handle_peer_failure(node_id)
            with runtime.coordinator.GlobalTable.lock:
                after_pending = len(runtime.coordinator.GlobalTable.task_pool)
                after_assigned = len(runtime.coordinator.GlobalTable.assigned_tasks)
            recovered = max(0, after_pending - before_pending)
            removed_assignments = max(0, before_assigned - after_assigned)
            print(
                f"[RECOVERY] {max(recovered, removed_assignments)} tarefa(s) "
                "devolvida(s) para a fila"
            )
            print("[RECOVERY] Localizações de fragmentos removidas")
            runtime.heartbeat.remove(node_id)


async def pupil_sync_service(runtime: ServerRuntime, interval: float = 2.0) -> None:
    await runtime.job_started.wait()
    last_confirmed: Optional[str] = None
    while not runtime.stop_event.is_set():
        await asyncio.sleep(interval)

        nodes = [
            node
            for node in runtime.coordinator.GlobalTable.get_all_nodes()
            if node.get("ready") is True
        ]

        if not nodes:

            continue
            
        await runtime.coordinator.pupil_manager.reconcile()
        pupil = runtime.coordinator.pupil_peer
        if pupil is None:
            continue
        try:
            synced = await runtime.coordinator.replicate_state_to_pupil()
            pupil_id = str(pupil.get("id_node") or "")
            if synced and pupil_id != last_confirmed:
                last_confirmed = pupil_id
                print(
                    f"[PUPIL] Snapshot confirmado em "
                    f"{short_id(pupil_id)}"
                )
        except Exception as exc:
            print(f"[WARNING] Falha ao sincronizar Peer Pupilo: {exc}")


async def pubsub_broadcast_service(runtime: ServerRuntime) -> None:
    """Bridge beta do tópico global enquanto não há runtime Pub/Sub exposto."""
    if runtime.pubsub_queue is None:
        return
    while not runtime.stop_event.is_set():
        try:
            publish = runtime.pubsub_queue.get_nowait()
        except queue.Empty:
            await asyncio.sleep(0.1)
            continue

        try:
            if publish.get("type") != MSG_PUBSUB_PUBLISH:
                continue
            notification = PubSubNotify(
                topic=publish.get("topic", "global_best_score"),
                payload=publish.get("payload") or {},
                lamport_clock=int(publish.get("lamport_clock") or 0),
            ).to_dict()
            nodes = runtime.coordinator.GlobalTable.get_all_nodes()
            sends = [
                send_once(
                    node["ip"],
                    node["port"],
                    notification,
                    expect_reply=False,
                    timeout=5.0,
                )
                for node in nodes
            ]
            if sends:
                await asyncio.gather(*sends, return_exceptions=True)
            payload = notification.get("payload") or {}
            print(
                f"[PUBSUB] Novo melhor score global: "
                f"{float(payload.get('f1_score') or 0.0):.4f}"
            )
        finally:
            runtime.pubsub_queue.task_done()


async def wait_for_minimum_peers(runtime: ServerRuntime) -> None:
    minimum = runtime.args.min_peers
    if minimum <= 1:
        runtime.job_started.set()
        return

    deadline = time.monotonic() + runtime.args.peer_wait_timeout
    last_count = -1
    while not runtime.stop_event.is_set():
        count = len(runtime.coordinator.GlobalTable.get_all_nodes())
        if count != last_count:
            print(f"[WAITING] {count}/{minimum} peers conectados")
            last_count = count
        if count >= minimum:
            break
        if time.monotonic() >= deadline:
            print(
                f"[WARNING] Timeout aguardando {minimum} peers; "
                f"iniciando com {count}"
            )
            break
        await asyncio.sleep(0.25)

    # No modo min-peers o grid é criado somente aqui, portanto nenhum JoinAck
    # anterior reservou tarefa. O scheduler fará o primeiro dispatch por TCP.
    runtime.coordinator.generate_grid_search(DEFAULT_GRID)
    runtime.job_started.set()
    print("[START] Iniciando distribuição das tarefas")


async def tracked_keep_alive_handler(
    msg: Dict[str, Any],
    writer: asyncio.StreamWriter,
    runtime: ServerRuntime,
) -> None:
    node_id = str(msg.get("id_node") or "")
    runtime.heartbeat.touch(node_id)
    await handle_keep_alive(
        msg,
        writer,
        coordinator=runtime.coordinator,
        status_queue=runtime.messenger.status_queue,
    )


async def start_server_runtime(runtime: ServerRuntime) -> None:
    register_all_handlers(
        runtime.messenger,
        runtime.coordinator,
        runtime.messenger.status_queue,
    )
    runtime.messenger.register_handler(
        MSG_KEEP_ALIVE,
        lambda msg, writer: tracked_keep_alive_handler(msg, writer, runtime),
    )

    create_task(runtime.tasks, runtime.messenger.start(), "server-tcp")
    await wait_for_listener(runtime.args.host, runtime.args.port)
    print(f"[READY] Servidor escutando em {runtime.args.host}:{runtime.args.port}")

    create_task(runtime.tasks, status_consumer(runtime), "server-status-consumer")
    create_task(runtime.tasks, scheduler_service(runtime), "server-scheduler")
    create_task(runtime.tasks, peer_failure_monitor(runtime), "server-heartbeat-monitor")

    if not runtime.args.disable_pupil:
        create_task(runtime.tasks, pupil_sync_service(runtime), "server-pupil-sync")
    if runtime.pubsub_queue is not None:
        create_task(runtime.tasks, pubsub_broadcast_service(runtime), "server-pubsub")

    if runtime.args.min_peers > 1:
        create_task(
            runtime.tasks,
            wait_for_minimum_peers(runtime),
            "server-wait-min-peers",
        )
    else:
        runtime.job_started.set()


async def shutdown_server_runtime(runtime: ServerRuntime) -> None:
    print("[SHUTDOWN] Encerrando serviços do servidor...")
    runtime.stop_event.set()
    await runtime.messenger.stop()
    await cancel_tasks(runtime.tasks)
    print("[SHUTDOWN] Mensageiro e rotinas do servidor encerrados")


def print_server_header(
    args: argparse.Namespace,
    run_id: str,
    identity: DatasetIdentity,
    task_count: int,
) -> None:
    print(SEPARATOR)
    print("Inicializando servidor Hyperparalelizer")
    print(f"Run ID: {run_id}")
    print(f"Dataset ID: {identity.short_id}")
    print(f"Endereço: {args.host}:{args.port}")
    print(f"Dataset: {identity.sample_count} amostras")
    print(f"Fragmentos: {args.fragments}")
    print(f"Tarefas: {task_count}")
    print(f"Modelo: {DEFAULT_MODEL_TYPE}")
    print("Maekawa: executado nos peers")
    print(f"Pub/Sub: {'desabilitado' if args.disable_pubsub else 'bridge beta habilitado'}")
    print(f"Pupilo: {'desabilitado' if args.disable_pupil else 'habilitado'}")
    print(SEPARATOR)


def print_completion_summary(runtime: ServerRuntime) -> None:
    best = runtime.coordinator.get_best_model() or {}
    elapsed = time.monotonic() - runtime.progress.started_at
    print(SEPARATOR)
    print("TREINAMENTO DISTRIBUÍDO FINALIZADO")
    print(f"Run ID: {runtime.run_id}")
    print(f"Total de tarefas: {runtime.progress.total_tasks}")
    print(f"Concluídas: {runtime.progress.completed}")
    print(f"Falhas definitivas: {runtime.progress.failed}")
    print(f"Tempo total: {elapsed:.2f}s")
    print(f"Melhor F1: {float(best.get('f1_score') or 0.0):.6f}")
    print(f"Melhor task: {best.get('task_id')}")
    print(f"Peer vencedor: {best.get('peer_id')}")
    print(f"Hiperparâmetros vencedores: {best.get('hyperparameters')}")
    print(SEPARATOR)


async def run_server(args: argparse.Namespace) -> None:
    X, y, identity = load_reference_dataset()
    run_id = uuid.uuid4().hex
    total_tasks = grid_size(DEFAULT_GRID)
    pubsub_queue: Optional[queue.Queue] = (
        None if args.disable_pubsub else queue.Queue()
    )

    global_table = GlobalTable()
    coordinator = Coordinator(
        dataset=(X, y),
        model=None,
        global_table=global_table,
        model_type=DEFAULT_MODEL_TYPE,
        model_config={},
        pubsub_queue=pubsub_queue,
        task_timeout=args.task_timeout,
        run_id=run_id,
        max_task_retries=args.max_task_retries,
    )
    fragment_ids = coordinator.fragment_dataset(n_fragments=args.fragments)
    if len(fragment_ids) != args.fragments:
        raise BootstrapError(
            f"Fragmentação gerou {len(fragment_ids)}, esperado {args.fragments}"
        )

    # Fluxo normal: tarefa inicial reservada por add_peer e enviada no JoinAck.
    # Com min_peers > 1, o grid é adiado para impedir reserva prematura.
    if args.min_peers <= 1:
        coordinator.generate_grid_search(DEFAULT_GRID)

    messenger = ServerMessenger(
        coordinator=coordinator,
        host=args.host,
        port=args.port,
    )
    runtime = ServerRuntime(
        coordinator=coordinator,
        messenger=messenger,
        progress=ServerProgress(total_tasks=total_tasks),
        heartbeat=ServerHeartbeatTracker(),
        run_id=run_id,
        dataset_identity=identity,
        args=args,
        pubsub_queue=pubsub_queue,
    )

    print_server_header(args, run_id, identity, total_tasks)
    await start_server_runtime(runtime)

    try:
        if args.stop_when_complete:
            await runtime.complete_event.wait()
            print_completion_summary(runtime)
        else:
            await runtime.stop_event.wait()
    except asyncio.CancelledError:
        raise
    finally:
        await shutdown_server_runtime(runtime)


# ---------------------------------------------------------------------------
# Runtime do peer
# ---------------------------------------------------------------------------


def print_peer_header(
    args: argparse.Namespace,
    local_run_id: str,
    storage_dir: Path,
    identity: DatasetIdentity,
) -> None:
    print(SEPARATOR)
    print("Inicializando peer Hyperparalelizer")
    print(f"Run ID local: {local_run_id}")
    print(f"Dataset esperado: {identity.short_id}")
    print(f"Peer: {args.host}:{args.port}")
    print(f"Servidor: {args.server_host}:{args.server_port}")
    print(f"Storage: {storage_dir}")
    print(f"Reset storage: {'sim' if args.reset_storage else 'não'}")
    print(f"Maekawa: {'desabilitado' if args.disable_maekawa else 'habilitado'}")
    print(f"Bully: {'desabilitado' if args.disable_bully else 'habilitado'}")
    print(f"Pub/Sub: {'desabilitado' if args.disable_pubsub else 'habilitado'}")
    print(SEPARATOR)


def print_join_summary(join: ValidatedJoin, identity: DatasetIdentity) -> None:
    print("[JOIN VALIDADO]")
    print(f"Node ID: {join.node_id}")
    print(f"Fragmento atribuído: {join.fragment_id}")
    print(f"Peers conhecidos: {len(join.peers)}")
    print(
        f"Tarefa inicial: "
        f"{join.initial_task.get('task_id') if join.initial_task else None}"
    )
    print(f"Dataset ID esperado: {identity.short_id}")


def build_bully_peer_map(peers: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        peer["id_node"]: {"ip": peer["ip"], "port": peer["port"]}
        for peer in normalize_peers(peers)
    }


def update_endpoints_after_election(
    bully: BullyElection,
    messenger: ReliablePeerMessenger,
    data_thread: ValidatingDataThread,
) -> bool:
    ip = bully.current_coordinator_ip
    port = bully.current_coordinator_port
    if not isinstance(ip, str) or not ip or not isinstance(port, int):
        return False
    messenger.server_ip = ip
    messenger.server_port = port
    data_thread.server_ip = ip
    data_thread.server_port = port
    print(f"[ELECTION] Novo servidor configurado em {ip}:{port}")
    return True


async def peer_heartbeat_loop(
    args: argparse.Namespace,
    node_id: str,
    messenger: ReliablePeerMessenger,
    data_thread: ValidatingDataThread,
    bully: Optional[BullyElection],
    stop_event: asyncio.Event,
) -> None:
    failures = 0
    unavailable = False
    election_started = False

    while not stop_event.is_set():
        await asyncio.sleep(args.heartbeat_interval)
        try:
            reply = await send_once(
                messenger.server_ip,
                messenger.server_port,
                KeepAlive(id_node=node_id).to_dict(),
                expect_reply=True,
                timeout=args.heartbeat_timeout,
            )
            ok = validate_ack(
                reply,
                expected_ref_type=MSG_KEEP_ALIVE,
                expected_ref_id=node_id,
            )
        except Exception as exc:
            print(f"[WARNING] Erro no heartbeat: {exc}")
            ok = False

        if ok:
            if unavailable:
                print("[HEARTBEAT] Conexão com o servidor restaurada")
            failures = 0
            unavailable = False
            election_started = False
            continue

        failures += 1
        print(
            f"[WARNING] Servidor não respondeu ao heartbeat "
            f"({failures}/{args.heartbeat_failures})"
        )
        if failures < args.heartbeat_failures:
            continue

        if not unavailable:
            print("[ERROR] Servidor considerado indisponível")
        unavailable = True

        if bully is None:
            continue

        if not election_started:
            election_started = True
            print("[ELECTION] Iniciando algoritmo Bully")
            try:
                await bully.detect_timeout_and_start()
            except Exception as exc:
                print(f"[ELECTION ERROR] {exc}")

        if update_endpoints_after_election(bully, messenger, data_thread):
            failures = 0


async def promoted_server_from_peer(
    args: argparse.Namespace,
    node_id: str,
    trainer: TrainerNode,
    p2p_node: P2PNode,
    p2p_task: asyncio.Task[Any],
    messenger: ReliablePeerMessenger,
    peer_tasks: List[asyncio.Task[Any]],
) -> None:
    """Promoção beta usando o snapshot recebido pelo TrainerNode.

    O servidor promovido usa a antiga porta P2P do vencedor. Isso é compatível
    com BullyElection.handle_coordinator(), que divulga o IP/porta do peer.
    """
    if not trainer.replica_global_table_snapshot:
        print(
            "[PROMOTION ERROR] Este peer venceu a eleição, mas não possui "
            "snapshot da GlobalTable. A promoção operacional foi abortada."
        )
        return

    print("[PROMOTION] Encerrando papel P2P para assumir como servidor")
    await cancel_tasks(peer_tasks)
    with contextlib.suppress(Exception):
        await p2p_node.stop()
    if not p2p_task.done():
        p2p_task.cancel()
        await asyncio.gather(p2p_task, return_exceptions=True)
    await asyncio.to_thread(messenger.stop)

    coordinator = trainer.promote_to_server()
    # O peer promovido deixa de ser executor. Sua tarefa em andamento, se
    # ainda constar no snapshot, é reenfileirada pelo método existente.
    coordinator.handle_peer_failure(node_id)

    promoted_args = argparse.Namespace(**vars(args))
    promoted_args.host = args.host
    promoted_args.port = args.port
    promoted_args.task_timeout = 30.0
    promoted_args.stop_when_complete = False
    promoted_args.min_peers = 1
    promoted_args.peer_wait_timeout = 0.0
    promoted_args.disable_pupil = False

    remaining = 0
    with coordinator.GlobalTable.lock:
        remaining = (
            len(coordinator.GlobalTable.task_pool)
            + len(coordinator.GlobalTable.assigned_tasks)
        )

    promoted_messenger = ServerMessenger(
        coordinator=coordinator,
        host=args.host,
        port=args.port,
    )
    runtime = ServerRuntime(
        coordinator=coordinator,
        messenger=promoted_messenger,
        progress=ServerProgress(total_tasks=remaining),
        heartbeat=ServerHeartbeatTracker(),
        run_id=f"promoted-{uuid.uuid4().hex}",
        dataset_identity=load_reference_dataset()[2],
        args=promoted_args,
        pubsub_queue=None if args.disable_pubsub else queue.Queue(),
    )
    runtime.job_started.set()
    print(
        f"[PROMOTION] Novo servidor será iniciado em {args.host}:{args.port} "
        f"com {remaining} tarefa(s) remanescente(s)"
    )
    await start_server_runtime(runtime)
    try:
        await runtime.stop_event.wait()
    finally:
        await shutdown_server_runtime(runtime)


async def run_peer(args: argparse.Namespace) -> None:
    _, _, identity = load_reference_dataset()
    local_run_id = f"peer-{args.port}-{uuid.uuid4().hex[:8]}"
    storage_dir = prepare_storage(resolve_storage_dir(args), args.reset_storage)
    spool_dir = storage_dir.parent / "pending_results"
    print_peer_header(args, local_run_id, storage_dir, identity)

    event_bus = InternalEventBus()
    data_thread = ValidatingDataThread(
        ip=args.host,
        listen_port=args.port,
        server_ip=args.server_host,
        server_port=args.server_port,
        storage_dir=str(storage_dir),
        event_bus=event_bus,
    )

    join_reply = await data_thread.join_network(timeout=args.join_timeout)
    join = validate_join_reply(join_reply)
    data_thread.node_id = join.node_id
    data_thread.fragment_id = join.fragment_id
    data_thread.known_peers = join.peers
    data_thread.initial_task = join.initial_task
    print_join_summary(join, identity)

    dataset_loader = ValidatingDatasetLoader(str(storage_dir), expected=identity)
    data_thread.attach_dataset_loader(dataset_loader)

    if args.disable_maekawa:
        mutex: Any = NoOpMutex()
    else:
        mutex = MaekawaMutex(node_id=join.node_id, quorum=join.peers)
        print(f"[MAEKAWA] Quórum inicial com {len(join.peers)} peer(s)")

    loop = asyncio.get_running_loop()
    messenger = ReliablePeerMessenger(
        node_id=join.node_id,
        server_ip=args.server_host,
        server_port=args.server_port,
        spool_dir=spool_dir,
        loop=loop,
    )
    trainer = TrainerNode(
        node_id=join.node_id,
        messenger=messenger,
        data_thread=data_thread,
        dataset_loader=dataset_loader,
        maekawa_mutex=mutex,
        event_bus=event_bus,
        run_id=join.run_id,
    )
    messenger.attach_trainer(trainer)

    p2p_node = P2PNode(host=args.host, port=args.port, node_id=join.node_id)
    messenger.register_handlers(p2p_node)
    register_peer_peer_handlers(p2p_node, storage_dir=str(storage_dir))
    if not args.disable_maekawa:
        mutex.register_handlers(p2p_node)

    promotion_requested = asyncio.Event()

    def request_promotion() -> None:
        promotion_requested.set()

    bully: Optional[BullyElection]
    if args.disable_bully:
        bully = None
    else:
        bully = BullyElection(
            my_id=join.node_id,
            globaltable_peers=build_bully_peer_map(join.peers),
            promote_callback=request_promotion,
        )
        bully.register_handlers(p2p_node)

    async def handle_sync_state(
        msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        snapshot_id = str(msg.get("snapshot_id") or "")
        pupil_id = str(msg.get("pupil_id") or "")
        pupil_epoch = int(msg.get("pupil_epoch") or 0)

        if pupil_epoch < trainer.pupil_epoch:
            await send_message(
                writer,
                ErrorMsg(code="STALE_SYNC_STATE", detail=snapshot_id).to_dict(),
            )
            return

        trainer.pupil_epoch = pupil_epoch

        if pupil_id != join.node_id:
            trainer.is_pupil = False
            trainer.replica_global_table_snapshot = {}
            with contextlib.suppress(Exception):
                await send_message(
                    writer,
                    Ack(ref_type=MSG_SYNC_STATE, ref_id=snapshot_id).to_dict(),
                )
            return

        trainer.is_pupil = True
        trainer.handle_sync_state(msg)
        await send_message(
            writer, Ack(ref_type=MSG_SYNC_STATE, ref_id=snapshot_id).to_dict()
        )

    async def handle_membership_update(
        msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        peers = msg.get("peers") or []
        normalized = normalize_peers(peers, exclude_node_id=join.node_id)
        data_thread.update_known_peers(normalized)
        if hasattr(mutex, "replace_quorum"):
            mutex.replace_quorum(normalized)
        if bully is not None:
            bully.peers = build_bully_peer_map(normalized)
        with contextlib.suppress(Exception):
            await send_message(
                writer,
                Ack(
                    ref_type=MSG_MEMBERSHIP_UPDATE,
                    ref_id=str(msg.get("epoch") or ""),
                ).to_dict(),
            )

    async def handle_pubsub_notify(
        msg: Dict[str, Any], writer: asyncio.StreamWriter
    ) -> None:
        payload = msg.get("payload") or {}
        score = float(payload.get("f1_score") or -1.0)
        if score > trainer.best_score:
            trainer.best_score = score
        print(f"[PUBSUB] Novo melhor score global: {score:.4f}")
        producer = payload.get("id_node")
        if producer:
            print(f"[PUBSUB] Modelo produzido pelo peer {short_id(str(producer))}")
        # O bridge publica com expect_reply=False; não responde.
        del writer

    p2p_node.register_handler(MSG_SYNC_STATE, handle_sync_state)
    p2p_node.register_handler(MSG_MEMBERSHIP_UPDATE, handle_membership_update)
    if not args.disable_pubsub:
        p2p_node.register_handler(MSG_PUBSUB_NOTIFY, handle_pubsub_notify)

    def on_fragment_acquired(event: Any) -> None:
        print(
            f"[FRAGMENT] {event.fragment_id} disponível via {event.source}"
        )

    def on_fragment_failed(event: Any) -> None:
        print(
            f"[FRAGMENT ERROR] {event.fragment_id}: {event.reason}"
        )

    def on_training_started(event: Any) -> None:
        print(f"[TRAINING] Iniciada task {short_id(event.task_id)}")

    def on_training_finished(event: Any) -> None:
        print(f"[TRAINING] Finalizada task {short_id(event.task_id)}")

    def on_training_failed(event: Any) -> None:
        print(
            f"[TRAINING ERROR] Task {short_id(event.task_id)}: {event.error}"
        )

    def on_local_best(event: Any) -> None:
        print(f"[LOCAL BEST] Score local atualizado para {event.score:.4f}")

    event_bus.subscribe(EVT_FRAGMENT_ACQUIRED, on_fragment_acquired)
    event_bus.subscribe(EVT_FRAGMENT_ASSEMBLY_FAILED, on_fragment_failed)
    event_bus.subscribe(EVT_TRAINING_STARTED, on_training_started)
    event_bus.subscribe(EVT_TRAINING_FINISHED, on_training_finished)
    event_bus.subscribe(EVT_TRAINING_FAILED, on_training_failed)
    event_bus.subscribe(EVT_BEST_MODEL_UPDATED, on_local_best)

    messenger.restore_pending_results()
    messenger.start()

    peer_tasks: List[asyncio.Task[Any]] = []
    p2p_task = create_task(peer_tasks, p2p_node.start(), "peer-p2p-server")
    await wait_for_listener(args.host, args.port)
    print(f"[READY] Peer P2P escutando em {args.host}:{args.port}")

    ready_reply = await send_once(
        args.server_host,
        args.server_port,
        PeerReady(id_node=join.node_id).to_dict(),
        expect_reply=True,
        timeout=10.0,
    )
    if validate_ack(
        ready_reply, expected_ref_type=MSG_PEER_READY, expected_ref_id=join.node_id
    ):
        print("[READY] PeerReady confirmado; apto a receber tarefas do servidor")
    else:
        print("[WARNING] Servidor não confirmou PeerReady")

    stop_event = asyncio.Event()
    heartbeat_task = create_task(
        peer_tasks,
        peer_heartbeat_loop(
            args,
            join.node_id,
            messenger,
            data_thread,
            bully,
            stop_event,
        ),
        "peer-server-heartbeat",
    )
    del heartbeat_task

    if join.initial_task is not None:
        # A tarefa já foi reservada por Coordinator.add_peer e veio no JoinAck.
        create_task(
            peer_tasks,
            trainer.try_submit_task(join.initial_task),
            "peer-initial-training-task",
        )

    try:
        if bully is None:
            await stop_event.wait()
        else:
            await promotion_requested.wait()
            stop_event.set()
            await promoted_server_from_peer(
                args=args,
                node_id=join.node_id,
                trainer=trainer,
                p2p_node=p2p_node,
                p2p_task=p2p_task,
                messenger=messenger,
                peer_tasks=peer_tasks,
            )
    except asyncio.CancelledError:
        raise
    finally:
        print("[SHUTDOWN] Encerrando serviços do peer...")
        stop_event.set()
        await cancel_tasks(peer_tasks)
        with contextlib.suppress(Exception):
            await p2p_node.stop()
        await asyncio.to_thread(messenger.stop)
        print("[SHUTDOWN] Finalizado com segurança")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("valor deve ser maior que zero")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("valor deve ser maior que zero")
    return parsed


def valid_port(value: str) -> int:
    parsed = int(value)
    if not (1 <= parsed <= 65535):
        raise argparse.ArgumentTypeError("porta deve estar entre 1 e 65535")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Hyperparalelizer - servidor e peer distribuído"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    server = subparsers.add_parser("server", help="Inicia o servidor central")
    server.add_argument("--host", default="127.0.0.1")
    server.add_argument("--port", type=valid_port, default=9000)
    server.add_argument("--fragments", type=positive_int, default=10)
    server.add_argument("--task-timeout", type=positive_float, default=30.0)
    server.add_argument("--heartbeat-interval", type=positive_float, default=5.0)
    server.add_argument("--heartbeat-timeout", type=positive_float, default=12.0)
    server.add_argument("--heartbeat-failures", type=positive_int, default=3)
    server.add_argument("--stop-when-complete", action="store_true")
    server.add_argument("--min-peers", type=positive_int, default=1)
    server.add_argument("--peer-wait-timeout", type=positive_float, default=30.0)
    server.add_argument("--disable-pubsub", action="store_true")
    server.add_argument("--disable-pupil", action="store_true")
    server.add_argument(
        "--max-task-retries",
        type=positive_int,
        default=3,
        help="Limite de tentativas por tarefa",
    )

    peer = subparsers.add_parser("peer", help="Inicia um nó de treinamento")
    peer.add_argument("--host", default="127.0.0.1")
    peer.add_argument("--port", type=valid_port, required=True)
    peer.add_argument("--server-host", default="127.0.0.1")
    peer.add_argument("--server-port", type=valid_port, default=9000)
    peer.add_argument("--storage-dir", default=None)
    peer.add_argument("--reset-storage", action="store_true")
    peer.add_argument("--join-timeout", type=positive_float, default=60.0)
    peer.add_argument("--heartbeat-interval", type=positive_float, default=5.0)
    peer.add_argument("--heartbeat-timeout", type=positive_float, default=3.0)
    peer.add_argument("--heartbeat-failures", type=positive_int, default=3)
    peer.add_argument("--disable-maekawa", action="store_true")
    peer.add_argument("--disable-bully", action="store_true")
    peer.add_argument("--disable-pubsub", action="store_true")

    return parser


async def async_main() -> None:
    parser = build_parser()

    parser.add_argument(
        "--max-task-retries",
        type=int,
        default=3,
        help="Limite de tentativas por tarefa",
    )

    args = parser.parse_args()

    if args.mode == "server":
        await run_server(args)
    elif args.mode == "peer":
        await run_peer(args)
    else:
        parser.error(f"Modo desconhecido: {args.mode}")


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Interrupção recebida; encerrado pelo usuário")
    except BootstrapError as exc:
        print(f"[BOOTSTRAP ERROR] {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()