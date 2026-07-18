from __future__ import annotations

"""tipos, validações e utilitários compartilhados pelo runtime do servidor e do peer
"""

import argparse
import asyncio
import contextlib
import hashlib
import os
import pickle
import queue
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
from sklearn.datasets import load_breast_cancer

from core.network import send_once
from hyperparalelizer.ml.dataset_loader import DatasetLoader
from hyperparalelizer.peer.data_thread import DataThread
from hyperparalelizer.peer.peer_messenger import PeerMessenger
from utils.protocol import (
    Ack,
    DatasetReady,
    MSG_ACK,
    MSG_ERROR,
    MSG_JOIN_ACK,
    MSG_TASK_RESULT,
    MSG_TRAINING_TASK,
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
    """Mantém o último contato observado de cada peer
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
    """Mutex permitido somente quando --disable-maekawa"""

    state = "RELEASED"

    async def request_access(self) -> None:
        self.state = "HELD"

    async def release_access(self) -> None:
        self.state = "RELEASED"



class ValidatingDatasetLoader(DatasetLoader):
    """DatasetLoader com validação de amostras e hash da execução"""

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
    """PeerMessenger com spool em disco e retry para TaskResult"""

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