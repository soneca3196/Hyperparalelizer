# mensagens internas entre threads do server (aquelas definidas no docs)
# sugestão: python @dataclass (pesquisar)

"""
server_inner_protocol.py - Mensagens/eventos internos do servidor (não
trafegam pela rede).

Espelha o padrão de hyperparalelizer/peer/peer_inner_protocol.py, mas do
lado do Coordinator/ServerMessenger: desacopla a thread de rede (que
aceita conexões e delega aos handlers em server_peer_protocol.py), a
rotina de verificação de timeout de tarefas (Coordinator.check_task_status)
e a thread de replicação/eleição (Bully, para o pupilo), sem que essas
partes precisem se conhecer diretamente.

Uso típico:

    bus = ServerEventBus()
    bus.subscribe(EVT_TASK_RESULT_RECEIVED, meu_callback)
    ...
    bus.publish(TaskResultReceived(task_id=..., node_id=..., status="success"))
"""

import queue
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional

from utils.logger import get_logger

log = get_logger("server_inner_protocol")


# Tipos de evento interno

EVT_PEER_JOINED = "PeerJoined"
EVT_DATASET_READY_ON_PEER = "DatasetReadyOnPeer"
EVT_TASK_DISPATCHED = "TaskDispatched"
EVT_TASK_RESULT_RECEIVED = "TaskResultReceived"
EVT_TASK_TIMED_OUT = "TaskTimedOut"
EVT_GLOBAL_BEST_MODEL_UPDATED = "GlobalBestModelUpdated"
EVT_PUPIL_SYNCED = "PupilSynced"
EVT_PEER_REMOVED = "PeerRemoved"


@dataclass
class PeerJoined:
    """Um novo peer se cadastrou na GlobalTable (JoinNetwork processado)."""
    node_id: str
    ip: str
    port: int
    fragment_id: Optional[str] = None
    task_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_PEER_JOINED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DatasetReadyOnPeer:
    """Um peer confirmou posse local de um fragmento (DatasetReady)."""
    node_id: str
    fragment_id: str
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_DATASET_READY_ON_PEER, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TaskDispatched:
    """O Coordinator despachou (ou redespachou) uma task para um peer."""
    task_id: str
    node_id: str
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_TASK_DISPATCHED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TaskResultReceived:
    """Resultado (sucesso ou falha) de uma task recebido de um peer."""
    task_id: str
    node_id: str
    status: str  # "success" | "failed"
    f1_score: Optional[float] = None
    is_new_best: bool = False
    error: Optional[str] = None
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_TASK_RESULT_RECEIVED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TaskTimedOut:
    """Uma task atribuída excedeu TASK_TIMEOUT e foi devolvida ao pool."""
    task_id: str
    node_id: str
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_TASK_TIMED_OUT, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class GlobalBestModelUpdated:
    """O melhor modelo global (GlobalTable.best_model) mudou."""
    task_id: str
    node_id: str
    f1_score: float
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_GLOBAL_BEST_MODEL_UPDATED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PupilSynced:
    """Um snapshot da GlobalTable foi replicado com sucesso para o pupilo."""
    pupil_ip: str
    pupil_port: int
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_PUPIL_SYNCED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PeerRemoved:
    """Um peer foi removido da GlobalTable (KeepAlive falhou)."""
    node_id: str
    reason: str = "keepalive_failed"
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_PEER_REMOVED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Barramento interno (thread-safe)

InternalEvent = Any
EventCallback = Callable[[InternalEvent], None]


class ServerEventBus:
    """Pub/sub thread-safe para eventos internos do servidor.

    Mesma implementação de hyperparalelizer.peer.peer_inner_protocol.
    InternalEventBus, duplicada aqui (em vez de compartilhada) para manter
    server/ e peer/ desacoplados um do outro.
    """

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[EventCallback]] = {}
        self._lock = threading.Lock()
        self._history: "queue.Queue[InternalEvent]" = queue.Queue()

    def subscribe(self, event_type: str, callback: EventCallback) -> None:
        with self._lock:
            self._subscribers.setdefault(event_type, []).append(callback)

    def unsubscribe(self, event_type: str, callback: EventCallback) -> None:
        with self._lock:
            callbacks = self._subscribers.get(event_type, [])
            if callback in callbacks:
                callbacks.remove(callback)

    def publish(self, event: InternalEvent) -> None:
        event_type = getattr(event, "type", None)
        if event_type is None:
            raise ValueError("event_type is None")

        self._history.put(event)

        with self._lock:
            callbacks = list(self._subscribers.get(event_type, []))

        for callback in callbacks:
            try:
                callback(event)
            except Exception as exc:
                log.error(f"ServerEventBus: assinante de '{event_type}' falhou - {exc}")

    def drain_history(self) -> List[InternalEvent]:
        """Esvazia e retorna o histórico (útil para debugging/testes)."""
        events: List[InternalEvent] = []
        while True:
            try:
                events.append(self._history.get_nowait())
            except queue.Empty:
                break
        return events
