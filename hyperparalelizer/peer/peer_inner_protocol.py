"""Mensagens entre threads/tarefas (não trafegam
pela rede)"""

import queue
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional

from utils.logger import get_logger

log = get_logger("peer_inner_protocol")


# Tipos de evento interno

EVT_FRAGMENT_ACQUIRED = "FragmentAcquired"
EVT_FRAGMENT_ASSEMBLY_FAILED = "FragmentAssemblyFailed"
EVT_TRAINING_STARTED = "TrainingStarted"
EVT_TRAINING_FINISHED = "TrainingFinished"
EVT_TRAINING_FAILED = "TrainingFailed"
EVT_BEST_MODEL_UPDATED = "BestModelUpdatedLocally"


@dataclass
class FragmentAcquired:
    """Fragmento passou a existir"""
    fragment_id: str
    node_id: str
    source: str  # "local" | "peer" | "server_backup"
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_FRAGMENT_ACQUIRED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FragmentAssemblyFailed:
    """Nenhuma fonte tem o fragmento"""
    fragment_id: str
    node_id: str
    reason: str = "no_source_available"
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_FRAGMENT_ASSEMBLY_FAILED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TrainingStarted:
    """Thread de treino"""
    task_id: str
    node_id: str
    fragment_ids: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_TRAINING_STARTED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TrainingFinished:
    """Treino concluído com sucesso"""
    task_id: str
    node_id: str
    metrics: Dict[str, float] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_TRAINING_FINISHED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TrainingFailed:
    """Treino abortado"""
    task_id: str
    node_id: str
    error: str
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_TRAINING_FAILED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BestModelUpdatedLocally:
    """Modelo bateu o próprio recorde (não o recorde global necessariamente)"""
    task_id: str
    node_id: str
    score: float
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=EVT_BEST_MODEL_UPDATED, init=False)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Barramento interno (thread-safe)

InternalEvent = Any
EventCallback = Callable[[InternalEvent], None]


class InternalEventBus:
    """Pub/sub para eventos do peer"""

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
        self._history.put(event)

        with self._lock:
            callbacks = list(self._subscribers.get(event_type, []))

        for callback in callbacks:
            try:
                callback(event)
            except Exception as exc:
                log.error(f"InternalEventBus: assinante de '{event_type}' falhou - {exc}")

    def drain_history(self) -> List[InternalEvent]:
        """Esvazia e retorna o histórico"""
        events: List[InternalEvent] = []
        while True:
            try:
                events.append(self._history.get_nowait())
            except queue.Empty:
                break
        return events