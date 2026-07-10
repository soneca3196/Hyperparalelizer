"""
protocol.py - Contratos de mensagens do sistema Hyperparalelizer.
"""
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional
import time


# Tipo de mensagem
MSG_JOIN_NETWORK = "JoinNetwork"
MSG_FIND_NODE = "FindNode"
MSG_KEEP_ALIVE = "KeepAlive"
MSG_TRAINING_TASK = "TrainingTask"
MSG_TASK_RESULT = "TaskResult"
MSG_REQUEST_BEST = "RequestBestModel"
MSG_SEND_BEST = "SendBestModel"
MSG_PUBSUB_SUBSCRIBE = "PubSubSubscribe"
MSG_PUBSUB_UNSUBSCRIBE = "PubSubUnsubscribe"  # added
MSG_PUBSUB_PUBLISH = "PubSubPublish"
MSG_PUBSUB_NOTIFY = "PubSubNotify"
MSG_ACK = "Ack"
MSG_ERROR = "Error"
MSG_MAEKAWA_REQUEST = "MaekawaRequest"
MSG_MAEKAWA_GRANT = "MaekawaGrant"
MSG_MAEKAWA_RELEASE = "MaekawaRelease"
MSG_BULLY_ELECTION = "BullyElection"
MSG_BULLY_ALIVE = "BullyAlive"
MSG_BULLY_COORDINATOR = "BullyCoordinator"
MSG_SYNC_STATE = "SyncState"


@dataclass
class JoinNetwork:
    id_node: str
    ip: str
    porta: int
    memoria_total_mb: float = 0.0
    memoria_disponivel_mb: float = 0.0

    type: str = field(default=MSG_JOIN_NETWORK, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class FindNode:
    id_node: str

    type: str = field(default=MSG_FIND_NODE, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class KeepAlive:
    id_node: str
    timestamp: float = field(default_factory=time.time)

    type: str = field(default=MSG_KEEP_ALIVE, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class TrainingTask:
    task_id: str
    id_node_origem: str
    dataset_fragmentos: List[str]
    parametros: Dict[str, Any]
    model_type: str
    model_config: Dict[str, Any] = field(default_factory=dict)

    type: str = field(default=MSG_TRAINING_TASK, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class TaskResult:
    task_id: str
    id_node: str
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    roc_auc: float
    tempo_treino_s: float = 0.0

    type: str = field(default=MSG_TASK_RESULT, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class RequestBestModel:
    id_node: str

    type: str = field(default=MSG_REQUEST_BEST, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class SendBestModel:
    id_node: str
    model_bytes: bytes
    metricas: Dict[str, float] = field(default_factory=dict)

    type: str = field(default=MSG_SEND_BEST, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class PubSubSubscribe:
    id_node: str
    topic: str
    listen_port: int  # porta do nó para receber notificações

    type: str = field(default=MSG_PUBSUB_SUBSCRIBE, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class PubSubUnsubscribe:  # added
    id_node: str
    topic: str

    type: str = field(default=MSG_PUBSUB_UNSUBSCRIBE, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class PubSubPublish:
    id_node: str
    topic: str
    payload: Dict[str, Any]
    lamport_clock: int = 0

    type: str = field(default=MSG_PUBSUB_PUBLISH, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class PubSubNotify:
    topic: str
    payload: Dict[str, Any]
    lamport_clock: int = 0

    type: str = field(default=MSG_PUBSUB_NOTIFY, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class Ack:
    ref_type: str
    ref_id: Optional[str] = None

    type: str = field(default=MSG_ACK, init=False)

    def to_dict(self):
        return asdict(self)


@dataclass
class ErrorMsg:
    code: str
    detail: str

    type: str = field(default=MSG_ERROR, init=False)

    def to_dict(self):
        return asdict(self)


def from_dict(data: Dict[str, Any]):
    """Builds a message object from dict data."""
    msg_type = data.get("type")
    cls = _TYPE_MAP.get(msg_type)

    if cls is None:
        raise ValueError(f"Unknown message type: {msg_type!r}")

    kwargs = {k: v for k, v in data.items() if k != "type"}
    return cls(**kwargs)


@dataclass
class MaekawaRequest:
    id_node: str
    timestamp: int
    type: str = field(default=MSG_MAEKAWA_REQUEST, init=False)
    def to_dict(self): return asdict(self)


@dataclass
class MaekawaGrant:
    id_node: str
    type: str = field(default=MSG_MAEKAWA_GRANT, init=False)
    def to_dict(self): return asdict(self)


@dataclass
class MaekawaRelease:
    id_node: str
    type: str = field(default=MSG_MAEKAWA_RELEASE, init=False)
    def to_dict(self): return asdict(self)


@dataclass
class SyncState:
    id_node: str # id do servidor
    dht_snapshot: dict
    task_queue_snapshot: list
    best_model_metrics: dict
    type: str = field(default=MSG_SYNC_STATE, init=False)
    def to_dict(self): return asdict(self)


@dataclass
class BullyElectionMsg:
    id_node: str
    type: str = field(default=MSG_BULLY_ELECTION, init=False)
    def to_dict(self): return asdict(self)


@dataclass
class BullyAliveMsg:
    id_node: str
    type: str = field(default=MSG_BULLY_ALIVE, init=False)
    def to_dict(self): return asdict(self)


@dataclass
class BullyCoordinatorMsg:
    id_node: str
    type: str = field(default=MSG_BULLY_COORDINATOR, init=False)
    def to_dict(self): return asdict(self)


# Mapeamento de tipo para as mensagens
_TYPE_MAP = {
    MSG_JOIN_NETWORK: JoinNetwork,
    MSG_FIND_NODE: FindNode,
    MSG_KEEP_ALIVE: KeepAlive,
    MSG_TRAINING_TASK: TrainingTask,
    MSG_TASK_RESULT: TaskResult,
    MSG_REQUEST_BEST: RequestBestModel,
    MSG_SEND_BEST: SendBestModel,
    MSG_PUBSUB_SUBSCRIBE: PubSubSubscribe,
    MSG_PUBSUB_UNSUBSCRIBE: PubSubUnsubscribe,  # added
    MSG_PUBSUB_PUBLISH: PubSubPublish,
    MSG_ACK: Ack,
    MSG_ERROR: ErrorMsg,
    MSG_MAEKAWA_REQUEST: MaekawaRequest,
    MSG_MAEKAWA_GRANT: MaekawaGrant,
    MSG_MAEKAWA_RELEASE: MaekawaRelease,
    MSG_BULLY_ELECTION: BullyElectionMsg, 
    MSG_BULLY_ALIVE: BullyAliveMsg,
    MSG_BULLY_COORDINATOR: BullyCoordinatorMsg,
    MSG_SYNC_STATE: SyncState,
}


