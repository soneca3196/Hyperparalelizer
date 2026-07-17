# (Pessoa 4) Nó "Barriga": orquestra tarefas e middleware

# servidor STATELESS
# O coordenador não mantém estado dinâmico interno. 
# Todas as informações críticas de rede, tarefas e nós residem e são gerenciadas na GlobalTable.

import asyncio
import itertools
import math
import pickle
import queue
import time
import uuid
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from core.network import send_once
from core.pubsub import TOPIC_GLOBAL_BEST_SCORE
from hyperparalelizer.global_table import GlobalTable, ServerState
from utils.logger import get_logger
from utils.protocol import MSG_ACK, MSG_SYNC_STATE, PubSubPublish, TrainingTask
from hyperparalelizer.server.pupil_manager import PupilManager

log = get_logger("coordinator")

TASK_TIMEOUT = 30.0  # segundos até uma tarefa ser considerada perdida


@dataclass
class Peer:
    ip: str
    port: int
    id_node: str = ""          # preenchido pelo GlobalTable após add_node
    hyperparameters: dict = field(default_factory=dict)
    metrics: dict = field(default_factory=dict)
    received_dataset: bool = False
    received_model: bool = False


class Coordinator:
    def __init__(self, dataset, model, global_table: GlobalTable,
                 model_type: str = "generic",
                 model_config: Optional[Dict[str, Any]] = None,
                 pubsub_queue: Optional[queue.Queue] = None,
                 task_timeout: float = TASK_TIMEOUT):
        # Configurações estáticas/inputs (não mudam ao longo do ciclo de vida)
        self.dataset = dataset
        self.model = model
        self.model_type = model_type
        self.model_config = model_config or {}

        # Dependências externas
        self.GlobalTable = global_table
        self._pubsub_queue = pubsub_queue  # fila Middleware → PubSubClient
        self.task_timeout = float(task_timeout)
        
        # Referência ao peer pupilo (se existir)
        self.pupil_peer: Optional[Dict[str, Any]] = None
        self.pupil_manager = PupilManager(self)

    def get_best_model(self) -> Optional[Dict[str, Any]]:
        best = self.GlobalTable.get_best_model()
        return dict(best) if best is not None else None

    def update_best_model( self, task_id: str, peer_id: str, hyperparameters: Dict[str, Any], metrics: Dict[str, Any], f1_score: float, model_bytes: bytes,) -> None:
        self.GlobalTable.set_best_model({
            "task_id": task_id,
            "peer_id": peer_id,
            "hyperparameters": dict(hyperparameters),
            "metrics": dict(metrics),
            "f1_score": float(f1_score),
            "model_bytes": model_bytes,
        })

    # ENDPOINT: ADICIONA NOVO PEER                                      
    def add_peer(self, peer: Peer,) -> tuple[str, Optional[str], Optional[TrainingTask]]:
        """
        Registra um peer e devolve as informações necessárias para o JoinAck.

        Returns:
            node_id: ID definitivo do peer.
            fragment_id: Fragmento inicialmente atribuído ao peer, ou None.
            task: Primeira tarefa reservada para o peer, ou None.
        """

        node_id = self.GlobalTable.add_node(peer.ip, peer.port, peer,)

        peer.id_node = node_id

        fragment_id: Optional[str] = None

        with self.GlobalTable.lock:
            dataset_fragments = list(self.GlobalTable.fragments_payloads.keys())

            all_nodes = list(self.GlobalTable.nodes.values())

            peer_index = next(
                (
                    index
                    for index, node in enumerate(all_nodes)
                    if node["node_id"] == node_id
                ),
                0,
            )

        if dataset_fragments:
            fragment_index = ( peer_index % len(dataset_fragments))
            fragment_id = dataset_fragments[fragment_index]

        task = self.assign_hyperparameters_to_peer(peer)

        return node_id, fragment_id, task

    def build_peer_list(self, exclude_node_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.GlobalTable.lock:
            nodes = list(self.GlobalTable.nodes.values())
        return [
            {"id_node": n["node_id"], "ip": n["ip"], "port": n["port"]}
            for n in nodes
            if n["node_id"] != exclude_node_id
        ]

    async def broadcast_membership_update(self) -> None:
        with self.GlobalTable.lock:
            nodes = list(self.GlobalTable.nodes.values())
        membership_epoch = len(nodes)
        sends = []
        for node in nodes:
            peers_for_node = self.build_peer_list(exclude_node_id=node["node_id"])
            msg = {
                "type": "MembershipUpdate",
                "epoch": membership_epoch,
                "peers": peers_for_node,
            }
            sends.append(send_once(node["ip"], node["port"], msg, expect_reply=False, timeout=5.0))
        if sends:
            await asyncio.gather(*sends, return_exceptions=True)
    
    # GRID SEARCH                                              
    def generate_grid_search(self, hyperparameters: Dict[str, List[Any]]) -> List[Dict[str, Any]]:
        with self.GlobalTable.lock:
            dataset_fragments = list(self.GlobalTable.fragments_payloads.keys())

        if not dataset_fragments:
            raise RuntimeError(
                "Execute fragment_dataset() antes de generate_grid_search()."
            )

        keys = list(hyperparameters.keys())
        values = list(hyperparameters.values())

        combinations = [
            dict(zip(keys, combo))
            for combo in itertools.product(*values)
        ]
        
        # Cada combinação vira uma TrainingTask populada diretamente na GlobalTable
        with self.GlobalTable.lock:
            self.GlobalTable.task_pool = [
                TrainingTask(
                    task_id=str(uuid.uuid4()),
                    id_node_origem="server",
                    dataset_fragmentos=dataset_fragments,
                    parametros=combo,
                    model_type=self.model_type,
                    model_config=dict(self.model_config),
                )
                for combo in combinations
            ]

        return combinations

    # ATRIBUIÇÃO DE HIPERPARÂMETROS                                        
    def assign_hyperparameters_to_peer(self, peer: Peer) -> Optional[TrainingTask]:
        with self.GlobalTable.lock:
            if not self.GlobalTable.task_pool:
                return None
            
            task = self.GlobalTable.task_pool.pop(0)
            self.GlobalTable.assigned_tasks[task.task_id] = {
                "peer": peer,
                "timestamp": time.time(),
                "task": task,
            }
            
        peer.hyperparameters = task.parametros
        return task

    # FRAGMENTAÇÃO DO DATASET                                              
    def fragment_dataset(self, n_fragments: Optional[int] = None) -> List[str]:
        """
        Divide self.dataset em n_fragments partes e registra cada fragmento
        na GlobalTable
        """
        with self.GlobalTable.lock:
            all_nodes = list(self.GlobalTable.nodes.values())
            
        current_peers_count = len(all_nodes)
            
        if n_fragments is None:
            n_fragments = max(1, current_peers_count)

        if n_fragments <= 0:
            raise ValueError("n_fragments deve ser maior que zero.")

        try:
            X_full, y_full = self.dataset
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "self.dataset deve ser uma tupla (X, y) com X e y do mesmo "
                "tamanho, prontos para fragmentação."
            ) from exc

        total = len(X_full)
        if total == 0:
            raise ValueError("dataset está vazio.")
        if len(y_full) != total:
            raise ValueError("X e y do dataset possuem tamanhos diferentes.")

        fragment_size = math.ceil(total / n_fragments)
        fragment_names: List[str] = []
        
        for i in range(n_fragments):
            X_frag = X_full[i * fragment_size: (i + 1) * fragment_size]
            y_frag = y_full[i * fragment_size: (i + 1) * fragment_size]
            frag_name = f"fragment_{i:04d}"
            fragment_names.append(frag_name)

            payload_bytes = pickle.dumps({"X": X_frag, "y": y_frag})

            with self.GlobalTable.lock:
                self.GlobalTable.fragments_payloads[frag_name] = payload_bytes

            if current_peers_count > 0:
                owner_node = all_nodes[i % current_peers_count]
                self.GlobalTable.add_fragment_location(frag_name, owner_node["node_id"])

        with self.GlobalTable.lock:
            self.GlobalTable.system_state = ServerState.DATASET_DISTRIBUTION

        return fragment_names

    # VERIFICAÇÃO DE STATUS / TIMEOUT                                      
    def check_task_status(self) -> List[str]:
        now = time.time()
        timed_out_ids: List[str] = []

        with self.GlobalTable.lock:
            for task_id, entry in list(self.GlobalTable.assigned_tasks.items()):
                timestamp = entry.get("timestamp")
                if timestamp is None:
                    continue
                if now - timestamp > self.task_timeout:
                    timed_out_ids.append(task_id)

            for task_id in timed_out_ids:
                task_info = self.GlobalTable.assigned_tasks.pop(task_id)
                self.GlobalTable.task_pool.append(task_info["task"])  # Devolve à fila central da GlobalTable

        return timed_out_ids

    # DESPACHO DE TAREFAS

    async def dispatch_next_task(self, peer: Peer) -> bool:
        """Reserva a próxima tarefa da fila e envia ao peer via TCP.

        Se o envio falhar (sem resposta ou Ack inválido), a tarefa é
        devolvida ao início de task_pool para ser reatribuída.
        Retorna True se a tarefa foi aceita pelo peer.
        """
        task = self.GlobalTable.reserve_next_task_for_peer(peer)
        if task is None:
            return False  # fila vazia ou peer já ocupado
        peer.hyperparameters = task.parametros

        reply = await send_once(
            peer.ip,
            peer.port,
            task.to_dict(),
            expect_reply=True,
        )

        if reply is None or reply.get("type") != MSG_ACK:
            # rollback: devolve a tarefa ao início da fila
            with self.GlobalTable.lock:
                self.GlobalTable.assigned_tasks.pop(task.task_id, None)
                peer.hyperparameters = {}
                self.GlobalTable.task_pool.insert(0, task)
            log.warning(
                f"dispatch_next_task: peer {peer.id_node[:8]}… "
                f"({peer.ip}:{peer.port}) não confirmou "
                f"tarefa {task.task_id[:8]}…, recolocada na fila"
            )
            return False

        with self.GlobalTable.lock:
            entry = self.GlobalTable.assigned_tasks.get(task.task_id)
            if entry is not None:
                entry["timestamp"] = time.time()

        log.info(
            f"dispatch_next_task: tarefa {task.task_id[:8]}… "
            f"enviada para {peer.id_node[:8]}… ({peer.ip}:{peer.port})"
        )
        return True

    async def dispatch_all_idle(self) -> List[str]:
        """Envia tarefas para todos os peers ociosos e prontos (ready=True)"""
        with self.GlobalTable.lock:
            busy_ids = {entry["peer"].id_node for entry in self.GlobalTable.assigned_tasks.values()}
            all_nodes = list(self.GlobalTable.nodes.values())

        idle_peers = [
            node["metadata"]
            for node in all_nodes
            if node["node_id"] not in busy_ids and node.get("ready") is True
        ]

        dispatched: List[str] = []
        for peer in idle_peers:
            with self.GlobalTable.lock:
                has_tasks = bool(self.GlobalTable.task_pool)
            if not has_tasks:
                break
            ok = await self.dispatch_next_task(peer)
            if ok:
                dispatched.append(peer.id_node)

        if dispatched:
            with self.GlobalTable.lock:
                self.GlobalTable.system_state = ServerState.MODEL_DISTRIBUTION

        return dispatched

    async def run_scheduler_loop(self, interval: float = 1.0, max_iterations: Optional[int] = None) -> int:
        """despacho e reatribuição de tarefas"""
        iterations = 0
        while max_iterations is None or iterations < max_iterations:
            self.check_task_status()
            dispatched = await self.dispatch_all_idle()
            iterations += 1
            with self.GlobalTable.lock:
                no_pending = not self.GlobalTable.task_pool
                no_running = not self.GlobalTable.assigned_tasks
            if not dispatched and no_pending and no_running:
                break
            if interval > 0:
                await asyncio.sleep(interval)
        return iterations

    # DISTRIBUIÇÃO                                                         
    def distribute_dataset(self):
        with self.GlobalTable.lock:
            self.GlobalTable.system_state = ServerState.DATASET_DISTRIBUTION

    def handle_peer_failure(self, node_id: str) -> None:
        """Remove um peer falho da GlobalTable e reencaminha suas tarefas para a fila."""
        with self.GlobalTable.lock:
            node_entry = self.GlobalTable.nodes.pop(node_id, None)
            if node_entry is None:
                return

            for fragment_name, locations in list(self.GlobalTable.fragments_locations.items()):
                if node_id in locations:
                    self.GlobalTable.fragments_locations[fragment_name] = [
                        loc for loc in locations if loc != node_id
                    ]

            for task_id, task_info in list(self.GlobalTable.assigned_tasks.items()):
                peer = task_info.get("peer")
                if peer is not None and getattr(peer, "id_node", None) == node_id:
                    task = task_info.get("task")
                    if task is not None:
                        self.GlobalTable.task_pool.insert(0, task)
                    self.GlobalTable.assigned_tasks.pop(task_id, None)

        log.warning(f"handle_peer_failure: peer {node_id[:8]}… removido e tarefas reencaminhadas")

    
    def persist_state(self, path: str | Path | None = None) -> None:
        """Persiste o estado da GlobalTable"""
        target = Path(path) if path is not None else Path("data/state.pkl")
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as handle:
            pickle.dump(self.GlobalTable.get_snapshot(), handle)

    @classmethod
    def load_state(cls, path: str | Path | None = None) -> "GlobalTable":
        """Carrega a GlobalTable a partir do disco"""
        target = Path(path) if path is not None else Path("data/state.pkl")
        if not target.exists():
            raise FileNotFoundError(target)
        with target.open("rb") as handle:
            snapshot = pickle.load(handle)
        return GlobalTable(snapshot=snapshot)

    def receive_task_result(self, task_id: str, metrics: Dict[str, Any],) -> Optional[tuple]:
        """
        `metrics` é o TaskResult inteiro recebido do peer (dict), incluindo
        os campos "status" e "error" quando o treino falhou.
        """
        is_new_best = False

        # Apenas consulta a tarefa (não remove ainda)
        with self.GlobalTable.lock:
            task_info = self.GlobalTable.assigned_tasks.get(task_id)

        if task_info is None:
            return None

        peer = task_info["peer"]
        task = task_info["task"]

        failed = metrics.get("status") == "failed" or metrics.get("error") is not None
        if failed:
            with self.GlobalTable.lock:
                self.GlobalTable.assigned_tasks.pop(task_id, None)
                self.GlobalTable.task_pool.insert(0, task)
            peer.hyperparameters = {}
            log.warning(
                f"receive_task_result: tarefa {task_id[:8]}… falhou no peer "
                f"{peer.id_node[:8]}… ({metrics.get('error')}), reenfileirada"
            )
            return peer, task


        with self.GlobalTable.lock:
            self.GlobalTable.assigned_tasks.pop(task_id, None)

        peer.metrics = metrics
        new_score = float(
            metrics.get("f1")
            if metrics.get("f1") is not None
            else metrics.get("f1_score")
            or 0.0
        )
        
        # Obtém o score do melhor modelo atual usando o método thread-safe da GlobalTable
        current_best = self.get_best_model()
        current_best_score = (
            current_best.get("f1_score", -1.0)
            if current_best is not None
            else -1.0
        )

        if new_score > current_best_score:
            is_new_best = True

            self.update_best_model(
                task_id=task_id,
                peer_id=peer.id_node,
                hyperparameters=task.parametros,
                metrics=dict(metrics),
                f1_score=new_score,
                model_bytes=metrics.get("model_bytes", b""),
            )

        if is_new_best and self._pubsub_queue is not None:
            publish_msg = PubSubPublish(
                id_node="server",
                topic=TOPIC_GLOBAL_BEST_SCORE,
                payload={
                    "task_id": task_id,
                    "id_node": peer.id_node,
                    "f1_score": new_score,
                    "accuracy": metrics.get("accuracy", 0.0),
                    "precision": metrics.get("precision", 0.0),
                    "recall": metrics.get("recall", 0.0),
                    "roc_auc": metrics.get("roc_auc", 0.0),
                },
            )

            self._pubsub_queue.put_nowait(
                publish_msg.to_dict()
            )

        return peer, task

    async def replicate_state_to_pupil(self) -> bool:
        pupil_peer = getattr(self, 'pupil_peer', None)
        if not isinstance(pupil_peer, dict):
            return False

        ip = pupil_peer.get('ip')
        port = pupil_peer.get('port')
        pupil_id = pupil_peer.get('id_node')
        if ip is None or port is None:
            return False

        snapshot_id = uuid.uuid4().hex
        msg = {
            "type": MSG_SYNC_STATE,
            "id_node": "server",
            "snapshot_id": snapshot_id,
            "pupil_id": pupil_id,
            "pupil_epoch": self.GlobalTable.pupil_epoch,
            "global_table_snapshot": self.GlobalTable.get_snapshot(),
        }

        reply = await send_once(ip, port, msg, expect_reply=True, timeout=5.0)
        return (
            isinstance(reply, dict)
            and reply.get("type") == MSG_ACK
            and reply.get("ref_type") == MSG_SYNC_STATE
            and str(reply.get("ref_id") or "") == snapshot_id
        )