"""Recebe TrainingTask, o dataset, treina e envia o resultado pro servidor"""

import asyncio
import pickle
from typing import Any, Dict, List, Optional

from sklearn.model_selection import train_test_split

from hyperparalelizer.ml.models import get_model
from hyperparalelizer.ml.evaluator import evaluate
from utils.logger import get_logger
from hyperparalelizer.peer.peer_inner_protocol import (
    InternalEventBus,
    TrainingStarted,
    TrainingFinished,
    TrainingFailed,
    BestModelUpdatedLocally,
)

from hyperparalelizer.server.coordinator import Coordinator
from hyperparalelizer.global_table import GlobalTable

from hyperparalelizer.peer.peer_inner_protocol import PupilPromoted

log = get_logger("trainer")


class TrainerNode:
    def __init__(self, node_id, messenger, data_thread, dataset_loader, maekawa_mutex, event_bus: Optional[InternalEventBus] = None, run_id: str = ""):
        """
        event_bus: opcional, publica o ciclo de vida do treino (TrainingStarted, TrainingFinished, TrainingFailed)
        """
        self.node_id = node_id
        self.messenger = messenger
        self.data_thread = data_thread
        self.dataset_loader = dataset_loader
        self.maekawa_mutex = maekawa_mutex
        self.event_bus = event_bus

        self.run_id = run_id or ""

        self.best_model = None
        self.best_score = -1.0
        self.busy = False

        self.replica_global_table_snapshot: dict = {}

        self.is_pupil = False
        self.pupil_epoch = 0

        self._training_lock = asyncio.Lock()
        self._current_task_id: Optional[str] = None
        self._processed_task_ids: set = set()

    async def try_submit_task(self, task: Dict[str, Any]) -> bool:
        """Aceita a task apenas se o peer estiver livre e ela ainda não tiver
        sido processada. Retorna False para PEER_BUSY / duplicata."""
        task_id = str(task.get("task_id") or "")
        if not task_id:
            return False
        task_run_id = task.get("run_id") or ""
        if self.run_id and task_run_id and task_run_id != self.run_id:
            log.warning(
                f"[Trainer {self.node_id[:8]}...] TrainingTask '{task_id}' "
                f"rejeitada: run_id divergente ({task_run_id} != {self.run_id})"
            )
            return False
        if task_id in self._processed_task_ids:
            return False
        if self._training_lock.locked():
            return False

        asyncio.create_task(self._run_guarded(task))
        return True

    async def _run_guarded(self, task: Dict[str, Any]) -> None:
        async with self._training_lock:
            task_id = str(task.get("task_id") or "")
            self._current_task_id = task_id
            try:
                await self.handle_training_task(task)
                self._processed_task_ids.add(task_id)
            finally:
                self._current_task_id = None


    def handle_global_best_score(self, payload: Dict[str, Any]) -> None:
        try:
            score = float(payload.get("f1_score", -1.0))
        except (TypeError, ValueError):
            return
        if score > self.best_score:
            self.best_score = score
            log.info(
                f"[Trainer {self.node_id[:8]}...] score global atualizado via Pub/Sub: {score:.4f}"
            )

    # Receber task

    async def handle_training_task(self, task: Dict[str, Any]) -> None:

        task_id_raw = task.get("task_id")
        if not isinstance(task_id_raw, str) or not task_id_raw:
            log.error(f"[Trainer {self.node_id[:8]}...] task_id ausente ou inválido")
            self._send_result(task, metrics=None, model_bytes=None, error="invalid_task")
            return

        task_id = task_id_raw

        log.info(f"[Trainer {self.node_id[:8]}...] TrainingTask '{task_id}' recebida")

        self.busy = True
        try:
            await self._run_training_task(task, task_id)
        finally:
            self.busy = False

    async def _run_training_task(self, task: Dict[str, Any], task_id: str) -> None:
        result_sent = False
        try:
            fragment_ids = task.get("dataset_fragmentos") or self._default_fragments()

            ok = await self.data_thread.assemble_many(fragment_ids)
            if not ok:
                log.error(f"[Trainer {self.node_id[:8]}...] dataset indisponível para '{task_id}'")
                self._send_result(task, metrics=None, model_bytes=None, error="dataset_unavailable")
                result_sent = True
                self._emit(TrainingFailed(task_id=task_id, node_id=self.node_id, error="dataset_unavailable"))
                return

            await self.data_thread.notify_dataset_ready(fragment_ids[0] if fragment_ids else None)

            # treino bloqueante roda em thread separada, sem travar o event loop
            self._emit(TrainingStarted(task_id=task_id, node_id=self.node_id, fragment_ids=fragment_ids))
            loop = asyncio.get_running_loop()

            metrics, is_new_best, model_bytes = await loop.run_in_executor(
                None, self._train_task, task, fragment_ids, task_id
            )

            # LOGICA DO MAEKAWA: Protege o envio se for um novo melhor modelo
            if is_new_best:
                await self.maekawa_mutex.request_access()
                self._send_result(task, metrics=metrics, model_bytes=model_bytes)
                result_sent = True
                await self.maekawa_mutex.release_access()
            else:
                self._send_result(task, metrics=metrics, model_bytes=model_bytes)
                result_sent = True

            self._emit(TrainingFinished(task_id=task_id, node_id=self.node_id, metrics=metrics or {}))

        except Exception as exc:
            log.error(f"[Trainer {self.node_id[:8]}...] erro durante treino: {exc}")
            if not result_sent:
                self._send_result(task, metrics=None, model_bytes=None, error=str(exc))
                result_sent = True
            self._emit(TrainingFailed(task_id=task_id, node_id=self.node_id, error=str(exc)))

        finally:
            if getattr(self.maekawa_mutex, "state", None) == "HELD":
                try:
                    await self.maekawa_mutex.release_access()
                except Exception:
                    pass

    def _emit(self, event) -> None:
        if self.event_bus is not None:
            self.event_bus.publish(event)

    def _default_fragments(self) -> List[str]:
        """Sem fragmentos explícitos, usa o fragmento atribuído no join"""
        return [self.data_thread.fragment_id] if self.data_thread.fragment_id else []

    # Treinamento (síncrono)

    def _train_task(self, task: Dict[str, Any], fragment_ids: List[str], task_id: str) -> tuple:
        print(f"[Trainer {self.node_id}] Iniciando treinamento..")

        X, y = self.dataset_loader.load(fragment_ids)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = get_model(task["model_type"], task["parametros"])
        model.fit(X_train, y_train)
        serialized_model = pickle.dumps(model)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)
        metrics = evaluate(y_test, y_pred, y_prob)

        print(f"[Trainer {self.node_id}] Métricas: {metrics}")

        score = metrics["f1"]  # pode ser trocado para ROC AUC
        is_new_best = False
        
        if score > self.best_score:
            self.best_score = score
            self.best_model = model
            is_new_best = True
            print(f"[Trainer {self.node_id}] Novo melhor modelo!")
            self._emit(
                BestModelUpdatedLocally(
                    task_id=task_id, node_id=self.node_id, score=score
                )
            )

        return metrics, is_new_best, serialized_model

    # Enviar resultado

    def _send_result(self, task: Dict[str, Any], metrics: Optional[Dict[str, float]], model_bytes: Optional[bytes] = None, error: Optional[str] = None,) -> None:
        message = {
            "type": "TaskResult",
            "task_id": task.get("task_id"),
            "run_id": task.get("run_id") or self.run_id,
            "status": "success" if error is None else "failed",
            "id_node": self.node_id,
            "accuracy": metrics["accuracy"] if metrics else None,
            "precision": metrics["precision"] if metrics else None,
            "recall": metrics["recall"] if metrics else None,
            "f1_score": metrics["f1"] if metrics else None,
            "roc_auc": metrics["roc_auc"] if metrics else None,
            "model_bytes": model_bytes,
            "error": error,
        }
        self.messenger.send(message)

    # Request best model

    async def handle_request_best_model(self) -> None:
        print(f"[Trainer {self.node_id}] Enviando melhor modelo..")

        if self.best_model is None:
            print("Nenhum modelo treinado ainda")
            return

        serialized_model = pickle.dumps(self.best_model)

        message = {
            "type": "SendBestModel",
            "id_node": self.node_id,
            "model_bytes": serialized_model,
            "metricas": {"f1_score": self.best_score},
        }

        self.messenger.send(message)

    # Replicação e Bully (Pupilo)

    def handle_sync_state(self, msg: dict):

        self.replica_global_table_snapshot = msg.get("global_table_snapshot", {})
        print(f"[Pupilo {self.node_id}] Estado de backup atualizado.")

    def promote_to_server(self) -> "Coordinator":
        if not self.is_pupil:
            raise RuntimeError("Peer não é o Pupilo ativo")
        print(f"[Pupilo {self.node_id}] Fui promovido! A assumir o estado do Coordenador...")
        tabela_recuperada = GlobalTable(snapshot=self.replica_global_table_snapshot)

        novo_coordenador = Coordinator(
            dataset=[],
            model=None,
            global_table=tabela_recuperada,
            model_type="generic",
            run_id=self.run_id,
        )

        # A GlobalTable restaurada carrega o pupil_id/pupil_epoch de antes da
        # queda (preservados em get_snapshot/overwrite_from_snapshot), mas o
        # PupilManager novo nasce zerado (epoch=0, pupil_id=None). Sem esta
        # sincronização, o primeiro reconcile() do novo coordenador reatribuiria
        # epoch=1 a partir do zero, e peers que já viram uma epoch maior
        # rejeitariam o SyncState como STALE_SYNC_STATE indefinidamente.
        novo_coordenador.pupil_manager._pupil_id = tabela_recuperada.pupil_id
        novo_coordenador.pupil_manager._epoch = tabela_recuperada.pupil_epoch
        if tabela_recuperada.pupil_id:
            pupil_node = tabela_recuperada.get_node(tabela_recuperada.pupil_id)
            if pupil_node is not None:
                novo_coordenador.pupil_peer = {
                    "id_node": pupil_node.get("node_id"),
                    "ip": pupil_node.get("ip"),
                    "port": pupil_node.get("port"),
                }

        if self.event_bus is not None:
            self._emit(PupilPromoted(coordinator=novo_coordenador))

        return novo_coordenador