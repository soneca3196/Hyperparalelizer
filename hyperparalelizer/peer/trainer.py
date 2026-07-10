"""Recebe TrainingTask, o dataset, treina e envia o resultado pro servidor"""

import asyncio
import pickle
from typing import Any, Dict, List, Optional

from sklearn.model_selection import train_test_split

from ml.models import get_model
from ml.evaluator import evaluate
from utils.logger import get_logger
from peer.peer_inner_protocol import (
    InternalEventBus,
    TrainingStarted,
    TrainingFinished,
    TrainingFailed,
    BestModelUpdatedLocally,
)

log = get_logger("trainer")


class TrainerNode:
    def __init__(self, node_id, messenger, data_thread, dataset_loader, event_bus: Optional[InternalEventBus] = None):
        """
        event_bus: opcional, publica o ciclo de vida do treino (TrainingStarted, TrainingFinished, TrainingFailed)
        """
        self.node_id = node_id
        self.messenger = messenger
        self.data_thread = data_thread
        self.dataset_loader = dataset_loader
        self.event_bus = event_bus

        self.best_model = None
        self.best_score = -1.0

    # Receber task

    async def handle_training_task(self, task: Dict[str, Any]) -> None:
        task_id = task.get("task_id")
        log.info(f"[Trainer {self.node_id[:8]}...] TrainingTask '{task_id}' recebida")

        # garante os fragmentos
        fragment_ids = task.get("dataset_fragmentos") or self._default_fragments()

        ok = await self.data_thread.assemble_many(fragment_ids)
        if not ok:
            log.error(f"[Trainer {self.node_id[:8]}...] dataset indisponível para '{task_id}'")
            self._send_result(task, metrics=None, error="dataset_unavailable")
            self._emit(TrainingFailed(task_id=task_id, node_id=self.node_id, error="dataset_unavailable"))
            return

        await self.data_thread.notify_dataset_ready(fragment_ids[0] if fragment_ids else None)

        # treino bloqueante roda em thread separada, sem travar o event loop
        self._emit(TrainingStarted(task_id=task_id, node_id=self.node_id, fragment_ids=fragment_ids))
        loop = asyncio.get_running_loop()
        try:
            metrics = await loop.run_in_executor(None, self._train_task, task, fragment_ids)
        except Exception as exc:
            log.error(f"[Trainer {self.node_id[:8]}...] erro durante treino: {exc}")
            self._send_result(task, metrics=None, error=str(exc))
            self._emit(TrainingFailed(task_id=task_id, node_id=self.node_id, error=str(exc)))
            return

        self._send_result(task, metrics=metrics)
        self._emit(TrainingFinished(task_id=task_id, node_id=self.node_id, metrics=metrics or {}))

    def _emit(self, event) -> None:
        if self.event_bus is not None:
            self.event_bus.publish(event)

    def _default_fragments(self) -> List[str]:
        """Sem fragmentos explícitos, usa o fragmento atribuído no join"""
        return [self.data_thread.fragment_id] if self.data_thread.fragment_id else []

    # Treinamento (síncrono)

    def _train_task(self, task: Dict[str, Any], fragment_ids: List[str]) -> Dict[str, Any]:
        print(f"[Trainer {self.node_id}] Iniciando treinamento..")

        X, y = self.dataset_loader.load(fragment_ids)

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model = get_model(task["model_type"], task["parametros"])
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)
        metrics = evaluate(y_test, y_pred, y_prob)

        print(f"[Trainer {self.node_id}] Métricas: {metrics}")

        score = metrics["f1"]  # pode ser trocado para ROC AUC
        if score > self.best_score:
            self.best_score = score
            self.best_model = model
            print(f"[Trainer {self.node_id}] Novo melhor modelo!")
            self._emit(
                BestModelUpdatedLocally(
                    task_id=task.get("task_id"), node_id=self.node_id, score=score
                )
            )

        return metrics

    # Enviar resultado

    def _send_result(
        self,
        task: Dict[str, Any],
        metrics: Optional[Dict[str, float]],
        error: Optional[str] = None,
    ) -> None:
        message = {
            "type": "TaskResult",
            "task_id": task.get("task_id"),
            "id_node": self.node_id,
            "accuracy": metrics["accuracy"] if metrics else None,
            "precision": metrics["precision"] if metrics else None,
            "recall": metrics["recall"] if metrics else None,
            "f1_score": metrics["f1"] if metrics else None,
            "roc_auc": metrics["roc_auc"] if metrics else None,
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
            "metricas": {"f1": self.best_score},
        }
        self.messenger.send(message)