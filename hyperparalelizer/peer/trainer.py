"""Recebe TrainingTask, o dataset, treina e envia o resultado pro servidor"""

import asyncio
import pickle
import threading
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

from peer.peer_inner_protocol import PupilPromoted

log = get_logger("trainer")


class TrainerNode:
    def __init__(self, node_id, messenger, data_thread, dataset_loader, maekawa_mutex, event_bus: Optional[InternalEventBus] = None):
        """
        event_bus: opcional, publica o ciclo de vida do treino (TrainingStarted, TrainingFinished, TrainingFailed)
        """
        self.node_id = node_id
        self.messenger = messenger
        self.data_thread = data_thread
        self.dataset_loader = dataset_loader
        self.maekawa_mutex = maekawa_mutex
        self.event_bus = event_bus

        self.best_model = None
        self.best_score = -1.0
        
        # Variáveis de Backup (Peer Pupilo)
        self.replica_global_table = {}
        self.replica_queue = []
        self.replica_best_model = {}

    # Receber task

    async def handle_training_task(self, task: Dict[str, Any]) -> None:

        task_id_raw = task.get("task_id")
        if not isinstance(task_id_raw, str) or not task_id_raw:
            log.error(f"[Trainer {self.node_id[:8]}...] task_id ausente ou inválido")
            self._send_result(task, metrics=None, error="invalid_task")
            return

        task_id = task_id_raw

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
            # Retorna as métricas e um booleano dizendo se quebrou o recorde
            metrics, is_new_best = await loop.run_in_executor(None, self._train_task, task, fragment_ids, task_id)
        except Exception as exc:
            log.error(f"[Trainer {self.node_id[:8]}...] erro durante treino: {exc}")
            self._send_result(task, metrics=None, error=str(exc))
            self._emit(TrainingFailed(task_id=task_id, node_id=self.node_id, error=str(exc)))
            return

        # LOGICA DO MAEKAWA: Protege o envio se for um novo melhor modelo
        if is_new_best:
            await self.maekawa_mutex.request_access()
            self._send_result(task, metrics=metrics)
            await self.maekawa_mutex.release_access()
        else:
            self._send_result(task, metrics=metrics)

        self._emit(TrainingFinished(task_id=task_id, node_id=self.node_id, metrics=metrics or {}))

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

        return metrics, is_new_best

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

    # Replicação e Bully (Pupilo)

    def handle_sync_state(self, msg: dict):
        # Guarda o backup passivamente
        self.replica_global_table = msg.get("global_table_snapshot", {})
        self.replica_queue = msg.get("task_queue_snapshot", [])
        self.replica_best_model = msg.get("best_model_metrics", {})
        print(f"[Pupilo {self.node_id}] Estado de backup atualizado.")


    def promote_to_server(self):
        print(f"[Pupilo {self.node_id}] Fui promovido! A assumir o estado do Coordenador...")
        
        # Reconstruir a Tabela com o backup
        tabela_recuperada = GlobalTable()
        tabela_recuperada.nodes = self.replica_global_table
        
        # Instanciar o novo Coordenador com a Tabela em vez da DHT
        novo_coordenador = Coordinator(
            dataset=[], 
            model=None, 
            GlobalTable=tabela_recuperada, 
            model_type="generic"
        )
        
        novo_coordenador.task_pool = self.replica_queue
        novo_coordenador.best_model = self.replica_best_model
        
        if self.event_bus is not None:
            self._emit(PupilPromoted(coordinator=novo_coordenador))