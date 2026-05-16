import threading
import pickle

from sklearn.model_selection import train_test_split

from ml.models import get_model
from ml.evaluator import evaluate

class TrainerNode:
    def __init__(self, node_id, messenger, dataset_loader):
        self.node_id = node_id
        self.messenger = messenger
        self.dataset_loader = dataset_loader

        self.best_model = None
        self.best_score = -1

    # =============================
    # 📥 RECEBER TASK
    # =============================
    def handle_training_task(self, task: dict):
        """
        task esperado:
        {
            "dataset": ...,
            "params": {...},
            "model": "mlp"
        }
        """

        thread = threading.Thread(target=self._train_task, args=(task,))
        thread.start()

    # =============================
    # 🧠 TREINAMENTO
    # =============================
    def _train_task(self, task):
        print(f"[Trainer {self.node_id}] Iniciando treinamento...")

        # 🔹 1. Carregar dataset
        X, y = self.dataset_loader.load(task["dataset"])

        # 🔹 2. Split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # 🔹 3. Criar modelo
        model = get_model(task["model"], task["params"])

        # 🔹 4. Treinar
        model.fit(X_train, y_train)

        # 🔹 5. Predições
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)

        # 🔹 6. Avaliar
        metrics = evaluate(y_test, y_pred, y_prob)

        print(f"[Trainer {self.node_id}] Métricas: {metrics}")

        # 🔹 7. Atualizar melhor modelo local
        score = metrics["f1"]  # você pode mudar para ROC AUC

        if score > self.best_score:
            self.best_score = score
            self.best_model = model
            print(f"[Trainer {self.node_id}] Novo melhor modelo!")

        # 🔹 8. Enviar resultado
        self._send_result(metrics)

    # =============================
    # 📤 ENVIAR RESULTADO
    # =============================
    def _send_result(self, metrics):
        message = {
            "type": "TaskResult",
            "id_node": self.node_id,
            "accuracy": metrics["accuracy"],
            "precision": metrics["precision"],
            "recall": metrics["recall"],
            "f1_score": metrics["f1"],
            "roc_auc": metrics["roc_auc"],
        }

        self.messenger.send(message)

    # =============================
    # 🧾 REQUEST BEST MODEL
    # =============================
    def handle_request_best_model(self):
        print(f"[Trainer {self.node_id}] Enviando melhor modelo...")

        if self.best_model is None:
            print("Nenhum modelo treinado ainda.")
            return

        serialized_model = pickle.dumps(self.best_model)

        message = {
            "type": "SendBestModel",
            "id_node": self.node_id,
            "model": serialized_model,
        }

        self.messenger.send(message)