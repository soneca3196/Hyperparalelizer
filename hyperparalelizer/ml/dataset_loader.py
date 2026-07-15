"""
dataset_loader.py - Carrega fragmentos de dataset já montados em disco.
"""

import os
import pickle
from typing import List, Tuple

import numpy as np

from utils.logger import get_logger

log = get_logger("dataset_loader")


class DatasetLoader:
    """Monta (X, y) a partir de um ou mais fragmentos já salvos em disco."""

    def __init__(self, storage_dir: str):
        self.storage_dir = storage_dir

    def _fragment_path(self, fragment_id: str) -> str:
        return os.path.join(self.storage_dir, f"{fragment_id}.bin")

    def _load_one(self, fragment_id: str) -> Tuple[np.ndarray, np.ndarray]:
        path = self._fragment_path(fragment_id)

        if not os.path.exists(path):
            raise FileNotFoundError(
                f"DatasetLoader: fragmento '{fragment_id}' não encontrado em "
                f"'{path}'. Ele precisa ter sido montado via DataThread "
                f"(assemble_dataset_for/assemble_many) antes do treino."
            )

        with open(path, "rb") as f:
            raw = f.read()

        try:
            payload = pickle.loads(raw)
        except Exception as exc:
            raise ValueError(
                f"DatasetLoader: não foi possível deserializar o fragmento "
                f"'{fragment_id}' — bytes corrompidos ou em formato "
                f"inesperado."
            ) from exc

        if not isinstance(payload, dict) or "X" not in payload or "y" not in payload:
            raise ValueError(
                f"DatasetLoader: fragmento '{fragment_id}' não está no "
                f"formato esperado {{'X': ..., 'y': ...}}."
            )

        X = np.asarray(payload["X"])
        y = np.asarray(payload["y"])

        if len(X) != len(y):
            raise ValueError(
                f"DatasetLoader: fragmento '{fragment_id}' tem X e y de "
                f"tamanhos diferentes ({len(X)} != {len(y)})."
            )

        return X, y

    def load(self, fragment_ids: List[str]) -> Tuple[np.ndarray, np.ndarray]:
        """FileNotFoundError/ValueError se algum fragmento estiver
        ausente ou corrompido
        """
        if not fragment_ids:
            raise ValueError("DatasetLoader.load: lista de fragment_ids vazia.")

        X_parts = []
        y_parts = []

        for fragment_id in fragment_ids:
            X_frag, y_frag = self._load_one(fragment_id)
            X_parts.append(X_frag)
            y_parts.append(y_frag)

        X = np.concatenate(X_parts, axis=0) if len(X_parts) > 1 else X_parts[0]
        y = np.concatenate(y_parts, axis=0) if len(y_parts) > 1 else y_parts[0]

        log.debug(
            f"DatasetLoader: carregado {len(fragment_ids)} fragmento(s), "
            f"{len(X)} amostras no total"
        )

        return X, y
