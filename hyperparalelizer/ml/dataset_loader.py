"""
dataset_loader.py - Carrega fragmentos de dataset já montados em disco.
"""

import hashlib
import os
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import List, Set, Tuple

import numpy as np

from utils.logger import get_logger

log = get_logger("dataset_loader")

DATASET_NAME = "sklearn.load_breast_cancer"
EXPECTED_SAMPLE_COUNT = 569


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
    """Carrega o dataset de referência do projeto (breast cancer) e sua identidade."""
    from sklearn.datasets import load_breast_cancer

    dataset = load_breast_cancer()
    X = dataset.data
    y = dataset.target
    identity = build_dataset_identity(DATASET_NAME, X, y)
    if identity.sample_count != EXPECTED_SAMPLE_COUNT:
        raise DatasetValidationError(
            f"Dataset de referência inesperado: {identity.sample_count} amostras"
        )
    return X, y, identity


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


class ValidatingDatasetLoader(DatasetLoader):
    """DatasetLoader com validação de amostras e hash da execução atual.

    Garante que os fragmentos montados localmente pertencem à mesma execução
    (mesmo dataset, mesmo run) antes de liberá-los para treino, evitando que
    fragmentos de uma execução anterior corrompam o resultado.
    """

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