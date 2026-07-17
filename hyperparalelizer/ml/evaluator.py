from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)

"""
Calcula métricas de classificação comparando rótulos reais com os preditos.

Detecta automaticamente se o problema é binário ou multiclasse e ajusta
as estratégias de agregação (average) de cada métrica de acordo.

Args:
    true: Rótulos verdadeiros (array-like de inteiros ou strings).
    pred: Rótulos preditos pelo modelo (mesmo formato de `true`).
    prob: Probabilidades de classe retornadas por predict_proba (opcional).
            - Classificação binária: array 1-D com a prob. da classe positiva.
            - Classificação multiclasse: matriz 2-D (n_amostras × n_classes).
            Se None, a métrica ROC-AUC não será calculada.

Returns:
    Dicionário com as seguintes métricas:
        - 'accuracy'  : proporção de predições corretas.
        - 'precision' : precisão média (binary ou macro).
        - 'recall'    : recall médio (binary ou macro).
        - 'f1'        : F1-score médio (binary ou macro).
        - 'roc_auc'   : área sob a curva ROC; None se `prob` não for fornecido
                        ou se o cálculo falhar (e.g., apenas uma classe presente).
"""

def evaluate(true, pred, prob=None) -> dict:
    import numpy as np

    n_classes = len(np.unique(true))
    # para classes binárias ou múltiplas
    avg = "binary" if n_classes == 2 else "macro"

    metrics = {}

    metrics["accuracy"] = accuracy_score(true, pred)
    metrics["precision"] = precision_score(true, pred, average=avg, zero_division=0)
    metrics["recall"] = recall_score(true, pred, average=avg, zero_division=0)

    f1_value = f1_score(true, pred, average=avg, zero_division=0)
    metrics["f1"] = f1_value
    metrics["f1_score"] = f1_value

    if prob is not None:
        try:
            if n_classes == 2:
                metrics["roc_auc"] = roc_auc_score(true, prob)
            else:
                metrics["roc_auc"] = roc_auc_score(
                    true, prob, multi_class="ovr", average="macro"
                )
        except Exception:
            metrics["roc_auc"] = None
    else:
        metrics["roc_auc"] = None

    return metrics