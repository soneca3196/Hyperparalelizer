from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)

def evaluate(true, pred, prob=None) -> dict:
    import numpy as np

    n_classes = len(np.unique(true))
    # para classes binárias ou múltiplas
    avg = "binary" if n_classes == 2 else "macro"

    metrics = {}

    metrics["accuracy"] = accuracy_score(true, pred)
    metrics["precision"] = precision_score(true, pred, average=avg, zero_division=0)
    metrics["recall"] = recall_score(true, pred, average=avg, zero_division=0)
    metrics["f1"] = f1_score(true, pred, average=avg, zero_division=0)

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