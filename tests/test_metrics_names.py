from hyperparalelizer.ml.evaluator import evaluate


def test_evaluate_exposes_f1_alias():
    true = [0, 1, 0, 1]
    pred = [0, 1, 1, 1]
    prob = [0.1, 0.9, 0.8, 0.7]

    metrics = evaluate(true, pred, prob)

    assert "f1" in metrics
    assert metrics["f1"] == metrics["f1_score"]
