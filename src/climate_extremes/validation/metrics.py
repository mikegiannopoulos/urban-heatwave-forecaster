from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

import pandas as pd


def continuous_metrics(
    predicted: Sequence[object],
    observed: Sequence[object],
) -> dict[str, float | int]:
    """Return paired continuous validation metrics."""
    _ensure_same_length(predicted, observed)
    predicted_values = pd.to_numeric(pd.Series(predicted), errors="coerce")
    observed_values = pd.to_numeric(pd.Series(observed), errors="coerce")
    paired = pd.DataFrame(
        {"predicted": predicted_values, "observed": observed_values}
    ).dropna()
    if paired.empty:
        raise ValueError("No valid paired observations remain.")

    errors = paired["predicted"] - paired["observed"]
    return {
        "n": int(len(paired)),
        "mae": float(errors.abs().mean()),
        "rmse": float(math.sqrt((errors**2).mean())),
        "bias": float(errors.mean()),
    }


def event_detection_metrics(
    predicted_event: Sequence[object],
    observed_event: Sequence[object],
) -> dict[str, float | int]:
    """Return paired binary event-detection validation metrics."""
    _ensure_same_length(predicted_event, observed_event)
    paired = _paired_non_missing(predicted_event, observed_event)
    if paired.empty:
        raise ValueError("No valid paired observations remain.")

    predicted = paired["predicted"].map(_coerce_event_bool)
    observed = paired["observed"].map(_coerce_event_bool)

    hits = int((predicted & observed).sum())
    misses = int((~predicted & observed).sum())
    false_alarms = int((predicted & ~observed).sum())
    correct_negatives = int((~predicted & ~observed).sum())
    precision = _safe_ratio(hits, hits + false_alarms)
    recall = _safe_ratio(hits, hits + misses)
    f1 = _safe_ratio(2 * precision * recall, precision + recall)

    return {
        "n": int(len(paired)),
        "hits": hits,
        "misses": misses,
        "false_alarms": false_alarms,
        "correct_negatives": correct_negatives,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def class_confusion_counts(
    predicted_class: Sequence[object],
    observed_class: Sequence[object],
    class_order: Sequence[object] | None = None,
) -> dict[str, dict[str, int]]:
    """Return observed-by-predicted class-counts as nested plain dictionaries."""
    _ensure_same_length(predicted_class, observed_class)
    paired = _paired_non_missing(predicted_class, observed_class)
    if paired.empty:
        raise ValueError("No valid paired labels remain.")

    paired = paired.astype(str)
    classes = _class_labels(paired, class_order)
    confusion = {
        observed: {predicted: 0 for predicted in classes}
        for observed in classes
    }
    for row in paired.itertuples(index=False):
        observed = str(row.observed)
        predicted = str(row.predicted)
        if observed not in confusion:
            confusion[observed] = {label: 0 for label in classes}
        if predicted not in confusion[observed]:
            for observed_counts in confusion.values():
                observed_counts[predicted] = 0
        confusion[observed][predicted] += 1
    return confusion


def class_agreement(
    predicted_class: Sequence[object],
    observed_class: Sequence[object],
) -> dict[str, float | int]:
    """Return exact class agreement metrics for paired class labels."""
    _ensure_same_length(predicted_class, observed_class)
    paired = _paired_non_missing(predicted_class, observed_class)
    if paired.empty:
        raise ValueError("No valid paired labels remain.")

    exact_matches = int((paired["predicted"].astype(str) == paired["observed"].astype(str)).sum())
    n = int(len(paired))
    return {
        "n": n,
        "exact_matches": exact_matches,
        "accuracy": float(exact_matches / n),
    }


def _ensure_same_length(left: Sequence[object], right: Sequence[object]) -> None:
    if len(left) != len(right):
        raise ValueError("Inputs must have the same length.")


def _paired_non_missing(
    predicted: Sequence[object],
    observed: Sequence[object],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "predicted": pd.Series(predicted, dtype="object"),
            "observed": pd.Series(observed, dtype="object"),
        }
    ).dropna()


def _coerce_event_bool(value: Any) -> bool:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "yes", "y", "1"}:
            return True
        if normalized in {"false", "f", "no", "n", "0", ""}:
            return False
    return bool(value)


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return float(numerator / denominator)


def _class_labels(
    paired: pd.DataFrame,
    class_order: Sequence[object] | None,
) -> list[str]:
    labels: list[str] = []
    for value in class_order or ():
        label = str(value)
        if label not in labels:
            labels.append(label)

    for value in [*paired["observed"].tolist(), *paired["predicted"].tolist()]:
        label = str(value)
        if label not in labels:
            labels.append(label)
    return labels
