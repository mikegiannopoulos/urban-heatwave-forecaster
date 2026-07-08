from __future__ import annotations

import math

import pandas as pd
import pytest

from climate_extremes.validation.metrics import (
    class_agreement,
    class_confusion_counts,
    continuous_metrics,
    event_detection_metrics,
)


def test_continuous_metrics_with_perfect_predictions():
    metrics = continuous_metrics([1.0, 2.0, 3.0], [1.0, 2.0, 3.0])

    assert metrics == {"n": 3, "mae": 0.0, "rmse": 0.0, "bias": 0.0}


def test_continuous_metrics_with_biased_predictions():
    metrics = continuous_metrics([2.0, 4.0, 6.0], [1.0, 2.0, 3.0])

    assert metrics["n"] == 3
    assert metrics["mae"] == 2.0
    assert metrics["rmse"] == pytest.approx(math.sqrt(14 / 3))
    assert metrics["bias"] == 2.0


def test_continuous_metrics_drops_nan_pairs():
    metrics = continuous_metrics(
        pd.Series([1.0, None, "bad", 4.0]),
        pd.Series([1.5, 2.0, 3.0, None]),
    )

    assert metrics == {"n": 1, "mae": 0.5, "rmse": 0.5, "bias": -0.5}


def test_continuous_metrics_length_mismatch_raises_value_error():
    with pytest.raises(ValueError, match="same length"):
        continuous_metrics([1.0, 2.0], [1.0])


def test_continuous_metrics_all_invalid_raises_value_error():
    with pytest.raises(ValueError, match="No valid paired observations"):
        continuous_metrics([None, "bad"], [1.0, None])


def test_event_detection_metrics_counts_known_outcomes():
    metrics = event_detection_metrics(
        [True, False, True, False, True],
        [True, True, False, False, True],
    )

    assert metrics["n"] == 5
    assert metrics["hits"] == 2
    assert metrics["misses"] == 1
    assert metrics["false_alarms"] == 1
    assert metrics["correct_negatives"] == 1
    assert metrics["precision"] == pytest.approx(2 / 3)
    assert metrics["recall"] == pytest.approx(2 / 3)
    assert metrics["f1"] == pytest.approx(2 / 3)


def test_event_detection_metrics_zero_division_returns_zero():
    metrics = event_detection_metrics([False, False], [False, False])

    assert metrics["hits"] == 0
    assert metrics["false_alarms"] == 0
    assert metrics["misses"] == 0
    assert metrics["precision"] == 0.0
    assert metrics["recall"] == 0.0
    assert metrics["f1"] == 0.0


def test_event_detection_metrics_drops_missing_pairs():
    metrics = event_detection_metrics([True, None, "false"], [True, True, "true"])

    assert metrics["n"] == 2
    assert metrics["hits"] == 1
    assert metrics["misses"] == 1


def test_event_detection_metrics_length_mismatch_raises_value_error():
    with pytest.raises(ValueError, match="same length"):
        event_detection_metrics([True, False], [True])


def test_event_detection_metrics_all_missing_raises_value_error():
    with pytest.raises(ValueError, match="No valid paired observations"):
        event_detection_metrics([None, pd.NA], [True, None])


def test_class_agreement_perfect_and_partial_cases():
    perfect = class_agreement(["none", "low"], ["none", "low"])
    partial = class_agreement(["none", "low", "high"], ["none", "high", "high"])

    assert perfect == {"n": 2, "exact_matches": 2, "accuracy": 1.0}
    assert partial["n"] == 3
    assert partial["exact_matches"] == 2
    assert partial["accuracy"] == pytest.approx(2 / 3)


def test_class_confusion_counts_with_known_class_labels():
    confusion = class_confusion_counts(
        predicted_class=["none", "low", "low", "high"],
        observed_class=["none", "none", "low", "high"],
    )

    assert confusion == {
        "none": {"none": 1, "low": 1, "high": 0},
        "low": {"none": 0, "low": 1, "high": 0},
        "high": {"none": 0, "low": 0, "high": 1},
    }


def test_class_confusion_counts_respects_class_order():
    confusion = class_confusion_counts(
        predicted_class=["low", "high"],
        observed_class=["high", "low"],
        class_order=["none", "low", "high"],
    )

    assert list(confusion) == ["none", "low", "high"]
    assert list(confusion["none"]) == ["none", "low", "high"]
    assert confusion["none"] == {"none": 0, "low": 0, "high": 0}
    assert confusion["low"]["high"] == 1
    assert confusion["high"]["low"] == 1


def test_class_metrics_drop_missing_labels():
    agreement = class_agreement(["none", None, "high"], ["none", "low", pd.NA])
    confusion = class_confusion_counts(["none", None, "high"], ["none", "low", pd.NA])

    assert agreement == {"n": 1, "exact_matches": 1, "accuracy": 1.0}
    assert confusion == {"none": {"none": 1}}


def test_class_metrics_length_mismatch_raises_value_error():
    with pytest.raises(ValueError, match="same length"):
        class_agreement(["none", "low"], ["none"])
    with pytest.raises(ValueError, match="same length"):
        class_confusion_counts(["none", "low"], ["none"])


def test_all_missing_class_labels_raise_value_error():
    with pytest.raises(ValueError, match="No valid paired labels"):
        class_agreement([None, pd.NA], [pd.NA, None])
    with pytest.raises(ValueError, match="No valid paired labels"):
        class_confusion_counts([None, pd.NA], [pd.NA, None])
