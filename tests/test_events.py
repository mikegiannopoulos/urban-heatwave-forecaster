from __future__ import annotations

import pandas as pd

from climate_extremes.core.events import label_consecutive_runs


def test_label_consecutive_runs_only_labels_runs_meeting_minimum_length():
    mask = pd.Series([True, True, False, True, True, True, False])

    run_ids = label_consecutive_runs(mask, min_run=3)

    assert run_ids.tolist() == [pd.NA, pd.NA, pd.NA, 1, 1, 1, pd.NA]
