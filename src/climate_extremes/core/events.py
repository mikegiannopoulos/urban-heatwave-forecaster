from __future__ import annotations

import pandas as pd


def label_consecutive_runs(mask: pd.Series, min_run: int = 3) -> pd.Series:
    """Label consecutive True runs that meet a minimum length."""
    if min_run < 1:
        raise ValueError("min_run must be at least 1.")

    flags = mask.fillna(False).astype(bool)
    if flags.empty:
        return pd.Series(index=mask.index, dtype="Int64")

    groups = flags.ne(flags.shift(fill_value=False)).cumsum()
    run_lengths = flags.groupby(groups).transform("sum")
    run_ids = groups.where(flags & (run_lengths >= min_run))

    if run_ids.notna().any():
        group_ids = pd.Index(run_ids.dropna().unique())
        remapped = {group_id: idx + 1 for idx, group_id in enumerate(group_ids)}
        run_ids = run_ids.map(remapped)

    return run_ids.astype("Int64")
