from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from climate_extremes.modules.precipitation.profiles import (
    DEFAULT_PRECIPITATION_DEFINITIONS,
    accumulation_column_name,
    threshold_column_name,
)


def _normalize_quantiles(quantiles: Iterable[float]) -> tuple[float, ...]:
    normalized = tuple(sorted({float(value) for value in quantiles}))
    if not normalized:
        raise ValueError("At least one precipitation quantile is required.")
    for quantile in normalized:
        if not 0 < quantile < 1:
            raise ValueError("All quantiles must be between 0 and 1.")
    return normalized


def _normalize_windows(accumulation_windows: Iterable[int]) -> tuple[int, ...]:
    normalized = tuple(sorted({int(value) for value in accumulation_windows}))
    if not normalized:
        raise ValueError("At least one precipitation accumulation window is required.")
    for window in normalized:
        if window < 1:
            raise ValueError("Accumulation windows must be positive integers.")
    return normalized


def build_precipitation_climatology_df(
    df: pd.DataFrame,
    quantiles: Iterable[float] = (0.95, 0.99),
    accumulation_windows: Iterable[int] = (1, 3),
    value_column: str = "precipitation_sum",
) -> pd.DataFrame:
    required_columns = {"date", value_column}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing columns for precipitation climatology: {missing}")

    working = df.copy()
    working["date"] = pd.to_datetime(working["date"])
    working = working.sort_values("date").reset_index(drop=True)
    working["day_of_year"] = working["date"].dt.dayofyear

    quantile_values = _normalize_quantiles(quantiles)
    accumulation_days = _normalize_windows(accumulation_windows)

    for window in accumulation_days:
        accumulation_col = accumulation_column_name(window)
        if window == 1:
            working[accumulation_col] = working[value_column]
        else:
            working[accumulation_col] = (
                working[value_column]
                .rolling(window=window, min_periods=window)
                .sum()
            )

    working = working[working["day_of_year"] != 366].copy()

    climatology = working[["day_of_year"]].drop_duplicates().sort_values("day_of_year")
    for window in accumulation_days:
        accumulation_col = accumulation_column_name(window)
        valid = working[["day_of_year", accumulation_col]].dropna()
        grouped = valid.groupby("day_of_year")[accumulation_col]
        for quantile in quantile_values:
            threshold_col = threshold_column_name(window, quantile)
            threshold_df = grouped.quantile(quantile).round(2).reset_index(name=threshold_col)
            climatology = climatology.merge(threshold_df, on="day_of_year", how="left")

    return climatology.reset_index(drop=True)


def build_precipitation_climatology(
    city_name: str,
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    quantiles: Iterable[float] | None = None,
    accumulation_windows: Iterable[int] | None = None,
) -> pd.DataFrame:
    city_key = city_name.lower()
    quantile_values = quantiles or {
        definition.quantile for definition in DEFAULT_PRECIPITATION_DEFINITIONS
    }
    accumulation_days = accumulation_windows or {
        definition.accumulation_days
        for definition in DEFAULT_PRECIPITATION_DEFINITIONS
    }

    if input_path is None:
        input_path = RAW_DATA_DIR / f"{city_key}_precipitation_historical.csv"
    if output_path is None:
        output_path = PROCESSED_DATA_DIR / f"{city_key}_precipitation_climatology.csv"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path, parse_dates=["date"])
    climatology = build_precipitation_climatology_df(
        df=df,
        quantiles=quantile_values,
        accumulation_windows=accumulation_days,
    )
    climatology.to_csv(output_path, index=False)
    print(f"Saved precipitation climatology to {output_path}")
    return climatology
