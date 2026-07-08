from __future__ import annotations

from pathlib import Path

import pandas as pd

from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR


def build_daily_percentile_climatology(
    df: pd.DataFrame,
    value_columns: tuple[str, ...] = ("tmin", "tmax"),
    quantile: float = 0.95,
) -> pd.DataFrame:
    if not 0 < quantile < 1:
        raise ValueError("quantile must be between 0 and 1.")

    missing_columns = {"date", *value_columns} - set(df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing columns for climatology: {missing}")

    working = df.copy()
    working["date"] = pd.to_datetime(working["date"])
    working["day_of_year"] = working["date"].dt.dayofyear
    working = working[working["day_of_year"] != 366]

    aggregation = {
        column: (lambda series: round(series.quantile(quantile), 2))
        for column in value_columns
    }
    climatology = working.groupby("day_of_year").agg(aggregation).reset_index()
    suffix = f"_{int(quantile * 100)}p"
    return climatology.rename(
        columns={column: f"{column}{suffix}" for column in value_columns}
    )


def build_percentile_climatology(
    city_name: str,
    input_path: str | Path | None = None,
    output_path: str | Path | None = None,
    value_columns: tuple[str, ...] = ("tmin", "tmax"),
    quantile: float = 0.95,
) -> pd.DataFrame:
    city_key = city_name.lower()

    if input_path is None:
        input_path = RAW_DATA_DIR / f"{city_key}_historical.csv"
    if output_path is None:
        output_path = PROCESSED_DATA_DIR / f"{city_key}_climatology_{int(quantile * 100)}p.csv"

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path, parse_dates=["date"])
    climatology = build_daily_percentile_climatology(
        df=df,
        value_columns=value_columns,
        quantile=quantile,
    )
    climatology.to_csv(output_path, index=False)
    print(f"Saved percentile climatology to {output_path}")
    return climatology
