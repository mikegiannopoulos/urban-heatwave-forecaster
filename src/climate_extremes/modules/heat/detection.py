from __future__ import annotations

from pathlib import Path

import pandas as pd

from climate_extremes.core.events import label_consecutive_runs


def _ensure_columns(frame: pd.DataFrame, required: set[str], label: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        missing_str = ", ".join(sorted(missing))
        raise ValueError(f"Missing {label} columns: {missing_str}")


def detect_heatwaves_df(
    forecast_df: pd.DataFrame,
    climatology_df: pd.DataFrame,
    min_run: int = 3,
) -> pd.DataFrame:
    """Return forecast df with heatwave flags using in-memory DataFrames."""
    _ensure_columns(forecast_df, {"date", "tmin", "tmax"}, "forecast")
    _ensure_columns(
        climatology_df,
        {"day_of_year", "tmin_95p", "tmax_95p"},
        "climatology",
    )

    forecast = forecast_df.copy()
    climatology = climatology_df.copy()
    forecast["date"] = pd.to_datetime(forecast["date"])
    forecast["day_of_year"] = forecast["date"].dt.dayofyear
    forecast.loc[forecast["day_of_year"] == 366, "day_of_year"] = 365

    forecast = forecast.merge(
        climatology[["day_of_year", "tmin_95p", "tmax_95p"]],
        on="day_of_year",
        how="left",
        validate="m:1",
    )

    if forecast[["tmin_95p", "tmax_95p"]].isna().any().any():
        raise ValueError(
            "Missing climatology thresholds for one or more forecast days."
        )

    forecast["exceeds_95p"] = (
        (forecast["tmin"] > forecast["tmin_95p"])
        & (forecast["tmax"] > forecast["tmax_95p"])
    )
    forecast["heatwave_id"] = label_consecutive_runs(
        forecast["exceeds_95p"],
        min_run=min_run,
    )
    return forecast.drop(columns=["day_of_year"])


def detect_heatwaves(
    forecast_path: str | Path,
    climatology_path: str | Path,
    min_run: int = 3,
) -> pd.DataFrame:
    """Return forecast df with exceedance and heatwave event columns."""
    forecast = pd.read_csv(forecast_path, parse_dates=["date"])
    climatology = pd.read_csv(climatology_path)
    return detect_heatwaves_df(forecast, climatology, min_run=min_run)
