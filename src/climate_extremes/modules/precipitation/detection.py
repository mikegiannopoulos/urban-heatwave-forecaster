from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

from climate_extremes.core.events import label_consecutive_runs
from climate_extremes.modules.precipitation.profiles import (
    DEFAULT_PRECIPITATION_DEFINITIONS,
    PrecipitationDefinition,
    accumulation_column_name,
    get_definition_by_name,
    threshold_column_name,
)


def _coerce_definition(
    definition: PrecipitationDefinition | str,
) -> PrecipitationDefinition:
    if isinstance(definition, PrecipitationDefinition):
        return definition
    return get_definition_by_name(definition)


def _prepare_accumulation_columns(
    forecast_df: pd.DataFrame,
    accumulation_windows: Iterable[int],
) -> pd.DataFrame:
    prepared = forecast_df.copy()
    prepared["date"] = pd.to_datetime(prepared["date"])
    prepared = prepared.sort_values("date").reset_index(drop=True)
    prepared["day_of_year"] = prepared["date"].dt.dayofyear
    prepared.loc[prepared["day_of_year"] == 366, "day_of_year"] = 365

    for window in sorted({int(value) for value in accumulation_windows}):
        accumulation_col = accumulation_column_name(window)
        if window == 1:
            prepared[accumulation_col] = prepared["precipitation_sum"]
        else:
            prepared[accumulation_col] = (
                prepared["precipitation_sum"]
                .rolling(window=window, min_periods=window)
                .sum()
            )
    return prepared


def detect_precipitation_events_df(
    forecast_df: pd.DataFrame,
    climatology_df: pd.DataFrame,
    definition: PrecipitationDefinition | str = DEFAULT_PRECIPITATION_DEFINITIONS[0],
) -> pd.DataFrame:
    required_forecast_columns = {"date", "precipitation_sum"}
    required_climatology_columns = {"day_of_year"}
    missing_forecast = required_forecast_columns - set(forecast_df.columns)
    missing_climatology = required_climatology_columns - set(climatology_df.columns)
    if missing_forecast:
        missing = ", ".join(sorted(missing_forecast))
        raise ValueError(f"Missing forecast columns for precipitation detection: {missing}")
    if missing_climatology:
        missing = ", ".join(sorted(missing_climatology))
        raise ValueError(
            f"Missing climatology columns for precipitation detection: {missing}"
        )

    definition_obj = _coerce_definition(definition)
    threshold_col = threshold_column_name(
        definition_obj.accumulation_days,
        definition_obj.quantile,
    )
    if threshold_col not in climatology_df.columns:
        raise ValueError(
            f"Missing climatology threshold column '{threshold_col}' for precipitation detection."
        )

    prepared = _prepare_accumulation_columns(
        forecast_df,
        accumulation_windows=(definition_obj.accumulation_days,),
    )
    accumulation_col = accumulation_column_name(definition_obj.accumulation_days)
    detected = prepared.merge(
        climatology_df[["day_of_year", threshold_col]],
        on="day_of_year",
        how="left",
        validate="m:1",
    )
    detected = detected.rename(
        columns={
            accumulation_col: "precip_accumulation_mm",
            threshold_col: "threshold_mm",
        }
    )
    accumulation = pd.to_numeric(
        detected["precip_accumulation_mm"],
        errors="coerce",
    )
    threshold = pd.to_numeric(
        detected["threshold_mm"],
        errors="coerce",
    )
    detected["exceeds_threshold"] = (
        accumulation.notna()
        & threshold.notna()
        & (accumulation > threshold)
        & (accumulation >= definition_obj.min_absolute_mm)
    )
    detected["precipitation_event_id"] = label_consecutive_runs(
        detected["exceeds_threshold"],
        min_run=definition_obj.min_run,
    )
    detected["definition_name"] = definition_obj.name
    detected["accumulation_days"] = definition_obj.accumulation_days
    detected["min_absolute_mm"] = definition_obj.min_absolute_mm
    return detected.drop(columns=["day_of_year"])


def detect_precipitation_events(
    forecast_path: str | Path,
    climatology_path: str | Path,
    definition: PrecipitationDefinition | str = DEFAULT_PRECIPITATION_DEFINITIONS[0],
) -> pd.DataFrame:
    forecast = pd.read_csv(forecast_path, parse_dates=["date"])
    climatology = pd.read_csv(climatology_path)
    return detect_precipitation_events_df(
        forecast_df=forecast,
        climatology_df=climatology,
        definition=definition,
    )
