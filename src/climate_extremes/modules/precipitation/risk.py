from __future__ import annotations

from typing import Iterable

import pandas as pd

from climate_extremes.core.schemas import HazardAssessment
from climate_extremes.core.scales import severity_from_score
from climate_extremes.modules.precipitation.detection import detect_precipitation_events_df
from climate_extremes.modules.precipitation.profiles import (
    DEFAULT_PRECIPITATION_DEFINITIONS,
    PrecipitationDefinition,
    get_definition_by_name,
)


def _coerce_definition(
    definition: PrecipitationDefinition | str,
) -> PrecipitationDefinition:
    if isinstance(definition, PrecipitationDefinition):
        return definition
    return get_definition_by_name(definition)


def assess_precipitation_risk(
    detected_df: pd.DataFrame,
    definition: PrecipitationDefinition | str,
) -> pd.DataFrame:
    required_columns = {
        "precipitation_sum",
        "precip_accumulation_mm",
        "threshold_mm",
        "precipitation_event_id",
    }
    missing_columns = required_columns - set(detected_df.columns)
    if missing_columns:
        missing = ", ".join(sorted(missing_columns))
        raise ValueError(f"Missing columns for precipitation risk assessment: {missing}")

    definition_obj = _coerce_definition(definition)
    assessed = detected_df.copy()

    threshold = assessed["threshold_mm"].where(assessed["threshold_mm"] > 0)
    assessed["exceedance_ratio"] = (assessed["precip_accumulation_mm"] / threshold).fillna(0.0)
    assessed["exceedance_amount_mm"] = (
        assessed["precip_accumulation_mm"] - assessed["threshold_mm"]
    ).fillna(0.0)

    assessed["risk_score"] = 0.0
    active_mask = assessed["precipitation_event_id"].notna()
    ratio_component = (
        (assessed["exceedance_ratio"] - 1.0).clip(lower=0.0) * 60.0
    ).clip(upper=60.0)
    daily_component = (assessed["precipitation_sum"].clip(lower=0.0) / 4.0).clip(
        upper=20.0
    )
    accumulation_component = min(
        max(definition_obj.accumulation_days - 1, 0) * 5.0,
        10.0,
    )
    assessed.loc[active_mask, "risk_score"] = (
        ratio_component.loc[active_mask] + daily_component.loc[active_mask] + accumulation_component
    ).clip(upper=100.0)
    assessed["risk_level"] = assessed["risk_score"].apply(severity_from_score)
    return assessed


def build_precipitation_assessment(
    event_detected: bool,
    severity_score: float,
    severity_class: str,
    key_metrics: dict[str, object] | None = None,
    confidence: str = "medium",
    metadata: dict[str, object] | None = None,
) -> HazardAssessment:
    return HazardAssessment(
        hazard="precipitation",
        event_detected=event_detected,
        severity_score=severity_score,
        severity_class=severity_class,
        confidence=confidence,
        key_metrics=key_metrics or {},
        metadata=metadata or {},
    )


def build_precipitation_assessment_from_frame(
    assessed_df: pd.DataFrame,
    definition: PrecipitationDefinition | str,
) -> HazardAssessment:
    definition_obj = _coerce_definition(definition)
    if "precipitation_event_id" not in assessed_df.columns:
        raise ValueError(
            "Precipitation assessment requires a 'precipitation_event_id' column."
        )

    working = assessed_df.copy()
    active_mask = working["precipitation_event_id"].notna()
    if not active_mask.any():
        return build_precipitation_assessment(
            event_detected=False,
            severity_score=0.0,
            severity_class="none",
            confidence="medium",
            key_metrics={
                "definition_name": definition_obj.name,
                "duration_days": 0,
                "peak_daily_total_mm": 0.0,
                "peak_accumulation_mm": 0.0,
                "peak_exceedance_ratio": 0.0,
            },
            metadata={
                "quantile": definition_obj.quantile,
                "accumulation_days": definition_obj.accumulation_days,
                "min_absolute_mm": definition_obj.min_absolute_mm,
            },
        )

    active = working.loc[active_mask].copy()
    longest_run = int(active.groupby("precipitation_event_id").size().max())
    peak_daily_total = float(active["precipitation_sum"].max())
    peak_accumulation = float(active["precip_accumulation_mm"].max())
    peak_exceedance_ratio = float(active["exceedance_ratio"].max())

    exceedance_component = min(max(peak_exceedance_ratio - 1.0, 0.0) * 60.0, 60.0)
    daily_component = min(max(peak_daily_total, 0.0) / 4.0, 20.0)
    duration_component = min(max(longest_run - 1, 0) * 8.0, 16.0)
    accumulation_component = min(
        max(definition_obj.accumulation_days - 1, 0) * 5.0,
        10.0,
    )
    severity_score = min(
        exceedance_component + daily_component + duration_component + accumulation_component,
        100.0,
    )
    confidence = (
        "high"
        if peak_exceedance_ratio >= 1.5
        or peak_daily_total >= 50.0
        or definition_obj.quantile >= 0.99
        else "medium"
    )

    return build_precipitation_assessment(
        event_detected=True,
        severity_score=severity_score,
        severity_class=severity_from_score(severity_score),
        confidence=confidence,
        key_metrics={
            "definition_name": definition_obj.name,
            "duration_days": longest_run,
            "peak_daily_total_mm": round(peak_daily_total, 2),
            "peak_accumulation_mm": round(peak_accumulation, 2),
            "peak_exceedance_ratio": round(peak_exceedance_ratio, 2),
        },
        metadata={
            "quantile": definition_obj.quantile,
            "accumulation_days": definition_obj.accumulation_days,
            "min_absolute_mm": definition_obj.min_absolute_mm,
        },
    )


def compare_precipitation_definitions(
    forecast_df: pd.DataFrame,
    climatology_df: pd.DataFrame,
    definitions: Iterable[PrecipitationDefinition] = DEFAULT_PRECIPITATION_DEFINITIONS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for definition in definitions:
        detected = detect_precipitation_events_df(
            forecast_df=forecast_df,
            climatology_df=climatology_df,
            definition=definition,
        )
        assessed = assess_precipitation_risk(detected, definition=definition)
        summary = build_precipitation_assessment_from_frame(
            assessed,
            definition=definition,
        )
        rows.append(
            {
                "definition_name": definition.name,
                "label": definition.label,
                "description": definition.description,
                "event_detected": summary.event_detected,
                "severity_score": summary.severity_score,
                "severity_class": summary.severity_class,
                "confidence": summary.confidence,
                "accumulation_days": definition.accumulation_days,
                "quantile": definition.quantile,
                **summary.key_metrics,
            }
        )

    comparison = pd.DataFrame(rows)
    if comparison.empty:
        return comparison
    return comparison.sort_values(
        ["event_detected", "severity_score", "quantile"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def backtest_precipitation_definitions(
    historical_df: pd.DataFrame,
    climatology_df: pd.DataFrame,
    definitions: Iterable[PrecipitationDefinition] = DEFAULT_PRECIPITATION_DEFINITIONS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    historical = historical_df.copy()
    historical["date"] = pd.to_datetime(historical["date"])

    for definition in definitions:
        detected = detect_precipitation_events_df(
            forecast_df=historical,
            climatology_df=climatology_df,
            definition=definition,
        )
        assessed = assess_precipitation_risk(detected, definition=definition)
        active = assessed.loc[assessed["precipitation_event_id"].notna()].copy()

        years = historical["date"].dt.year.nunique()
        if active.empty:
            rows.append(
                {
                    "definition_name": definition.name,
                    "label": definition.label,
                    "description": definition.description,
                "quantile": definition.quantile,
                "accumulation_days": definition.accumulation_days,
                "min_absolute_mm": definition.min_absolute_mm,
                "event_count": 0,
                    "event_days": 0,
                    "years_with_events": 0,
                    "mean_events_per_year": 0.0,
                    "mean_event_days_per_year": 0.0,
                    "max_event_duration_days": 0,
                    "max_event_severity_score": 0.0,
                    "peak_daily_total_mm": 0.0,
                    "peak_accumulation_mm": 0.0,
                    "peak_exceedance_ratio": 0.0,
                    "top_event_start_date": None,
                    "top_event_end_date": None,
                }
            )
            continue

        event_groups = active.groupby("precipitation_event_id", dropna=True)
        event_summaries: list[dict[str, object]] = []
        for event_id, event_frame in event_groups:
            assessment = build_precipitation_assessment_from_frame(
                event_frame,
                definition=definition,
            )
            event_summaries.append(
                {
                    "event_id": int(event_id),
                    "start_date": event_frame["date"].min(),
                    "end_date": event_frame["date"].max(),
                    "duration_days": int(len(event_frame)),
                    "severity_score": float(assessment.severity_score),
                    "peak_daily_total_mm": float(event_frame["precipitation_sum"].max()),
                    "peak_accumulation_mm": float(
                        event_frame["precip_accumulation_mm"].max()
                    ),
                    "peak_exceedance_ratio": float(event_frame["exceedance_ratio"].max()),
                }
            )

        event_summary_df = pd.DataFrame(event_summaries).sort_values(
            ["severity_score", "duration_days", "peak_daily_total_mm"],
            ascending=[False, False, False],
        )
        top_event = event_summary_df.iloc[0]

        years_with_events = active["date"].dt.year.nunique()
        rows.append(
            {
                "definition_name": definition.name,
                "label": definition.label,
                "description": definition.description,
                "quantile": definition.quantile,
                "accumulation_days": definition.accumulation_days,
                "min_absolute_mm": definition.min_absolute_mm,
                "event_count": int(event_summary_df.shape[0]),
                "event_days": int(active.shape[0]),
                "years_with_events": int(years_with_events),
                "mean_events_per_year": round(event_summary_df.shape[0] / years, 2),
                "mean_event_days_per_year": round(active.shape[0] / years, 2),
                "max_event_duration_days": int(event_summary_df["duration_days"].max()),
                "max_event_severity_score": round(
                    float(event_summary_df["severity_score"].max()),
                    2,
                ),
                "peak_daily_total_mm": round(
                    float(event_summary_df["peak_daily_total_mm"].max()),
                    2,
                ),
                "peak_accumulation_mm": round(
                    float(event_summary_df["peak_accumulation_mm"].max()),
                    2,
                ),
                "peak_exceedance_ratio": round(
                    float(event_summary_df["peak_exceedance_ratio"].max()),
                    2,
                ),
                "top_event_start_date": top_event["start_date"].date().isoformat(),
                "top_event_end_date": top_event["end_date"].date().isoformat(),
            }
        )

    backtest = pd.DataFrame(rows)
    if backtest.empty:
        return backtest
    return backtest.sort_values(
        ["event_count", "mean_events_per_year", "max_event_severity_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def calibrate_precipitation_definition_floor(
    historical_df: pd.DataFrame,
    climatology_df: pd.DataFrame,
    base_definition: PrecipitationDefinition | str,
    floor_values_mm: Iterable[float],
) -> pd.DataFrame:
    definition_obj = _coerce_definition(base_definition)
    calibration_definitions = [
        definition_obj.with_floor(min_absolute_mm=value)
        for value in floor_values_mm
    ]
    calibration = backtest_precipitation_definitions(
        historical_df=historical_df,
        climatology_df=climatology_df,
        definitions=calibration_definitions,
    )
    if calibration.empty:
        return calibration
    return calibration.sort_values(
        ["mean_events_per_year", "event_count", "min_absolute_mm"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
