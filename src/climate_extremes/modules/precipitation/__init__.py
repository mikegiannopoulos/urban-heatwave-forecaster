"""Precipitation-extremes workflows and comparison helpers."""

from __future__ import annotations

from climate_extremes.modules.precipitation.baselines import (
    build_precipitation_climatology,
    build_precipitation_climatology_df,
)
from climate_extremes.modules.precipitation.detection import (
    detect_precipitation_events,
    detect_precipitation_events_df,
)
from climate_extremes.modules.precipitation.profiles import (
    DAILY_BURST_95P,
    DAILY_BURST_99P,
    DEFAULT_PRECIPITATION_DEFINITIONS,
    WET_SPELL_3DAY_95P,
    PrecipitationDefinition,
    get_definition_by_name,
)
from climate_extremes.modules.precipitation.risk import (
    assess_precipitation_risk,
    backtest_precipitation_definitions,
    calibrate_precipitation_definition_floor,
    build_precipitation_assessment,
    build_precipitation_assessment_from_frame,
    compare_precipitation_definitions,
)

__all__ = [
    "DAILY_BURST_95P",
    "DAILY_BURST_99P",
    "DEFAULT_PRECIPITATION_DEFINITIONS",
    "WET_SPELL_3DAY_95P",
    "PrecipitationDefinition",
    "assess_precipitation_risk",
    "backtest_precipitation_definitions",
    "calibrate_precipitation_definition_floor",
    "build_precipitation_assessment",
    "build_precipitation_assessment_from_frame",
    "build_precipitation_climatology",
    "build_precipitation_climatology_df",
    "compare_precipitation_definitions",
    "detect_precipitation_events",
    "detect_precipitation_events_df",
    "get_definition_by_name",
]
