from __future__ import annotations

import pandas as pd

from climate_extremes.modules.heat.detection import detect_heatwaves_df
from climate_extremes.modules.heat.risk import (
    assess_heatwave_risk,
    build_heat_assessment,
)


def test_detect_heatwaves_df_marks_three_day_heatwave_runs():
    forecast_df = pd.DataFrame(
        {
            "date": pd.date_range("2026-07-01", periods=4, freq="D"),
            "tmin": [25.0, 26.0, 27.0, 20.0],
            "tmax": [37.0, 38.0, 39.0, 28.0],
            "city": ["athens"] * 4,
        }
    )
    climatology_df = pd.DataFrame(
        {
            "day_of_year": [182, 183, 184, 185],
            "tmin_95p": [24.0, 24.0, 24.0, 24.0],
            "tmax_95p": [35.0, 35.0, 35.0, 35.0],
        }
    )

    detected = detect_heatwaves_df(forecast_df, climatology_df, min_run=3)

    assert detected["exceeds_95p"].tolist() == [True, True, True, False]
    assert detected["heatwave_id"].tolist() == [1, 1, 1, pd.NA]


def test_assess_heatwave_risk_escalates_for_high_vulnerability():
    forecast_df = pd.DataFrame(
        {
            "date": ["2026-07-01"],
            "city": ["athens"],
            "tmax": [36.0],
            "heatwave_id": [1],
        }
    )
    vulnerability_df = pd.DataFrame(
        {
            "city": ["athens"],
            "elderly_percent": [21.0],
            "green_cover_percent": [20.0],
            "density_per_km2": [2500.0],
        }
    )

    assessed = assess_heatwave_risk(forecast_df, vulnerability_df)

    assert assessed.loc[0, "risk_level"] == "Extreme"
    assert bool(assessed.loc[0, "high_vulnerability"]) is True


def test_build_heat_assessment_returns_shared_contract():
    risk_df = pd.DataFrame(
        {
            "date": pd.date_range("2026-07-01", periods=3, freq="D"),
            "tmax": [37.0, 38.5, 39.0],
            "tmax_95p": [35.0, 35.0, 35.0],
            "heatwave_id": [1, 1, 1],
            "risk_level": ["High", "Extreme", "Extreme"],
        }
    )

    assessment = build_heat_assessment(risk_df)

    assert assessment.hazard == "heat"
    assert assessment.event_detected is True
    assert assessment.severity_class in {"severe", "extreme"}
    assert assessment.key_metrics["duration_days"] == 3
