from __future__ import annotations

import pandas as pd

from climate_extremes.modules.precipitation import (
    DAILY_BURST_95P,
    DAILY_BURST_99P,
    WET_SPELL_3DAY_95P,
    assess_precipitation_risk,
    backtest_precipitation_definitions,
    calibrate_precipitation_definition_floor,
    build_precipitation_assessment_from_frame,
    build_precipitation_climatology_df,
    compare_precipitation_definitions,
    detect_precipitation_events_df,
)


def _sample_precipitation_forecast() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2026-07-01", periods=4, freq="D"),
            "city": ["athens"] * 4,
            "precipitation_sum": [12.0, 40.0, 18.0, 4.0],
        }
    )


def _sample_precipitation_climatology() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "day_of_year": [182, 183, 184, 185],
            "precipitation_sum_1d_95p": [20.0, 30.0, 20.0, 20.0],
            "precipitation_sum_1d_99p": [30.0, 45.0, 30.0, 30.0],
            "precipitation_sum_3d_95p": [pd.NA, pd.NA, 60.0, 50.0],
        }
    )


def test_build_precipitation_climatology_df_supports_daily_and_rolling_windows():
    historical_df = pd.DataFrame(
        {
            "date": pd.date_range("2020-01-01", periods=5, freq="D"),
            "precipitation_sum": [2.0, 5.0, 11.0, 7.0, 3.0],
        }
    )

    climatology = build_precipitation_climatology_df(
        historical_df,
        quantiles=(0.95,),
        accumulation_windows=(1, 3),
    )

    assert "precipitation_sum_1d_95p" in climatology.columns
    assert "precipitation_sum_3d_95p" in climatology.columns
    assert climatology.loc[climatology["day_of_year"] == 3, "precipitation_sum_3d_95p"].item() == 18.0


def test_detect_precipitation_events_df_distinguishes_burst_and_wet_spell_profiles():
    forecast_df = _sample_precipitation_forecast()
    climatology_df = _sample_precipitation_climatology()

    daily_95 = detect_precipitation_events_df(forecast_df, climatology_df, DAILY_BURST_95P)
    daily_99 = detect_precipitation_events_df(forecast_df, climatology_df, DAILY_BURST_99P)
    wet_spell = detect_precipitation_events_df(forecast_df, climatology_df, WET_SPELL_3DAY_95P)

    assert daily_95["precipitation_event_id"].tolist() == [pd.NA, 1, pd.NA, pd.NA]
    assert daily_99["precipitation_event_id"].tolist() == [pd.NA, pd.NA, pd.NA, pd.NA]
    assert wet_spell["precipitation_event_id"].tolist() == [pd.NA, pd.NA, 1, 1]
    assert float(wet_spell.loc[2, "min_absolute_mm"]) == 40.0


def test_assess_and_summarize_precipitation_definition_returns_shared_contract():
    detected = detect_precipitation_events_df(
        _sample_precipitation_forecast(),
        _sample_precipitation_climatology(),
        WET_SPELL_3DAY_95P,
    )
    assessed = assess_precipitation_risk(detected, WET_SPELL_3DAY_95P)
    assessment = build_precipitation_assessment_from_frame(
        assessed,
        WET_SPELL_3DAY_95P,
    )

    assert assessment.hazard == "precipitation"
    assert assessment.event_detected is True
    assert assessment.key_metrics["definition_name"] == "wet-spell-3day-95p"
    assert assessment.key_metrics["duration_days"] == 2
    assert assessment.metadata["min_absolute_mm"] == 40.0


def test_absolute_floor_filters_dry_percentile_exceedances():
    forecast_df = pd.DataFrame(
        {
            "date": pd.date_range("2026-07-01", periods=3, freq="D"),
            "city": ["athens"] * 3,
            "precipitation_sum": [0.4, 0.6, 0.8],
        }
    )
    climatology_df = pd.DataFrame(
        {
            "day_of_year": [182, 183, 184],
            "precipitation_sum_1d_95p": [0.1, 0.2, 0.3],
            "precipitation_sum_1d_99p": [0.2, 0.3, 0.4],
            "precipitation_sum_3d_95p": [pd.NA, pd.NA, 0.5],
        }
    )

    daily_95 = detect_precipitation_events_df(forecast_df, climatology_df, DAILY_BURST_95P)
    wet_spell = detect_precipitation_events_df(forecast_df, climatology_df, WET_SPELL_3DAY_95P)

    assert daily_95["precipitation_event_id"].isna().all()
    assert wet_spell["precipitation_event_id"].isna().all()


def test_compare_precipitation_definitions_returns_candidate_table():
    comparison = compare_precipitation_definitions(
        _sample_precipitation_forecast(),
        _sample_precipitation_climatology(),
    )

    assert comparison["definition_name"].tolist() == [
        "wet-spell-3day-95p",
        "daily-burst-95p",
        "daily-burst-99p",
    ]
    assert comparison["event_detected"].tolist() == [True, True, False]


def test_backtest_precipitation_definitions_summarizes_historical_event_frequency():
    historical_df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2020-07-01",
                    "2020-07-02",
                    "2020-07-03",
                    "2020-07-04",
                    "2021-07-01",
                    "2021-07-02",
                    "2021-07-03",
                    "2021-07-04",
                ]
            ),
            "city": ["athens"] * 8,
            "precipitation_sum": [5.0, 36.0, 18.0, 2.0, 4.0, 28.0, 12.0, 3.0],
        }
    )
    climatology_df = pd.DataFrame(
        {
            "day_of_year": [183, 184, 185, 186],
            "precipitation_sum_1d_95p": [20.0, 30.0, 20.0, 20.0],
            "precipitation_sum_1d_99p": [25.0, 40.0, 25.0, 25.0],
            "precipitation_sum_3d_95p": [pd.NA, pd.NA, 50.0, 45.0],
        }
    )

    backtest = backtest_precipitation_definitions(historical_df, climatology_df)

    assert backtest.iloc[0]["definition_name"] == "daily-burst-95p"
    assert int(backtest.loc[backtest["definition_name"] == "wet-spell-3day-95p", "event_count"].item()) == 1
    assert int(backtest.loc[backtest["definition_name"] == "wet-spell-3day-95p", "event_days"].item()) == 2
    assert int(backtest.loc[backtest["definition_name"] == "daily-burst-95p", "years_with_events"].item()) == 2


def test_calibrate_precipitation_definition_floor_sweeps_candidate_floors():
    historical_df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2020-07-01",
                    "2020-07-02",
                    "2020-07-03",
                    "2020-07-04",
                    "2021-07-01",
                    "2021-07-02",
                    "2021-07-03",
                    "2021-07-04",
                ]
            ),
            "city": ["athens"] * 8,
            "precipitation_sum": [5.0, 36.0, 18.0, 2.0, 4.0, 28.0, 12.0, 3.0],
        }
    )
    climatology_df = pd.DataFrame(
        {
            "day_of_year": [183, 184, 185, 186],
            "precipitation_sum_1d_95p": [20.0, 30.0, 20.0, 20.0],
            "precipitation_sum_1d_99p": [25.0, 40.0, 25.0, 25.0],
            "precipitation_sum_3d_95p": [pd.NA, pd.NA, 50.0, 45.0],
        }
    )

    calibration = calibrate_precipitation_definition_floor(
        historical_df=historical_df,
        climatology_df=climatology_df,
        base_definition=WET_SPELL_3DAY_95P,
        floor_values_mm=[30, 40, 60],
    )

    assert calibration["min_absolute_mm"].tolist() == [30.0, 40.0, 60.0]
    assert calibration.loc[calibration["min_absolute_mm"] == 30.0, "event_count"].item() >= calibration.loc[
        calibration["min_absolute_mm"] == 60.0, "event_count"
    ].item()
