from __future__ import annotations

import pandas as pd

from climate_extremes.core.cities import get_city_location
from climate_extremes.core.locations import Location
from climate_extremes.modules.precipitation.profiles import DEFAULT_PRECIPITATION_DEFINITIONS
from climate_extremes.modules.precipitation import workflow


def test_precipitation_output_paths_use_slug_for_arbitrary_location(tmp_path):
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country_code="SE",
        timezone="Europe/Stockholm",
    )

    paths = workflow.precipitation_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )

    assert paths.label == "gothenburg-se"
    assert paths.forecast.name == "gothenburg-se_precipitation_forecast.csv"
    assert paths.historical.name == "gothenburg-se_precipitation_historical.csv"
    assert paths.climatology.name == "gothenburg-se_precipitation_climatology.csv"
    assert paths.detected.name == "gothenburg-se_precipitation_daily_burst_95p_detected.csv"
    assert paths.risk.name == "gothenburg-se_precipitation_daily_burst_95p_risk.csv"
    assert paths.backtest_summary.name == "gothenburg-se_precipitation_backtest_summary.csv"
    assert paths.floor_calibration.name == "gothenburg-se_daily_burst_95p_floor_calibration.csv"


def test_precipitation_output_paths_preserve_demo_city_filenames(tmp_path):
    location = get_city_location("Athens")

    paths = workflow.precipitation_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )

    assert paths.label == "athens"
    assert paths.forecast.name == "athens_precipitation_forecast.csv"
    assert paths.historical.name == "athens_precipitation_historical.csv"
    assert paths.climatology.name == "athens_precipitation_climatology.csv"


def test_run_precipitation_for_location_can_be_invoked_with_mocked_steps(
    monkeypatch,
    tmp_path,
):
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country_code="SE",
        timezone="Europe/Stockholm",
    )
    paths = workflow.precipitation_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )
    historical = pd.DataFrame(
        {
            "date": pd.date_range("2001-01-01", periods=3, freq="D"),
            "precipitation_sum": [1.0, 2.0, 3.0],
            "city": ["gothenburg-se"] * 3,
        }
    )
    climatology = pd.DataFrame(
        {
            "day_of_year": [1, 2, 3],
            "precip_1day_95p": [10.0, 10.0, 10.0],
            "precip_1day_99p": [15.0, 15.0, 15.0],
            "precip_3day_95p": [20.0, 20.0, 20.0],
        }
    )
    forecast = pd.DataFrame(
        {
            "date": pd.date_range("2030-01-01", periods=3, freq="D"),
            "precipitation_sum": [5.0, 40.0, 6.0],
            "city": ["gothenburg-se"] * 3,
        }
    )
    detected = forecast.assign(
        precip_accumulation_mm=[5.0, 40.0, 6.0],
        threshold_mm=[10.0, 10.0, 10.0],
        exceeds_threshold=[False, True, False],
        precipitation_event_id=[pd.NA, 1, pd.NA],
        definition_name=DEFAULT_PRECIPITATION_DEFINITIONS[0].name,
    )
    assessed = detected.assign(risk_score=[0.0, 30.0, 0.0], risk_level=["none", "mild", "none"])
    backtest = pd.DataFrame({"definition_name": ["daily-burst-95p"], "event_count": [0]})

    monkeypatch.setattr(
        workflow,
        "fetch_historical_precipitation_data_for_location",
        lambda *args, **kwargs: historical,
    )
    monkeypatch.setattr(
        workflow,
        "fetch_precipitation_forecast_for_location",
        lambda *args, **kwargs: forecast,
    )
    monkeypatch.setattr(
        workflow,
        "build_precipitation_climatology_df",
        lambda *args, **kwargs: climatology,
    )
    monkeypatch.setattr(
        workflow,
        "detect_precipitation_events_df",
        lambda *args, **kwargs: detected,
    )
    monkeypatch.setattr(
        workflow,
        "assess_precipitation_risk",
        lambda *args, **kwargs: assessed,
    )
    monkeypatch.setattr(
        workflow,
        "backtest_precipitation_definitions",
        lambda *args, **kwargs: backtest,
    )

    returned_paths = workflow.run_precipitation_for_location(location, paths=paths)

    assert returned_paths == paths
    assert paths.climatology.exists()
    assert paths.detected.exists()
    assert paths.risk.exists()
    assert paths.backtest_summary.exists()
