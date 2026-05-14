from __future__ import annotations

import pandas as pd

from climate_extremes.core.locations import Location
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.modules.heat import workflow as heat_workflow
from climate_extremes.modules.precipitation import workflow as precipitation_workflow


def test_hazard_workflow_result_helpers(tmp_path):
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country_code="SE",
    )
    output = tmp_path / "gothenburg-se_forecast.csv"
    output.write_text("date,tmax\n")

    result = HazardWorkflowResult(
        hazard="heat",
        location=location,
        output_label="gothenburg-se",
        generated_files={"forecast": output, "risk": tmp_path / "missing.csv"},
        summary={"forecast_days": 1},
        warnings=["No vulnerability data."],
    )

    assert result.generated_file_names() == {
        "forecast": "gothenburg-se_forecast.csv",
        "risk": "missing.csv",
    }
    assert result.existing_generated_files() == {"forecast": output}
    assert "heat: completed" in result.short_summary()
    assert result.as_dict()["generated_files"]["forecast"] == str(output)
    assert result.as_dict()["location"]["name"] == "Gothenburg"


def test_heat_workflow_result_metadata_with_missing_vulnerability(monkeypatch, tmp_path):
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country_code="SE",
    )
    paths = heat_workflow.heat_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )
    historical = pd.DataFrame(
        {
            "date": pd.date_range("2001-01-01", periods=2, freq="D"),
            "tmin": [10.0, 11.0],
            "tmax": [20.0, 21.0],
            "city": ["gothenburg-se"] * 2,
        }
    )
    climatology = pd.DataFrame(
        {
            "day_of_year": [1, 2],
            "tmin_95p": [15.0, 15.0],
            "tmax_95p": [25.0, 25.0],
        }
    )
    forecast = pd.DataFrame(
        {
            "date": pd.date_range("2030-01-01", periods=2, freq="D"),
            "tmin": [16.0, 17.0],
            "tmax": [30.0, 31.0],
            "city": ["gothenburg-se"] * 2,
        }
    )
    detected = forecast.assign(
        tmin_95p=[15.0, 15.0],
        tmax_95p=[25.0, 25.0],
        exceeds_95p=[True, True],
        heatwave_id=[1, 1],
    )
    risk = detected.assign(risk_level=["Mild", "Moderate"], high_vulnerability=[False, False])

    monkeypatch.setattr(
        heat_workflow,
        "fetch_historical_temperature_data_for_location",
        lambda *args, **kwargs: historical,
    )
    monkeypatch.setattr(
        heat_workflow,
        "build_daily_percentile_climatology",
        lambda *args, **kwargs: climatology,
    )
    monkeypatch.setattr(
        heat_workflow,
        "fetch_ecmwf_forecast_for_location",
        lambda *args, **kwargs: forecast,
    )
    monkeypatch.setattr(
        heat_workflow,
        "detect_heatwaves_df",
        lambda *args, **kwargs: detected,
    )
    monkeypatch.setattr(
        heat_workflow,
        "assess_heatwave_risk_with_optional_vulnerability",
        lambda *args, **kwargs: risk,
    )

    result = heat_workflow.run_heat_for_location_result(
        location,
        paths=paths,
        vulnerability_path=tmp_path / "missing_vulnerability.csv",
    )

    assert result.hazard == "heat"
    assert result.output_label == "gothenburg-se"
    assert result.generated_files["risk"] == paths.risk
    assert result.summary["heatwave_days"] == 2
    assert result.warnings
    assert "vulnerability" in result.warnings[0]


def test_precipitation_workflow_result_metadata(monkeypatch, tmp_path):
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country_code="SE",
    )
    paths = precipitation_workflow.precipitation_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )
    historical = pd.DataFrame(
        {
            "date": pd.date_range("2001-01-01", periods=2, freq="D"),
            "precipitation_sum": [1.0, 2.0],
            "city": ["gothenburg-se"] * 2,
        }
    )
    climatology = pd.DataFrame(
        {
            "day_of_year": [1, 2],
            "precipitation_sum_1d_95p": [10.0, 10.0],
            "precipitation_sum_1d_99p": [15.0, 15.0],
            "precipitation_sum_3d_95p": [20.0, 20.0],
        }
    )
    forecast = pd.DataFrame(
        {
            "date": pd.date_range("2030-01-01", periods=2, freq="D"),
            "precipitation_sum": [30.0, 1.0],
            "city": ["gothenburg-se"] * 2,
        }
    )
    detected = forecast.assign(
        precip_accumulation_mm=[30.0, 1.0],
        threshold_mm=[10.0, 10.0],
        precipitation_event_id=[1, pd.NA],
    )
    assessed = detected.assign(risk_score=[25.0, 0.0], risk_level=["moderate", "none"])
    backtest = pd.DataFrame({"definition_name": ["daily-burst-95p"], "event_count": [0]})

    monkeypatch.setattr(
        precipitation_workflow,
        "fetch_historical_precipitation_data_for_location",
        lambda *args, **kwargs: historical,
    )
    monkeypatch.setattr(
        precipitation_workflow,
        "build_precipitation_climatology_df",
        lambda *args, **kwargs: climatology,
    )
    monkeypatch.setattr(
        precipitation_workflow,
        "fetch_precipitation_forecast_for_location",
        lambda *args, **kwargs: forecast,
    )
    monkeypatch.setattr(
        precipitation_workflow,
        "detect_precipitation_events_df",
        lambda *args, **kwargs: detected,
    )
    monkeypatch.setattr(
        precipitation_workflow,
        "assess_precipitation_risk",
        lambda *args, **kwargs: assessed,
    )
    monkeypatch.setattr(
        precipitation_workflow,
        "backtest_precipitation_definitions",
        lambda *args, **kwargs: backtest,
    )

    result = precipitation_workflow.run_precipitation_for_location_result(
        location,
        paths=paths,
    )

    assert result.hazard == "precipitation"
    assert result.is_experimental is True
    assert result.output_label == "gothenburg-se"
    assert result.generated_files["backtest_summary"] == paths.backtest_summary
    assert result.summary["event_days"] == 1
    assert result.summary["definition_name"] == "daily-burst-95p"
    assert result.warnings
