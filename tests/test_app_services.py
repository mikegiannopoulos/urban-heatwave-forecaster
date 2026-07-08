from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pandas as pd

from climate_extremes.core.locations import Location
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.modules.precipitation.profiles import WET_SPELL_3DAY_95P


def _write_csv(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_services_module_does_not_import_streamlit():
    sys.modules.pop("streamlit", None)
    importlib.import_module("climate_extremes.app.services")

    assert "streamlit" not in sys.modules


def test_heat_app_workflow_loads_expected_csvs(monkeypatch, tmp_path):
    from climate_extremes.app import services

    location = Location(name="Gothenburg", latitude=57.7089, longitude=11.9746)
    detected_path = _write_csv(
        tmp_path / "detected.csv",
        "date,tmax,exceeds_95p,heatwave_id\n2030-01-01,30,True,1\n",
    )
    risk_path = _write_csv(
        tmp_path / "risk.csv",
        "date,tmax,risk_level\n2030-01-01,30,High\n",
    )
    climatology_path = _write_csv(
        tmp_path / "climatology.csv",
        "day_of_year,tmax_95p\n1,25\n",
    )
    vulnerability_path = _write_csv(
        tmp_path / "vulnerability.csv",
        "city,elderly_percent,green_cover_percent,density_per_km2\n"
        "gothenburg-se,20,30,1000\n",
    )
    result = HazardWorkflowResult(
        hazard="heat",
        location=location,
        output_label="gothenburg-se",
        generated_files={
            "detected": detected_path,
            "risk": risk_path,
            "climatology": climatology_path,
        },
    )

    monkeypatch.setattr(
        services,
        "run_heat_for_location_result",
        lambda received_location, vulnerability_path: result,
    )

    data = services.run_heat_app_workflow(
        location,
        vulnerability_path=vulnerability_path,
    )

    assert data.result == result
    assert data.output_label == "gothenburg-se"
    assert data.climatology_path == climatology_path
    assert data.detected_df.loc[0, "is_hot"] == True
    assert data.risk_df.loc[0, "risk_level"] == "High"
    assert data.vulnerability_df.loc[0, "city"] == "gothenburg-se"


def test_precipitation_app_workflow_loads_expected_csvs(monkeypatch, tmp_path):
    from climate_extremes.app import services

    location = Location(name="Gothenburg", latitude=57.7089, longitude=11.9746)
    detected_path = _write_csv(
        tmp_path / "precip_detected.csv",
        "date,precipitation_sum,precip_accumulation_mm,threshold_mm,precipitation_event_id\n"
        "2030-01-01,40,40,20,1\n",
    )
    risk_path = _write_csv(
        tmp_path / "precip_risk.csv",
        "date,precipitation_sum,precip_accumulation_mm,threshold_mm,precipitation_event_id,"
        "exceedance_ratio,risk_score,risk_level\n"
        "2030-01-01,40,40,20,1,2.0,70,high\n",
    )
    result = HazardWorkflowResult(
        hazard="precipitation",
        location=location,
        output_label="gothenburg-se",
        is_experimental=True,
        generated_files={
            "detected": detected_path,
            "risk": risk_path,
        },
    )

    monkeypatch.setattr(
        services,
        "run_precipitation_for_location_result",
        lambda received_location, definition: result,
    )

    data = services.run_precipitation_app_workflow(
        location,
        definition=WET_SPELL_3DAY_95P,
    )

    assert data.result == result
    assert data.detected_df.loc[0, "precipitation_sum"] == 40
    assert data.risk_df.loc[0, "risk_score"] == 70
    assert data.assessment.hazard == "precipitation"
    assert data.assessment.event_detected is True


def test_prepare_precipitation_app_data_does_not_require_optional_backtest(tmp_path):
    from climate_extremes.app.services import prepare_precipitation_app_data

    detected_path = _write_csv(
        tmp_path / "precip_detected.csv",
        "date,precipitation_sum,precip_accumulation_mm,threshold_mm,precipitation_event_id\n"
        "2030-01-01,1,1,20,\n",
    )
    risk_path = _write_csv(
        tmp_path / "precip_risk.csv",
        "date,precipitation_sum,precip_accumulation_mm,threshold_mm,precipitation_event_id,"
        "exceedance_ratio,risk_score,risk_level\n"
        "2030-01-01,1,1,20,,0.0,0,none\n",
    )
    result = HazardWorkflowResult(
        hazard="precipitation",
        location=Location(name="Gothenburg", latitude=57.7089, longitude=11.9746),
        output_label="gothenburg-se",
        generated_files={"detected": detected_path, "risk": risk_path},
    )

    data = prepare_precipitation_app_data(result, WET_SPELL_3DAY_95P)

    assert data.assessment.event_detected is False
