from __future__ import annotations

import pandas as pd

from climate_extremes.core.cities import get_city_coordinates, get_city_location
from climate_extremes.core.locations import Location
from climate_extremes.modules.heat.workflow import (
    assess_heatwave_risk_with_optional_vulnerability,
    heat_output_paths,
)


def test_heat_output_paths_use_slug_for_arbitrary_location(tmp_path):
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country_code="SE",
        timezone="Europe/Stockholm",
    )

    paths = heat_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )

    assert paths.label == "gothenburg-se"
    assert paths.forecast.name == "gothenburg-se_forecast.csv"
    assert paths.historical.name == "gothenburg-se_historical.csv"
    assert paths.climatology.name == "gothenburg-se_climatology_95p.csv"
    assert paths.detected.name == "gothenburg-se_forecast_with_heatwaves.csv"


def test_heat_output_paths_preserve_demo_city_filenames(tmp_path):
    location = get_city_location("Athens")
    lat, lon = get_city_coordinates("Athens")

    paths = heat_output_paths(
        location,
        raw_data_dir=tmp_path / "raw",
        processed_data_dir=tmp_path / "processed",
    )

    assert (location.latitude, location.longitude) == (lat, lon)
    assert paths.label == "athens"
    assert paths.forecast.name == "athens_forecast.csv"
    assert paths.historical.name == "athens_historical.csv"


def test_missing_vulnerability_data_for_arbitrary_location_does_not_crash():
    detected_df = pd.DataFrame(
        {
            "date": ["2026-07-01"],
            "city": ["gothenburg-se"],
            "tmax": [36.0],
            "heatwave_id": [1],
        }
    )

    assessed = assess_heatwave_risk_with_optional_vulnerability(detected_df)

    assert assessed.loc[0, "risk_level"] == "High"
    assert bool(assessed.loc[0, "high_vulnerability"]) is False
