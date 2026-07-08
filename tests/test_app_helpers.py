from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from climate_extremes.app.location_selection import (
    ADVANCED_DEMO_SECTION_LABEL,
    COORDINATES_MODE,
    DEFAULT_LOCATION_MODE,
    GLOBAL_SEARCH_MODE,
    MAIN_LOCATION_MODES,
    filter_locations_by_country_code,
    format_candidate_label,
    format_location_details,
    format_location_label,
    is_location_selection_complete,
    validate_coordinate_location_input,
)
from climate_extremes.app.result_loading import (
    csv_path_from_result,
    generated_files_summary,
    load_result_csvs,
    read_generated_csv,
    vulnerability_unavailable,
)
from climate_extremes.core.locations import Location
from climate_extremes.core.results import HazardWorkflowResult


def test_main_location_workflow_defaults_to_global_search_without_demo_mode():
    assert DEFAULT_LOCATION_MODE == GLOBAL_SEARCH_MODE
    assert MAIN_LOCATION_MODES == (GLOBAL_SEARCH_MODE, COORDINATES_MODE)
    assert "Demo preset" not in MAIN_LOCATION_MODES
    assert ADVANCED_DEMO_SECTION_LABEL == "Advanced demo tools"


def test_location_label_formatting_includes_admin_country_and_coordinates():
    location = Location(
        name="Gothenburg",
        latitude=57.7089,
        longitude=11.9746,
        country="Sweden",
        country_code="SE",
        admin1="Västra Götaland",
        timezone="Europe/Stockholm",
    )

    assert format_location_label(location) == "Gothenburg, Västra Götaland, Sweden"
    assert format_location_details(location) == "57.7089, 11.9746 · Europe/Stockholm"
    assert (
        format_candidate_label(location)
        == "Gothenburg, Västra Götaland, Sweden (SE) · 57.7089, 11.9746"
    )


def test_country_code_filter_and_selection_completion():
    locations = [
        Location(name="Paris", latitude=48.8567, longitude=2.3522, country_code="FR"),
        Location(name="Paris", latitude=33.6609, longitude=-95.5555, country_code="US"),
    ]

    assert filter_locations_by_country_code(locations, "fr") == [locations[0]]
    assert filter_locations_by_country_code(locations, "") == locations
    assert is_location_selection_complete(locations[0]) is True
    assert is_location_selection_complete(None) is False


def test_validate_coordinate_location_input_returns_location_and_rejects_bad_values():
    location = validate_coordinate_location_input(
        name=" Gothenburg ",
        latitude=57.7089,
        longitude=11.9746,
        country_code="se",
        timezone=" Europe/Stockholm ",
    )

    assert location.name == "Gothenburg"
    assert location.country_code == "SE"
    assert location.timezone == "Europe/Stockholm"

    with pytest.raises(ValueError, match="Latitude"):
        validate_coordinate_location_input("Bad", 91.0, 0.0)
    with pytest.raises(ValueError, match="Longitude"):
        validate_coordinate_location_input("Bad", 0.0, 181.0)
    with pytest.raises(ValueError, match="Country code"):
        validate_coordinate_location_input("Bad", 0.0, 0.0, country_code="SWE")


def test_result_file_summary_and_missing_file_handling(tmp_path):
    forecast_path = tmp_path / "forecast.csv"
    forecast_path.write_text("date,tmax\n2030-01-01,30\n")
    result = HazardWorkflowResult(
        hazard="heat",
        location=Location(name="Gothenburg", latitude=57.7089, longitude=11.9746),
        output_label="gothenburg-se",
        generated_files={
            "forecast": forecast_path,
            "risk": tmp_path / "missing.csv",
        },
    )

    assert generated_files_summary(result)[0] == f"Forecast: `{forecast_path}`"
    assert csv_path_from_result(result, "forecast") == forecast_path
    assert csv_path_from_result(result, "optional", required=False) is None
    with pytest.raises(FileNotFoundError, match="missing"):
        csv_path_from_result(result, "risk")
    with pytest.raises(FileNotFoundError, match="did not report"):
        csv_path_from_result(result, "historical")


def test_read_generated_csv_and_load_result_csvs(tmp_path):
    forecast_path = tmp_path / "forecast.csv"
    risk_path = tmp_path / "risk.csv"
    forecast_path.write_text("date,tmax\n2030-01-01,30\n")
    risk_path.write_text("date,risk_level\n2030-01-01,High\n")
    result = HazardWorkflowResult(
        hazard="heat",
        location=Location(name="Gothenburg", latitude=57.7089, longitude=11.9746),
        output_label="gothenburg-se",
        generated_files={"forecast": forecast_path, "risk": risk_path},
    )

    forecast_df = read_generated_csv(result, "forecast", parse_dates=["date"])
    loaded = load_result_csvs(result, ["forecast", "risk"], parse_dates=["date"])

    assert forecast_df.loc[0, "tmax"] == 30
    assert set(loaded) == {"forecast", "risk"}
    assert pd.api.types.is_datetime64_any_dtype(loaded["risk"]["date"])


def test_vulnerability_unavailable_detects_missing_rows():
    vulnerability_df = pd.DataFrame(
        {
            "city": ["athens"],
            "elderly_percent": [21.0],
            "green_cover_percent": [20.0],
            "density_per_km2": [2500.0],
        }
    )

    assert vulnerability_unavailable(vulnerability_df, "athens") is False
    assert vulnerability_unavailable(vulnerability_df, "gothenburg-se") is True
