from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from climate_extremes.core.cities import get_city_location, normalize_city_name
from climate_extremes.core.locations import Location, location_slug
from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.io.openmeteo import (
    fetch_historical_precipitation_data_for_location,
    fetch_precipitation_forecast_for_location,
)
from climate_extremes.modules.precipitation.baselines import (
    build_precipitation_climatology_df,
)
from climate_extremes.modules.precipitation.detection import (
    detect_precipitation_events_df,
)
from climate_extremes.modules.precipitation.profiles import (
    DEFAULT_PRECIPITATION_DEFINITIONS,
    PrecipitationDefinition,
    get_definition_by_name,
)
from climate_extremes.modules.precipitation.risk import (
    assess_precipitation_risk,
    backtest_precipitation_definitions,
)


@dataclass(frozen=True)
class PrecipitationOutputPaths:
    label: str
    definition_slug: str
    historical: Path
    forecast: Path
    climatology: Path
    detected: Path
    risk: Path
    backtest_summary: Path
    floor_calibration: Path


def precipitation_definition_slug(definition_name: str) -> str:
    return definition_name.replace("-", "_")


def precipitation_output_label(
    location: Location,
    *,
    preserve_demo_city_names: bool = True,
) -> str:
    """Return the output label for precipitation files, preserving demo-city names."""
    if preserve_demo_city_names:
        try:
            demo_key = normalize_city_name(location.name)
            demo_location = get_city_location(demo_key)
        except KeyError:
            pass
        else:
            country_matches = (
                not location.country_code
                or not demo_location.country_code
                or location.country_code.upper() == demo_location.country_code.upper()
            )
            coordinates_match = (
                abs(location.latitude - demo_location.latitude) <= 0.1
                and abs(location.longitude - demo_location.longitude) <= 0.1
            )
            if country_matches and coordinates_match:
                return demo_key

    return location_slug(location)


def precipitation_output_paths(
    location: Location,
    *,
    definition: PrecipitationDefinition | str = DEFAULT_PRECIPITATION_DEFINITIONS[0],
    preserve_demo_city_names: bool = True,
    raw_data_dir: Path = RAW_DATA_DIR,
    processed_data_dir: Path = PROCESSED_DATA_DIR,
) -> PrecipitationOutputPaths:
    """Return standard precipitation workflow paths for a Location."""
    definition_obj = (
        definition if isinstance(definition, PrecipitationDefinition)
        else get_definition_by_name(definition)
    )
    label = precipitation_output_label(
        location,
        preserve_demo_city_names=preserve_demo_city_names,
    )
    definition_slug = precipitation_definition_slug(definition_obj.name)
    return PrecipitationOutputPaths(
        label=label,
        definition_slug=definition_slug,
        historical=raw_data_dir / f"{label}_precipitation_historical.csv",
        forecast=raw_data_dir / f"{label}_precipitation_forecast.csv",
        climatology=processed_data_dir / f"{label}_precipitation_climatology.csv",
        detected=processed_data_dir / f"{label}_precipitation_{definition_slug}_detected.csv",
        risk=processed_data_dir / f"{label}_precipitation_{definition_slug}_risk.csv",
        backtest_summary=processed_data_dir / f"{label}_precipitation_backtest_summary.csv",
        floor_calibration=processed_data_dir / f"{label}_{definition_slug}_floor_calibration.csv",
    )


def _precipitation_generated_files(paths: PrecipitationOutputPaths) -> dict[str, Path]:
    return {
        "historical": paths.historical,
        "forecast": paths.forecast,
        "climatology": paths.climatology,
        "detected": paths.detected,
        "risk": paths.risk,
        "backtest_summary": paths.backtest_summary,
    }


def _precipitation_summary(
    historical_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    detected_df: pd.DataFrame,
    assessed_df: pd.DataFrame,
    backtest_df: pd.DataFrame,
    definition: PrecipitationDefinition,
) -> dict[str, object]:
    summary: dict[str, object] = {
        "definition_name": definition.name,
        "historical_rows": int(len(historical_df)),
        "forecast_days": int(len(forecast_df)),
        "detected_rows": int(len(detected_df)),
        "risk_rows": int(len(assessed_df)),
        "backtest_rows": int(len(backtest_df)),
    }
    if "precipitation_event_id" in detected_df.columns:
        summary["event_days"] = int(detected_df["precipitation_event_id"].notna().sum())
    if "risk_score" in assessed_df.columns and not assessed_df["risk_score"].dropna().empty:
        summary["max_risk_score"] = float(assessed_df["risk_score"].max())
    return summary


def run_precipitation_for_location_result(
    location: Location,
    *,
    definition: PrecipitationDefinition | str = DEFAULT_PRECIPITATION_DEFINITIONS[0],
    paths: PrecipitationOutputPaths | None = None,
) -> HazardWorkflowResult:
    """Run the experimental precipitation workflow and return result metadata."""
    definition_obj = (
        definition if isinstance(definition, PrecipitationDefinition)
        else get_definition_by_name(definition)
    )
    output_paths = paths or precipitation_output_paths(location, definition=definition_obj)

    historical_df = fetch_historical_precipitation_data_for_location(
        location,
        save_path=output_paths.historical,
        output_label=output_paths.label,
    )

    climatology = build_precipitation_climatology_df(historical_df)
    output_paths.climatology.parent.mkdir(parents=True, exist_ok=True)
    climatology.to_csv(output_paths.climatology, index=False)

    forecast_df = fetch_precipitation_forecast_for_location(
        location,
        save_path=output_paths.forecast,
        output_label=output_paths.label,
    )

    detected = detect_precipitation_events_df(
        forecast_df=forecast_df,
        climatology_df=climatology,
        definition=definition_obj,
    )
    output_paths.detected.parent.mkdir(parents=True, exist_ok=True)
    detected.to_csv(output_paths.detected, index=False)

    assessed = assess_precipitation_risk(detected, definition=definition_obj)
    output_paths.risk.parent.mkdir(parents=True, exist_ok=True)
    assessed.to_csv(output_paths.risk, index=False)

    backtest = backtest_precipitation_definitions(
        historical_df=historical_df,
        climatology_df=climatology,
    )
    output_paths.backtest_summary.parent.mkdir(parents=True, exist_ok=True)
    backtest.to_csv(output_paths.backtest_summary, index=False)

    return HazardWorkflowResult(
        hazard="precipitation",
        location=location,
        output_label=output_paths.label,
        is_experimental=True,
        generated_files=_precipitation_generated_files(output_paths),
        summary=_precipitation_summary(
            historical_df,
            forecast_df,
            detected,
            assessed,
            backtest,
            definition_obj,
        ),
        warnings=[
            "Precipitation definitions are experimental/candidate and not yet fully validated.",
            "Floor calibration is not run by this workflow; calibration output path is reserved for candidate tuning.",
        ],
    )


def run_precipitation_for_location(
    location: Location,
    *,
    definition: PrecipitationDefinition | str = DEFAULT_PRECIPITATION_DEFINITIONS[0],
    paths: PrecipitationOutputPaths | None = None,
) -> PrecipitationOutputPaths:
    """Run the experimental precipitation backend workflow for a Location."""
    result = run_precipitation_for_location_result(
        location,
        definition=definition,
        paths=paths,
    )
    if paths is not None:
        return paths
    return precipitation_output_paths(
        location,
        definition=str(
            result.summary.get("definition_name", DEFAULT_PRECIPITATION_DEFINITIONS[0].name)
        ),
    )
