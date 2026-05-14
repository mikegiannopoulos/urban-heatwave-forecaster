from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from climate_extremes.baselines.percentiles import build_daily_percentile_climatology
from climate_extremes.core.cities import get_city_location, normalize_city_name
from climate_extremes.core.locations import Location, location_slug
from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.io.openmeteo import (
    fetch_ecmwf_forecast_for_location,
    fetch_historical_temperature_data_for_location,
)
from climate_extremes.modules.heat.detection import detect_heatwaves_df
from climate_extremes.modules.heat.risk import assess_heatwave_risk


@dataclass(frozen=True)
class HeatOutputPaths:
    label: str
    historical: Path
    forecast: Path
    climatology: Path
    detected: Path
    risk: Path


def heat_output_label(
    location: Location,
    *,
    preserve_demo_city_names: bool = True,
) -> str:
    """Return the output label for heat files, preserving demo-city names."""
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


def heat_output_paths(
    location: Location,
    *,
    preserve_demo_city_names: bool = True,
    raw_data_dir: Path = RAW_DATA_DIR,
    processed_data_dir: Path = PROCESSED_DATA_DIR,
) -> HeatOutputPaths:
    """Return standard heat workflow paths for a Location."""
    label = heat_output_label(
        location,
        preserve_demo_city_names=preserve_demo_city_names,
    )
    return HeatOutputPaths(
        label=label,
        historical=raw_data_dir / f"{label}_historical.csv",
        forecast=raw_data_dir / f"{label}_forecast.csv",
        climatology=processed_data_dir / f"{label}_climatology_95p.csv",
        detected=processed_data_dir / f"{label}_forecast_with_heatwaves.csv",
        risk=processed_data_dir / f"{label}_heatwave_risk.csv",
    )


def _empty_vulnerability_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "city",
            "elderly_percent",
            "green_cover_percent",
            "density_per_km2",
        ]
    )


def assess_heatwave_risk_with_optional_vulnerability(
    detected_df: pd.DataFrame,
    vulnerability_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Assess heat risk without inventing vulnerability data for unknown places."""
    return assess_heatwave_risk(
        detected_df,
        vulnerability_df if vulnerability_df is not None else _empty_vulnerability_frame(),
    )


def _heat_generated_files(paths: HeatOutputPaths) -> dict[str, Path]:
    return {
        "historical": paths.historical,
        "forecast": paths.forecast,
        "climatology": paths.climatology,
        "detected": paths.detected,
        "risk": paths.risk,
    }


def _heat_summary(
    historical_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    detected_df: pd.DataFrame,
    risk_df: pd.DataFrame,
) -> dict[str, object]:
    summary: dict[str, object] = {
        "historical_rows": int(len(historical_df)),
        "forecast_days": int(len(forecast_df)),
        "detected_rows": int(len(detected_df)),
        "risk_rows": int(len(risk_df)),
    }
    if "heatwave_id" in detected_df.columns:
        summary["heatwave_days"] = int(detected_df["heatwave_id"].notna().sum())
    if "risk_level" in risk_df.columns and not risk_df["risk_level"].dropna().empty:
        summary["max_risk_level"] = str(risk_df["risk_level"].dropna().iloc[-1])
    return summary


def _heat_vulnerability_warning(
    output_label: str,
    vulnerability_df: pd.DataFrame,
) -> str | None:
    if vulnerability_df.empty or "city" not in vulnerability_df.columns:
        return "No urban vulnerability data were available; heat risk was not vulnerability-adjusted."

    cities = vulnerability_df["city"].astype(str).str.strip().str.lower()
    if output_label.lower() not in set(cities):
        return (
            "No matching urban vulnerability row was available for this location; "
            "heat risk was not vulnerability-adjusted."
        )
    return None


def run_heat_for_location_result(
    location: Location,
    *,
    min_run: int = 3,
    vulnerability_path: str | Path | None = None,
    paths: HeatOutputPaths | None = None,
) -> HazardWorkflowResult:
    """Run the heat backend workflow and return structured result metadata."""
    output_paths = paths or heat_output_paths(location)

    historical_df = fetch_historical_temperature_data_for_location(
        location,
        save_path=output_paths.historical,
        output_label=output_paths.label,
    )

    climatology = build_daily_percentile_climatology(historical_df)
    output_paths.climatology.parent.mkdir(parents=True, exist_ok=True)
    climatology.to_csv(output_paths.climatology, index=False)

    forecast_df = fetch_ecmwf_forecast_for_location(
        location,
        save_path=output_paths.forecast,
        output_label=output_paths.label,
    )

    detected = detect_heatwaves_df(forecast_df, climatology, min_run=min_run)
    output_paths.detected.parent.mkdir(parents=True, exist_ok=True)
    detected.to_csv(output_paths.detected, index=False)

    vulnerability_file = Path(vulnerability_path or (RAW_DATA_DIR / "urban_vulnerability.csv"))
    vulnerability_df = (
        pd.read_csv(vulnerability_file)
        if vulnerability_file.exists()
        else _empty_vulnerability_frame()
    )
    risk = assess_heatwave_risk_with_optional_vulnerability(detected, vulnerability_df)
    output_paths.risk.parent.mkdir(parents=True, exist_ok=True)
    risk.to_csv(output_paths.risk, index=False)

    warnings = []
    vulnerability_warning = _heat_vulnerability_warning(
        output_paths.label,
        vulnerability_df,
    )
    if vulnerability_warning:
        warnings.append(vulnerability_warning)

    return HazardWorkflowResult(
        hazard="heat",
        location=location,
        output_label=output_paths.label,
        generated_files=_heat_generated_files(output_paths),
        summary=_heat_summary(historical_df, forecast_df, detected, risk),
        warnings=warnings,
    )


def run_heat_for_location(
    location: Location,
    *,
    min_run: int = 3,
    vulnerability_path: str | Path | None = None,
    paths: HeatOutputPaths | None = None,
) -> HeatOutputPaths:
    """Run the heat backend workflow for a Location and save standard outputs."""
    result = run_heat_for_location_result(
        location,
        min_run=min_run,
        vulnerability_path=vulnerability_path,
        paths=paths,
    )
    return HeatOutputPaths(
        label=result.output_label,
        historical=result.generated_files["historical"],
        forecast=result.generated_files["forecast"],
        climatology=result.generated_files["climatology"],
        detected=result.generated_files["detected"],
        risk=result.generated_files["risk"],
    )
