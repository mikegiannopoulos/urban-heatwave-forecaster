from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import typer

from climate_extremes.baselines.percentiles import build_percentile_climatology
from climate_extremes.core.cities import (
    get_city_coordinates,
    list_supported_cities,
    normalize_city_name,
)
from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from climate_extremes.io.openmeteo import (
    fetch_ecmwf_forecast,
    fetch_historical_precipitation_data,
    fetch_historical_temperature_data,
    fetch_precipitation_forecast,
)
from climate_extremes.modules.heat.detection import detect_heatwaves
from climate_extremes.modules.heat.risk import (
    assess_heatwave_risk,
    build_heat_assessment,
)
from climate_extremes.modules.precipitation import (
    DEFAULT_PRECIPITATION_DEFINITIONS,
    assess_precipitation_risk,
    backtest_precipitation_definitions,
    calibrate_precipitation_definition_floor,
    build_precipitation_assessment_from_frame,
    build_precipitation_climatology,
    compare_precipitation_definitions,
    detect_precipitation_events,
    get_definition_by_name,
)

app = typer.Typer(help="Climate Extremes CLI")
heat_app = typer.Typer(help="Heat hazard workflows")
precipitation_app = typer.Typer(help="Heavy precipitation hazard workflows")
app.add_typer(heat_app, name="heat")
app.add_typer(precipitation_app, name="precipitation")


def _normalize_city(city: str) -> str:
    try:
        return normalize_city_name(city)
    except KeyError:
        supported = ", ".join(list_supported_cities())
        typer.echo(f"Unknown city: {city}. Supported cities: {supported}")
        raise typer.Exit(code=1) from None


def _definition_slug(definition_name: str) -> str:
    return definition_name.replace("-", "_")


@app.command("fetch-historical")
def fetch_historical(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Fetch the historical temperature archive used for climatologies."""
    city_key = _normalize_city(city)
    lat, lon = get_city_coordinates(city_key)
    fetch_historical_temperature_data(lat=lat, lon=lon, city=city_key)


@app.command("build-baseline")
def build_baseline(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Build the daily percentile climatology for CITY."""
    city_key = _normalize_city(city)
    build_percentile_climatology(city_key)


@heat_app.command("fetch")
def heat_fetch(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Fetch the heat-module forecast for CITY."""
    city_key = _normalize_city(city)
    lat, lon = get_city_coordinates(city_key)
    fetch_ecmwf_forecast(lat=lat, lon=lon, city_name=city_key)


@heat_app.command("detect")
def heat_detect(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    min_run: int = typer.Option(3, help="Minimum consecutive exceedance days."),
):
    """Detect heatwaves in CITY."""
    city_key = _normalize_city(city)
    forecast_path = RAW_DATA_DIR / f"{city_key}_forecast.csv"
    climatology_path = PROCESSED_DATA_DIR / f"{city_key}_climatology_95p.csv"

    df = detect_heatwaves(
        forecast_path=forecast_path,
        climatology_path=climatology_path,
        min_run=min_run,
    )

    output_path = PROCESSED_DATA_DIR / f"{city_key}_forecast_with_heatwaves.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    typer.echo(f"Saved: {output_path}")


@heat_app.command("assess")
def heat_assess(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Assess heat risk for CITY."""
    city_key = _normalize_city(city)
    vuln_path = RAW_DATA_DIR / "urban_vulnerability.csv"
    forecast_path = PROCESSED_DATA_DIR / f"{city_key}_forecast_with_heatwaves.csv"
    output_path = PROCESSED_DATA_DIR / f"{city_key}_heatwave_risk.csv"

    if not vuln_path.exists() or not forecast_path.exists():
        typer.echo("Missing required input files.")
        raise typer.Exit(code=1)

    df_forecast = pd.read_csv(forecast_path)
    vulnerability_df = pd.read_csv(vuln_path)

    if "is_hot" not in df_forecast.columns:
        if "exceeds_95p" in df_forecast.columns:
            df_forecast["is_hot"] = df_forecast["exceeds_95p"]
        else:
            raise ValueError(
                "Missing both 'is_hot' and 'exceeds_95p' columns. "
                "Run heatwave detection first."
            )

    df_risk = assess_heatwave_risk(df_forecast, vulnerability_df)
    df_risk.to_csv(output_path, index=False)
    typer.echo(f"Saved: {output_path}")


@heat_app.command("summarize")
def heat_summarize(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    input_path: Path | None = typer.Option(
        None,
        "--input-path",
        help="Optional path to a heat risk CSV. Defaults to the processed output.",
    ),
):
    """Return the standardized module summary for heat."""
    city_key = _normalize_city(city)
    risk_path = input_path or (PROCESSED_DATA_DIR / f"{city_key}_heatwave_risk.csv")
    if not risk_path.exists():
        typer.echo(f"Missing risk file: {risk_path}")
        raise typer.Exit(code=1)

    assessment = build_heat_assessment(pd.read_csv(risk_path))
    typer.echo(json.dumps(assessment.to_dict(), indent=2))


@precipitation_app.command("fetch-historical")
def precipitation_fetch_historical(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Fetch historical precipitation data for CITY."""
    city_key = _normalize_city(city)
    lat, lon = get_city_coordinates(city_key)
    fetch_historical_precipitation_data(lat=lat, lon=lon, city=city_key)


@precipitation_app.command("build-baseline")
def precipitation_build_baseline(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Build precipitation climatology thresholds for CITY."""
    city_key = _normalize_city(city)
    build_precipitation_climatology(city_key)


@precipitation_app.command("fetch")
def precipitation_fetch(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Fetch the precipitation-module forecast for CITY."""
    city_key = _normalize_city(city)
    lat, lon = get_city_coordinates(city_key)
    fetch_precipitation_forecast(lat=lat, lon=lon, city_name=city_key)


@precipitation_app.command("detect")
def precipitation_detect(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    definition: str = typer.Option(
        DEFAULT_PRECIPITATION_DEFINITIONS[0].name,
        "--definition",
        help="Candidate definition name to test.",
    ),
):
    """Detect heavy precipitation events in CITY using a candidate definition."""
    city_key = _normalize_city(city)
    definition_obj = get_definition_by_name(definition)
    definition_slug = _definition_slug(definition_obj.name)
    forecast_path = RAW_DATA_DIR / f"{city_key}_precipitation_forecast.csv"
    climatology_path = PROCESSED_DATA_DIR / f"{city_key}_precipitation_climatology.csv"
    output_path = (
        PROCESSED_DATA_DIR
        / f"{city_key}_precipitation_{definition_slug}_detected.csv"
    )

    df = detect_precipitation_events(
        forecast_path=forecast_path,
        climatology_path=climatology_path,
        definition=definition_obj,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    typer.echo(f"Saved: {output_path}")


@precipitation_app.command("assess")
def precipitation_assess(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    definition: str = typer.Option(
        DEFAULT_PRECIPITATION_DEFINITIONS[0].name,
        "--definition",
        help="Candidate definition name to assess.",
    ),
):
    """Assess heavy precipitation severity for CITY."""
    city_key = _normalize_city(city)
    definition_obj = get_definition_by_name(definition)
    definition_slug = _definition_slug(definition_obj.name)
    input_path = (
        PROCESSED_DATA_DIR
        / f"{city_key}_precipitation_{definition_slug}_detected.csv"
    )
    output_path = (
        PROCESSED_DATA_DIR
        / f"{city_key}_precipitation_{definition_slug}_risk.csv"
    )
    if not input_path.exists():
        typer.echo(f"Missing detected precipitation file: {input_path}")
        raise typer.Exit(code=1)

    assessed = assess_precipitation_risk(pd.read_csv(input_path), definition_obj)
    assessed.to_csv(output_path, index=False)
    typer.echo(f"Saved: {output_path}")


@precipitation_app.command("compare")
def precipitation_compare(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Compare the default precipitation candidate definitions for CITY."""
    city_key = _normalize_city(city)
    forecast_path = RAW_DATA_DIR / f"{city_key}_precipitation_forecast.csv"
    climatology_path = PROCESSED_DATA_DIR / f"{city_key}_precipitation_climatology.csv"
    if not forecast_path.exists() or not climatology_path.exists():
        typer.echo("Missing required precipitation forecast or climatology input files.")
        raise typer.Exit(code=1)

    comparison = compare_precipitation_definitions(
        forecast_df=pd.read_csv(forecast_path),
        climatology_df=pd.read_csv(climatology_path),
    )
    typer.echo(json.dumps(json.loads(comparison.to_json(orient="records")), indent=2))


@precipitation_app.command("backtest")
def precipitation_backtest(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
):
    """Backtest the default precipitation candidate definitions on historical data."""
    city_key = _normalize_city(city)
    historical_path = RAW_DATA_DIR / f"{city_key}_precipitation_historical.csv"
    climatology_path = PROCESSED_DATA_DIR / f"{city_key}_precipitation_climatology.csv"
    output_path = PROCESSED_DATA_DIR / f"{city_key}_precipitation_backtest_summary.csv"
    if not historical_path.exists() or not climatology_path.exists():
        typer.echo("Missing required precipitation historical or climatology input files.")
        raise typer.Exit(code=1)

    backtest = backtest_precipitation_definitions(
        historical_df=pd.read_csv(historical_path),
        climatology_df=pd.read_csv(climatology_path),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    backtest.to_csv(output_path, index=False)
    typer.echo(json.dumps(json.loads(backtest.to_json(orient="records")), indent=2))
    typer.echo(f"Saved: {output_path}")


@precipitation_app.command("calibrate")
def precipitation_calibrate(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    definition: str = typer.Option(
        "wet-spell-3day-95p",
        "--definition",
        help="Base precipitation definition to calibrate.",
    ),
    floors: str = typer.Option(
        "30,40,50,60",
        "--floors",
        help="Comma-separated absolute floor values in millimeters.",
    ),
):
    """Sweep absolute rainfall floors for a precipitation definition on historical data."""
    city_key = _normalize_city(city)
    historical_path = RAW_DATA_DIR / f"{city_key}_precipitation_historical.csv"
    climatology_path = PROCESSED_DATA_DIR / f"{city_key}_precipitation_climatology.csv"
    definition_obj = get_definition_by_name(definition)
    output_path = (
        PROCESSED_DATA_DIR
        / f"{city_key}_{_definition_slug(definition_obj.name)}_floor_calibration.csv"
    )
    if not historical_path.exists() or not climatology_path.exists():
        typer.echo("Missing required precipitation historical or climatology input files.")
        raise typer.Exit(code=1)

    floor_values = [float(value.strip()) for value in floors.split(",") if value.strip()]
    calibration = calibrate_precipitation_definition_floor(
        historical_df=pd.read_csv(historical_path),
        climatology_df=pd.read_csv(climatology_path),
        base_definition=definition_obj,
        floor_values_mm=floor_values,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    calibration.to_csv(output_path, index=False)
    typer.echo(json.dumps(json.loads(calibration.to_json(orient="records")), indent=2))
    typer.echo(f"Saved: {output_path}")


@precipitation_app.command("summarize")
def precipitation_summarize(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    definition: str = typer.Option(
        DEFAULT_PRECIPITATION_DEFINITIONS[0].name,
        "--definition",
        help="Candidate definition name to summarize.",
    ),
):
    """Return the standardized precipitation summary for one tested definition."""
    city_key = _normalize_city(city)
    definition_obj = get_definition_by_name(definition)
    definition_slug = _definition_slug(definition_obj.name)
    input_path = (
        PROCESSED_DATA_DIR
        / f"{city_key}_precipitation_{definition_slug}_risk.csv"
    )
    if not input_path.exists():
        typer.echo(f"Missing assessed precipitation file: {input_path}")
        raise typer.Exit(code=1)

    assessment = build_precipitation_assessment_from_frame(
        pd.read_csv(input_path),
        definition=definition_obj,
    )
    typer.echo(json.dumps(assessment.to_dict(), indent=2))


if __name__ == "__main__":
    app()
