from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import typer

from climate_extremes.baselines.percentiles import build_percentile_climatology
from climate_extremes.core.cities import (
    get_city_location,
    get_city_coordinates,
    list_supported_cities,
    normalize_city_name,
)
from climate_extremes.core.locations import Location
from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.io.geocoding import OpenMeteoGeocodingError, search_locations
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
from climate_extremes.modules.heat.workflow import run_heat_for_location_result
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
    run_precipitation_for_location_result,
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


def _echo_location_candidates(locations: list[Location]) -> None:
    for index, candidate in enumerate(locations[:10], start=1):
        parts = [candidate.name]
        if candidate.admin1:
            parts.append(candidate.admin1)
        if candidate.country:
            country = candidate.country
            if candidate.country_code:
                country = f"{country} ({candidate.country_code})"
            parts.append(country)
        typer.echo(
            f"  {index}. {', '.join(parts)} "
            f"[{candidate.latitude:.4f}, {candidate.longitude:.4f}]"
        )


def _print_workflow_result(result: HazardWorkflowResult) -> None:
    typer.echo(result.short_summary())
    typer.echo(f"Location: {result.location.name}")
    typer.echo(f"Label: {result.output_label}")
    if result.is_experimental:
        typer.echo("Status: experimental/candidate")
    typer.echo("Generated files:")
    for label, path in result.generated_files.items():
        typer.echo(f"  {label}: {path}")
    for warning in result.warnings:
        typer.echo(f"Warning: {warning}")


def _resolve_location(
    *,
    city: str | None,
    location_query: str | None,
    latitude: float | None,
    longitude: float | None,
    name: str | None,
    country_code: str | None,
    timezone: str | None,
) -> Location:
    explicit_coordinates = latitude is not None or longitude is not None
    modes = sum(bool(value) for value in (city, location_query, explicit_coordinates))
    if modes != 1:
        typer.echo(
            "Choose exactly one location mode: --city, --location, or "
            "--latitude/--longitude."
        )
        raise typer.Exit(code=1)

    if city:
        try:
            return get_city_location(city)
        except KeyError:
            supported = ", ".join(list_supported_cities())
            typer.echo(f"Unknown city: {city}. Supported demo cities: {supported}")
            raise typer.Exit(code=1) from None

    if explicit_coordinates:
        if latitude is None or longitude is None:
            typer.echo("Both --latitude and --longitude are required.")
            raise typer.Exit(code=1)
        return Location(
            name=name or f"{latitude:.4f},{longitude:.4f}",
            latitude=latitude,
            longitude=longitude,
            country_code=country_code.upper() if country_code else None,
            timezone=timezone,
        )

    try:
        candidates = search_locations(location_query or "")
    except (OpenMeteoGeocodingError, ValueError) as exc:
        typer.echo(f"Location search failed: {exc}")
        raise typer.Exit(code=1) from None

    if country_code:
        normalized_code = country_code.upper()
        candidates = [
            candidate
            for candidate in candidates
            if candidate.country_code and candidate.country_code.upper() == normalized_code
        ]

    if not candidates:
        typer.echo(f"No locations found for: {location_query}")
        raise typer.Exit(code=1)

    if len(candidates) == 1:
        return candidates[0]

    typer.echo(
        "Location search is ambiguous. Use --country-code if it selects exactly "
        "one candidate, or use explicit --latitude/--longitude."
    )
    _echo_location_candidates(candidates)
    raise typer.Exit(code=1)


@app.command("search-location")
def search_location(
    query: str = typer.Argument(..., help="Location search query, e.g. Gothenburg."),
    count: int = typer.Option(10, "--count", "-n", help="Maximum candidates to show."),
):
    """Search global locations using Open-Meteo geocoding."""
    try:
        locations = search_locations(query, count=count)
    except (OpenMeteoGeocodingError, ValueError) as exc:
        typer.echo(f"Location search failed: {exc}")
        raise typer.Exit(code=1) from None

    if not locations:
        typer.echo(f"No locations found for: {query}")
        return

    for index, location in enumerate(locations, start=1):
        place_parts = [location.name]
        if location.admin1:
            place_parts.append(location.admin1)
        if location.country:
            country = location.country
            if location.country_code:
                country = f"{country} ({location.country_code})"
            place_parts.append(country)

        details = [
            f"lat {location.latitude:.4f}",
            f"lon {location.longitude:.4f}",
        ]
        if location.timezone:
            details.append(f"timezone {location.timezone}")
        if location.population is not None:
            details.append(f"population {location.population:,}")

        typer.echo(f"{index}. {', '.join(place_parts)}")
        typer.echo(f"   {' | '.join(details)}")


@app.command("run-heat")
def run_heat(
    city: str | None = typer.Option(
        None,
        "--city",
        "-c",
        help="Supported demo city name, e.g. Athens.",
    ),
    location: str | None = typer.Option(
        None,
        "--location",
        "-l",
        help="Global location search query, e.g. Gothenburg.",
    ),
    country_code: str | None = typer.Option(
        None,
        "--country-code",
        help="ISO country code used to disambiguate --location or label coordinates.",
    ),
    latitude: float | None = typer.Option(None, "--latitude", help="Explicit latitude."),
    longitude: float | None = typer.Option(None, "--longitude", help="Explicit longitude."),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Display/output name for explicit coordinates.",
    ),
    timezone: str | None = typer.Option(
        None,
        "--timezone",
        help="IANA timezone for explicit coordinates, e.g. Europe/Stockholm.",
    ),
    min_run: int = typer.Option(3, help="Minimum consecutive exceedance days."),
):
    """Run the heat backend workflow for a demo city or global location."""
    resolved = _resolve_location(
        city=city,
        location_query=location,
        latitude=latitude,
        longitude=longitude,
        name=name,
        country_code=country_code,
        timezone=timezone,
    )
    result = run_heat_for_location_result(resolved, min_run=min_run)
    _print_workflow_result(result)


@app.command("run-precipitation")
def run_precipitation(
    city: str | None = typer.Option(
        None,
        "--city",
        "-c",
        help="Supported demo city name, e.g. Athens.",
    ),
    location: str | None = typer.Option(
        None,
        "--location",
        "-l",
        help="Global location search query, e.g. Gothenburg.",
    ),
    country_code: str | None = typer.Option(
        None,
        "--country-code",
        help="ISO country code used to disambiguate --location or label coordinates.",
    ),
    latitude: float | None = typer.Option(None, "--latitude", help="Explicit latitude."),
    longitude: float | None = typer.Option(None, "--longitude", help="Explicit longitude."),
    name: str | None = typer.Option(
        None,
        "--name",
        help="Display/output name for explicit coordinates.",
    ),
    timezone: str | None = typer.Option(
        None,
        "--timezone",
        help="IANA timezone for explicit coordinates, e.g. Europe/Stockholm.",
    ),
    definition: str = typer.Option(
        DEFAULT_PRECIPITATION_DEFINITIONS[0].name,
        "--definition",
        help="Candidate precipitation definition name to run.",
    ),
):
    """Run the experimental precipitation backend workflow for a location."""
    resolved = _resolve_location(
        city=city,
        location_query=location,
        latitude=latitude,
        longitude=longitude,
        name=name,
        country_code=country_code,
        timezone=timezone,
    )
    definition_obj = get_definition_by_name(definition)
    result = run_precipitation_for_location_result(resolved, definition=definition_obj)
    _print_workflow_result(result)


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
