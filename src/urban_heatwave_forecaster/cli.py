from __future__ import annotations

import pandas as pd
import typer

from climate_extremes.core.cities import (
    get_city_coordinates,
    list_supported_cities,
    normalize_city_name,
)
from climate_extremes.core.paths import PROCESSED_DATA_DIR, RAW_DATA_DIR
from climate_extremes.io.openmeteo import fetch_ecmwf_forecast
from climate_extremes.modules.heat.detection import detect_heatwaves
from climate_extremes.modules.heat.risk import assess_heatwave_risk

app = typer.Typer(help="Climate Hazards Forecaster legacy compatibility CLI")


def _normalize_city(city: str) -> str:
    try:
        return normalize_city_name(city)
    except KeyError:
        supported = ", ".join(list_supported_cities())
        typer.echo(f"Unknown city: {city}. Supported cities: {supported}")
        raise typer.Exit(code=1) from None


@app.command()
def fetch(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens.")
):
    """Fetch forecast for CITY."""
    city_key = _normalize_city(city)
    lat, lon = get_city_coordinates(city_key)
    fetch_ecmwf_forecast(lat, lon, city_key)


@app.command()
def detect(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    min_run: int = 3,
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


@app.command()
def assess(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens.")
):
    """Assess risk based on detected heatwaves."""
    city_key = _normalize_city(city)
    vuln_path = RAW_DATA_DIR / "urban_vulnerability.csv"
    forecast_path = PROCESSED_DATA_DIR / f"{city_key}_forecast_with_heatwaves.csv"
    output_path = PROCESSED_DATA_DIR / f"{city_key}_heatwave_risk.csv"

    if not vuln_path.exists() or not forecast_path.exists():
        typer.echo("Missing required input files.")
        raise typer.Exit(1)

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


if __name__ == "__main__":
    app()
