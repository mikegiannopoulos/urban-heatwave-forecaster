from pathlib import Path

import pandas as pd
import typer

app = typer.Typer(help="Urban Heatwave Forecaster CLI")

COORDS = {
    "athens": (37.9838, 23.7278),
    "rome": (41.8919, 12.5113),
    "stockholm": (59.3294, 18.0687),
    "london": (51.5085, -0.1257),
}


def _normalize_city(city: str) -> str:
    city_key = city.strip().lower()
    if city_key not in COORDS:
        supported = ", ".join(sorted(name.title() for name in COORDS))
        typer.echo(f"Unknown city: {city}. Supported cities: {supported}")
        raise typer.Exit(code=1)
    return city_key


@app.command()
def fetch(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens.")
):
    """Fetch forecast for CITY."""
    from . import data_fetcher

    city_key = _normalize_city(city)
    lat, lon = COORDS[city_key]
    data_fetcher.fetch_ecmwf_forecast(lat, lon, city_key)


@app.command()
def detect(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens."),
    min_run: int = 3,
):
    """Detect heatwaves in CITY."""
    from . import detect_heatwaves

    city_key = _normalize_city(city)
    forecast_path = Path(f"data/raw/{city_key}_forecast.csv")
    climatology_path = Path(f"data/processed/{city_key}_climatology_95p.csv")

    df = detect_heatwaves.detect_heatwaves(
        forecast_path=forecast_path,
        climatology_path=climatology_path,
        min_run=min_run,
    )

    output_path = Path(f"data/processed/{city_key}_forecast_with_heatwaves.csv")
    df.to_csv(output_path, index=False)
    typer.echo(f"Saved: {output_path}")


@app.command()
def assess(
    city: str = typer.Option(..., "--city", "-c", help="City name, e.g. Athens.")
):
    """Assess risk based on detected heatwaves."""
    from . import risk_model

    city_key = _normalize_city(city)
    vuln_path = Path("data/raw/urban_vulnerability.csv")
    forecast_path = Path(f"data/processed/{city_key}_forecast_with_heatwaves.csv")
    output_path = Path(f"data/processed/{city_key}_heatwave_risk.csv")

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

    df_risk = risk_model.assess_heatwave_risk(df_forecast, vulnerability_df)
    df_risk.to_csv(output_path, index=False)
    typer.echo(f"Saved: {output_path}")


if __name__ == "__main__":
    app()
