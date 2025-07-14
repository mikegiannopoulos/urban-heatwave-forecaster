import typer
from . import data_fetcher, detect_heatwaves, risk_model

app = typer.Typer(help="Urban Heatwave Forecaster CLI")

@app.command()
@app.command()
def fetch(city: str):
    """Fetch forecast for CITY (e.g. 'Athens')"""

    coords = {
        "Athens":     (37.9838, 23.7278),
        "Rome":       (41.8919, 12.5113),
        "Stockholm":  (59.3294, 18.0687),
        "London":     (51.5085, -0.1257)
    }

    if city not in coords:
        print(f"❌ Unknown city: {city}")
        raise typer.Exit(code=1)

    lat, lon = coords[city]
    data_fetcher.fetch_ecmwf_forecast(lat, lon, city)

from pathlib import Path

@app.command()
def detect(city: str, min_run: int = 3):
    """Detect heatwaves in CITY"""

    city = city.lower()
    forecast_path    = Path(f"data/raw/{city}_forecast.csv")
    climatology_path = Path(f"data/processed/{city}_climatology_95p.csv")

    df = detect_heatwaves.detect_heatwaves(
        forecast_path=forecast_path,
        climatology_path=climatology_path,
        min_run=min_run
    )

    output_path = Path(f"data/processed/{city}_forecast_with_heatwaves.csv")
    df.to_csv(output_path, index=False)
    print(f"✅ Saved: {output_path}")


import pandas as pd

@app.command()
@app.command()
def assess(city: str):
    """Assess risk based on detected heatwaves"""

    city = city.lower()
    vuln_path = Path("data/raw/urban_vulnerability.csv")
    forecast_path = Path(f"data/processed/{city}_forecast_with_heatwaves.csv")
    output_path = Path(f"data/processed/{city}_heatwave_risk.csv")

    if not vuln_path.exists() or not forecast_path.exists():
        print("❌ Missing required input files.")
        raise typer.Exit(1)

    df_forecast = pd.read_csv(forecast_path)
    vulnerability_df = pd.read_csv(vuln_path)

    # Ensure required column exists
    if "is_hot" not in df_forecast.columns:
        if "exceeds_95p" in df_forecast.columns:
            df_forecast["is_hot"] = df_forecast["exceeds_95p"]
        else:
            raise ValueError("Missing both 'is_hot' and 'exceeds_95p' columns. Run heatwave detection first.")

    df_risk = risk_model.assess_heatwave_risk(df_forecast, vulnerability_df)
    df_risk.to_csv(output_path, index=False)
    print(f"✅ Saved: {output_path}")



if __name__ == "__main__":
    app()
