import openmeteo_requests
import pandas as pd
from pathlib import Path
import requests_cache
from retry_requests import retry
from pathlib import Path

# Always resolve paths from the repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"


def fetch_ecmwf_forecast(
    lat: float,
    lon: float,
    city_name: str,
    save_path: str | Path = None
) -> pd.DataFrame:
    # --- setup ---
    cache = requests_cache.CachedSession('.cache', expire_after=3600)
    sess  = retry(cache, retries=5, backoff_factor=0.2)
    client = openmeteo_requests.Client(session=sess)

    # --- request hourly temperature ---
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m"],
        "models": "ecmwf_ifs025",
        "forecast_days": 7,
        "timezone": "auto"
    }
    res = client.weather_api(url, params=params)[0]

    # --- build DataFrame of hourly values ---
    h = res.Hourly()
    times = pd.date_range(
        start = pd.to_datetime(h.Time(), unit="s", utc=True),
        end   = pd.to_datetime(h.TimeEnd(), unit="s", utc=True),
        freq  = pd.Timedelta(seconds=h.Interval()),
        inclusive="left"
    )
    temps = h.Variables(0).ValuesAsNumpy()

    df = pd.DataFrame({
        "datetime": times,
        "temperature": temps
    })
    df["date"] = df["datetime"].dt.date

    # --- aggregate to daily min & max ---
    df_daily = df.groupby("date").agg(
        tmin=("temperature", "min"),
        tmax=("temperature", "max")
    ).reset_index()
    df_daily["city"] = city_name.lower()

    from datetime import date

    # Drop the first row if it's earlier than today
    today = date.today()
    if df_daily.loc[0, "date"] < today:
        df_daily = df_daily[df_daily["date"] >= today].reset_index(drop=True)


       # --- save ---
    if save_path is None:
        save_path = DATA_DIR / f"{city_name.lower()}_forecast.csv"
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df_daily.to_csv(save_path, index=False)

    print(f"✅ Saved: {save_path}")
    return df_daily


if __name__ == "__main__":
    for city in [
        {"name": "Athens", "lat": 37.9838, "lon": 23.7278},
        {"name": "Rome",   "lat": 41.8919, "lon": 12.5113},
        {"name": "Stockholm",   "lat": 59.3294, "lon": 18.0687},
        {"name": "London", "lat": 51.5085, "lon": -0.1257}
    ]:
        fetch_ecmwf_forecast(
            lat=city["lat"],
            lon=city["lon"],
            city_name=city["name"]
        )
