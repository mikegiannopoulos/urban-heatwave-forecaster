import openmeteo_requests
import pandas as pd
from pathlib import Path
import requests_cache
from retry_requests import retry
from datetime import date

# Always resolve paths from the repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_MULTI_MODELS = ("ecmwf_ifs025", "gfs_seamless", "icon_seamless")


def _build_openmeteo_client() -> openmeteo_requests.Client:
    cache = requests_cache.CachedSession(".cache", expire_after=3600)
    sess = retry(cache, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=sess)


def _daily_temperature_from_response(
    response,
    city_name: str,
    include_model_col: bool = False,
    model: str | None = None,
) -> pd.DataFrame:
    # --- build DataFrame of hourly values ---
    hourly = response.Hourly()
    times = pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
    temps = hourly.Variables(0).ValuesAsNumpy()

    df = pd.DataFrame({"datetime": times, "temperature": temps})
    df["date"] = df["datetime"].dt.date

    # --- aggregate to daily min & max ---
    df_daily = df.groupby("date").agg(
        tmin=("temperature", "min"),
        tmax=("temperature", "max"),
    ).reset_index()
    df_daily["city"] = city_name.lower()
    if include_model_col and model:
        df_daily["model"] = model

    # Drop the first row if it's earlier than today
    today = date.today()
    if not df_daily.empty and df_daily.loc[0, "date"] < today:
        df_daily = df_daily[df_daily["date"] >= today].reset_index(drop=True)

    return df_daily


def fetch_forecast_for_model(
    lat: float,
    lon: float,
    city_name: str,
    model: str = "ecmwf_ifs025",
    forecast_days: int = 7,
    save_path: str | Path | None = None,
    include_model_col: bool = True,
) -> pd.DataFrame:
    client = _build_openmeteo_client()

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m"],
        "models": model,
        "forecast_days": forecast_days,
        "timezone": "auto",
    }
    response = client.weather_api(url, params=params)[0]
    df_daily = _daily_temperature_from_response(
        response,
        city_name=city_name,
        include_model_col=include_model_col,
        model=model,
    )

    # --- save ---
    if save_path is None:
        suffix = f"_{model}" if include_model_col else ""
        save_path = DATA_DIR / f"{city_name.lower()}{suffix}_forecast.csv"
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df_daily.to_csv(save_path, index=False)

    print(f"✅ Saved {model}: {save_path}")
    return df_daily


def fetch_ecmwf_forecast(
    lat: float,
    lon: float,
    city_name: str,
    save_path: str | Path | None = None,
) -> pd.DataFrame:
    if save_path is None:
        save_path = DATA_DIR / f"{city_name.lower()}_forecast.csv"
    return fetch_forecast_for_model(
        lat=lat,
        lon=lon,
        city_name=city_name,
        model="ecmwf_ifs025",
        forecast_days=7,
        save_path=save_path,
        include_model_col=False,
    )


def fetch_multi_model_forecast(
    lat: float,
    lon: float,
    city_name: str,
    models: list[str] | tuple[str, ...] | None = None,
    forecast_days: int = 7,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    requested_models = list(models or DEFAULT_MULTI_MODELS)
    # de-duplicate while preserving order
    requested_models = list(dict.fromkeys(requested_models))

    frames = []
    failures: list[dict[str, str]] = []

    for model in requested_models:
        try:
            df_model = fetch_forecast_for_model(
                lat=lat,
                lon=lon,
                city_name=city_name,
                model=model,
                forecast_days=forecast_days,
                save_path=DATA_DIR / f"{city_name.lower()}_{model}_forecast.csv",
                include_model_col=True,
            )
            frames.append(df_model)
        except Exception as exc:
            failures.append({"model": model, "error": str(exc)})

    if not frames:
        failed_details = ", ".join(
            f"{item['model']}: {item['error']}" for item in failures
        )
        raise RuntimeError(f"Failed to fetch all requested models ({failed_details})")

    combined = pd.concat(frames, ignore_index=True)
    return combined, failures


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
