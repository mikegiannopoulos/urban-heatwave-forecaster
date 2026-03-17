import logging
from pathlib import Path
from datetime import date

import pandas as pd
import requests
import requests_cache
from retry_requests import retry

# Always resolve paths from the repo root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_MULTI_MODELS = ("ecmwf_ifs025", "gfs_seamless", "icon_seamless")
LOGGER = logging.getLogger(__name__)


def _build_retry_session():
    cache = requests_cache.CachedSession(".cache", expire_after=3600)
    return retry(cache, retries=5, backoff_factor=0.2)


def _daily_temperature_from_hourly_data(
    times,
    temps,
    city_name: str,
    include_model_col: bool = False,
    model: str | None = None,
) -> pd.DataFrame:
    if len(times) != len(temps):
        raise ValueError(
            "Open-Meteo returned mismatched hourly timestamps and temperatures."
        )

    # JSON responses already respect the requested timezone and are easier to parse
    # than FlatBuffers in constrained environments like Streamlit Cloud.
    timestamps = pd.to_datetime(times)
    if pd.isna(timestamps).any():
        raise ValueError("Open-Meteo returned unparsable hourly timestamps.")

    df = pd.DataFrame({"datetime": timestamps, "temperature": temps})
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


def _fetch_forecast_payload(url: str, params: dict, model: str) -> dict:
    session = _build_retry_session()
    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        details = ""
        if response is not None:
            details = f" (status {response.status_code}: {response.text[:300]})"
        raise RuntimeError(
            f"Open-Meteo forecast request failed for model '{model}'{details}"
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("Open-Meteo forecast response was not valid JSON.") from exc

    if payload.get("error"):
        reason = payload.get("reason", "Unknown error")
        raise RuntimeError(
            f"Open-Meteo forecast request failed for model '{model}': {reason}"
        )

    hourly = payload.get("hourly") or {}
    if "time" not in hourly or "temperature_2m" not in hourly:
        raise RuntimeError(
            f"Open-Meteo forecast response for model '{model}' was missing hourly temperature data."
        )

    return payload


def fetch_forecast_for_model(
    lat: float,
    lon: float,
    city_name: str,
    model: str = "ecmwf_ifs025",
    forecast_days: int = 7,
    save_path: str | Path | None = None,
    include_model_col: bool = True,
) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": model,
        "forecast_days": forecast_days,
        "timezone": "auto",
    }
    payload = _fetch_forecast_payload(url, params, model=model)
    hourly = payload["hourly"]
    df_daily = _daily_temperature_from_hourly_data(
        times=hourly["time"],
        temps=hourly["temperature_2m"],
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

    LOGGER.info("Saved %s forecast to %s", model, save_path)
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
