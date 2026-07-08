from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import openmeteo_requests
import pandas as pd
import requests
import requests_cache
from retry_requests import retry

from climate_extremes.core.locations import Location, location_slug
from climate_extremes.core.paths import CACHE_DIR, RAW_DATA_DIR

DEFAULT_MULTI_MODELS = ("ecmwf_ifs025", "gfs_seamless", "icon_seamless")
LOGGER = logging.getLogger(__name__)


def _build_retry_session():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / "forecast_cache"
    cache = requests_cache.CachedSession(str(cache_path), expire_after=3600)
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

    timestamps = pd.to_datetime(times)
    if pd.isna(timestamps).any():
        raise ValueError("Open-Meteo returned unparsable hourly timestamps.")

    df = pd.DataFrame({"datetime": timestamps, "temperature": temps})
    df["date"] = df["datetime"].dt.date
    df_daily = df.groupby("date").agg(
        tmin=("temperature", "min"),
        tmax=("temperature", "max"),
    ).reset_index()
    df_daily["city"] = city_name.lower()
    if include_model_col and model:
        df_daily["model"] = model

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

    return payload


def _forecast_temperature_params(
    lat: float,
    lon: float,
    model: str,
    forecast_days: int,
    timezone: str | None = None,
) -> dict:
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m",
        "models": model,
        "forecast_days": forecast_days,
        "timezone": timezone or "auto",
    }


def _historical_temperature_params(
    lat: float,
    lon: float,
    timezone: str | None = None,
) -> dict:
    return {
        "latitude": lat,
        "longitude": lon,
        "start_date": "1991-01-01",
        "end_date": "2020-12-31",
        "daily": ["temperature_2m_min", "temperature_2m_max"],
        "timezone": timezone or "auto",
    }


def _precipitation_forecast_params(
    lat: float,
    lon: float,
    model: str,
    forecast_days: int,
    timezone: str | None = None,
) -> dict:
    return {
        "latitude": lat,
        "longitude": lon,
        "daily": "precipitation_sum",
        "models": model,
        "forecast_days": forecast_days,
        "timezone": timezone or "auto",
    }


def _historical_precipitation_params(
    lat: float,
    lon: float,
    timezone: str | None = None,
) -> dict:
    return {
        "latitude": lat,
        "longitude": lon,
        "start_date": "1991-01-01",
        "end_date": "2020-12-31",
        "daily": ["precipitation_sum"],
        "timezone": timezone or "auto",
    }


def _daily_value_dataframe(
    times,
    values,
    city_name: str,
    value_column: str,
    include_model_col: bool = False,
    model: str | None = None,
) -> pd.DataFrame:
    if len(times) != len(values):
        raise ValueError(
            f"Open-Meteo returned mismatched daily timestamps and '{value_column}' values."
        )

    timestamps = pd.to_datetime(times)
    if pd.isna(timestamps).any():
        raise ValueError("Open-Meteo returned unparsable daily timestamps.")

    frame = pd.DataFrame({"date": timestamps.date, value_column: values})
    frame["city"] = city_name.lower()
    if include_model_col and model:
        frame["model"] = model

    today = date.today()
    if not frame.empty and frame.loc[0, "date"] < today:
        frame = frame[frame["date"] >= today].reset_index(drop=True)

    return frame


def fetch_forecast_for_model(
    lat: float,
    lon: float,
    city_name: str,
    model: str = "ecmwf_ifs025",
    forecast_days: int = 7,
    save_path: str | Path | None = None,
    include_model_col: bool = True,
    timezone: str | None = None,
) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = _forecast_temperature_params(
        lat=lat,
        lon=lon,
        model=model,
        forecast_days=forecast_days,
        timezone=timezone,
    )
    payload = _fetch_forecast_payload(url, params, model=model)
    hourly = payload["hourly"]
    if "time" not in hourly or "temperature_2m" not in hourly:
        raise RuntimeError(
            f"Open-Meteo forecast response for model '{model}' was missing hourly temperature data."
        )
    df_daily = _daily_temperature_from_hourly_data(
        times=hourly["time"],
        temps=hourly["temperature_2m"],
        city_name=city_name,
        include_model_col=include_model_col,
        model=model,
    )

    if save_path is None:
        suffix = f"_{model}" if include_model_col else ""
        save_path = RAW_DATA_DIR / f"{city_name.lower()}{suffix}_forecast.csv"
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df_daily.to_csv(save_path, index=False)

    LOGGER.info("Saved %s forecast to %s", model, save_path)
    print(f"Saved {model}: {save_path}")
    return df_daily


def fetch_ecmwf_forecast(
    lat: float,
    lon: float,
    city_name: str,
    save_path: str | Path | None = None,
    timezone: str | None = None,
) -> pd.DataFrame:
    if save_path is None:
        save_path = RAW_DATA_DIR / f"{city_name.lower()}_forecast.csv"
    return fetch_forecast_for_model(
        lat=lat,
        lon=lon,
        city_name=city_name,
        model="ecmwf_ifs025",
        forecast_days=7,
        save_path=save_path,
        include_model_col=False,
        timezone=timezone,
    )


def fetch_ecmwf_forecast_for_location(
    location: Location,
    save_path: str | Path | None = None,
    output_label: str | None = None,
) -> pd.DataFrame:
    """Fetch ECMWF temperature forecast for a Location."""
    label = output_label or location_slug(location)
    return fetch_ecmwf_forecast(
        lat=location.latitude,
        lon=location.longitude,
        city_name=label,
        save_path=save_path,
        timezone=location.timezone,
    )


def fetch_multi_model_forecast(
    lat: float,
    lon: float,
    city_name: str,
    models: list[str] | tuple[str, ...] | None = None,
    forecast_days: int = 7,
    timezone: str | None = None,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    requested_models = list(models or DEFAULT_MULTI_MODELS)
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
                save_path=RAW_DATA_DIR / f"{city_name.lower()}_{model}_forecast.csv",
                include_model_col=True,
                timezone=timezone,
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


def fetch_historical_temperature_data(
    lat: float,
    lon: float,
    city: str,
    save_path: str | Path | None = None,
    timezone: str | None = None,
) -> pd.DataFrame:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    client = openmeteo_requests.Client(
        session=retry(
            requests_cache.CachedSession(
                str(CACHE_DIR / "historical_cache"),
                expire_after=-1,
            ),
            retries=5,
        )
    )

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = _historical_temperature_params(
        lat=lat,
        lon=lon,
        timezone=timezone,
    )

    response = client.weather_api(url, params=params)[0]
    daily = response.Daily()

    start_utc = pd.to_datetime(daily.Time(), unit="s", utc=True)
    end_utc = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
    step = pd.Timedelta(seconds=daily.Interval())
    utc_dates = pd.date_range(
        start=start_utc,
        end=end_utc,
        freq=step,
        inclusive="left",
    )

    offset = pd.to_timedelta(response.UtcOffsetSeconds(), unit="s")
    local_dates = (utc_dates + offset).normalize()

    tmin = daily.Variables(0).ValuesAsNumpy()
    tmax = daily.Variables(1).ValuesAsNumpy()

    df = pd.DataFrame({"date": local_dates, "tmin": tmin, "tmax": tmax})
    df["city"] = city.lower()

    if save_path is None:
        save_path = RAW_DATA_DIR / f"{city.lower()}_historical.csv"
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"Saved {len(df):,} rows to {save_path}")
    return df


def fetch_historical_temperature_data_for_location(
    location: Location,
    save_path: str | Path | None = None,
    output_label: str | None = None,
) -> pd.DataFrame:
    """Fetch historical temperature archive for a Location."""
    label = output_label or location_slug(location)
    return fetch_historical_temperature_data(
        lat=location.latitude,
        lon=location.longitude,
        city=label,
        save_path=save_path,
        timezone=location.timezone,
    )


def fetch_precipitation_forecast(
    lat: float,
    lon: float,
    city_name: str,
    model: str = "ecmwf_ifs025",
    forecast_days: int = 7,
    save_path: str | Path | None = None,
    include_model_col: bool = False,
    timezone: str | None = None,
) -> pd.DataFrame:
    url = "https://api.open-meteo.com/v1/forecast"
    params = _precipitation_forecast_params(
        lat=lat,
        lon=lon,
        model=model,
        forecast_days=forecast_days,
        timezone=timezone,
    )
    payload = _fetch_forecast_payload(url, params, model=model)
    daily = payload.get("daily") or {}
    if "time" not in daily or "precipitation_sum" not in daily:
        raise RuntimeError(
            f"Open-Meteo forecast response for model '{model}' was missing daily precipitation data."
        )

    df_daily = _daily_value_dataframe(
        times=daily["time"],
        values=daily["precipitation_sum"],
        city_name=city_name,
        value_column="precipitation_sum",
        include_model_col=include_model_col,
        model=model,
    )

    if save_path is None:
        suffix = f"_{model}" if include_model_col else ""
        save_path = RAW_DATA_DIR / f"{city_name.lower()}{suffix}_precipitation_forecast.csv"
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df_daily.to_csv(save_path, index=False)

    LOGGER.info("Saved precipitation forecast for %s to %s", model, save_path)
    print(f"Saved precipitation forecast {model}: {save_path}")
    return df_daily


def fetch_precipitation_forecast_for_location(
    location: Location,
    save_path: str | Path | None = None,
    output_label: str | None = None,
    model: str = "ecmwf_ifs025",
    forecast_days: int = 7,
) -> pd.DataFrame:
    """Fetch precipitation forecast for a Location."""
    label = output_label or location_slug(location)
    return fetch_precipitation_forecast(
        lat=location.latitude,
        lon=location.longitude,
        city_name=label,
        model=model,
        forecast_days=forecast_days,
        save_path=save_path,
        include_model_col=False,
        timezone=location.timezone,
    )


def fetch_historical_precipitation_data(
    lat: float,
    lon: float,
    city: str,
    save_path: str | Path | None = None,
    timezone: str | None = None,
) -> pd.DataFrame:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    client = openmeteo_requests.Client(
        session=retry(
            requests_cache.CachedSession(
                str(CACHE_DIR / "historical_cache"),
                expire_after=-1,
            ),
            retries=5,
        )
    )

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = _historical_precipitation_params(
        lat=lat,
        lon=lon,
        timezone=timezone,
    )

    response = client.weather_api(url, params=params)[0]
    daily = response.Daily()

    start_utc = pd.to_datetime(daily.Time(), unit="s", utc=True)
    end_utc = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
    step = pd.Timedelta(seconds=daily.Interval())
    utc_dates = pd.date_range(
        start=start_utc,
        end=end_utc,
        freq=step,
        inclusive="left",
    )

    offset = pd.to_timedelta(response.UtcOffsetSeconds(), unit="s")
    local_dates = (utc_dates + offset).normalize()
    precipitation_sum = daily.Variables(0).ValuesAsNumpy()

    df = pd.DataFrame(
        {
            "date": local_dates,
            "precipitation_sum": precipitation_sum,
        }
    )
    df["city"] = city.lower()

    if save_path is None:
        save_path = RAW_DATA_DIR / f"{city.lower()}_precipitation_historical.csv"
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"Saved {len(df):,} precipitation rows to {save_path}")
    return df


def fetch_historical_precipitation_data_for_location(
    location: Location,
    save_path: str | Path | None = None,
    output_label: str | None = None,
) -> pd.DataFrame:
    """Fetch historical precipitation archive for a Location."""
    label = output_label or location_slug(location)
    return fetch_historical_precipitation_data(
        lat=location.latitude,
        lon=location.longitude,
        city=label,
        save_path=save_path,
        timezone=location.timezone,
    )
