import openmeteo_requests, pandas as pd, requests_cache
from retry_requests import retry
from pathlib import Path

def fetch_historical_data(lat, lon, city, save_path=None):
    client = openmeteo_requests.Client(
        session=retry(requests_cache.CachedSession(".cache", expire_after=-1), retries=5)
    )

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude":  lat,
        "longitude": lon,
        "start_date": "1991-01-01",
        "end_date":   "2020-12-31",
        "daily": ["temperature_2m_min", "temperature_2m_max"],
        "timezone": "auto",                       # let API tell us the offset
    }

    res    = client.weather_api(url, params=params)[0]
    daily  = res.Daily()

    # --- build local-date index -------------------------------------------
    start_utc = pd.to_datetime(daily.Time(),    unit="s", utc=True)
    end_utc   = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
    step      = pd.Timedelta(seconds=daily.Interval())   # 86 400 s
    utc_dates = pd.date_range(start=start_utc, end=end_utc,
                              freq=step, inclusive="left")

    # shift from UTC to local time then drop the time-of-day part
    offset = pd.to_timedelta(res.UtcOffsetSeconds(), unit="s")
    local_dates = (utc_dates + offset).normalize()

    # --- data --------------------------------------------------------------
    tmin = daily.Variables(0).ValuesAsNumpy()
    tmax = daily.Variables(1).ValuesAsNumpy()

    df = pd.DataFrame({"date": local_dates, "tmin": tmin, "tmax": tmax})
    df["city"] = city.lower()

    # --- save --------------------------------------------------------------
    if save_path is None:
        save_path = Path(f"data/raw/{city.lower()}_historical.csv")
    save_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(save_path, index=False)
    print(f"✅  Saved {len(df):,} rows ➜ {save_path}")
    return df

if __name__ == "__main__":
    cities = [
        {"name": "Athens", "lat": 37.9838, "lon": 23.7278},
        {"name": "Rome",   "lat": 41.8919, "lon": 12.5113},
        {"name": "Stockholm",   "lat": 59.3294, "lon": 18.0687},
        {"name": "London", "lat": 51.5085, "lon": -0.1257},

    ]
    for c in cities:
        fetch_historical_data(c["lat"], c["lon"], c["name"])
