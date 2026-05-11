from climate_extremes.io.openmeteo import fetch_historical_temperature_data


def fetch_historical_data(*args, **kwargs):
    return fetch_historical_temperature_data(*args, **kwargs)


__all__ = ["fetch_historical_data", "fetch_historical_temperature_data"]
