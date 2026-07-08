from __future__ import annotations

from climate_extremes.io import openmeteo


def test_temperature_forecast_params_include_coordinates_and_timezone():
    params = openmeteo._forecast_temperature_params(
        lat=57.7089,
        lon=11.9746,
        model="ecmwf_ifs025",
        forecast_days=7,
        timezone="Europe/Stockholm",
    )

    assert params == {
        "latitude": 57.7089,
        "longitude": 11.9746,
        "hourly": "temperature_2m",
        "models": "ecmwf_ifs025",
        "forecast_days": 7,
        "timezone": "Europe/Stockholm",
    }


def test_historical_temperature_params_include_coordinates_and_timezone():
    params = openmeteo._historical_temperature_params(
        lat=57.7089,
        lon=11.9746,
        timezone="Europe/Stockholm",
    )

    assert params == {
        "latitude": 57.7089,
        "longitude": 11.9746,
        "start_date": "1991-01-01",
        "end_date": "2020-12-31",
        "daily": ["temperature_2m_min", "temperature_2m_max"],
        "timezone": "Europe/Stockholm",
    }


def test_coordinate_forecast_fetch_uses_constructed_params(monkeypatch, tmp_path):
    captured = {}

    def fake_fetch_payload(url, params, model):
        captured["url"] = url
        captured["params"] = params
        captured["model"] = model
        return {
            "hourly": {
                "time": [
                    "2030-07-01T00:00",
                    "2030-07-01T12:00",
                    "2030-07-02T00:00",
                    "2030-07-02T12:00",
                ],
                "temperature_2m": [20.0, 30.0, 21.0, 31.0],
            }
        }

    monkeypatch.setattr(openmeteo, "_fetch_forecast_payload", fake_fetch_payload)

    frame = openmeteo.fetch_forecast_for_model(
        lat=57.7089,
        lon=11.9746,
        city_name="gothenburg-se",
        model="ecmwf_ifs025",
        forecast_days=7,
        save_path=tmp_path / "forecast.csv",
        include_model_col=False,
        timezone="Europe/Stockholm",
    )

    assert captured["params"]["latitude"] == 57.7089
    assert captured["params"]["longitude"] == 11.9746
    assert captured["params"]["timezone"] == "Europe/Stockholm"
    assert frame["city"].tolist() == ["gothenburg-se", "gothenburg-se"]


def test_precipitation_forecast_params_include_coordinates_and_timezone():
    params = openmeteo._precipitation_forecast_params(
        lat=57.7089,
        lon=11.9746,
        model="ecmwf_ifs025",
        forecast_days=7,
        timezone="Europe/Stockholm",
    )

    assert params == {
        "latitude": 57.7089,
        "longitude": 11.9746,
        "daily": "precipitation_sum",
        "models": "ecmwf_ifs025",
        "forecast_days": 7,
        "timezone": "Europe/Stockholm",
    }


def test_historical_precipitation_params_include_coordinates_and_timezone():
    params = openmeteo._historical_precipitation_params(
        lat=57.7089,
        lon=11.9746,
        timezone="Europe/Stockholm",
    )

    assert params == {
        "latitude": 57.7089,
        "longitude": 11.9746,
        "start_date": "1991-01-01",
        "end_date": "2020-12-31",
        "daily": ["precipitation_sum"],
        "timezone": "Europe/Stockholm",
    }


def test_coordinate_precipitation_fetch_uses_constructed_params(monkeypatch, tmp_path):
    captured = {}

    def fake_fetch_payload(url, params, model):
        captured["url"] = url
        captured["params"] = params
        captured["model"] = model
        return {
            "daily": {
                "time": ["2030-07-01", "2030-07-02"],
                "precipitation_sum": [12.0, 0.5],
            }
        }

    monkeypatch.setattr(openmeteo, "_fetch_forecast_payload", fake_fetch_payload)

    frame = openmeteo.fetch_precipitation_forecast(
        lat=57.7089,
        lon=11.9746,
        city_name="gothenburg-se",
        model="ecmwf_ifs025",
        forecast_days=7,
        save_path=tmp_path / "precipitation_forecast.csv",
        timezone="Europe/Stockholm",
    )

    assert captured["params"]["latitude"] == 57.7089
    assert captured["params"]["longitude"] == 11.9746
    assert captured["params"]["timezone"] == "Europe/Stockholm"
    assert frame["city"].tolist() == ["gothenburg-se", "gothenburg-se"]
