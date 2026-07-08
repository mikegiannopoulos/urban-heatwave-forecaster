from __future__ import annotations

import pytest

from climate_extremes.io.geocoding import (
    OPEN_METEO_GEOCODING_URL,
    OpenMeteoGeocodingError,
    parse_openmeteo_geocoding_response,
    search_locations,
)


SAMPLE_GEOCODING_RESPONSE = {
    "results": [
        {
            "id": 2711537,
            "name": "Gothenburg",
            "latitude": 57.70716,
            "longitude": 11.96679,
            "elevation": 13.0,
            "feature_code": "PPLA",
            "country_code": "SE",
            "admin1_id": 3337385,
            "timezone": "Europe/Stockholm",
            "population": 587549,
            "country_id": 2661886,
            "country": "Sweden",
            "admin1": "Västra Götaland County",
        }
    ],
    "generationtime_ms": 0.8,
}


class FakeResponse:
    status_code = 200
    text = ""

    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload: dict):
        self.payload = payload
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse(self.payload)


def test_parse_openmeteo_geocoding_response_returns_locations():
    locations = parse_openmeteo_geocoding_response(SAMPLE_GEOCODING_RESPONSE)

    assert len(locations) == 1
    location = locations[0]
    assert location.name == "Gothenburg"
    assert location.country == "Sweden"
    assert location.country_code == "SE"
    assert location.latitude == 57.70716
    assert location.longitude == 11.96679
    assert location.timezone == "Europe/Stockholm"
    assert location.population == 587549


def test_parse_openmeteo_geocoding_response_handles_no_results():
    assert parse_openmeteo_geocoding_response({}) == []
    assert parse_openmeteo_geocoding_response({"results": []}) == []


def test_parse_openmeteo_geocoding_response_raises_clear_api_error():
    with pytest.raises(OpenMeteoGeocodingError, match="bad query"):
        parse_openmeteo_geocoding_response({"error": True, "reason": "bad query"})


def test_search_locations_uses_injected_session_without_live_api_call():
    session = FakeSession(SAMPLE_GEOCODING_RESPONSE)

    locations = search_locations("Gothenburg", count=5, session=session)

    assert [location.name for location in locations] == ["Gothenburg"]
    assert session.calls == [
        {
            "url": OPEN_METEO_GEOCODING_URL,
            "params": {
                "name": "Gothenburg",
                "count": 5,
                "language": "en",
                "format": "json",
            },
            "timeout": 15,
        }
    ]
