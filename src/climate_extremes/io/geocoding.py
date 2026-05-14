from __future__ import annotations

import requests

from climate_extremes.core.locations import (
    Location,
    location_from_openmeteo_result,
)

OPEN_METEO_GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"


class OpenMeteoGeocodingError(RuntimeError):
    """Raised when Open-Meteo geocoding cannot complete successfully."""


def parse_openmeteo_geocoding_response(payload: dict) -> list[Location]:
    """Parse an Open-Meteo geocoding payload into Location objects."""
    if payload.get("error"):
        reason = payload.get("reason", "Unknown Open-Meteo geocoding error")
        raise OpenMeteoGeocodingError(reason)

    results = payload.get("results") or []
    if not isinstance(results, list):
        raise OpenMeteoGeocodingError(
            "Open-Meteo geocoding response field 'results' was not a list."
        )

    try:
        return [location_from_openmeteo_result(result) for result in results]
    except (TypeError, ValueError) as exc:
        raise OpenMeteoGeocodingError(
            f"Open-Meteo geocoding response contained an invalid result: {exc}"
        ) from exc


def search_locations(
    query: str,
    *,
    count: int = 10,
    language: str = "en",
    session: requests.Session | None = None,
    timeout: int = 15,
) -> list[Location]:
    """Search Open-Meteo geocoding for candidate global locations."""
    search_query = query.strip()
    if not search_query:
        raise ValueError("Location search query cannot be empty.")

    params = {
        "name": search_query,
        "count": count,
        "language": language,
        "format": "json",
    }
    client = session or requests.Session()

    try:
        response = client.get(OPEN_METEO_GEOCODING_URL, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        details = ""
        if response is not None:
            details = f" (status {response.status_code}: {response.text[:300]})"
        raise OpenMeteoGeocodingError(
            f"Open-Meteo geocoding request failed for '{search_query}'{details}"
        ) from exc

    try:
        payload = response.json()
    except ValueError as exc:
        raise OpenMeteoGeocodingError(
            "Open-Meteo geocoding response was not valid JSON."
        ) from exc

    return parse_openmeteo_geocoding_response(payload)
