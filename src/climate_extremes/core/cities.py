from __future__ import annotations

from climate_extremes.core.locations import Location

SUPPORTED_CITIES = {
    "athens": {
        "name": "Athens",
        "lat": 37.9838,
        "lon": 23.7278,
        "country": "Greece",
        "country_code": "GR",
        "admin1": "Attica",
        "timezone": "Europe/Athens",
    },
    "rome": {
        "name": "Rome",
        "lat": 41.8919,
        "lon": 12.5113,
        "country": "Italy",
        "country_code": "IT",
        "admin1": "Lazio",
        "timezone": "Europe/Rome",
    },
    "stockholm": {
        "name": "Stockholm",
        "lat": 59.3294,
        "lon": 18.0687,
        "country": "Sweden",
        "country_code": "SE",
        "admin1": "Stockholm County",
        "timezone": "Europe/Stockholm",
    },
    "london": {
        "name": "London",
        "lat": 51.5085,
        "lon": -0.1257,
        "country": "United Kingdom",
        "country_code": "GB",
        "admin1": "England",
        "timezone": "Europe/London",
    },
}


def normalize_city_name(city: str) -> str:
    city_key = city.strip().lower()
    if city_key not in SUPPORTED_CITIES:
        raise KeyError(city)
    return city_key


def get_city_coordinates(city: str) -> tuple[float, float]:
    city_key = normalize_city_name(city)
    meta = SUPPORTED_CITIES[city_key]
    return meta["lat"], meta["lon"]


def get_city_location(city: str) -> Location:
    """Resolve a supported demo city into a shared Location object."""
    city_key = normalize_city_name(city)
    meta = SUPPORTED_CITIES[city_key]
    return Location(
        name=meta["name"],
        latitude=meta["lat"],
        longitude=meta["lon"],
        country=meta.get("country"),
        country_code=meta.get("country_code"),
        admin1=meta.get("admin1"),
        timezone=meta.get("timezone"),
        population=meta.get("population"),
        elevation=meta.get("elevation"),
    )


def list_supported_city_locations() -> list[Location]:
    return [get_city_location(name) for name in sorted(SUPPORTED_CITIES)]


def list_supported_cities() -> list[str]:
    return [name.title() for name in sorted(SUPPORTED_CITIES)]
