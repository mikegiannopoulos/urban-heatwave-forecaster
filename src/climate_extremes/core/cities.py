from __future__ import annotations

SUPPORTED_CITIES = {
    "athens": {"lat": 37.9838, "lon": 23.7278},
    "rome": {"lat": 41.8919, "lon": 12.5113},
    "stockholm": {"lat": 59.3294, "lon": 18.0687},
    "london": {"lat": 51.5085, "lon": -0.1257},
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


def list_supported_cities() -> list[str]:
    return [name.title() for name in sorted(SUPPORTED_CITIES)]
