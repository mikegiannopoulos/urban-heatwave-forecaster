from __future__ import annotations

from climate_extremes.core.cities import get_city_coordinates, get_city_location
from climate_extremes.core.locations import (
    Location,
    location_from_openmeteo_result,
    location_slug,
)


def test_location_slug_is_filesystem_safe_and_disambiguated():
    location = Location(
        name="São Paulo",
        latitude=-23.5505,
        longitude=-46.6333,
        country="Brazil",
        country_code="BR",
        admin1="São Paulo",
    )

    assert location_slug(location) == "sao-paulo-sao-paulo-br"
    assert location.slug == "sao-paulo-sao-paulo-br"


def test_location_from_openmeteo_result_parses_optional_fields():
    result = {
        "name": "Gothenburg",
        "latitude": 57.7072,
        "longitude": 11.9668,
        "country": "Sweden",
        "country_code": "SE",
        "admin1": "Västra Götaland County",
        "timezone": "Europe/Stockholm",
        "population": 572799,
        "elevation": 13.0,
    }

    location = location_from_openmeteo_result(result)

    assert location.name == "Gothenburg"
    assert location.latitude == 57.7072
    assert location.longitude == 11.9668
    assert location.country == "Sweden"
    assert location.country_code == "SE"
    assert location.admin1 == "Västra Götaland County"
    assert location.timezone == "Europe/Stockholm"
    assert location.population == 572799
    assert location.elevation == 13.0


def test_demo_city_resolution_still_matches_coordinate_registry():
    location = get_city_location("Athens")
    lat, lon = get_city_coordinates("Athens")

    assert location.name == "Athens"
    assert location.country_code == "GR"
    assert location.timezone == "Europe/Athens"
    assert (location.latitude, location.longitude) == (lat, lon)
