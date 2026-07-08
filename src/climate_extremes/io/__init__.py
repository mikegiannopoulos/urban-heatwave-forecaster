"""Input/output adapters for climate-extremes workflows."""

from climate_extremes.io.geocoding import (
    OpenMeteoGeocodingError,
    parse_openmeteo_geocoding_response,
    search_locations,
)

__all__ = [
    "OpenMeteoGeocodingError",
    "parse_openmeteo_geocoding_response",
    "search_locations",
]
