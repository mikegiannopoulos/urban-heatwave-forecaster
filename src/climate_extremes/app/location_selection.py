from __future__ import annotations

from climate_extremes.core.locations import Location

GLOBAL_SEARCH_MODE = "Global search"
COORDINATES_MODE = "Coordinates"
DEFAULT_LOCATION_MODE = GLOBAL_SEARCH_MODE
MAIN_LOCATION_MODES = (GLOBAL_SEARCH_MODE, COORDINATES_MODE)
ADVANCED_DEMO_SECTION_LABEL = "Advanced demo tools"


def format_location_label(location: Location) -> str:
    parts = [location.name]
    if location.admin1:
        parts.append(location.admin1)
    if location.country:
        parts.append(location.country)
    return ", ".join(parts)


def format_location_details(location: Location) -> str:
    details = f"{location.latitude:.4f}, {location.longitude:.4f}"
    if location.timezone:
        details = f"{details} · {location.timezone}"
    return details


def format_candidate_label(location: Location) -> str:
    label = format_location_label(location)
    if location.country_code and location.country_code not in label:
        label = f"{label} ({location.country_code})"
    return f"{label} · {location.latitude:.4f}, {location.longitude:.4f}"


def filter_locations_by_country_code(
    locations: list[Location],
    country_code: str = "",
) -> list[Location]:
    normalized_country = country_code.strip().upper()
    if not normalized_country:
        return locations
    return [
        location
        for location in locations
        if location.country_code
        and location.country_code.upper() == normalized_country
    ]


def validate_coordinate_location_input(
    name: str,
    latitude: float,
    longitude: float,
    country_code: str = "",
    timezone: str = "",
) -> Location:
    if not -90.0 <= float(latitude) <= 90.0:
        raise ValueError("Latitude must be between -90 and 90.")
    if not -180.0 <= float(longitude) <= 180.0:
        raise ValueError("Longitude must be between -180 and 180.")

    cleaned_name = name.strip() or "Custom location"
    cleaned_country_code = country_code.strip().upper() or None
    if cleaned_country_code and len(cleaned_country_code) != 2:
        raise ValueError("Country code must be a two-letter ISO code.")

    return Location(
        name=cleaned_name,
        latitude=float(latitude),
        longitude=float(longitude),
        country_code=cleaned_country_code,
        timezone=timezone.strip() or None,
    )


def is_location_selection_complete(location: Location | None) -> bool:
    return location is not None
