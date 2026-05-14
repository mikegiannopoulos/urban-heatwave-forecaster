from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


@dataclass(frozen=True)
class Location:
    """Shared geographic location model for climate-extremes workflows."""

    name: str
    latitude: float
    longitude: float
    country: str | None = None
    country_code: str | None = None
    admin1: str | None = None
    timezone: str | None = None
    population: int | None = None
    elevation: float | None = None

    @property
    def slug(self) -> str:
        return location_slug(self)


def _slug_part(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text.lower()).strip("-")
    return re.sub(r"-+", "-", text)


def location_slug(location: Location) -> str:
    """Return a filesystem-safe slug that disambiguates common city names."""
    parts = [
        location.name,
        location.admin1,
        location.country_code or location.country,
    ]
    slug = "-".join(_slug_part(item) for item in parts if item)
    if slug:
        return slug
    return f"location-{location.latitude:.4f}-{location.longitude:.4f}".replace(".", "-")


def location_from_openmeteo_result(result: dict) -> Location:
    """Convert one Open-Meteo geocoding result item into a Location."""
    try:
        name = result["name"]
        latitude = result["latitude"]
        longitude = result["longitude"]
    except KeyError as exc:
        raise ValueError(
            f"Open-Meteo geocoding result missing required field: {exc.args[0]}"
        ) from exc

    return Location(
        name=str(name),
        latitude=float(latitude),
        longitude=float(longitude),
        country=result.get("country"),
        country_code=result.get("country_code"),
        admin1=result.get("admin1"),
        timezone=result.get("timezone"),
        population=_optional_int(result.get("population")),
        elevation=_optional_float(result.get("elevation")),
    )


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)
