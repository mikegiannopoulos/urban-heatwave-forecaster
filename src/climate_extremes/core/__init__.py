"""Shared platform utilities and schemas."""

from climate_extremes.core.locations import (
    Location,
    location_from_openmeteo_result,
    location_slug,
)
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.core.schemas import HazardAssessment
from climate_extremes.core.summary import summarize_hazards

__all__ = [
    "HazardAssessment",
    "HazardWorkflowResult",
    "Location",
    "location_from_openmeteo_result",
    "location_slug",
    "summarize_hazards",
]
