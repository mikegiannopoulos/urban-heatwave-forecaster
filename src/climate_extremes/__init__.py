"""Modular tooling for climate-extremes detection and assessment."""

from climate_extremes.core.schemas import HazardAssessment
from climate_extremes.core.summary import summarize_hazards

__all__ = ["HazardAssessment", "summarize_hazards"]
