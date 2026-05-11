from __future__ import annotations

from typing import Iterable, Mapping

from climate_extremes.core.schemas import HazardAssessment
from climate_extremes.core.scales import SEVERITY_TO_RANK


def _coerce_assessment(
    assessment: HazardAssessment | Mapping[str, object],
) -> HazardAssessment:
    if isinstance(assessment, HazardAssessment):
        return assessment
    return HazardAssessment.from_mapping(assessment)


def summarize_hazards(
    assessments: Iterable[HazardAssessment | Mapping[str, object]],
) -> dict[str, object]:
    module_summaries = [_coerce_assessment(item) for item in assessments]
    if not module_summaries:
        return {
            "overall_status": "no module assessments available",
            "primary_hazard": None,
            "active_hazards": [],
            "hazard_count": 0,
            "summary_class": "none",
            "module_summaries": [],
        }

    active = [
        item for item in module_summaries if item.event_detected or item.severity_score > 0
    ]
    ranking_pool = active or module_summaries
    primary = max(
        ranking_pool,
        key=lambda item: (item.severity_score, SEVERITY_TO_RANK[item.severity_class]),
    )

    if not active:
        overall_status = "no active hazard conditions"
        summary_class = "none"
    elif len(active) == 1:
        overall_status = f"heightened {primary.hazard} conditions"
        summary_class = primary.severity_class
    else:
        overall_status = "elevated multi-hazard conditions"
        summary_class = primary.severity_class

    return {
        "overall_status": overall_status,
        "primary_hazard": primary.hazard if active else None,
        "active_hazards": [item.hazard for item in active],
        "hazard_count": len(active),
        "summary_class": summary_class,
        "module_summaries": [item.to_dict() for item in module_summaries],
    }
