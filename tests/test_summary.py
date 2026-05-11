from __future__ import annotations

from climate_extremes.core.summary import summarize_hazards
from climate_extremes.modules.precipitation import build_precipitation_assessment


def test_summarize_hazards_prefers_primary_active_hazard():
    summary = summarize_hazards(
        [
            {
                "hazard": "heat",
                "event_detected": True,
                "severity_score": 78,
                "severity_class": "severe",
                "confidence": "medium",
                "key_metrics": {"duration_days": 4},
            },
            build_precipitation_assessment(
                event_detected=True,
                severity_score=52,
                severity_class="high",
                key_metrics={"daily_total_mm": 48},
            ),
        ]
    )

    assert summary["overall_status"] == "elevated multi-hazard conditions"
    assert summary["primary_hazard"] == "heat"
    assert summary["hazard_count"] == 2
    assert summary["summary_class"] == "severe"
