from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from climate_extremes.core.scales import clip_score, coerce_severity_class

Scalar = str | int | float | bool | None


@dataclass(frozen=True)
class HazardAssessment:
    hazard: str
    event_detected: bool
    severity_score: float
    severity_class: str
    confidence: str = "medium"
    key_metrics: Mapping[str, Scalar] = field(default_factory=dict)
    metadata: Mapping[str, Scalar] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "hazard", str(self.hazard).strip().lower())
        object.__setattr__(self, "event_detected", bool(self.event_detected))
        object.__setattr__(self, "severity_score", clip_score(self.severity_score))
        object.__setattr__(
            self,
            "severity_class",
            coerce_severity_class(self.severity_class),
        )
        object.__setattr__(self, "confidence", str(self.confidence).strip().lower())
        object.__setattr__(self, "key_metrics", dict(self.key_metrics))
        object.__setattr__(self, "metadata", dict(self.metadata))

    @classmethod
    def from_mapping(cls, payload: Mapping[str, object]) -> "HazardAssessment":
        return cls(
            hazard=str(payload.get("hazard", "")),
            event_detected=bool(payload.get("event_detected", False)),
            severity_score=float(payload.get("severity_score", 0.0)),
            severity_class=str(payload.get("severity_class", "none")),
            confidence=str(payload.get("confidence", "medium")),
            key_metrics=dict(payload.get("key_metrics", {})),
            metadata=dict(payload.get("metadata", {})),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "hazard": self.hazard,
            "event_detected": self.event_detected,
            "severity_score": self.severity_score,
            "severity_class": self.severity_class,
            "confidence": self.confidence,
            "key_metrics": dict(self.key_metrics),
            "metadata": dict(self.metadata),
        }
