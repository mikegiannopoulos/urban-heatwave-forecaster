from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from climate_extremes.core.locations import Location


@dataclass(frozen=True)
class HazardWorkflowResult:
    """Structured metadata returned by backend hazard workflow runs."""

    hazard: str
    location: Location
    output_label: str
    status: str = "completed"
    is_experimental: bool = False
    generated_files: dict[str, Path] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def generated_file_names(self) -> dict[str, str]:
        return {key: path.name for key, path in self.generated_files.items()}

    def existing_generated_files(self) -> dict[str, Path]:
        return {
            key: path
            for key, path in self.generated_files.items()
            if path.exists()
        }

    def short_summary(self) -> str:
        status = f"{self.hazard}: {self.status}"
        if self.is_experimental:
            status += " (experimental)"
        return f"{status} for {self.location.name} [{self.output_label}]"

    def as_dict(self) -> dict[str, Any]:
        return {
            "hazard": self.hazard,
            "location": asdict(self.location),
            "output_label": self.output_label,
            "status": self.status,
            "is_experimental": self.is_experimental,
            "generated_files": {
                key: str(path) for key, path in self.generated_files.items()
            },
            "summary": dict(self.summary),
            "warnings": list(self.warnings),
        }
