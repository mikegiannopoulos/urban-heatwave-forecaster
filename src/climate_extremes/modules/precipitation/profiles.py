from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PrecipitationDefinition:
    name: str
    label: str
    quantile: float
    accumulation_days: int = 1
    min_run: int = 1
    min_absolute_mm: float = 0.0
    description: str = ""

    def with_floor(self, min_absolute_mm: float) -> "PrecipitationDefinition":
        floor_text = int(min_absolute_mm) if float(min_absolute_mm).is_integer() else min_absolute_mm
        accumulation_label = (
            f"{floor_text} mm/day"
            if self.accumulation_days == 1
            else f"{floor_text} mm/{self.accumulation_days}-day"
        )
        return PrecipitationDefinition(
            name=f"{self.name}-floor-{str(min_absolute_mm).replace('.', '_')}",
            label=f"{self.label} [{accumulation_label}]",
            quantile=self.quantile,
            accumulation_days=self.accumulation_days,
            min_run=self.min_run,
            min_absolute_mm=float(min_absolute_mm),
            description=(
                f"{self.label} calibrated with an absolute floor of {accumulation_label}."
            ),
        )


def quantile_suffix(quantile: float) -> str:
    return f"{int(round(quantile * 100))}p"


def accumulation_column_name(accumulation_days: int) -> str:
    return f"precipitation_sum_{accumulation_days}d"


def threshold_column_name(accumulation_days: int, quantile: float) -> str:
    return f"{accumulation_column_name(accumulation_days)}_{quantile_suffix(quantile)}"


DAILY_BURST_95P = PrecipitationDefinition(
    name="daily-burst-95p",
    label="Daily burst (95th percentile)",
    quantile=0.95,
    accumulation_days=1,
    min_run=1,
    min_absolute_mm=20.0,
    description="Sensitive single-day trigger for notable rainfall bursts, constrained by a 20 mm/day floor.",
)

DAILY_BURST_99P = PrecipitationDefinition(
    name="daily-burst-99p",
    label="Daily burst (99th percentile)",
    quantile=0.99,
    accumulation_days=1,
    min_run=1,
    min_absolute_mm=25.0,
    description="Stricter single-day trigger for only the rarest bursts, constrained by a 25 mm/day floor.",
)

WET_SPELL_3DAY_95P = PrecipitationDefinition(
    name="wet-spell-3day-95p",
    label="3-day wet spell (95th percentile)",
    quantile=0.95,
    accumulation_days=3,
    min_run=1,
    min_absolute_mm=40.0,
    description="Captures short-duration persistence through 3-day rainfall totals, constrained by a 40 mm/3-day floor.",
)

DEFAULT_PRECIPITATION_DEFINITIONS = (
    DAILY_BURST_95P,
    DAILY_BURST_99P,
    WET_SPELL_3DAY_95P,
)


def get_definition_by_name(name: str) -> PrecipitationDefinition:
    key = name.strip().lower()
    for definition in DEFAULT_PRECIPITATION_DEFINITIONS:
        if definition.name == key:
            return definition
    supported = ", ".join(item.name for item in DEFAULT_PRECIPITATION_DEFINITIONS)
    raise KeyError(f"Unknown precipitation definition '{name}'. Supported: {supported}")
