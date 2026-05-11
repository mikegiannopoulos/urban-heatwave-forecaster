"""Heat module implementations and helpers."""

from climate_extremes.modules.heat.detection import detect_heatwaves, detect_heatwaves_df
from climate_extremes.modules.heat.risk import (
    HEAT_RISK_ORDER,
    assess_heatwave_risk,
    build_heat_assessment,
)

__all__ = [
    "HEAT_RISK_ORDER",
    "assess_heatwave_risk",
    "build_heat_assessment",
    "detect_heatwaves",
    "detect_heatwaves_df",
]
