"""Heat module implementations and helpers."""

from climate_extremes.modules.heat.detection import detect_heatwaves, detect_heatwaves_df
from climate_extremes.modules.heat.risk import (
    HEAT_RISK_ORDER,
    assess_heatwave_risk,
    build_heat_assessment,
)
from climate_extremes.modules.heat.workflow import (
    HeatOutputPaths,
    assess_heatwave_risk_with_optional_vulnerability,
    heat_output_label,
    heat_output_paths,
    run_heat_for_location,
    run_heat_for_location_result,
)

__all__ = [
    "HEAT_RISK_ORDER",
    "assess_heatwave_risk",
    "build_heat_assessment",
    "detect_heatwaves",
    "detect_heatwaves_df",
    "HeatOutputPaths",
    "assess_heatwave_risk_with_optional_vulnerability",
    "heat_output_label",
    "heat_output_paths",
    "run_heat_for_location",
    "run_heat_for_location_result",
]
