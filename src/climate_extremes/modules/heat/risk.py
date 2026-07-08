from __future__ import annotations

import pandas as pd

from climate_extremes.core.schemas import HazardAssessment
from climate_extremes.core.scales import severity_from_score

HEAT_RISK_ORDER = ["None", "Mild", "Moderate", "High", "Extreme"]
HEAT_RISK_TO_SHARED_SCORE = {
    "None": 0.0,
    "Mild": 20.0,
    "Moderate": 40.0,
    "High": 60.0,
    "Extreme": 80.0,
}


def categorize_heat_risk(temp: float) -> str:
    if temp >= 38:
        return "Extreme"
    if temp >= 35:
        return "High"
    if temp >= 32:
        return "Moderate"
    if temp >= 30:
        return "Mild"
    return "None"


def assess_heatwave_risk(
    df: pd.DataFrame,
    vulnerability_df: pd.DataFrame,
) -> pd.DataFrame:
    """Assign heat risk levels and adjust them using urban vulnerability data."""
    required_forecast_columns = {"tmax", "city"}
    required_vulnerability_columns = {
        "city",
        "elderly_percent",
        "green_cover_percent",
        "density_per_km2",
    }

    missing_forecast = required_forecast_columns - set(df.columns)
    missing_vulnerability = required_vulnerability_columns - set(vulnerability_df.columns)
    if missing_forecast:
        missing = ", ".join(sorted(missing_forecast))
        raise ValueError(f"Missing forecast columns for heat risk assessment: {missing}")
    if missing_vulnerability:
        missing = ", ".join(sorted(missing_vulnerability))
        raise ValueError(
            f"Missing vulnerability columns for heat risk assessment: {missing}"
        )

    forecast = df.copy()
    vulnerability = vulnerability_df.copy()
    forecast["risk_level"] = forecast["tmax"].apply(categorize_heat_risk)

    forecast["city"] = forecast["city"].astype(str).str.strip().str.lower()
    vulnerability["city"] = vulnerability["city"].astype(str).str.strip().str.lower()

    forecast = forecast.merge(vulnerability, on="city", how="left")
    forecast["high_vulnerability"] = (
        (forecast["elderly_percent"] > 20)
        | (forecast["density_per_km2"] > 2000)
        | (forecast["green_cover_percent"] < 25)
    )

    escalation_map = {
        "None": "Mild",
        "Mild": "Moderate",
        "Moderate": "High",
        "High": "Extreme",
        "Extreme": "Extreme",
    }
    high_vulnerability = forecast["high_vulnerability"].fillna(False)
    forecast.loc[high_vulnerability, "risk_level"] = forecast.loc[
        high_vulnerability, "risk_level"
    ].map(escalation_map)
    return forecast


def build_heat_assessment(df: pd.DataFrame) -> HazardAssessment:
    """Create the shared module contract for the heat hazard."""
    if "heatwave_id" not in df.columns:
        raise ValueError("Heat assessment requires a 'heatwave_id' column.")

    working = df.copy()
    heatwave_mask = working["heatwave_id"].notna()
    if not heatwave_mask.any():
        return HazardAssessment(
            hazard="heat",
            event_detected=False,
            severity_score=0.0,
            severity_class="none",
            confidence="medium",
            key_metrics={
                "heatwave_days": 0,
                "duration_days": 0,
                "threshold_exceedance": 0.0,
            },
            metadata={"legacy_risk_level": "None"},
        )

    heatwave_days = working.loc[heatwave_mask].copy()
    if "risk_level" in heatwave_days.columns:
        max_risk_label = max(
            heatwave_days["risk_level"].dropna(),
            key=lambda label: HEAT_RISK_TO_SHARED_SCORE.get(label, 0.0),
            default="None",
        )
        base_score = HEAT_RISK_TO_SHARED_SCORE.get(max_risk_label, 0.0)
    else:
        max_risk_label = "None"
        base_score = 0.0

    if {"tmax", "tmax_95p"}.issubset(heatwave_days.columns):
        peak_exceedance = float(
            (heatwave_days["tmax"] - heatwave_days["tmax_95p"]).max()
        )
    else:
        peak_exceedance = 0.0

    longest_run = int(heatwave_days.groupby("heatwave_id").size().max())
    duration_bonus = min(max(longest_run - 3, 0) * 5.0, 15.0)
    exceedance_bonus = min(max(peak_exceedance, 0.0) * 4.0, 12.0)

    if base_score == 0.0:
        base_score = min(max(peak_exceedance, 0.0) * 15.0 + longest_run * 8.0, 80.0)

    severity_score = min(base_score + duration_bonus + exceedance_bonus, 100.0)
    confidence = "high" if peak_exceedance >= 3.0 or longest_run >= 5 else "medium"

    return HazardAssessment(
        hazard="heat",
        event_detected=True,
        severity_score=severity_score,
        severity_class=severity_from_score(severity_score),
        confidence=confidence,
        key_metrics={
            "heatwave_days": int(heatwave_mask.sum()),
            "duration_days": longest_run,
            "threshold_exceedance": round(peak_exceedance, 2),
        },
        metadata={"legacy_risk_level": max_risk_label},
    )
