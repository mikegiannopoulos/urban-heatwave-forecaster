from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd

RISK_ORDER = ["None", "Mild", "Moderate", "High", "Extreme"]
RISK_TO_SCORE = {risk: score for score, risk in enumerate(RISK_ORDER)}
HEAT_RISK_EMOJI = {
    "Extreme": "🔥🔥",
    "High": "🔥",
    "Moderate": "🌡️",
    "Mild": "☀️",
    "None": "❄️",
}
HEAT_RISK_TABLE_COLUMNS = [
    "Date",
    "Tmax (°C)",
    "Temperature Class",
    "Final Heat Class",
    "",
]
PRECIPITATION_TABLE_COLUMNS = [
    "Date",
    "Daily Rain (mm)",
    "3-Day Total (mm)",
    "Threshold (mm)",
    "Event Flag",
    "Severity Class",
]
PROBABILISTIC_DISPLAY_COLUMNS = [
    "Date",
    "Models",
    "P(Heatwave)",
    "P(High+)",
    "P(Extreme)",
    "Most Likely Risk",
    "Consensus Risk (>=50%)",
    "Expected Risk Score",
]
HAZARD_STATUS_LABELS = {
    "no module assessments available": "No hazard modules available",
    "no active hazard conditions": "No hazard signal",
    "elevated multi-hazard conditions": "Multiple hazard signals",
}
CITY_COMPARISON_COLUMNS = [
    "city",
    "lat",
    "lon",
    "heatwave_days",
    "escalated_days",
    "peak_tmax",
    "peak_tmax_anomaly",
    "max_risk_score",
    "max_risk_level",
]
CITY_COMPARISON_TABLE_COLUMNS = [
    "City",
    "Max Heat Class",
    "Heatwave Days",
    "Peak Tmax (°C)",
    "Peak Tmax Anomaly (°C)",
]


@dataclass(frozen=True)
class ProbabilisticHeatDisplayData:
    probability_df: pd.DataFrame
    risk_probabilities: pd.DataFrame
    model_codes: list[str]


def base_risk_from_tmax(temp: float) -> str:
    if temp >= 38:
        return "Extreme"
    if temp >= 35:
        return "High"
    if temp >= 32:
        return "Moderate"
    if temp >= 30:
        return "Mild"
    return "None"


def prepare_heat_risk_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"])
    if "tmax" in out.columns:
        out["base_risk_level"] = out["tmax"].apply(base_risk_from_tmax)
    elif "base_risk_level" not in out.columns:
        out["base_risk_level"] = pd.NA

    out["base_risk_score"] = out["base_risk_level"].map(RISK_TO_SCORE)
    if "risk_level" in out.columns:
        out["adjusted_risk_score"] = out["risk_level"].map(RISK_TO_SCORE)
    else:
        out["risk_level"] = pd.NA
        out["adjusted_risk_score"] = pd.NA
    out["risk_escalated"] = out["adjusted_risk_score"] > out["base_risk_score"]
    return out


def prepare_temperature_display_frame(detected_df: pd.DataFrame) -> pd.DataFrame:
    frame = detected_df.copy()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    if "heatwave_id" in frame.columns:
        frame["Heatwave"] = frame["heatwave_id"].notna().map({True: "Yes", False: "No"})
    else:
        frame["Heatwave"] = "No"
    frame["tmax_anomaly"] = _difference_or_na(frame, "tmax", "tmax_95p")
    frame["tmin_anomaly"] = _difference_or_na(frame, "tmin", "tmin_95p")
    return frame


def heat_summary_metrics(
    temperature_df: pd.DataFrame,
    risk_df: pd.DataFrame,
) -> dict[str, Any]:
    heatwave_days = (
        int(temperature_df["heatwave_id"].notna().sum())
        if "heatwave_id" in temperature_df.columns
        else 0
    )
    extreme_days = (
        int((risk_df["risk_level"] == "Extreme").sum())
        if "risk_level" in risk_df.columns
        else 0
    )
    escalated_days = (
        int(risk_df["risk_escalated"].fillna(False).sum())
        if "risk_escalated" in risk_df.columns
        else 0
    )
    return {
        "heatwave_days": heatwave_days,
        "extreme_days": extreme_days,
        "escalated_days": escalated_days,
        "max_tmax": _safe_max(temperature_df, "tmax"),
        "max_tmax_anomaly": _safe_max(temperature_df, "tmax_anomaly"),
    }


def prepare_heat_assessment_payload(
    temperature_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    heat_metrics: dict[str, Any],
) -> dict[str, Any]:
    score, risk_level = _select_heat_summary_risk(risk_df)
    return {
        "hazard": "heat",
        "event_detected": bool(
            temperature_df["heatwave_id"].notna().any()
            if "heatwave_id" in temperature_df.columns
            else False
        ),
        "severity_score": float(score * 25),
        "severity_class": risk_level.lower(),
        "confidence": "medium",
        "key_metrics": {
            "heatwave_days": int(heat_metrics["heatwave_days"]),
            "peak_tmax_c": _round_optional(heat_metrics["max_tmax"]),
            "peak_tmax_anomaly_c": _round_optional(heat_metrics["max_tmax_anomaly"]),
        },
        "metadata": {"module": "heat"},
    }


def prepare_heat_risk_table(risk_df: pd.DataFrame) -> pd.DataFrame:
    display = risk_df.copy()
    _ensure_columns(
        display,
        {
            "date": pd.NaT,
            "tmax": pd.NA,
            "base_risk_level": pd.NA,
            "risk_level": pd.NA,
            "risk_escalated": False,
        },
    )
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%a, %b %d")
    display["icon"] = display["risk_level"].map(HEAT_RISK_EMOJI)

    table = display[
        ["date", "tmax", "base_risk_level", "risk_level", "icon"]
    ].copy()
    table.columns = HEAT_RISK_TABLE_COLUMNS
    return table.reset_index(drop=True)


def prepare_precipitation_plot_frame(risk_df: pd.DataFrame) -> pd.DataFrame:
    frame = risk_df.copy()
    if "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"])
    return frame


def prepare_precipitation_table(risk_df: pd.DataFrame) -> pd.DataFrame:
    display = prepare_precipitation_plot_frame(risk_df)
    _ensure_columns(
        display,
        {
            "date": pd.NaT,
            "precipitation_sum": pd.NA,
            "precip_accumulation_mm": pd.NA,
            "threshold_mm": pd.NA,
            "exceeds_threshold": pd.NA,
            "risk_level": pd.NA,
        },
    )
    display["date"] = pd.to_datetime(display["date"]).dt.strftime("%a, %b %d")
    table = display[
        [
            "date",
            "precipitation_sum",
            "precip_accumulation_mm",
            "threshold_mm",
            "exceeds_threshold",
            "risk_level",
        ]
    ].copy()
    table.columns = PRECIPITATION_TABLE_COLUMNS
    return table.reset_index(drop=True)


def prepare_probabilistic_heat_data(
    ensemble_risk_df: pd.DataFrame,
    risk_order: list[str] | tuple[str, ...] = RISK_ORDER,
) -> ProbabilisticHeatDisplayData:
    risk_levels = list(risk_order)
    if ensemble_risk_df.empty or "date" not in ensemble_risk_df.columns:
        empty_probs = pd.DataFrame(columns=risk_levels)
        return ProbabilisticHeatDisplayData(
            probability_df=_empty_probability_frame(),
            risk_probabilities=empty_probs,
            model_codes=[],
        )

    working = ensemble_risk_df.copy()
    working["date"] = pd.to_datetime(working["date"])
    _ensure_columns(
        working,
        {
            "model": "unknown",
            "heatwave_id": pd.NA,
            "risk_level": pd.NA,
            "adjusted_risk_score": pd.NA,
        },
    )

    working = working.sort_values(["date", "model"]).reset_index(drop=True)
    models_available = working.groupby("date")["model"].nunique().sort_index()

    if working["risk_level"].dropna().empty:
        risk_counts = pd.DataFrame(0, index=models_available.index, columns=risk_levels)
    else:
        risk_counts = (
            working.pivot_table(
                index="date",
                columns="risk_level",
                values="model",
                aggfunc="count",
                fill_value=0,
            )
            .reindex(columns=risk_levels, fill_value=0)
            .sort_index()
        )

    denominators = risk_counts.sum(axis=1).replace(0, pd.NA)
    risk_probs = risk_counts.div(denominators, axis=0).fillna(0.0)

    probability_df = pd.DataFrame(index=risk_probs.index)
    probability_df["models_available"] = models_available
    probability_df["p_heatwave"] = (
        working.groupby("date")["heatwave_id"]
        .apply(lambda series: series.notna().mean())
        .sort_index()
    )
    probability_df["p_high_plus"] = (
        working.groupby("date")["risk_level"]
        .apply(lambda series: series.isin(["High", "Extreme"]).mean())
        .sort_index()
    )
    probability_df["p_extreme"] = (
        working.groupby("date")["risk_level"]
        .apply(lambda series: (series == "Extreme").mean())
        .sort_index()
    )
    probability_df["expected_risk_score"] = (
        working.groupby("date")["adjusted_risk_score"].mean().sort_index()
    )
    probability_df["most_likely_risk"] = risk_probs.idxmax(axis=1)
    probability_df["consensus_risk"] = risk_probs.apply(
        lambda row: consensus_from_probabilities(row, risk_levels),
        axis=1,
    )

    return ProbabilisticHeatDisplayData(
        probability_df=probability_df,
        risk_probabilities=risk_probs,
        model_codes=list(dict.fromkeys(working["model"].astype(str))),
    )


def consensus_from_probabilities(
    probabilities: pd.Series,
    risk_order: list[str] | tuple[str, ...] = RISK_ORDER,
) -> str:
    for level in reversed(list(risk_order)):
        if probabilities.get(level, 0.0) >= 0.5:
            return level
    return "Uncertain"


def format_hazard_status(status: object) -> str:
    normalized = str(status or "").strip().lower()
    if normalized.startswith("heightened ") and normalized.endswith(" conditions"):
        hazard = normalized.removeprefix("heightened ").removesuffix(" conditions")
        return f"{hazard.title()} hazard signal"
    return HAZARD_STATUS_LABELS.get(normalized, str(status).replace("_", " ").title())


def format_heat_signal_message(heatwave_days: int, location_label: str) -> tuple[str, str]:
    if heatwave_days > 0:
        return (
            "success",
            f"Heat hazard signal detected: {heatwave_days} heatwave day(s) in the forecast window for {location_label}.",
        )
    return (
        "info",
        f"No heatwave signal in the forecast window for {location_label}.",
    )


def prepare_probabilistic_display_table(probability_df: pd.DataFrame) -> pd.DataFrame:
    if probability_df.empty:
        return pd.DataFrame(columns=PROBABILISTIC_DISPLAY_COLUMNS)

    display = probability_df.reset_index().copy()
    if "date" not in display.columns:
        first_column = display.columns[0]
        display = display.rename(columns={first_column: "date"})
    _ensure_columns(
        display,
        {
            "models_available": 0,
            "p_heatwave": 0.0,
            "p_high_plus": 0.0,
            "p_extreme": 0.0,
            "expected_risk_score": pd.NA,
            "most_likely_risk": pd.NA,
            "consensus_risk": pd.NA,
        },
    )
    display["Date"] = pd.to_datetime(display["date"]).dt.strftime("%a, %b %d")
    display["P(Heatwave)"] = (display["p_heatwave"] * 100).round(1)
    display["P(High+)"] = (display["p_high_plus"] * 100).round(1)
    display["P(Extreme)"] = (display["p_extreme"] * 100).round(1)
    display["Expected Risk Score"] = pd.to_numeric(
        display["expected_risk_score"],
        errors="coerce",
    ).round(2)
    display["Models"] = display["models_available"].fillna(0).astype(int)
    display["Most Likely Risk"] = display["most_likely_risk"]
    display["Consensus Risk (>=50%)"] = display["consensus_risk"]
    return display[PROBABILISTIC_DISPLAY_COLUMNS]


def prepare_city_comparison_row(
    city: str,
    lat: float,
    lon: float,
    detected_df: pd.DataFrame,
    risk_df: pd.DataFrame,
    risk_order: list[str] | tuple[str, ...] = RISK_ORDER,
) -> dict[str, object]:
    detected = prepare_temperature_display_frame(detected_df)
    risk = prepare_heat_risk_dataframe(risk_df)
    max_risk_score = _safe_max(risk, "adjusted_risk_score")
    max_risk_index = int(max_risk_score) if max_risk_score is not None else 0
    max_risk_index = min(max(max_risk_index, 0), len(risk_order) - 1)
    return {
        "city": city,
        "lat": lat,
        "lon": lon,
        "heatwave_days": (
            int(detected["heatwave_id"].notna().sum())
            if "heatwave_id" in detected.columns
            else 0
        ),
        "escalated_days": (
            int(risk["risk_escalated"].fillna(False).sum())
            if "risk_escalated" in risk.columns
            else 0
        ),
        "peak_tmax": _safe_max(detected, "tmax"),
        "peak_tmax_anomaly": _safe_max(detected, "tmax_anomaly"),
        "max_risk_score": max_risk_index,
        "max_risk_level": list(risk_order)[max_risk_index],
    }


def prepare_city_comparison_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=CITY_COMPARISON_COLUMNS)
    frame = pd.DataFrame(rows).reindex(columns=CITY_COMPARISON_COLUMNS)
    return frame.sort_values(
        ["max_risk_score", "peak_tmax"],
        ascending=[False, False],
        na_position="last",
    ).reset_index(drop=True)


def prepare_city_comparison_table(compare_df: pd.DataFrame) -> pd.DataFrame:
    if compare_df.empty:
        return pd.DataFrame(columns=CITY_COMPARISON_TABLE_COLUMNS)
    table = compare_df[
        [
            "city",
            "max_risk_level",
            "heatwave_days",
            "peak_tmax",
            "peak_tmax_anomaly",
        ]
    ].copy()
    table.columns = CITY_COMPARISON_TABLE_COLUMNS
    return table


def prepare_city_map_hover_text(compare_df: pd.DataFrame) -> pd.Series:
    if compare_df.empty:
        return pd.Series(dtype=object)
    return compare_df.apply(
        lambda row: (
            f"{row['city']}<br>"
            f"Max risk: {row['max_risk_level']}<br>"
            f"Peak Tmax: {_format_optional_temperature(row['peak_tmax'])}<br>"
            f"Heatwave days: {row['heatwave_days']}"
        ),
        axis=1,
    )


def _difference_or_na(frame: pd.DataFrame, left: str, right: str) -> pd.Series:
    if left not in frame.columns or right not in frame.columns:
        return pd.Series([pd.NA] * len(frame), index=frame.index)
    return frame[left] - frame[right]


def _ensure_columns(frame: pd.DataFrame, defaults: dict[str, object]) -> None:
    for column, default in defaults.items():
        if column not in frame.columns:
            frame[column] = default


def _safe_max(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns or frame.empty:
        return None
    value = frame[column].max()
    if pd.isna(value):
        return None
    result = float(value)
    return None if math.isnan(result) else result


def _select_heat_summary_risk(risk_df: pd.DataFrame) -> tuple[float, str]:
    if risk_df.empty:
        return 0.0, "None"

    for score_column, label_column in (
        ("adjusted_risk_score", "risk_level"),
        ("base_risk_score", "base_risk_level"),
    ):
        selection = _select_risk_from_score_column(risk_df, score_column, label_column)
        if selection is not None:
            return selection

    for label_column in ("risk_level", "base_risk_level"):
        selection = _select_risk_from_label_column(risk_df, label_column)
        if selection is not None:
            return selection

    return 0.0, "None"


def _select_risk_from_score_column(
    risk_df: pd.DataFrame,
    score_column: str,
    label_column: str,
) -> tuple[float, str] | None:
    if score_column not in risk_df.columns:
        return None
    scores = pd.to_numeric(risk_df[score_column], errors="coerce")
    valid_scores = scores.dropna()
    if valid_scores.empty:
        return None
    index = valid_scores.idxmax()
    score = float(valid_scores.loc[index])
    label = _label_for_risk_row(risk_df, index, label_column, score)
    return score, label


def _select_risk_from_label_column(
    risk_df: pd.DataFrame,
    label_column: str,
) -> tuple[float, str] | None:
    if label_column not in risk_df.columns:
        return None
    labels = risk_df[label_column].map(_normalize_heat_risk_label)
    scores = labels.map(RISK_TO_SCORE)
    valid_scores = scores.dropna()
    if valid_scores.empty:
        return None
    index = valid_scores.idxmax()
    score = float(valid_scores.loc[index])
    label = labels.loc[index]
    return score, label if isinstance(label, str) else "None"


def _label_for_risk_row(
    risk_df: pd.DataFrame,
    index: object,
    label_column: str,
    score: float,
) -> str:
    if label_column in risk_df.columns:
        label = _normalize_heat_risk_label(risk_df.at[index, label_column])
        if label is not None:
            return label
    score_index = int(score)
    score_index = min(max(score_index, 0), len(RISK_ORDER) - 1)
    return RISK_ORDER[score_index]


def _normalize_heat_risk_label(value: object) -> str | None:
    if pd.isna(value):
        return None
    normalized = str(value).strip().lower()
    for label in RISK_ORDER:
        if normalized == label.lower():
            return label
    return None


def _round_optional(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 1)


def _empty_probability_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "models_available",
            "p_heatwave",
            "p_high_plus",
            "p_extreme",
            "expected_risk_score",
            "most_likely_risk",
            "consensus_risk",
        ]
    )


def _format_optional_float(value: object) -> str:
    if value is None or pd.isna(value):
        return "Unavailable"
    return f"{float(value):.1f}"


def _format_optional_temperature(value: object) -> str:
    formatted = _format_optional_float(value)
    return formatted if formatted == "Unavailable" else f"{formatted}°C"
