from __future__ import annotations

NORMALIZED_SEVERITY_ORDER = ("none", "low", "moderate", "high", "severe", "extreme")
SEVERITY_TO_RANK = {
    level: rank for rank, level in enumerate(NORMALIZED_SEVERITY_ORDER)
}

SEVERITY_SYNONYMS = {
    "none": "none",
    "mild": "low",
    "low": "low",
    "moderate": "moderate",
    "high": "high",
    "severe": "severe",
    "extreme": "extreme",
}


def clip_score(score: float) -> float:
    return round(max(0.0, min(100.0, float(score))), 2)


def coerce_severity_class(label: str) -> str:
    key = str(label).strip().lower()
    if key not in SEVERITY_SYNONYMS:
        supported = ", ".join(NORMALIZED_SEVERITY_ORDER)
        raise ValueError(f"Unknown severity class '{label}'. Supported: {supported}")
    return SEVERITY_SYNONYMS[key]


def severity_from_score(score: float) -> str:
    normalized_score = clip_score(score)
    if normalized_score == 0:
        return "none"
    if normalized_score < 25:
        return "low"
    if normalized_score < 50:
        return "moderate"
    if normalized_score < 75:
        return "high"
    if normalized_score < 90:
        return "severe"
    return "extreme"
