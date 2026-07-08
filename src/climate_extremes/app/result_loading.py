from __future__ import annotations

from pathlib import Path

import pandas as pd

from climate_extremes.core.results import HazardWorkflowResult

VULNERABILITY_COLUMNS = [
    "city",
    "elderly_percent",
    "green_cover_percent",
    "density_per_km2",
]


def empty_vulnerability_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=VULNERABILITY_COLUMNS)


def load_vulnerability_data(
    vulnerability_path: str | Path = "data/raw/urban_vulnerability.csv",
) -> pd.DataFrame:
    path = Path(vulnerability_path)
    if not path.exists():
        return empty_vulnerability_frame()
    return pd.read_csv(path)


def vulnerability_row_for_label(
    vulnerability_df: pd.DataFrame,
    output_label: str,
) -> pd.Series | None:
    if vulnerability_df.empty or "city" not in vulnerability_df.columns:
        return None
    cities = vulnerability_df["city"].astype(str).str.strip().str.lower()
    matches = vulnerability_df.loc[cities == output_label.lower()]
    if matches.empty:
        return None
    return matches.iloc[0]


def csv_path_from_result(
    result: HazardWorkflowResult,
    key: str,
    *,
    required: bool = True,
) -> Path | None:
    path = result.generated_files.get(key)
    if path is None:
        if required:
            raise FileNotFoundError(f"Workflow did not report a '{key}' output file.")
        return None
    if required and not path.exists():
        raise FileNotFoundError(f"Expected workflow output is missing: {path}")
    return path


def read_generated_csv(
    result: HazardWorkflowResult,
    file_key: str,
    *,
    parse_dates: list[str] | None = None,
) -> pd.DataFrame:
    path = csv_path_from_result(result, file_key, required=True)
    if path is None:
        raise FileNotFoundError(f"Workflow did not report a '{file_key}' output file.")
    return pd.read_csv(path, parse_dates=parse_dates)


def load_result_csvs(
    result: HazardWorkflowResult,
    required_keys: list[str] | tuple[str, ...],
    *,
    parse_dates: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    return {
        key: read_generated_csv(result, key, parse_dates=parse_dates)
        for key in required_keys
    }


def generated_files_summary(result: HazardWorkflowResult) -> list[str]:
    return [
        f"{label.replace('_', ' ').title()}: `{path}`"
        for label, path in result.generated_files.items()
    ]


def vulnerability_unavailable(
    vulnerability_df: pd.DataFrame,
    output_label: str,
) -> bool:
    return vulnerability_row_for_label(vulnerability_df, output_label) is None
