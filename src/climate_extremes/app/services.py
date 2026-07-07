from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from climate_extremes.app.result_loading import (
    load_result_csvs,
    load_vulnerability_data,
)
from climate_extremes.core.locations import Location
from climate_extremes.core.results import HazardWorkflowResult
from climate_extremes.modules.heat.workflow import run_heat_for_location_result
from climate_extremes.modules.precipitation.profiles import (
    PrecipitationDefinition,
)
from climate_extremes.modules.precipitation.risk import (
    build_precipitation_assessment_from_frame,
)
from climate_extremes.modules.precipitation.workflow import (
    run_precipitation_for_location_result,
)


@dataclass(frozen=True)
class HeatAppData:
    result: HazardWorkflowResult
    detected_df: pd.DataFrame
    risk_df: pd.DataFrame
    vulnerability_df: pd.DataFrame
    climatology_path: Path

    @property
    def output_label(self) -> str:
        return self.result.output_label


@dataclass(frozen=True)
class PrecipitationAppData:
    result: HazardWorkflowResult
    detected_df: pd.DataFrame
    risk_df: pd.DataFrame
    assessment: object


def prepare_heat_app_data(
    result: HazardWorkflowResult,
    *,
    vulnerability_path: str | Path = "data/raw/urban_vulnerability.csv",
) -> HeatAppData:
    frames = load_result_csvs(result, ["detected", "risk"], parse_dates=["date"])
    detected_df = frames["detected"]
    risk_df = frames["risk"]

    if "is_hot" not in detected_df.columns and "exceeds_95p" in detected_df.columns:
        detected_df = detected_df.copy()
        detected_df["is_hot"] = detected_df["exceeds_95p"]

    climatology_path = result.generated_files.get("climatology")
    if climatology_path is None:
        raise FileNotFoundError("Workflow did not report a 'climatology' output file.")

    return HeatAppData(
        result=result,
        detected_df=detected_df,
        risk_df=risk_df,
        vulnerability_df=load_vulnerability_data(vulnerability_path),
        climatology_path=climatology_path,
    )


def run_heat_app_workflow(
    location: Location,
    *,
    vulnerability_path: str | Path = "data/raw/urban_vulnerability.csv",
) -> HeatAppData:
    result = run_heat_for_location_result(
        location,
        vulnerability_path=vulnerability_path,
    )
    return prepare_heat_app_data(result, vulnerability_path=vulnerability_path)


def prepare_precipitation_app_data(
    result: HazardWorkflowResult,
    definition: PrecipitationDefinition,
) -> PrecipitationAppData:
    frames = load_result_csvs(result, ["detected", "risk"], parse_dates=["date"])
    risk_df = frames["risk"]
    return PrecipitationAppData(
        result=result,
        detected_df=frames["detected"],
        risk_df=risk_df,
        assessment=build_precipitation_assessment_from_frame(risk_df, definition),
    )


def run_precipitation_app_workflow(
    location: Location,
    *,
    definition: PrecipitationDefinition,
) -> PrecipitationAppData:
    result = run_precipitation_for_location_result(location, definition=definition)
    return prepare_precipitation_app_data(result, definition)
