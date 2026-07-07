from __future__ import annotations

import importlib
import sys

import pandas as pd

from climate_extremes.app.display_data import (
    CITY_COMPARISON_TABLE_COLUMNS,
    HEAT_RISK_TABLE_COLUMNS,
    PRECIPITATION_TABLE_COLUMNS,
    PROBABILISTIC_DISPLAY_COLUMNS,
    heat_summary_metrics,
    prepare_city_comparison_frame,
    prepare_city_comparison_row,
    prepare_city_comparison_table,
    prepare_city_map_hover_text,
    prepare_heat_risk_dataframe,
    prepare_heat_risk_table,
    prepare_probabilistic_display_table,
    prepare_probabilistic_heat_data,
    prepare_precipitation_plot_frame,
    prepare_precipitation_table,
    prepare_temperature_display_frame,
)


def test_display_data_module_does_not_import_streamlit():
    sys.modules.pop("streamlit", None)
    importlib.import_module("climate_extremes.app.display_data")

    assert "streamlit" not in sys.modules


def test_prepare_temperature_display_frame_adds_dates_flags_and_anomalies():
    detected_df = pd.DataFrame(
        {
            "date": ["2030-07-01", "2030-07-02"],
            "tmin": [20.0, 22.0],
            "tmax": [34.0, 36.0],
            "tmin_95p": [19.0, 20.0],
            "tmax_95p": [33.0, 34.0],
            "heatwave_id": [pd.NA, 1],
        }
    )

    frame = prepare_temperature_display_frame(detected_df)

    assert pd.api.types.is_datetime64_any_dtype(frame["date"])
    assert frame["Heatwave"].tolist() == ["No", "Yes"]
    assert frame["tmax_anomaly"].tolist() == [1.0, 2.0]
    assert frame["tmin_anomaly"].tolist() == [1.0, 2.0]


def test_prepare_temperature_display_frame_handles_missing_optional_columns():
    frame = prepare_temperature_display_frame(
        pd.DataFrame({"date": ["2030-07-01"], "tmax": [34.0]})
    )

    assert frame["Heatwave"].tolist() == ["No"]
    assert frame["tmax_anomaly"].isna().all()
    assert frame["tmin_anomaly"].isna().all()


def test_prepare_heat_risk_dataframe_and_metrics():
    risk_df = pd.DataFrame(
        {
            "date": ["2030-07-01", "2030-07-02"],
            "tmax": [31.0, 36.0],
            "risk_level": ["Mild", "Extreme"],
        }
    )
    temperature_df = pd.DataFrame(
        {
            "tmax": [31.0, 36.0],
            "tmax_anomaly": [1.0, 3.0],
            "heatwave_id": [pd.NA, 1],
        }
    )

    prepared = prepare_heat_risk_dataframe(risk_df)
    metrics = heat_summary_metrics(temperature_df, prepared)

    assert prepared["base_risk_level"].tolist() == ["Mild", "High"]
    assert prepared["risk_escalated"].tolist() == [False, True]
    assert metrics == {
        "heatwave_days": 1,
        "extreme_days": 1,
        "escalated_days": 1,
        "max_tmax": 36.0,
        "max_tmax_anomaly": 3.0,
    }


def test_prepare_heat_risk_table_columns_and_missing_escalation():
    risk_df = prepare_heat_risk_dataframe(
        pd.DataFrame(
            {
                "date": ["2030-07-01"],
                "tmax": [36.0],
                "risk_level": ["High"],
            }
        )
    ).drop(columns=["risk_escalated"])

    table = prepare_heat_risk_table(risk_df)

    assert table.columns.tolist() == HEAT_RISK_TABLE_COLUMNS
    assert table.loc[0, "Date"] == pd.Timestamp("2030-07-01").strftime("%a, %b %d")
    assert table.loc[0, "Final Risk"] == "High"
    assert table.loc[0, "Vulnerability Lift"] == ""


def test_prepare_precipitation_plot_and_table_frames():
    risk_df = pd.DataFrame(
        {
            "date": ["2030-07-01"],
            "precipitation_sum": [42.0],
            "precip_accumulation_mm": [75.0],
            "threshold_mm": [50.0],
            "exceeds_threshold": [True],
            "risk_level": ["high"],
        }
    )

    plot_frame = prepare_precipitation_plot_frame(risk_df)
    table = prepare_precipitation_table(risk_df)

    assert pd.api.types.is_datetime64_any_dtype(plot_frame["date"])
    assert table.columns.tolist() == PRECIPITATION_TABLE_COLUMNS
    assert table.loc[0, "Date"] == pd.Timestamp("2030-07-01").strftime("%a, %b %d")
    assert table.loc[0, "Daily Rain (mm)"] == 42.0


def test_prepare_precipitation_table_handles_missing_optional_columns_and_empty_frame():
    missing_columns = prepare_precipitation_table(
        pd.DataFrame({"date": ["2030-07-01"], "precipitation_sum": [0.0]})
    )
    empty = prepare_precipitation_table(pd.DataFrame())

    assert missing_columns.columns.tolist() == PRECIPITATION_TABLE_COLUMNS
    assert pd.isna(missing_columns.loc[0, "3-Day Total (mm)"])
    assert empty.columns.tolist() == PRECIPITATION_TABLE_COLUMNS
    assert empty.empty


def test_prepare_probabilistic_heat_data_and_display_table():
    ensemble_df = pd.DataFrame(
        {
            "date": [
                "2030-07-01",
                "2030-07-01",
                "2030-07-02",
                "2030-07-02",
            ],
            "model": ["ecmwf", "gfs", "ecmwf", "gfs"],
            "heatwave_id": [1, pd.NA, 2, 2],
            "risk_level": ["High", "None", "Extreme", "High"],
            "adjusted_risk_score": [3, 0, 4, 3],
        }
    )

    prepared = prepare_probabilistic_heat_data(ensemble_df)
    probability_df = prepared.probability_df
    table = prepare_probabilistic_display_table(probability_df)

    assert prepared.model_codes == ["ecmwf", "gfs"]
    assert probability_df.loc[pd.Timestamp("2030-07-01"), "models_available"] == 2
    assert probability_df.loc[pd.Timestamp("2030-07-01"), "p_heatwave"] == 0.5
    assert probability_df.loc[pd.Timestamp("2030-07-01"), "p_high_plus"] == 0.5
    assert probability_df.loc[pd.Timestamp("2030-07-02"), "p_extreme"] == 0.5
    assert probability_df.loc[pd.Timestamp("2030-07-02"), "consensus_risk"] == "Extreme"
    assert table.columns.tolist() == PROBABILISTIC_DISPLAY_COLUMNS
    assert table.loc[0, "Date"] == pd.Timestamp("2030-07-01").strftime("%a, %b %d")
    assert table.loc[0, "P(Heatwave)"] == 50.0


def test_prepare_probabilistic_heat_data_handles_missing_optional_columns():
    prepared = prepare_probabilistic_heat_data(
        pd.DataFrame({"date": ["2030-07-01", "2030-07-02"]})
    )
    table = prepare_probabilistic_display_table(prepared.probability_df)

    assert prepared.model_codes == ["unknown"]
    assert prepared.probability_df["models_available"].tolist() == [1, 1]
    assert prepared.probability_df["p_heatwave"].tolist() == [0.0, 0.0]
    assert prepared.probability_df["p_high_plus"].tolist() == [0.0, 0.0]
    assert table.columns.tolist() == PROBABILISTIC_DISPLAY_COLUMNS


def test_prepare_probabilistic_heat_data_handles_empty_frame():
    prepared = prepare_probabilistic_heat_data(pd.DataFrame())
    table = prepare_probabilistic_display_table(prepared.probability_df)

    assert prepared.model_codes == []
    assert prepared.probability_df.empty
    assert prepared.risk_probabilities.empty
    assert table.columns.tolist() == PROBABILISTIC_DISPLAY_COLUMNS
    assert table.empty


def test_prepare_city_comparison_data_and_display_table():
    athens_detected = pd.DataFrame(
        {
            "date": ["2030-07-01", "2030-07-02"],
            "tmax": [34.0, 39.0],
            "tmax_95p": [32.0, 35.0],
            "heatwave_id": [pd.NA, 1],
        }
    )
    athens_risk = pd.DataFrame(
        {
            "date": ["2030-07-01", "2030-07-02"],
            "tmax": [34.0, 39.0],
            "risk_level": ["Moderate", "Extreme"],
        }
    )
    london_detected = pd.DataFrame(
        {
            "date": ["2030-07-01"],
            "tmax": [31.0],
            "tmax_95p": [30.0],
            "heatwave_id": [pd.NA],
        }
    )
    london_risk = pd.DataFrame(
        {
            "date": ["2030-07-01"],
            "tmax": [31.0],
            "risk_level": ["Mild"],
        }
    )

    rows = [
        prepare_city_comparison_row(
            "London",
            51.5,
            -0.1,
            london_detected,
            london_risk,
        ),
        prepare_city_comparison_row(
            "Athens",
            37.9,
            23.7,
            athens_detected,
            athens_risk,
        ),
    ]
    frame = prepare_city_comparison_frame(rows)
    table = prepare_city_comparison_table(frame)
    hover_text = prepare_city_map_hover_text(frame)

    assert frame["city"].tolist() == ["Athens", "London"]
    assert frame.loc[0, "heatwave_days"] == 1
    assert frame.loc[0, "max_risk_level"] == "Extreme"
    assert table.columns.tolist() == CITY_COMPARISON_TABLE_COLUMNS
    assert table.loc[0, "City"] == "Athens"
    assert "Peak Tmax: 39.0°C" in hover_text.iloc[0]


def test_prepare_city_comparison_data_handles_missing_outputs():
    row = prepare_city_comparison_row(
        "Missing City",
        0.0,
        0.0,
        pd.DataFrame(),
        pd.DataFrame(),
    )
    frame = prepare_city_comparison_frame([row])
    table = prepare_city_comparison_table(frame)
    hover_text = prepare_city_map_hover_text(frame)

    assert row["heatwave_days"] == 0
    assert row["escalated_days"] == 0
    assert row["peak_tmax"] is None
    assert row["max_risk_level"] == "None"
    assert table.columns.tolist() == CITY_COMPARISON_TABLE_COLUMNS
    assert "Peak Tmax: Unavailable" in hover_text.iloc[0]
