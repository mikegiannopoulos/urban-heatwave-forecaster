import streamlit as st
import pandas as pd
import sys
from PIL import Image
from pathlib import Path
from datetime import timedelta
import time
import plotly.graph_objects as go

# Ensure local src/ is first so deployed envs don't import stale installed packages.
SRC_PATH = str(Path(__file__).resolve().parent / "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from urban_heatwave_forecaster import data_fetcher, detect_heatwaves, risk_model
from climate_extremes.app.display_data import (
    format_hazard_status,
    format_heat_signal_message,
    heat_summary_metrics,
    prepare_heat_assessment_payload,
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
from climate_extremes.app.location_selection import (
    ADVANCED_DEMO_SECTION_LABEL,
    COORDINATES_MODE,
    DEFAULT_LOCATION_MODE,
    GLOBAL_SEARCH_MODE,
    MAIN_LOCATION_MODES,
    filter_locations_by_country_code,
    format_candidate_label,
    format_location_details,
    format_location_label,
    validate_coordinate_location_input,
)
from climate_extremes.app.result_loading import (
    generated_files_summary,
)
from climate_extremes.app.services import (
    run_heat_app_workflow,
    run_precipitation_app_workflow,
)
from climate_extremes.core.cities import get_city_location
from climate_extremes.core.locations import Location
from climate_extremes.core.summary import summarize_hazards
from climate_extremes.io.geocoding import OpenMeteoGeocodingError, search_locations
from climate_extremes.modules.precipitation import (
    WET_SPELL_3DAY_95P,
)

RISK_ORDER = ["None", "Mild", "Moderate", "High", "Extreme"]
RISK_COLORS = {
    "None": "#a8ddb5",
    "Mild": "#fee08b",
    "Moderate": "#fdae61",
    "High": "#f46d43",
    "Extreme": "#d73027",
}
MODEL_OPTIONS = {
    "ECMWF IFS 0.25°": "ecmwf_ifs025",
    "GFS Seamless": "gfs_seamless",
    "ICON Seamless": "icon_seamless",
}
MODEL_LABEL_BY_CODE = {code: label for label, code in MODEL_OPTIONS.items()}
SHARED_CLASS_COLORS = {
    "none": "#9db4a2",
    "low": "#d9e78f",
    "moderate": "#f7c66f",
    "high": "#f08a5d",
    "severe": "#d95763",
    "extreme": "#9c2f4f",
}
DEMO_CITY_NAMES = ["Athens", "Rome", "Stockholm", "London"]


@st.cache_data(ttl=3600, show_spinner=False)
def cached_location_search(query: str, country_code: str = "") -> list[Location]:
    candidates = search_locations(query)
    return filter_locations_by_country_code(candidates, country_code)


def format_shared_class(label: str) -> str:
    return str(label).replace("_", " ").title()


def fetch_multi_model_forecast_compat(
    lat: float,
    lon: float,
    city_name: str,
    models: list[str],
    forecast_days: int = 7,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    if hasattr(data_fetcher, "fetch_multi_model_forecast"):
        return data_fetcher.fetch_multi_model_forecast(
            lat=lat,
            lon=lon,
            city_name=city_name,
            models=models,
            forecast_days=forecast_days,
        )

    # Backward-compatible fallback for environments that have only per-model fetch.
    if hasattr(data_fetcher, "fetch_forecast_for_model"):
        frames = []
        failures: list[dict[str, str]] = []
        for model in models:
            try:
                out = data_fetcher.fetch_forecast_for_model(
                    lat=lat,
                    lon=lon,
                    city_name=city_name,
                    model=model,
                    forecast_days=forecast_days,
                    save_path=Path(f"data/raw/{city_name.lower()}_{model}_forecast.csv"),
                    include_model_col=True,
                )
                frames.append(out)
            except Exception as exc:
                failures.append({"model": model, "error": str(exc)})

        if not frames:
            raise RuntimeError("No requested models could be fetched in compatibility mode.")
        return pd.concat(frames, ignore_index=True), failures

    raise AttributeError(
        "Deployed data_fetcher module is missing multi-model forecast functions."
    )


def detect_heatwaves_df_compat(
    forecast_df: pd.DataFrame,
    clim_path: Path,
    min_run: int = 3,
) -> pd.DataFrame:
    if hasattr(detect_heatwaves, "detect_heatwaves_df"):
        clim_df = pd.read_csv(clim_path)
        return detect_heatwaves.detect_heatwaves_df(
            forecast_df=forecast_df,
            climatology_df=clim_df,
            min_run=min_run,
        )

    temp_forecast_path = Path("data/raw/_temp_model_forecast.csv")
    temp_forecast_path.parent.mkdir(parents=True, exist_ok=True)
    forecast_df.to_csv(temp_forecast_path, index=False)
    return detect_heatwaves.detect_heatwaves(
        forecast_path=temp_forecast_path,
        climatology_path=clim_path,
        min_run=min_run,
    )


@st.cache_data(ttl=3600, show_spinner=False)
def run_pipeline_for_city(city_name: str, lat: float, lon: float):
    city_lower = city_name.lower()
    forecast_df = data_fetcher.fetch_ecmwf_forecast(lat, lon, city_name)

    clim_path = Path(f"data/processed/{city_lower}_climatology_95p.csv")
    forecast_path = Path(f"data/raw/{city_lower}_forecast.csv")
    forecast_df.to_csv(forecast_path, index=False)
    detected_df = detect_heatwaves.detect_heatwaves(forecast_path, clim_path)

    if "is_hot" not in detected_df.columns and "exceeds_95p" in detected_df.columns:
        detected_df["is_hot"] = detected_df["exceeds_95p"]

    vulnerability_df = pd.read_csv("data/raw/urban_vulnerability.csv")
    risk_df = risk_model.assess_heatwave_risk(detected_df.copy(), vulnerability_df)
    risk_df = prepare_heat_risk_dataframe(risk_df)
    return detected_df, risk_df

# --- Paths & logo ---
ROOT = Path(__file__).resolve().parent
LOGO_PATH = ROOT / "assets" / "urban-heatwave-forecaster_new.png"

# If the file path is wrong, this will raise early and be obvious
logo_img = Image.open(LOGO_PATH)

# Page config (icon shows in browser/tab and Streamlit menu)
st.set_page_config(page_title="Urban Heatwave Forecaster",
                   page_icon=logo_img, layout="wide")

left, right = st.columns([0.1, 0.9], vertical_alignment="center")  # adjust ratio as needed
with left:
    st.image(logo_img, width=150)
with right:
    # We'll set the title dynamically later when city is chosen
    st.markdown("<h2 style='margin:0;'>Urban Heatwave Forecaster</h2>", unsafe_allow_html=True)


st.markdown("""
<style>
@keyframes spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}

.gear {
  width: 100px;
  height: 100px;
  border: 12px solid #ff4b4b;  /* matches button color */
  border-radius: 50%;
  border-top-color: transparent;
  margin: 20px auto;
  animation: spin 1s linear infinite;
}
</style>
""", unsafe_allow_html=True)


with st.expander("🔍 How This Works"):
    st.markdown("""
    **Overview**  
    This tool forecasts climate extremes by combining short-term weather forecasts with long-term climate norms. The current app supports a full **heat** workflow and an experimental/candidate **heavy precipitation** module.

    **Heatwave Detection**  
    According to the European State of the Climate (ESOTC), a [heatwave](https://climate.copernicus.eu/heatwaves-brief-introduction) happens when for at least three days in a row, both the daytime highs and nighttime lows are hotter than what’s normal for that time of year. Specifically, hotter than 95% of past temperatures recorded between 1991 and 2020.
    
    Simply put, day is marked as a heatwave day if:
    
    $$
    T_{\\min} > T_{\\min}^{95p} \\quad \\text{and} \\quad T_{\\max} > T_{\\max}^{95p}
    $$
    
    for **at least 3 consecutive days** (run length ≥ 3).

    **Hazard Signals**
    The dashboard focuses on meteorological climate-hazard signals:
    
    - Heatwave days based on temperature exceedance and run length
    - Peak temperature and anomaly above the climatological threshold
    - Experimental/candidate heavy-precipitation signals
    - Multi-hazard status from the active module summaries

    **Data Sources**
    - Forecast: [ECMWF IFS 0.25° model via Open-Meteo](https://open-meteo.com/)
    - Historical normals: [ECMWF IFS model 1991–2020 reanalysis](https://open-meteo.com/en/docs/historical-weather-api)
    """, unsafe_allow_html=True)

with st.expander("📦 How the Data Flows"):
        st.markdown("""
### 🧬 Step-by-Step Processing

1. **🛰️ Forecast Data (ECMWF)**  
   Retrieves the next 7 days of hourly temperature from Open-Meteo API.

2. **📊 Daily Aggregation**  
   Calculates daily Tmin/Tmax values from hourly data.

3. **📚 Climatology Baseline (1991–2020)**  
   Loads historical temperature data to compute 95th percentiles per day-of-year.

4. **🔥 Heatwave Detection**  
   Flags any run of **≥3 days** where both Tmin and Tmax exceed climatology thresholds.

5. **🧮 Hazard Assessment**
   Assigns hazard-specific severity and harmonizes outputs into a shared multi-hazard summary.

6. **📈 Final Output**
   Heat details, precipitation details, and a combined climate-extremes summary reflect all the above in real time.
        """)

# --- Sidebar: Location selection ---
st.sidebar.header("Location")
location_modes = list(MAIN_LOCATION_MODES)
location_mode = st.sidebar.radio(
    "Location source",
    location_modes,
    index=location_modes.index(DEFAULT_LOCATION_MODE),
    horizontal=False,
)

selected_location: Location | None = None
is_demo_location = False
run_multi_city_comparison = False

if location_mode == GLOBAL_SEARCH_MODE:
    search_query = st.sidebar.text_input("City or place name", placeholder="Gothenburg")
    search_country_code = st.sidebar.text_input(
        "Country code filter",
        placeholder="SE",
        max_chars=2,
        help="Optional ISO country code to narrow results.",
    )
    if search_query.strip():
        try:
            location_candidates = cached_location_search(
                search_query.strip(),
                search_country_code,
            )
        except (OpenMeteoGeocodingError, ValueError) as exc:
            st.sidebar.error(f"Location search failed: {exc}")
            location_candidates = []

        if not location_candidates:
            st.sidebar.warning("No matching locations found.")
        elif len(location_candidates) == 1:
            selected_location = location_candidates[0]
            st.sidebar.success(f"Selected {format_location_label(selected_location)}")
        else:
            labels = [format_candidate_label(candidate) for candidate in location_candidates]
            selected_label = st.sidebar.selectbox(
                "Select a matched location",
                ["Choose a location..."] + labels,
                help="Multiple geocoding candidates were found; choose one explicitly.",
            )
            if selected_label != "Choose a location...":
                selected_location = location_candidates[labels.index(selected_label)]
            else:
                st.sidebar.info("Choose one candidate before running a forecast.")
elif location_mode == COORDINATES_MODE:
    custom_name = st.sidebar.text_input("Location name", value="Custom location")
    custom_lat = st.sidebar.number_input(
        "Latitude",
        min_value=-90.0,
        max_value=90.0,
        value=57.7089,
        format="%.4f",
    )
    custom_lon = st.sidebar.number_input(
        "Longitude",
        min_value=-180.0,
        max_value=180.0,
        value=11.9746,
        format="%.4f",
    )
    custom_country_code = st.sidebar.text_input(
        "Country code",
        placeholder="SE",
        max_chars=2,
    )
    custom_timezone = st.sidebar.text_input(
        "Timezone",
        placeholder="Europe/Stockholm",
    )
    try:
        selected_location = validate_coordinate_location_input(
            name=custom_name,
            latitude=float(custom_lat),
            longitude=float(custom_lon),
            country_code=custom_country_code,
            timezone=custom_timezone,
        )
    except ValueError as exc:
        st.sidebar.error(str(exc))

with st.sidebar.expander(ADVANCED_DEMO_SECTION_LABEL):
    use_demo_location = st.checkbox(
        "Use built-in demo city",
        value=False,
        help="Overrides the selected location for compatibility and regression checks.",
    )
    if use_demo_location:
        demo_city = st.selectbox("Demo city", DEMO_CITY_NAMES)
        selected_location = get_city_location(demo_city)
        is_demo_location = True
        run_multi_city_comparison = st.checkbox(
            "Run 4-city demo comparison",
            value=False,
            help="Runs additional forecast calls for Athens, Rome, Stockholm, and London.",
        )
    else:
        st.caption("Demo tools are kept for compatibility checks, not the main workflow.")

if selected_location is None:
    st.title("Climate Extremes Assessment")
    st.info("Search for a global city/place or enter coordinates in the sidebar.")
    st.stop()

city = selected_location.name
city_lower = city.lower()
lat = selected_location.latitude
lon = selected_location.longitude
st.sidebar.caption(f"Using: {format_location_label(selected_location)}")
st.sidebar.caption(format_location_details(selected_location))

run_probabilistic_risk = st.sidebar.checkbox(
    "Show multi-model uncertainty",
    value=True,
    help="Combines multiple weather models and shows heat-risk probabilities."
)
include_precipitation_module = st.sidebar.checkbox(
    "Include experimental precipitation module",
    value=True,
    help="Runs the experimental/candidate 3-day wet-spell precipitation module alongside heat.",
)
prob_model_labels = st.sidebar.multiselect(
    "Weather models for uncertainty view",
    options=list(MODEL_OPTIONS.keys()),
    default=list(MODEL_OPTIONS.keys()),
    disabled=not run_probabilistic_risk,
)
selected_prob_models = [MODEL_OPTIONS[label] for label in prob_model_labels]

# --- Coordinates ---
latlon = {
    "Athens": (37.9838, 23.7278),
    "Rome": (41.8919, 12.5113),
    "Stockholm": (59.3294, 18.0687),
    "London": (51.5085, -0.1257)
}

# --- Button to Generate Forecast ---
st.title(f"Climate Extremes Assessment – {format_location_label(selected_location)}")
st.caption(
    f"Selected location: {format_location_details(selected_location)}"
)

if st.button("Generate Climate Extremes Forecast", type="primary"):
        
    # Create a placeholder for the gear
    gear_placeholder = st.empty()
    gear_placeholder.markdown('<div class="gear"></div>', unsafe_allow_html=True)


    # Pause 2 seconds before starting computation
    time.sleep(1)

    # Clear the gear before showing results
    gear_placeholder.empty()

    # 1. Run heat workflow through the backend result contract
    with st.spinner("Running heat workflow..."):
        try:
            heat_app_data = run_heat_app_workflow(selected_location)
        except Exception as exc:
            st.error(f"Unable to run heat workflow: {exc}")
            st.stop()

    heat_result = heat_app_data.result
    detected_df = heat_app_data.detected_df
    risk_df = heat_app_data.risk_df
    for warning in heat_result.warnings:
        if "vulnerability" not in warning.lower():
            st.warning(warning)

    with st.expander("Generated backend outputs"):
        st.markdown(f"**Heat** · `{heat_result.output_label}`")
        for line in generated_files_summary(heat_result):
            st.write(line)

    risk_df = prepare_heat_risk_dataframe(risk_df)
    vulnerability_df = heat_app_data.vulnerability_df
    output_label = heat_app_data.output_label
    clim_path = heat_app_data.climatology_path

    precipitation_detected_df = None
    precipitation_risk_df = None
    precipitation_assessment = None
    precipitation_result = None
    precipitation_error = None
    if include_precipitation_module:
        with st.spinner("Running precipitation module..."):
            try:
                precipitation_app_data = run_precipitation_app_workflow(
                    selected_location,
                    definition=WET_SPELL_3DAY_95P,
                )
                precipitation_result = precipitation_app_data.result
                precipitation_detected_df = precipitation_app_data.detected_df
                precipitation_risk_df = precipitation_app_data.risk_df
                precipitation_assessment = precipitation_app_data.assessment
            except Exception as exc:
                precipitation_error = str(exc)
        if precipitation_result is not None:
            with st.expander("Precipitation outputs"):
                st.markdown("**Precipitation** · experimental/candidate")
                for line in generated_files_summary(precipitation_result):
                    st.write(line)

    fig_df = prepare_temperature_display_frame(detected_df)
    heat_metrics = heat_summary_metrics(fig_df, risk_df)
    heatwave_days = heat_metrics["heatwave_days"]
    extreme_days = heat_metrics["extreme_days"]

    heat_assessment_payload = prepare_heat_assessment_payload(
        fig_df,
        risk_df,
        heat_metrics,
    )
    assessments = [heat_assessment_payload]
    if precipitation_assessment is not None:
        assessments.append(precipitation_assessment)
    multi_hazard_summary = summarize_hazards(assessments)
    
    max_tmax = heat_metrics["max_tmax"]
    max_anomaly = heat_metrics["max_tmax_anomaly"]
    forecast_window = f"{len(fig_df)} days"
    location_label = format_location_label(selected_location)

    st.subheader("🌐 Climate Hazard Overview")
    primary_hazard = multi_hazard_summary["primary_hazard"] or "None"
    overview_1, overview_2, overview_3, overview_4 = st.columns(4)
    overview_1.metric("Overall Hazard Status", format_hazard_status(multi_hazard_summary["overall_status"]))
    overview_2.metric("Hazards Signaled", int(multi_hazard_summary["hazard_count"]))
    overview_3.metric("Forecast Window", forecast_window)
    overview_4.metric("Selected Location", location_label)

    message_level, heat_message = format_heat_signal_message(int(heatwave_days), location_label)
    if message_level == "success":
        st.success(heat_message)
    else:
        st.info(heat_message)
    if precipitation_error:
        st.warning(f"Precipitation module unavailable for this run: {precipitation_error}")

    st.subheader("🔥 Heat Hazard Summary")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Heatwave Days", heatwave_days)
    m2.metric("Extreme Heat Days", int(extreme_days))
    m3.metric("Peak Tmax", f"{max_tmax:.1f}°C" if max_tmax is not None else "Unavailable")
    m4.metric(
        "Peak Tmax Anomaly",
        f"{max_anomaly:+.1f}°C" if max_anomaly is not None else "Unavailable",
    )

    if precipitation_assessment is not None:
        st.subheader("🌧️ Precipitation Hazard Summary")
        precip_summary_1, precip_summary_2, precip_summary_3, precip_summary_4 = st.columns(4)
        precip_summary_1.metric("Status", "Signal detected" if precipitation_assessment.event_detected else "No signal")
        precip_summary_2.metric("Science Status", "Experimental")
        precip_summary_3.metric("Severity Class", format_shared_class(precipitation_assessment.severity_class))
        precip_summary_4.metric("Definition", WET_SPELL_3DAY_95P.name)

    st.subheader("🌐 Module Summary")
    mh1, mh2, mh3 = st.columns(3)
    mh1.metric("Summary Class", format_shared_class(multi_hazard_summary["summary_class"]))
    mh2.metric("Primary Hazard", format_shared_class(primary_hazard))
    mh3.metric("Module Assessments", len(multi_hazard_summary["module_summaries"]))

    module_cards = st.columns(max(len(multi_hazard_summary["module_summaries"]), 1))
    for idx, module_summary in enumerate(multi_hazard_summary["module_summaries"]):
        with module_cards[idx]:
            hazard_name = format_shared_class(module_summary["hazard"])
            st.markdown(f"**{hazard_name}**")
            badge_color = SHARED_CLASS_COLORS.get(module_summary["severity_class"], "#cccccc")
            st.markdown(
                f"<div style='padding:0.5rem 0.75rem;border-radius:0.75rem;background:{badge_color};color:white;font-weight:600;margin-bottom:0.5rem;'>{format_shared_class(module_summary['severity_class'])}</div>",
                unsafe_allow_html=True,
            )
            st.caption(
                f"Detected: {'Yes' if module_summary['event_detected'] else 'No'} | Score: {module_summary['severity_score']:.1f}"
            )
            for metric_key, metric_value in list(module_summary.get("key_metrics", {}).items())[:3]:
                st.write(f"{metric_key.replace('_', ' ').title()}: {metric_value}")

    # --- Plotly Chart 1: Forecast vs Climatology ---
    st.subheader("📈 Forecast vs Climatology Thresholds")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=fig_df["date"],
        y=fig_df["tmax"],
        mode="lines+markers",
        name="Tmax",
        line=dict(color="#ff6f3c", width=2.5),
        marker=dict(size=6)
    ))
    fig.add_trace(go.Scatter(
        x=fig_df["date"],
        y=fig_df["tmax_95p"],
        mode="lines",
        name="Tmax 95th pct",
        line=dict(color="#ff6f3c", width=2, dash="dash")
    ))
    fig.add_trace(go.Scatter(
        x=fig_df["date"],
        y=fig_df["tmin"],
        mode="lines+markers",
        name="Tmin",
        line=dict(color="#3399ff", width=2),
        marker=dict(size=5)
    ))
    fig.add_trace(go.Scatter(
        x=fig_df["date"],
        y=fig_df["tmin_95p"],
        mode="lines",
        name="Tmin 95th pct",
        line=dict(color="#3399ff", width=2, dash="dash")
    ))

    # Add shaded heatwave periods
    shapes = []
    for _, group in fig_df[fig_df["heatwave_id"].notna()].groupby("heatwave_id"):
        start = group["date"].min()
        end = group["date"].max() + timedelta(days=1)
        shapes.append(dict(
            type="rect",
            xref="x", yref="paper",
            x0=start, x1=end,
            y0=0, y1=1,
            fillcolor="rgba(255,0,0,0.15)",
            line_width=0,
            layer="below"
        ))

    fig.update_layout(
        title="Daily Tmin/Tmax Against 95th-Percentile Normals",
        xaxis_title="Date",
        yaxis_title="Temperature (°C)",
        shapes=shapes,
        legend=dict(title=""),
        margin=dict(l=40, r=20, t=60, b=40)
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- Plotly Chart 2: Daily Anomalies ---
    st.subheader("🌡️ Daily Temperature Anomalies vs 95th Percentile")
    anomaly_fig = go.Figure()
    anomaly_fig.add_trace(go.Bar(
        x=fig_df["date"],
        y=fig_df["tmax_anomaly"],
        name="Tmax anomaly",
        marker_color="#ff6f3c"
    ))
    anomaly_fig.add_trace(go.Bar(
        x=fig_df["date"],
        y=fig_df["tmin_anomaly"],
        name="Tmin anomaly",
        marker_color="#3399ff"
    ))
    anomaly_fig.add_hline(y=0, line_dash="dot", line_color="gray")
    anomaly_fig.update_layout(
        barmode="group",
        xaxis_title="Date",
        yaxis_title="Anomaly (°C)",
        margin=dict(l=40, r=20, t=30, b=40),
        legend=dict(title="")
    )
    st.plotly_chart(anomaly_fig, use_container_width=True)

    # --- Plotly Chart 3: Heat hazard severity ---
    st.subheader("🧮 Heat Hazard Classification")
    risk_fig = go.Figure()
    risk_fig.add_trace(go.Scatter(
        x=risk_df["date"],
        y=risk_df["base_risk_score"],
        mode="lines+markers",
        name="Temperature-only class",
        line=dict(color="#8e8e8e", width=2, dash="dot"),
        marker=dict(size=7)
    ))
    risk_fig.add_trace(go.Scatter(
        x=risk_df["date"],
        y=risk_df["adjusted_risk_score"],
        mode="lines+markers",
        name="Final heat class",
        line=dict(color="#d7263d", width=3),
        marker=dict(size=9)
    ))
    if risk_df["risk_escalated"].any():
        risk_fig.add_trace(go.Scatter(
            x=risk_df.loc[risk_df["risk_escalated"], "date"],
            y=risk_df.loc[risk_df["risk_escalated"], "adjusted_risk_score"],
            mode="markers",
            name="Local adjustment present",
            marker=dict(size=13, color="#ffa600", symbol="diamond")
        ))
    risk_fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Risk Level",
        yaxis=dict(
            tickmode="array",
            tickvals=list(range(len(RISK_ORDER))),
            ticktext=RISK_ORDER,
            range=[-0.3, len(RISK_ORDER) - 0.7]
        ),
        margin=dict(l=40, r=20, t=30, b=40),
        legend=dict(title="")
    )
    st.plotly_chart(risk_fig, use_container_width=True)

    styled = prepare_heat_risk_table(risk_df)

    st.subheader("📋 Heat Hazard Table")
    st.dataframe(styled)

    if precipitation_assessment is not None and precipitation_risk_df is not None:
        st.subheader("🌧️ Heavy Precipitation Details")
        st.caption("Experimental/candidate module; thresholds and definitions should be validated before operational use.")

        precip_plot_df = prepare_precipitation_plot_frame(precipitation_risk_df)
        precip_fig = go.Figure()
        precip_fig.add_trace(
            go.Bar(
                x=precip_plot_df["date"],
                y=precip_plot_df["precipitation_sum"],
                name="Daily precipitation (mm)",
                marker_color="#5aa9e6",
            )
        )
        precip_fig.add_trace(
            go.Scatter(
                x=precip_plot_df["date"],
                y=precip_plot_df["precip_accumulation_mm"],
                mode="lines+markers",
                name="3-day accumulation (mm)",
                line=dict(color="#1b4965", width=3),
            )
        )
        precip_fig.add_trace(
            go.Scatter(
                x=precip_plot_df["date"],
                y=precip_plot_df["threshold_mm"],
                mode="lines",
                name="3-day threshold (mm)",
                line=dict(color="#d1495b", width=2, dash="dash"),
            )
        )
        precip_fig.update_layout(
            title="Heavy Precipitation Module: Daily Totals vs 3-Day Threshold",
            xaxis_title="Date",
            yaxis_title="Precipitation (mm)",
            margin=dict(l=40, r=20, t=50, b=40),
            legend=dict(title=""),
        )
        st.plotly_chart(precip_fig, use_container_width=True)

        precip_display = prepare_precipitation_table(precipitation_risk_df)
        st.dataframe(precip_display, use_container_width=True, hide_index=True)

    if run_probabilistic_risk:
        st.subheader("🎲 Probabilistic Multi-Model Risk")
        st.caption(
            "Daily probabilities built from multiple forecast models using the same detection and risk pipeline."
        )

        if not selected_prob_models:
            st.info("Select at least one model in the sidebar to compute probabilistic risk.")
        else:
            ensemble_frames = []
            failed_models = []

            if "ecmwf_ifs025" in selected_prob_models:
                ecmwf_frame = risk_df.copy()
                ecmwf_frame["model"] = "ecmwf_ifs025"
                ensemble_frames.append(ecmwf_frame)

            additional_models = [
                model for model in selected_prob_models if model != "ecmwf_ifs025"
            ]

            if additional_models:
                multi_forecast_df = pd.DataFrame()
                with st.spinner("Fetching additional forecast models..."):
                    try:
                        multi_forecast_df, failed_models = fetch_multi_model_forecast_compat(
                            lat=lat,
                            lon=lon,
                            city_name=output_label,
                            models=additional_models,
                            forecast_days=7,
                        )
                    except (RuntimeError, AttributeError) as exc:
                        failed_models = [
                            {"model": model, "error": str(exc)} for model in additional_models
                        ]

                if not multi_forecast_df.empty:
                    for model_code, model_forecast in multi_forecast_df.groupby("model"):
                        model_detected = detect_heatwaves_df_compat(
                            forecast_df=model_forecast[["date", "tmin", "tmax", "city"]],
                            clim_path=clim_path,
                            min_run=3,
                        )
                        if (
                            "is_hot" not in model_detected.columns
                            and "exceeds_95p" in model_detected.columns
                        ):
                            model_detected["is_hot"] = model_detected["exceeds_95p"]

                        model_risk = risk_model.assess_heatwave_risk(
                            model_detected.copy(),
                            vulnerability_df.copy(),
                        )
                        model_risk = prepare_heat_risk_dataframe(model_risk)
                        model_risk["model"] = model_code
                        ensemble_frames.append(model_risk)

            if failed_models:
                failed_names = ", ".join(
                    f"{MODEL_LABEL_BY_CODE.get(item['model'], item['model'])}"
                    for item in failed_models
                )
                st.warning(
                    f"Excluded unavailable models in this run: {failed_names}."
                )

            if ensemble_frames:
                ensemble_risk_df = pd.concat(ensemble_frames, ignore_index=True)
                probability_data = prepare_probabilistic_heat_data(
                    ensemble_risk_df,
                    RISK_ORDER,
                )
                probability_df = probability_data.probability_df
                risk_probs = probability_data.risk_probabilities
                model_codes_used = probability_data.model_codes
                model_labels_used = [
                    f"{MODEL_LABEL_BY_CODE.get(code, code)} ({code})"
                    for code in model_codes_used
                ]
                st.caption(f"Models used: {', '.join(model_labels_used)}")

                prob_fig = go.Figure()
                prob_fig.add_trace(
                    go.Scatter(
                        x=probability_df.index,
                        y=probability_df["p_heatwave"] * 100,
                        mode="lines+markers",
                        name="P(Heatwave)",
                        line=dict(color="#6a4c93", width=2.5),
                    )
                )
                prob_fig.add_trace(
                    go.Scatter(
                        x=probability_df.index,
                        y=probability_df["p_high_plus"] * 100,
                        mode="lines+markers",
                        name="P(High+)",
                        line=dict(color="#f46d43", width=2.5),
                    )
                )
                prob_fig.add_trace(
                    go.Scatter(
                        x=probability_df.index,
                        y=probability_df["p_extreme"] * 100,
                        mode="lines+markers",
                        name="P(Extreme)",
                        line=dict(color="#d73027", width=2.5),
                    )
                )
                prob_fig.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Probability (%)",
                    yaxis=dict(range=[0, 100]),
                    margin=dict(l=40, r=20, t=30, b=40),
                    legend=dict(title=""),
                )
                st.plotly_chart(prob_fig, use_container_width=True)

                dist_fig = go.Figure()
                for level in RISK_ORDER:
                    dist_fig.add_trace(
                        go.Bar(
                            x=risk_probs.index,
                            y=risk_probs[level] * 100,
                            name=level,
                            marker_color=RISK_COLORS[level],
                        )
                    )
                dist_fig.update_layout(
                    barmode="stack",
                    xaxis_title="Date",
                    yaxis_title="Risk Probability (%)",
                    yaxis=dict(range=[0, 100]),
                    margin=dict(l=40, r=20, t=30, b=40),
                    legend=dict(title=""),
                    title="Risk-Level Probability Distribution by Day",
                )
                st.plotly_chart(dist_fig, use_container_width=True)

                st.dataframe(
                    prepare_probabilistic_display_table(probability_df),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.error(
                    "No probabilistic output available because all selected model fetches failed."
                )

    if run_multi_city_comparison:
        st.subheader("🌍 4-City Comparison")
        st.caption(
            "Comparing Athens, Rome, Stockholm, and London using the same heat pipeline and risk rules."
        )

        with st.spinner("Building multi-city comparison..."):
            comparison_rows = []
            for comp_city, (comp_lat, comp_lon) in latlon.items():
                if comp_city == city:
                    comp_detected_df = fig_df.copy()
                    comp_risk_df = risk_df.copy()
                else:
                    comp_detected_df, comp_risk_df = run_pipeline_for_city(
                        comp_city,
                        comp_lat,
                        comp_lon
                    )
                    comp_detected_df = comp_detected_df.copy()
                    comp_detected_df["date"] = pd.to_datetime(comp_detected_df["date"])
                    comp_risk_df = comp_risk_df.copy()
                    comp_risk_df["date"] = pd.to_datetime(comp_risk_df["date"])

                comparison_rows.append(
                    prepare_city_comparison_row(
                        comp_city,
                        comp_lat,
                        comp_lon,
                        comp_detected_df,
                        comp_risk_df,
                        RISK_ORDER,
                    )
                )

            compare_df = prepare_city_comparison_frame(comparison_rows)

        map_text = prepare_city_map_hover_text(compare_df)

        map_fig = go.Figure(
            go.Scattergeo(
                lon=compare_df["lon"],
                lat=compare_df["lat"],
                mode="markers+text",
                text=compare_df["city"],
                textposition="top center",
                hovertemplate=map_text + "<extra></extra>",
                marker=dict(
                    size=12 + (compare_df["heatwave_days"] * 2),
                    color=compare_df["max_risk_score"],
                    cmin=0,
                    cmax=4,
                    colorscale=[
                        [0.00, "#a8ddb5"],
                        [0.25, "#fee08b"],
                        [0.50, "#fdae61"],
                        [0.75, "#f46d43"],
                        [1.00, "#d73027"],
                    ],
                    line=dict(color="white", width=1),
                    colorbar=dict(
                        title="Max Risk",
                        tickmode="array",
                        tickvals=list(range(len(RISK_ORDER))),
                        ticktext=RISK_ORDER
                    ),
                ),
            )
        )
        map_fig.update_layout(
            margin=dict(l=10, r=10, t=30, b=10),
            geo=dict(
                scope="europe",
                projection_type="natural earth",
                showland=True,
                landcolor="#f7f3e9",
                showcountries=True,
                countrycolor="#c9c0ad",
                lataxis=dict(range=[35, 62]),
                lonaxis=dict(range=[-12, 31]),
            ),
            title="City Risk Map (Marker Color = Max Risk, Marker Size = Heatwave Days)"
        )
        st.plotly_chart(map_fig, use_container_width=True)

        compare_chart = go.Figure()
        compare_chart.add_trace(
            go.Bar(
                x=compare_df["city"],
                y=compare_df["peak_tmax"],
                name="Peak Tmax (°C)",
                marker_color="#ff6f3c"
            )
        )
        compare_chart.add_trace(
            go.Bar(
                x=compare_df["city"],
                y=compare_df["peak_tmax_anomaly"],
                name="Peak Tmax anomaly (°C)",
                marker_color="#6a4c93"
            )
        )
        compare_chart.update_layout(
            barmode="group",
            xaxis_title="City",
            yaxis_title="Temperature (°C)",
            margin=dict(l=40, r=20, t=30, b=40),
            legend=dict(title="")
        )
        st.plotly_chart(compare_chart, use_container_width=True)

        compare_display = prepare_city_comparison_table(compare_df)
        st.dataframe(compare_display, use_container_width=True, hide_index=True)
