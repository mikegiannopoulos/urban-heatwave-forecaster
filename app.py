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

RISK_ORDER = ["None", "Mild", "Moderate", "High", "Extreme"]
RISK_TO_SCORE = {risk: score for score, risk in enumerate(RISK_ORDER)}
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


def enrich_risk_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out["base_risk_level"] = out["tmax"].apply(base_risk_from_tmax)
    out["base_risk_score"] = out["base_risk_level"].map(RISK_TO_SCORE)
    out["adjusted_risk_score"] = out["risk_level"].map(RISK_TO_SCORE)
    out["risk_escalated"] = out["adjusted_risk_score"] > out["base_risk_score"]
    return out


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
    risk_df = enrich_risk_dataframe(risk_df)
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
    This tool forecasts potential heatwave risk by combining short-term temperature forecasts with long-term climate norms and urban vulnerability data.

    **Heatwave Detection**  
    According to the European State of the Climate (ESOTC), a [heatwave](https://climate.copernicus.eu/heatwaves-brief-introduction) happens when for at least three days in a row, both the daytime highs and nighttime lows are hotter than what’s normal for that time of year. Specifically, hotter than 95% of past temperatures recorded between 1991 and 2020.
    
    Simply put, day is marked as a heatwave day if:
    
    $$
    T_{\\min} > T_{\\min}^{95p} \\quad \\text{and} \\quad T_{\\max} > T_{\\max}^{95p}
    $$
    
    for **at least 3 consecutive days** (run length ≥ 3).

    **Risk Assessment Factors**     
    Each day is assessed for urban heat risk by considering:
    
    - Maximum temperature forecast
    - Urban density (people/km²)
    - Green cover percentage
    - Elderly population percentage
    
    A vulnerability score elevates the base risk level.

    **Data Sources**
    - Forecast: [ECMWF IFS 0.25° model via Open-Meteo](https://open-meteo.com/)
    - Historical normals: [ECMWF IFS model 1991–2020 reanalysis](https://open-meteo.com/en/docs/historical-weather-api)
    - Population and green area data: [Urban dataset](https://hugsi.green/cities/index)
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

5. **🏙️ Urban Vulnerability Data**  
   Merges in city-level data: elderly %, density, green space.

6. **🧮 Risk Scoring**  
   Assigns a heatwave risk level for each day using temperature + vulnerability.

7. **📈 Final Output**  
   Risk table + interactive chart reflect all the above in real time.
        """)

# --- Sidebar: City selection ---
city = st.sidebar.selectbox("Select a city", ["Athens", "Rome", "Stockholm", "London"])
city_lower = city.lower()
run_multi_city_comparison = st.sidebar.checkbox(
    "Enable 4-city comparison",
    value=False,
    help="Runs additional forecast calls for all available cities."
)
run_probabilistic_risk = st.sidebar.checkbox(
    "Enable probabilistic multi-model risk",
    value=True,
    help="Combines multiple weather models and shows risk probabilities."
)
prob_model_labels = st.sidebar.multiselect(
    "Models for probabilistic risk",
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
lat, lon = latlon[city]

# --- Button to Generate Forecast ---
st.title(f"Heatwave Risk Assessment – {city}")

if st.button("Generate Heatwave Forecast", type="primary"):
        
    # Create a placeholder for the gear
    gear_placeholder = st.empty()
    gear_placeholder.markdown('<div class="gear"></div>', unsafe_allow_html=True)


    # Pause 2 seconds before starting computation
    time.sleep(1)

    # Clear the gear before showing results
    gear_placeholder.empty()

    # 1. Fetch forecast
    with st.spinner("Fetching forecast..."):
        forecast_df = data_fetcher.fetch_ecmwf_forecast(lat, lon, city)

    # 2. Heatwave detection
    clim_path = Path(f"data/processed/{city_lower}_climatology_95p.csv")
    forecast_path = Path(f"data/raw/{city_lower}_forecast.csv")
    forecast_df.to_csv(forecast_path, index=False)
    detected_df = detect_heatwaves.detect_heatwaves(forecast_path, clim_path)

    # Ensure 'is_hot' exists
    if "is_hot" not in detected_df.columns and "exceeds_95p" in detected_df.columns:
        detected_df["is_hot"] = detected_df["exceeds_95p"]

    # 3. Risk assessment
    vulnerability_df = pd.read_csv("data/raw/urban_vulnerability.csv")
    risk_df = risk_model.assess_heatwave_risk(detected_df, vulnerability_df)
    risk_df = enrich_risk_dataframe(risk_df)
    
    # --- Summary Metrics ---
    heatwave_days = detected_df["heatwave_id"].notna().sum()
    extreme_days = (risk_df["risk_level"] == "Extreme").sum()

    # --- Heatwave Message ---
    if detected_df["heatwave_id"].notna().any():
        st.success(f"**Heatwave detected!** {heatwave_days} heatwave day(s) forecasted for {city}.")
    else:
        st.info(f"No heatwave forecasted for {city} in the next 7 days.")

    fig_df = detected_df.copy()
    fig_df["date"] = pd.to_datetime(fig_df["date"])
    fig_df["Heatwave"] = fig_df["heatwave_id"].notna().map({True: "Yes", False: "No"})
    fig_df["tmax_anomaly"] = fig_df["tmax"] - fig_df["tmax_95p"]
    fig_df["tmin_anomaly"] = fig_df["tmin"] - fig_df["tmin_95p"]

    city_vuln = vulnerability_df.loc[vulnerability_df["city"] == city_lower].iloc[0]
    escalated_days = int(risk_df["risk_escalated"].sum())
    max_tmax = float(fig_df["tmax"].max())
    max_anomaly = float(fig_df["tmax_anomaly"].max())

    # --- Summary Metrics ---
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Heatwave Days", heatwave_days)
    m2.metric("Extreme-Risk Days", int(extreme_days))
    m3.metric("Peak Tmax", f"{max_tmax:.1f}°C")
    m4.metric("Tmax Peak Anomaly", f"{max_anomaly:+.1f}°C")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Elderly Share", f"{city_vuln['elderly_percent']:.1f}%")
    c2.metric("Green Cover", f"{city_vuln['green_cover_percent']:.1f}%")
    c3.metric("Density", f"{int(city_vuln['density_per_km2']):,}/km²")
    c4.metric("Risk Escalation Days", escalated_days)

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

    # --- Plotly Chart 3: Risk decomposition ---
    st.subheader("🧮 Risk Decomposition: Base vs Vulnerability-Adjusted")
    risk_fig = go.Figure()
    risk_fig.add_trace(go.Scatter(
        x=risk_df["date"],
        y=risk_df["base_risk_score"],
        mode="lines+markers",
        name="Base risk (temperature only)",
        line=dict(color="#8e8e8e", width=2, dash="dot"),
        marker=dict(size=7)
    ))
    risk_fig.add_trace(go.Scatter(
        x=risk_df["date"],
        y=risk_df["adjusted_risk_score"],
        mode="lines+markers",
        name="Adjusted risk (with vulnerability)",
        line=dict(color="#d7263d", width=3),
        marker=dict(size=9)
    ))
    risk_fig.add_trace(go.Scatter(
        x=risk_df.loc[risk_df["risk_escalated"], "date"],
        y=risk_df.loc[risk_df["risk_escalated"], "adjusted_risk_score"],
        mode="markers",
        name="Escalated by vulnerability",
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

    # --- Color-coded Risk Table ---
    emoji_map = {"Extreme": "🔥🔥", "High": "🔥", "Moderate": "🌡️", "Mild": "☀️", "None": "❄️"}
    risk_display = risk_df.copy()

    # Format date as "Tue, Jul 29"
    risk_display["date"] = pd.to_datetime(risk_display["date"])
    risk_display["date"] = risk_display["date"].dt.strftime("%a, %b %d")

    # Map emojis
    risk_display["⚠️"] = risk_display["risk_level"].map(emoji_map)
    risk_display["Escalated"] = risk_display["risk_escalated"].map({True: "⬆️", False: ""})

    # Select and rename columns
    styled = risk_display[["date", "tmax", "base_risk_level", "risk_level", "Escalated", "⚠️"]]
    styled.columns = ["Date", "Tmax (°C)", "Base Risk", "Final Risk", "Vulnerability Lift", ""]

    # Remove the index before displaying
    styled = styled.reset_index(drop=True)

    st.subheader("📋 Heatwave Risk Table")
    st.dataframe(styled)

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
                            city_name=city,
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
                        model_risk = enrich_risk_dataframe(model_risk)
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
                ensemble_risk_df["date"] = pd.to_datetime(ensemble_risk_df["date"])

                models_available = (
                    ensemble_risk_df.groupby("date")["model"].nunique().sort_index()
                )
                risk_counts = (
                    ensemble_risk_df.pivot_table(
                        index="date",
                        columns="risk_level",
                        values="model",
                        aggfunc="count",
                        fill_value=0,
                    )
                    .reindex(columns=RISK_ORDER, fill_value=0)
                    .sort_index()
                )
                risk_probs = risk_counts.div(risk_counts.sum(axis=1), axis=0).fillna(0.0)

                probability_df = pd.DataFrame(index=risk_probs.index)
                probability_df["models_available"] = models_available
                probability_df["p_heatwave"] = (
                    ensemble_risk_df.groupby("date")["heatwave_id"]
                    .apply(lambda s: s.notna().mean())
                    .sort_index()
                )
                probability_df["p_high_plus"] = (
                    ensemble_risk_df.groupby("date")["risk_level"]
                    .apply(lambda s: s.isin(["High", "Extreme"]).mean())
                    .sort_index()
                )
                probability_df["p_extreme"] = (
                    ensemble_risk_df.groupby("date")["risk_level"]
                    .apply(lambda s: (s == "Extreme").mean())
                    .sort_index()
                )
                probability_df["expected_risk_score"] = (
                    ensemble_risk_df.groupby("date")["adjusted_risk_score"].mean().sort_index()
                )
                probability_df["most_likely_risk"] = risk_probs.idxmax(axis=1)

                def _consensus_from_probs(row: pd.Series) -> str:
                    for level in reversed(RISK_ORDER):
                        if row[level] >= 0.5:
                            return level
                    return "Uncertain"

                probability_df["consensus_risk"] = risk_probs.apply(_consensus_from_probs, axis=1)

                model_codes_used = list(dict.fromkeys(ensemble_risk_df["model"]))
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

                prob_display = probability_df.reset_index().copy()
                prob_display["date"] = pd.to_datetime(prob_display["date"]).dt.strftime("%a, %b %d")
                prob_display["P(Heatwave)"] = (prob_display["p_heatwave"] * 100).round(1)
                prob_display["P(High+)"] = (prob_display["p_high_plus"] * 100).round(1)
                prob_display["P(Extreme)"] = (prob_display["p_extreme"] * 100).round(1)
                prob_display["Expected Risk Score"] = prob_display["expected_risk_score"].round(2)
                prob_display["Models"] = prob_display["models_available"].astype(int)

                st.dataframe(
                    prob_display[
                        [
                            "date",
                            "Models",
                            "P(Heatwave)",
                            "P(High+)",
                            "P(Extreme)",
                            "most_likely_risk",
                            "consensus_risk",
                            "Expected Risk Score",
                        ]
                    ].rename(
                        columns={
                            "date": "Date",
                            "most_likely_risk": "Most Likely Risk",
                            "consensus_risk": "Consensus Risk (>=50%)",
                        }
                    ),
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
            "Comparing Athens, Rome, Stockholm, and London using the same pipeline and risk rules."
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

                comp_detected_df["tmax_anomaly"] = (
                    comp_detected_df["tmax"] - comp_detected_df["tmax_95p"]
                )
                comp_risk_df = enrich_risk_dataframe(comp_risk_df)

                max_risk_score = int(comp_risk_df["adjusted_risk_score"].max())
                comparison_rows.append(
                    {
                        "city": comp_city,
                        "lat": comp_lat,
                        "lon": comp_lon,
                        "heatwave_days": int(comp_detected_df["heatwave_id"].notna().sum()),
                        "escalated_days": int(comp_risk_df["risk_escalated"].sum()),
                        "peak_tmax": float(comp_detected_df["tmax"].max()),
                        "peak_tmax_anomaly": float(comp_detected_df["tmax_anomaly"].max()),
                        "max_risk_score": max_risk_score,
                        "max_risk_level": RISK_ORDER[max_risk_score],
                    }
                )

            compare_df = pd.DataFrame(comparison_rows).sort_values(
                ["max_risk_score", "peak_tmax"],
                ascending=[False, False]
            )

        map_text = compare_df.apply(
            lambda row: (
                f"{row['city']}<br>"
                f"Max risk: {row['max_risk_level']}<br>"
                f"Peak Tmax: {row['peak_tmax']:.1f}°C<br>"
                f"Heatwave days: {row['heatwave_days']}"
            ),
            axis=1
        )

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

        compare_display = compare_df[
            [
                "city",
                "max_risk_level",
                "heatwave_days",
                "escalated_days",
                "peak_tmax",
                "peak_tmax_anomaly",
            ]
        ].copy()
        compare_display.columns = [
            "City",
            "Max Risk",
            "Heatwave Days",
            "Escalation Days",
            "Peak Tmax (°C)",
            "Peak Tmax Anomaly (°C)",
        ]
        st.dataframe(compare_display, use_container_width=True, hide_index=True)
