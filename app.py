import streamlit as st
import pandas as pd
import sys
from PIL import Image
from pathlib import Path
from datetime import timedelta
import time
import plotly.graph_objects as go

# Fix: add src/ to path so Streamlit can find your module
sys.path.append(str(Path(__file__).resolve().parent / "src"))

from urban_heatwave_forecaster import data_fetcher, detect_heatwaves, risk_model

RISK_ORDER = ["None", "Mild", "Moderate", "High", "Extreme"]
RISK_TO_SCORE = {risk: score for score, risk in enumerate(RISK_ORDER)}


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

# --- Paths & logo ---
ROOT = Path(__file__).resolve().parent
LOGO_PATH = ROOT / "assets" / "urban-heatwave-forecaster.png"

# If the file path is wrong, this will raise early and be obvious
logo_img = Image.open(LOGO_PATH)

# Page config (icon shows in browser/tab and Streamlit menu)
st.set_page_config(page_title="Urban Heatwave Forecaster",
                   page_icon=logo_img, layout="wide")

left, right = st.columns([0.07, 0.93])  # adjust ratio as needed
with left:
    st.image(logo_img, width=100)
with right:
    # We'll set the title dynamically later when city is chosen
    st.markdown("## Urban Heatwave Forecaster")


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

    risk_df = risk_df.copy()
    risk_df["date"] = pd.to_datetime(risk_df["date"])
    risk_df["base_risk_level"] = risk_df["tmax"].apply(base_risk_from_tmax)
    risk_df["base_risk_score"] = risk_df["base_risk_level"].map(RISK_TO_SCORE)
    risk_df["adjusted_risk_score"] = risk_df["risk_level"].map(RISK_TO_SCORE)
    risk_df["risk_escalated"] = risk_df["adjusted_risk_score"] > risk_df["base_risk_score"]

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
