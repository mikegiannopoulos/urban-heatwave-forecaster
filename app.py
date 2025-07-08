import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

# Fix: add src/ to path so Streamlit can find your module
sys.path.append(str(Path(__file__).resolve().parent / "src"))

from urban_heatwave_forecaster import data_fetcher, detect_heatwaves, risk_model

st.set_page_config(page_title="Urban Heatwave Forecaster", layout="wide")

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

    - $T_{\\min}^{95p}, T_{\\max}^{95p}$ are the daily 95th percentile thresholds, based on 1991–2020 historical data.
    
    **Risk Assessment Factors**
    Each day is assessed for urban heat risk by considering:
    
    - Maximum temperature forecast
    - Elderly population percentage
    - Urban density (people/km²)
    - Green cover percentage

    A vulnerability score elevates the base risk level.

    **Data Sources**
    - Forecast: [ECMWF IFS 0.25° model via Open-Meteo](https://open-meteo.com/)
    - Historical normals: [ECMWF IFS model 1991–2020 reanalysis](https://open-meteo.com/en/docs/historical-weather-api)
    - Population and green area data: [Urban dataset](https://hugsi.green/cities/index)
    """, unsafe_allow_html=True)


# --- Sidebar: City selection ---
city = st.sidebar.selectbox("Select a city", ["Athens", "Rome", "Stockholm"])
city_lower = city.lower()

# --- Run forecast step ---
latlon = {
    "Athens": (37.9838, 23.7278),
    "Rome": (41.8919, 12.5113),
    "Stockholm": (59.3294, 18.0687),
}
lat, lon = latlon[city]
with st.spinner("Fetching forecast..."):
    forecast_df = data_fetcher.fetch_ecmwf_forecast(lat, lon, city)

# --- Run heatwave detection ---
clim_path = Path(f"data/processed/{city_lower}_climatology_95p.csv")
forecast_path = Path(f"data/raw/{city_lower}_forecast.csv")
forecast_df.to_csv(forecast_path, index=False)
detected_path = Path(f"data/processed/{city_lower}_forecast_with_heatwaves.csv")

vuln_path = Path("data/raw/urban_vulnerability.csv")
vulnerability_df = pd.read_csv(vuln_path)

with st.spinner("🔍 Calculating climatology and heatwave risk..."):
    # Heatwave detection
    detected_df = detect_heatwaves.detect_heatwaves(forecast_path, clim_path)
    detected_df.to_csv(detected_path, index=False)

    # Add is_hot if needed
    if "is_hot" not in detected_df.columns and "exceeds_95p" in detected_df.columns:
        detected_df["is_hot"] = detected_df["exceeds_95p"]

    # Risk model
    risk_df = risk_model.assess_heatwave_risk(detected_df, vulnerability_df)


# --- Run risk assessment ---
vuln_path = Path("data/raw/urban_vulnerability.csv")
vulnerability_df = pd.read_csv(vuln_path)
if "is_hot" not in detected_df.columns and "exceeds_95p" in detected_df.columns:
    detected_df["is_hot"] = detected_df["exceeds_95p"]
with st.spinner("Assessing risk..."):
    risk_df = risk_model.assess_heatwave_risk(detected_df, vulnerability_df)

# --- Summary Metrics ---
heatwave_days = detected_df["heatwave_id"].notna().sum()
extreme_days = (risk_df["risk_level"] == "Extreme").sum()
elderly_pct = int(vulnerability_df.loc[vulnerability_df["city"] == city_lower, "elderly_percent"].iloc[0])

st.title(f"Heatwave Risk Assessment – {city}")
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

import plotly.graph_objects as go
from datetime import timedelta

# Step 1: Create base line trace (one line, all tmax values)
fig_df = detected_df.copy()
fig_df["Heatwave"] = fig_df["heatwave_id"].notna().map({True: "Yes", False: "No"})

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=fig_df["date"],
    y=fig_df["tmax"],
    mode="lines+markers",
    name="Tmax",
    line=dict(color="blue", width=2),
    marker=dict(size=6)
))

# Step 2: Add shaded heatwave periods as background rectangles
heatwave_groups = fig_df[fig_df["heatwave_id"].notna()].groupby("heatwave_id")

shapes = []
for _, group in heatwave_groups:
    start = group["date"].min()
    end = group["date"].max() + timedelta(days=1)  # pad right edge
    shapes.append(dict(
        type="rect",
        xref="x", yref="paper",
        x0=start, x1=end,
        y0=0, y1=1,
        fillcolor="rgba(255,0,0,0.15)",  # light red
        line_width=0,
        layer="below"
    ))

fig.update_layout(
    title="Max Temperature Forecast",
    xaxis_title="Date",
    yaxis_title="Tmax (°C)",
    shapes=shapes,
    legend=dict(title=""),
    margin=dict(l=40, r=20, t=60, b=40)
)

st.plotly_chart(fig, use_container_width=True)

# --- Color-coded Risk Table ---
emoji_map = {"Extreme": "🔥🔥", "High": "🔥", "Moderate": "🌡️", "Mild": "☀️", "None": "❄️"}
risk_display = risk_df.copy()
risk_display["⚠️"] = risk_display["risk_level"].map(emoji_map)
styled = risk_display[["date", "tmax", "risk_level", "⚠️", "high_vulnerability"]]
styled.columns = ["Date", "Tmax (°C)", "Risk Level", "", "High Vulnerability?"]
st.subheader("📋 Heatwave Risk Table")
st.dataframe(styled)

