import pandas as pd
from pathlib import Path

def assess_heatwave_risk(df: pd.DataFrame, vulnerability_df: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns a risk level based on tmax (daily max temperature) and modifies it using vulnerability data.

    Args:
        df (pd.DataFrame): DataFrame with columns ['date', 'tmin', 'tmax', 'city', 'is_hot'].
        vulnerability_df (pd.DataFrame): DataFrame with columns ['city', 'elderly_percent',
        'green_cover_percent', 'density_per_km2'].

    Returns:
        pd.DataFrame: DataFrame with an additional 'risk_level' column.
    """

    def categorize(temp):
        if temp >= 38:
            return "Extreme"
        elif temp >= 35:
            return "High"
        elif temp >= 32:
            return "Moderate"
        elif temp >= 30:
            return "Mild"
        else:
            return "None"

    df["risk_level"] = df["tmax"].apply(categorize)
    # Normalize city names
    df["city"] = df["city"].str.strip().str.lower()
    vulnerability_df["city"] = vulnerability_df["city"].str.strip().str.lower()

    # Merge vulnerability data
    df = df.merge(vulnerability_df, on="city", how="left")

    # Optional: Flag if vulnerability is high
    df["high_vulnerability"] = (
        (df["elderly_percent"] > 20) |
        (df["density_per_km2"] > 2000) |
        (df["green_cover_percent"] < 25)
    )

    # Optional: Escalate risk level if vulnerability is high
    escalation_map = {
        "None": "Mild",
        "Mild": "Moderate",
        "Moderate": "High",
        "High": "Extreme",
        "Extreme": "Extreme"  # Cap
    }

    df.loc[df["high_vulnerability"], "risk_level"] = df.loc[df["high_vulnerability"], "risk_level"].map(escalation_map)

    return df

if __name__ == "__main__":
    city_name = "athens" #  “athens, rome, or stockholm”
    forecast_file = Path(f"data/processed/{city_name}_forecast_with_heatwaves.csv")
    vulnerability_file = Path("data/raw/urban_vulnerability.csv")

    if not forecast_file.exists():
        raise FileNotFoundError(f"No forecast file found at: {forecast_file}")
    if not vulnerability_file.exists():
        raise FileNotFoundError(f"No vulnerability file found at: {vulnerability_file}")

    df = pd.read_csv(forecast_file)
    vulnerability_df = pd.read_csv(vulnerability_file)

    if "is_hot" not in df.columns:
        if "exceeds_95p" in df.columns:
            df["is_hot"] = df["exceeds_95p"]
        else:
            raise ValueError("Missing both 'is_hot' and 'exceeds_95p' columns. Run heatwave detection first.")


    df_with_risks = assess_heatwave_risk(df, vulnerability_df)

    # Save the final enriched risk dataset
    output_path = Path(f"data/processed/{city_name}_heatwave_risk.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_with_risks.to_csv(output_path, index=False)

    print(f"✅ Enriched dataset saved to: {output_path}")

    print("🌡️ Heatwave Risk Assessment:")
    print(df_with_risks)