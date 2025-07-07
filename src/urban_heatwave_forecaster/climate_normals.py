import pandas as pd
from pathlib import Path

def build_percentile_climatology(city_name, input_path=None, output_path=None):
    city_name = city_name.lower()

    # Paths
    if input_path is None:
        input_path = Path(f"data/raw/{city_name}_historical.csv")
    if output_path is None:
        output_path = Path(f"data/processed/{city_name}_climatology_95p.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load historical data
    df = pd.read_csv(input_path, parse_dates=["date"])
    df["day_of_year"] = df["date"].dt.dayofyear

    # Drop leap day to keep it simple (optional)
    df = df[df["day_of_year"] != 366]

    # Group by day of year and compute 95th percentiles
    climatology = df.groupby("day_of_year").agg({
        "tmin": lambda x: round(x.quantile(0.95), 2),
        "tmax": lambda x: round(x.quantile(0.95), 2)
    }).reset_index()

    climatology.rename(columns={
        "tmin": "tmin_95p",
        "tmax": "tmax_95p"
    }, inplace=True)

    climatology.to_csv(output_path, index=False)
    print(f"✅ Saved 95th percentile climatology to: {output_path}")

    return climatology

if __name__ == "__main__":
    for city in ["Athens", "Rome", "Stockholm"]:
        build_percentile_climatology(city)
