import pandas as pd
from pathlib import Path

def detect_heatwaves(forecast_path, climatology_path, min_run=3):
    """Return forecast df with two new columns:
       • exceeds_95p  -  both Tmin & Tmax above daily 95-percentile
       • heatwave_id  -  integer ID of each ≥min_run-day event (NaN elsewhere)
    """
    # ── load ────────────────────────────────────────────────────────────────
    fc  = pd.read_csv(forecast_path,   parse_dates=["date"])
    clim = pd.read_csv(climatology_path)

    # ── join thresholds ────────────────────────────────────────────────────
    fc["day_of_year"] = fc["date"].dt.dayofyear
    fc = fc.merge(clim, on="day_of_year", how="left")

    # ── flag exceedance ────────────────────────────────────────────────────
    fc["exceeds_95p"] = (
        (fc["tmin"] > fc["tmin_95p"]) &
        (fc["tmax"] > fc["tmax_95p"])
    )

    # ── identify consecutive runs ≥ min_run ────────────────────────────────
    # 1) break sequence whenever the flag is False
    grp = (fc["exceeds_95p"] != fc["exceeds_95p"].shift()).cumsum()
    # 2) size of each run
    run_lengths = fc.groupby(grp)["exceeds_95p"].transform("sum")
    # 3) assign ID only to runs long enough
    fc["heatwave_id"] = (
        grp.where((fc["exceeds_95p"]) & (run_lengths >= min_run))
    )

    return fc.drop(columns=["day_of_year"])

# ── CLI helper for quick testing ───────────────────────────────────────────
if __name__ == "__main__":
    city = "athens" #  “athens, rome, or stockholm”
    forecast_path    = Path(f"data/raw/{city}_forecast.csv")
    climatology_path = Path(f"data/processed/{city}_climatology_95p.csv")

    out = detect_heatwaves(forecast_path, climatology_path)
    out.to_csv(f"data/processed/{city}_forecast_with_heatwaves.csv", index=False)

    print(out[["date", "tmin", "tmax", "exceeds_95p", "heatwave_id"]])
