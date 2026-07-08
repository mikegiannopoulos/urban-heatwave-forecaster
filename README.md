# <img src="assets/urban-heatwave-forecaster_new.png" alt="Climate Hazards Forecaster Logo" width="80"> Climate Hazards Forecaster

Climate Hazards Forecaster is a modular app for forecasting and assessing climate hazards from short-term weather forecasts and climatological baselines.

Today, the fully implemented module is **heat**. The new package structure, shared contracts, and summary layer are in place so that **heavy precipitation extremes** and later **drought** can be added without bending the codebase back around heat-only assumptions.

The `climate_extremes` package is the current scientific/platform namespace. The `urban_heatwave_forecaster` package remains as a compatibility layer while the broader hazard architecture takes shape.

## Current Status

- `heat` is the working module end to end: forecast fetch, climatology baseline, heatwave detection, risk assessment, and UI.
- `climate_extremes` is the platform package with shared schemas, utilities, and CLI entry points.
- `urban_heatwave_forecaster` remains available so existing imports and commands do not break mid-refactor.
- `precipitation` now has a first testing implementation with candidate definitions that can be compared side by side.

## Design Principles

- Keep hazard science module-specific.
- Standardize interfaces, not the raw physics.
- Reuse generic plumbing for data access, climatologies, run detection, and summaries.
- Prefer separate hazard scores plus a shared summary layer over a premature single total-risk number.

## Project Structure

```text
climate-hazards-forecaster/
├── src/
│   ├── climate_extremes/
│   │   ├── baselines/              # Generic climatology builders
│   │   ├── core/                   # Shared schemas, scales, city registry, summaries
│   │   ├── io/                     # Data access adapters
│   │   ├── modules/
│   │   │   ├── heat/               # Implemented heat module
│   │   │   └── precipitation/      # Candidate precipitation definitions, baselines, detection, comparison
│   │   └── cli.py                  # New platform CLI
│   └── urban_heatwave_forecaster/  # Legacy compatibility layer
├── app.py                          # Streamlit climate hazard assessment app
├── tests/                          # Unit tests for shared utilities and heat module
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Shared Module Contract

Each hazard module should be able to emit a summary with the same top-level schema:

```json
{
  "hazard": "heat",
  "event_detected": true,
  "severity_score": 78,
  "severity_class": "severe",
  "confidence": "medium",
  "key_metrics": {
    "duration_days": 4,
    "threshold_exceedance": 2.3
  },
  "metadata": {
    "legacy_risk_level": "Extreme"
  }
}
```

These summaries can then be combined by the shared multi-hazard summary layer without forcing all hazards into one raw scoring formula.

## Quick Start

### 1. Create and activate a virtual environment

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install the package

```bash
pip install -e .
```

### 3. Run the heat module with the new platform CLI

```bash
python -m climate_extremes.cli fetch-historical --city Athens
python -m climate_extremes.cli build-baseline --city Athens
python -m climate_extremes.cli heat fetch --city Athens
python -m climate_extremes.cli heat detect --city Athens
python -m climate_extremes.cli heat assess --city Athens
python -m climate_extremes.cli heat summarize --city Athens
```

Global location search is available for user-selected climate hazard runs:

```bash
python -m climate_extremes.cli search-location "Gothenburg"
```

### 4. Legacy commands still work

```bash
python -m urban_heatwave_forecaster.cli fetch --city Athens
python -m urban_heatwave_forecaster.cli detect --city Athens
python -m urban_heatwave_forecaster.cli assess --city Athens
```

### 5. Test precipitation candidate definitions

```bash
python -m climate_extremes.cli precipitation fetch-historical --city Athens
python -m climate_extremes.cli precipitation build-baseline --city Athens
python -m climate_extremes.cli precipitation fetch --city Athens
python -m climate_extremes.cli precipitation compare --city Athens
```

This compares the current candidate definitions:

- `daily-burst-95p`
- `daily-burst-99p`
- `wet-spell-3day-95p`

### 6. Launch the climate hazard dashboard

```bash
streamlit run app.py
```

The Streamlit app defaults to global city/place search, with coordinates available as
a manual scientific fallback. Built-in demo-city controls remain available only in an
advanced demo section for compatibility checks.

## Multi-Hazard Combination Strategy

The platform is designed around a layered combination model:

1. Each module computes its own hazard-specific severity.
2. Each module maps into a shared categorical language such as `none`, `moderate`, `high`, or `extreme`.
3. The platform summary identifies the primary hazard, active hazards, hazard count, and overall multi-hazard status.
4. A single total-risk score is intentionally deferred until more modules exist and the aggregation can be defended scientifically.

## Roadmap

- Evaluate precipitation definition behavior on real-city forecast and historical baselines, then promote the best-performing candidate to the default workflow
- Add compound-event logic for overlapping hazards
- Generalize the Streamlit UI from heat-only views to a module switcher
- Expand baseline builders for additional variables and hazard-specific thresholds

## License

GNU Affero General Public License v3.0 (AGPL-3.0) © 2025 Michael Giannopoulos & Contributors

## Acknowledgments

- Forecasts and archives: [Open-Meteo](https://open-meteo.com/)
- Urban vulnerability context: [HUGSI](https://hugsi.green/cities/index)
