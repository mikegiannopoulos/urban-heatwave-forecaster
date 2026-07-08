from __future__ import annotations

from typer.testing import CliRunner

from climate_extremes import cli as cli_module
from climate_extremes.core.locations import Location


def test_run_heat_location_search_fails_when_ambiguous(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        cli_module,
        "search_locations",
        lambda query: [
            Location(name="Paris", latitude=48.8567, longitude=2.3522, country_code="FR"),
            Location(name="Paris", latitude=33.6609, longitude=-95.5555, country_code="US"),
        ],
    )

    result = runner.invoke(cli_module.app, ["run-heat", "--location", "Paris"])

    assert result.exit_code == 1
    assert "ambiguous" in result.output
    assert "explicit --latitude/--longitude" in result.output


def test_run_precipitation_location_search_fails_when_ambiguous(monkeypatch):
    runner = CliRunner()

    monkeypatch.setattr(
        cli_module,
        "search_locations",
        lambda query: [
            Location(name="Paris", latitude=48.8567, longitude=2.3522, country_code="FR"),
            Location(name="Paris", latitude=33.6609, longitude=-95.5555, country_code="US"),
        ],
    )

    result = runner.invoke(
        cli_module.app,
        ["run-precipitation", "--location", "Paris"],
    )

    assert result.exit_code == 1
    assert "ambiguous" in result.output
    assert "explicit --latitude/--longitude" in result.output
