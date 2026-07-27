"""Release and privacy gates for the 3.1.0-beta.1 frozen fixtures."""

from __future__ import annotations

from pathlib import Path
import json


FIXTURES = Path(__file__).parent / "fixtures"
FROZEN_FIXTURES = (
    "garmin_activity_archive.json",
    "garmin_fit_structural_summary.json",
    "garmin_health_events.json",
    "garmin_respiration_spo2.json",
    "garmin_segmented_charts.json",
    "garmin_sleep_streams.json",
    "garmin_sleep_structured.json",
    "garmin_summary_training.json",
)


def test_frozen_fixtures_have_provenance_and_redaction_version() -> None:
    for name in FROZEN_FIXTURES:
        fixture = json.loads((FIXTURES / name).read_text())
        assert isinstance(fixture, dict)
        assert isinstance(fixture.get("_provenance") or fixture.get("provenance"), (str, dict))
        version = fixture.get("_redaction_version")
        if version is None and isinstance(fixture.get("provenance"), dict):
            version = fixture["provenance"].get("redaction_version")
        assert version == "3.1.0-beta.1"


def test_release_metadata_targets_beta_and_core_gate() -> None:
    manifest = json.loads(Path("custom_components/garmin_connect/manifest.json").read_text())
    hacs = json.loads(Path("hacs.json").read_text())
    assert manifest["version"] == "3.1.0-beta.1"
    assert hacs["homeassistant"] == "2026.7.4"


def test_fixture_text_has_no_obvious_credentials_or_route_payloads() -> None:
    for name in FROZEN_FIXTURES:
        text = (FIXTURES / name).read_text().lower()
        assert "refresh_token" not in text
        assert "access_token" not in text
        assert "password" not in text
        if name == "garmin_activity_archive.json":
            assert "polyline" not in text
            assert "latitude" not in text
            assert "longitude" not in text
