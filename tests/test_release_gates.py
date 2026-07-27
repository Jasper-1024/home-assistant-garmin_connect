"""Release and privacy gates for the 3.1.0-beta.1 frozen fixtures."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from custom_components.garmin_connect.diagnostics import async_get_config_entry_diagnostics
from custom_components.garmin_connect.history import HistoryCalendarEvent, HistoryStatus, HistorySyncReport

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
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
STATES = ("success", "sparse", "schema_drift")


def test_frozen_fixtures_have_provenance_and_redaction_version() -> None:
    for name in FROZEN_FIXTURES:
        fixture = json.loads((FIXTURES / name).read_text())
        assert isinstance(fixture, dict)
        assert isinstance(fixture.get("_provenance") or fixture.get("provenance"), (str, dict))
        version = fixture.get("_redaction_version")
        if version is None and isinstance(fixture.get("provenance"), dict):
            version = fixture["provenance"].get("redaction_version")
        assert version == "3.1.0-beta.1"


def test_each_family_has_all_sanitized_release_states() -> None:
    for name in FROZEN_FIXTURES:
        stem = name.removesuffix(".json")
        for state in STATES:
            fixture = json.loads((FIXTURES / f"{stem}.{state}.json").read_text())
            assert fixture["_redaction_version"] == "3.1.0-beta.1"
            assert isinstance(fixture["_provenance"], str)


def test_release_metadata_targets_beta_and_core_gate() -> None:
    manifest = json.loads((ROOT / "custom_components/garmin_connect/manifest.json").read_text())
    hacs = json.loads((ROOT / "hacs.json").read_text())
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


def _assert_private_snapshot(value: object) -> None:
    forbidden = {"token", "refresh_token", "client_id", "account_key", "email", "address", "gps", "latitude", "longitude", "measurement", "measurements", "array", "values"}
    if isinstance(value, dict):
        assert not forbidden.intersection(value)
        for item in value.values():
            _assert_private_snapshot(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _assert_private_snapshot(item)
    elif isinstance(value, str):
        assert not any(word in value.lower() for word in forbidden)


@pytest.mark.asyncio
async def test_release_privacy_snapshots_cover_diagnostics_status_action_and_calendars() -> None:
    status = HistoryStatus("idle", current_date="2026-07-24").as_attributes()
    report = HistorySyncReport(outcome="written")
    action_response = {
        "outcome": report.outcome,
        "processed_dates": [],
        "count_basis": "adapter_classification",
        "inserted_count": report.inserted_count,
        "updated_count": report.updated_count,
        "skipped_count": report.skipped_count,
        "error_type": report.error_type,
    }
    start = datetime(2026, 7, 24, tzinfo=UTC)
    calendars = {
        name: (HistoryCalendarEvent(start, start, name),)
        for name in ("sleep", "health", "activity")
    }
    entry = MagicMock(data={"history_account_key": "opaque-account-key"}, runtime_data=MagicMock())
    entry.runtime_data.core = MagicMock(data={}, last_update_success=True, update_interval=None)
    with patch("custom_components.garmin_connect.diagnostics.fields", return_value=[]):
        diagnostics = await async_get_config_entry_diagnostics(MagicMock(), entry)
    _assert_private_snapshot({"diagnostics": diagnostics, "status": status, "action": action_response, "calendars": calendars})
