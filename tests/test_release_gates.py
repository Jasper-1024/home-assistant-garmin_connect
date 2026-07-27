"""Release and privacy gates for the 3.1.0-beta.1 frozen fixtures."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.garmin_connect.diagnostics import async_get_config_entry_diagnostics
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    HistoryStatus,
    HistorySyncReport,
)
from custom_components.garmin_connect.services import async_setup_services

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
FAMILY_MARKERS = {
    "garmin_activity_archive": "lastActivity",
    "garmin_fit_structural_summary": "summary",
    "garmin_health_events": "events",
    "garmin_respiration_spo2": "respiration",
    "garmin_segmented_charts": "steps",
    "garmin_sleep_streams": "sleepHeartRate",
    "garmin_sleep_structured": "sleepData",
    "garmin_summary_training": "daily_summary",
}


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
            assert FAMILY_MARKERS[stem] in fixture
            result = _dispatch_fixture_contract(stem, state, fixture)
            assert result in {"success", "empty", "schema-drift"}


def _dispatch_fixture_contract(stem: str, state: str, fixture: dict) -> str:
    """Run the bounded family contract for each release fixture state."""
    marker = FAMILY_MARKERS[stem]
    payload = fixture[marker]
    if state == "sparse":
        if stem == "garmin_fit_structural_summary":
            assert payload == {"message_counts": {}, "message_fields": {}}
            return "empty"
        assert _is_structurally_empty(payload)
        return "empty"
    if state == "schema_drift":
        assert fixture.get("unknown_structural_field") == "redacted"
        return "schema-drift"
    assert payload not in ({}, [], None)
    if stem == "garmin_fit_structural_summary":
        assert isinstance(payload.get("message_counts"), dict)
        assert isinstance(payload.get("message_fields"), dict)
    elif stem == "garmin_activity_archive":
        assert isinstance(payload, dict) and ("activityType" in payload or "activityTypeKey" in payload)
    elif stem == "garmin_health_events":
        assert isinstance(payload, list) and payload and isinstance(payload[0], dict)
    elif stem == "garmin_summary_training":
        assert isinstance(payload, dict) and "calendarDate" in payload
    else:
        assert isinstance(payload, (dict, list))
    return "success"


def _is_structurally_empty(value: object) -> bool:
    """Accept only bounded empty containers/None for sparse fixtures."""
    if value is None:
        return True
    if isinstance(value, (list, tuple)):
        return not value
    if isinstance(value, dict):
        return all(_is_structurally_empty(item) for item in value.values())
    return False


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
    forbidden = {"token", "refresh_token", "client_id", "account_key", "opaque-account", "email", "address", "gps", "latitude", "longitude", "measurement", "measurements", "array", "values"}
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
    archive = object.__new__(GarminHistoryArchive)
    archive._status = HistoryStatus(HistoryArchiveState.IDLE, current_date="2026-07-24")
    archive._sleep_sessions = {"2026": {"sleep": {"start": "2026-07-24T22:00:00+00:00", "end": "2026-07-25T06:00:00+00:00", "kind": "main"}}}
    archive._health_events = {"2026": {"health": {"start": "2026-07-24T10:00:00+00:00", "end": "2026-07-24T10:15:00+00:00", "category": "activity", "event_type": "event"}}}
    archive._activities = {"2026": {"activity": {"start": "2026-07-24T12:00:00+00:00", "end": "2026-07-24T13:00:00+00:00", "name": "Activity"}}}
    archive._async_load_sleep_partitions = AsyncMock()
    status = archive.status.as_attributes()
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
    calendar_events = {
        name: await archive.async_get_calendar_events(name, date(2026, 7, 24), date(2026, 7, 25))
        for name in ("sleep", "health", "activity")
    }
    calendars = {
        name: tuple(
            {"start": event.start.isoformat(), "end": event.end.isoformat(), "summary": event.summary}
            for event in events
        )
        for name, events in calendar_events.items()
    }
    entry = MagicMock(data={"history_account_key": "opaque-account-key"}, runtime_data=MagicMock())
    entry.runtime_data.core = MagicMock(data={}, last_update_success=True, update_interval=None)
    entry.runtime_data.history_archive = archive
    hass = MagicMock()
    hass.config_entries.async_entries.return_value = [entry]
    await async_setup_services(hass)
    handler = next(call[0][2] for call in hass.services.async_register.call_args_list if call[0][1] == "sync_history")
    call = MagicMock(data={"date": date(2026, 7, 24)})
    archive.async_sync_range = AsyncMock(return_value=report)
    action_response = await handler(call)
    history_field = MagicMock()
    history_field.name = "history_archive"
    with patch("custom_components.garmin_connect.diagnostics.fields", return_value=[history_field]):
        diagnostics = await async_get_config_entry_diagnostics(MagicMock(), entry)
    _assert_private_snapshot({"diagnostics": diagnostics, "status": status, "action": action_response, "calendars": calendars})
