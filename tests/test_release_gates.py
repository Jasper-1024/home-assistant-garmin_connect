"""Release and privacy gates for the 3.1.0-beta.1 frozen fixtures."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from custom_components.garmin_connect.diagnostics import async_get_config_entry_diagnostics
from custom_components.garmin_connect.fit_archive import FitArchiveError, persisted_fit_summary
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    HistoryStatus,
    HistorySyncReport,
)
from custom_components.garmin_connect.history_source import (
    DAILY_SUMMARY_FIELDS,
    TRAINING_STATUS_FIELDS,
    HistorySchemaError,
    normalize_activities,
    normalize_health_events,
    normalize_respiration,
    normalize_snapshot,
    normalize_spo2,
    normalize_steps,
)
from custom_components.garmin_connect.services import async_setup_services
from custom_components.garmin_connect.sleep_archive import SleepSchemaError, parse_sleep_sessions

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

RELEASE_GATE_GROUPS: dict[str, tuple[str, ...]] = {
    "seven_day_reconciliation": (
        "tests/test_history.py::test_reconciliation_failure_stays_open_at_window_boundary",
        "tests/test_history.py::test_settled_date_stays_terminal_after_confirmation_restart_and_next_scheduler",
    ),
    "manual_repair": (
        "tests/test_history.py::test_manual_repair_accepts_one_and_31_days_but_rejects_32_before_requests",
        "tests/test_history.py::test_manual_repair_reopens_settled_date_while_archive_is_disabled",
    ),
    "fit_pacing_restart_skip_isolation": (
        "tests/test_fit_queue.py::test_fit_queue_paces_downloads_and_recovers_pending_work_after_restart",
        "tests/test_fit_queue.py::test_valid_local_fit_completes_queue_without_download",
        "tests/test_fit_queue.py::test_fit_queue_isolated_by_account_and_background_gate",
        "tests/test_fit_archive.py::test_fit_archive_validates_privately_and_atomically",
    ),
    "rate_limit_backoff_restart_expiry": (
        "tests/test_history.py::test_archive_rate_limit_enters_durable_backoff_without_cadence",
        "tests/test_history.py::test_archive_rate_limit_backoff_survives_restart_and_expires_once",
        "tests/test_history.py::test_first_sync_rate_limit_retries_after_expiry_without_restart",
    ),
    "reauth_and_failure_isolation": (
        "tests/test_history.py::test_archive_auth_classification_requires_genuine_account_failure",
        "tests/test_history.py::test_archive_cycle_failure_does_not_break_foreground_request",
        "tests/test_history.py::test_malformed_structured_record_fails_archive_without_blocking_foreground_work",
    ),
    "dormant_historical_backfill": (
        "tests/test_history.py::test_start_keeps_historical_backfill_dormant",
        "tests/test_init.py::test_real_config_entry_lifecycle_keeps_backfill_dormant_and_surfaces_visible",
    ),
    "exact_status_privacy": (
        "tests/test_release_gates.py::test_release_privacy_snapshots_cover_diagnostics_status_action_and_calendars",
        "tests/test_history.py::test_status_sensor_exposes_only_privacy_safe_placeholders",
    ),
}

REQUIRED_RELEASE_GATE_GROUPS = frozenset(
    {
        "seven_day_reconciliation",
        "manual_repair",
        "fit_pacing_restart_skip_isolation",
        "rate_limit_backoff_restart_expiry",
        "reauth_and_failure_isolation",
        "dormant_historical_backfill",
        "exact_status_privacy",
    }
)


def test_executable_release_gate_matrix_covers_the_archive_contract() -> None:
    """Keep the single release command's behavior coverage explicit."""
    assert set(RELEASE_GATE_GROUPS) == REQUIRED_RELEASE_GATE_GROUPS
    for targets in RELEASE_GATE_GROUPS.values():
        assert targets
        for target in targets:
            path, separator, test_name = target.partition("::")
            assert separator == "::"
            assert path.startswith("tests/")
            assert test_name.startswith("test_")
            source = (ROOT / path).read_text()
            assert f"def {test_name}" in source


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
            assert persisted_fit_summary(payload)["message_counts"] == {}
            return "empty"
        if stem == "garmin_activity_archive":
            assert normalize_activities([], date(2026, 7, 24)) == ()
        elif stem == "garmin_health_events":
            assert normalize_health_events([], date(2026, 7, 24)) == ()
        elif stem == "garmin_respiration_spo2":
            assert normalize_respiration({"respirationValuesArray": []}, date(2026, 7, 24)).presence == "empty"
            assert normalize_spo2({"spO2SingleValues": []}, date(2026, 7, 24), "single").presence == "empty"
        elif stem == "garmin_segmented_charts":
            assert normalize_steps({"stepsValuesArray": []}, date(2026, 7, 24)).readings == ()
        elif stem == "garmin_sleep_streams":
            assert parse_sleep_sessions(fixture, date(2026, 7, 24)) == ()
        elif stem == "garmin_sleep_structured":
            assert parse_sleep_sessions(fixture, date(2026, 2, 28)) == ()
        elif stem == "garmin_summary_training":
            assert normalize_snapshot({}, date(2026, 7, 24), DAILY_SUMMARY_FIELDS).fields["abnormal_heart_rate_alerts"][0] == "absent"
            assert normalize_snapshot({}, date(2026, 7, 24), TRAINING_STATUS_FIELDS).fields
        assert _is_structurally_empty(payload)
        return "empty"
    if state == "schema_drift":
        assert fixture.get("unknown_structural_field") == "redacted"
        try:
            if stem == "garmin_respiration_spo2":
                normalize_respiration(fixture["respiration"], date(2026, 7, 24))
            elif stem == "garmin_segmented_charts":
                normalize_steps(fixture["steps"], date(2026, 7, 24))
            elif stem == "garmin_sleep_streams":
                parse_sleep_sessions(fixture, date(2026, 7, 24))
            elif stem == "garmin_summary_training":
                normalize_snapshot(fixture["daily_summary"], date(2026, 7, 24), DAILY_SUMMARY_FIELDS)
            elif stem == "garmin_sleep_structured":
                parse_sleep_sessions(fixture, date(2026, 2, 28))
            elif stem == "garmin_activity_archive":
                normalize_activities([fixture["lastActivity"]], date(2026, 7, 24))
            elif stem == "garmin_health_events":
                normalize_health_events(fixture["events"], date(2026, 7, 24))
            elif stem == "garmin_fit_structural_summary":
                persisted_fit_summary(fixture["summary"])
            else:
                return "schema-drift"
        except (FitArchiveError, HistorySchemaError, SleepSchemaError):
            return "schema-drift"
        raise AssertionError(f"{stem} schema drift was accepted")
    if stem == "garmin_sleep_streams":
        assert isinstance(fixture.get("sleepHeartRateValueDescriptorsDTOList"), list)
        assert fixture["sleepHeartRateValueDescriptorsDTOList"]
        assert isinstance(payload, list)
        return "success"
    assert payload not in ({}, [], None)
    source = fixture if stem == "garmin_sleep_structured" else json.loads((FIXTURES / f"{stem}.json").read_text())
    if stem == "garmin_activity_archive":
        assert normalize_activities(source["activities"], date(2026, 7, 24))
    elif stem == "garmin_health_events":
        assert normalize_health_events(source["events"], date(2026, 7, 24))
    elif stem == "garmin_sleep_structured":
        assert parse_sleep_sessions(source, date(2026, 2, 28))
    elif stem == "garmin_fit_structural_summary":
        assert persisted_fit_summary(source["summary"])
    elif stem == "garmin_respiration_spo2":
        assert normalize_respiration(source["respiration"], date(2026, 7, 24)).presence == "present"
        assert normalize_spo2(source["spo2"], date(2026, 7, 24), "continuous").presence == "present"
    elif stem == "garmin_segmented_charts":
        assert normalize_steps(source["steps"], date(2026, 7, 24))
    elif stem == "garmin_sleep_streams":
        assert parse_sleep_sessions(source, date(2026, 7, 24))
    elif stem == "garmin_summary_training":
        assert normalize_snapshot(source["daily_summary"], date(2026, 7, 24), DAILY_SUMMARY_FIELDS)
        assert normalize_snapshot(source["training_status"], date(2026, 7, 24), TRAINING_STATUS_FIELDS)
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


def test_release_guidance_is_prospective_and_reversible() -> None:
    """Operator guidance must not revive the fixed-date backfill rollout."""
    guidance = (ROOT / "docs" / "release-3.1.0-beta.1.md").read_text()
    assert "2026-07-24" not in guidance
    assert "full-year enablement switch" in guidance
    assert "Prospective Archive" in guidance
    assert "Historical Backfill" in guidance
    assert "Home Assistant backup" in guidance
    assert "explicitly enable" in guidance
    assert "Restart Home Assistant" in guidance
    assert "Disablement is the reversible stop mechanism" in guidance
    assert "no archive deletion" in guidance
    assert "fifteen-minute freshness target" in guidance
    assert "seven-day reconciliation" in guidance


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
