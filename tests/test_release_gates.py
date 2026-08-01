"""Release and privacy gates for the 3.1.0-beta.2 candidate and beta.1 historical fixtures."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml
from homeassistant.config_entries import ConfigEntryState

from custom_components.garmin_connect import history as history_module
from custom_components.garmin_connect.const import DEFAULT_SCAN_INTERVAL
from custom_components.garmin_connect.coordinator import GarminConnectCoordinators
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
from scripts import release_gate

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests" / "fixtures"
FORK_URL = "https://github.com/Jasper-1024/home-assistant-garmin_connect"
FORK_ISSUES_URL = f"{FORK_URL}/issues"
FORK_DISCUSSIONS_URL = f"{FORK_URL}/discussions"
FORK_PULLS_URL = f"{FORK_URL}/pulls"
FORK_SECURITY_ADVISORIES_URL = f"{FORK_URL}/security/advisories"
UPSTREAM_URL = "https://github.com/cyberjunky/home-assistant-garmin_connect"
PUBLIC_FEEDBACK_URL = FORK_PULLS_URL
DEFAULT_POLLING_PROMISE = "900 seconds (15 minutes)"
MARKDOWN_LINK_URLS = re.compile(r"\[[^]]+\]\((https?://[^)\s]+)\)")
POLLING_DEFAULT_CLAIM = re.compile(
    r"(?ix)^"
    r"(?=.*\b(?:poll(?:ing|ed)?|scan[ -]interval|next[ -]poll)\b)"
    r"(?=.*\bdefault(?:s|ing)?\b)"
    r"(?=.*\b(?:\d+\s*(?:seconds?|minutes?)|fifteen[- ]minutes)\b)"
    r".+$"
)
LEGACY_MAINTENANCE_URLS = (
    f"{UPSTREAM_URL}/issues",
    f"{UPSTREAM_URL}/discussions",
    f"{UPSTREAM_URL}/pulls",
    f"{UPSTREAM_URL}/security/advisories",
)
USER_VISIBLE_MAINTENANCE_DOCUMENTS = (
    ROOT / "README.md",
    ROOT / "docs" / "release-3.1.0-beta.2.md",
    ROOT / "docs" / "garmin_connect.markdown",
    ROOT / "docs" / "feedback" / "README.md",
    ROOT / "SECURITY.md",
    *(ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml"),
    ROOT / ".github" / "pull_request_template.md",
)
USER_VISIBLE_POLLING_DOCUMENTS = (
    ROOT / "README.md",
    ROOT / "SECURITY.md",
    *ROOT.glob("docs/**/*.md"),
    *ROOT.glob("docs/**/*.markdown"),
    *(ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml"),
    ROOT / ".github" / "pull_request_template.md",
)
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
        "tests/test_fit_queue.py::test_fit_backlog_over_256_survives_restart_and_is_fully_consumed",
        "tests/test_fit_queue.py::test_valid_local_fit_completes_queue_without_download",
        "tests/test_fit_queue.py::test_fit_queue_isolated_by_account_and_background_gate",
        "tests/test_fit_queue.py::test_runtime_corrupt_local_fit_is_removed_and_downloaded_again",
        "tests/test_fit_archive.py::test_fit_archive_validates_privately_and_atomically",
        "tests/test_fit_archive.py::test_fit_archive_degrades_when_directory_fsync_is_unsupported",
        "tests/test_fit_archive.py::test_fit_archive_uses_path_chmod_without_fchmod",
        "tests/test_fit_archive.py::test_fit_archive_uses_windows_safe_permission_fallback",
        "tests/test_fit_archive.py::test_fit_archive_does_not_swallow_path_permission_errors",
    ),
    "rate_limit_backoff_restart_expiry": (
        "tests/test_history.py::test_archive_rate_limit_enters_durable_backoff_without_cadence",
        "tests/test_history.py::test_archive_rate_limit_backoff_survives_restart_and_expires_once",
        "tests/test_history.py::test_first_sync_rate_limit_retries_after_expiry_without_restart",
        "tests/test_history.py::test_successful_archive_status_persists_and_restores_schedule",
    ),
    "reauth_and_failure_isolation": (
        "tests/test_history.py::test_archive_auth_classification_requires_genuine_account_failure",
        "tests/test_history.py::test_archive_cycle_failure_does_not_break_foreground_request",
        "tests/test_history.py::test_malformed_structured_record_fails_archive_without_blocking_foreground_work",
    ),
    "downgrade_owner_continuity": (
        "tests/test_history.py::test_downgrade_reauth_recovers_bound_archive_identity",
        "tests/test_history.py::test_missing_key_never_adopts_mismatched_or_unbound_archive",
        "tests/test_history.py::test_numeric_legacy_owner_binding_migrates_after_profile_verification",
        "tests/test_history.py::test_downgrade_reauth_preserves_recorder_calendar_and_valid_fit_artifacts",
    ),
    "dormant_historical_backfill": (
        "tests/test_history.py::test_start_keeps_historical_backfill_dormant",
        "tests/test_init.py::test_real_config_entry_lifecycle_keeps_backfill_dormant_and_surfaces_visible",
    ),
    "exact_status_privacy": (
        "tests/test_release_gates.py::test_release_privacy_snapshots_cover_diagnostics_status_action_and_calendars",
        "tests/test_history.py::test_status_sensor_exposes_only_privacy_safe_placeholders",
    ),
    "recorder_capability_contract": (
        "tests/test_history.py::test_recorder_compatibility_uses_real_scratch_recorder",
        "tests/test_history.py::test_has_supported_home_assistant_version",
        "tests/test_history.py::test_recorder_compatibility_accepts_supported_versions_with_a_slow_queue_task",
        "tests/test_history.py::test_recorder_compatibility_rejects_versions_below_the_hacs_minimum",
        "tests/test_history.py::test_recorder_compatibility_rejects_missing_durable_import_seam",
        "tests/test_init.py::test_stalled_recorder_check_starts_archive_in_background_and_can_cancel",
    ),
}

REQUIRED_RELEASE_GATE_GROUPS = frozenset(
    {
        "seven_day_reconciliation",
        "manual_repair",
        "fit_pacing_restart_skip_isolation",
        "rate_limit_backoff_restart_expiry",
        "reauth_and_failure_isolation",
        "downgrade_owner_continuity",
        "dormant_historical_backfill",
        "exact_status_privacy",
        "recorder_capability_contract",
    }
)
DOCUMENTED_RELEASE_GATE_EXTRAS = (
    "tests/test_history_recorder.py::test_release_gate_scratch_recorder_restart_revision_and_no_state_changed",
    "tests/test_fit_archive.py::test_optional_private_captured_fit_replay",
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

    guidance = (ROOT / "docs" / "release-3.1.0-beta.2.md").read_text()
    command_match = re.search(
        r"(?ms)^The executable release-gate command is the following single pytest invocation\n"
        r"\(the matrix is also declared in `tests/test_release_gates\.py`\):\n\n"
        r"^```text\n(?P<command>.*?)^```$",
        guidance,
    )
    assert command_match is not None
    command_lines = tuple(line.strip() for line in command_match["command"].splitlines() if line.strip())
    assert command_lines
    assert command_lines[0].removesuffix("\\").strip() in {"pytest", "pytest -q"}
    assert all(line.endswith("\\") for line in command_lines[:-1])
    assert not command_lines[-1].endswith("\\")
    documented_targets = tuple(
        line.removesuffix("\\").strip()
        for line in command_lines[1:]
    )
    assert documented_targets == (
        "tests/test_release_gates.py",
        *(target for group in RELEASE_GATE_GROUPS.values() for target in group),
        *DOCUMENTED_RELEASE_GATE_EXTRAS,
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


def _markdown_link_urls(text: str) -> set[str]:
    """Return HTTP(S) targets from Markdown links."""
    return set(MARKDOWN_LINK_URLS.findall(text))


def _yaml_strings(value: object) -> list[str]:
    """Flatten scalar strings from a parsed issue-form document."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for child in value for item in _yaml_strings(child)]
    if isinstance(value, dict):
        return [item for child in value.values() for item in _yaml_strings(child)]
    return []


def _markdown_table_cells(line: str) -> list[str]:
    """Return cells from one simple Markdown table row."""
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _polling_default_claims(text: str) -> list[str]:
    """Return self-contained polling-default statements from user-facing text."""
    claims: list[str] = []
    prose_lines: list[str] = []
    lines = text.splitlines()
    index = 0

    while index < len(lines):
        if not lines[index].lstrip().startswith("|"):
            prose_lines.append(lines[index])
            index += 1
            continue

        table_lines: list[str] = []
        while index < len(lines) and lines[index].lstrip().startswith("|"):
            table_lines.append(lines[index])
            index += 1

        if len(table_lines) < 3:
            continue

        header = _markdown_table_cells(table_lines[0])
        for row in table_lines[2:]:
            claim = " ".join((*header, *_markdown_table_cells(row)))
            if POLLING_DEFAULT_CLAIM.fullmatch(claim):
                claims.append(claim)

    for paragraph in re.split(r"\n\s*\n", "\n".join(prose_lines)):
        for sentence in re.split(r"(?<=[.!?;])\s+", " ".join(paragraph.split())):
            if POLLING_DEFAULT_CLAIM.fullmatch(sentence):
                claims.append(sentence)

    return claims


def _canonical_home_assistant_version(value: object) -> tuple[int, int, int]:
    """Parse Home Assistant's stable three-part release version."""
    assert isinstance(value, str)
    match = re.fullmatch(
        r"([1-9][0-9]*)\.([0-9]|[1-9][0-9]*)\.([0-9]|[1-9][0-9]*)", value
    )
    assert match, f"not a canonical Home Assistant version: {value!r}"
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def _workflow_home_assistant_pins() -> dict[str, list[str]]:
    """Read release workflow install commands without a YAML dependency."""
    workflow = (ROOT / ".github" / "workflows" / "tests.yaml").read_text().splitlines()
    job_name: str | None = None
    pins: dict[str, list[str]] = {"lint": [], "tests": [], "release-gates": []}
    install = re.compile(
        r'\s*- run: pip install "homeassistant==([^"]+)" -r requirements\.txt -r requirements_lint\.txt'
    )
    for line in workflow:
        job = re.fullmatch(r"  ([a-z][a-z0-9_-]*):", line)
        if job:
            job_name = job.group(1)
            continue
        install_match = install.fullmatch(line)
        if install_match and job_name in pins:
            pins[job_name].append(install_match.group(1))
    return pins


def test_release_metadata_targets_beta_and_core_gate() -> None:
    manifest = json.loads((ROOT / "custom_components/garmin_connect/manifest.json").read_text())
    hacs = json.loads((ROOT / "hacs.json").read_text())
    requirements = (ROOT / "requirements.txt").read_text().splitlines()
    floor = hacs["homeassistant"]
    parsed_floor = _canonical_home_assistant_version(floor)

    assert manifest["version"] == "3.1.0-beta.8"
    assert [line for line in requirements if line.startswith("homeassistant")] == [
        f"homeassistant>={floor}"
    ]
    assert _workflow_home_assistant_pins() == {
        "lint": [floor],
        "tests": [floor],
        "release-gates": [floor],
    }
    assert history_module._RECORDER_MINIMUM_HOME_ASSISTANT_VERSION == parsed_floor
    assert history_module._has_supported_home_assistant_version(floor)


def test_release_gate_metadata_matches_manifest_requirements_and_beta_semver() -> None:
    """The offline gate must cover every runtime dependency declared by manifest."""
    release = release_gate.read_release_metadata(ROOT)

    assert release.version == "3.1.0-beta.8"
    assert release.home_assistant == "2026.7.4"
    assert release.requirements["ha-garmin"] == "0.1.31"
    assert release.requirements["garmin-fit-sdk"] == "21.208.0"
    assert release_gate.BETA_VERSION.fullmatch(release.version)
    assert not release_gate.BETA_VERSION.fullmatch("3.1.0-beta1")
    assert not release_gate.BETA_VERSION.fullmatch("3.1.0-beta-1")


def test_release_gate_requires_complete_candidate_identity_arguments() -> None:
    """A tag cannot be checked independently from its intended candidate and Release."""
    assert release_gate.main(["--release-tag", "3.1.0-beta.2"]) == 1



def test_release_gate_rejects_beta1_tag_for_beta2_candidate() -> None:
    """The historical beta.1 tag cannot identify the beta.2 manifest candidate."""
    release = release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {})

    with pytest.raises(release_gate.GateError, match="must equal manifest"):
        release_gate.check_candidate_identity(
            release, "a" * 40, "3.1.0-beta.1", "3.1.0-beta.2"
        )


def test_release_gate_defaults_identity_versions_from_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A candidate SHA uses the dynamic manifest version for both identity names."""
    release = release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {})
    observed: list[tuple[object, ...]] = []
    monkeypatch.setattr(release_gate, "read_release_metadata", lambda: release)
    monkeypatch.setattr(
        release_gate, "check_candidate_identity", lambda *args: observed.append(args)
    )

    assert release_gate.main(["--candidate-sha", "a" * 40]) == 0
    assert observed == [(release, "a" * 40, "3.1.0-beta.2", "3.1.0-beta.2")]


def test_release_gate_reports_missing_manifest_distribution(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing manifest distribution must fail the CLI without a traceback."""
    release = release_gate.ReleaseMetadata(
        "3.1.0-beta.2", "2026.7.4", {}, {"ha-garmin": "0.1.31"}
    )
    monkeypatch.setattr(release_gate, "read_release_metadata", lambda: release)
    monkeypatch.setattr(
        release_gate.metadata,
        "version",
        lambda _: (_ for _ in ()).throw(release_gate.metadata.PackageNotFoundError()),
    )

    assert release_gate.main(["--check-installed"]) == 1
    assert capsys.readouterr().err == (
        "release gate failed: required distribution ha-garmin is not installed; "
        "expected ha-garmin==0.1.31\n"
    )


def test_release_gate_reports_unimportable_manifest_module(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A broken manifest import must fail the CLI without a traceback."""
    release = release_gate.ReleaseMetadata(
        "3.1.0-beta.2", "2026.7.4", {}, {"ha-garmin": "0.1.31"}
    )
    monkeypatch.setattr(release_gate, "read_release_metadata", lambda: release)
    monkeypatch.setattr(release_gate.metadata, "version", lambda _: "0.1.31")
    monkeypatch.setattr(
        release_gate.importlib,
        "import_module",
        lambda _: (_ for _ in ()).throw(ImportError("broken dependency")),
    )

    assert release_gate.main(["--check-installed"]) == 1
    assert capsys.readouterr().err == (
        "release gate failed: required module ha_garmin for distribution ha-garmin "
        "cannot be imported; expected ha-garmin==0.1.31\n"
    )


def test_release_gate_reports_missing_home_assistant_distribution(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing Home Assistant distribution must fail the CLI without a traceback."""
    release = release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {})
    monkeypatch.setattr(release_gate, "read_release_metadata", lambda: release)
    monkeypatch.setattr(
        release_gate.metadata,
        "version",
        lambda _: (_ for _ in ()).throw(release_gate.metadata.PackageNotFoundError()),
    )

    assert release_gate.main(["--check-installed"]) == 1
    assert capsys.readouterr().err == (
        "release gate failed: required distribution homeassistant is not installed; "
        "expected homeassistant==2026.7.4\n"
    )


def _release_gate_git_responses(
    candidate: str,
    head: str,
    tag_type: str,
    tagged_commit: str,
    worktree_status: str = "",
) -> object:
    """Return a deterministic Git seam for candidate-identity tests."""

    def fake_git(_: Path, *args: str) -> str:
        responses = {
            ("status", "--porcelain"): worktree_status,
            ("rev-parse", f"{candidate}^{{commit}}"): candidate,
            ("rev-parse", "--verify", f"{candidate}^{{commit}}"): candidate,
            ("rev-parse", "--verify", "HEAD^{commit}"): head,
            ("cat-file", "-t", "refs/tags/3.1.0-beta.2"): tag_type,
            (
                "rev-parse",
                "--verify",
                "refs/tags/3.1.0-beta.2^{commit}",
            ): tagged_commit,
            ("rev-parse", "refs/tags/3.1.0-beta.2^{commit}"): tagged_commit,
        }
        return responses[args]

    return fake_git


def test_release_gate_rejects_candidate_sha_not_at_checkout_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tag on another commit cannot pass from this clean checkout."""
    candidate = "a" * 40
    monkeypatch.setattr(
        release_gate,
        "_git",
        _release_gate_git_responses(candidate, "b" * 40, "tag", candidate),
    )

    with pytest.raises(release_gate.GateError, match="current HEAD"):
        release_gate.check_candidate_identity(
            release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {}),
            candidate,
            "3.1.0-beta.2",
            "3.1.0-beta.2",
        )


def test_release_gate_rejects_dirty_worktree(monkeypatch: pytest.MonkeyPatch) -> None:
    """Candidate identity cannot be established from a modified checkout."""
    candidate = "a" * 40
    monkeypatch.setattr(
        release_gate,
        "_git",
        _release_gate_git_responses(candidate, candidate, "tag", candidate, " M manifest.json"),
    )

    with pytest.raises(release_gate.GateError, match="clean worktree"):
        release_gate.check_candidate_identity(
            release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {}),
            candidate,
            "3.1.0-beta.2",
            "3.1.0-beta.2",
        )


def test_release_gate_rejects_lightweight_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    """A direct commit ref is not an annotated release tag."""
    candidate = "a" * 40
    monkeypatch.setattr(
        release_gate,
        "_git",
        _release_gate_git_responses(candidate, candidate, "commit", candidate),
    )

    with pytest.raises(release_gate.GateError, match="annotated"):
        release_gate.check_candidate_identity(
            release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {}),
            candidate,
            "3.1.0-beta.2",
            "3.1.0-beta.2",
        )


def test_release_gate_rejects_tag_at_different_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An annotated tag must resolve to the candidate checkout commit."""
    candidate = "a" * 40
    monkeypatch.setattr(
        release_gate,
        "_git",
        _release_gate_git_responses(candidate, candidate, "tag", "b" * 40),
    )

    with pytest.raises(release_gate.GateError, match="does not resolve"):
        release_gate.check_candidate_identity(
            release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {}),
            candidate,
            "3.1.0-beta.2",
            "3.1.0-beta.2",
        )


def test_release_gate_rejects_ambiguous_candidate_ref() -> None:
    """Candidate identity must start with a complete commit object ID."""
    with pytest.raises(release_gate.GateError, match="full commit SHA"):
        release_gate.check_candidate_identity(
            release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {}),
            "HEAD",
            "3.1.0-beta.2",
            "3.1.0-beta.2",
        )


def test_release_gate_accepts_annotated_tag_at_checkout_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A full commit SHA, HEAD, and annotated tag must identify one commit."""
    candidate = "a" * 40
    monkeypatch.setattr(
        release_gate,
        "_git",
        _release_gate_git_responses(candidate, candidate, "tag", candidate),
    )

    release_gate.check_candidate_identity(
        release_gate.ReleaseMetadata("3.1.0-beta.2", "2026.7.4", {}, {}),
        candidate,
        "3.1.0-beta.2",
        "3.1.0-beta.2",
    )


def test_public_package_metadata_and_feedback_paths_use_the_fork_pr_flow() -> None:
    """Keep public maintenance paths on the maintained fork's executable flow."""
    manifest = json.loads(
        (ROOT / "custom_components/garmin_connect/manifest.json").read_text()
    )
    hacs = json.loads((ROOT / "hacs.json").read_text())
    public_documents = {
        path: path.read_text() for path in USER_VISIBLE_MAINTENANCE_DOCUMENTS
    }
    issue_forms = {
        path: yaml.safe_load(public_documents[path])
        for path in (
            ROOT / ".github" / "ISSUE_TEMPLATE" / "bug.yml",
            ROOT / ".github" / "ISSUE_TEMPLATE" / "feature_request.yml",
        )
    }
    pull_request_template = public_documents[
        ROOT / ".github" / "pull_request_template.md"
    ]
    feedback_guide = public_documents[ROOT / "docs" / "feedback" / "README.md"]
    security = public_documents[ROOT / "SECURITY.md"]

    assert hacs["render_readme"] is True
    assert manifest["codeowners"] == ["@Jasper-1024"]
    assert manifest["documentation"] == FORK_URL
    assert manifest["issue_tracker"] == PUBLIC_FEEDBACK_URL

    for path, text in public_documents.items():
        assert FORK_ISSUES_URL not in text, path
        assert FORK_DISCUSSIONS_URL not in text, path
        assert not any(url in text for url in LEGACY_MAINTENANCE_URLS), path

    readme = public_documents[ROOT / "README.md"]
    release = public_documents[ROOT / "docs" / "release-3.1.0-beta.2.md"]
    assert FORK_URL in readme
    assert UPSTREAM_URL in readme
    assert "https://github.com/cyberjunky/ha-garmin" in readme
    assert "owner=Jasper-1024&repository=home-assistant-garmin_connect" in readme
    assert DEFAULT_POLLING_PROMISE in readme
    assert "Prospective Archive is **off by default**" in readme
    assert "no archive-deletion action" in readme
    assert "no built-in retention period" in readme
    normalized_readme = " ".join(readme.split())
    assert "GitHub Issues and Discussions are disabled on this fork" in normalized_readme
    assert "Plane is not a public support endpoint" in normalized_readme
    normalized_release = " ".join(release.split())
    assert FORK_URL in release
    assert "GitHub Issues and Discussions are disabled on this fork" in normalized_release
    assert "Current entity polling defaults to **900 seconds (15 minutes)**" in normalized_release
    assert "Plane only for internal triage and archival" in normalized_release
    assert "no built-in retention period" in normalized_release

    feedback_paths = (readme, release, feedback_guide, pull_request_template)
    for text in feedback_paths:
        normalized_text = " ".join(text.split())
        assert PUBLIC_FEEDBACK_URL in _markdown_link_urls(text)
        assert "fork" in text.lower()
        assert "branch" in text.lower()
        assert "docs/feedback/<topic>.md" in text
        assert "passwords, tokens" in normalized_text
        assert "raw health data" in normalized_text
    assert "An unchanged fork branch cannot produce a pull request" in feedback_guide
    assert (
        "This PR adds the required redacted `docs/feedback/<topic>.md` file."
        in pull_request_template
    )

    for path, form in issue_forms.items():
        assert isinstance(form, dict), path
        assert isinstance(form.get("name"), str), path
        assert isinstance(form.get("description"), str), path
        assert isinstance(form.get("body"), list), path
        form_text = "\n".join(_yaml_strings(form))
        assert PUBLIC_FEEDBACK_URL in _markdown_link_urls(form_text), path
        assert "This form cannot be submitted because GitHub Issues are disabled" in form_text, path
        assert "docs/feedback/<topic>.md" in form_text, path
        assert "passwords, tokens, or raw health data" in form_text, path

    assert "## Public feedback" in pull_request_template
    assert "## Fork version" in pull_request_template
    assert "GitHub Issues and Discussions are disabled on this fork" in pull_request_template
    assert FORK_SECURITY_ADVISORIES_URL in _markdown_link_urls(security)
    assert PUBLIC_FEEDBACK_URL not in _markdown_link_urls(security)
    assert UPSTREAM_URL in security
    assert "https://github.com/cyberjunky/ha-garmin" in security
    integration_docs = public_documents[ROOT / "docs" / "garmin_connect.markdown"]
    assert "@cyberjunky" not in integration_docs
    assert "@Jasper-1024" in integration_docs


def test_public_polling_docs_match_the_runtime_default() -> None:
    """Require every public polling-default claim to use the runtime promise."""
    assert DEFAULT_SCAN_INTERVAL == 900
    integration_docs = ROOT / "docs" / "garmin_connect.markdown"
    assert (
        "Data is polled from Garmin Connect every 900 seconds (15 minutes) by default."
        in integration_docs.read_text()
    )

    claims = [
        (path, claim)
        for path in USER_VISIBLE_POLLING_DOCUMENTS
        for text in (path.read_text(),)
        for claim in _polling_default_claims(text)
    ]
    assert claims
    assert all(
        DEFAULT_POLLING_PROMISE in " ".join(claim.split()) for _, claim in claims
    ), claims


@pytest.mark.parametrize(
    ("text", "expected"),
    (
        (
            "The default scan interval is 900 seconds (15 minutes).",
            ["The default scan interval is 900 seconds (15 minutes)."],
        ),
        (
            "| Option | Default | Description |\n"
            "| --- | --- | --- |\n"
            "| Scan interval | 900 seconds (15 minutes) | Poll Garmin Connect |",
            [
                "Option Default Description Scan interval "
                "900 seconds (15 minutes) Poll Garmin Connect"
            ],
        ),
        (
            "Research data has 5-minute granularity. "
            "The integration polls independently.",
            [],
        ),
        (
            "Polling defaults to 5 minutes.",
            ["Polling defaults to 5 minutes."],
        ),
    ),
)
def test_polling_default_claim_detection_is_local_to_the_claim(
    text: str, expected: list[str]
) -> None:
    """Do not confuse unrelated data granularity with a polling default."""
    assert _polling_default_claims(text) == expected


def test_release_guidance_is_prospective_and_reversible() -> None:
    """Operator guidance must not revive the fixed-date backfill rollout."""
    guidance = (ROOT / "docs" / "release-3.1.0-beta.2.md").read_text()
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
    assert "Recorder capability contract" in guidance
    downgrade = guidance.split("## Downgrade and re-authentication safety", 1)[1]
    assert "3.0.14" in downgrade
    assert "backup" in downgrade.lower()
    assert "same\nGarmin account" in downgrade
    assert "configured Garmin profile" in downgrade
    assert "different Garmin account" in downgrade
    assert "original account" in downgrade
    assert "fails closed" in downgrade


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
    core = MagicMock()
    core.client = MagicMock()

    async def async_request(_priority: object, requester: object) -> object:
        return await requester()

    core.async_request = async_request
    entry = MagicMock(data={"history_account_key": "opaque-account-key"})
    entry.state = ConfigEntryState.LOADED
    entry.runtime_data = GarminConnectCoordinators(
        core=core,
        activity=None,
        training=None,
        body=None,
        goals=None,
        gear=None,
        blood_pressure=None,
        menstrual=None,
        nutrition=None,
        history_archive=archive,
    )
    hass = MagicMock()
    hass.config_entries.async_entries.return_value = [entry]
    await async_setup_services(hass)
    handler = next(call[0][2] for call in hass.services.async_register.call_args_list if call[0][1] == "sync_history")
    call = MagicMock(data={"date": date(2026, 7, 24)})
    archive.async_sync_range = AsyncMock(return_value=report)
    action_response = await handler(call)
    diagnostics = await async_get_config_entry_diagnostics(MagicMock(), entry)
    _assert_private_snapshot({"diagnostics": diagnostics, "status": status, "action": action_response, "calendars": calendars})
