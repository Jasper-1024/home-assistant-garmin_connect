"""Public-seam tests for the durable prospective FIT queue."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.garmin_connect import history as history_module
from custom_components.garmin_connect.const import CONF_ARCHIVE_ENABLED
from custom_components.garmin_connect.fit_archive import FitArchiveError, fit_file_name
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_recorder import RecorderWriteOutcome
from custom_components.garmin_connect.history_source import normalize_activities


class _Store:
    def __init__(self, data: dict | None = None) -> None:
        self.data = data

    async def async_load(self) -> dict | None:
        return self.data

    async def async_save(self, data: dict) -> None:
        self.data = data


def _archive(
    source: object,
    client: object,
    store: _Store,
    *,
    clock=None,
    account_key: str = "opaque-account-key-1234567890",
    stores: dict | None = None,
    request_gate=None,
    archive_enabled: bool = False,
) -> GarminHistoryArchive:
    data = {"history_account_key": account_key}
    if archive_enabled:
        data.update(
            {
                "archive_activation_date": "2026-08-01",
                "archive_last_enabled": True,
            }
        )
    entry = MagicMock(
        data=data,
        entry_id="entry-1",
        options={CONF_ARCHIVE_ENABLED: archive_enabled},
    )
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=client), request_gate=request_gate
    )
    hass = MagicMock()
    hass.config.path.return_value = "/tmp/garmin-connect-fit-queue"
    stores = {} if stores is None else stores
    stores.setdefault("garmin_connect.entry-1.history_catalog", store)
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=SimpleNamespace(
            async_check=AsyncMock(
                return_value=RecorderCompatibilityResult.compatible_result()
            )
        ),
        store_factory=lambda _hass, _version, path, **_kwargs: stores.setdefault(
            path, _Store()
        ),
        source_factory=lambda *_args: source,
        recorder_factory=lambda: recorder,
        clock=clock,
    )
    return archive


def _fit_summary() -> dict:
    return {
        "message_counts": {"record": 1},
        "message_fields": {"record": ["timestamp"]},
        "time_coverage": {"start": None, "end": None},
        "presence": {
            "heart_rate": False,
            "temperature": False,
            "gps": False,
            "cadence": False,
            "speed": False,
            "power": False,
            "training_effect": False,
            "training_load": False,
            "recovery_time": False,
            "recovery": False,
        },
    }


def _activity():
    return normalize_activities(
        [
            {
                "activityId": 123,
                "activityType": "running",
                "startTime": "2026-08-01T10:00:00Z",
                "durationInSeconds": 60,
            }
        ],
        date(2026, 8, 1),
    )[0]


def _activity_record(activity) -> dict:
    return {
        "logical_id": activity.logical_id,
        "activity_id": activity.activity_id,
        "revision": activity.revision,
        "calendar_date": activity.calendar_date.isoformat(),
        "activity_type": activity.activity_type,
        "name": activity.name,
        "start": activity.start.isoformat(),
        "end": activity.end.isoformat() if activity.end else None,
        "duration_seconds": activity.duration_seconds,
        "training_effect": activity.training_effect,
        "load": activity.load,
        "recovery": activity.recovery,
    }


@pytest.mark.asyncio
async def test_activity_discovery_deduplicates_durable_fit_work_without_download():
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    archive = _archive(Source(), client, store)
    await archive.async_start()

    first = await archive.async_sync_range(activity.calendar_date, activity.calendar_date)
    second = await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    assert first.outcome == "written"
    assert second.outcome == "written"
    assert first.fit_count == 0
    assert second.fit_count == 0
    client.download_activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_shared_fit_is_copied_only_for_indexed_account_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    activity = _activity()
    account_key = "opaque-account-key-1234567890"
    fit_root = tmp_path / "fit"
    fit_root.mkdir(mode=0o700)
    legacy_path = fit_root / fit_file_name(activity.logical_id)
    legacy_path.write_bytes(b"legacy FIT")
    legacy_path.chmod(0o600)
    unowned_path = fit_root / fit_file_name("a" * 24)
    unowned_path.write_bytes(b"unowned legacy FIT")
    unowned_path.chmod(0o600)
    summary = _fit_summary()
    catalog = _Store(
        {
            "schema_version": 1,
            "account_key": account_key,
            "completed_dates": [],
            "sleep_index": {"2026": [activity.logical_id]},
            "event_index": {},
            "activity_index": {"2026": [activity.logical_id]},
            "fit_queue": [],
            "fit_queue_quarantine": [],
            "fit_queue_error": None,
            "fit_last_eligible_download": None,
        }
    )
    partition = _Store(
        {
            "schema_version": 1,
            "sleep_schema_version": 1,
            "account_key": account_key,
            "year": "2026",
            "sessions": {},
            "events": {},
            "activities": {activity.logical_id: _activity_record(activity)},
            "fits": {
                activity.logical_id: {
                    "logical_id": activity.logical_id,
                    "path": legacy_path.name,
                    "summary": summary,
                }
            },
        }
    )
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        "garmin_connect.entry-1.sleep_2026": partition,
    }
    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {
            **summary,
            "file": {"integrity_ok": True, "decode_ok": True},
        },
    )
    archive = _archive(
        MagicMock(),
        MagicMock(),
        catalog,
        account_key=account_key,
        stores=stores,
    )
    archive._hass.config.path.return_value = str(fit_root)

    await archive.async_start()

    migrated_path = fit_root / account_key / legacy_path.name
    assert migrated_path.is_file()
    assert migrated_path.stat().st_mode & 0o777 == 0o600
    assert legacy_path.read_bytes() == b"legacy FIT"
    assert unowned_path.is_file()
    events = await archive.async_get_calendar_events(
        "activity", activity.calendar_date, activity.calendar_date
    )
    assert len(events) == 1


@pytest.mark.asyncio
async def test_corrupt_legacy_fit_copy_is_removed_and_retried_without_touching_valid_fit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    corrupt_activity = _activity()
    valid_activity = normalize_activities(
        [
            {
                "activityId": 456,
                "activityType": "cycling",
                "startTime": "2026-08-01T11:00:00Z",
                "durationInSeconds": 120,
            }
        ],
        date(2026, 8, 1),
    )[0]
    account_key = "opaque-account-key-1234567890"
    fit_root = tmp_path / "fit"
    fit_root.mkdir(mode=0o700)
    corrupt_legacy_path = fit_root / fit_file_name(corrupt_activity.logical_id)
    corrupt_legacy_path.write_bytes(b"corrupt legacy FIT")
    corrupt_legacy_path.chmod(0o600)
    valid_path = fit_root / account_key / fit_file_name(valid_activity.logical_id)
    valid_path.parent.mkdir(mode=0o700)
    valid_path.write_bytes(b"valid account FIT")
    valid_path.chmod(0o600)
    summary = _fit_summary()
    catalog = _Store(
        {
            "schema_version": 1,
            "account_key": account_key,
            "completed_dates": [],
            "sleep_index": {
                "2026": [corrupt_activity.logical_id, valid_activity.logical_id]
            },
            "event_index": {},
            "activity_index": {
                "2026": [corrupt_activity.logical_id, valid_activity.logical_id]
            },
            "fit_queue": [],
            "fit_queue_quarantine": [],
            "fit_queue_error": None,
            "fit_last_eligible_download": None,
        }
    )
    partition = _Store(
        {
            "schema_version": 1,
            "sleep_schema_version": 1,
            "account_key": account_key,
            "year": "2026",
            "sessions": {},
            "events": {},
            "activities": {
                corrupt_activity.logical_id: _activity_record(corrupt_activity),
                valid_activity.logical_id: _activity_record(valid_activity),
            },
            "fits": {
                corrupt_activity.logical_id: {
                    "logical_id": corrupt_activity.logical_id,
                    "path": corrupt_legacy_path.name,
                    "summary": summary,
                },
                valid_activity.logical_id: {
                    "logical_id": valid_activity.logical_id,
                    "path": valid_path.name,
                    "summary": summary,
                },
            },
        }
    )
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        "garmin_connect.entry-1.sleep_2026": partition,
    }

    def inspect(path: Path, _mode: int) -> dict:
        if path.read_bytes() == b"corrupt legacy FIT":
            raise FitArchiveError("FIT decode failed")
        return {**summary, "file": {"integrity_ok": True, "decode_ok": True}}

    monkeypatch.setattr(history_module, "inspect_fit", inspect)

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (
                (corrupt_activity, valid_activity)
                if metric == "timed_activities"
                else ()
            )

    client = MagicMock()
    client.download_activity = AsyncMock(return_value=b"replacement FIT")
    archive = _archive(
        Source(),
        client,
        catalog,
        account_key=account_key,
        stores=stores,
        clock=lambda: datetime(2026, 8, 1, 13, tzinfo=UTC),
    )
    archive._hass.config.path.return_value = str(fit_root)

    await archive.async_start()

    corrupt_account_path = fit_root / account_key / corrupt_legacy_path.name
    assert not corrupt_account_path.exists()
    assert valid_path.read_bytes() == b"valid account FIT"

    await archive.async_sync_range(date(2026, 8, 1), date(2026, 8, 1), fit_limit=1)

    client.download_activity.assert_awaited_once_with(int(corrupt_activity.activity_id), "fit")
    assert corrupt_account_path.read_bytes() == b"replacement FIT"
    assert valid_path.read_bytes() == b"valid account FIT"
    assert corrupt_legacy_path.read_bytes() == b"corrupt legacy FIT"


@pytest.mark.asyncio
async def test_legacy_shared_fit_is_copied_for_duplicate_accounts_without_stealing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    activity = _activity()
    fit_root = tmp_path / "fit"
    fit_root.mkdir(mode=0o700)
    legacy_path = fit_root / fit_file_name(activity.logical_id)
    legacy_path.write_bytes(b"shared legacy FIT")
    legacy_path.chmod(0o600)
    summary = _fit_summary()

    def stores_for(account_key: str) -> dict[str, _Store]:
        catalog = _Store(
            {
                "schema_version": 1,
                "account_key": account_key,
                "completed_dates": [],
                "sleep_index": {"2026": [activity.logical_id]},
                "event_index": {},
                "activity_index": {"2026": [activity.logical_id]},
                "fit_queue": [],
                "fit_queue_quarantine": [],
                "fit_queue_error": None,
                "fit_last_eligible_download": None,
            }
        )
        partition = _Store(
            {
                "schema_version": 1,
                "sleep_schema_version": 1,
                "account_key": account_key,
                "year": "2026",
                "sessions": {},
                "events": {},
                "activities": {activity.logical_id: _activity_record(activity)},
                "fits": {
                    activity.logical_id: {
                        "logical_id": activity.logical_id,
                        "path": legacy_path.name,
                        "summary": summary,
                    }
                },
            }
        )
        return {
            "garmin_connect.entry-1.history_catalog": catalog,
            "garmin_connect.entry-1.sleep_2026": partition,
        }

    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {
            **summary,
            "file": {"integrity_ok": True, "decode_ok": True},
        },
    )
    account_a = "account-a-opaque-key-1234567890"
    account_b = "account-b-opaque-key-1234567890"
    stores_a = stores_for(account_a)
    stores_b = stores_for(account_b)
    archive_a = _archive(
        MagicMock(), MagicMock(), stores_a[next(iter(stores_a))],
        account_key=account_a, stores=stores_a,
    )
    archive_b = _archive(
        MagicMock(), MagicMock(), stores_b[next(iter(stores_b))],
        account_key=account_b, stores=stores_b,
    )
    for archive in (archive_a, archive_b):
        archive._hass.config.path.return_value = str(fit_root)
        await archive.async_start()

    assert (fit_root / account_a / legacy_path.name).read_bytes() == b"shared legacy FIT"
    assert (fit_root / account_b / legacy_path.name).read_bytes() == b"shared legacy FIT"
    assert legacy_path.read_bytes() == b"shared legacy FIT"


@pytest.mark.asyncio
async def test_pending_fit_follows_cross_year_source_calendar_correction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    first_activity = normalize_activities(
        [
            {
                "activityId": 123,
                "activityType": "running",
                "startTime": "2026-12-31T23:00:00Z",
                "startTimeLocal": "2026-12-31T23:00:00+00:00",
                "durationInSeconds": 60,
            }
        ],
        date(2026, 12, 31),
    )[0]
    corrected_activity = normalize_activities(
        [
            {
                "activityId": 123,
                "activityType": "running",
                "startTime": "2026-12-31T23:00:00Z",
                "startTimeLocal": "2027-01-01T00:00:00+01:00",
                "durationInSeconds": 60,
            }
        ],
        date(2027, 1, 1),
    )[0]
    assert first_activity.logical_id == corrected_activity.logical_id
    assert first_activity.calendar_date != corrected_activity.calendar_date

    class Source:
        current = first_activity

        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (self.current,) if metric == "timed_activities" else ()

    async def archive_fit(**kwargs):
        if len(attempted_paths) == 0:
            attempted_paths.append(Path(kwargs["directory"]))
            raise FitArchiveError("temporary FIT failure")
        attempted_paths.append(Path(kwargs["directory"]))
        directory = Path(kwargs["directory"])
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / fit_file_name(kwargs["logical_id"])
        path.write_bytes(b"corrected FIT")
        path.chmod(0o600)
        return {
            "logical_id": kwargs["logical_id"],
            "path": path.name,
            "summary": _fit_summary(),
        }

    attempted_paths: list[Path] = []
    monkeypatch.setattr(history_module, "async_archive_fit", archive_fit)
    now = [datetime(2026, 12, 31, 23, 30, tzinfo=UTC)]
    store = _Store()
    archive = _archive(
        Source(),
        MagicMock(),
        store,
        clock=lambda: now[0],
    )
    archive._hass.config.path.return_value = str(tmp_path / "fit")
    await archive.async_start()

    first_report = await archive.async_sync_range(
        first_activity.calendar_date, first_activity.calendar_date
    )
    assert first_report.fit_count == 0

    Source.current = corrected_activity
    now[0] += timedelta(hours=1)
    corrected_report = await archive.async_sync_range(
        corrected_activity.calendar_date, corrected_activity.calendar_date
    )

    assert corrected_report.fit_count == 1
    assert attempted_paths == [
        tmp_path / "fit" / "opaque-account-key-1234567890",
        tmp_path / "fit" / "opaque-account-key-1234567890",
    ]
    fit_path = (
        tmp_path
        / "fit"
        / "opaque-account-key-1234567890"
        / fit_file_name(corrected_activity.logical_id)
    )
    assert fit_path.is_file()
    assert len(
        await archive.async_get_calendar_events(
            "activity", corrected_activity.calendar_date, corrected_activity.calendar_date
        )
    ) == 1


@pytest.mark.asyncio
async def test_malformed_fit_queue_is_quarantined_without_touching_activity_catalog():
    activity = _activity()
    activity_record = {
        "logical_id": activity.logical_id,
        "activity_id": activity.activity_id,
        "revision": activity.revision,
        "calendar_date": activity.calendar_date.isoformat(),
        "activity_type": activity.activity_type,
        "name": activity.name,
        "start": activity.start.isoformat(),
        "end": None,
        "duration_seconds": activity.duration_seconds,
        "training_effect": activity.training_effect,
        "load": activity.load,
        "recovery": activity.recovery,
    }
    account_key = "opaque-account-key-1234567890"
    queue_item = {
        "logical_id": activity.logical_id,
        "activity_id": activity.activity_id,
        "year": "2026",
        "calendar_date": "2026-08-01",
    }
    store = _Store(
        {
            "schema_version": 1,
            "account_key": account_key,
            "completed_dates": [],
            "presence": {"2026-08-01": {"heart_rate": "present"}},
            "sleep_index": {},
            "event_index": {},
            "activity_index": {"2026": [activity.logical_id]},
            "fit_queue": [
                queue_item,
                dict(queue_item),
                {"logical_id": "not-an-id"},
                {
                    **queue_item,
                    "logical_id": "f" * 24,
                    "activity_id": "not-numeric",
                },
            ],
            "fit_last_eligible_download": None,
        }
    )
    partition = _Store(
        {
            "schema_version": 1,
            "sleep_schema_version": 1,
            "account_key": account_key,
            "year": "2026",
            "sessions": {},
            "events": {},
            "activities": {activity.logical_id: activity_record},
            "fits": {},
        }
    )
    stores = {
        "garmin_connect.entry-1.history_catalog": store,
        "garmin_connect.entry-1.sleep_2026": partition,
    }
    archive = _archive(
        MagicMock(),
        MagicMock(),
        store,
        account_key=account_key,
        stores=stores,
    )

    await archive.async_start()

    assert archive.status.state.value == "disabled"
    assert store.data["activity_index"] == {"2026": [activity.logical_id]}
    events = await archive.async_get_calendar_events(
        "activity", activity.calendar_date, activity.calendar_date
    )
    assert len(events) == 1
    restarted = _archive(
        MagicMock(),
        MagicMock(),
        store,
        account_key=account_key,
        stores=stores,
    )
    await restarted.async_start()
    events_after_restart = await restarted.async_get_calendar_events(
        "activity", activity.calendar_date, activity.calendar_date
    )
    assert len(events_after_restart) == 1


@pytest.mark.asyncio
async def test_fit_queue_paces_downloads_and_recovers_pending_work_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    activities = normalize_activities(
        [
            {
                "activityId": 123,
                "activityType": "running",
                "startTime": "2026-08-01T10:00:00Z",
                "durationInSeconds": 60,
            },
            {
                "activityId": 124,
                "activityType": "cycling",
                "startTime": "2026-08-01T12:00:00Z",
                "durationInSeconds": 60,
            },
        ],
        date(2026, 8, 1),
    )

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return activities if metric == "timed_activities" else ()

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    now = [datetime(2026, 8, 1, 13, tzinfo=UTC)]
    async def archive_one(**kwargs):
        directory = Path(kwargs["directory"])
        directory.mkdir(parents=True, exist_ok=True)
        fit_path = directory / fit_file_name(kwargs["logical_id"])
        fit_path.write_bytes(b"fit")
        fit_path.chmod(0o600)
        return {
            "logical_id": kwargs["logical_id"],
            "path": fit_file_name(kwargs["logical_id"]),
            "summary": _fit_summary(),
        }

    archive_fit = AsyncMock(side_effect=archive_one)
    monkeypatch.setattr(history_module, "async_archive_fit", archive_fit)
    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {
            **_fit_summary(),
            "file": {"integrity_ok": True, "decode_ok": True},
        },
    )
    stores = {}
    archive = _archive(Source(), client, store, clock=lambda: now[0], stores=stores)
    archive._hass.config.path.return_value = str(tmp_path / "fit")
    await archive.async_start()

    first_report = await archive.async_sync_range(
        date(2026, 8, 1), date(2026, 8, 1)
    )
    assert archive_fit.await_count == 1
    assert first_report.fit_count == 1
    assert len(tuple((tmp_path / "fit" / "opaque-account-key-1234567890").glob("*.fit"))) == 1

    second_report = await archive.async_sync_range(
        date(2026, 8, 1), date(2026, 8, 1)
    )
    assert archive_fit.await_count == 1
    assert second_report.fit_count == 0

    restarted = _archive(Source(), client, store, clock=lambda: now[0], stores=stores)
    restarted._hass.config.path.return_value = str(tmp_path / "fit")
    await restarted.async_start()
    now[0] += timedelta(hours=1)
    restarted_report = await restarted.async_sync_range(
        date(2026, 8, 1), date(2026, 8, 1)
    )

    assert archive_fit.await_count == 2
    assert restarted_report.fit_count == 1
    assert len(tuple((tmp_path / "fit" / "opaque-account-key-1234567890").glob("*.fit"))) == 2


@pytest.mark.asyncio
async def test_fit_backlog_over_256_survives_restart_and_is_fully_consumed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    activity_count = 260
    first_activities = normalize_activities(
        [
            {
                "activityId": 2000 + index,
                "activityType": "running",
                "startTime": "2026-08-01T10:00:00Z",
                "durationInSeconds": 60,
            }
            for index in range(256)
        ],
        date(2026, 8, 1),
    )
    later_activities = normalize_activities(
        [
            {
                "activityId": 3000 + index,
                "activityType": "running",
                "startTime": "2026-08-02T10:00:00Z",
                "durationInSeconds": 60,
            }
            for index in range(4)
        ],
        date(2026, 8, 2),
    )
    activities_by_date = {
        date(2026, 8, 1): first_activities,
        date(2026, 8, 2): later_activities,
    }

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return activities_by_date.get(_target, ()) if metric == "timed_activities" else ()

    summary = _fit_summary()
    client = MagicMock()
    client.download_activity = AsyncMock(return_value=b"fit")
    archive_fit = AsyncMock()

    async def archive_one(**kwargs):
        directory = Path(kwargs["directory"])
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / fit_file_name(kwargs["logical_id"])
        path.write_bytes(b"fit")
        path.chmod(0o600)
        return {
            "logical_id": kwargs["logical_id"],
            "path": path.name,
            "summary": summary,
        }

    archive_fit.side_effect = archive_one
    monkeypatch.setattr(history_module, "async_archive_fit", archive_fit)
    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {
            **summary,
            "file": {"integrity_ok": True, "decode_ok": True},
        },
    )
    now = [datetime(2026, 8, 1, 13, tzinfo=UTC)]
    store = _Store()
    stores = {}
    archive = _archive(Source(), client, store, clock=lambda: now[0], stores=stores)
    archive._hass.config.path.return_value = str(tmp_path / "fit")
    await archive.async_start()

    first_report = await archive.async_sync_range(
        date(2026, 8, 1), date(2026, 8, 1), fit_limit=1
    )
    assert first_report.fit_count == 1
    second_report = await archive.async_sync_range(
        date(2026, 8, 2), date(2026, 8, 2), fit_limit=0
    )
    assert second_report.fit_count == 0
    assert len(tuple((tmp_path / "fit" / "opaque-account-key-1234567890").glob("*.fit"))) == 1

    restarted = _archive(
        Source(), client, store, clock=lambda: now[0], stores=stores
    )
    restarted._hass.config.path.return_value = str(tmp_path / "fit")
    await restarted.async_start()

    for _ in range(activity_count - 1):
        now[0] += timedelta(hours=1)
        report = await restarted.async_sync_range(
            date(2026, 8, 1), date(2026, 8, 1), fit_limit=1
        )
        assert report.fit_count == 1

    assert archive_fit.await_count == activity_count
    assert len(tuple((tmp_path / "fit" / "opaque-account-key-1234567890").glob("*.fit"))) == activity_count


@pytest.mark.asyncio
async def test_fit_failure_keeps_structured_activity_and_pending_work(
    monkeypatch: pytest.MonkeyPatch,
):
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    monkeypatch.setattr(
        history_module,
        "async_archive_fit",
        AsyncMock(side_effect=FitArchiveError("malformed FIT")),
    )
    archive = _archive(
        Source(),
        client,
        store,
        clock=lambda: datetime(2026, 8, 1, 13, tzinfo=UTC),
    )
    await archive.async_start()

    report = await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    assert report.outcome == "written", report
    assert archive.status.state.value == "disabled"
    assert report.fit_count == 0
    events = await archive.async_get_calendar_events(
        "activity", activity.calendar_date, activity.calendar_date
    )
    assert len(events) == 1


@pytest.mark.asyncio
async def test_valid_local_fit_completes_queue_without_download(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    account_key = "opaque-account-key-1234567890"
    hass_path = tmp_path / "fit"
    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {**_fit_summary(), "file": {"integrity_ok": True, "decode_ok": True}},
    )
    archive = _archive(
        Source(),
        client,
        store,
        clock=lambda: datetime(2026, 8, 1, 13, tzinfo=UTC),
        account_key=account_key,
    )
    archive._hass.config.path.return_value = str(hass_path)
    fit_path = hass_path / account_key / fit_file_name(activity.logical_id)
    fit_path.parent.mkdir(parents=True)
    fit_path.write_bytes(b"existing FIT")
    fit_path.chmod(0o600)
    await archive.async_start()

    await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    client.download_activity.assert_not_awaited()
    events = await archive.async_get_calendar_events(
        "activity", activity.calendar_date, activity.calendar_date
    )
    assert len(events) == 1


@pytest.mark.asyncio
async def test_local_fit_symlink_is_not_accepted_as_account_owned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    account_key = "opaque-account-key-1234567890"
    fit_root = tmp_path / "fit"
    outside_account = tmp_path / "other-account"
    outside_account.mkdir(mode=0o700)
    outside_fit = outside_account / fit_file_name(activity.logical_id)
    outside_fit.write_bytes(b"other account FIT")
    outside_fit.chmod(0o600)
    fit_root.mkdir(mode=0o700)
    (fit_root / account_key).symlink_to(outside_account, target_is_directory=True)

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {
            **_fit_summary(),
            "file": {"integrity_ok": True, "decode_ok": True},
        },
    )
    archive = _archive(Source(), client, store, account_key=account_key)
    archive._hass.config.path.return_value = str(fit_root)
    await archive.async_start()

    report = await archive.async_sync_range(
        activity.calendar_date, activity.calendar_date
    )

    assert report.outcome == "written"
    client.download_activity.assert_not_awaited()
    events = await archive.async_get_calendar_events(
        "activity", activity.calendar_date, activity.calendar_date
    )
    assert len(events) == 1
    assert (fit_root / account_key).is_symlink()


@pytest.mark.asyncio
async def test_manual_repair_processes_fit_when_archive_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
):
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    async def archive_one(**kwargs):
        directory = Path(kwargs["directory"])
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / fit_file_name(kwargs["logical_id"])
        path.write_bytes(b"manual FIT")
        path.chmod(0o600)
        return {
            "logical_id": kwargs["logical_id"],
            "path": path.name,
            "summary": _fit_summary(),
        }

    archive_fit = AsyncMock(side_effect=archive_one)
    monkeypatch.setattr(history_module, "async_archive_fit", archive_fit)
    archive = _archive(Source(), client, store)
    await archive.async_start()

    report = await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    assert report.outcome == "written"
    assert report.fit_count == 1
    assert archive.archive_enabled is False
    assert (
        Path(archive._hass.config.path.return_value)
        / "opaque-account-key-1234567890"
        / fit_file_name(activity.logical_id)
    ).is_file()


@pytest.mark.asyncio
async def test_cancelled_fit_attempt_persists_pacing_and_pending_work(
    monkeypatch: pytest.MonkeyPatch,
):
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    client = MagicMock()
    client.download_activity = AsyncMock()
    store = _Store()
    monkeypatch.setattr(
        history_module,
        "async_archive_fit",
        AsyncMock(side_effect=asyncio.CancelledError),
    )
    now = [datetime(2026, 8, 1, 13, tzinfo=UTC)]
    archive = _archive(
        Source(),
        client,
        store,
        clock=lambda: now[0],
    )
    await archive.async_start()

    with pytest.raises(asyncio.CancelledError):
        await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    now[0] += timedelta(hours=1)
    monkeypatch.setattr(
        history_module,
        "async_archive_fit",
        AsyncMock(
            return_value={
                "logical_id": activity.logical_id,
                "path": fit_file_name(activity.logical_id),
                "summary": _fit_summary(),
            }
        ),
    )
    report = await archive.async_sync_range(
        activity.calendar_date, activity.calendar_date
    )
    assert report.fit_count == 1


@pytest.mark.asyncio
async def test_fit_queue_isolated_by_account_and_background_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    activity = _activity()

    class Source:
        async def async_fetch(self, _target, _metric):
            return ()

        async def async_fetch_details(self, _target, metric):
            return (activity,) if metric == "timed_activities" else ()

    class Gate:
        def __init__(self):
            self.priorities = []

        async def async_request(self, priority, request):
            self.priorities.append(priority)
            return await request()

    client = MagicMock()
    client.download_activity = AsyncMock(return_value=b"fit")
    monkeypatch.setattr(
        history_module,
        "inspect_fit",
        lambda _path, _mode: {
            **_fit_summary(),
            "file": {"integrity_ok": True, "decode_ok": True},
        },
    )
    gate_a = Gate()
    gate_b = Gate()
    store_a = _Store()
    store_b = _Store()
    archive_a = _archive(
        Source(),
        client,
        store_a,
        account_key="account-a-opaque-key-1234567890",
        request_gate=gate_a,
        clock=lambda: datetime(2026, 8, 1, 13, tzinfo=UTC),
    )
    archive_b = _archive(
        Source(),
        client,
        store_b,
        account_key="account-b-opaque-key-1234567890",
        request_gate=gate_b,
        clock=lambda: datetime(2026, 8, 1, 13, tzinfo=UTC),
    )
    for archive in (archive_a, archive_b):
        archive._hass.config.path.return_value = str(tmp_path / "fit")
        await archive.async_start()

    report_a = await archive_a.async_sync_range(
        activity.calendar_date, activity.calendar_date
    )
    report_b = await archive_b.async_sync_range(
        activity.calendar_date, activity.calendar_date
    )

    assert report_a.fit_count == 1
    assert report_b.fit_count == 1
    assert gate_a.priorities == [history_module.GarminRequestPriority.BACKGROUND]
    assert gate_b.priorities == [history_module.GarminRequestPriority.BACKGROUND]
    assert (tmp_path / "fit" / "account-a-opaque-key-1234567890").is_dir()
    assert (tmp_path / "fit" / "account-b-opaque-key-1234567890").is_dir()
