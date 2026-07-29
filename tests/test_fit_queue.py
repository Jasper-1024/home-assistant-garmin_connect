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
) -> GarminHistoryArchive:
    entry = MagicMock(
        data={"history_account_key": account_key},
        entry_id="entry-1",
        options={CONF_ARCHIVE_ENABLED: False},
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


def _enable_after_start(archive: GarminHistoryArchive) -> None:
    archive._archive_enabled = True


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
    client.download_activity.assert_not_awaited()
    assert store.data["fit_queue"] == [
        {
            "logical_id": activity.logical_id,
            "activity_id": activity.activity_id,
            "year": "2026",
            "calendar_date": "2026-08-01",
        }
    ]


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
        (directory / fit_file_name(kwargs["logical_id"])).write_bytes(b"fit")
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
    _enable_after_start(archive)

    await archive.async_sync_range(date(2026, 8, 1), date(2026, 8, 1))
    assert archive_fit.await_count == 1
    assert len(store.data["fit_queue"]) == 1
    assert store.data["fit_last_eligible_download"] == "2026-08-01T13:00:00+00:00"

    await archive.async_sync_range(date(2026, 8, 1), date(2026, 8, 1))
    assert archive_fit.await_count == 1

    restarted = _archive(Source(), client, store, clock=lambda: now[0], stores=stores)
    restarted._hass.config.path.return_value = str(tmp_path / "fit")
    await restarted.async_start()
    _enable_after_start(restarted)
    now[0] += timedelta(hours=1)
    await restarted.async_sync_range(date(2026, 8, 1), date(2026, 8, 1))

    assert archive_fit.await_count == 2
    assert store.data["fit_queue"] == []


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
    _enable_after_start(archive)

    report = await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    assert report.outcome == "written", report
    assert archive.status.state.value == "idle"
    assert store.data["fit_queue"]
    assert store.data["activity_index"]["2026"] == [activity.logical_id]


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
    _enable_after_start(archive)

    await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    client.download_activity.assert_not_awaited()
    assert store.data["fit_queue"] == []


@pytest.mark.asyncio
async def test_disabled_archive_preserves_pending_fit_without_processing(
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
    archive_fit = AsyncMock()
    monkeypatch.setattr(history_module, "async_archive_fit", archive_fit)
    archive = _archive(Source(), client, store)
    await archive.async_start()

    await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    archive_fit.assert_not_awaited()
    client.download_activity.assert_not_awaited()
    assert store.data["fit_queue"]


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
    archive = _archive(
        Source(),
        client,
        store,
        clock=lambda: datetime(2026, 8, 1, 13, tzinfo=UTC),
    )
    await archive.async_start()
    _enable_after_start(archive)

    with pytest.raises(asyncio.CancelledError):
        await archive.async_sync_range(activity.calendar_date, activity.calendar_date)

    assert store.data["fit_queue"]
    assert store.data["fit_last_eligible_download"] == "2026-08-01T13:00:00+00:00"


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
        _enable_after_start(archive)

    await archive_a.async_sync_range(activity.calendar_date, activity.calendar_date)
    await archive_b.async_sync_range(activity.calendar_date, activity.calendar_date)

    assert store_a.data["fit_queue"] == []
    assert store_b.data["fit_queue"] == []
    assert gate_a.priorities == [history_module.GarminRequestPriority.BACKGROUND]
    assert gate_b.priorities == [history_module.GarminRequestPriority.BACKGROUND]
    assert (tmp_path / "fit" / "account-a-opaque-key-1234567890").is_dir()
    assert (tmp_path / "fit" / "account-b-opaque-key-1234567890").is_dir()
