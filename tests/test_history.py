"""Tests for the Garmin history archive lifecycle seam."""

from __future__ import annotations

import asyncio
import json
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from ha_garmin.exceptions import (
    GarminAPIError,
    GarminAuthError,
    GarminConnectError,
    GarminRateLimitError,
)
from homeassistant import loader
from homeassistant.config_entries import ConfigEntries
from homeassistant.core import HomeAssistant
from homeassistant.helpers.recorder import async_initialize_recorder
from homeassistant.setup import async_setup_component

import custom_components.garmin_connect.const as const
from custom_components.garmin_connect import history_recorder as history_recorder_module
from custom_components.garmin_connect.const import (
    CONF_ARCHIVE_ACTIVATION_DATE,
    CONF_ARCHIVE_ENABLED,
    CONF_ARCHIVE_PREVIOUSLY_ENABLED,
)
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    HistorySyncReport,
    HomeAssistantRecorderCompatibility,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_recorder import (
    GarminHistoryRecorder,
    RecorderWriteOutcome,
    statistic_id_for,
)
from custom_components.garmin_connect.history_sensor import GarminHistoryStatusSensor
from custom_components.garmin_connect.history_source import (
    GarminHistorySource,
    NormalizedSample,
    SegmentedData,
    SnapshotData,
    SourceSeries,
    normalize_activities,
    normalize_health_events,
)
from custom_components.garmin_connect.request_gate import (
    GarminRequestGate,
    GarminRequestPriority,
)
from custom_components.garmin_connect.sleep_archive import (
    SleepSession,
    SleepStream,
    SleepStreamPoint,
    parse_sleep_sessions,
)


class FakeStore:
    """Minimal Store adapter for archive lifecycle tests."""

    def __init__(self, data: dict | None = None) -> None:
        self.data = data
        self.saved: list[dict] = []

    async def async_load(self) -> dict | None:
        return self.data

    async def async_save(self, data: dict) -> None:
        self.saved.append(data)
        self.data = data


class FakeRecorderChecker:
    """Recorder compatibility adapter with an observable call."""

    def __init__(self, result: RecorderCompatibilityResult) -> None:
        self.result = result
        self.check = AsyncMock(return_value=result)

    async def async_check(self) -> RecorderCompatibilityResult:
        return await self.check()


class DeterministicTimer:
    """Timer adapter whose wakeups are driven explicitly by a test."""

    def __init__(self) -> None:
        self.scheduled: list[list[object]] = []

    def call_later(self, delay: timedelta, callback: Callable[[], None]) -> Callable[[], None]:
        slot = [delay, callback, True]
        self.scheduled.append(slot)

        def cancel() -> None:
            slot[2] = False

        return cancel

    def fire_next(self) -> None:
        for slot in self.scheduled:
            if slot[2]:
                slot[2] = False
                cast(Callable[[], None], slot[1])()
                return
        raise AssertionError("no timer wakeup is scheduled")

    @property
    def active(self) -> list[list[object]]:
        return [slot for slot in self.scheduled if slot[2]]


def _entry(*, entry_id: str = "entry-1", data: dict | None = None) -> MagicMock:
    entry = MagicMock()
    entry.entry_id = entry_id
    entry.title = "Garmin account"
    entry.data = data or {}
    entry.options = {}
    return entry


def _hass() -> MagicMock:
    hass = MagicMock()
    hass.config_entries.async_update_entry = MagicMock()
    return hass


def _store_factory(store: FakeStore):
    def factory(*args, **kwargs) -> FakeStore:
        return store

    return factory


def _reconciliation_store(
    target: date,
    *,
    state: str = "open",
    has_records: bool = False,
    outcome: str = "empty",
) -> FakeStore:
    """Build the smallest durable catalog for automatic-date tests."""
    reconciliation = {
        "state": state,
        "fingerprint": None,
        "has_records": has_records,
    }
    reconciliation["outcome"] = outcome
    return FakeStore(
        {
            "schema_version": 1,
            "account_key": "opaque-account-key-1234567890",
            "completed_dates": [],
            "reconciliation": {
                target.isoformat(): reconciliation
            },
            "reconciliation_family_presence": {},
            "hrv_summaries": {},
            "numeric_source_date_index": [],
            "numeric_source_date_dates": {},
            "numeric_source_date_pending": {},
            "numeric_source_date_tombstones": {},
            "numeric_source_date_outbox": {},
            "numeric_source_date_confirmed": {},
            "presence": {},
            "sleep_index": {},
            "event_index": {},
            "activity_index": {},
        }
    )


class ReconciliationSource:
    """Deterministic source returning one mutable heart-rate family."""

    def __init__(self, values: dict[date, tuple[float, ...]]) -> None:
        self.values = values
        self.presence: dict[date, str] = {}
        self.requested: list[date] = []

    async def async_fetch_details(self, target: date, metric: str) -> object:
        self.requested.append(target)
        if metric in {
            "sleep_sessions",
            "health_events_daily",
            "health_events_body_battery",
            "timed_activities",
        }:
            return ()
        values = self.values.get(target, ()) if metric == "heart_rate" else ()
        samples = tuple(
            NormalizedSample(
                datetime(2026, 8, 1, index, tzinfo=UTC),
                target,
                f"{target.isoformat()}T{index:02d}:00:00Z",
                value,
            )
            for index, value in enumerate(values)
        )
        return SourceSeries(
            samples,
            self.presence.get(target, "present" if samples else "empty"),
        )


def _enabled_reconciliation_archive(
    store: FakeStore,
    source: ReconciliationSource,
    now: list[datetime],
    timer: DeterministicTimer,
    activation_date: str | None = "2026-08-01",
    previously_enabled: bool = True,
    recorder: Any | None = None,
) -> GarminHistoryArchive:
    """Create an enabled archive with deterministic automatic cycles."""
    hass = _hass()
    data = {
        "history_account_key": "opaque-account-key-1234567890",
        CONF_ARCHIVE_PREVIOUSLY_ENABLED: previously_enabled,
    }
    if activation_date is not None:
        data[CONF_ARCHIVE_ACTIVATION_DATE] = activation_date
    entry = _entry(data=data)
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    if recorder is None:
        recorder = MagicMock()
        recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    annual_stores: dict[str, FakeStore] = {}

    def store_factory(_hass: object, _version: int, path: str, **kwargs: object) -> FakeStore:
        if path.endswith(".history_catalog"):
            return store
        return annual_stores.setdefault(path, FakeStore())

    return GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(
            RecorderCompatibilityResult.compatible_result()
        ),
        store_factory=store_factory,
        source_factory=lambda *_args: source,
        recorder_factory=lambda: recorder,
        clock=lambda: now[0],
        timer_factory=timer.call_later,
    )


def _manual_repair_archive(
    store: FakeStore,
    source: ReconciliationSource,
    *,
    activation_date: str | None = None,
    previously_enabled: bool | None = None,
) -> GarminHistoryArchive:
    """Create a disabled archive for bounded Manual Repair tests."""
    data: dict[str, Any] = {
        "history_account_key": "opaque-account-key-1234567890",
    }
    if activation_date is not None:
        data[CONF_ARCHIVE_ACTIVATION_DATE] = activation_date
    if previously_enabled is not None:
        data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] = previously_enabled
    entry = _entry(data=data)
    entry.options = {CONF_ARCHIVE_ENABLED: False}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    annual_stores: dict[str, FakeStore] = {}

    def store_factory(_hass: object, _version: int, path: str, **kwargs: object) -> FakeStore:
        if path.endswith(".history_catalog"):
            return store
        return annual_stores.setdefault(path, FakeStore())

    return GarminHistoryArchive(
        _hass(),
        entry,
        recorder_checker=FakeRecorderChecker(
            RecorderCompatibilityResult.compatible_result()
        ),
        store_factory=store_factory,
        source_factory=lambda *_args: source,
        recorder_factory=lambda: recorder,
    )


async def _run_reconciliation_cycle(
    archive: GarminHistoryArchive,
    timer: DeterministicTimer,
    source: ReconciliationSource,
) -> None:
    """Fire one cadence and await its observable remote requests."""
    expected_requests = len(source.requested) + 19
    for _ in range(1000):
        if timer.active:
            break
        await asyncio.sleep(0)
    timer.fire_next()
    for _ in range(1000):
        if len(source.requested) >= expected_requests:
            return
        await asyncio.sleep(0)
    raise AssertionError("archive cycle did not issue its observable requests")


async def _wait_for_remote_requests(
    source: ReconciliationSource, expected_requests: int
) -> None:
    """Await first synchronization through the public source boundary."""
    for _ in range(1000):
        if len(source.requested) >= expected_requests:
            return
        await asyncio.sleep(0)
    raise AssertionError("archive synchronization did not issue observable requests")


async def _wait_for_archive_state(
    archive: GarminHistoryArchive, expected: HistoryArchiveState
) -> None:
    """Await a public archive status transition."""
    for _ in range(1000):
        if archive.status.state is expected:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"archive did not reach public state {expected.value}")


@pytest.mark.asyncio
async def test_reconciliation_requires_one_later_unchanged_confirmation() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({target: (72.0,)})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    assert source.requested.count(target) == 19

    await _run_reconciliation_cycle(archive, timer, source)

    assert source.requested.count(target) == 38
    await archive.async_stop()


@pytest.mark.asyncio
async def test_settled_date_stays_terminal_after_confirmation_restart_and_next_scheduler() -> None:
    """An unchanged confirmation settles a date permanently for automatic work."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({target: (72.0,)})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    await _run_reconciliation_cycle(archive, timer, source)

    settled_requests = source.requested.count(target)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == settled_requests
    await archive.async_stop()

    now[0] = datetime(2026, 8, 6, tzinfo=UTC)
    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(store, source, now, restarted_timer)
    await restarted.async_start()
    await _wait_for_remote_requests(source, len(source.requested) + 19)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    assert source.requested.count(target) == settled_requests
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_reconciliation_retries_empty_then_saves_delayed_and_changed_data() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({target: ()})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == 19

    source.values[target] = (70.0,)
    await _run_reconciliation_cycle(archive, timer, source)

    source.values[target] = (71.0,)
    await _run_reconciliation_cycle(archive, timer, source)

    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == 76
    await archive.async_stop()


@pytest.mark.asyncio
async def test_empty_archive_date_settles_as_continuity_gap_at_window_boundary() -> None:
    target = date(2026, 8, 1)
    store = _reconciliation_store(target)
    source = ReconciliationSource({})
    now = [datetime(2026, 8, 2, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == 19

    now[0] = datetime(2026, 8, 8, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)

    assert source.requested.count(target) == 19
    now[0] = datetime(2026, 8, 7, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == 19
    await archive.async_stop()


@pytest.mark.asyncio
async def test_missing_sleep_stream_keeps_reconciliation_date_open() -> None:
    """A sleep session without its raw stream family cannot settle a date."""
    target = date(2026, 8, 4)
    sleep_session = SleepSession(
        "sleep-id",
        "main_sleep",
        datetime(2026, 8, 4, 0, tzinfo=UTC),
        datetime(2026, 8, 4, 8, tzinfo=UTC),
        target,
        "sleep-revision",
        {},
        (),
        (),
        (),
    )

    class MissingSleepStreamSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "sleep_sessions" and target_date == target:
                return (sleep_session,)
            return await super().async_fetch_details(target_date, metric)

    source = MissingSleepStreamSource({})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        _reconciliation_store(target), source, now, timer
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    await _run_reconciliation_cycle(archive, timer, source)

    requests_before_window_end = source.requested.count(target)
    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)

    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)

    assert source.requested.count(target) > requests_before_window_end
    await archive.async_stop()


@pytest.mark.asyncio
async def test_successful_sleep_stream_can_settle_after_unchanged_confirmation() -> None:
    """A complete raw sleep-stream observation participates in settlement."""
    target = date(2026, 8, 4)
    sleep_session = SleepSession(
        "sleep-id",
        "main_sleep",
        datetime(2026, 8, 4, 0, tzinfo=UTC),
        datetime(2026, 8, 4, 8, tzinfo=UTC),
        target,
        "sleep-revision",
        {},
        (),
        (),
        (),
        streams=tuple(
            SleepStream(
                metric,
                (
                    SleepStreamPoint(
                        datetime(2026, 8, 4, 1, tzinfo=UTC),
                        "2026-08-04T01:00:00Z",
                        60.0,
                    ),
                ),
            )
            for metric in (
                "heart_rate",
                "hrv",
                "body_battery",
                "stress",
                "respiration",
                "spo2",
                "movement",
            )
        ),
    )

    class SuccessfulSleepStreamSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "sleep_sessions" and target_date == target:
                return (sleep_session,)
            return await super().async_fetch_details(target_date, metric)

    source = SuccessfulSleepStreamSource({})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        _reconciliation_store(target), source, now, timer
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    first_confirmation_requests = source.requested.count(target)

    await _run_reconciliation_cycle(archive, timer, source)
    settled_requests = source.requested.count(target)
    assert settled_requests > first_confirmation_requests

    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == settled_requests
    await archive.async_stop()


@pytest.mark.asyncio
async def test_partial_sleep_stream_stays_open_through_window() -> None:
    """A single sleep substream cannot settle the date as complete."""
    target = date(2026, 8, 4)
    sleep_session = SleepSession(
        "sleep-id",
        "main_sleep",
        datetime(2026, 8, 4, 0, tzinfo=UTC),
        datetime(2026, 8, 4, 8, tzinfo=UTC),
        target,
        "sleep-revision",
        {},
        (),
        (),
        (),
        streams=(
            SleepStream(
                "heart_rate",
                (
                    SleepStreamPoint(
                        datetime(2026, 8, 4, 1, tzinfo=UTC),
                        "2026-08-04T01:00:00Z",
                        60.0,
                    ),
                ),
            ),
        ),
    )

    class PartialSleepStreamSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "sleep_sessions" and target_date == target:
                return (sleep_session,)
            return await super().async_fetch_details(target_date, metric)

    source = PartialSleepStreamSource({})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        _reconciliation_store(target), source, now, timer
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    public_presence = archive.get_history_presence(target, target)[target.isoformat()]
    assert public_presence["sleep_stream:heart_rate"] == "present"
    assert public_presence["sleep_stream:hrv"] == "missing"

    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    requests_at_window_boundary = source.requested.count(target)
    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)

    assert source.requested.count(target) > requests_at_window_boundary
    await archive.async_stop()


@pytest.mark.asyncio
async def test_present_then_empty_observation_stays_open_and_records_current_presence() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({target: (72.0,)})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    source.values[target] = ()
    await _run_reconciliation_cycle(archive, timer, source)
    await _run_reconciliation_cycle(archive, timer, source)

    assert archive.get_history_presence(target, target)[target.isoformat()]["heart_rate"] == "empty"
    await archive.async_stop()


@pytest.mark.asyncio
async def test_present_then_missing_observation_stays_open_and_records_current_presence() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({target: (72.0,)})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    source.presence[target] = "missing"
    source.values[target] = ()
    await _run_reconciliation_cycle(archive, timer, source)
    await _run_reconciliation_cycle(archive, timer, source)

    assert archive.get_history_presence(target, target)[target.isoformat()]["heart_rate"] == "missing"
    await archive.async_stop()


@pytest.mark.asyncio
async def test_settled_date_is_local_first_and_survives_restart() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target, state="settled")
    source = ReconciliationSource({target: (72.0,)})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == 0
    await archive.async_stop()

    now[0] = datetime(2026, 8, 6, tzinfo=UTC)
    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(store, source, now, restarted_timer)
    await restarted.async_start()
    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    assert source.requested.count(target) == 0
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_reconciliation_respects_activation_boundary_and_current_rollover() -> None:
    before_activation = date(2026, 8, 1)
    store = _reconciliation_store(before_activation)
    source = ReconciliationSource({before_activation: (70.0,)})
    now = [datetime(2026, 8, 3, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        store, source, now, timer, activation_date="2026-08-02"
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(before_activation) == 0

    now[0] = datetime(2026, 8, 4, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    assert date(2026, 8, 3) in source.requested
    await archive.async_stop()


@pytest.mark.asyncio
async def test_partial_structured_failure_retains_observed_records_and_stays_open() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({})
    sleep_details = (
        SleepSession(
            "sleep-id",
            "main_sleep",
            datetime(2026, 8, 4, 0, tzinfo=UTC),
            datetime(2026, 8, 4, 8, tzinfo=UTC),
            target,
            "sleep-revision",
            {},
            (),
            (),
            (),
            streams=(
                SleepStream(
                    "heart_rate",
                    (
                        SleepStreamPoint(
                            datetime(2026, 8, 4, 1, tzinfo=UTC),
                            "2026-08-04T01:00:00Z",
                            60.0,
                        ),
                    ),
                ),
            ),
        ),
    )

    class PartialFailureSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "sleep_sessions" and target_date == target:
                return sleep_details
            return await super().async_fetch_details(target_date, metric)

    source = PartialFailureSource({})
    recorder = MagicMock()
    recorder.async_write = AsyncMock(
        side_effect=[RecorderWriteOutcome(0) for _ in range(45)]
        + [OSError("injected structured write failure")]
    )
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        store,
        source,
        [datetime(2026, 8, 5, tzinfo=UTC)],
        timer,
        recorder=recorder,
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    await _wait_for_archive_state(archive, HistoryArchiveState.FAILED)

    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.get_history_presence(target, target)[target.isoformat()][
        "sleep_stream:heart_rate"
    ] == "present"
    first_failed_target_request_count = source.requested.count(target)

    recorder.async_write.side_effect = [RecorderWriteOutcome(0) for _ in range(32)]
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) > first_failed_target_request_count
    await archive.async_stop()


@pytest.mark.asyncio
async def test_sleep_stream_failure_recovery_requires_unchanged_confirmation() -> None:
    """A recovered sleep stream needs one later unchanged observation."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    sleep_details = (
        SleepSession(
            "sleep-id",
            "main_sleep",
            datetime(2026, 8, 4, 0, tzinfo=UTC),
            datetime(2026, 8, 4, 8, tzinfo=UTC),
            target,
            "sleep-revision",
            {},
            (),
            (),
            (),
            streams=tuple(
                SleepStream(
                    metric,
                    (
                        SleepStreamPoint(
                            datetime(2026, 8, 4, 1, tzinfo=UTC),
                            "2026-08-04T01:00:00Z",
                            60.0,
                        ),
                    ),
                )
                for metric in (
                    "heart_rate",
                    "hrv",
                    "body_battery",
                    "stress",
                    "respiration",
                    "spo2",
                    "movement",
                )
            ),
        ),
    )

    class SleepFailureSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "sleep_sessions" and target_date == target:
                return sleep_details
            return await super().async_fetch_details(target_date, metric)

    source = SleepFailureSource({})
    failed = True

    async def write(statistic_id: str, metadata: Any, samples: object) -> RecorderWriteOutcome:
        nonlocal failed
        if failed and ":sleep_" in statistic_id:
            failed = False
            return RecorderWriteOutcome(0, "failed", "writer")
        return RecorderWriteOutcome(0)

    recorder = MagicMock()
    recorder.async_write = AsyncMock(side_effect=write)
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        store,
        source,
        [datetime(2026, 8, 5, tzinfo=UTC)],
        timer,
        recorder=recorder,
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    failed_requests = source.requested.count(target)

    await _run_reconciliation_cycle(archive, timer, source)
    recovered_requests = source.requested.count(target)
    assert recovered_requests > failed_requests

    await _run_reconciliation_cycle(archive, timer, source)
    confirmed_requests = source.requested.count(target)
    assert confirmed_requests > recovered_requests

    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) == confirmed_requests
    await archive.async_stop()


@pytest.mark.asyncio
async def test_sleep_stream_failure_survives_restart_and_settles_after_unchanged_confirmation() -> None:
    """A persisted sleep-stream failure remains Open through restart and recovery."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    sleep_details = (
        SleepSession(
            "sleep-id",
            "main_sleep",
            datetime(2026, 8, 4, 0, tzinfo=UTC),
            datetime(2026, 8, 4, 8, tzinfo=UTC),
            target,
            "sleep-revision",
            {},
            (),
            (),
            (),
            streams=tuple(
                SleepStream(
                    metric,
                    (
                        SleepStreamPoint(
                            datetime(2026, 8, 4, 1, tzinfo=UTC),
                            "2026-08-04T01:00:00Z",
                            60.0,
                        ),
                    ),
                )
                for metric in (
                    "heart_rate",
                    "hrv",
                    "body_battery",
                    "stress",
                    "respiration",
                    "spo2",
                    "movement",
                )
            ),
        ),
    )

    class SleepFailureSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "sleep_sessions" and target_date == target:
                return sleep_details
            return await super().async_fetch_details(target_date, metric)

    source = SleepFailureSource({})
    failed = True

    async def write(statistic_id: str, metadata: Any, samples: object) -> RecorderWriteOutcome:
        nonlocal failed
        if failed and ":sleep_" in statistic_id:
            failed = False
            return RecorderWriteOutcome(0, "failed", "writer")
        return RecorderWriteOutcome(0)

    recorder = MagicMock()
    recorder.async_write = AsyncMock(side_effect=write)
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        store, source, now, timer, recorder=recorder
    )

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    await _wait_for_archive_state(archive, HistoryArchiveState.FAILED)
    await archive.async_stop()

    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(
        store, source, now, restarted_timer, recorder=recorder
    )
    await restarted.async_start()
    assert restarted.status.state is HistoryArchiveState.IDLE
    await _wait_for_remote_requests(source, len(source.requested) + 19)

    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    requests_after_recovery = source.requested.count(target)

    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    requests_after_confirmation = source.requested.count(target)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    assert source.requested.count(target) == requests_after_confirmation
    assert requests_after_confirmation > requests_after_recovery
    await restarted.async_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_metric", "failure_kind"),
    (("heart_rate", "http"), ("health_events_daily", "schema"), ("body_battery", "family")),
)
async def test_reconciliation_failure_stays_open_at_window_boundary(
    failure_metric: str, failure_kind: str
) -> None:
    """A failed or unknown family cannot become an empty continuity gap."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)

    class FailureSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if target_date == target and metric == failure_metric:
                self.requested.append(target_date)
                if failure_kind == "http":
                    raise GarminConnectError("private endpoint failed")
                if failure_kind == "schema":
                    return {"unexpected": "shape"}
                raise ValueError("family conversion failed")
            return await super().async_fetch_details(target_date, metric)

    source = FailureSource({})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    now[0] = datetime(2026, 8, 11, tzinfo=UTC)
    await archive.async_stop()
    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(store, source, now, restarted_timer)
    await restarted.async_start()
    expected_requests = len(source.requested) + 19
    await _wait_for_remote_requests(source, expected_requests)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)

    requests_before_open_date_probe = source.requested.count(target)
    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    assert source.requested.count(target) > requests_before_open_date_probe
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_failed_then_empty_observation_remains_open_after_window_end() -> None:
    """A later empty response cannot erase an earlier failed observation."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)

    class FailureThenEmptySource(ReconciliationSource):
        def __init__(self) -> None:
            super().__init__({})
            self.fail_target = True

        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if self.fail_target and target_date == target and metric == "heart_rate":
                self.requested.append(target_date)
                raise GarminConnectError("temporary family failure")
            return await super().async_fetch_details(target_date, metric)

    source = FailureThenEmptySource()
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    failed_requests = source.requested.count(target)

    source.fail_target = False
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) > failed_requests

    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    requests_before_reopen_probe = source.requested.count(target)

    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) > requests_before_reopen_probe
    await archive.async_stop()


@pytest.mark.asyncio
async def test_partial_then_empty_observation_remains_open_after_window_end() -> None:
    """A later complete-empty response cannot erase prior partial evidence."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    source = ReconciliationSource({})
    source.presence[target] = "partial"
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    source.presence.pop(target)
    await _run_reconciliation_cycle(archive, timer, source)

    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    requests_before_reopen_probe = source.requested.count(target)

    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    assert source.requested.count(target) > requests_before_reopen_probe
    await archive.async_stop()


@pytest.mark.asyncio
async def test_incomplete_checkpoint_survives_restart_and_empty_window() -> None:
    """An incomplete checkpoint keeps an empty date Open across restart."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target, outcome="incomplete")
    source = ReconciliationSource({})
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await archive.async_stop()

    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(store, source, now, restarted_timer)
    await restarted.async_start()
    await _wait_for_remote_requests(source, len(source.requested) + 19)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)

    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    requests_before_reopen_probe = source.requested.count(target)

    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(restarted, restarted_timer, source)
    assert source.requested.count(target) > requests_before_reopen_probe
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_public_archive_failure_then_complete_records_then_unchanged_settles() -> None:
    """A recovered complete observation can reach terminal settlement."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)

    class FailureThenRecordsSource(ReconciliationSource):
        def __init__(self) -> None:
            super().__init__({target: (72.0,)})
            self.fail_target = True

        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if self.fail_target and target_date == target and metric == "heart_rate":
                self.requested.append(target_date)
                raise GarminConnectError("temporary family failure")
            return await super().async_fetch_details(target_date, metric)

    source = FailureThenRecordsSource()
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    failed_requests = source.requested.count(target)

    source.fail_target = False
    await _run_reconciliation_cycle(archive, timer, source)
    complete_requests = source.requested.count(target)
    assert complete_requests > failed_requests
    assert archive.get_history_presence(target, target)[target.isoformat()]["heart_rate"] == "present"

    await _run_reconciliation_cycle(archive, timer, source)
    settled_requests = source.requested.count(target)
    await _run_reconciliation_cycle(archive, timer, source)

    assert settled_requests > complete_requests
    assert source.requested.count(target) == settled_requests
    await archive.async_stop()


@pytest.mark.asyncio
async def test_public_archive_failure_then_empty_through_window_stays_open() -> None:
    """A failed observation prevents empty-through-window gap settlement."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)

    class FailureThenEmptySource(ReconciliationSource):
        def __init__(self) -> None:
            super().__init__({})
            self.fail_target = True

        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if self.fail_target and target_date == target and metric == "heart_rate":
                self.requested.append(target_date)
                raise GarminConnectError("temporary family failure")
            return await super().async_fetch_details(target_date, metric)

    source = FailureThenEmptySource()
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)
    source.fail_target = False
    await _run_reconciliation_cycle(archive, timer, source)

    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)
    requests_at_window_end = source.requested.count(target)

    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    await _run_reconciliation_cycle(archive, timer, source)

    assert source.requested.count(target) > requests_at_window_end
    await archive.async_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("interruption", ("success", "failure", "cancel"))
async def test_reconciliation_family_checkpoint_survives_interruption(
    interruption: str,
) -> None:
    """A family checkpoint keeps an interrupted date Open across restart."""
    target = date(2026, 8, 4)
    store = FakeStore()
    now = [datetime(2026, 8, 4, tzinfo=UTC)]
    timer = DeterministicTimer()
    family_started = asyncio.Event()
    release_family = asyncio.Event()

    class InterruptedSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if target_date == target and metric == "heart_rate":
                if interruption == "cancel":
                    family_started.set()
                    await release_family.wait()
                if interruption == "failure":
                    raise GarminConnectError("temporary family failure")
                return SourceSeries(
                    (
                        NormalizedSample(
                            datetime(2026, 8, 4, 12, tzinfo=UTC),
                            target,
                            "2026-08-04T12:00:00Z",
                            72.0,
                        ),
                    ),
                    "present",
                )
            if target_date == target and metric == "sleep_sessions":
                family_started.set()
                await release_family.wait()
            return await super().async_fetch_details(target_date, metric)

    source = InterruptedSource({})
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await family_started.wait()
    await archive.async_stop()

    restarted_source = ReconciliationSource({})
    now[0] = datetime(2026, 8, 5, tzinfo=UTC)
    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(
        store, restarted_source, now, restarted_timer
    )
    await restarted.async_start()
    await _wait_for_remote_requests(restarted_source, 19)
    await _run_reconciliation_cycle(restarted, restarted_timer, restarted_source)

    assert target in restarted_source.requested
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_reenable_does_not_expire_open_date_before_new_activation_boundary() -> None:
    target = date(2026, 8, 1)
    store = _reconciliation_store(target)
    source = ReconciliationSource({})
    now = [datetime(2026, 8, 9, tzinfo=UTC)]
    timer = DeterministicTimer()
    archive = _enabled_reconciliation_archive(
        store,
        source,
        now,
        timer,
        activation_date=None,
        previously_enabled=False,
    )

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 8, tzinfo=UTC),
        ):
            await archive.async_start()
    await _wait_for_remote_requests(source, 19)
    await _run_reconciliation_cycle(archive, timer, source)

    assert archive.activation_date == date(2026, 8, 8)
    assert source.requested.count(target) == 0
    await archive.async_stop()


@pytest.mark.asyncio
async def test_disablement_stops_reconciliation_and_retains_public_queries() -> None:
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-08-01",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: False}
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(
            RecorderCompatibilityResult.compatible_result()
        ),
        store_factory=_store_factory(store),
        source_factory=lambda *_args: source,
    )

    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.DISABLED
    source.async_fetch.assert_not_awaited()


def _archive(
    hass: MagicMock,
    entry: MagicMock,
    checker: FakeRecorderChecker,
    store: FakeStore | None = None,
) -> GarminHistoryArchive:
    store = store or FakeStore()
    return GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=_store_factory(store),
    )


def _structured_calendar_archive(
    metric: str,
    payload: object,
) -> tuple[GarminHistoryArchive, dict[str, FakeStore]]:
    """Build a structured archive with one metric supplied by a test source."""
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=None
    )
    stores: dict[str, FakeStore] = {}

    def store_factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    class Source:
        async def async_fetch_details(
            self, _request_date: date, requested_metric: str
        ) -> object:
            if requested_metric == metric:
                return payload
            if requested_metric in {
                "sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"
            }:
                return ()
            return SourceSeries((), "missing")

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(
            RecorderCompatibilityResult.compatible_result()
        ),
        store_factory=store_factory,
        source_factory=lambda _client, _gate: Source(),
        recorder_factory=lambda: recorder,
    )
    return archive, stores


async def _sync_health_calendar_records(
    raw_events: list[dict[str, object]],
) -> tuple[GarminHistoryArchive, dict[str, FakeStore]]:
    """Archive captured health records through the public synchronization seam."""
    target = date(2026, 7, 24)
    health_events = normalize_health_events({"events": raw_events}, target)
    archive, stores = _structured_calendar_archive("health_events_daily", health_events)
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    return archive, stores


def _activity_calendar_archive(
    activities: tuple[object, ...],
) -> tuple[GarminHistoryArchive, dict[str, FakeStore]]:
    return _structured_calendar_archive("timed_activities", activities)


async def test_start_persists_opaque_account_key_and_reuses_it() -> None:
    """The identity is generated once and survives a new archive instance."""
    hass = _hass()
    entry = _entry()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    first_store = FakeStore()

    first = _archive(hass, entry, checker, first_store)
    await first.async_start()

    persisted_data = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    account_key = persisted_data["history_account_key"]
    assert len(account_key) >= 20
    assert "@" not in account_key
    assert entry.title not in account_key
    assert first.status.state is HistoryArchiveState.DISABLED
    assert first_store.saved[0]["account_key"] == account_key

    entry.data = persisted_data
    second = _archive(hass, entry, checker, FakeStore(data=first_store.data))
    await second.async_start()

    assert second.status.state is HistoryArchiveState.DISABLED
    assert hass.config_entries.async_update_entry.call_count == 1


async def test_start_keeps_historical_backfill_dormant() -> None:
    """Normal archive setup must not construct the legacy backfill scheduler."""
    hass = _hass()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    archive = _archive(hass, _entry(), checker)

    with patch("custom_components.garmin_connect.history.BackfillScheduler") as backfill:
        await archive.async_start()

    backfill.assert_not_called()


@pytest.mark.asyncio
async def test_numeric_source_calendar_dates_are_durable_across_restart_and_upsert() -> None:
    target = date(2026, 7, 24)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    def store_factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1))

    class Source:
        def __init__(self) -> None:
            self.source_date = target

        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {"sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            return SourceSeries(
                (
                    NormalizedSample(
                        datetime(2026, 7, 24, 23, 30, tzinfo=UTC),
                        self.source_date,
                        (
                            "2026-07-24T23:30:00+00:00"
                            if self.source_date == target
                            else "2026-07-25T07:30:00+08:00"
                        ),
                        60.0,
                    ),
                ),
                "present",
            )

    source = Source()
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=store_factory,
        source_factory=lambda client, gate: source,
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()
    report = await archive.async_sync_range(target, target)
    assert report.outcome == "written"
    catalog = stores["garmin_connect.entry-1.history_catalog"]
    assert "numeric_source_calendar_dates" not in catalog.data
    assert catalog.data["numeric_source_date_index"] == ["2026"]
    numeric_store = stores["garmin_connect.entry-1.numeric_source_dates_2026"]
    statistic_id = next(key for key in numeric_store.data["dates"] if key.endswith(":heart_rate"))
    assert numeric_store.data["dates"][statistic_id] == {
        "2026-07-24T23:30:00+00:00": "2026-07-24"
    }

    restarted = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=store_factory,
        source_factory=lambda client, gate: source,
        recorder_factory=lambda: recorder,
    )
    await restarted.async_start()
    source.source_date = date(2026, 7, 25)
    report = await restarted.async_sync_range(date(2026, 7, 25), date(2026, 7, 25))
    assert report.outcome == "written"
    assert numeric_store.data["dates"][statistic_id] == {
        "2026-07-24T23:30:00+00:00": "2026-07-25"
    }


@pytest.mark.asyncio
async def test_calendar_exposes_instantaneous_health_source_record() -> None:
    """A health event with only a Source Instant remains Calendar-queryable."""
    target = date(2026, 7, 24)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=None
    )
    stores: dict[str, FakeStore] = {}

    def store_factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    health_event = normalize_health_events(
        {
            "events": [
                {
                    "source": "GARMIN",
                    "type": "abnormalHeartRate",
                    "category": "abnormal",
                    "occurrenceTime": "2026-07-24T00:34:56+02:00",
                }
            ]
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "health_events_daily":
                return (health_event,)
            if metric in {
                "sleep_sessions",
                "health_events_body_battery",
                "timed_activities",
            }:
                return ()
            return SourceSeries((), "missing")

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=store_factory,
        source_factory=lambda _client, _gate: Source(),
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()

    assert (await archive.async_sync_range(target, target)).outcome == "written"

    events = await archive.async_get_calendar_events("health", target, target)
    assert [(event.summary, event.start, event.end) for event in events] == [
        (
            "abnormal",
            datetime(2026, 7, 23, 22, 34, 56, tzinfo=UTC),
            datetime(2026, 7, 23, 22, 34, 57, tzinfo=UTC),
        )
    ]


@pytest.mark.asyncio
async def test_raw_health_gmt_fixture_preserves_source_instant_and_calendar_query() -> None:
    """Naive Garmin GMT fields are UTC Source Instants and remain queryable."""
    fixture = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "garmin_health_events.naive_gmt.json"
        ).read_text()
    )
    target = date(2026, 7, 24)
    source_event = normalize_health_events(fixture, target)[0]

    source_start = datetime(2026, 7, 24, 23, tzinfo=UTC)
    source_end = datetime(2026, 7, 24, 23, 1, tzinfo=UTC)
    assert source_event.start == source_start
    assert source_event.end == source_end
    assert source_event.occurrence == datetime(2026, 7, 24, 23, 0, 30, tzinfo=UTC)

    archive, _stores = await _sync_health_calendar_records(fixture["events"])
    events = await archive.async_get_calendar_events("health", target, target)

    assert [(event.summary, event.start, event.end) for event in events] == [
        ("abnormal", source_start, source_end)
    ]


@pytest.mark.parametrize(
    ("raw_event", "expected_start", "stored_start", "stored_end"),
    [
        pytest.param(
            {"startTime": "2026-07-24T10:00:00Z"},
            datetime(2026, 7, 24, 10, 0, tzinfo=UTC),
            "2026-07-24T10:00:00+00:00",
            None,
            id="start-source-instant-only",
        ),
        pytest.param(
            {"endTime": "2026-07-24T11:00:00Z"},
            datetime(2026, 7, 24, 11, 0, tzinfo=UTC),
            None,
            "2026-07-24T11:00:00+00:00",
            id="end-source-instant-only",
        ),
    ],
)
async def test_health_single_endpoint_source_instant_is_queryable(
    raw_event: dict[str, object],
    expected_start: datetime,
    stored_start: str | None,
    stored_end: str | None,
) -> None:
    """A single endpoint projects to one second without changing Source Instants."""
    archive, stores = await _sync_health_calendar_records([raw_event])

    events = await archive.async_get_calendar_events(
        "health", date(2026, 7, 24), date(2026, 7, 24)
    )

    assert [(event.start, event.end) for event in events] == [
        (expected_start, expected_start + timedelta(seconds=1))
    ]
    stored_event = next(
        iter(stores["garmin_connect.entry-1.sleep_2026"].data["events"].values())
    )
    assert (stored_event["start"], stored_event["end"]) == (
        stored_start,
        stored_end,
    )


async def test_health_equal_endpoint_source_instants_project_to_valid_interval() -> None:
    """Equal Source Instants project to one second without changing persistence."""
    archive, stores = await _sync_health_calendar_records(
        [
            {
                "startTime": "2026-07-24T10:00:00Z",
                "endTime": "2026-07-24T10:00:00Z",
            }
        ]
    )

    events = await archive.async_get_calendar_events(
        "health", date(2026, 7, 24), date(2026, 7, 24)
    )

    source_instant = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    assert [(event.start, event.end) for event in events] == [
        (source_instant, source_instant + timedelta(seconds=1))
    ]
    stored_event = next(
        iter(stores["garmin_connect.entry-1.sleep_2026"].data["events"].values())
    )
    assert stored_event["start"] == stored_event["end"]


async def test_health_reversed_interval_is_skipped_without_hiding_valid_record() -> None:
    """A reversed Source Instant interval cannot fail the Calendar query."""
    archive, stores = await _sync_health_calendar_records(
        [
            {
                "category": "reversed",
                "startTime": "2026-07-24T11:00:00Z",
                "endTime": "2026-07-24T10:00:00Z",
            },
            {
                "category": "valid",
                "startTime": "2026-07-24T12:00:00Z",
                "endTime": "2026-07-24T13:00:00Z",
            },
        ]
    )

    events = await archive.async_get_calendar_events(
        "health", date(2026, 7, 24), date(2026, 7, 24)
    )

    assert [(event.summary, event.start, event.end) for event in events] == [
        (
            "valid",
            datetime(2026, 7, 24, 12, 0, tzinfo=UTC),
            datetime(2026, 7, 24, 13, 0, tzinfo=UTC),
        )
    ]
    assert len(stores["garmin_connect.entry-1.sleep_2026"].data["events"]) == 2


async def test_activity_with_gmt_source_instants_is_calendar_queryable_without_duration() -> None:
    """A raw activity with GMT Source Instants needs no synthetic duration."""
    target = date(2026, 1, 1)
    activities = normalize_activities(
        [
            {
                "activityId": 2,
                "activityType": {"typeKey": "walking"},
                "activityName": "New Year walk",
                "startTimeGMT": "2025-12-31T22:30:00.000",
                "startTimeLocal": "2026-01-01T00:30:00.000",
                "endTimeGMT": "2025-12-31T22:31:00.000",
            }
        ],
        target,
    )
    archive, _stores = _activity_calendar_archive(activities)
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"

    events = await archive.async_get_calendar_events(
        "activity", date(2025, 12, 31), date(2025, 12, 31)
    )

    assert [(event.summary, event.start, event.end) for event in events] == [
        (
            "New Year walk",
            datetime(2025, 12, 31, 22, 30, tzinfo=UTC),
            datetime(2025, 12, 31, 22, 31, tzinfo=UTC),
        )
    ]


@pytest.mark.asyncio
async def test_calendar_retains_distinct_same_time_activities() -> None:
    """Calendar deduplication cannot thin distinct activity Source Records."""
    target = date(2026, 7, 24)
    activities = normalize_activities(
        {
            "activities": [
                {
                    "activityId": 1,
                    "activityType": "running",
                    "activityName": "Morning run",
                    "startTime": "2026-07-24T06:00:00Z",
                    "endTime": "2026-07-24T07:00:00Z",
                },
                {
                    "activityId": 2,
                    "activityType": "running",
                    "activityName": "Morning run",
                    "startTime": "2026-07-24T06:00:00Z",
                    "endTime": "2026-07-24T07:00:00Z",
                },
            ]
        },
        target,
    )
    archive, stores = _activity_calendar_archive(activities)
    await archive.async_start()

    assert (await archive.async_sync_range(target, target)).outcome == "written"
    events = await archive.async_get_calendar_events("activity", target, target)

    assert [event.summary for event in events] == ["Morning run", "Morning run"]
    assert len(stores["garmin_connect.entry-1.sleep_2026"].data["activities"]) == 2


@pytest.mark.asyncio
async def test_structured_archive_upserts_and_survives_disabled_restart_per_account() -> None:
    """Structured Source Records stay durable, revision-aware, and account-isolated."""
    target = date(2026, 7, 24)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-one-123"})
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=None
    )
    stores: dict[str, FakeStore] = {}

    def store_factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    class Source:
        def __init__(self) -> None:
            self.revised = False

        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "sleep_sessions":
                return parse_sleep_sessions(
                    {
                        "sleepData": {
                            "sleepStartTimestampGMT": "2026-07-24T00:30:00+02:00",
                            "sleepEndTimestampGMT": "2026-07-24T08:30:00+02:00",
                            "sleepScores": {"overall": 81 if self.revised else 80},
                        }
                    },
                    target,
                )
            if metric == "health_events_daily":
                return normalize_health_events(
                    {
                        "events": [
                            {
                                "source": "GARMIN",
                                "type": "abnormalHeartRate",
                                "category": "revised" if self.revised else "abnormal",
                                "occurrenceTime": "2026-07-24T00:15:00+02:00",
                            }
                        ]
                    },
                    target,
                )
            if metric == "timed_activities":
                return normalize_activities(
                    {
                        "activities": [
                            {
                                "activityId": 99,
                                "activityType": "running",
                                "activityName": "Revised run" if self.revised else "Morning run",
                                "startTime": "2026-07-24T06:00:00Z",
                                "endTime": "2026-07-24T07:00:00Z",
                            }
                        ]
                    },
                    target,
                )
            if metric == "health_events_body_battery":
                return ()
            return SourceSeries((), "missing")

    source = Source()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    def make_archive(config_entry: MagicMock, source_factory) -> GarminHistoryArchive:
        return GarminHistoryArchive(
            hass,
            config_entry,
            recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
            store_factory=store_factory,
            source_factory=source_factory,
            recorder_factory=lambda: recorder,
        )

    archive = make_archive(entry, lambda _client, _gate: source)
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    first_partition = stores["garmin_connect.entry-1.sleep_2026"].data
    first_revisions = {
        family: next(iter(first_partition[family].values()))["revision"]
        for family in ("sessions", "events", "activities")
    }

    assert [event.summary for event in await archive.async_get_calendar_events("sleep", target, target)] == ["Sleep"]
    assert [event.summary for event in await archive.async_get_calendar_events("health", target, target)] == ["abnormal"]
    assert [event.summary for event in await archive.async_get_calendar_events("activity", target, target)] == ["Morning run"]

    source.revised = True
    archive._completed_dates.clear()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    revised_partition = stores["garmin_connect.entry-1.sleep_2026"].data
    assert {
        family: len(revised_partition[family])
        for family in ("sessions", "events", "activities")
    } == {"sessions": 1, "events": 1, "activities": 1}
    assert {
        family: next(iter(revised_partition[family].values()))["revision"]
        for family in ("sessions", "events", "activities")
    } != first_revisions

    no_request_source = MagicMock()
    no_request_source.async_fetch_details = AsyncMock(side_effect=AssertionError)
    restarted = make_archive(entry, lambda _client, _gate: no_request_source)
    await restarted.async_start()
    no_request_source.async_fetch_details.assert_not_awaited()
    assert restarted.archive_enabled is False
    assert [event.summary for event in await restarted.async_get_calendar_events("sleep", target, target)] == ["Sleep"]
    assert [event.summary for event in await restarted.async_get_calendar_events("health", target, target)] == ["revised"]
    assert [event.summary for event in await restarted.async_get_calendar_events("activity", target, target)] == ["Revised run"]

    other_entry = _entry(
        entry_id="entry-2", data={"history_account_key": "opaque-account-two-123"}
    )
    other_entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=None
    )
    other_source = Source()
    other_archive = make_archive(other_entry, lambda _client, _gate: other_source)
    await other_archive.async_start()
    assert (await other_archive.async_sync_range(target, target)).outcome == "written"

    assert [event.summary for event in await other_archive.async_get_calendar_events("activity", target, target)] == ["Morning run"]
    assert stores["garmin_connect.entry-1.sleep_2026"].data["account_key"] == "opaque-account-one-123"
    assert stores["garmin_connect.entry-2.sleep_2026"].data["account_key"] == "opaque-account-two-123"


@pytest.mark.asyncio
async def test_malformed_structured_record_fails_archive_without_blocking_foreground_work() -> None:
    """A malformed structured record is isolated from healthy current-value work."""
    target = date(2026, 7, 24)
    gate = GarminRequestGate()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=gate
    )

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "sleep_sessions":
                return "malformed sleep structure"
            if metric in {
                "health_events_daily",
                "health_events_body_battery",
                "timed_activities",
            }:
                return ()
            return SourceSeries((), "missing")

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        _hass(),
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda _client, _gate: Source(),
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert (report.outcome, report.error_type) == ("failed", "sync_failed")
    assert archive.status.state is HistoryArchiveState.FAILED
    assert await gate.async_request(
        GarminRequestPriority.FOREGROUND, _current_value
    ) == "current-value"


@pytest.mark.asyncio
async def test_scratch_recorder_archive_confirms_bucket_revision_and_provenance_after_restart() -> None:
    """Exercise Recorder identity, revisions, restart, and private provenance together."""
    target = date(2027, 1, 1)
    bucket = datetime(2026, 12, 31, 16, tzinfo=UTC)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    def store_factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    class ScratchRecorder:
        def __init__(self) -> None:
            self.rows: dict[tuple[str, datetime], dict[str, float | datetime]] = {}

        def async_import_statistics(self, metadata, stats, table) -> None:
            del table
            for row in stats:
                self.rows[(metadata["statistic_id"], row["start"])] = row

        def queue_task(self, task) -> None:
            task.future.set_result(None)

    class Source:
        def __init__(self) -> None:
            self.value = 60.0

        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {
                "sleep_sessions",
                "health_events_daily",
                "health_events_body_battery",
                "timed_activities",
            }:
                return ()
            if metric == "daily_summary":
                return SnapshotData(
                    {"abnormal_heart_rate_alerts": ("absent", None)},
                    bucket,
                    target.isoformat(),
                    calendar_date=target,
                )
            if metric == "training_status":
                return SnapshotData(
                    {
                        "acute_load": ("absent", None),
                        "chronic_load": ("absent", None),
                        "load_balance": ("absent", None),
                        "acwr": ("absent", None),
                        "vo2_max": ("absent", None),
                        "fitness_trend": ("absent", None),
                        "recovery_time": ("absent", None),
                    },
                    bucket,
                    target.isoformat(),
                    calendar_date=target,
                )
            if metric != "heart_rate":
                return SourceSeries((), "empty")
            return SourceSeries(
                (
                    NormalizedSample(
                        datetime(2027, 1, 1, tzinfo=ZoneInfo("Asia/Taipei")),
                        target,
                        "2027-01-01",
                        self.value,
                    ),
                ),
                "present",
            )

    recorder = ScratchRecorder()
    source = Source()

    def make_archive() -> GarminHistoryArchive:
        return GarminHistoryArchive(
            hass,
            entry,
            recorder_checker=checker,
            store_factory=store_factory,
            source_factory=lambda _client, _gate: source,
            recorder_factory=lambda: GarminHistoryRecorder(recorder),
        )

    first = make_archive()
    await first.async_start()
    assert (await first.async_sync_range(target, target)).outcome == "written"
    statistic_id = statistic_id_for("opaque-account-key-123", "heart_rate")
    assert recorder.rows[(statistic_id, bucket)]["mean"] == 60.0
    numeric_store = stores["garmin_connect.entry-1.numeric_source_dates_2026"]
    assert numeric_store.data["dates"][statistic_id] == {bucket.isoformat(): target.isoformat()}

    restarted = make_archive()
    await restarted.async_start()
    restarted._completed_dates.clear()
    source.value = 61.0
    assert (await restarted.async_sync_range(target, target)).outcome == "written"

    assert list(recorder.rows) == [(statistic_id, bucket)]
    assert recorder.rows[(statistic_id, bucket)]["mean"] == 61.0
    assert numeric_store.data["dates"][statistic_id] == {bucket.isoformat(): target.isoformat()}


@pytest.mark.asyncio
async def test_stalled_recorder_barrier_leaves_numeric_provenance_intent_recoverable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = date(2026, 12, 31)
    instant = datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    def store_factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    class StalledRecorder:
        def async_import_statistics(self, metadata, stats, table) -> None:
            del metadata, stats, table

        def queue_task(self, task) -> None:
            del task

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {
                "sleep_sessions",
                "health_events_daily",
                "health_events_body_battery",
                "timed_activities",
            }:
                return ()
            if metric in {"daily_summary", "training_status"}:
                return SnapshotData({}, instant, target.isoformat(), calendar_date=target)
            if metric == "heart_rate":
                return SourceSeries(
                    (NormalizedSample(instant, target, instant.isoformat(), 60.0),),
                    "present",
                )
            return SourceSeries((), "empty")

    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_TIMEOUT", 0)
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=store_factory,
        source_factory=lambda _client, _gate: Source(),
        recorder_factory=lambda: GarminHistoryRecorder(StalledRecorder()),
    )
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    statistic_id = statistic_id_for("opaque-account-key-123", "heart_rate")
    catalog = stores["garmin_connect.entry-1.history_catalog"].data
    assert report.outcome == "failed"
    assert report.error_type == "recorder_barrier"
    assert catalog["numeric_source_date_outbox"] == {
        "2026": {statistic_id: {instant.isoformat(): target.isoformat()}}
    }
    assert "garmin_connect.entry-1.numeric_source_dates_2026" not in stores


@pytest.mark.asyncio
async def test_full_numeric_presence_catalog_survives_restart() -> None:
    """All current numeric families remain durable, including duplicate totals."""
    target = date(2026, 7, 24)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    store = FakeStore()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    training_fields = {
        "acute_load": ("absent", None),
        "chronic_load": ("absent", None),
        "load_balance": ("absent", None),
        "acwr": ("absent", None),
        "vo2_max": ("absent", None),
        "fitness_trend": ("absent", None),
        "recovery_time": ("absent", None),
    }

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {"sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            if metric in {"steps", "floors", "intensity_moderate", "intensity_vigorous"}:
                total_keys = {
                    "steps": ("totalSteps",),
                    "floors": ("floorsAscended", "floorsDescended", "floorsAscendedInMeters", "floorsDescendedInMeters", "totalFloors"),
                    "intensity_moderate": ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes"),
                    "intensity_vigorous": ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes"),
                }[metric]
                return SegmentedData((), presence="empty", total_presence=dict.fromkeys(total_keys, "absent"))
            if metric == "daily_summary":
                return SnapshotData({"abnormal_heart_rate_alerts": ("absent", None)}, datetime.combine(target, datetime.min.time(), tzinfo=UTC), target.isoformat())
            if metric == "training_status":
                return SnapshotData(training_fields, datetime.combine(target, datetime.min.time(), tzinfo=UTC), target.isoformat())
            return SourceSeries((), "empty")

    def make_archive() -> GarminHistoryArchive:
        return GarminHistoryArchive(
            hass,
            entry,
            recorder_checker=checker,
            store_factory=_store_factory(store),
            source_factory=lambda _client, _gate: Source(),
            recorder_factory=lambda: recorder,
        )

    archive = make_archive()
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    assert len(store.data["presence"][target.isoformat()]) == 33

    restarted = make_archive()
    await restarted.async_start()
    assert restarted.status.state is HistoryArchiveState.DISABLED
    assert restarted.get_history_presence(target, target) == archive.get_history_presence(target, target)


@pytest.mark.asyncio
async def test_numeric_checkpoint_survives_restart_without_gap() -> None:
    """A numeric observation is durable before structured work can finish."""
    target = date(2026, 8, 4)
    store = _reconciliation_store(target)
    now = [datetime(2026, 8, 4, tzinfo=UTC)]
    initial_source = ReconciliationSource({})
    initial_timer = DeterministicTimer()
    initial = _enabled_reconciliation_archive(
        store, initial_source, now, initial_timer
    )

    await initial.async_start()
    await _wait_for_remote_requests(initial_source, 19)
    await _wait_for_archive_state(initial, HistoryArchiveState.IDLE)
    assert initial.get_history_presence(target, target)[target.isoformat()][
        "heart_rate"
    ] == "empty"
    await initial.async_stop()

    sleep_started = asyncio.Event()
    release_sleep = asyncio.Event()

    class InterruptedNumericSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if target_date == target and metric == "sleep_sessions":
                sleep_started.set()
                await release_sleep.wait()
            return await super().async_fetch_details(target_date, metric)

    interrupted_source = InterruptedNumericSource({target: (72.0,)})
    interrupted_timer = DeterministicTimer()
    interrupted = _enabled_reconciliation_archive(
        store, interrupted_source, now, interrupted_timer
    )
    await interrupted.async_start()
    await sleep_started.wait()

    assert interrupted_source.requested.count(target) > 0

    release_sleep.set()
    await interrupted.async_stop()

    now[0] = datetime(2026, 8, 12, tzinfo=UTC)
    restarted_source = ReconciliationSource({})
    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(
        store, restarted_source, now, restarted_timer
    )
    await restarted.async_start()
    await _wait_for_remote_requests(restarted_source, 19)
    await _wait_for_archive_state(restarted, HistoryArchiveState.IDLE)
    requests_before_window = restarted_source.requested.count(target)
    await _run_reconciliation_cycle(restarted, restarted_timer, restarted_source)
    assert restarted_source.requested.count(target) == requests_before_window
    assert restarted.get_history_presence(target, target)[target.isoformat()][
        "heart_rate"
    ] == "present"

    now[0] = datetime(2026, 8, 10, tzinfo=UTC)
    requests_before_retry = restarted_source.requested.count(target)
    await _run_reconciliation_cycle(restarted, restarted_timer, restarted_source)
    assert restarted_source.requested.count(target) > requests_before_retry
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_segmented_totals_are_written_with_provenance_and_revisions() -> None:
    target = date(2026, 7, 24)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    store = FakeStore()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    writes: list[tuple[str, object, tuple[NormalizedSample, ...]]] = []
    recorder = MagicMock()

    async def write(statistic_id, metadata, samples):
        writes.append((statistic_id, metadata, tuple(samples)))
        return RecorderWriteOutcome(len(samples))

    recorder.async_write = AsyncMock(side_effect=write)

    class Source:
        def __init__(self) -> None:
            self.revised = False

        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {"sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            if metric == "steps":
                return SegmentedData((), {"totalSteps": 0.0}, "empty", {"totalSteps": "present"})
            if metric == "floors":
                return SegmentedData((), {"totalFloors": 2.0}, "empty", {"totalFloors": "present"})
            if metric in {"intensity_moderate", "intensity_vigorous"}:
                value = 9.0 if self.revised else 7.0
                return SegmentedData((), {"totalIntensityMinutes": value}, "empty", {"totalIntensityMinutes": "present"})
            return SourceSeries((), "empty")

    source = Source()
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=_store_factory(store),
        source_factory=lambda _client, _gate: source,
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    total_writes = {
        statistic_id.rsplit(":", 1)[-1]: samples[-1].value
        for statistic_id, _metadata, samples in writes
        if samples and statistic_id.rsplit(":", 1)[-1] in {
            "floors_daily_total", "intensity_daily_total", "steps_daily_total"
        }
    }
    assert total_writes == {
        "floors_daily_total": 2.0,
        "intensity_daily_total": 7.0,
        "steps_daily_total": 0.0,
    }
    assert archive.get_history_presence(target, target)[target.isoformat()]["floors:totalFloors"] == "present"

    source.revised = True
    archive._completed_dates.clear()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    assert any(
        statistic_id.endswith(":intensity_daily_total") and samples[-1].value == 9.0
        for statistic_id, _metadata, samples in writes
    )


@pytest.mark.asyncio
async def test_date_only_segmented_total_uses_cross_year_calendar_bucket() -> None:
    """Daily totals use the UTC+08:00 bucket and retain Source Calendar Date."""
    target = date(2027, 1, 1)
    writes: list[tuple[str, tuple[NormalizedSample, ...]]] = []
    recorder = MagicMock()

    async def write(statistic_id, _metadata, samples):
        writes.append((statistic_id, tuple(samples)))
        return RecorderWriteOutcome(len(samples))

    recorder.async_write = AsyncMock(side_effect=write)

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "steps":
                return SegmentedData(
                    (), {"totalSteps": 123.0}, "empty", {"totalSteps": "present"}
                )
            return ()

    archive = GarminHistoryArchive(
        _hass(),
        _entry(data={"history_account_key": "opaque-account-key-123"}),
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda _client, _gate: Source(),
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"

    samples = next(
        samples
        for statistic_id, samples in writes
        if statistic_id.endswith(":steps_daily_total")
    )
    assert samples == (
        NormalizedSample(
            datetime(2026, 12, 31, 16, tzinfo=UTC), target, target.isoformat(), 123.0
        ),
    )


@pytest.mark.asyncio
async def test_numeric_manifest_recovery_replays_after_partition_failure() -> None:
    target = date(2026, 12, 31)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    class FlakyStore(FakeStore):
        fail_saves = True

        async def async_save(self, data):
            if self.fail_saves and "numeric_source_dates_" in getattr(self, "path", ""):
                raise OSError("simulated crash")
            await super().async_save(data)

    def factory(_hass, _version, path, **kwargs):
        store = stores.setdefault(path, FlakyStore())
        store.path = path
        return store

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {"sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            return SourceSeries((NormalizedSample(datetime(2026, 12, 31, tzinfo=UTC), target, target.isoformat(), 1),), "present")

    def make_archive() -> GarminHistoryArchive:
        return GarminHistoryArchive(
            hass, entry, recorder_checker=checker, store_factory=factory,
            source_factory=lambda _client, _gate: Source(), recorder_factory=lambda: recorder,
        )

    first = make_archive()
    await first.async_start()
    assert (await first.async_sync_range(target, target)).outcome == "failed"
    assert stores["garmin_connect.entry-1.history_catalog"].data["numeric_source_date_pending"]
    outbox = stores["garmin_connect.entry-1.history_catalog"].data[
        "numeric_source_date_outbox"
    ]
    assert outbox["2026"][next(iter(outbox["2026"]))][
        "2026-12-31T00:00:00+00:00"
    ] == target.isoformat()

    stores["garmin_connect.entry-1.numeric_source_dates_2026"].fail_saves = False
    restarted = make_archive()
    await restarted.async_start()
    assert restarted.status.state is HistoryArchiveState.DISABLED
    assert target.isoformat() not in restarted._completed_dates
    assert (await restarted.async_sync_range(target, target)).outcome == "written"
    assert stores["garmin_connect.entry-1.numeric_source_dates_2026"].data["dates"]
    assert not stores["garmin_connect.entry-1.history_catalog"].data[
        "numeric_source_date_outbox"
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_kind", ("partition", "catalog"))
async def test_confirmed_numeric_provenance_survives_storage_failure_and_empty_replay(
    failure_kind: str,
) -> None:
    """A Recorder write keeps Source Calendar Date through empty recovery."""
    target = date(2026, 12, 31)
    instant = datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    class FlakyStore(FakeStore):
        fail_partition_save = failure_kind == "partition"
        fail_catalog_save = False
        trigger_catalog_failure = failure_kind == "catalog"

        async def async_save(self, data: dict) -> None:
            if self.fail_catalog_save and self.path.endswith("history_catalog"):
                self.fail_catalog_save = False
                raise OSError("simulated catalog failure")
            if self.fail_partition_save and "numeric_source_dates_" in self.path:
                raise OSError("simulated partition failure")
            await super().async_save(data)
            if (
                self.trigger_catalog_failure
                and "numeric_source_dates_" in self.path
            ):
                self.trigger_catalog_failure = False
                catalog = stores["garmin_connect.entry-1.history_catalog"]
                catalog.fail_catalog_save = True

    def factory(_hass, _version, path, **kwargs):
        store = stores.setdefault(path, FlakyStore())
        store.path = path
        return store

    class SampleSource:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "heart_rate":
                return SourceSeries(
                    (NormalizedSample(instant, target, instant.isoformat(), 60.0),),
                    "present",
                )
            return ()

    class EmptySource:
        async def async_fetch_details(self, _request_date: date, _metric: str) -> object:
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1))

    def make_archive(source: object) -> GarminHistoryArchive:
        return GarminHistoryArchive(
            hass,
            entry,
            recorder_checker=checker,
            store_factory=factory,
            source_factory=lambda _client, _gate: source,
            recorder_factory=lambda: recorder,
        )

    first = make_archive(SampleSource())
    await first.async_start()
    assert (await first.async_sync_range(target, target)).outcome == "failed"

    partition = stores["garmin_connect.entry-1.numeric_source_dates_2026"]
    partition.fail_partition_save = False
    restarted = make_archive(EmptySource())
    await restarted.async_start()
    assert (await restarted.async_sync_range(target, target)).outcome == "written"

    statistic_id = statistic_id_for("opaque-account-key-123", "heart_rate")
    assert partition.data["dates"][statistic_id] == {instant.isoformat(): target.isoformat()}
    assert partition.data["tombstones"] == []


@pytest.mark.asyncio
async def test_recorder_success_manifest_failure_restarts_without_numeric_tombstone() -> None:
    """A confirmed Recorder row remains queryable after a manifest failure."""
    target = date(2026, 12, 31)
    instant = datetime(2026, 12, 31, 23, 30, tzinfo=UTC)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    class ManifestFailureStore(FakeStore):
        def __init__(self) -> None:
            super().__init__()
            self.fail_confirmed_manifest = True

        async def async_save(self, data: dict) -> None:
            if self.fail_confirmed_manifest and data.get("numeric_source_date_confirmed"):
                raise OSError("simulated manifest failure")
            await super().async_save(data)

    def factory(_hass, _version, path, **kwargs):
        if path.endswith("history_catalog"):
            return stores.setdefault(path, ManifestFailureStore())
        return stores.setdefault(path, FakeStore())

    class SampleSource:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "heart_rate":
                return SourceSeries(
                    (NormalizedSample(instant, target, instant.isoformat(), 60.0),),
                    "present",
                )
            return ()

    class EmptySource:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric == "heart_rate":
                return SourceSeries((), "empty")
            return ()

    rows: dict[datetime, NormalizedSample] = {}
    recorder = MagicMock()

    async def write(_statistic_id, _metadata, samples):
        rows.update({sample.timestamp: sample for sample in samples})
        return RecorderWriteOutcome(len(samples))

    recorder.async_write = AsyncMock(side_effect=write)

    def make_archive(source: object) -> GarminHistoryArchive:
        return GarminHistoryArchive(
            hass,
            entry,
            recorder_checker=checker,
            store_factory=factory,
            source_factory=lambda _client, _gate: source,
            recorder_factory=lambda: recorder,
        )

    first = make_archive(SampleSource())
    await first.async_start()
    first_report = await first.async_sync_range(target, target)
    assert (first_report.outcome, first_report.error_type) == ("failed", "sync_failed")
    assert first.status.state is HistoryArchiveState.FAILED
    assert rows[instant].value == 60.0

    restarted = make_archive(EmptySource())
    await restarted.async_start()
    assert (await restarted.async_sync_range(target, target)).outcome == "written"
    assert rows[instant].value == 60.0
    statistic_id = statistic_id_for("opaque-account-key-123", "heart_rate")
    partition = stores["garmin_connect.entry-1.numeric_source_dates_2026"]
    assert partition.data["dates"][statistic_id] == {
        instant.isoformat(): target.isoformat()
    }
    assert partition.data["tombstones"] == []


@pytest.mark.asyncio
async def test_recorder_failure_outbox_does_not_confirm_provenance_on_empty_retry() -> None:
    """An unbarriered Recorder write cannot become durable provenance after restart."""
    target = date(2026, 12, 31)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    stores: dict[str, FakeStore] = {}

    def factory(_hass, _version, path, **kwargs):
        store = stores.setdefault(path, FakeStore())
        store.path = path
        return store

    class SampleSource:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {"sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            return SourceSeries(
                (
                    NormalizedSample(
                        datetime(2026, 12, 31, tzinfo=UTC),
                        target,
                        target.isoformat(),
                        1.0,
                    ),
                ),
                "present",
            )

    class EmptySource:
        async def async_fetch_details(self, _request_date: date, _metric: str) -> object:
            return ()

    failed_recorder = MagicMock()
    failed_recorder.async_write = AsyncMock(
        return_value=RecorderWriteOutcome(0, "failed", "recorder_write")
    )

    first = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=factory,
        source_factory=lambda _client, _gate: SampleSource(),
        recorder_factory=lambda: failed_recorder,
    )
    await first.async_start()

    assert (await first.async_sync_range(target, target)).outcome == "failed"
    assert stores["garmin_connect.entry-1.history_catalog"].data[
        "numeric_source_date_outbox"
    ]

    retry_recorder = MagicMock()
    retry_recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    restarted = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=factory,
        source_factory=lambda _client, _gate: EmptySource(),
        recorder_factory=lambda: retry_recorder,
    )
    await restarted.async_start()

    assert target.isoformat() not in restarted._completed_dates
    assert target.isoformat() in restarted._numeric_source_date_replay_dates
    assert (await restarted.async_sync_range(target, target)).outcome == "written"
    numeric_store = stores["garmin_connect.entry-1.numeric_source_dates_2026"]
    assert numeric_store.data["dates"] == {}
    assert numeric_store.data["tombstones"] == [target.isoformat()]
    assert stores["garmin_connect.entry-1.history_catalog"].data["completed_dates"] == [
        target.isoformat()
    ]


@pytest.mark.asyncio
async def test_malformed_numeric_partition_does_not_abort_archive_startup() -> None:
    target = date(2026, 7, 24)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    catalog = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "completed_dates": [target.isoformat()],
        "hrv_summaries": {},
        "numeric_source_date_index": ["2026"],
        "numeric_source_date_dates": {"2026": [target.isoformat()]},
        "numeric_source_date_pending": {},
        "presence": {},
        "sleep_index": {},
        "event_index": {},
        "activity_index": {},
    })
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        "garmin_connect.entry-1.numeric_source_dates_2026": FakeStore({"corrupt": True}),
    }

    def factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=factory,
    )
    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.DISABLED
    assert target.isoformat() not in archive._completed_dates
    assert target.isoformat() in archive._numeric_source_date_replay_dates
    assert catalog.data["numeric_source_date_pending"]["2026"] == [target.isoformat()]


@pytest.mark.asyncio
async def test_corrupt_numeric_partition_replays_only_affected_dates() -> None:
    retained = date(2026, 7, 24)
    affected = date(2026, 7, 25)
    statistic_id = statistic_id_for("opaque-account-key-123", "heart_rate")
    catalog = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "completed_dates": [retained.isoformat(), affected.isoformat()],
        "hrv_summaries": {},
        "numeric_source_date_index": ["2026"],
        "numeric_source_date_dates": {"2026": [retained.isoformat(), affected.isoformat()]},
        "numeric_source_date_pending": {},
        "numeric_source_date_outbox": {},
        "presence": {},
        "sleep_index": {},
        "event_index": {},
        "activity_index": {},
    })
    partition = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "year": "2026",
        "dates": {
            statistic_id: {
                "2026-07-24T01:00:00+00:00": retained.isoformat(),
                "malformed-instant": affected.isoformat(),
            }
        },
    })
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        "garmin_connect.entry-1.numeric_source_dates_2026": partition,
    }

    def factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    archive = GarminHistoryArchive(
        _hass(),
        _entry(data={"history_account_key": "opaque-account-key-123"}),
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=factory,
    )
    await archive.async_start()

    assert archive._numeric_source_calendar_dates_by_year["2026"][statistic_id] == {
        "2026-07-24T01:00:00+00:00": retained.isoformat(),
    }
    assert retained.isoformat() in archive._completed_dates
    assert affected.isoformat() not in archive._completed_dates
    assert archive._numeric_source_date_replay_dates == {affected.isoformat()}


@pytest.mark.asyncio
async def test_empty_numeric_replay_persists_tombstone_before_clearing_pending() -> None:
    target = date(2026, 7, 24)
    catalog = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "completed_dates": [target.isoformat()],
        "hrv_summaries": {},
        "numeric_source_date_index": ["2026"],
        "numeric_source_date_dates": {"2026": [target.isoformat()]},
        "numeric_source_date_pending": {},
        "numeric_source_date_outbox": {},
        "presence": {},
        "sleep_index": {},
        "event_index": {},
        "activity_index": {},
    })
    partition = FakeStore({"corrupt": True})
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        "garmin_connect.entry-1.numeric_source_dates_2026": partition,
    }

    def factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    class EmptySource:
        async def async_fetch_details(self, _target: date, _metric: str) -> object:
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})

    def make_archive() -> GarminHistoryArchive:
        return GarminHistoryArchive(
            _hass(), entry,
            recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
            store_factory=factory,
            source_factory=lambda _client, _gate: EmptySource(),
            recorder_factory=lambda: recorder,
        )

    first = make_archive()
    await first.async_start()
    assert target.isoformat() not in first._completed_dates
    assert (await first.async_sync_range(target, target)).outcome == "written"
    assert partition.data["tombstones"] == [target.isoformat()]
    assert catalog.data["numeric_source_date_pending"] == {}

    restarted = make_archive()
    await restarted.async_start()
    assert target.isoformat() in restarted._completed_dates
    assert target.isoformat() not in restarted._numeric_source_date_replay_dates


@pytest.mark.asyncio
async def test_cross_year_numeric_source_calendar_date_catalog_tombstone_restarts() -> None:
    """Source Calendar Date tombstones use the Source Instant UTC year."""
    source_date = date(2027, 1, 1)
    instant_year = "2026"
    catalog = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "completed_dates": [source_date.isoformat()],
        "hrv_summaries": {},
        "numeric_source_date_index": [instant_year],
        "numeric_source_date_dates": {instant_year: [source_date.isoformat()]},
        "numeric_source_date_pending": {},
        "numeric_source_date_outbox": {},
        "numeric_source_date_tombstones": {instant_year: [source_date.isoformat()]},
        "presence": {},
        "sleep_index": {},
        "event_index": {},
        "activity_index": {},
    })
    stores = {"garmin_connect.entry-1.history_catalog": catalog}

    def factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    archive = GarminHistoryArchive(
        _hass(),
        _entry(data={"history_account_key": "opaque-account-key-123"}),
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=factory,
    )
    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.DISABLED
    assert source_date.isoformat() in archive._completed_dates
    assert source_date.isoformat() not in archive._numeric_source_date_replay_dates


@pytest.mark.asyncio
async def test_cross_year_numeric_source_calendar_date_partition_tombstone_restarts() -> None:
    """Partition tombstones recover Source Calendar Dates across a UTC year."""
    source_date = date(2027, 1, 1)
    instant_year = "2026"
    catalog = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "completed_dates": [source_date.isoformat()],
        "hrv_summaries": {},
        "numeric_source_date_index": [instant_year],
        "numeric_source_date_dates": {instant_year: [source_date.isoformat()]},
        "numeric_source_date_pending": {instant_year: [source_date.isoformat()]},
        "numeric_source_date_outbox": {},
        "presence": {},
        "sleep_index": {},
        "event_index": {},
        "activity_index": {},
    })
    partition = FakeStore({
        "schema_version": 1,
        "account_key": "opaque-account-key-123",
        "year": instant_year,
        "dates": {},
        "tombstones": [source_date.isoformat()],
    })
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        f"garmin_connect.entry-1.numeric_source_dates_{instant_year}": partition,
    }

    def factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    archive = GarminHistoryArchive(
        _hass(),
        _entry(data={"history_account_key": "opaque-account-key-123"}),
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=factory,
    )
    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.DISABLED
    assert source_date.isoformat() in archive._completed_dates
    assert source_date.isoformat() not in archive._numeric_source_date_replay_dates
    assert catalog.data is not None
    assert catalog.data["numeric_source_date_tombstones"] == {
        instant_year: [source_date.isoformat()]
    }
    assert catalog.data["numeric_source_date_pending"] == {}


@pytest.mark.asyncio
async def test_numeric_partition_replay_is_date_scoped_and_empty_retries_settle() -> None:
    first_date = date(2026, 7, 24)
    second_date = date(2026, 7, 25)
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-123"})
    catalog = FakeStore(
        {
            "schema_version": 1,
            "account_key": "opaque-account-key-123",
            "completed_dates": [first_date.isoformat(), second_date.isoformat()],
            "hrv_summaries": {},
            "numeric_source_date_index": ["2026"],
            "numeric_source_date_dates": {"2026": [first_date.isoformat()]},
            "numeric_source_date_pending": {},
            "presence": {},
            "sleep_index": {},
            "event_index": {},
            "activity_index": {},
        }
    )
    stores = {
        "garmin_connect.entry-1.history_catalog": catalog,
        "garmin_connect.entry-1.numeric_source_dates_2026": FakeStore({"corrupt": True}),
    }

    def factory(_hass, _version, path, **kwargs):
        return stores.setdefault(path, FakeStore())

    class Source:
        async def async_fetch_details(self, _request_date: date, metric: str) -> object:
            if metric in {"sleep_sessions", "health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            return SourceSeries((), "empty")

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=factory,
        source_factory=lambda _client, _gate: Source(),
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()

    assert archive._completed_dates == set()
    assert catalog.data["numeric_source_date_pending"]["2026"] == [
        first_date.isoformat(),
        second_date.isoformat(),
    ]

    first = await archive.async_sync_range(first_date, first_date)

    assert first.outcome == "written"
    assert catalog.data["completed_dates"] == [first_date.isoformat()]
    assert catalog.data["numeric_source_date_pending"]["2026"] == [second_date.isoformat()]
    assert first_date.isoformat() not in archive._numeric_source_date_replay_dates

    repaired = await archive.async_sync_range(first_date, first_date)
    assert repaired.outcome == "written"
    assert repaired.skipped_count == 0
    assert catalog.data["numeric_source_date_pending"]["2026"] == [second_date.isoformat()]


def test_history_status_and_archive_metadata_use_one_public_contract() -> None:
    """Public archive states and persisted metadata names stay bounded."""
    assert {state.value for state in HistoryArchiveState} == {
        "disabled",
        "idle",
        "syncing",
        "backoff",
        "failed",
    }
    assert const.CONF_ARCHIVE_PREVIOUSLY_ENABLED == "archive_last_enabled"
    assert not hasattr(const, "CONF_ARCHIVE_LAST_ENABLED")


async def test_enablement_persists_archive_activation_date() -> None:
    """Enablement records the Home Assistant date as Archive Activation Date."""
    hass = _hass()
    entry = _entry(data={CONF_ARCHIVE_PREVIOUSLY_ENABLED: False})
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)

    with patch("custom_components.garmin_connect.history.dt_util.utcnow", return_value=now):
        await _archive(hass, entry, checker).async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert persisted[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True


async def test_enabled_start_syncs_only_the_archive_activation_date() -> None:
    """Archive enablement immediately imports one bounded current-day batch."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    source = MagicMock()

    async def fetch(target: date, metric: str):
        if metric == "heart_rate":
            return (
                NormalizedSample(
                    datetime.combine(target, datetime.min.time(), tzinfo=UTC),
                    target,
                    target.isoformat(),
                    72.0,
                ),
            )
        return ()

    source.async_fetch = AsyncMock(side_effect=fetch)
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 30, tzinfo=UTC),
    ), patch(
        "custom_components.garmin_connect.history.dt_util.DEFAULT_TIME_ZONE",
        ZoneInfo("Asia/Taipei"),
    ):
        await archive.async_start()
        first_sync_task = archive._first_sync_task
        assert first_sync_task is not None
        await first_sync_task

    assert archive.status.state is HistoryArchiveState.IDLE
    assert source.async_fetch.await_count == 15
    assert {call.args[0] for call in source.async_fetch.await_args_list} == {
        date(2026, 8, 4)
    }
    assert recorder.async_write.await_args_list[0].args[2][0].value == 72.0


async def test_successful_first_sync_starts_fifteen_minute_local_day_cycles() -> None:
    """A successful activation schedules current Archive Activation Date cycles."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    now = [datetime(2026, 8, 3, 23, 30, tzinfo=UTC)]
    timer = DeterministicTimer()
    requested_dates: list[date] = []
    first_cycle_done = asyncio.Event()
    second_cycle_done = asyncio.Event()

    async def fetch(target: date, _metric: str):
        requested_dates.append(target)
        if len(requested_dates) == 30:
            first_cycle_done.set()
        if len(requested_dates) == 45:
            second_cycle_done.set()
        return ()

    source = MagicMock()
    source.async_fetch = AsyncMock(side_effect=fetch)
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        clock=lambda: now[0],
        timer_factory=timer.call_later,
    )

    with patch(
        "custom_components.garmin_connect.history.dt_util.DEFAULT_TIME_ZONE",
        ZoneInfo("Asia/Taipei"),
    ):
        await archive.async_start()
        first_sync_task = archive._first_sync_task
        assert first_sync_task is not None
        await first_sync_task

        assert [slot[0] for slot in timer.active] == [timedelta(minutes=15)]
        timer.fire_next()
        await asyncio.wait_for(first_cycle_done.wait(), timeout=0.1)
        assert set(requested_dates) == {date(2026, 8, 4)}

        now[0] = datetime(2026, 8, 4, 23, 30, tzinfo=UTC)
        timer.fire_next()
        await asyncio.wait_for(second_cycle_done.wait(), timeout=0.1)

    assert set(requested_dates) == {date(2026, 8, 4), date(2026, 8, 5)}
    await archive.async_stop()


@pytest.mark.asyncio
async def test_successful_archive_status_persists_and_restores_schedule() -> None:
    """A successful sync exposes and restores its lifecycle timestamps."""
    now = [datetime(2026, 8, 4, 12, tzinfo=UTC)]
    timer = DeterministicTimer()
    store = FakeStore()
    source = ReconciliationSource({})
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_remote_requests(source, 19)

    expected_last_success = now[0].isoformat()
    expected_next_run = (now[0] + timedelta(minutes=15)).isoformat()
    assert archive.status.as_attributes() == {
        "archive_state": "idle",
        "activation_date": "2026-08-01",
        "last_success": expected_last_success,
        "next_eligible_run": expected_next_run,
    }

    await archive.async_stop()
    restarted_timer = DeterministicTimer()
    restarted = _enabled_reconciliation_archive(
        store, ReconciliationSource({}), now, restarted_timer
    )
    await restarted.async_start()

    assert restarted.status.as_attributes()["last_success"] == expected_last_success
    assert restarted.status.as_attributes()["next_eligible_run"] == expected_next_run
    await restarted.async_stop()


async def test_recurring_archive_yields_real_shared_gate_to_foreground_refresh() -> None:
    """Foreground work keeps priority and current-value continuity during a cycle."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    timer = DeterministicTimer()
    gate = GarminRequestGate()
    cycle_started = asyncio.Event()
    release_cycle_request = asyncio.Event()
    first_sync_done = asyncio.Event()
    cycle_done = asyncio.Event()
    events: list[str] = []

    class Client:
        _base_url = "https://garmin.example"

        def __init__(self) -> None:
            self.requests = 0
            self.cycle_requests = 0
            self.cycle_enabled = False

        async def get_user_profile(self):
            return SimpleNamespace(display_name="athlete")

        async def _mark_request(self):
            self.requests += 1
            if not self.cycle_enabled:
                if self.requests == 19:
                    first_sync_done.set()
                return {}
            self.cycle_requests += 1
            events.append(f"archive-{self.cycle_requests}")
            if self.cycle_requests == 1:
                cycle_started.set()
                await release_cycle_request.wait()
            if self.cycle_requests == 19:
                cycle_done.set()
            return {}

        async def _request(self, *_args, **_kwargs):
            return await self._mark_request()

        async def _get_hrv_data_raw(self, _target):
            return await self._mark_request()

        async def _get_sleep_data_raw(self, _target):
            return await self._mark_request()

        async def _get_user_summary_raw(self, _target):
            return await self._mark_request()

        async def get_activities(self, _offset, _limit):
            await self._mark_request()
            return []

        async def get_training_status(self, _target):
            return await self._mark_request()

    client = Client()
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=client), request_gate=gate
    )
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda source_client, request_gate: GarminHistorySource(
            source_client, request_gate
        ),
        recorder_factory=lambda: recorder,
        timer_factory=timer.call_later,
    )

    await archive.async_start()
    await asyncio.wait_for(first_sync_done.wait(), timeout=0.1)
    for _ in range(100):
        if timer.active:
            break
        await asyncio.sleep(0)
    assert timer.active, (archive.status.error_type, client.requests)

    client.cycle_enabled = True
    timer.fire_next()
    await asyncio.wait_for(cycle_started.wait(), timeout=0.1)

    async def current_value_request() -> str:
        events.append("current")
        return "current-value"

    current_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.FOREGROUND, current_value_request)
    )
    await asyncio.sleep(0)
    release_cycle_request.set()

    assert await asyncio.wait_for(current_task, timeout=0.1) == "current-value"
    assert events.index("current") < events.index("archive-2")

    await asyncio.wait_for(cycle_done.wait(), timeout=0.1)
    assert archive.status.state is HistoryArchiveState.IDLE
    await archive.async_stop()


async def test_archive_cycle_failure_does_not_break_foreground_request() -> None:
    """A failed recurring cycle leaves healthy current-value work functional."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    timer = DeterministicTimer()
    gate = GarminRequestGate()
    cycle_started = asyncio.Event()
    release_cycle_request = asyncio.Event()
    first_sync_done = asyncio.Event()

    class FailingSource:
        def __init__(self, request_gate: GarminRequestGate) -> None:
            self.request_gate = request_gate
            self.calls = 0
            self.cycle_enabled = False

        async def async_fetch(self, _target: date, _metric: str):
            self.calls += 1
            if self.calls == 15:
                first_sync_done.set()

            async def request():
                if self.cycle_enabled:
                    cycle_started.set()
                    await release_cycle_request.wait()
                    raise OSError("archive endpoint unavailable")
                return ()

            return await self.request_gate.async_request(
                GarminRequestPriority.BACKGROUND, request
            )

    source: FailingSource | None = None

    def source_factory(_client, request_gate):
        nonlocal source
        if source is None:
            source = FailingSource(request_gate)
        return source

    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=gate
    )
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=source_factory,
        recorder_factory=lambda: recorder,
        timer_factory=timer.call_later,
    )

    await archive.async_start()
    await asyncio.wait_for(first_sync_done.wait(), timeout=0.1)
    for _ in range(100):
        if timer.active:
            break
        await asyncio.sleep(0)
    assert timer.active
    assert source is not None
    source.cycle_enabled = True
    timer.fire_next()
    await asyncio.wait_for(cycle_started.wait(), timeout=0.1)

    async def current_value_request() -> str:
        return "current-value"

    current_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.FOREGROUND, current_value_request)
    )
    await asyncio.sleep(0)
    release_cycle_request.set()

    assert await asyncio.wait_for(current_task, timeout=0.1) == "current-value"
    for _ in range(100):
        if archive.status.state is HistoryArchiveState.FAILED:
            break
        await asyncio.sleep(0)
    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "sync_failed"
    await archive.async_stop()


async def test_archive_scheduler_failure_does_not_break_foreground_request() -> None:
    """A cadence scheduling failure leaves healthy current-value work functional."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    gate = GarminRequestGate()
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=gate
    )
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    def failing_timer(_delay: timedelta, _callback: Callable[[], None]) -> Callable[[], None]:
        raise OSError("timer unavailable")

    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *_args: source,
        recorder_factory=lambda: recorder,
        timer_factory=failing_timer,
    )

    await archive.async_start()
    first_sync_task = archive._first_sync_task
    assert first_sync_task is not None
    await first_sync_task

    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "schedule"
    assert (
        await gate.async_request(
            GarminRequestPriority.FOREGROUND, lambda: _current_value()
        )
        == "current-value"
    )
    await archive.async_stop()


async def _current_value() -> str:
    """Return the healthy current-value result used by isolation tests."""
    return "current-value"


async def test_failed_first_sync_does_not_start_recurring_cadence() -> None:
    """Activation failure leaves no future archive wakeup armed."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    timer = DeterministicTimer()
    source = MagicMock()
    source.async_fetch = AsyncMock(
        return_value=(
            NormalizedSample(
                datetime(2026, 8, 4, tzinfo=UTC),
                date(2026, 8, 4),
                "2026-08-04T00:00:00Z",
                72.0,
            ),
        )
    )
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(
        return_value=RecorderWriteOutcome(0, "failed", "recorder_write")
    )
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        timer_factory=timer.call_later,
    )

    await archive.async_start()
    first_sync_task = archive._first_sync_task
    assert first_sync_task is not None
    await first_sync_task

    assert archive.status.state is HistoryArchiveState.FAILED
    assert timer.active == []


@pytest.mark.asyncio
async def test_archive_rate_limit_enters_durable_backoff_without_cadence() -> None:
    """An archive 429 pauses only archive work for the durable 24-hour window."""
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    store = FakeStore()

    class RateLimitedSource(ReconciliationSource):
        def __init__(self) -> None:
            super().__init__({})
            self.fail_once = True

        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if self.fail_once and metric == "heart_rate":
                self.fail_once = False
                self.requested.append(target_date)
                raise GarminRateLimitError("429")
            return await super().async_fetch_details(target_date, metric)

    source = RateLimitedSource()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_archive_state(archive, HistoryArchiveState.BACKOFF)

    assert archive.status.as_attributes() == {
        "archive_state": "backoff",
        "activation_date": "2026-08-01",
        "next_eligible_run": "2026-08-06T00:00:00+00:00",
        "safe_error_class": "rate_limited",
    }
    assert store.data["archive_backoff_until"] == "2026-08-06T00:00:00+00:00"
    assert [slot[0] for slot in timer.active] == [timedelta(hours=24)]

    now[0] = datetime(2026, 8, 6, tzinfo=UTC)
    timer.fire_next()
    await _wait_for_remote_requests(source, 20)
    assert archive.status.state is HistoryArchiveState.IDLE
    await archive.async_stop()


@pytest.mark.asyncio
async def test_first_sync_rate_limit_retries_after_expiry_without_restart() -> None:
    """An initial archive 429 arms an expiry retry without a reload."""
    now = [datetime(2026, 8, 5, tzinfo=UTC)]
    timer = DeterministicTimer()
    store = FakeStore()

    class RateLimitedSource(ReconciliationSource):
        def __init__(self) -> None:
            super().__init__({})
            self.fail_once = True

        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if self.fail_once and metric == "heart_rate":
                self.fail_once = False
                self.requested.append(target_date)
                raise GarminRateLimitError("429")
            return await super().async_fetch_details(target_date, metric)

    source = RateLimitedSource()
    archive = _enabled_reconciliation_archive(store, source, now, timer)

    await archive.async_start()
    await _wait_for_archive_state(archive, HistoryArchiveState.BACKOFF)
    assert [slot[0] for slot in timer.active] == [timedelta(hours=24)]

    now[0] = datetime(2026, 8, 6, tzinfo=UTC)
    timer.fire_next()
    await _wait_for_remote_requests(source, 20)

    assert archive.status.state is HistoryArchiveState.IDLE
    assert "safe_error_class" not in archive.status.as_attributes()
    await archive.async_stop()


@pytest.mark.asyncio
async def test_archive_rate_limit_backoff_survives_restart_and_expires_once() -> None:
    """Restart preserves an active backoff and expiry permits one normal attempt."""
    store = FakeStore(
        {
            "schema_version": 1,
            "account_key": "opaque-account-key-1234567890",
            "completed_dates": [],
            "reconciliation": {},
            "reconciliation_family_presence": {},
            "hrv_summaries": {},
            "numeric_source_date_index": [],
            "numeric_source_date_dates": {},
            "numeric_source_date_pending": {},
            "numeric_source_date_tombstones": {},
            "numeric_source_date_outbox": {},
            "numeric_source_date_confirmed": {},
            "presence": {},
            "sleep_index": {},
            "event_index": {},
            "activity_index": {},
            "archive_backoff_until": "2026-08-06T00:00:00+00:00",
        }
    )
    source = ReconciliationSource({})
    before_expiry = [datetime(2026, 8, 5, 23, 59, tzinfo=UTC)]
    before_timer = DeterministicTimer()
    before = _enabled_reconciliation_archive(
        store, source, before_expiry, before_timer
    )

    await before.async_start()
    await _wait_for_archive_state(before, HistoryArchiveState.BACKOFF)
    assert source.requested == []
    assert [slot[0] for slot in before_timer.active] == [timedelta(minutes=1)]
    before_timer.fire_next()
    await asyncio.sleep(0)
    assert source.requested == []
    assert [slot[0] for slot in before_timer.active] == [timedelta(minutes=1)]
    await before.async_stop()

    after_expiry = [datetime(2026, 8, 6, tzinfo=UTC)]
    after_timer = DeterministicTimer()
    after = _enabled_reconciliation_archive(store, source, after_expiry, after_timer)
    await after.async_start()
    await _wait_for_remote_requests(source, 19)
    first_sync_requests = len(source.requested)
    assert after.status.state is HistoryArchiveState.IDLE
    assert first_sync_requests == 19
    assert [slot[0] for slot in after_timer.active] == [timedelta(minutes=15)]
    await after.async_stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "should_reauth"),
    ((GarminAPIError("forbidden", 403), False), (GarminAuthError("expired"), True)),
)
async def test_archive_auth_classification_requires_genuine_account_failure(
    error: GarminConnectError, should_reauth: bool
) -> None:
    """Ordinary endpoint authorization errors do not trigger account reauth."""
    reauth = AsyncMock()
    timer = DeterministicTimer()
    store = FakeStore()

    class ErrorSource(ReconciliationSource):
        async def async_fetch_details(self, target_date: date, metric: str) -> object:
            if metric == "heart_rate":
                self.requested.append(target_date)
                raise error
            return await super().async_fetch_details(target_date, metric)

    archive = _enabled_reconciliation_archive(
        store,
        ErrorSource({}),
        [datetime(2026, 8, 5, tzinfo=UTC)],
        timer,
    )
    archive._entry.async_start_reauth = reauth

    await archive.async_start()
    await _wait_for_archive_state(archive, HistoryArchiveState.FAILED)

    if should_reauth:
        reauth.assert_awaited_once_with(archive._hass)
        assert archive.status.error_type == "reauth_required"
    else:
        reauth.assert_not_awaited()
        assert archive.status.error_type == "garmin_client_error"
    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.as_attributes()["safe_error_class"] in {
        "garmin_client_error",
        "reauth_required",
    }
    await archive.async_stop()


async def test_restart_restores_one_cadence_without_replaying_missed_ticks() -> None:
    """Restart schedules a fresh cadence and does not replay downtime."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-08-04",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    store = FakeStore()
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    now = [datetime(2026, 8, 4, 12, tzinfo=UTC)]

    first_timer = DeterministicTimer()
    first = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(store),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        clock=lambda: now[0],
        timer_factory=first_timer.call_later,
    )
    await first.async_start()
    first_sync_task = first._first_sync_task
    assert first_sync_task is not None
    await first_sync_task
    assert len(first_timer.active) == 1
    await first.async_stop()

    now[0] = datetime(2026, 8, 6, 12, tzinfo=UTC)
    second_timer = DeterministicTimer()
    restarted = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(store),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        clock=lambda: now[0],
        timer_factory=second_timer.call_later,
    )
    await restarted.async_start()
    restarted_first_sync = restarted._first_sync_task
    assert restarted_first_sync is not None
    await restarted_first_sync

    assert len(second_timer.active) == 1
    assert [slot[0] for slot in second_timer.active] == [timedelta(minutes=15)]
    assert source.async_fetch.await_count == 30
    assert {call.args[0] for call in source.async_fetch.await_args_list[:15]} == {date(2026, 8, 4)}
    assert {call.args[0] for call in source.async_fetch.await_args_list[15:]} == {date(2026, 8, 6)}
    await restarted.async_stop()


async def test_cycle_ticks_coalesce_while_one_cycle_is_running() -> None:
    """A slow cycle admits one follow-up, never a timer backlog."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    timer = DeterministicTimer()
    cycle_started = asyncio.Event()
    release_cycle = asyncio.Event()
    follow_up_done = asyncio.Event()
    calls = 0

    async def fetch(_target: date, _metric: str):
        nonlocal calls
        calls += 1
        if calls == 16:
            cycle_started.set()
            await release_cycle.wait()
        if calls == 45:
            follow_up_done.set()
        return ()

    source = MagicMock()
    source.async_fetch = AsyncMock(side_effect=fetch)
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        timer_factory=timer.call_later,
    )

    await archive.async_start()
    first_sync_task = archive._first_sync_task
    assert first_sync_task is not None
    await first_sync_task
    timer.fire_next()
    await asyncio.wait_for(cycle_started.wait(), timeout=0.1)

    timer.fire_next()
    await asyncio.sleep(0)
    timer.fire_next()
    await asyncio.sleep(0)
    timer.fire_next()
    await asyncio.sleep(0)
    assert calls == 16

    release_cycle.set()
    await asyncio.wait_for(follow_up_done.wait(), timeout=0.1)
    assert calls == 45
    await asyncio.sleep(0)
    assert calls == 45
    assert len(timer.active) == 1
    await archive.async_stop()


async def test_stop_cancels_cycle_and_future_wakeup() -> None:
    """Disablement/unload cancels an active cycle and its next tick."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    timer = DeterministicTimer()
    cycle_started = asyncio.Event()
    cycle_cancelled = asyncio.Event()
    calls = 0

    async def fetch(_target: date, _metric: str):
        nonlocal calls
        calls += 1
        if calls == 16:
            cycle_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cycle_cancelled.set()
                raise
        return ()

    source = MagicMock()
    source.async_fetch = AsyncMock(side_effect=fetch)
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        timer_factory=timer.call_later,
    )

    await archive.async_start()
    first_sync_task = archive._first_sync_task
    assert first_sync_task is not None
    await first_sync_task
    timer.fire_next()
    await asyncio.wait_for(cycle_started.wait(), timeout=0.1)

    await archive.async_stop()

    assert cycle_cancelled.is_set()
    assert timer.active == []


async def test_first_sync_requests_completed_archive_activation_date() -> None:
    """Enablement retries today even when a prior repair checkpoint exists."""
    hass = _hass()
    target_date = date(2026, 8, 4)
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    source = MagicMock()
    requested_dates: list[date] = []

    async def fetch(target: date, _metric: str):
        requested_dates.append(target)
        return ()

    source.async_fetch = AsyncMock(side_effect=fetch)
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    store = FakeStore(
        {
            "account_key": "opaque-account-key-1234567890",
            "schema_version": 1,
            "completed_dates": [target_date.isoformat()],
            "hrv_summaries": {},
            "presence": {},
            "sleep_index": {},
            "event_index": {},
            "activity_index": {},
        }
    )
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(store),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, tzinfo=UTC),
    ):
        await archive.async_start()
        first_sync_task = archive._first_sync_task
        assert first_sync_task is not None
        await first_sync_task

    assert requested_dates
    assert set(requested_dates) == {target_date}


async def test_disabled_start_does_not_request_an_archive_date() -> None:
    """Disabled archive setup retains infrastructure without Garmin work."""
    hass = _hass()
    entry = _entry(data={"history_account_key": "opaque-account-key-1234567890"})
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    source.async_fetch_details = None
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
    )

    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.DISABLED
    source.async_fetch.assert_not_awaited()


async def test_failed_first_sync_fails_closed_without_background_work() -> None:
    """A required history write failure stops the first archive attempt."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    source = MagicMock()
    source.async_fetch = AsyncMock(
        return_value=(
            NormalizedSample(
                datetime(2026, 8, 4, tzinfo=UTC),
                date(2026, 8, 4),
                "2026-08-04T00:00:00Z",
                72.0,
            ),
        )
    )
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(
        return_value=RecorderWriteOutcome(0, "failed", "recorder_write")
    )
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, tzinfo=UTC),
    ):
        await archive.async_start()
        first_sync_task = archive._first_sync_task
        assert first_sync_task is not None
        await first_sync_task

    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "recorder_write"


async def test_stop_cancels_an_in_flight_first_sync() -> None:
    """Startup returns while unload can cancel the bounded first request."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    entry.runtime_data = SimpleNamespace(
        core=SimpleNamespace(client=object()), request_gate=object()
    )
    started = asyncio.Event()

    async def fetch(_target: date, _metric: str):
        started.set()
        await asyncio.Event().wait()
        return ()

    source = MagicMock()
    source.async_fetch = AsyncMock(side_effect=fetch)
    source.async_fetch_details = None
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(FakeStore()),
        source_factory=lambda *args: source,
    )

    await asyncio.wait_for(archive.async_start(), timeout=0.1)
    await started.wait()
    await archive.async_stop()


async def test_enablement_uses_configured_timezone_for_archive_activation_date() -> None:
    """Archive Activation Date follows HA timezone across UTC midnight."""
    hass = _hass()
    entry = _entry(data={CONF_ARCHIVE_PREVIOUSLY_ENABLED: False})
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())

    with (
        patch(
            "custom_components.garmin_connect.history.dt_util.utcnow",
            return_value=datetime(2026, 8, 3, 23, 30, tzinfo=UTC),
        ),
        patch(
            "custom_components.garmin_connect.history.dt_util.DEFAULT_TIME_ZONE",
            ZoneInfo("Asia/Taipei"),
        ),
    ):
        await _archive(hass, entry, checker).async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-04"


async def test_reenablement_replaces_activation_date_without_starting_backfill() -> None:
    """Re-enabling starts a new prospective boundary and preserves old data."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-07-01",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)

    with patch("custom_components.garmin_connect.history.dt_util.utcnow", return_value=now):
        archive = _archive(hass, entry, checker)
        await archive.async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert persisted[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True
    assert archive.async_sync_range is not None


@pytest.mark.parametrize(
    "last_enabled",
    [pytest.param(None, id="marker-missing"), pytest.param(False, id="marker-false")],
)
async def test_enablement_preserves_malformed_activation_date(
    last_enabled: bool | None,
) -> None:
    """A malformed boundary is never replaced during a new enablement."""
    hass = _hass()
    data = {
        "history_account_key": "opaque-account-key-1234567890",
        CONF_ARCHIVE_ACTIVATION_DATE: "not-a-date",
    }
    if last_enabled is not None:
        data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] = last_enabled
    entry = _entry(data=data)
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 1, 30, tzinfo=UTC),
    ):
        archive = _archive(hass, entry, checker)
        await archive.async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "not-a-date"
    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "activation_date_invalid"


@pytest.mark.parametrize("activation_date", [None, "not-a-date"])
async def test_enabled_archive_fails_closed_for_invalid_persisted_activation_date(
    activation_date: str | None,
) -> None:
    """A recorded enablement cannot silently lose its archive boundary."""
    hass = _hass()
    data = {
        "history_account_key": "opaque-account-key-1234567890",
        CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
    }
    if activation_date is not None:
        data[CONF_ARCHIVE_ACTIVATION_DATE] = activation_date
    entry = _entry(data=data)
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())

    archive = _archive(hass, entry, checker)
    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "activation_date_invalid"
    assert archive.activation_date is None
    hass.config_entries.async_update_entry.assert_not_called()


async def test_archive_lifecycle_persists_through_reload_restart_and_reenablement() -> None:
    """Reload preserves identity; re-enable sets a new Archive Activation Date."""
    hass = _hass()
    entry = _entry(data={CONF_ARCHIVE_PREVIOUSLY_ENABLED: False})
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    store = FakeStore()
    first_now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)

    with patch("custom_components.garmin_connect.history.dt_util.utcnow", return_value=first_now):
        first = _archive(hass, entry, checker, store)
        await first.async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    account_key = persisted["history_account_key"]
    entry.data = persisted
    hass.config_entries.async_update_entry.reset_mock()

    restarted = _archive(hass, entry, checker, FakeStore(data=store.data))
    await restarted.async_start()

    assert restarted.activation_date == date(2026, 8, 3)
    assert restarted.status.state is HistoryArchiveState.IDLE
    assert hass.config_entries.async_update_entry.call_count == 0

    entry.options = {CONF_ARCHIVE_ENABLED: False}
    disabled = _archive(hass, entry, checker, FakeStore(data=store.data))
    await disabled.async_start()
    disabled_persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]

    assert disabled.status.state is HistoryArchiveState.DISABLED
    assert disabled.activation_date == date(2026, 8, 3)
    assert disabled_persisted["history_account_key"] == account_key
    assert disabled_persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert disabled_persisted[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is False

    entry.data = disabled_persisted
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    second_now = datetime(2026, 8, 10, 1, 30, tzinfo=UTC)
    with patch("custom_components.garmin_connect.history.dt_util.utcnow", return_value=second_now):
        reenabled = _archive(hass, entry, checker, FakeStore(data=store.data))
        await reenabled.async_start()

    reenabled_persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert reenabled.status.state is HistoryArchiveState.IDLE
    assert reenabled.activation_date == date(2026, 8, 10)
    assert reenabled_persisted["history_account_key"] == account_key
    assert reenabled_persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-10"
    assert reenabled_persisted[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True


async def test_disablement_preserves_activation_date_and_manual_repair() -> None:
    """Disabling stops automatic work but leaves archive queries and repair intact."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-07-01",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: False}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())

    archive = _archive(hass, entry, checker)
    await archive.async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-07-01"
    assert persisted[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is False
    assert archive.archive_enabled is False
    assert archive.activation_date == date(2026, 7, 1)


async def test_disabled_archive_keeps_query_surface_available() -> None:
    """Disablement does not hide retained archive queries."""
    hass = _hass()
    entry = _entry(
        data={
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-07-01",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: False}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    archive = _archive(hass, entry, checker)
    await archive.async_start()

    assert archive.get_history_presence(date(2026, 7, 1), date(2026, 7, 1)) == {}
    assert await archive.async_get_calendar_events(
        "sleep", date(2026, 7, 1), date(2026, 7, 1)
    ) == ()
    sensor = GarminHistoryStatusSensor(archive, "entry-1")
    assert sensor.native_value == "disabled"
    assert sensor.extra_state_attributes["activation_date"] == "2026-07-01"


@pytest.mark.asyncio
async def test_manual_repair_reopens_settled_date_while_archive_is_disabled() -> None:
    """Manual Repair must force one requested date past its settled checkpoint."""
    target = date(2026, 7, 24)
    store = _reconciliation_store(target, state="settled", has_records=True, outcome="records")
    store.data["completed_dates"] = [target.isoformat()]
    source = ReconciliationSource({target: (72.0,)})
    archive = _manual_repair_archive(
        store,
        source,
        activation_date="2026-08-01",
        previously_enabled=True,
    )

    await archive.async_start()
    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written"
    assert source.requested
    assert set(source.requested) == {target}
    assert archive.archive_enabled is False
    assert archive.activation_date == date(2026, 8, 1)
    assert archive._cycle_timer_cancel is None
    assert archive._backfill is None
    assert archive.get_history_presence(target, target)[target.isoformat()][
        "heart_rate"
    ] == "present"
    assert store.data["reconciliation"][target.isoformat()]["state"] == "open"

    await archive.async_stop()
    restarted = _manual_repair_archive(
        store,
        source,
        activation_date="2026-08-01",
        previously_enabled=True,
    )
    await restarted.async_start()
    assert restarted.get_history_presence(target, target)[target.isoformat()][
        "heart_rate"
    ] == "present"
    assert store.data["reconciliation"][target.isoformat()]["state"] == "open"
    await restarted.async_stop()


@pytest.mark.asyncio
async def test_manual_repair_accepts_one_and_31_days_but_rejects_32_before_requests() -> None:
    """Manual Repair validates the inclusive bound before touching Garmin."""
    source = ReconciliationSource({date(2026, 7, 24): (72.0,)})
    store = _reconciliation_store(date(2026, 7, 24))
    archive = _manual_repair_archive(store, source)
    await archive.async_start()

    start = date(2026, 7, 24)
    too_large = await archive.async_sync_range(start, start + timedelta(days=31))
    assert too_large == HistorySyncReport(outcome="invalid", error_type="range_too_large")
    assert source.requested == []

    one_day = await archive.async_sync_range(start, start)
    assert one_day.outcome == "written"
    assert set(source.requested) == {start}

    source.requested.clear()
    thirty_one = await archive.async_sync_range(start, start + timedelta(days=30))
    assert thirty_one.outcome == "written"
    assert len(source.requested) == 31 * 19
    assert set(source.requested) == {
        start + timedelta(days=offset) for offset in range(31)
    }


@pytest.mark.asyncio
async def test_manual_repair_keeps_per_date_results_when_a_later_date_fails() -> None:
    """A failed range operation cannot overwrite or invent date outcomes."""
    first = date(2026, 7, 24)
    second = first + timedelta(days=1)
    third = second + timedelta(days=1)
    store = _reconciliation_store(first)
    store.data["reconciliation"].update(
        {
            second.isoformat(): {
                "state": "open",
                "fingerprint": None,
                "has_records": False,
                "outcome": "empty",
            },
            third.isoformat(): {
                "state": "settled",
                "fingerprint": "a" * 64,
                "has_records": True,
                "outcome": "records",
            },
        }
    )

    class FailLaterDateSource(ReconciliationSource):
        def __init__(self) -> None:
            super().__init__({first: (72.0,), second: (73.0,)})
            self.fail_second = True
            self.first_event = normalize_health_events(
                {
                    "events": [
                        {
                            "source": "garmin",
                            "type": "daily_event",
                            "category": "health",
                            "occurrenceTime": "2026-07-24T01:00:00+00:00",
                        }
                    ]
                },
                first,
            )[0]

        async def async_fetch_details(self, target: date, metric: str) -> object:
            if target == second and self.fail_second:
                self.requested.append(target)
                raise RuntimeError("second date unavailable")
            if target == first and metric == "health_events_daily":
                self.requested.append(target)
                return (self.first_event,)
            return await super().async_fetch_details(target, metric)

    source = FailLaterDateSource()
    archive = _manual_repair_archive(store, source)
    await archive.async_start()

    report = await archive.async_sync_range(first, third)

    assert report.outcome == "failed"
    assert source.requested.count(first) == 19
    assert source.requested.count(second) == 19
    assert source.requested.count(third) == 0
    assert archive.get_history_presence(first, third)[first.isoformat()][
        "heart_rate"
    ] == "present"
    assert archive.get_history_presence(first, third)[second.isoformat()][
        "heart_rate"
    ] == "failed"
    assert third.isoformat() not in archive.get_history_presence(first, third)
    assert store.data["event_index"] == {"2026": [source.first_event.logical_id]}
    calendar_events = await archive.async_get_calendar_events("health", first, third)
    assert [event.summary for event in calendar_events] == ["health"]
    assert store.data["reconciliation"][first.isoformat()]["outcome"] == "records"
    assert store.data["reconciliation"][second.isoformat()]["state"] == "open"
    assert store.data["reconciliation"][second.isoformat()]["outcome"] == "failed"
    assert store.data["reconciliation"][third.isoformat()] == {
        "state": "settled",
        "fingerprint": "a" * 64,
        "has_records": True,
        "outcome": "records",
    }

    source.fail_second = False
    retry = await archive.async_sync_range(second, second)

    assert retry.outcome == "written"
    assert source.requested.count(second) == 38
    assert source.requested.count(third) == 0
    assert store.data["reconciliation"][second.isoformat()]["state"] == "open"
    assert store.data["reconciliation"][second.isoformat()]["outcome"] == "records"
    assert archive.get_history_presence(second, second)[second.isoformat()][
        "heart_rate"
    ] == "present"


async def test_different_entries_get_different_account_keys() -> None:
    """Account identity is random and cannot cross config entries."""
    hass = _hass()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    entry_one = _entry(entry_id="entry-1")
    entry_two = _entry(entry_id="entry-2")

    await _archive(hass, entry_one, checker).async_start()
    first_key = hass.config_entries.async_update_entry.call_args.kwargs["data"][
        "history_account_key"
    ]
    await _archive(hass, entry_two, checker).async_start()
    second_key = hass.config_entries.async_update_entry.call_args.kwargs["data"][
        "history_account_key"
    ]

    assert first_key != second_key


async def test_recorder_incompatibility_disables_history_without_writes() -> None:
    """Recorder incompatibility is observable and fail-closed."""
    hass = _hass()
    checker = FakeRecorderChecker(
        RecorderCompatibilityResult.incompatible_result("recorder_signature")
    )
    store = FakeStore()
    archive = _archive(hass, _entry(), checker, store)

    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "recorder_signature"
    assert checker.check.await_count == 1
    assert store.saved


async def test_store_failure_does_not_raise_or_probe_recorder() -> None:
    """Store startup failure leaves current-value setup independent."""
    hass = _hass()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())

    def failing_store(*args, **kwargs):
        raise OSError("private storage unavailable")

    archive = GarminHistoryArchive(
        hass,
        _entry(),
        recorder_checker=checker,
        store_factory=failing_store,
    )

    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.FAILED
    assert archive.status.error_type == "store_initialization"
    checker.check.assert_not_awaited()


async def test_status_sensor_exposes_only_privacy_safe_placeholders() -> None:
    """The status entity never exposes the opaque identity or health values."""
    hass = _hass()
    checker = FakeRecorderChecker(
        RecorderCompatibilityResult.incompatible_result("missing_recorder_api")
    )
    archive = _archive(hass, _entry(), checker)
    await archive.async_start()

    sensor = GarminHistoryStatusSensor(archive, "entry-1")

    assert sensor.native_value == "failed"
    assert sensor.extra_state_attributes == {
        "archive_state": "failed",
        "safe_error_class": "missing_recorder_api",
    }
    assert "history_account_key" not in sensor.extra_state_attributes


async def test_status_sensor_receives_active_archive_notifications() -> None:
    """The status entity is notified when the archive lifecycle changes."""
    hass = _hass()
    checker = FakeRecorderChecker(
        RecorderCompatibilityResult.incompatible_result("missing_recorder_api")
    )
    archive = _archive(hass, _entry(), checker)
    sensor = GarminHistoryStatusSensor(archive, "entry-1")
    writes = MagicMock()
    sensor.async_write_ha_state = writes
    remove = archive.add_status_listener(sensor.async_write_ha_state)

    await archive.async_start()

    writes.assert_called_once_with()
    remove()


async def test_stop_is_idempotent() -> None:
    """Unload can stop an archive more than once without leaving work behind."""
    hass = _hass()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    archive = _archive(hass, _entry(), checker)
    await archive.async_start()

    await archive.async_stop()
    await archive.async_stop()

    assert archive.status.state is HistoryArchiveState.DISABLED


async def test_recorder_compatibility_uses_real_scratch_recorder() -> None:
    """The non-writing compatibility check must pass a disposable Recorder."""
    with tempfile.TemporaryDirectory(prefix="ha-garmin-recorder-") as config_dir:
        hass = HomeAssistant(config_dir)
        hass.config_entries = ConfigEntries(hass, {})
        loader.async_setup(hass)
        async_initialize_recorder(hass)
        configured = await async_setup_component(
            hass,
            "recorder",
            {
                "recorder": {
                    "db_url": f"sqlite:///{Path(config_dir) / 'scratch.db'}",
                    "commit_interval": 0,
                    "auto_purge": False,
                }
            },
        )
        assert configured
        await hass.async_start()
        try:
            result = await HomeAssistantRecorderCompatibility(hass).async_check()
        finally:
            await hass.async_stop()

    assert result == RecorderCompatibilityResult.compatible_result()
