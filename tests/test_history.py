"""Tests for the Garmin history archive lifecycle seam."""

from __future__ import annotations

import asyncio
import tempfile
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from homeassistant import loader
from homeassistant.config_entries import ConfigEntries
from homeassistant.core import HomeAssistant
from homeassistant.helpers.recorder import async_initialize_recorder
from homeassistant.setup import async_setup_component

import custom_components.garmin_connect.const as const
from custom_components.garmin_connect.const import (
    CONF_ARCHIVE_ACTIVATION_DATE,
    CONF_ARCHIVE_ENABLED,
    CONF_ARCHIVE_PREVIOUSLY_ENABLED,
)
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    HomeAssistantRecorderCompatibility,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_recorder import RecorderWriteOutcome
from custom_components.garmin_connect.history_sensor import GarminHistoryStatusSensor
from custom_components.garmin_connect.history_source import GarminHistorySource, NormalizedSample
from custom_components.garmin_connect.request_gate import (
    GarminRequestGate,
    GarminRequestPriority,
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


async def test_enablement_persists_local_activation_date() -> None:
    """Enabling archival records the current Home Assistant local date."""
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


async def test_enabled_start_syncs_only_the_current_local_date() -> None:
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
    """A successful activation schedules only current-local-date cycles."""
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

    first_timer = DeterministicTimer()
    first = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(store),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        timer_factory=first_timer.call_later,
    )
    await first.async_start()
    first_sync_task = first._first_sync_task
    assert first_sync_task is not None
    await first_sync_task
    assert len(first_timer.active) == 1
    await first.async_stop()

    second_timer = DeterministicTimer()
    restarted = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=FakeRecorderChecker(RecorderCompatibilityResult.compatible_result()),
        store_factory=_store_factory(store),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
        timer_factory=second_timer.call_later,
    )
    await restarted.async_start()
    restarted_first_sync = restarted._first_sync_task
    assert restarted_first_sync is not None
    await restarted_first_sync

    assert len(second_timer.active) == 1
    assert [slot[0] for slot in second_timer.active] == [timedelta(minutes=15)]
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
    assert calls == 16

    release_cycle.set()
    await asyncio.wait_for(follow_up_done.wait(), timeout=0.1)
    assert calls == 45
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


async def test_first_sync_requests_completed_current_local_date() -> None:
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


async def test_enablement_uses_configured_local_date_across_utc_midnight() -> None:
    """Enablement uses HA's local calendar date, not the UTC date."""
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
    """Reload/restart preserves identity, while re-enable establishes a new date."""
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
    assert set(sensor.extra_state_attributes) == {
        "recorder_target",
        "archive_state",
        "activation_date",
        "current_date",
        "processed_dates",
        "record_count",
        "error_type",
        "queued_count",
        "completed_count",
        "next_eligible_run",
        "last_success",
        "backoff_until",
        "safe_error_class",
    }
    assert "history_account_key" not in sensor.extra_state_attributes


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
