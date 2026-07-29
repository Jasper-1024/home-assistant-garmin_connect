"""Tests for the Garmin history archive lifecycle seam."""

from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from homeassistant import loader
from homeassistant.config_entries import ConfigEntries
from homeassistant.core import HomeAssistant
from homeassistant.helpers.recorder import async_initialize_recorder
from homeassistant.setup import async_setup_component

from custom_components.garmin_connect.const import (
    CONF_ARCHIVE_ACTIVATION_DATE,
    CONF_ARCHIVE_ENABLED,
    CONF_ARCHIVE_LAST_ENABLED,
)
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    HomeAssistantRecorderCompatibility,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_sensor import GarminHistoryStatusSensor


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
    assert first.status.state is HistoryArchiveState.IDLE
    assert first_store.saved[0]["account_key"] == account_key

    entry.data = persisted_data
    second = _archive(hass, entry, checker, FakeStore(data=first_store.data))
    await second.async_start()

    assert second.status.state is HistoryArchiveState.IDLE
    assert hass.config_entries.async_update_entry.call_count == 1


async def test_start_keeps_historical_backfill_dormant() -> None:
    """Normal archive setup must not construct the legacy backfill scheduler."""
    hass = _hass()
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    archive = _archive(hass, _entry(), checker)

    with patch("custom_components.garmin_connect.history.BackfillScheduler") as backfill:
        await archive.async_start()

    backfill.assert_not_called()


async def test_enablement_persists_local_activation_date() -> None:
    """Enabling archival records the current Home Assistant local date."""
    hass = _hass()
    entry = _entry(data={CONF_ARCHIVE_LAST_ENABLED: False})
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)

    with patch("custom_components.garmin_connect.history.dt_util.now", return_value=now):
        await _archive(hass, entry, checker).async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert persisted[CONF_ARCHIVE_LAST_ENABLED] is True


async def test_reenablement_replaces_activation_date_without_starting_backfill() -> None:
    """Re-enabling starts a new prospective boundary and preserves old data."""
    hass = _hass()
    entry = _entry(
        data={
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-07-01",
            CONF_ARCHIVE_LAST_ENABLED: False,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())
    now = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)

    with patch("custom_components.garmin_connect.history.dt_util.now", return_value=now):
        archive = _archive(hass, entry, checker)
        await archive.async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert persisted[CONF_ARCHIVE_LAST_ENABLED] is True
    assert archive.async_sync_range is not None


async def test_disablement_preserves_activation_date_and_manual_repair() -> None:
    """Disabling stops automatic work but leaves archive queries and repair intact."""
    hass = _hass()
    entry = _entry(
        data={
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-07-01",
            CONF_ARCHIVE_LAST_ENABLED: True,
        }
    )
    entry.options = {CONF_ARCHIVE_ENABLED: False}
    checker = FakeRecorderChecker(RecorderCompatibilityResult.compatible_result())

    archive = _archive(hass, entry, checker)
    await archive.async_start()

    persisted = hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert persisted[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-07-01"
    assert persisted[CONF_ARCHIVE_LAST_ENABLED] is False
    assert archive.archive_enabled is False
    assert archive.activation_date == date(2026, 7, 1)


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

    assert archive.status.state is HistoryArchiveState.COMPATIBILITY_DISABLED
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

    assert sensor.native_value == "compatibility-disabled"
    assert set(sensor.extra_state_attributes) == {
        "recorder_target",
        "archive_state",
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

    assert archive.status.state is HistoryArchiveState.IDLE


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
