"""Tests for Garmin Connect integration setup and migration."""

import asyncio
from contextlib import ExitStack
from datetime import UTC, date, datetime, timedelta
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant import loader
from homeassistant.config_entries import (
    SOURCE_REAUTH,
    SOURCE_RECONFIGURE,
    SOURCE_USER,
    ConfigEntries,
    ConfigEntry,
    ConfigEntryNotReady,
    ConfigEntryState,
)
from homeassistant.core import HomeAssistant

from custom_components.garmin_connect import (
    _ENTRY_UPDATE_STATES,
    _migrate_entity_unique_ids,
    async_migrate_entry,
    async_options_update_listener,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.garmin_connect.const import (
    CONF_ARCHIVE_ACTIVATION_DATE,
    CONF_ARCHIVE_ENABLED,
    CONF_ARCHIVE_PREVIOUSLY_ENABLED,
    CONF_CLIENT_ID,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_TOKEN,
    DOMAIN,
)
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_recorder import RecorderWriteOutcome
from custom_components.garmin_connect.request_gate import (
    GarminRequestGate,
    GarminRequestPriority,
)

from .conftest import ENTRY_DATA

_COORD_TARGETS = [
    "custom_components.garmin_connect.CoreCoordinator",
    "custom_components.garmin_connect.ActivityCoordinator",
    "custom_components.garmin_connect.TrainingCoordinator",
    "custom_components.garmin_connect.BodyCoordinator",
    "custom_components.garmin_connect.GoalsCoordinator",
    "custom_components.garmin_connect.GearCoordinator",
    "custom_components.garmin_connect.BloodPressureCoordinator",
    "custom_components.garmin_connect.MenstrualCoordinator",
    "custom_components.garmin_connect.NutritionCoordinator",
]


def _coord_mock() -> MagicMock:
    """Return a coordinator mock with async methods stubbed."""
    c = MagicMock()
    c.async_config_entry_first_refresh = AsyncMock()
    c.async_refresh = AsyncMock()
    c.data = {}
    return c


def _config_entry_mock(**kwargs) -> MagicMock:
    """Return a config-entry double with HA-managed task creation."""
    entry = MagicMock(**kwargs)
    entry.async_create_task.side_effect = (
        lambda _hass, target, name: asyncio.create_task(target, name=name)
    )
    return entry


def _configure_entry_task_factory(hass: MagicMock) -> None:
    """Make a mocked hass run tasks created through a real ConfigEntry."""
    hass.async_create_task_internal.side_effect = (
        lambda target, name, _eager_start: asyncio.create_task(target, name=name)
    )


async def _await_archive_start_task(entry: object) -> None:
    """Wait for a setup-created archive task when it has a finite startup."""
    state = _ENTRY_UPDATE_STATES.get(entry)
    assert state is not None
    task = state.archive_start_task
    if task is not None:
        await task


def _stack_coordinators(stack: ExitStack, coord: MagicMock) -> list[MagicMock]:
    """Push patches for all 9 coordinator constructors onto an ExitStack."""
    return [
        stack.enter_context(patch(target, return_value=coord)) for target in _COORD_TARGETS
    ]


def _real_config_entry(entry_id: str = "entry-transaction") -> ConfigEntry:
    """Build a ConfigEntry for lifecycle transaction tests."""
    return ConfigEntry(
        version=2,
        minor_version=1,
        domain=DOMAIN,
        title="Garmin account",
        data=dict(ENTRY_DATA),
        options={},
        source=SOURCE_USER,
        unique_id="garmin-account",
        discovery_keys=MappingProxyType({}),
        subentries_data=None,
        entry_id=entry_id,
    )


class _LifecycleTimer:
    """Deterministic timer used through the archive's normal timer seam."""

    def __init__(self) -> None:
        self.slots: list[list[object]] = []

    def call_later(self, delay: timedelta, callback) -> object:
        slot = [delay, callback, True]
        self.slots.append(slot)

        def cancel() -> None:
            slot[2] = False

        return cancel

    @property
    def active(self) -> list[list[object]]:
        return [slot for slot in self.slots if slot[2]]

    def fire_next(self) -> None:
        for slot in self.slots:
            if slot[2]:
                slot[2] = False
                slot[1]()
                return
        raise AssertionError("no timer wakeup is scheduled")


class _RuntimeHistoryClient:
    """Empty Garmin endpoint double that exercises GarminHistorySource."""

    _base_url = "https://garmin.example"

    def __init__(self) -> None:
        self.requests = 0
        self.events: list[str] = []
        self.first_sync_done = asyncio.Event()
        self.block_cycle = False
        self.cycle_started = asyncio.Event()
        self.cycle_cancelled = asyncio.Event()
        self.release_cycle = asyncio.Event()

    async def _archive_request(self) -> dict:
        self.requests += 1
        self.events.append(f"archive-{self.requests}")
        if self.requests == 14:
            self.first_sync_done.set()
        if self.block_cycle and not self.cycle_started.is_set():
            self.cycle_started.set()
            try:
                await self.release_cycle.wait()
            except asyncio.CancelledError:
                self.cycle_cancelled.set()
                raise
        return {}

    async def get_user_profile(self) -> SimpleNamespace:
        return SimpleNamespace(display_name="athlete", profile_id=123456789)

    async def _request(self, *args, **_kwargs):
        result = await self._archive_request()
        if len(args) > 1 and "bodyBattery/events" in str(args[1]):
            return []
        return result

    async def _get_hrv_data_raw(self, _target: date) -> dict:
        return await self._archive_request()

    async def _get_sleep_data_raw(self, _target: date) -> dict:
        return await self._archive_request()

    async def _get_user_summary_raw(self, _target: date) -> dict:
        return await self._archive_request()

    async def get_activities(self, _offset: int, _limit: int) -> list:
        await self._archive_request()
        return []

    async def get_training_status(self, _target: date) -> dict:
        return await self._archive_request()


# ── Setup tests ───────────────────────────────────────────────────────────────


async def test_options_update_listener_reloads_config_entry() -> None:
    """Option changes use HA's config-entry reload scheduler."""
    entry = MagicMock(entry_id="entry-1")
    hass = MagicMock()
    schedule_reload = MagicMock()
    hass.config_entries.async_schedule_reload = schedule_reload

    await async_options_update_listener(hass, entry)

    schedule_reload.assert_called_once_with("entry-1")


async def test_options_update_persists_transition_before_reload() -> None:
    """Enablement date is persisted before setup can reload the entry."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {CONF_ARCHIVE_PREVIOUSLY_ENABLED: False}
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config_entries.async_update_entry = MagicMock(
        side_effect=lambda updated_entry, *, data: setattr(updated_entry, "data", data)
    )
    def schedule_reload(_entry_id: str) -> None:
        assert entry.data[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
        assert entry.data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True

    hass.config_entries.async_schedule_reload = MagicMock(side_effect=schedule_reload)

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 59, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)


async def test_options_update_does_not_rewrite_stable_enablement() -> None:
    """A reload of already-enabled archival does not create another transition."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {
        CONF_ARCHIVE_ACTIVATION_DATE: "2026-08-03",
        CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
    }
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config_entries.async_update_entry = MagicMock()
    schedule_reload = MagicMock()
    hass.config_entries.async_schedule_reload = schedule_reload

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, 0, 1, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)

    hass.config_entries.async_update_entry.assert_not_called()
    schedule_reload.assert_called_once_with("entry-1")


async def test_options_update_coalesces_in_flight_changes() -> None:
    """Several option transitions before setup starts schedule one reload."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {}
    entry.options = {CONF_SCAN_INTERVAL: 300}
    hass = MagicMock()
    schedule_reload = MagicMock()
    hass.config_entries.async_schedule_reload = schedule_reload

    await async_options_update_listener(hass, entry)
    entry.options = {CONF_SCAN_INTERVAL: 301}
    await async_options_update_listener(hass, entry)

    schedule_reload.assert_called_once_with("entry-1")


async def test_setup_persists_enablement_before_current_refresh_failure() -> None:
    """Setup records enablement before the current-value path can fail."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {
        **ENTRY_DATA,
        CONF_ARCHIVE_PREVIOUSLY_ENABLED: False,
    }
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config.country = "US"
    hass.config_entries.async_update_entry = MagicMock(
        side_effect=lambda updated_entry, *, data: setattr(updated_entry, "data", data)
    )

    coord = _coord_mock()
    coord.async_config_entry_first_refresh.side_effect = RuntimeError("current refresh failed")
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        with patch(
            "custom_components.garmin_connect.history.dt_util.utcnow",
            return_value=datetime(2026, 8, 3, 23, 30, tzinfo=UTC),
        ):
            with pytest.raises(RuntimeError, match="current refresh failed"):
                await async_setup_entry(hass, entry)

    assert entry.data[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert entry.data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True


async def test_enablement_date_survives_store_failure_after_midnight() -> None:
    """A Store startup failure cannot replace the transition date after midnight."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {CONF_ARCHIVE_PREVIOUSLY_ENABLED: False}
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config_entries.async_update_entry = MagicMock(
        side_effect=lambda updated_entry, *, data: setattr(updated_entry, "data", data)
    )
    def failing_store(*_args, **_kwargs):
        raise OSError("private storage unavailable")

    hass.config_entries.async_schedule_reload = MagicMock()

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 59, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)

    archive = GarminHistoryArchive(hass, entry, store_factory=failing_store)
    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, 0, 1, tzinfo=UTC),
    ):
        await archive.async_start()
    assert archive.status.error_type == "store_initialization"

    assert entry.data[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert entry.data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True


async def test_enablement_date_survives_recorder_startup_failure_after_midnight() -> None:
    """A Recorder startup failure cannot replace the transition date after midnight."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {CONF_ARCHIVE_PREVIOUSLY_ENABLED: False}
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config_entries.async_update_entry = MagicMock(
        side_effect=lambda updated_entry, *, data: setattr(updated_entry, "data", data)
    )
    store = MagicMock()
    store.async_load = AsyncMock(return_value=None)
    store.async_save = AsyncMock()
    checker = MagicMock()
    checker.async_check = AsyncMock(
        return_value=RecorderCompatibilityResult.incompatible_result("recorder_signature")
    )

    hass.config_entries.async_schedule_reload = MagicMock()

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 59, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)

    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=checker,
        store_factory=lambda *_args, **_kwargs: store,
    )
    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, 0, 1, tzinfo=UTC),
    ):
        await archive.async_start()
    assert archive.status.error_type == "recorder_signature"

    assert entry.data[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
    assert entry.data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True


async def test_real_config_entry_lifecycle_keeps_backfill_dormant_and_surfaces_visible(
    tmp_path,
) -> None:
    """Normal HA lifecycle paths cannot reach Historical Backfill."""
    hass = HomeAssistant(str(tmp_path))
    hass.config_entries = ConfigEntries(hass, {})
    loader.async_setup(hass)
    entry = ConfigEntry(
        version=2,
        minor_version=1,
        domain=DOMAIN,
        title="Garmin account",
        data=dict(ENTRY_DATA),
        options={CONF_ARCHIVE_ENABLED: False},
        source=SOURCE_USER,
        unique_id="garmin-account",
        discovery_keys=MappingProxyType({}),
        subentries_data=None,
        entry_id="entry-1",
    )
    coordinator = _coord_mock()

    async def assert_disabled_surfaces() -> None:
        archive = entry.runtime_data.history_archive
        assert archive.archive_enabled is False
        assert archive.status.state.value == "disabled"
        assert callable(archive.async_sync_range)
        assert archive.get_history_presence(date(2026, 7, 1), date(2026, 7, 1)) == {}
        assert (
            await archive.async_get_calendar_events("sleep", date(2026, 7, 1), date(2026, 7, 1))
            == ()
        )
        assert hass.services.has_service(DOMAIN, "sync_history")

    try:
        with ExitStack() as stack:
            stack.enter_context(
                patch("custom_components.garmin_connect.GarminAuth", return_value=MagicMock())
            )
            stack.enter_context(
                patch("custom_components.garmin_connect.GarminClient", return_value=MagicMock())
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.HomeAssistantRecorderCompatibility.async_check",
                    new=AsyncMock(return_value=MagicMock(compatible=True, error_type=None)),
                )
            )
            source = MagicMock()
            source.async_fetch = AsyncMock(return_value=())
            source.async_fetch_details = None
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.GarminHistorySource",
                    return_value=source,
                )
            )
            recorder = MagicMock()
            recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.GarminHistoryRecorder",
                    return_value=recorder,
                )
            )
            stack.enter_context(
                patch(
                    "homeassistant.helpers.recorder.get_instance",
                    return_value=recorder,
                )
            )
            backfill = stack.enter_context(
                patch("custom_components.garmin_connect.history.BackfillScheduler")
            )
            first_sync = stack.enter_context(
                patch.object(GarminHistoryArchive, "_async_run_first_sync", new=AsyncMock())
            )
            coordinator_constructors = _stack_coordinators(stack, coordinator)
            stack.enter_context(
                patch.object(hass.config_entries, "async_forward_entry_setups", new=AsyncMock())
            )
            stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_unload_platforms",
                    new=AsyncMock(return_value=True),
                )
            )

            await hass.config_entries.async_add(entry)
            await _await_archive_start_task(entry)
            await assert_disabled_surfaces()
            account_key = entry.data["history_account_key"]

            assert await hass.config_entries.async_reload(entry.entry_id)
            await _await_archive_start_task(entry)
            await assert_disabled_surfaces()
            assert entry.data["history_account_key"] == account_key

            assert await hass.config_entries.async_unload(entry.entry_id)
            assert entry.state is ConfigEntryState.NOT_LOADED
            assert await hass.config_entries.async_setup(entry.entry_id)
            await _await_archive_start_task(entry)
            await assert_disabled_surfaces()
            assert entry.data["history_account_key"] == account_key

            schedule_reload = stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_schedule_reload",
                    wraps=hass.config_entries.async_schedule_reload,
                )
            )
            reload_entry = stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_reload",
                    wraps=hass.config_entries.async_reload,
                )
            )

            constructed = []
            setup_reached = asyncio.Event()
            release_setup = asyncio.Event()

            def coordinator_constructor(*args, **_kwargs) -> MagicMock:
                configured_entry = args[1]
                configured = _coord_mock()
                configured.update_interval = timedelta(
                    seconds=configured_entry.options[CONF_SCAN_INTERVAL]
                )
                constructed.append(configured)
                return configured

            async def block_stale_setup() -> None:
                setup_reached.set()
                assert entry.update_listeners == []
                await release_setup.wait()

            def core_constructor(*args, **kwargs) -> MagicMock:
                configured = coordinator_constructor(*args, **kwargs)
                configured.async_config_entry_first_refresh = AsyncMock(
                    side_effect=block_stale_setup
                )
                return configured

            for constructor in coordinator_constructors:
                constructor.side_effect = coordinator_constructor
            coordinator_constructors[0].side_effect = core_constructor

            hass.config_entries.async_update_entry(
                entry,
                options={CONF_ARCHIVE_ENABLED: True, CONF_SCAN_INTERVAL: 601},
            )
            await asyncio.wait_for(setup_reached.wait(), timeout=0.1)

            # The first reload has unloaded its listener. This update must be
            # reconciled after setup rather than silently recorded as applied.
            hass.config_entries.async_update_entry(
                entry,
                options={CONF_ARCHIVE_ENABLED: True, CONF_SCAN_INTERVAL: 602},
            )
            release_setup.set()
            await hass.async_block_till_done()

            assert schedule_reload.call_count == 2
            assert entry.runtime_data.core.update_interval == timedelta(seconds=602)
            assert first_sync.await_count == 1
            assert len(constructed) == 18
            assert all(
                configured.update_interval == timedelta(seconds=601)
                for configured in constructed[:9]
            )
            assert all(
                configured.update_interval == timedelta(seconds=602)
                for configured in constructed[9:]
            )

            for constructor in coordinator_constructors:
                constructor.side_effect = None
                constructor.return_value = coordinator
            hass.config_entries.async_update_entry(
                entry,
                options={CONF_ARCHIVE_ENABLED: False, CONF_SCAN_INTERVAL: 602},
            )
            await hass.async_block_till_done()
            schedule_reload.reset_mock()
            reload_entry.reset_mock()
            first_sync.reset_mock()

            with patch(
                "custom_components.garmin_connect.history.dt_util.utcnow",
                return_value=datetime(2026, 8, 10, tzinfo=UTC),
            ):
                hass.config_entries.async_update_entry(entry, options={CONF_ARCHIVE_ENABLED: True})
                hass.config_entries.async_update_entry(
                    entry,
                    data={**entry.data, CONF_TOKEN: "token-before-options-listener"},
                )
                await hass.async_block_till_done()

            schedule_reload.assert_called_once_with(entry.entry_id)
            assert entry.data[CONF_TOKEN] == "token-before-options-listener"
            assert entry.runtime_data.history_archive.archive_enabled is True
            assert entry.runtime_data.history_archive.status.state.value == "idle"
            assert entry.runtime_data.history_archive.activation_date == date(2026, 8, 10)
            assert entry.data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True
            first_sync.assert_awaited_once()
            assert backfill.call_count == 0

            hass.config_entries.async_update_entry(
                entry, data={**entry.data, CONF_TOKEN: "refreshed-token"}
            )
            await hass.async_block_till_done()
            schedule_reload.assert_called_once()

            hass.config_entries.async_update_entry(
                entry, data={**entry.data, CONF_TOKEN: "second-refreshed-token"}
            )
            await hass.async_block_till_done()
            schedule_reload.assert_called_once()

            hass.config_entries.async_update_entry(
                entry,
                options={CONF_ARCHIVE_ENABLED: True, CONF_SCAN_INTERVAL: 602},
            )
            await hass.async_block_till_done()
            assert schedule_reload.call_count == 2

            from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

            for flow_source, finish_method, token, is_cn in (
                (SOURCE_REAUTH, "_async_finish_reauth", "reauth-token", False),
                (SOURCE_RECONFIGURE, "_async_finish_reconfigure", "reconfigure-token", True),
            ):
                flow = GarminConnectConfigFlow()
                flow.hass = hass
                flow.context = {"source": flow_source, "entry_id": entry.entry_id}
                flow._auth = MagicMock(
                    di_token=token,
                    di_refresh_token=f"{token}-refresh",
                    di_client_id="GARMIN_CONNECT_MOBILE_ANDROID_DI",
                )
                flow._is_cn = is_cn

                await getattr(flow, finish_method)()
                await hass.async_block_till_done()

            assert schedule_reload.call_count == 2
            assert reload_entry.await_count == 4

            assert await hass.config_entries.async_unload(entry.entry_id)
            assert entry.state is ConfigEntryState.NOT_LOADED
            assert entry._on_unload == []
    finally:
        hass.config_entries._store._async_cleanup_delay_listener()
        await hass.async_stop(force=True)


async def test_option_disablement_reload_cancels_recurring_archive_work(tmp_path) -> None:
    """Config-option disablement reload cancels active work and future wakeups."""
    hass = HomeAssistant(str(tmp_path))
    hass.config_entries = ConfigEntries(hass, {})
    loader.async_setup(hass)
    entry = ConfigEntry(
        version=2,
        minor_version=1,
        domain=DOMAIN,
        title="Garmin account",
        data={
            **ENTRY_DATA,
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-08-04",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        },
        options={CONF_ARCHIVE_ENABLED: True},
        source=SOURCE_USER,
        unique_id="garmin-account",
        discovery_keys=MappingProxyType({}),
        subentries_data=None,
        entry_id="entry-1",
    )
    coordinator = _coord_mock()
    client = _RuntimeHistoryClient()
    coordinator.client = client
    timer = _LifecycleTimer()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    try:
        with ExitStack() as stack:
            stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.GarminClient",
                    return_value=client,
                )
            )
            _stack_coordinators(stack, coordinator)
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.dt_util.utcnow",
                    return_value=datetime(2026, 8, 4, 12, tzinfo=UTC),
                )
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history._default_history_timer_factory",
                    new=timer.call_later,
                )
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.HomeAssistantRecorderCompatibility.async_check",
                    new=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result()),
                )
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.GarminHistoryRecorder",
                    return_value=recorder,
                )
            )
            stack.enter_context(
                patch("homeassistant.helpers.recorder.get_instance", return_value=recorder)
            )
            stack.enter_context(
                patch("custom_components.garmin_connect.async_setup_services", new=AsyncMock())
            )
            stack.enter_context(
                patch.object(hass.config_entries, "async_forward_entry_setups", new=AsyncMock())
            )
            stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_unload_platforms",
                    new=AsyncMock(return_value=True),
                )
            )

            await hass.config_entries.async_add(entry)
            await _await_archive_start_task(entry)
            first_archive = entry.runtime_data.history_archive
            await asyncio.wait_for(client.first_sync_done.wait(), timeout=1)
            await hass.async_block_till_done()
            assert timer.active, (first_archive.status, client.requests, client.events)

            client.block_cycle = True
            timer.fire_next()
            await asyncio.wait_for(client.cycle_started.wait(), timeout=1)

            requests_before_disable = client.requests
            hass.config_entries.async_update_entry(entry, options={CONF_ARCHIVE_ENABLED: False})
            await hass.async_block_till_done()

            assert client.cycle_cancelled.is_set()
            assert client.requests == requests_before_disable
            assert timer.active == []
            assert entry.runtime_data.history_archive is not first_archive
            assert entry.runtime_data.history_archive.archive_enabled is False
    finally:
        hass.config_entries._store._async_cleanup_delay_listener()
        await hass.async_stop(force=True)


async def test_runtime_archive_prioritizes_foreground_work_through_shared_gate(tmp_path) -> None:
    """A real archive cycle yields to a foreground request on runtime_data's gate."""
    hass = HomeAssistant(str(tmp_path))
    hass.config_entries = ConfigEntries(hass, {})
    loader.async_setup(hass)
    entry = ConfigEntry(
        version=2,
        minor_version=1,
        domain=DOMAIN,
        title="Garmin account",
        data={
            **ENTRY_DATA,
            "history_account_key": "opaque-account-key-1234567890",
            CONF_ARCHIVE_ACTIVATION_DATE: "2026-08-04",
            CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        },
        options={CONF_ARCHIVE_ENABLED: True},
        source=SOURCE_USER,
        unique_id="garmin-account",
        discovery_keys=MappingProxyType({}),
        subentries_data=None,
        entry_id="entry-1",
    )
    coordinator = _coord_mock()
    client = _RuntimeHistoryClient()
    coordinator.client = client
    timer = _LifecycleTimer()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))

    try:
        with ExitStack() as stack:
            stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.GarminClient",
                    return_value=client,
                )
            )
            _stack_coordinators(stack, coordinator)
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.dt_util.utcnow",
                    return_value=datetime(2026, 8, 4, 12, tzinfo=UTC),
                )
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history._default_history_timer_factory",
                    new=timer.call_later,
                )
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.HomeAssistantRecorderCompatibility.async_check",
                    new=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result()),
                )
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.GarminHistoryRecorder",
                    return_value=recorder,
                )
            )
            stack.enter_context(
                patch("homeassistant.helpers.recorder.get_instance", return_value=recorder)
            )
            stack.enter_context(
                patch("custom_components.garmin_connect.async_setup_services", new=AsyncMock())
            )
            stack.enter_context(
                patch.object(hass.config_entries, "async_forward_entry_setups", new=AsyncMock())
            )
            stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_unload_platforms",
                    new=AsyncMock(return_value=True),
                )
            )

            await hass.config_entries.async_add(entry)
            await _await_archive_start_task(entry)
            await asyncio.wait_for(client.first_sync_done.wait(), timeout=1)
            await hass.async_block_till_done()
            for _ in range(100):
                if timer.active:
                    break
                await asyncio.sleep(0)

            client.block_cycle = True
            timer.fire_next()
            await asyncio.wait_for(client.cycle_started.wait(), timeout=1)

            async def current_value_request() -> str:
                client.events.append("current")
                return "current-value"

            current_task = asyncio.create_task(
                entry.runtime_data.request_gate.async_request(
                    GarminRequestPriority.FOREGROUND,
                    current_value_request,
                )
            )
            await asyncio.sleep(0)
            assert not current_task.done()

            client.release_cycle.set()
            assert await asyncio.wait_for(current_task, timeout=1) == "current-value"
            await hass.async_block_till_done()

            current_index = client.events.index("current")
            next_archive_index = next(
                index
                for index, event in enumerate(client.events)
                if index > current_index and event.startswith("archive-")
            )
            assert current_index < next_archive_index
    finally:
        hass.config_entries._store._async_cleanup_delay_listener()
        await hass.async_stop(force=True)


async def test_setup_entry_success() -> None:
    """Test that a config entry sets up correctly and returns True."""
    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)

    coord = _coord_mock()
    with ExitStack() as stack:
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminAuth", return_value=MagicMock())
        )
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        stack.enter_context(
            patch(
                "custom_components.garmin_connect.async_setup_services",
                new=AsyncMock(),
            )
        )
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        result = await async_setup_entry(hass, entry)

    assert result is True
    assert entry.runtime_data is not None
    entry.async_create_task.assert_called_once()
    assert entry.async_create_task.call_args.args[0] is hass
    assert entry.async_create_task.call_args.kwargs == {
        "name": "garmin_connect archive startup"
    }


async def test_enabled_first_sync_does_not_delay_platform_forwarding() -> None:
    """Platform setup completes while the enabled first archive request waits."""
    entry = _config_entry_mock(entry_id="entry-1", title="Garmin account")
    entry.data = {
        **ENTRY_DATA,
        "history_account_key": "opaque-account-key-1234567890",
        CONF_ARCHIVE_PREVIOUSLY_ENABLED: True,
        CONF_ARCHIVE_ACTIVATION_DATE: "2026-08-04",
    }
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    coord = _coord_mock()
    started = asyncio.Event()
    release = asyncio.Event()

    class Source:
        async def async_fetch(self, _target, _metric):
            started.set()
            await release.wait()
            return ()

    store = MagicMock()
    store.async_load = AsyncMock(return_value=None)
    store.async_save = AsyncMock()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=MagicMock(
            async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())
        ),
        store_factory=lambda *args, **kwargs: store,
        source_factory=lambda *args: Source(),
        recorder_factory=lambda: recorder,
    )

    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminHistoryArchive", return_value=archive)
        )

        setup_task = asyncio.create_task(async_setup_entry(hass, entry))
        await asyncio.wait_for(setup_task, timeout=0.1)

    assert setup_task.result() is True
    hass.config_entries.async_forward_entry_setups.assert_awaited_once()
    await started.wait()
    await archive.async_stop()


async def test_options_changed_during_archive_start_reload_latest_once(tmp_path) -> None:
    """Archive startup observes updates without running a duplicate first sync."""
    hass = HomeAssistant(str(tmp_path))
    hass.config_entries = ConfigEntries(hass, {})
    loader.async_setup(hass)
    entry = ConfigEntry(
        version=2,
        minor_version=1,
        domain=DOMAIN,
        title="Garmin account",
        data={
            **ENTRY_DATA,
            "history_account_key": "opaque-account-key-1234567890",
        },
        options={CONF_ARCHIVE_ENABLED: False, CONF_SCAN_INTERVAL: 600},
        source=SOURCE_USER,
        unique_id="garmin-account",
        discovery_keys=MappingProxyType({}),
        subentries_data=None,
        entry_id="entry-archive-start-race",
    )
    coordinator = _coord_mock()
    archive_started = asyncio.Event()
    release_archive = asyncio.Event()
    block_archive_start = False
    constructed: list[MagicMock] = []
    original_archive_start = GarminHistoryArchive.async_start

    def coordinator_constructor(*args, **_kwargs) -> MagicMock:
        configured_entry = args[1]
        configured = _coord_mock()
        configured.update_interval = timedelta(
            seconds=configured_entry.options[CONF_SCAN_INTERVAL]
        )
        constructed.append(configured)
        return configured

    async def start_archive(archive: GarminHistoryArchive) -> None:
        if block_archive_start:
            archive_started.set()
            await release_archive.wait()
        await original_archive_start(archive)

    try:
        with ExitStack() as stack:
            stack.enter_context(
                patch("custom_components.garmin_connect.GarminAuth", return_value=MagicMock())
            )
            stack.enter_context(
                patch("custom_components.garmin_connect.GarminClient", return_value=MagicMock())
            )
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.HomeAssistantRecorderCompatibility.async_check",
                    new=AsyncMock(return_value=MagicMock(compatible=True, error_type=None)),
                )
            )
            source = MagicMock()
            source.async_fetch = AsyncMock(return_value=())
            source.async_fetch_details = None
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.GarminHistorySource",
                    return_value=source,
                )
            )
            recorder = MagicMock()
            recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
            stack.enter_context(
                patch(
                    "custom_components.garmin_connect.history.GarminHistoryRecorder",
                    return_value=recorder,
                )
            )
            stack.enter_context(
                patch(
                    "homeassistant.helpers.recorder.get_instance",
                    return_value=recorder,
                )
            )
            first_sync = stack.enter_context(
                patch.object(GarminHistoryArchive, "_async_run_first_sync", new=AsyncMock())
            )
            coordinator_constructors = _stack_coordinators(stack, coordinator)
            for constructor in coordinator_constructors:
                constructor.side_effect = coordinator_constructor
            stack.enter_context(
                patch.object(hass.config_entries, "async_forward_entry_setups", new=AsyncMock())
            )
            stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_unload_platforms",
                    new=AsyncMock(return_value=True),
                )
            )
            schedule_reload = stack.enter_context(
                patch.object(
                    hass.config_entries,
                    "async_schedule_reload",
                    wraps=hass.config_entries.async_schedule_reload,
                )
            )
            stack.enter_context(
                patch.object(GarminHistoryArchive, "async_start", new=start_archive)
            )

            await hass.config_entries.async_add(entry)
            await hass.async_block_till_done()
            assert entry.runtime_data.core.update_interval == timedelta(seconds=600)

            block_archive_start = True
            hass.config_entries.async_update_entry(
                entry,
                options={CONF_ARCHIVE_ENABLED: True, CONF_SCAN_INTERVAL: 601},
            )
            await asyncio.wait_for(archive_started.wait(), timeout=0.1)
            assert len(entry.update_listeners) == 1

            hass.config_entries.async_update_entry(
                entry,
                options={CONF_ARCHIVE_ENABLED: True, CONF_SCAN_INTERVAL: 602},
            )
            block_archive_start = False
            release_archive.set()
            await hass.async_block_till_done()

            assert schedule_reload.call_count == 2
            assert entry.runtime_data.core.update_interval == timedelta(seconds=602)
            first_sync.assert_awaited_once()
            assert len(constructed) == 27
    finally:
        hass.config_entries._store._async_cleanup_delay_listener()
        await hass.async_stop(force=True)


async def test_setup_entry_stores_all_coordinators() -> None:
    """runtime_data must be a GarminConnectCoordinators with all 9 fields."""
    from custom_components.garmin_connect.coordinator import GarminConnectCoordinators

    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)

    coord = _coord_mock()
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        await async_setup_entry(hass, entry)

    assert isinstance(entry.runtime_data, GarminConnectCoordinators)
    for field in (
        "core",
        "activity",
        "training",
        "body",
        "goals",
        "gear",
        "blood_pressure",
        "menstrual",
        "nutrition",
    ):
        assert getattr(entry.runtime_data, field) is coord


async def test_setup_entry_passes_one_gate_to_all_coordinators() -> None:
    """All current coordinators for one entry share the account gate."""
    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)
    hass.config_entries.async_forward_entry_setups = AsyncMock()

    coord = _coord_mock()
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        constructor_patches = [
            stack.enter_context(patch(target, return_value=coord)) for target in _COORD_TARGETS
        ]
        await async_setup_entry(hass, entry)

    gates = [constructor.call_args.args[4] for constructor in constructor_patches]
    assert isinstance(gates[0], GarminRequestGate)
    assert all(gate is gates[0] for gate in gates)
    assert entry.runtime_data.request_gate is gates[0]


async def test_setup_entry_restores_di_tokens_onto_auth() -> None:
    """Tokens from config entry data must be assigned to auth before client is built."""
    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)

    captured: dict = {}

    def _capture_auth(is_cn=False):
        auth = MagicMock()
        captured["auth"] = auth
        return auth

    coord = _coord_mock()
    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "custom_components.garmin_connect.GarminAuth",
                side_effect=_capture_auth,
            )
        )
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        await async_setup_entry(hass, entry)

    auth = captured["auth"]
    assert auth.di_token == ENTRY_DATA[CONF_TOKEN]
    assert auth.di_refresh_token == ENTRY_DATA[CONF_REFRESH_TOKEN]
    assert auth.di_client_id == ENTRY_DATA[CONF_CLIENT_ID]


async def test_setup_entry_registers_services_when_not_present() -> None:
    """Services are registered when has_service returns False."""
    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)

    coord = _coord_mock()
    setup_services = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        stack.enter_context(
            patch(
                "custom_components.garmin_connect.async_setup_services",
                setup_services,
            )
        )
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        await async_setup_entry(hass, entry)

    setup_services.assert_awaited_once()


async def test_archive_startup_failure_does_not_block_current_setup() -> None:
    """Archive startup failure must leave coordinators and sensor setup active."""
    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)
    hass.config_entries.async_forward_entry_setups = AsyncMock()

    coord = _coord_mock()
    archive = MagicMock()
    archive.async_start = AsyncMock(side_effect=RuntimeError("archive unavailable"))
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminHistoryArchive", return_value=archive)
        )

        result = await async_setup_entry(hass, entry)
        await _await_archive_start_task(entry)

    assert result is True
    archive.async_start.assert_awaited_once()
    hass.config_entries.async_forward_entry_setups.assert_awaited_once()


async def test_stalled_recorder_check_starts_archive_in_background_and_can_cancel() -> None:
    """A slow Recorder barrier cannot delay current-value setup."""
    entry = _config_entry_mock(entry_id="entry-stalled-recorder", title="Garmin account")
    entry.data = {**ENTRY_DATA, "history_account_key": "opaque-account-key-1234567890"}
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    coordinator = _coord_mock()
    created_tasks: list[asyncio.Task[None]] = []
    entry.async_create_task.side_effect = (
        lambda _hass, target, name: created_tasks.append(
            asyncio.create_task(target, name=name)
        )
        or created_tasks[-1]
    )
    store = MagicMock()
    store.async_load = AsyncMock(return_value=None)
    store.async_save = AsyncMock()
    recorder_check_started = asyncio.Event()

    async def never_complete() -> RecorderCompatibilityResult:
        recorder_check_started.set()
        await asyncio.Event().wait()

    archive = GarminHistoryArchive(
        hass,
        entry,
        recorder_checker=MagicMock(async_check=AsyncMock(side_effect=never_complete)),
        store_factory=lambda *_args, **_kwargs: store,
    )
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coordinator)
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminHistoryArchive", return_value=archive)
        )

        assert await asyncio.wait_for(async_setup_entry(hass, entry), timeout=0.1) is True

    await asyncio.wait_for(recorder_check_started.wait(), timeout=0.1)
    assert hass.config_entries.async_forward_entry_setups.await_count == 1
    assert created_tasks and not created_tasks[0].done()

    created_tasks[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await created_tasks[0]


async def test_missing_recorder_task_only_fails_archive_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RecorderTask remains an archive-only dependency during core setup."""
    entry = _config_entry_mock(entry_id="entry-missing-recorder-task")
    entry.data = {**ENTRY_DATA, "history_account_key": "opaque-account-key-1234567890"}
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    coordinator = _coord_mock()
    store = MagicMock()
    store.async_load = AsyncMock(return_value=None)
    store.async_save = AsyncMock()
    archive = GarminHistoryArchive(
        hass,
        entry,
        store_factory=lambda *_args, **_kwargs: store,
    )
    monkeypatch.setattr(
        "custom_components.garmin_connect.history_recorder._load_recorder_task",
        lambda: (_ for _ in ()).throw(TypeError("RecorderTask is unavailable")),
    )

    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coordinator)
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminHistoryArchive", return_value=archive)
        )

        assert await async_setup_entry(hass, entry) is True
        await _await_archive_start_task(entry)

    assert archive.status.state.value == "failed"
    assert archive.status.error_type == "recorder_signature"
    assert hass.config_entries.async_forward_entry_setups.await_count == 1


async def test_setup_entry_skips_services_when_already_registered() -> None:
    """Services are not re-registered when has_service returns True."""
    entry = _config_entry_mock()
    entry.data = dict(ENTRY_DATA)
    entry.options = {}
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=True)

    coord = _coord_mock()
    setup_services = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        stack.enter_context(
            patch(
                "custom_components.garmin_connect.async_setup_services",
                setup_services,
            )
        )
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        await async_setup_entry(hass, entry)

    setup_services.assert_not_awaited()


async def test_real_entry_setup_rolls_back_partial_platform_failure_and_retries() -> None:
    """A failed platform setup leaves no runtime and a retry installs one."""
    entry = _real_config_entry()
    hass = MagicMock()
    _configure_entry_task_factory(hass)
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)
    hass.config_entries.async_forward_entry_setups = AsyncMock(
        side_effect=[RuntimeError("calendar setup failed"), None]
    )
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
    coord = _coord_mock()
    first_archive = MagicMock(spec=GarminHistoryArchive)
    first_archive.async_start = AsyncMock()
    first_archive.async_stop = AsyncMock()
    second_archive = MagicMock(spec=GarminHistoryArchive)
    second_archive.async_start = AsyncMock()
    second_archive.async_stop = AsyncMock()

    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        constructors = _stack_coordinators(stack, coord)
        stack.enter_context(
            patch(
                "custom_components.garmin_connect.GarminHistoryArchive",
                side_effect=[first_archive, second_archive],
            )
        )
        setup_services = stack.enter_context(
            patch("custom_components.garmin_connect.async_setup_services", new=AsyncMock())
        )

        with pytest.raises(RuntimeError, match="calendar setup failed"):
            await async_setup_entry(hass, entry)

        first_gate = constructors[0].call_args.args[4]
        assert first_gate._closed is True
        first_archive.async_stop.assert_awaited_once()
        assert not hasattr(entry, "runtime_data")
        assert entry.update_listeners == []
        assert entry._on_unload is None
        setup_services.assert_not_awaited()

        assert await async_setup_entry(hass, entry) is True
        await _await_archive_start_task(entry)

    assert entry.runtime_data.request_gate is not first_gate
    assert entry.runtime_data.request_gate._closed is False
    assert len(entry.update_listeners) == 1
    second_archive.async_start.assert_awaited_once()


async def test_setup_rollback_platform_refusal_retains_runtime_until_retry() -> None:
    """A refused rollback preserves the platform runtime and retries teardown first."""
    entry = _real_config_entry()
    hass = MagicMock()
    _configure_entry_task_factory(hass)
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)
    hass.config_entries.async_entries = MagicMock(return_value=[entry])
    hass.config_entries.async_forward_entry_setups = AsyncMock(
        side_effect=[RuntimeError("calendar setup failed"), None]
    )
    hass.config_entries.async_unload_platforms = AsyncMock(side_effect=[False, True])
    coord = _coord_mock()
    first_archive = MagicMock(spec=GarminHistoryArchive)
    first_archive.async_start = AsyncMock()
    first_archive.async_stop = AsyncMock()
    second_archive = MagicMock(spec=GarminHistoryArchive)
    second_archive.async_start = AsyncMock()
    second_archive.async_stop = AsyncMock()

    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        constructors = _stack_coordinators(stack, coord)
        stack.enter_context(
            patch(
                "custom_components.garmin_connect.GarminHistoryArchive",
                side_effect=[first_archive, second_archive],
            )
        )
        unload_services = stack.enter_context(
            patch("custom_components.garmin_connect.async_unload_services", new=AsyncMock())
        )
        stack.enter_context(
            patch("custom_components.garmin_connect.async_setup_services", new=AsyncMock())
        )

        with pytest.raises(RuntimeError, match="calendar setup failed"):
            await async_setup_entry(hass, entry)

        retained_runtime = entry.runtime_data
        first_gate = constructors[0].call_args.args[4]
        assert first_gate._closed is False
        first_archive.async_stop.assert_not_awaited()
        assert entry.update_listeners == []

        assert await async_setup_entry(hass, entry) is True

    assert entry.runtime_data is not retained_runtime
    assert first_gate._closed is True
    first_archive.async_stop.assert_awaited_once()
    assert hass.config_entries.async_unload_platforms.await_count == 2
    unload_services.assert_awaited_once_with(hass)


async def test_real_entry_setup_cancellation_propagates_and_rolls_back() -> None:
    """Cancelling setup is not converted to ConfigEntryNotReady or leaked work."""
    entry = _real_config_entry()
    hass = MagicMock()
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
    coord = _coord_mock()
    refresh_started = asyncio.Event()
    release_refresh = asyncio.Event()

    async def wait_for_refresh() -> None:
        refresh_started.set()
        await release_refresh.wait()

    coord.async_config_entry_first_refresh.side_effect = wait_for_refresh
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        constructors = _stack_coordinators(stack, coord)
        setup_task = asyncio.create_task(async_setup_entry(hass, entry))
        await refresh_started.wait()
        setup_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await setup_task

    gate = constructors[0].call_args.args[4]
    assert gate._closed is True
    assert not hasattr(entry, "runtime_data")
    assert entry.update_listeners == []


async def test_real_entry_timeout_becomes_not_ready() -> None:
    """Only a timeout maps to ConfigEntryNotReady during setup."""
    entry = _real_config_entry()
    hass = MagicMock()
    hass.config.country = "US"
    coord = _coord_mock()
    coord.async_config_entry_first_refresh.side_effect = asyncio.TimeoutError

    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        constructors = _stack_coordinators(stack, coord)
        with pytest.raises(ConfigEntryNotReady, match="timed out"):
            await async_setup_entry(hass, entry)

    assert constructors[0].call_args.args[4]._closed is True


async def test_unload_cancels_pending_archive_start_and_releases_runtime() -> None:
    """Unload owns a pending optional archive startup task."""
    entry = _real_config_entry()
    hass = MagicMock()
    _configure_entry_task_factory(hass)
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
    coord = _coord_mock()
    archive = MagicMock(spec=GarminHistoryArchive)
    archive_started = asyncio.Event()
    async def start_archive() -> None:
        archive_started.set()
        await asyncio.Event().wait()

    archive.async_start = AsyncMock(side_effect=start_archive)
    archive.async_stop = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        constructors = _stack_coordinators(stack, coord)
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminHistoryArchive", return_value=archive)
        )
        unload_services = stack.enter_context(
            patch("custom_components.garmin_connect.async_unload_services", new=AsyncMock())
        )
        stack.enter_context(
            patch("custom_components.garmin_connect.async_setup_services", new=AsyncMock())
        )

        assert await async_setup_entry(hass, entry) is True
        await archive_started.wait()
        assert await async_unload_entry(hass, entry) is True

    assert constructors[0].call_args.args[4]._closed is True
    archive.async_stop.assert_awaited_once()
    unload_services.assert_awaited_once_with(hass)
    assert not hasattr(entry, "runtime_data")


async def test_unload_keeps_services_owned_by_another_loaded_entry() -> None:
    """Unloading entry A cannot unregister services entry B still needs."""
    entry_a = _real_config_entry()
    entry_b = _real_config_entry("entry-b")
    entry_b.runtime_data = object()
    hass = MagicMock()
    _configure_entry_task_factory(hass)
    hass.config.country = "US"
    hass.services.has_service = MagicMock(return_value=False)
    hass.config_entries.async_entries = MagicMock(return_value=[entry_a, entry_b])
    hass.config_entries.async_forward_entry_setups = AsyncMock()
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
    coord = _coord_mock()
    archive = MagicMock(spec=GarminHistoryArchive)
    archive_started = asyncio.Event()

    async def start_archive() -> None:
        archive_started.set()
        await asyncio.Event().wait()

    archive.async_start = AsyncMock(side_effect=start_archive)
    archive.async_stop = AsyncMock()
    with ExitStack() as stack:
        stack.enter_context(patch("custom_components.garmin_connect.GarminAuth"))
        stack.enter_context(patch("custom_components.garmin_connect.GarminClient"))
        _stack_coordinators(stack, coord)
        stack.enter_context(
            patch("custom_components.garmin_connect.GarminHistoryArchive", return_value=archive)
        )
        unload_services = stack.enter_context(
            patch("custom_components.garmin_connect.async_unload_services", new=AsyncMock())
        )
        stack.enter_context(
            patch("custom_components.garmin_connect.async_setup_services", new=AsyncMock())
        )

        assert await async_setup_entry(hass, entry_a) is True
        await archive_started.wait()
        assert await async_unload_entry(hass, entry_a) is True

    unload_services.assert_not_awaited()
    assert entry_b.runtime_data is not None


# ── Unload tests ──────────────────────────────────────────────────────────────


async def test_unload_entry_unregisters_services_when_last_entry() -> None:
    """Services are removed when the last config entry is unloaded."""
    entry = MagicMock()
    hass = MagicMock()
    hass.config_entries.async_entries = MagicMock(return_value=[entry])
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)

    unload_services = AsyncMock()
    with patch("custom_components.garmin_connect.async_unload_services", unload_services):
        result = await async_unload_entry(hass, entry)

    assert result is True
    unload_services.assert_awaited_once()


async def test_unload_entry_keeps_services_when_other_entries_exist() -> None:
    """Services stay registered while another entry has a live runtime."""
    entry1, entry2 = MagicMock(), MagicMock()
    entry2.runtime_data = object()
    hass = MagicMock()
    hass.config_entries.async_entries = MagicMock(return_value=[entry1, entry2])
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)

    unload_services = AsyncMock()
    with patch("custom_components.garmin_connect.async_unload_services", unload_services):
        result = await async_unload_entry(hass, entry1)

    assert result is True
    unload_services.assert_not_awaited()


async def test_unload_entry_stops_history_archive() -> None:
    """Config-entry unload must stop the per-entry archive first."""
    from custom_components.garmin_connect.coordinator import GarminConnectCoordinators

    entry = MagicMock()
    archive = MagicMock(spec=GarminHistoryArchive)
    archive.async_stop = AsyncMock()
    request_gate = MagicMock(spec=GarminRequestGate)
    request_gate.async_close = AsyncMock()
    coord = _coord_mock()
    entry.runtime_data = GarminConnectCoordinators(
        core=coord,
        activity=coord,
        training=coord,
        body=coord,
        goals=coord,
        gear=coord,
        blood_pressure=coord,
        menstrual=coord,
        nutrition=coord,
        request_gate=request_gate,
        history_archive=archive,
    )
    hass = MagicMock()
    hass.config_entries.async_entries = MagicMock(return_value=[entry])
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)

    with patch("custom_components.garmin_connect.async_unload_services", new=AsyncMock()):
        result = await async_unload_entry(hass, entry)

    assert result is True
    request_gate.async_close.assert_awaited_once()
    archive.async_stop.assert_awaited_once()


async def test_real_entry_partial_unload_keeps_runtime_usable() -> None:
    """A refused platform unload cannot stop the still-loaded runtime."""
    from custom_components.garmin_connect.coordinator import GarminConnectCoordinators

    entry = _real_config_entry()
    archive = MagicMock(spec=GarminHistoryArchive)
    archive.async_stop = AsyncMock()
    gate = GarminRequestGate()
    coord = _coord_mock()
    runtime = GarminConnectCoordinators(
        core=coord,
        activity=coord,
        training=coord,
        body=coord,
        goals=coord,
        gear=coord,
        blood_pressure=coord,
        menstrual=coord,
        nutrition=coord,
        request_gate=gate,
        history_archive=archive,
    )
    entry.runtime_data = runtime
    hass = MagicMock()
    hass.config_entries.async_entries = MagicMock(return_value=[entry])
    hass.config_entries.async_unload_platforms = AsyncMock(side_effect=[False, True])

    with patch("custom_components.garmin_connect.async_unload_services", new=AsyncMock()):
        assert await async_unload_entry(hass, entry) is False
        assert entry.runtime_data is runtime
        assert gate._closed is False
        archive.async_stop.assert_not_awaited()

        assert await async_unload_entry(hass, entry) is True

    assert gate._closed is True
    archive.async_stop.assert_awaited_once()


async def test_unload_exception_reopens_reload_gate() -> None:
    """A failed platform unload cannot suppress the next options reload."""
    entry = MagicMock(entry_id="entry-unload-exception")
    entry.data = {}
    entry.options = {CONF_SCAN_INTERVAL: 300}
    hass = MagicMock()
    schedule_reload = MagicMock()
    hass.config_entries.async_schedule_reload = schedule_reload
    hass.config_entries.async_unload_platforms = AsyncMock(
        side_effect=RuntimeError("platform unload failed")
    )

    await async_options_update_listener(hass, entry)
    schedule_reload.assert_called_once_with(entry.entry_id)

    with pytest.raises(RuntimeError, match="platform unload failed"):
        await async_unload_entry(hass, entry)

    entry.options = {CONF_SCAN_INTERVAL: 301}
    await async_options_update_listener(hass, entry)

    assert schedule_reload.call_count == 2
    schedule_reload.assert_called_with(entry.entry_id)


async def test_unload_entry_cancels_active_current_refresh() -> None:
    """Unloading an entry cannot strand an in-flight current refresh."""
    from custom_components.garmin_connect.coordinator import GarminConnectCoordinators

    gate = GarminRequestGate()
    request_started = asyncio.Event()

    async def requester() -> None:
        request_started.set()
        await asyncio.Event().wait()

    active_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.FOREGROUND, requester)
    )
    await request_started.wait()

    entry = MagicMock()
    coord = _coord_mock()
    entry.runtime_data = GarminConnectCoordinators(
        core=coord,
        activity=coord,
        training=coord,
        body=coord,
        goals=coord,
        gear=coord,
        blood_pressure=coord,
        menstrual=coord,
        nutrition=coord,
        request_gate=gate,
    )
    hass = MagicMock()
    hass.config_entries.async_entries = MagicMock(return_value=[entry])
    hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)

    with patch("custom_components.garmin_connect.async_unload_services", new=AsyncMock()):
        assert await async_unload_entry(hass, entry) is True

    with pytest.raises(asyncio.CancelledError):
        await active_task


# ── Migration tests ───────────────────────────────────────────────────────────


async def test_migrate_v1_to_v2_bumps_version_and_triggers_reauth() -> None:
    """V1 entries must be bumped to v2 and reauth triggered."""
    mock_entry = MagicMock()
    mock_entry.version = 1
    mock_entry.unique_id = "user@example.com"
    mock_entry.entry_id = "test_entry_id"
    mock_entry.title = "user@example.com"
    mock_hass = MagicMock()

    with (
        patch("custom_components.garmin_connect.er.async_get"),
        patch(
            "custom_components.garmin_connect.er.async_entries_for_config_entry",
            return_value=[],
        ),
    ):
        result = await async_migrate_entry(mock_hass, mock_entry)

    assert result is True
    mock_hass.config_entries.async_update_entry.assert_called_once_with(mock_entry, version=2)
    mock_entry.async_start_reauth.assert_called_once_with(mock_hass)


async def test_migrate_v2_is_noop() -> None:
    """V2 entries must not be migrated again."""
    mock_entry = MagicMock()
    mock_entry.version = 2
    mock_hass = MagicMock()

    result = await async_migrate_entry(mock_hass, mock_entry)

    assert result is True
    mock_hass.config_entries.async_update_entry.assert_not_called()


async def test_migrate_entity_unique_ids_unchanged_key() -> None:
    """Entities with unchanged keys get prefix migrated (email -> entry_id)."""
    mock_hass = MagicMock()
    mock_entry = MagicMock()
    mock_entry.entry_id = "new_entry_id"

    mock_entity = MagicMock()
    mock_entity.unique_id = "user@example.com_totalSteps"
    mock_entity.entity_id = "sensor.total_steps"

    mock_registry = MagicMock()

    with (
        patch("custom_components.garmin_connect.er.async_get", return_value=mock_registry),
        patch(
            "custom_components.garmin_connect.er.async_entries_for_config_entry",
            return_value=[mock_entity],
        ),
    ):
        _migrate_entity_unique_ids(mock_hass, mock_entry, "user@example.com")

    mock_registry.async_update_entity.assert_called_once_with(
        "sensor.total_steps",
        new_unique_id="new_entry_id_totalSteps",
    )


async def test_migrate_entity_unique_ids_renamed_key() -> None:
    """Entities with renamed keys get both prefix and key migrated."""
    mock_hass = MagicMock()
    mock_entry = MagicMock()
    mock_entry.entry_id = "new_entry_id"

    mock_entity = MagicMock()
    mock_entity.unique_id = "user@example.com_sleepingSeconds"
    mock_entity.entity_id = "sensor.sleeping_time"

    mock_registry = MagicMock()

    with (
        patch("custom_components.garmin_connect.er.async_get", return_value=mock_registry),
        patch(
            "custom_components.garmin_connect.er.async_entries_for_config_entry",
            return_value=[mock_entity],
        ),
    ):
        _migrate_entity_unique_ids(mock_hass, mock_entry, "user@example.com")

    mock_registry.async_update_entity.assert_called_once_with(
        "sensor.sleeping_time",
        new_unique_id="new_entry_id_sleepingMinutes",
    )


async def test_migrate_entity_unique_ids_dropped_key_skipped() -> None:
    """Dropped sensors (mapped to None) must be skipped during migration."""
    mock_hass = MagicMock()
    mock_entry = MagicMock()
    mock_entry.entry_id = "new_entry_id"

    mock_entity = MagicMock()
    mock_entity.unique_id = "user@example.com_netCalorieGoal"
    mock_entity.entity_id = "sensor.net_calorie_goal"

    mock_registry = MagicMock()

    with (
        patch("custom_components.garmin_connect.er.async_get", return_value=mock_registry),
        patch(
            "custom_components.garmin_connect.er.async_entries_for_config_entry",
            return_value=[mock_entity],
        ),
    ):
        _migrate_entity_unique_ids(mock_hass, mock_entry, "user@example.com")

    mock_registry.async_update_entity.assert_not_called()


async def test_migrate_entity_unique_ids_conflict_handled() -> None:
    """Unique_id conflicts must be logged but not fail migration."""
    mock_hass = MagicMock()
    mock_entry = MagicMock()
    mock_entry.entry_id = "new_entry_id"

    mock_entity = MagicMock()
    mock_entity.unique_id = "user@example.com_totalSteps"
    mock_entity.entity_id = "sensor.total_steps"

    mock_registry = MagicMock()
    mock_registry.async_update_entity.side_effect = ValueError("conflict")

    with (
        patch("custom_components.garmin_connect.er.async_get", return_value=mock_registry),
        patch(
            "custom_components.garmin_connect.er.async_entries_for_config_entry",
            return_value=[mock_entity],
        ),
    ):
        _migrate_entity_unique_ids(mock_hass, mock_entry, "user@example.com")

    mock_registry.async_update_entity.assert_called_once()


async def test_migrate_entity_non_matching_prefix_skipped() -> None:
    """Entities not matching the old prefix must be skipped."""
    mock_hass = MagicMock()
    mock_entry = MagicMock()
    mock_entry.entry_id = "new_entry_id"

    mock_entity = MagicMock()
    mock_entity.unique_id = "other_prefix_totalSteps"
    mock_entity.entity_id = "sensor.total_steps"

    mock_registry = MagicMock()

    with (
        patch("custom_components.garmin_connect.er.async_get", return_value=mock_registry),
        patch(
            "custom_components.garmin_connect.er.async_entries_for_config_entry",
            return_value=[mock_entity],
        ),
    ):
        _migrate_entity_unique_ids(mock_hass, mock_entry, "user@example.com")

    mock_registry.async_update_entity.assert_not_called()
