"""Tests for Garmin Connect integration setup and migration."""

import asyncio
from contextlib import ExitStack
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from homeassistant import loader
from homeassistant.config_entries import SOURCE_USER, ConfigEntries, ConfigEntry, ConfigEntryState
from homeassistant.core import HomeAssistant

from custom_components.garmin_connect import (
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
    CONF_TOKEN,
    DOMAIN,
)
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    RecorderCompatibilityResult,
)
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


def _stack_coordinators(stack: ExitStack, coord: MagicMock) -> None:
    """Push patches for all 9 coordinator constructors onto an ExitStack."""
    for target in _COORD_TARGETS:
        stack.enter_context(patch(target, return_value=coord))


# ── Setup tests ───────────────────────────────────────────────────────────────


async def test_options_update_listener_reloads_config_entry() -> None:
    """Option changes use the config-entry reload lifecycle seam."""
    entry = MagicMock(entry_id="entry-1")
    hass = MagicMock()
    reload = AsyncMock()
    hass.config_entries.async_reload = reload
    tasks = []
    hass.async_create_task = MagicMock(side_effect=tasks.append)

    await async_options_update_listener(hass, entry)
    await tasks[0]

    reload.assert_awaited_once_with("entry-1")


async def test_options_update_persists_transition_before_reload() -> None:
    """Enablement date is persisted before setup can reload the entry."""
    entry = MagicMock(entry_id="entry-1")
    entry.data = {CONF_ARCHIVE_PREVIOUSLY_ENABLED: False}
    entry.options = {CONF_ARCHIVE_ENABLED: True}
    hass = MagicMock()
    hass.config_entries.async_update_entry = MagicMock(
        side_effect=lambda updated_entry, *, data: setattr(updated_entry, "data", data)
    )
    tasks = []

    async def reload(_entry_id: str) -> None:
        assert entry.data[CONF_ARCHIVE_ACTIVATION_DATE] == "2026-08-03"
        assert entry.data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] is True

    hass.config_entries.async_reload = reload
    hass.async_create_task = MagicMock(side_effect=tasks.append)

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 59, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)
    await tasks[0]


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
    reload = AsyncMock()
    tasks = []
    hass.config_entries.async_reload = reload
    hass.async_create_task = MagicMock(side_effect=tasks.append)

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, 0, 1, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)
    await tasks[0]

    hass.config_entries.async_update_entry.assert_not_called()
    reload.assert_awaited_once_with("entry-1")


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
    tasks = []

    def failing_store(*_args, **_kwargs):
        raise OSError("private storage unavailable")

    async def reload(_entry_id: str) -> None:
        archive = GarminHistoryArchive(hass, entry, store_factory=failing_store)
        with patch(
            "custom_components.garmin_connect.history.dt_util.utcnow",
            return_value=datetime(2026, 8, 4, 0, 1, tzinfo=UTC),
        ):
            await archive.async_start()
        assert archive.status.error_type == "store_initialization"

    hass.config_entries.async_reload = reload
    hass.async_create_task = MagicMock(side_effect=tasks.append)

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 59, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)
    await tasks[0]

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
    tasks = []
    store = MagicMock()
    store.async_load = AsyncMock(return_value=None)
    store.async_save = AsyncMock()
    checker = MagicMock()
    checker.async_check = AsyncMock(
        return_value=RecorderCompatibilityResult.incompatible_result("recorder_signature")
    )

    async def reload(_entry_id: str) -> None:
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

    hass.config_entries.async_reload = reload
    hass.async_create_task = MagicMock(side_effect=tasks.append)

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 3, 23, 59, tzinfo=UTC),
    ):
        await async_options_update_listener(hass, entry)
    await tasks[0]

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
        discovery_keys={},
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
            backfill = stack.enter_context(
                patch("custom_components.garmin_connect.history.BackfillScheduler")
            )
            _stack_coordinators(stack, coordinator)
            hass.config_entries.async_forward_entry_setups = AsyncMock()
            hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)

            await hass.config_entries.async_add(entry)
            await assert_disabled_surfaces()
            account_key = entry.data["history_account_key"]

            assert await hass.config_entries.async_reload(entry.entry_id)
            await assert_disabled_surfaces()
            assert entry.data["history_account_key"] == account_key

            assert await hass.config_entries.async_unload(entry.entry_id)
            assert entry.state is ConfigEntryState.NOT_LOADED
            assert await hass.config_entries.async_setup(entry.entry_id)
            await assert_disabled_surfaces()
            assert entry.data["history_account_key"] == account_key

            with patch(
                "custom_components.garmin_connect.history.dt_util.utcnow",
                return_value=datetime(2026, 8, 10, tzinfo=UTC),
            ):
                hass.config_entries.async_update_entry(entry, options={CONF_ARCHIVE_ENABLED: True})
                await hass.async_block_till_done()

            assert entry.runtime_data.history_archive.archive_enabled is True
            assert entry.runtime_data.history_archive.status.state.value == "idle"
            assert entry.runtime_data.history_archive.activation_date == date(2026, 8, 10)
            assert backfill.call_count == 0
    finally:
        hass.config_entries._store._async_cleanup_delay_listener()
        await hass.async_stop(force=True)


async def test_setup_entry_success() -> None:
    """Test that a config entry sets up correctly and returns True."""
    entry = MagicMock()
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


async def test_setup_entry_stores_all_coordinators() -> None:
    """runtime_data must be a GarminConnectCoordinators with all 9 fields."""
    from custom_components.garmin_connect.coordinator import GarminConnectCoordinators

    entry = MagicMock()
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
    entry = MagicMock()
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
    entry = MagicMock()
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
    entry = MagicMock()
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
    entry = MagicMock()
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

    assert result is True
    archive.async_start.assert_awaited_once()
    hass.config_entries.async_forward_entry_setups.assert_awaited_once()


async def test_setup_entry_skips_services_when_already_registered() -> None:
    """Services are not re-registered when has_service returns True."""
    entry = MagicMock()
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
    """Services are NOT removed when other config entries remain loaded."""
    entry1, entry2 = MagicMock(), MagicMock()
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
