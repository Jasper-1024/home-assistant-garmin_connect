"""The Garmin Connect integration."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast
from weakref import WeakKeyDictionary

from ha_garmin import GarminAuth, GarminClient
from homeassistant.config_entries import ConfigEntryNotReady
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er

from .const import (
    CONF_CLIENT_ID,
    CONF_DEBUG_CAPTURE_ENABLED,
    CONF_DEBUG_REPLAY_SESSION,
    CONF_IS_CN,
    CONF_REFRESH_TOKEN,
    CONF_TOKEN,
    DOMAIN,
)
from .coordinator import (
    ActivityCoordinator,
    BloodPressureCoordinator,
    BodyCoordinator,
    CoreCoordinator,
    GarminConnectConfigEntry,
    GarminConnectCoordinators,
    GearCoordinator,
    GoalsCoordinator,
    MenstrualCoordinator,
    NutritionCoordinator,
    TrainingCoordinator,
)
from .debug_capture import GarminDebugCapture
from .history import GarminHistoryArchive, _persist_archive_enablement_transition
from .request_gate import GarminRequestGate
from .services import async_setup_services, async_unload_services

_LOGGER = logging.getLogger(__name__)

# Recorder's queue confirmation can legitimately wait for its own five-minute
# hard limit. Archive is optional, so core setup gets a separate short bound.
_ARCHIVE_STARTUP_TIMEOUT = 5


@dataclass(slots=True)
class _EntryUpdateState:
    """Observed and applied config-entry options plus one reload flight."""

    options: dict[str, Any]
    data: dict[str, Any]
    applied_options: dict[str, Any] | None = None
    reload_requested: bool = False
    reload_scheduled: bool = False
    archive_start_task: asyncio.Task[None] | None = None


_ENTRY_UPDATE_STATES: WeakKeyDictionary[object, _EntryUpdateState] = WeakKeyDictionary()

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.CALENDAR]

# Mapping of old sensor keys (v1) to new sensor keys (v2).
# Keys present in both versions are migrated by unique_id prefix only.
# Keys listed here were renamed between versions.
_V1_KEY_RENAMES: dict[str, str | None] = {
    "activeSeconds": "activeMinutes",
    "activityStressDuration": "activityStressMinutes",
    "boneMass": "boneMassKg",
    "highStressDuration": "highStressMinutes",
    "highlyActiveSeconds": "highlyActiveMinutes",
    "hrvStatus": "hrvStatusText",
    "latestRespirationTimeGMT": "latestRespirationTime",
    "latestSpo2ReadingTimeLocal": "latestSpo2ReadingTime",
    "lowStressDuration": "lowStressMinutes",
    "measurableAsleepDuration": "measurableAsleepDurationMinutes",
    "measurableAwakeDuration": "measurableAwakeDurationMinutes",
    "mediumStressDuration": "mediumStressMinutes",
    "muscleMass": "muscleMassKg",
    "restStressDuration": "restStressMinutes",
    "sedentarySeconds": "sedentaryMinutes",
    "sleepTimeSeconds": "sleepTimeMinutes",
    "sleepingSeconds": "sleepingMinutes",
    "stressDuration": "stressMinutes",
    "stressQualifier": "stressQualifierText",
    "totalStressDuration": "stressMinutes",
    "uncategorizedStressDuration": "uncategorizedStressMinutes",
    "wellnessEndTimeLocal": "wellnessEndTime",
    "wellnessStartTimeLocal": "wellnessStartTime",
    # Dropped sensors (no equivalent in v2)
    "netCalorieGoal": None,
    "netRemainingKilocalories": None,
    "wellnessDescription": None,
}


async def async_migrate_entry(hass: HomeAssistant, entry: GarminConnectConfigEntry) -> bool:
    """Migrate a config entry from v1 to v2.

    V1 used garminconnect/garth (OAuth1 tokens, email as unique_id prefix).
    V2 uses ha-garmin (DI tokens, entry_id as unique_id prefix).

    Tokens are incompatible so reauth is required. Entity unique_ids are
    migrated in the entity registry so existing entity_ids are preserved.
    """
    if entry.version == 1:
        _LOGGER.info("Migrating Garmin Connect entry %s from v1 to v2", entry.title)

        # The old unique_id was the email address; it's also used as the
        # prefix for all entity unique_ids (e.g. "user@example.com_totalSteps").
        old_prefix = entry.unique_id or ""

        if old_prefix:
            _migrate_entity_unique_ids(hass, entry, old_prefix)

        # Bump version and trigger reauth (tokens are incompatible).
        hass.config_entries.async_update_entry(entry, version=2)
        entry.async_start_reauth(hass)
        _LOGGER.info("Migration to v2 complete for %s — reauth required", entry.title)

    return True


def _migrate_entity_unique_ids(
    hass: HomeAssistant,
    entry: GarminConnectConfigEntry,
    old_prefix: str,
) -> None:
    """Rewrite entity unique_ids from v1 (email_key) to v2 (entry_id_key).

    Also applies key renames so the entity registry keeps existing entity_ids
    intact (e.g. sensor.total_steps stays sensor.total_steps).
    """
    registry = er.async_get(hass)
    entities = er.async_entries_for_config_entry(registry, entry.entry_id)
    new_prefix = entry.entry_id

    for entity in entities:
        old_uid = entity.unique_id
        if not old_uid.startswith(old_prefix + "_"):
            continue

        old_key = old_uid[len(old_prefix) + 1 :]

        # Determine the new key: renamed, dropped, or unchanged.
        if old_key in _V1_KEY_RENAMES:
            new_key = _V1_KEY_RENAMES[old_key]
            if new_key is None:
                _LOGGER.debug(
                    "Sensor %s (%s) has been removed in v2, skipping",
                    entity.entity_id,
                    old_key,
                )
                continue
        else:
            new_key = old_key

        new_uid = f"{new_prefix}_{new_key}"

        if new_uid == old_uid:
            continue

        try:
            registry.async_update_entity(entity.entity_id, new_unique_id=new_uid)
            _LOGGER.debug(
                "Migrated %s unique_id: %s -> %s",
                entity.entity_id,
                old_uid,
                new_uid,
            )
        except ValueError:
            _LOGGER.warning(
                "Could not migrate %s (%s -> %s): unique_id conflict",
                entity.entity_id,
                old_uid,
                new_uid,
            )


async def async_setup_entry(hass: HomeAssistant, entry: GarminConnectConfigEntry) -> bool:
    """Set up Garmin Connect from a config entry."""
    _persist_archive_enablement_transition(hass, entry)

    if not await _async_clear_incomplete_setup(hass, entry):
        raise ConfigEntryNotReady(
            "Garmin platform cleanup is still pending; will retry setup"
        )

    if CONF_TOKEN not in entry.data:
        # Migration from v1 bumps version and starts reauth but setup still runs.
        # Without valid DI tokens there's nothing to set up — reauth will fix it.
        _LOGGER.debug("Skipping setup for %s — reauth pending", entry.title)
        return False

    # Coordinators read options only during construction. Retain that exact
    # snapshot so setup can detect an update arriving while it awaits refreshes.
    applied_options = dict(entry.options)
    is_cn = applied_options.get(CONF_IS_CN, False)
    auth = GarminAuth(is_cn=is_cn)
    auth.di_token = entry.data[CONF_TOKEN]
    auth.di_refresh_token = entry.data[CONF_REFRESH_TOKEN]
    auth.di_client_id = entry.data[CONF_CLIENT_ID]

    client = GarminClient(auth, is_cn=is_cn)
    debug_capture = GarminDebugCapture(
        Path(hass.config.path("tmp", DOMAIN)),
        entry.entry_id,
        capture_enabled=bool(applied_options.get(CONF_DEBUG_CAPTURE_ENABLED, False)),
        replay_session=applied_options.get(CONF_DEBUG_REPLAY_SESSION) or None,
    )
    debug_capture.install(client)
    request_gate = GarminRequestGate()

    coordinators = GarminConnectCoordinators(
        core=CoreCoordinator(hass, entry, client, auth, request_gate),
        activity=ActivityCoordinator(hass, entry, client, auth, request_gate),
        training=TrainingCoordinator(hass, entry, client, auth, request_gate),
        body=BodyCoordinator(hass, entry, client, auth, request_gate),
        goals=GoalsCoordinator(hass, entry, client, auth, request_gate),
        gear=GearCoordinator(hass, entry, client, auth, request_gate),
        blood_pressure=BloodPressureCoordinator(hass, entry, client, auth, request_gate),
        menstrual=MenstrualCoordinator(hass, entry, client, auth, request_gate),
        nutrition=NutritionCoordinator(hass, entry, client, auth, request_gate),
        request_gate=request_gate,
    )

    history_archive: GarminHistoryArchive | None = None
    runtime_attached = False
    platforms_setup_attempted = False
    services_setup_attempted = False
    remove_options_listener: Callable[[], None] | None = None
    try:
        try:
            await coordinators.core.async_config_entry_first_refresh()
        except TimeoutError as err:
            raise ConfigEntryNotReady(
                "Garmin API timed out during setup; will retry"
            ) from err

        refresh_results = await asyncio.gather(
            coordinators.activity.async_refresh(),
            coordinators.training.async_refresh(),
            coordinators.body.async_refresh(),
            coordinators.goals.async_refresh(),
            coordinators.gear.async_refresh(),
            coordinators.blood_pressure.async_refresh(),
            coordinators.menstrual.async_refresh(),
            coordinators.nutrition.async_refresh(),
            return_exceptions=True,
        )
        for result in refresh_results:
            if isinstance(result, asyncio.CancelledError):
                raise result

        # Platform setup needs runtime_data, so attach it provisionally and
        # remove it again if any later setup step fails.
        entry.runtime_data = coordinators
        runtime_attached = True
        history_archive = GarminHistoryArchive(hass, entry)
        coordinators.history_archive = history_archive

        platforms_setup_attempted = True
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        if not hass.services.has_service(DOMAIN, "set_active_gear"):
            services_setup_attempted = True
            await async_setup_services(hass)

        # Start observing desired options before archive startup. Its first
        # sync is background work, but async_start itself still awaits store
        # and recorder initialization, during which an option update must not
        # be lost. Keep the listener provisional until setup commits so a
        # cancelled setup cannot leave an accumulated callback behind.
        remove_options_listener = _add_options_update_listener(entry)
        reload_needed = _record_entry_update_state(hass, entry, applied_options)

        # Do not start a first archive sync from a runtime already known to have
        # stale coordinator options. The compensating reload starts it once with
        # the latest snapshot.
        if reload_needed:
            entry.async_on_unload(remove_options_listener)
            remove_options_listener = None
            _schedule_entry_reload(hass, entry, _ENTRY_UPDATE_STATES[entry])
            return True

        state = _ENTRY_UPDATE_STATES[entry]
        archive_start = history_archive.async_start()
        state.archive_start_task = entry.async_create_task(
            hass,
            archive_start,
            name=f"{DOMAIN} archive startup",
        )
        try:
            await asyncio.wait_for(
                state.archive_start_task, timeout=_ARCHIVE_STARTUP_TIMEOUT
            )
        except TimeoutError:
            # wait_for has cancelled and joined the startup task before this
            # branch runs. Mark the optional archive failed and release any
            # partially initialized resources; never retain a pending task.
            try:
                await history_archive.async_abort_startup("startup_timeout")
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.warning(
                    "Garmin history archive could not stop after startup timeout for "
                    "entry %s",
                    entry.entry_id,
                )
            _LOGGER.warning(
                "Garmin history archive startup timed out for entry %s",
                entry.entry_id,
            )
        except asyncio.CancelledError:
            # An options listener cancels this child task to prevent an
            # obsolete archive from scheduling its first sync. Propagate only
            # cancellation of setup itself; the replacement reload owns an
            # options-driven cancellation.
            if not state.reload_requested:
                raise
        except Exception:
            # The archive is optional. Its implementation must never prevent
            # current-value coordinators from loading.
            _LOGGER.warning(
                "Garmin history archive could not start for entry %s",
                entry.entry_id,
            )
        finally:
            state.archive_start_task = None

        # Reconcile an update that arrived while archive startup was awaiting.
        # Stop this archive before reloading so its newly scheduled background
        # first sync cannot run alongside the replacement runtime's first sync.
        reload_needed = _record_entry_update_state(hass, entry, applied_options)
        reload_needed = reload_needed or state.reload_requested
        if reload_needed:
            try:
                await history_archive.async_stop()
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.warning(
                    "Garmin history archive could not stop after an options update for "
                    "entry %s",
                    entry.entry_id,
                )

        # Commit the provisional listener only after the final await. Failed
        # setup removes it below rather than retaining no-op callbacks.
        entry.async_on_unload(remove_options_listener)
        remove_options_listener = None
        if reload_needed:
            _schedule_entry_reload(hass, entry, _ENTRY_UPDATE_STATES[entry])
        return True
    except BaseException:
        if remove_options_listener is not None:
            remove_options_listener()
        await _async_rollback_setup(
            hass,
            entry,
            coordinators,
            history_archive,
            runtime_attached,
            platforms_setup_attempted,
            services_setup_attempted,
        )
        raise


def _add_options_update_listener(entry: GarminConnectConfigEntry) -> Callable[[], None]:
    """Start observing options until setup commits it to the entry lifetime."""
    return entry.add_update_listener(async_options_update_listener)


async def _async_rollback_setup(
    hass: HomeAssistant,
    entry: GarminConnectConfigEntry,
    coordinators: GarminConnectCoordinators,
    history_archive: GarminHistoryArchive | None,
    runtime_attached: bool,
    platforms_setup_attempted: bool,
    services_setup_attempted: bool,
) -> bool:
    """Undo only resources acquired by a setup that did not commit."""
    if platforms_setup_attempted:
        try:
            platforms_unloaded = cast(
                bool, await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
            )
        except Exception:
            _LOGGER.exception(
                "Could not roll back Garmin platforms for %s; retaining its runtime "
                "for a later unload or setup retry",
                entry.entry_id,
            )
            return False

        if not platforms_unloaded:
            _LOGGER.error(
                "Garmin platform rollback refused for %s; retaining runtime, archive, "
                "request gate, and services for a later unload or setup retry",
                entry.entry_id,
            )
            return False

    if services_setup_attempted and not _has_other_loaded_runtime(hass, entry):
        try:
            await async_unload_services(hass)
        except Exception:
            _LOGGER.exception("Could not roll back Garmin services after setup failure")

    if runtime_attached:
        await _async_release_runtime(entry, coordinators, history_archive)
    elif coordinators.request_gate is not None:
        await coordinators.request_gate.async_close()
    return True


async def _async_clear_incomplete_setup(
    hass: HomeAssistant, entry: GarminConnectConfigEntry
) -> bool:
    """Finish a prior failed setup only after its platforms are gone."""
    runtime = getattr(entry, "runtime_data", None)
    if not isinstance(runtime, GarminConnectCoordinators):
        return True

    _LOGGER.warning(
        "Retrying cleanup of Garmin runtime retained after an incomplete setup for %s",
        entry.entry_id,
    )
    try:
        platforms_unloaded = cast(
            bool, await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
        )
    except Exception:
        _LOGGER.exception("Could not retry Garmin platform cleanup for %s", entry.entry_id)
        return False

    if not platforms_unloaded:
        _LOGGER.error(
            "Garmin platform cleanup is still refused for %s; retaining its runtime "
            "until a later retry",
            entry.entry_id,
        )
        return False

    await _async_release_runtime(entry, runtime, runtime.history_archive)
    if not _has_other_loaded_runtime(hass, entry):
        await async_unload_services(hass)
    return True


async def _async_release_runtime(
    entry: GarminConnectConfigEntry,
    coordinators: GarminConnectCoordinators,
    history_archive: GarminHistoryArchive | None,
) -> None:
    """Stop resources only after their platforms no longer reference them."""
    try:
        if history_archive is not None:
            await history_archive.async_stop()
    finally:
        if coordinators.request_gate is not None:
            await coordinators.request_gate.async_close()
        if getattr(entry, "runtime_data", None) is coordinators:
            delattr(entry, "runtime_data")
        _ENTRY_UPDATE_STATES.pop(entry, None)


def _has_other_loaded_runtime(hass: HomeAssistant, entry: GarminConnectConfigEntry) -> bool:
    """Return whether another Garmin entry still owns the global services."""
    return any(
        candidate is not entry and getattr(candidate, "runtime_data", None) is not None
        for candidate in hass.config_entries.async_entries(DOMAIN)
    )


async def async_options_update_listener(
    hass: HomeAssistant, entry: GarminConnectConfigEntry
) -> None:
    """Reload an entry when an options transition is observed.

    Config-entry update listeners run for both data and options updates, but
    the listener receives the entry's *current* values, not the values for
    the particular update that queued it.  An options update can therefore be
    followed by a token or archive metadata update before its listener runs.
    Keep the options transition in that case rather than treating the merged
    snapshot as a data-only update.

    Archive enablement persists its boundary in entry data, which schedules a
    second listener invocation; recording the state before that write makes
    the nested data-only update a no-op here.
    """
    current_options = dict(entry.options)
    current_data = dict(entry.data)
    state = _ENTRY_UPDATE_STATES.get(entry)
    if state is None:
        # Direct callers without the normal setup registration are treated as
        # option updates for backwards-compatible listener behavior.
        state = _EntryUpdateState(options=current_options, data=current_data)
        _ENTRY_UPDATE_STATES[entry] = state
        options_changed = True
    else:
        options_changed = current_options != state.options

    if _is_explicit_token_reconfiguration(
        state.options,
        state.data,
        current_options,
        current_data,
    ):
        state.options = current_options
        state.data = current_data
        return

    state.options = current_options
    state.data = current_data

    if not options_changed:
        return

    _persist_archive_enablement_transition(hass, entry)
    state.data = dict(entry.data)
    state.reload_requested = True

    # archive.async_start schedules its first sync before returning. Cancel
    # the startup task as soon as options make its snapshot obsolete so that
    # the replacement runtime is the only one to start that first sync.
    if (
        (archive_start_task := state.archive_start_task) is not None
        and not archive_start_task.done()
    ):
        archive_start_task.cancel()

    # Multiple options updates may arrive before a reload starts. One reload
    # observes the latest entry values, so coalesce them into the same flight.
    _schedule_entry_reload(hass, entry, state)


def _record_entry_update_state(
    hass: HomeAssistant,
    entry: GarminConnectConfigEntry,
    applied_options: dict[str, Any],
) -> bool:
    """Record setup's option snapshot and report whether it became stale."""
    current_options = dict(entry.options)
    reload_needed = current_options != applied_options
    if reload_needed:
        # Persist an archive transition from the latest desired options before
        # the compensating reload.
        _persist_archive_enablement_transition(hass, entry)

    state = _ENTRY_UPDATE_STATES.get(entry)
    if state is None:
        _ENTRY_UPDATE_STATES[entry] = _EntryUpdateState(
            options=current_options,
            data=dict(entry.data),
            applied_options=applied_options,
            reload_requested=reload_needed,
        )
        return reload_needed

    state.options = current_options
    state.data = dict(entry.data)
    state.applied_options = applied_options
    state.reload_requested = state.reload_requested or reload_needed
    return reload_needed


def _schedule_entry_reload(
    hass: HomeAssistant,
    entry: GarminConnectConfigEntry,
    state: _EntryUpdateState,
) -> None:
    """Schedule one HA-managed reload unless this state already owns it."""
    if state.reload_scheduled:
        return

    state.reload_scheduled = True
    hass.config_entries.async_schedule_reload(entry.entry_id)


def _is_explicit_token_reconfiguration(
    previous_options: dict[str, Any],
    previous_data: dict[str, Any],
    current_options: dict[str, Any],
    current_data: dict[str, Any],
) -> bool:
    """Return whether a reauth/reconfigure flow owns this entry update."""
    changed_options = {
        key
        for key in previous_options.keys() | current_options.keys()
        if previous_options.get(key) != current_options.get(key)
    }
    if changed_options != {CONF_IS_CN}:
        return False

    changed_data = {
        key
        for key in previous_data.keys() | current_data.keys()
        if previous_data.get(key) != current_data.get(key)
    }
    return {CONF_TOKEN, CONF_REFRESH_TOKEN}.issubset(changed_data)


async def async_unload_entry(hass: HomeAssistant, entry: GarminConnectConfigEntry) -> bool:
    """Unload a config entry."""
    try:
        unload_ok = cast(
            bool, await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
        )
    except BaseException:
        if state := _ENTRY_UPDATE_STATES.get(entry):
            state.reload_scheduled = False
        raise

    if not unload_ok:
        if state := _ENTRY_UPDATE_STATES.get(entry):
            state.reload_scheduled = False
        return False

    runtime = getattr(entry, "runtime_data", None)
    if isinstance(runtime, GarminConnectCoordinators):
        await _async_release_runtime(entry, runtime, runtime.history_archive)

    if not _has_other_loaded_runtime(hass, entry):
        await async_unload_services(hass)

    return True
