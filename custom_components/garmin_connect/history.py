"""Lifecycle seam for Garmin history.

This module deliberately stops at archive lifecycle and compatibility.  Garmin
history fetching, normalization, Recorder imports, Calendar records, and FIT
archival belong to later vertical slices.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Any, Protocol

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import (
    CONF_HISTORY_ACCOUNT_KEY,
    DOMAIN,
    HISTORY_STORE_VERSION,
    RECORDER_COMPATIBILITY_TARGET,
)
from .history_recorder import (
    HEART_RATE_METADATA,
    STRESS_METADATA,
    GarminHistoryRecorder,
    RecorderWriteOutcome,
    statistic_id_for,
)
from .history_source import GarminHistorySource

_LOGGER = logging.getLogger(__name__)

_ACCOUNT_KEY_MIN_LENGTH = 20
_ACCOUNT_KEY_MAX_LENGTH = 128
_ACCOUNT_KEY_ALPHABET = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_RECORDER_SUPPORTED_VERSIONS = frozenset({"2026.7.3", "2026.7.4"})
_RECORDER_BARRIER_TIMEOUT = 10
_HISTORY_MIN_DATE = date(2026, 1, 1)
_HISTORY_MAX_DAYS = 31


class HistoryArchiveState(StrEnum):
    """Observable states of the history archive."""

    IDLE = "idle"
    RUNNING = "running"
    COMPATIBILITY_DISABLED = "compatibility-disabled"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class HistoryStatus:
    """Privacy-safe archive status snapshot."""

    state: HistoryArchiveState
    recorder_target: str = RECORDER_COMPATIBILITY_TARGET
    current_date: str | None = None
    processed_dates: int = 0
    record_count: int = 0
    error_type: str | None = None

    def as_attributes(self) -> dict[str, Any]:
        """Return the bounded status attributes exposed by the sensor."""
        return {
            "recorder_target": self.recorder_target,
            "archive_state": self.state.value,
            "current_date": self.current_date,
            "processed_dates": self.processed_dates,
            "record_count": self.record_count,
            "error_type": self.error_type,
        }


@dataclass(frozen=True, slots=True)
class HistorySyncReport:
    """Bounded result shape reserved for the future sync interface."""

    processed_dates: tuple[date, ...] = ()
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    fit_count: int = 0
    outcome: str = "not_implemented"
    error_type: str | None = None


@dataclass(frozen=True, slots=True)
class HistoryCalendarEvent:
    """Safe Calendar result shape reserved for the future query interface."""

    start: datetime
    end: datetime
    summary: str


@dataclass(frozen=True, slots=True)
class RecorderCompatibilityResult:
    """Result of the private Recorder compatibility seam."""

    compatible: bool
    error_type: str | None = None

    @classmethod
    def compatible_result(cls) -> RecorderCompatibilityResult:
        """Return a successful compatibility result."""
        return cls(compatible=True)

    @classmethod
    def incompatible_result(cls, error_type: str) -> RecorderCompatibilityResult:
        """Return a bounded failure result."""
        return cls(compatible=False, error_type=error_type)


class RecorderCompatibilityChecker(Protocol):
    """Adapter used by the archive to probe Home Assistant Recorder."""

    async def async_check(self) -> RecorderCompatibilityResult:
        """Check the Recorder contract without importing history data."""


class HomeAssistantRecorderCompatibility:
    """Check the Recorder path used by future raw-statistics imports.

    The imports are intentionally lazy.  A Recorder API change must disable
    this archive, not prevent the Garmin current-value integration from
    loading.
    """

    async def async_check(self) -> RecorderCompatibilityResult:
        """Validate symbols, signatures, and a real queue barrier."""
        try:
            from homeassistant.const import __version__ as home_assistant_version

            if home_assistant_version not in _RECORDER_SUPPORTED_VERSIONS:
                return RecorderCompatibilityResult.incompatible_result(
                    "unsupported_home_assistant_version"
                )

            from homeassistant.components.recorder.core import Recorder
            from homeassistant.components.recorder.db_schema import Statistics
            from homeassistant.components.recorder.models import (
                StatisticData,
                StatisticMetaData,
            )
            from homeassistant.components.recorder.tasks import SynchronizeTask
            from homeassistant.helpers.recorder import get_instance

            _require_parameters(
                Recorder.async_import_statistics,
                ("self", "metadata", "stats", "table"),
            )
            _require_parameters(Recorder.queue_task, ("self", "task"))
            _require_parameters(SynchronizeTask, ("future",))
            _require_typed_dict_keys(StatisticData, ("start", "mean", "min", "max"))
            _require_typed_dict_keys(
                StatisticMetaData,
                (
                    "mean_type",
                    "has_sum",
                    "name",
                    "source",
                    "statistic_id",
                    "unit_class",
                    "unit_of_measurement",
                ),
            )
            if not isinstance(Statistics, type):
                return RecorderCompatibilityResult.incompatible_result("statistics_model")

            recorder = get_instance(self.hass)
            future = self.hass.loop.create_future()
            recorder.queue_task(SynchronizeTask(future))
            await asyncio.wait_for(future, timeout=_RECORDER_BARRIER_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except AttributeError, ImportError, TypeError, ValueError:
            return RecorderCompatibilityResult.incompatible_result("recorder_signature")
        except TimeoutError:
            return RecorderCompatibilityResult.incompatible_result("recorder_barrier")
        except Exception:
            # Do not include exception text: Recorder errors can contain
            # configuration paths or other private deployment details.
            return RecorderCompatibilityResult.incompatible_result("recorder_unavailable")

        return RecorderCompatibilityResult.compatible_result()

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the checker for one Home Assistant instance."""
        self.hass = hass


def _require_parameters(callable_obj: Any, expected: tuple[str, ...]) -> None:
    """Raise when a version-sensitive call shape changes."""
    parameters = tuple(inspect.signature(callable_obj).parameters)
    if parameters != expected:
        raise TypeError("Recorder signature changed")


def _require_typed_dict_keys(type_obj: Any, expected: tuple[str, ...]) -> None:
    """Raise when a Recorder TypedDict no longer carries required fields."""
    annotations = getattr(type_obj, "__annotations__", {})
    if any(key not in annotations for key in expected):
        raise TypeError("Recorder model changed")


class GarminHistoryArchive:
    """Deep lifecycle module for one Garmin config entry."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        recorder_checker: RecorderCompatibilityChecker | None = None,
        store_factory: Callable[..., Any] | None = None,
        source_factory: Callable[..., GarminHistorySource] | None = None,
        recorder_factory: Callable[..., GarminHistoryRecorder] | None = None,
    ) -> None:
        """Initialize an archive without doing I/O or creating tasks."""
        self._hass = hass
        self._entry = entry
        self._recorder_checker = recorder_checker or HomeAssistantRecorderCompatibility(hass)
        self._store_factory = store_factory
        self._source_factory = source_factory
        self._recorder_factory = recorder_factory
        self._store: Any | None = None
        self._started = False
        self._tasks: set[asyncio.Task[Any]] = set()
        self._status = HistoryStatus(HistoryArchiveState.IDLE)
        self._completed_dates: set[str] = set()
        self._sync_lock = asyncio.Lock()

    @property
    def status(self) -> HistoryStatus:
        """Return the immutable, privacy-safe current status."""
        return self._status

    async def async_start(self) -> None:
        """Initialize identity, Store catalog, and Recorder compatibility."""
        if self._started:
            return
        self._started = True

        try:
            account_key = self._async_ensure_account_key()
        except asyncio.CancelledError:
            self._started = False
            await self.async_stop()
            raise
        except Exception:
            self._set_failed("identity_initialization")
            _LOGGER.warning(
                "Garmin history archive identity initialization failed for entry %s",
                self._entry.entry_id,
            )
            return

        try:
            await self._async_initialize_store(account_key)
        except asyncio.CancelledError:
            self._started = False
            await self.async_stop()
            raise
        except Exception:
            self._set_failed("store_initialization")
            _LOGGER.warning(
                "Garmin history archive Store initialization failed for entry %s",
                self._entry.entry_id,
            )
            return

        try:
            compatibility = await self._recorder_checker.async_check()
        except asyncio.CancelledError:
            self._started = False
            await self.async_stop()
            raise
        except Exception:
            self._set_failed("startup")
            _LOGGER.warning(
                "Garmin history archive startup failed for entry %s",
                self._entry.entry_id,
            )
            return

        if not compatibility.compatible:
            self._status = HistoryStatus(
                HistoryArchiveState.COMPATIBILITY_DISABLED,
                error_type=compatibility.error_type or "recorder_incompatible",
            )
            _LOGGER.warning(
                "Garmin history archive disabled for entry %s: %s",
                self._entry.entry_id,
                self._status.error_type,
            )
            return

        self._status = HistoryStatus(HistoryArchiveState.IDLE)

    async def async_stop(self) -> None:
        """Stop archive tasks and leave no background work behind."""
        self._started = False
        tasks = tuple(self._tasks)
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def async_sync_range(self, start_date: date, end_date: date) -> HistorySyncReport:
        """Fetch and import the supported intraday metrics for an inclusive range."""
        validation_error = _validate_sync_range(start_date, end_date)
        if validation_error:
            return HistorySyncReport(outcome="invalid", error_type=validation_error)
        if self._status.state is not HistoryArchiveState.IDLE:
            return HistorySyncReport(outcome="disabled", error_type=self._status.error_type)
        if self._sync_lock.locked():
            return HistorySyncReport(outcome="busy", error_type="sync_in_progress")

        async with self._sync_lock:
            return await self._async_sync_range(start_date, end_date)

    async def _async_sync_range(self, start_date: date, end_date: date) -> HistorySyncReport:
        """Run one serialized, checkpointed manual sync."""

        runtime_data = getattr(self._entry, "runtime_data", None)
        client = getattr(getattr(runtime_data, "core", None), "client", None)
        request_gate = getattr(runtime_data, "request_gate", None)
        if client is None:
            return HistorySyncReport(outcome="failed", error_type="integration_not_loaded")
        source = (self._source_factory or GarminHistorySource)(client, request_gate)
        if self._recorder_factory:
            recorder = self._recorder_factory()
        else:
            from homeassistant.helpers.recorder import get_instance

            recorder = GarminHistoryRecorder(get_instance(self._hass))
        processed: list[date] = []
        skipped = 0
        inserted = 0
        self._status = HistoryStatus(HistoryArchiveState.RUNNING)
        for offset in range((end_date - start_date).days + 1):
            target = start_date.fromordinal(start_date.toordinal() + offset)
            target_key = target.isoformat()
            if target_key in self._completed_dates:
                skipped += 1
                continue
            self._status = HistoryStatus(HistoryArchiveState.RUNNING, current_date=target_key, processed_dates=len(processed), record_count=inserted)
            try:
                for metric, metadata in (("heart_rate", HEART_RATE_METADATA), ("stress", STRESS_METADATA)):
                    samples = await source.async_fetch(target, metric)
                    outcome: RecorderWriteOutcome = await recorder.async_write(
                        statistic_id_for(self._account_key(), metric), metadata, samples
                    )
                    if outcome.outcome != "written":
                        return HistorySyncReport(tuple(processed), inserted, skipped_count=skipped, outcome=outcome.outcome, error_type=outcome.error_type)
                    inserted += outcome.accepted_count
                processed.append(target)
                completed_dates = self._completed_dates | {target_key}
                await self._store.async_save({"schema_version": HISTORY_STORE_VERSION, "account_key": self._account_key(), "completed_dates": sorted(completed_dates)})
                self._completed_dates = completed_dates
            except asyncio.CancelledError:
                raise
            except (AttributeError, ImportError, TypeError, ValueError, RuntimeError):
                self._status = HistoryStatus(HistoryArchiveState.FAILED, current_date=target_key, processed_dates=len(processed), record_count=inserted, error_type="sync_failed")
                return HistorySyncReport(tuple(processed), inserted, skipped_count=skipped, outcome="failed", error_type="sync_failed")
        self._status = HistoryStatus(HistoryArchiveState.IDLE, current_date=end_date.isoformat(), processed_dates=len(processed), record_count=inserted)
        return HistorySyncReport(tuple(processed), inserted, skipped_count=skipped, outcome="written")

    def _account_key(self) -> str:
        """Return the persisted opaque account key."""
        account_key = self._entry.data.get(CONF_HISTORY_ACCOUNT_KEY)
        if not isinstance(account_key, str) or not _is_valid_account_key(account_key):
            raise RuntimeError("account identity unavailable")
        return account_key

    async def async_get_calendar_events(
        self,
        calendar: str,
        start_date: date,
        end_date: date,
    ) -> tuple[HistoryCalendarEvent, ...]:
        """Return no Calendar records until the later Store slice exists."""
        del calendar, start_date, end_date
        return ()

    def _async_ensure_account_key(self) -> str:
        """Load or create the opaque identity persisted in the config entry."""
        current = self._entry.data.get(CONF_HISTORY_ACCOUNT_KEY)
        if isinstance(current, str) and _is_valid_account_key(current):
            return current

        account_key = secrets.token_urlsafe(24)
        data = {**self._entry.data, CONF_HISTORY_ACCOUNT_KEY: account_key}
        self._hass.config_entries.async_update_entry(self._entry, data=data)
        return account_key

    async def _async_initialize_store(self, account_key: str) -> None:
        """Create and validate the per-account Store catalog."""
        store_factory = self._store_factory
        if store_factory is None:
            from homeassistant.helpers.storage import Store

            store_factory = Store

        self._store = store_factory(
            self._hass,
            HISTORY_STORE_VERSION,
            f"{DOMAIN}.{self._entry.entry_id}.history_catalog",
            private=True,
            atomic_writes=True,
        )
        catalog = await self._store.async_load()
        if catalog is None:
            await self._store.async_save(
                {
                    "schema_version": HISTORY_STORE_VERSION,
                    "account_key": account_key,
                    "completed_dates": [],
                }
            )
            return
        if not isinstance(catalog, Mapping) or catalog.get("account_key") != account_key:
            raise ValueError("Store identity mismatch")
        completed = catalog.get("completed_dates", [])
        if not isinstance(completed, list) or any(not isinstance(item, str) for item in completed):
            raise ValueError("Store checkpoint is invalid")
        self._completed_dates = set(completed)

    def _set_failed(self, error_type: str) -> None:
        """Set a bounded startup failure without exposing exception details."""
        self._status = HistoryStatus(HistoryArchiveState.FAILED, error_type=error_type)


def _is_valid_account_key(value: str) -> bool:
    """Accept only opaque URL-safe generated-key-shaped values."""
    return _ACCOUNT_KEY_MIN_LENGTH <= len(value) <= _ACCOUNT_KEY_MAX_LENGTH and not (
        set(value) - _ACCOUNT_KEY_ALPHABET
    )


def _validate_sync_range(start_date: date, end_date: date) -> str | None:
    """Validate the bounded inclusive manual sync range."""
    if start_date > end_date:
        return "reversed_range"
    if start_date < _HISTORY_MIN_DATE or end_date < _HISTORY_MIN_DATE:
        return "date_before_minimum"
    if (end_date - start_date).days + 1 > _HISTORY_MAX_DAYS:
        return "range_too_large"
    return None
