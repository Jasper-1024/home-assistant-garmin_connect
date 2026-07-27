"""Privacy-safe Garmin intraday history archive lifecycle and manual sync."""

from __future__ import annotations

import asyncio
import inspect
import logging
import secrets
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
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
    BODY_BATTERY_METADATA,
    DAILY_ABNORMAL_HR_METADATA,
    FLOORS_ASCENDED_DAILY_METADATA,
    FLOORS_ASCENDED_METERS_DAILY_METADATA,
    FLOORS_DESCENDED_DAILY_METADATA,
    FLOORS_DESCENDED_METERS_DAILY_METADATA,
    FLOORS_METADATA,
    HEART_RATE_METADATA,
    MODERATE_INTENSITY_DAILY_METADATA,
    MODERATE_INTENSITY_METADATA,
    NIGHTLY_HRV_METADATA,
    RESPIRATION_AVERAGE_METADATA,
    RESPIRATION_RAW_METADATA,
    SLEEP_BODY_BATTERY_METADATA,
    SLEEP_HEART_RATE_METADATA,
    SLEEP_HRV_METADATA,
    SLEEP_MOVEMENT_METADATA,
    SLEEP_RESPIRATION_METADATA,
    SLEEP_SPO2_METADATA,
    SLEEP_STRESS_METADATA,
    SPO2_CONTINUOUS_METADATA,
    SPO2_HOURLY_METADATA,
    SPO2_SINGLE_METADATA,
    STEPS_DAILY_TOTAL_METADATA,
    STEPS_METADATA,
    STRESS_METADATA,
    TRAINING_ACUTE_LOAD_METADATA,
    TRAINING_ACWR_METADATA,
    TRAINING_CHRONIC_LOAD_METADATA,
    TRAINING_FITNESS_TREND_METADATA,
    TRAINING_LOAD_BALANCE_METADATA,
    TRAINING_RECOVERY_TIME_METADATA,
    TRAINING_VO2_MAX_METADATA,
    VIGOROUS_INTENSITY_DAILY_METADATA,
    VIGOROUS_INTENSITY_METADATA,
    GarminHistoryRecorder,
    RecorderWriteOutcome,
    statistic_id_for,
)
from .history_source import (
    GarminHistorySource,
    HRVData,
    HRVSummary,
    NormalizedHealthEvent,
    NormalizedSample,
    SegmentedData,
    SnapshotData,
    SourceSeries,
    health_event_from_record,
    health_event_record,
)
from .sleep_archive import SleepSchemaError, SleepSession, session_from_record, session_record

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
_PRESENCE_STATES = frozenset({"null", "empty", "missing", "unsupported", "returned-empty", "present", "absent"})
_SLEEP_SCHEMA_VERSION = 1
_SLEEP_STREAM_METADATA = {
    "heart_rate": SLEEP_HEART_RATE_METADATA,
    "hrv": SLEEP_HRV_METADATA,
    "body_battery": SLEEP_BODY_BATTERY_METADATA,
    "stress": SLEEP_STRESS_METADATA,
    "respiration": SLEEP_RESPIRATION_METADATA,
    "spo2": SLEEP_SPO2_METADATA,
    "movement": SLEEP_MOVEMENT_METADATA,
}


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
        self._runtime_sync_failure = False
        self._recorder_adapter: Any | None = None
        self._hrv_summaries: dict[str, dict[str, Any]] = {}
        self._sleep_sessions: dict[str, dict[str, dict[str, Any]]] = {}
        self._health_events: dict[str, dict[str, dict[str, Any]]] = {}
        self._sleep_partition_stores: dict[str, Any] = {}
        self._presence: dict[str, dict[str, str]] = {}

    @property
    def status(self) -> HistoryStatus:
        """Return the immutable, privacy-safe current status."""
        return self._status

    @property
    def hrv_summaries(self) -> Mapping[str, Mapping[str, Any]]:
        """Return the private, bounded HRV summary catalog seam."""
        return {key: dict(value) for key, value in self._hrv_summaries.items()}

    def get_hrv_summaries(self, start_date: date, end_date: date) -> tuple[tuple[date, HRVSummary], ...]:
        """Query the private, bounded persisted HRV summary catalog."""
        if start_date > end_date or (end_date - start_date).days + 1 > _HISTORY_MAX_DAYS:
            return ()
        result: list[tuple[date, HRVSummary]] = []
        for key, value in self._hrv_summaries.items():
            try:
                target = date.fromisoformat(key)
            except ValueError:
                continue
            if not start_date <= target <= end_date:
                continue
            baseline = value.get("baseline")
            result.append((target, HRVSummary(value.get("status"), value.get("last_night_avg"), value.get("last_night_5_min_high"), value.get("weekly_avg"), baseline)))
        return tuple(sorted(result))

    def get_history_presence(self, start_date: date, end_date: date) -> dict[str, dict[str, str]]:
        """Query bounded source availability without payload or identity data."""
        if start_date > end_date or (end_date - start_date).days + 1 > _HISTORY_MAX_DAYS:
            return {}
        return {
            key: dict(value)
            for key, value in self._presence.items()
            if start_date <= date.fromisoformat(key) <= end_date
        }

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
            if not (self._status.state is HistoryArchiveState.FAILED and self._runtime_sync_failure):
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
            self._runtime_sync_failure = True
            self._status = HistoryStatus(HistoryArchiveState.FAILED, error_type="integration_not_loaded")
            return HistorySyncReport(outcome="failed", error_type="integration_not_loaded")
        source = (self._source_factory or GarminHistorySource)(client, request_gate)
        if self._recorder_adapter is None:
            if self._recorder_factory:
                self._recorder_adapter = self._recorder_factory()
            else:
                from homeassistant.helpers.recorder import get_instance

                self._recorder_adapter = GarminHistoryRecorder(get_instance(self._hass))
        recorder = self._recorder_adapter
        store = self._store
        if store is None:
            self._runtime_sync_failure = True
            self._status = HistoryStatus(HistoryArchiveState.FAILED, error_type="store_unavailable")
            return HistorySyncReport(outcome="failed", error_type="store_unavailable")
        processed: list[date] = []
        skipped = 0
        inserted = 0
        updated = 0
        health_events: list[NormalizedHealthEvent] = []
        presence = {key: dict(value) for key, value in self._presence.items()}
        sleep_sessions = {year: dict(records) for year, records in self._sleep_sessions.items()}
        self._status = HistoryStatus(HistoryArchiveState.RUNNING)
        for offset in range((end_date - start_date).days + 1):
            target = start_date.fromordinal(start_date.toordinal() + offset)
            target_key = target.isoformat()
            if target_key in self._completed_dates:
                skipped += 1
                processed.append(target)
                self._status = HistoryStatus(HistoryArchiveState.RUNNING, current_date=target_key, processed_dates=len(processed), record_count=inserted + updated)
                continue
            self._status = HistoryStatus(HistoryArchiveState.RUNNING, current_date=target_key, processed_dates=len(processed), record_count=inserted + updated)
            try:
                for metric, metadata in (
                    ("heart_rate", HEART_RATE_METADATA),
                    ("stress", STRESS_METADATA),
                    ("body_battery", BODY_BATTERY_METADATA),
                    ("nightly_hrv", NIGHTLY_HRV_METADATA),
                    ("steps", STEPS_METADATA),
                    ("floors", FLOORS_METADATA),
                    ("intensity_moderate", MODERATE_INTENSITY_METADATA),
                    ("intensity_vigorous", VIGOROUS_INTENSITY_METADATA),
                    ("respiration_raw", RESPIRATION_RAW_METADATA),
                    ("respiration_average", RESPIRATION_AVERAGE_METADATA),
                    ("spo2_single", SPO2_SINGLE_METADATA),
                    ("spo2_continuous", SPO2_CONTINUOUS_METADATA),
                    ("spo2_hourly", SPO2_HOURLY_METADATA),
                    ("daily_summary", None),
                    ("training_status", None),
                ):
                    try:
                        details_descriptor = inspect.getattr_static(source, "async_fetch_details")
                    except AttributeError:
                        details_descriptor = None
                    details: Any = None
                    if callable(details_descriptor):
                        bound_details = source.async_fetch_details
                        if inspect.iscoroutinefunction(details_descriptor) or inspect.iscoroutinefunction(bound_details):
                            details = bound_details(target, metric)
                        elif callable(bound_details):
                            candidate = bound_details(target, metric)
                            if inspect.isawaitable(candidate) or isinstance(candidate, (HRVData, SegmentedData, SourceSeries, SnapshotData, tuple)):
                                details = candidate
                    if inspect.isawaitable(details):
                        details = await details
                    if not isinstance(details, (HRVData, SegmentedData, SourceSeries, SnapshotData, tuple)):
                        details = await source.async_fetch(target, metric)
                    if isinstance(details, HRVData):
                        samples = details.readings
                        if details.summary is not None:
                            self._hrv_summaries[target_key] = {
                                "status": details.summary.status,
                                "last_night_avg": details.summary.last_night_avg,
                                "last_night_5_min_high": details.summary.last_night_5_min_high,
                                "weekly_avg": details.summary.weekly_avg,
                                "baseline": details.summary.baseline,
                            }
                    elif isinstance(details, SegmentedData):
                        samples = details.readings
                    elif isinstance(details, SourceSeries):
                        samples = details.readings
                        presence.setdefault(target_key, {})[metric] = details.presence
                    elif isinstance(details, SnapshotData):
                        health_events.extend(details.events)
                        samples = ()
                        snapshot_metadata = {
                            "abnormal_heart_rate_alerts": DAILY_ABNORMAL_HR_METADATA,
                            **{
                                "acute_load": TRAINING_ACUTE_LOAD_METADATA,
                                "chronic_load": TRAINING_CHRONIC_LOAD_METADATA,
                                "load_balance": TRAINING_LOAD_BALANCE_METADATA,
                                "acwr": TRAINING_ACWR_METADATA,
                                "vo2_max": TRAINING_VO2_MAX_METADATA,
                                "fitness_trend": TRAINING_FITNESS_TREND_METADATA,
                                "recovery_time": TRAINING_RECOVERY_TIME_METADATA,
                            },
                        }
                        for field, (state, value) in details.fields.items():
                            presence.setdefault(target_key, {})[f"{metric}:{field}"] = state
                            metadata_for_field = snapshot_metadata.get(field)
                            if state != "present" or value is None or metadata_for_field is None:
                                continue
                            snapshot = NormalizedSample(details.timestamp, target, details.raw_timestamp, value)
                            snapshot_outcome = await recorder.async_write(
                                statistic_id_for(self._account_key(), metadata_for_field.key),
                                metadata_for_field,
                                (snapshot,),
                            )
                            if snapshot_outcome.outcome != "written":
                                return HistorySyncReport(tuple(processed), inserted, updated, skipped, outcome=snapshot_outcome.outcome, error_type=snapshot_outcome.error_type)
                            inserted += getattr(snapshot_outcome, "inserted_count", snapshot_outcome.accepted_count)
                            updated += getattr(snapshot_outcome, "updated_count", 0)
                            skipped += getattr(snapshot_outcome, "skipped_count", 0)
                        outcome = RecorderWriteOutcome(0)
                    elif metadata is None:
                        outcome = RecorderWriteOutcome(0)
                    else:
                        samples = details
                    if isinstance(details, SnapshotData) or metadata is None:
                        outcome = RecorderWriteOutcome(0)
                    else:
                        outcome = await recorder.async_write(
                            statistic_id_for(self._account_key(), metric), metadata, samples
                        )
                    if isinstance(details, SegmentedData) and details.totals:
                        total_metadata = {
                            ("steps", "totalSteps"): STEPS_DAILY_TOTAL_METADATA,
                            ("floors", "floorsAscended"): FLOORS_ASCENDED_DAILY_METADATA,
                            ("floors", "floorsDescended"): FLOORS_DESCENDED_DAILY_METADATA,
                            ("floors", "floorsAscendedInMeters"): FLOORS_ASCENDED_METERS_DAILY_METADATA,
                            ("floors", "floorsDescendedInMeters"): FLOORS_DESCENDED_METERS_DAILY_METADATA,
                            ("intensity_moderate", "moderateIntensityMinutes"): MODERATE_INTENSITY_DAILY_METADATA,
                            ("intensity_vigorous", "vigorousIntensityMinutes"): VIGOROUS_INTENSITY_DAILY_METADATA,
                        }
                        for total_key, total_value in details.totals.items():
                            total_metric = total_metadata.get((metric, total_key))
                            if total_metric is None:
                                continue
                            total_sample = NormalizedSample(datetime.combine(target, time.min, tzinfo=UTC), target, target.isoformat(), total_value)
                            total_outcome = await recorder.async_write(statistic_id_for(self._account_key(), total_metric.key), total_metric, (total_sample,))
                            if total_outcome.outcome != "written":
                                self._runtime_sync_failure = True
                                self._status = HistoryStatus(HistoryArchiveState.FAILED, current_date=target_key, processed_dates=len(processed), record_count=inserted + updated, error_type=total_outcome.error_type or "sync_failed")
                                return HistorySyncReport(tuple(processed), inserted, updated, skipped, outcome=total_outcome.outcome, error_type=total_outcome.error_type)
                            inserted += getattr(total_outcome, "inserted_count", total_outcome.accepted_count)
                            updated += getattr(total_outcome, "updated_count", 0)
                            skipped += getattr(total_outcome, "skipped_count", 0)
                    if outcome.outcome != "written":
                        self._runtime_sync_failure = True
                        self._status = HistoryStatus(HistoryArchiveState.FAILED, current_date=target_key, processed_dates=len(processed), record_count=inserted, error_type=outcome.error_type or "sync_failed")
                        return HistorySyncReport(tuple(processed), inserted, updated, skipped, outcome=outcome.outcome, error_type=outcome.error_type)
                    inserted += getattr(outcome, "inserted_count", outcome.accepted_count)
                    updated += getattr(outcome, "updated_count", 0)
                    skipped += getattr(outcome, "skipped_count", 0)
                try:
                    sleep_descriptor = inspect.getattr_static(source, "async_fetch_details")
                except AttributeError:
                    sleep_descriptor = None
                sleep_fetch = getattr(source, "async_fetch_details", None)
                sleep_details = (
                    await sleep_fetch(target, "sleep_sessions")
                    if callable(sleep_descriptor) and callable(sleep_fetch)
                    else ()
                )
                if not isinstance(sleep_details, tuple) or any(not isinstance(item, SleepSession) for item in sleep_details):
                    raise SleepSchemaError("sleep session result has invalid shape")
                for session in sleep_details:
                    year = str(session.start.year)
                    sleep_sessions.setdefault(year, {})[session.logical_id] = session_record(session)
                    for stream in session.streams:
                        metadata_for_stream = _SLEEP_STREAM_METADATA.get(stream.metric)
                        if metadata_for_stream is None:
                            raise SleepSchemaError("sleep stream metric is unsupported")
                        samples = tuple(
                            NormalizedSample(point.timestamp, session.calendar_date, point.raw_timestamp, point.value)
                            for point in stream.points
                            if point.value is not None and point.value >= 0
                        )
                        stream_outcome = await recorder.async_write(
                            statistic_id_for(
                                self._account_key(),
                                f"{metadata_for_stream.key}:{session.logical_id}",
                            ),
                            metadata_for_stream,
                            samples,
                        )
                        if stream_outcome.outcome != "written":
                            raise RuntimeError(stream_outcome.error_type or "sleep_stream_write_failed")
                        inserted += getattr(stream_outcome, "inserted_count", stream_outcome.accepted_count)
                        updated += getattr(stream_outcome, "updated_count", 0)
                        skipped += getattr(stream_outcome, "skipped_count", 0)
                for event_metric in ("health_events_daily", "health_events_body_battery"):
                    event_details = await sleep_fetch(target, event_metric) if callable(sleep_descriptor) and callable(sleep_fetch) else ()
                    if not isinstance(event_details, tuple) or any(not isinstance(item, NormalizedHealthEvent) for item in event_details):
                        raise ValueError("health event result has invalid shape")
                    health_events.extend(event_details)
                events_by_year = {year: dict(records) for year, records in self._health_events.items()}
                for event in health_events:
                    year = str((event.start or event.occurrence or datetime.combine(event.calendar_date, time.min, tzinfo=UTC)).year)
                    events_by_year.setdefault(year, {})[event.logical_id] = health_event_record(event)
                processed.append(target)
                completed_dates = self._completed_dates | {target_key}
                await self._async_save_sleep_partitions(sleep_sessions, events_by_year)
                # Publish the catalog checkpoint only after every affected annual
                # partition is durable. A failed partition save must be replayed.
                await store.async_save({"schema_version": HISTORY_STORE_VERSION, "sleep_schema_version": _SLEEP_SCHEMA_VERSION, "account_key": self._account_key(), "completed_dates": sorted(completed_dates), "hrv_summaries": self._hrv_summaries, "presence": presence, "sleep_index": {year: sorted(records) for year, records in sleep_sessions.items()}, "event_index": {year: sorted(records) for year, records in events_by_year.items()}})
                self._completed_dates = completed_dates
                self._presence = presence
                self._sleep_sessions = sleep_sessions
                self._health_events = events_by_year
            except asyncio.CancelledError:
                self._status = HistoryStatus(HistoryArchiveState.IDLE, current_date=target_key, processed_dates=len(processed), record_count=inserted + updated)
                raise
            except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
                self._runtime_sync_failure = True
                self._status = HistoryStatus(HistoryArchiveState.FAILED, current_date=target_key, processed_dates=len(processed), record_count=inserted, error_type="sync_failed")
                return HistorySyncReport(tuple(processed), inserted, updated, skipped, outcome="failed", error_type="sync_failed")
        self._runtime_sync_failure = False
        self._status = HistoryStatus(HistoryArchiveState.IDLE, current_date=end_date.isoformat(), processed_dates=len(processed), record_count=inserted + updated)
        return HistorySyncReport(tuple(processed), inserted, updated, skipped, outcome="written")

    async def _async_save_sleep_partitions(
        self, sessions_by_year: Mapping[str, Mapping[str, dict[str, Any]]],
        events_by_year: Mapping[str, Mapping[str, dict[str, Any]]] | None = None,
    ) -> None:
        """Atomically checkpoint each annual sleep partition."""
        store_factory = self._store_factory
        if store_factory is None:
            from homeassistant.helpers.storage import Store

            store_factory = Store
        for year in set(sessions_by_year) | set(events_by_year or {}):
            records = sessions_by_year.get(year, {})
            if year not in self._sleep_partition_stores:
                self._sleep_partition_stores[year] = store_factory(
                    self._hass,
                    HISTORY_STORE_VERSION,
                    f"{DOMAIN}.{self._entry.entry_id}.sleep_{year}",
                    private=True,
                    atomic_writes=True,
                )
            await self._sleep_partition_stores[year].async_save(
                {
                    "schema_version": HISTORY_STORE_VERSION,
                    "sleep_schema_version": _SLEEP_SCHEMA_VERSION,
                    "account_key": self._account_key(),
                    "year": year,
                    "sessions": dict(records),
                    "events": dict((events_by_year or {}).get(year, {})),
                }
            )

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
        """Return privacy-safe structured sleep and nap events."""
        if calendar not in {"sleep", "health"}:
            return ()
        if start_date > end_date:
            return ()
        await self._async_load_sleep_partitions(
            {str(year) for year in range(start_date.year - 1, end_date.year + 2)}
        )
        events: dict[tuple[datetime, datetime, str], HistoryCalendarEvent] = {}
        if calendar == "health":
            for records in self._health_events.values():
                for record in records.values():
                    start = datetime.fromisoformat(record["start"]) if record.get("start") else None
                    end = datetime.fromisoformat(record["end"]) if record.get("end") else start
                    occurrence = datetime.fromisoformat(record["occurrence"]) if record.get("occurrence") else start
                    point = occurrence or start
                    if point is not None and start_date <= point.date() <= end_date:
                        summary = str(record.get("category") or record.get("event_type") or "Health event")[:64]
                        events[(point, end or point, summary)] = HistoryCalendarEvent(point, end or point, summary)
            return tuple(sorted(events.values(), key=lambda event: event.start))
        for records in self._sleep_sessions.values():
            for record in records.values():
                start = datetime.fromisoformat(record["start"])
                end = datetime.fromisoformat(record["end"])
                if start.date() <= end_date and end.date() >= start_date:
                    summary = "Sleep" if record["kind"] == "main" else "Nap"
                    events[(start, end, summary)] = HistoryCalendarEvent(start, end, summary)
        return tuple(sorted(events.values(), key=lambda event: event.start))

    async def _async_load_sleep_partitions(self, years: set[str]) -> None:
        """Load only requested annual partitions; ignore bad data safely."""
        store_factory = self._store_factory
        if store_factory is None:
            from homeassistant.helpers.storage import Store

            store_factory = Store
        for year in years:
            self._sleep_sessions.pop(year, None)
            self._health_events.pop(year, None)
            if year not in self._sleep_partition_stores:
                self._sleep_partition_stores[year] = store_factory(
                    self._hass,
                    HISTORY_STORE_VERSION,
                    f"{DOMAIN}.{self._entry.entry_id}.sleep_{year}",
                    private=True,
                    atomic_writes=True,
                )
            try:
                partition = await self._sleep_partition_stores[year].async_load()
                if partition is None:
                    continue
                if (
                    not isinstance(partition, Mapping)
                    or partition.get("account_key") != self._account_key()
                    or partition.get("year") != year
                    or partition.get("sleep_schema_version", _SLEEP_SCHEMA_VERSION) != _SLEEP_SCHEMA_VERSION
                or not isinstance(partition.get("sessions", {}), Mapping)
                ):
                    continue
                parsed: dict[str, dict[str, Any]] = {}
                for logical_id, record in partition.get("sessions", {}).items():
                    if not isinstance(record, Mapping):
                        raise SleepSchemaError("sleep partition record is invalid")
                    restored = session_from_record(dict(record))
                    if restored.logical_id != logical_id or str(restored.start.year) != year:
                        raise SleepSchemaError("sleep partition record is invalid")
                    parsed[logical_id] = dict(record)
                self._sleep_sessions[year] = parsed
                raw_events = partition.get("events", {})
                if not isinstance(raw_events, Mapping):
                    raise SleepSchemaError("health event partition is invalid")
                parsed_events: dict[str, dict[str, Any]] = {}
                for logical_id, record in raw_events.items():
                    restored_event = health_event_from_record(record)
                    if restored_event.logical_id != logical_id:
                        raise SleepSchemaError("health event partition is invalid")
                    parsed_events[logical_id] = health_event_record(restored_event)
                self._health_events[year] = parsed_events
            except (KeyError, TypeError, ValueError, OSError):
                self._sleep_sessions.pop(year, None)
                self._health_events.pop(year, None)

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
                    "hrv_summaries": {},
                    "presence": {},
                    "sleep_index": {},
                    "event_index": {},
                }
            )
            return
        if not isinstance(catalog, Mapping) or catalog.get("account_key") != account_key:
            raise ValueError("Store identity mismatch")
        completed = catalog.get("completed_dates", [])
        if not isinstance(completed, list) or any(not isinstance(item, str) for item in completed):
            raise ValueError("Store checkpoint is invalid")
        if len(set(completed)) != len(completed):
            raise ValueError("Store checkpoint is invalid")
        parsed_dates: set[str] = set()
        for item in completed:
            try:
                parsed = date.fromisoformat(item)
            except ValueError as err:
                raise ValueError("Store checkpoint is invalid") from err
            if parsed.isoformat() != item or parsed < _HISTORY_MIN_DATE:
                raise ValueError("Store checkpoint is invalid")
            parsed_dates.add(item)
        self._completed_dates = parsed_dates
        summaries = catalog.get("hrv_summaries", {})
        if isinstance(summaries, Mapping):
            self._hrv_summaries = {key: dict(value) for key, value in summaries.items() if isinstance(key, str) and isinstance(value, Mapping)}
        raw_presence = catalog.get("presence", {})
        if not isinstance(raw_presence, Mapping):
            raise ValueError("Store presence catalog is invalid")
        parsed_presence: dict[str, dict[str, str]] = {}
        for key, metrics in raw_presence.items():
            if not isinstance(key, str):
                raise ValueError("Store presence catalog is invalid")
            parsed_date = date.fromisoformat(key)
            if parsed_date.isoformat() != key or parsed_date < _HISTORY_MIN_DATE:
                raise ValueError("Store presence catalog is invalid")
            if not isinstance(metrics, Mapping) or len(metrics) > 32:
                raise ValueError("Store presence catalog is invalid")
            bounded: dict[str, str] = {}
            for metric, state in metrics.items():
                if not isinstance(metric, str) or len(metric) > 64 or state not in _PRESENCE_STATES:
                    raise ValueError("Store presence catalog is invalid")
                bounded[metric] = state
            parsed_presence[key] = bounded
        self._presence = parsed_presence
        raw_sleep = catalog.get("sleep_index", catalog.get("sleep_sessions", {}))
        raw_events = catalog.get("event_index", {})
        if catalog.get("sleep_schema_version", _SLEEP_SCHEMA_VERSION) != _SLEEP_SCHEMA_VERSION:
            raise ValueError("Sleep catalog version is unsupported")
        if not isinstance(raw_sleep, Mapping):
            raise ValueError("Sleep catalog is invalid")
        sleep_years: set[str] = set()
        for year, records in raw_sleep.items():
            if not isinstance(year, str) or len(year) != 4 or not year.isdecimal():
                raise ValueError("Sleep catalog is invalid")
            if isinstance(records, Mapping):
                # Read legacy catalogs once, but never use their records as the
                # source of truth after startup.
                records = list(records)
            if not isinstance(records, list) or len(records) > 10000:
                raise ValueError("Sleep catalog is invalid")
            if any(not isinstance(logical_id, str) or len(logical_id) > 64 for logical_id in records):
                raise ValueError("Sleep catalog is invalid")
            sleep_years.add(year)
        if not isinstance(raw_events, Mapping):
            raise ValueError("Health event catalog is invalid")
        for year, records in raw_events.items():
            if not isinstance(year, str) or len(year) != 4 or not year.isdecimal() or not isinstance(records, list) or len(records) > 10000:
                raise ValueError("Health event catalog is invalid")
            sleep_years.add(year)
        self._sleep_sessions = {}
        await self._async_load_sleep_partitions(sleep_years)
        missing_years = sleep_years - set(self._sleep_sessions)
        if missing_years:
            self._completed_dates = {
                value for value in self._completed_dates
                if value[:4] not in missing_years
            }

    def _set_failed(self, error_type: str) -> None:
        """Set a bounded startup failure without exposing exception details."""
        self._runtime_sync_failure = False
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
