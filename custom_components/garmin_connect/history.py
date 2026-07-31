"""Privacy-safe Garmin intraday history archive lifecycle and manual sync."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import inspect
import json
import logging
import os
import secrets
import stat
from collections.abc import Callable, Collection, Iterator, Mapping
from dataclasses import dataclass, field, fields, is_dataclass, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal, Protocol, cast

from ha_garmin.exceptions import (
    GarminAuthError,
    GarminConnectError,
    GarminRateLimitError,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .backfill import (
    BackfillScheduler,
    BackfillState,
    count_uncompleted_dates,
)
from .const import (
    CONF_ARCHIVE_ACTIVATION_DATE,
    CONF_ARCHIVE_ENABLED,
    CONF_ARCHIVE_PREVIOUSLY_ENABLED,
    CONF_HISTORY_ACCOUNT_KEY,
    DOMAIN,
    HISTORY_OWNER_FINGERPRINT,
    HISTORY_STORE_VERSION,
    RECORDER_COMPATIBILITY_TARGET,
)
from .fit_archive import (
    FitArchiveError,
    async_archive_fit,
    ensure_private_fit_directory,
    fit_file_name,
    fit_record,
    fsync_directory,
    inspect_fit,
    validate_private_fit_file,
    validated_fit_summary,
)
from .fit_queue import serialize_fit_pacing, serialize_fit_queue
from .history_calendar import (
    HistoryCalendarEvent,
    add_structured_calendar_event,
    project_activity_interval,
    project_health_interval,
)
from .history_recorder import (
    BODY_BATTERY_METADATA,
    DAILY_ABNORMAL_HR_METADATA,
    FLOORS_ASCENDED_DAILY_METADATA,
    FLOORS_ASCENDED_METERS_DAILY_METADATA,
    FLOORS_DESCENDED_DAILY_METADATA,
    FLOORS_DESCENDED_METERS_DAILY_METADATA,
    FLOORS_METADATA,
    FLOORS_TOTAL_DAILY_METADATA,
    HEART_RATE_METADATA,
    INTENSITY_TOTAL_DAILY_METADATA,
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
    _RecorderBarrierTimeoutError,
    _require_durable_import_statistics_contract,
    _require_recorder_task_contract,
    async_confirm_recorder_queue,
    statistic_id_for,
)
from .history_source import (
    GarminHistorySource,
    HRVData,
    HRVSummary,
    NormalizedActivity,
    NormalizedHealthEvent,
    NormalizedSample,
    SegmentedData,
    SnapshotData,
    SourceSeries,
    activity_from_record,
    health_event_from_record,
    health_event_record,
)
from .request_gate import GarminRequestPriority
from .sleep_archive import SleepSchemaError, SleepSession, session_from_record, session_record

_LOGGER = logging.getLogger(__name__)

_ACCOUNT_KEY_MIN_LENGTH = 20
_ACCOUNT_KEY_MAX_LENGTH = 128
_ACCOUNT_KEY_ALPHABET = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
)
_RECORDER_MINIMUM_HOME_ASSISTANT_VERSION = (2026, 7, 4)
_HISTORY_MIN_DATE = date(2026, 1, 1)
_HISTORY_MAX_DAYS = 31
_FIRST_SYNC_FIT_LIMIT = 1
_PROSPECTIVE_CYCLE_INTERVAL = timedelta(minutes=15)
_RECONCILIATION_WINDOW = timedelta(days=7)
_FIT_PACING_INTERVAL = timedelta(hours=1)
_PROSPECTIVE_CYCLE_FIT_LIMIT = 1
_ARCHIVE_RATE_LIMIT_BACKOFF = timedelta(hours=24)
_FIT_LOGICAL_ID_LENGTH = 24
_FIT_QUEUE_QUARANTINE_MAX_ITEMS = 256
_FIT_QUEUE_FIELDS = frozenset(
    {"logical_id", "activity_id", "year", "calendar_date"}
)
_DATE_SUMMARY_BUCKET_TIME_ZONE = timezone(timedelta(hours=8))
_PRESENCE_STATES = frozenset({"null", "empty", "all-null", "missing", "unsupported", "returned-empty", "present", "absent", "failed", "mixed", "unknown", "partial", "incomplete"})
# The frozen numeric catalog currently produces 33 base presence keys: 13
# families, 12 segmented total contexts, and 8 snapshot fields. Seven aggregate
# sleep-stream states retain date-level availability without session-key growth.
_MAX_PRESENCE_METRICS = 64
_SLEEP_SCHEMA_VERSION = 1
_SLEEP_PRESENCE_PREFIX = "sleep_stream:"
_SLEEP_STREAM_METADATA = {
    "heart_rate": SLEEP_HEART_RATE_METADATA,
    "hrv": SLEEP_HRV_METADATA,
    "body_battery": SLEEP_BODY_BATTERY_METADATA,
    "stress": SLEEP_STRESS_METADATA,
    "respiration": SLEEP_RESPIRATION_METADATA,
    "spo2": SLEEP_SPO2_METADATA,
    "movement": SLEEP_MOVEMENT_METADATA,
}
# Garmin documents -1 as the no-data sentinel for sleep stress; it is not a
# numeric measurement and therefore is not sent to Recorder.
_SLEEP_NEGATIVE_SENTINELS = {"stress": frozenset({-1.0})}
_NUMERIC_FAMILY_METADATA = (
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
)
_STRUCTURED_FAMILIES = (
    "sleep_sessions",
    "health_events_daily",
    "health_events_body_battery",
    "timed_activities",
)
_FROZEN_ARCHIVE_FAMILIES = tuple(
    family for family, _metadata in _NUMERIC_FAMILY_METADATA
) + _STRUCTURED_FAMILIES + ("sleep_stream",)
_RECONCILIATION_FAMILIES = _FROZEN_ARCHIVE_FAMILIES
_RECONCILIATION_EXPLICIT_EMPTY_STATES = frozenset(
    {"empty", "all-null", "null", "returned-empty", "absent"}
)
_RECONCILIATION_UNAVAILABLE_STATES = frozenset(
    {"missing", "failed", "unsupported", "unknown", "partial", "incomplete"}
)
_RECONCILIATION_EVIDENCE_RANK = {
    "failed": 4,
    "incomplete": 3,
    "missing": 2,
    "partial": 2,
    "unsupported": 2,
    "unknown": 2,
}
_RECONCILIATION_OUTCOMES = frozenset(
    {"records", "empty", "incomplete", "failed", "continuity_gap"}
)
def _fingerprint_value(value: Any) -> Any:
    """Convert normalized source details into deterministic private JSON data."""
    if is_dataclass(value):
        return {
            field.name: _fingerprint_value(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _fingerprint_value(item)
            for key, item in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (tuple, list)):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


def _fingerprint_details(value: Any) -> str:
    """Return a stable, non-public fingerprint for one returned family."""
    payload = json.dumps(
        _fingerprint_value(value), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def _runtime_request_executor(runtime_data: Any) -> Any:
    """Return the loaded runtime lifecycle, or a legacy injected gate."""
    # Keep the import local: coordinator imports history for its archive type.
    from .coordinator import BaseGarminCoordinator, GarminConnectCoordinators

    if isinstance(runtime_data, GarminConnectCoordinators) and isinstance(
        runtime_data.core, BaseGarminCoordinator
    ):
        return runtime_data
    return getattr(runtime_data, "request_gate", None)


@dataclass(frozen=True, slots=True)
class _NormalizedDetailAdapter:
    """Expose every normalized response through one private detail adapter."""

    details: Any
    detail_type: str | None
    records: bool
    presence_resolver: Callable[[bool], str]
    samples: Any
    write_samples: bool = False
    metric_presence: str | None = None
    total_presence: Mapping[str, str] = field(default_factory=dict)
    totals: Mapping[str, float] | None = None
    summary: HRVSummary | None = None
    events: tuple[NormalizedHealthEvent, ...] = ()
    fields: Mapping[str, tuple[str, float | None]] | None = None

    @property
    def supported(self) -> bool:
        return self.detail_type is not None

    def has_records(self) -> bool:
        return self.records

    def presence(self, available: bool) -> str:
        return self.presence_resolver(available)


def _snapshot_presence(details: SnapshotData, available: bool) -> str:
    states = {state for state, _value in details.fields.values()}
    if "present" in states or details.events:
        return "present"
    if states & _RECONCILIATION_UNAVAILABLE_STATES:
        return "missing"
    return "empty" if available else "missing"


def _tuple_presence(details: tuple, available: bool) -> str:
    return "present" if details else "empty" if available else "missing"


def _normalized_detail_adapter(details: Any) -> _NormalizedDetailAdapter:
    if isinstance(details, HRVData):
        return _NormalizedDetailAdapter(
            details,
            "hrv",
            bool(details.readings or details.summary is not None),
            lambda _available: details.presence,
            details.readings,
            write_samples=True,
            metric_presence=details.presence,
            summary=details.summary,
        )
    if isinstance(details, SegmentedData):
        return _NormalizedDetailAdapter(
            details,
            "segmented",
            bool(details.readings or details.totals),
            lambda _available: details.presence,
            details.readings,
            write_samples=True,
            metric_presence=details.presence,
            total_presence=details.total_presence,
            totals=details.totals,
        )
    if isinstance(details, SourceSeries):
        return _NormalizedDetailAdapter(
            details,
            "series",
            bool(details.readings or details.presence == "present"),
            lambda _available: details.presence,
            details.readings,
            write_samples=True,
            metric_presence=details.presence,
        )
    if isinstance(details, SnapshotData):
        return _NormalizedDetailAdapter(
            details,
            "snapshot",
            bool(
                details.events
                or any(
                    state == "present" and value is not None
                    for state, value in details.fields.values()
                )
            ),
            lambda available: _snapshot_presence(details, available),
            (),
            events=details.events,
            fields=details.fields,
        )
    if isinstance(details, tuple):
        return _NormalizedDetailAdapter(
            details,
            "tuple",
            bool(details),
            lambda available: _tuple_presence(details, available),
            details,
        )
    return _NormalizedDetailAdapter(details, None, False, lambda _available: "unknown", details)


@dataclass(frozen=True, slots=True)
class _NormalizedDetailRecord:
    """Pair normalized details with their single dispatch adapter."""

    details: Any
    adapter: _NormalizedDetailAdapter

    @property
    def detail_type(self) -> str | None:
        return self.adapter.detail_type


def _normalized_detail_record(details: Any) -> _NormalizedDetailRecord:
    """Build the normalized detail record consumed by numeric import dispatch."""
    return _NormalizedDetailRecord(details, _normalized_detail_adapter(details))


def _details_have_records(details: Any) -> bool:
    """Return whether normalized details contain a Source Record."""
    return _normalized_detail_adapter(details).has_records()


def _details_presence(details: Any, *, available: bool = True) -> str:
    """Return presence from this normalized response, never from prior state."""
    return _normalized_detail_adapter(details).presence(available)


@dataclass(frozen=True, slots=True)
class _FamilyObservation:
    """One normalized family observation used by reconciliation bookkeeping."""

    details: Any
    presence: str
    fingerprint: str
    has_records: bool
    error_type: str | None = None

    @classmethod
    def from_details(
        cls, details: Any, *, available: bool = True
    ) -> _FamilyObservation:
        has_records = _details_have_records(details)
        return cls(
            details,
            _details_presence(details, available=available),
            _fingerprint_details(details),
            has_records,
        )

    @classmethod
    def failed(cls, family: str, error_type: str) -> _FamilyObservation:
        """Build bounded evidence for a family that could not be observed."""
        return cls(
            (),
            "failed",
            _fingerprint_details({"family": family, "error_type": error_type}),
            False,
            error_type,
        )


@dataclass(slots=True)
class _FamilyObservationAccumulator:
    """Collect one durable observation per frozen family."""

    observations: dict[str, _FamilyObservation]

    @classmethod
    def create(cls) -> _FamilyObservationAccumulator:
        return cls({})

    def record(self, family: str, observation: _FamilyObservation) -> None:
        previous = self.observations.get(family)
        if previous is not None:
            merged_presence = _merge_reconciliation_evidence(
                previous.presence, observation.presence
            )
            if merged_presence != observation.presence:
                return
        self.observations[family] = observation

    def record_failure(self, family: str, error_type: str) -> _FamilyObservation:
        observation = _FamilyObservation.failed(family, error_type)
        self.record(family, observation)
        return observation

    @property
    def has_records(self) -> bool:
        return any(observation.has_records for observation in self.observations.values())

    @property
    def fingerprints(self) -> dict[str, str]:
        return {
            family: observation.fingerprint
            for family, observation in self.observations.items()
        }

    @property
    def presence(self) -> dict[str, str]:
        return {
            family: observation.presence
            for family, observation in self.observations.items()
        }

@dataclass(frozen=True, slots=True)
class _ReconciliationObservation:
    """One date-level observation, including incomplete failed attempts."""

    fingerprint: str
    has_records: bool
    complete: bool
    explicit_empty: bool


@dataclass(slots=True)
class _ReconciliationEntry:
    """Typed in-memory form of one durable reconciliation ledger entry."""

    state: Literal["open", "settled"]
    fingerprint: str | None
    has_records: bool
    outcome: Literal["records", "empty", "incomplete", "failed", "continuity_gap"]

    def as_record(self) -> dict[str, Any]:
        """Return the stable JSON representation."""
        return {
            "state": self.state,
            "fingerprint": self.fingerprint,
            "has_records": self.has_records,
            "outcome": self.outcome,
        }


def _reconciliation_records(
    entries: Mapping[str, _ReconciliationEntry],
) -> dict[str, dict[str, Any]]:
    """Return the stable JSON representation of the reconciliation ledger."""
    return {key: value.as_record() for key, value in entries.items()}


async def _async_observe_family(
    fetch: Callable[..., Any] | None,
    target: date,
    family: str,
    accumulator: _FamilyObservationAccumulator,
) -> _FamilyObservation:
    """Read one structured family and record its bounded observation."""
    available = fetch is not None
    try:
        details = await fetch(target, family) if fetch is not None else ()
        observation = _FamilyObservation.from_details(details, available=available)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        error_type = _safe_family_error_type(error)
        if error_type in {"rate_limited", "reauth_required"}:
            raise _ArchivePolicyError(error_type) from error
        observation = accumulator.record_failure(family, error_type)
        _LOGGER.warning("Garmin structured family failed for %s (%s)", target, family)
        return observation
    accumulator.record(family, observation)
    return observation


def _safe_family_error_type(error: BaseException) -> str:
    """Map a family exception to a privacy-safe public error class."""
    return _archive_error_type(error)


def _archive_error_type(error: BaseException) -> str:
    """Classify an archive failure without exposing exception details."""
    status = getattr(error, "status", None)
    if status is None:
        status = getattr(error, "status_code", None)
    if isinstance(error, GarminRateLimitError) or status == 429:
        return "rate_limited"
    if isinstance(error, GarminAuthError) or status == 401:
        return "reauth_required"
    return "garmin_client_error" if isinstance(error, GarminConnectError) else "sync_failed"


def _merge_reconciliation_evidence(previous: str | None, current: str) -> str:
    """Retain the most severe reconciliation evidence across observations."""
    previous_rank = _RECONCILIATION_EVIDENCE_RANK.get(previous or "", 0)
    current_rank = _RECONCILIATION_EVIDENCE_RANK.get(current, 0)
    if previous_rank > current_rank:
        return cast(str, previous)
    return current


def _date_reconciliation_observation(
    accumulator: _FamilyObservationAccumulator,
    *,
    family_presence: Mapping[str, str] | None = None,
) -> _ReconciliationObservation:
    """Build the date observation used for complete and partial attempts."""
    family_presence = accumulator.presence if family_presence is None else family_presence
    complete = all(
        family in family_presence
        and family_presence[family] not in _RECONCILIATION_UNAVAILABLE_STATES
        for family in _FROZEN_ARCHIVE_FAMILIES
    )
    return _ReconciliationObservation(
        _fingerprint_details(accumulator.fingerprints),
        accumulator.has_records,
        complete,
        complete
        and not accumulator.has_records
        and all(
            family_presence[family] in _RECONCILIATION_EXPLICIT_EMPTY_STATES
            for family in _FROZEN_ARCHIVE_FAMILIES
        ),
    )


def _aggregate_sleep_presence(
    sessions_by_year: Mapping[str, Mapping[str, dict[str, Any]]], target: date
) -> dict[str, str]:
    """Summarize per-session sleep availability into seven bounded date keys."""
    states_by_metric: dict[str, list[str]] = {}
    for records in sessions_by_year.values():
        for record in records.values():
            if record.get("calendar_date") != target.isoformat():
                continue
            stream_presence = record.get("stream_presence")
            if not isinstance(stream_presence, Mapping):
                continue
            for metric, state in stream_presence.items():
                if metric in _SLEEP_STREAM_METADATA and state in _PRESENCE_STATES:
                    states_by_metric.setdefault(metric, []).append(state)
    aggregate: dict[str, str] = {}
    for metric, states in states_by_metric.items():
        distinct = set(states)
        aggregate[f"{_SLEEP_PRESENCE_PREFIX}{metric}"] = (
            next(iter(distinct))
            if len(distinct) == 1
            else "present" if "present" in distinct else "mixed"
        )
    return aggregate


def _sleep_stream_observation(
    sleep_details: tuple[SleepSession, ...],
) -> _FamilyObservation:
    """Build one reconciliation observation for the raw sleep-stream family."""
    streams = tuple(
        (session.logical_id, stream)
        for session in sleep_details
        for stream in session.streams
    )
    if not sleep_details:
        presence = "empty"
    elif not streams:
        presence = "missing"
    else:
        streams_by_session = {
            session.logical_id: {stream.metric: stream for stream in session.streams}
            for session in sleep_details
        }
        if any(
            any(metric not in session_streams for metric in _SLEEP_STREAM_METADATA)
            for session_streams in streams_by_session.values()
        ):
            presence = "missing"
        else:
            stream_states = {
                stream.presence
                for session_streams in streams_by_session.values()
                for stream in session_streams.values()
            }
            if stream_states & _RECONCILIATION_UNAVAILABLE_STATES:
                presence = "missing"
            elif "present" in stream_states:
                presence = "present"
            elif "all-null" in stream_states:
                presence = "all-null"
            elif "null" in stream_states:
                presence = "null"
            else:
                presence = "empty"
    has_records = any(
        point.value is not None
        and point.value not in _SLEEP_NEGATIVE_SENTINELS.get(stream.metric, frozenset())
        for _session_id, stream in streams
        for point in stream.points
    )
    return _FamilyObservation(
        streams,
        presence,
        _fingerprint_details(streams),
        has_records,
    )


class HistoryArchiveState(StrEnum):
    """Observable states of the history archive."""

    IDLE = "idle"
    DISABLED = "disabled"
    SYNCING = "syncing"
    BACKOFF = "backoff"
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
    queued_count: int = 0
    completed_count: int = 0
    next_eligible_run: str | None = None
    last_success: str | None = None
    backoff_until: str | None = None
    safe_error_class: str | None = None
    activation_date: str | None = None

    def as_attributes(self) -> dict[str, Any]:
        """Return only the minimal lifecycle contract exposed by the sensor."""
        attributes: dict[str, Any] = {"archive_state": self.state.value}
        for key, value in (
            ("activation_date", self.activation_date),
            ("last_success", self.last_success),
            ("next_eligible_run", self.next_eligible_run),
            ("safe_error_class", self.safe_error_class or self.error_type),
        ):
            if value is not None:
                attributes[key] = value
        return attributes


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
    date_results: tuple[tuple[date, HistorySyncReport], ...] = ()


class _InvalidArchiveActivationDateError(ValueError):
    """Raised when enabled archival has no trustworthy persisted boundary."""


class _NumericFamilyError(RuntimeError):
    """Raised when one numeric family cannot be archived for a date."""

    def __init__(
        self,
        error_type: str = "numeric_family_failed",
        *,
        write_failure: bool = False,
        observation: _FamilyObservation | None = None,
    ) -> None:
        super().__init__(error_type)
        self.error_type = error_type
        self.write_failure = write_failure
        self.observation = observation


class _ArchivePolicyError(RuntimeError):
    """Stop archive work when a policy requires a state transition."""

    def __init__(self, error_type: str) -> None:
        super().__init__(error_type)
        self.error_type = error_type


@dataclass(frozen=True, slots=True)
class _NumericImportResult:
    """Counts plus the cohesive observation returned by one numeric family."""

    inserted_count: int
    updated_count: int
    skipped_count: int
    observation: _FamilyObservation


@dataclass(slots=True)
class _StructuredCheckpoint:
    """Mutable annual structured state carried through one sync date."""

    presence: dict[str, dict[str, str]]
    sessions_by_year: dict[str, dict[str, dict[str, Any]]]
    events_by_year: dict[str, dict[str, dict[str, Any]]]
    activities_by_year: dict[str, dict[str, dict[str, Any]]]
    dirty_years: set[str]


def _fit_queue_entry(
    logical_id: str,
    activity_id: str,
    year: str,
    calendar_date: date,
) -> dict[str, str]:
    """Return one bounded, account-local pending FIT record."""
    return {
        "logical_id": logical_id,
        "activity_id": activity_id,
        "year": year,
        "calendar_date": calendar_date.isoformat(),
    }


def _parse_fit_queue_entry(item: Mapping[str, Any]) -> dict[str, str]:
    """Validate one durable FIT queue record without coercing identity."""
    if set(item) != _FIT_QUEUE_FIELDS:
        raise ValueError("FIT queue entry fields are invalid")
    logical_id = item.get("logical_id")
    activity_id = item.get("activity_id")
    year = item.get("year")
    calendar_date = item.get("calendar_date")
    if (
        not isinstance(logical_id, str)
        or len(logical_id) != _FIT_LOGICAL_ID_LENGTH
        or any(character not in "0123456789abcdef" for character in logical_id)
        or not isinstance(activity_id, str)
        or not activity_id.isdecimal()
        or len(activity_id) > 32
        or str(int(activity_id)) != activity_id
        or not isinstance(year, str)
        or len(year) != 4
        or not year.isdecimal()
        or not isinstance(calendar_date, str)
    ):
        raise ValueError("FIT queue entry identity is invalid")
    try:
        parsed_calendar_date = date.fromisoformat(calendar_date)
    except ValueError as err:
        raise ValueError("FIT queue entry date is invalid") from err
    if (
        parsed_calendar_date.isoformat() != calendar_date
        or str(parsed_calendar_date.year) != year
        or parsed_calendar_date < _HISTORY_MIN_DATE
    ):
        raise ValueError("FIT queue entry date is invalid")
    return _fit_queue_entry(logical_id, activity_id, year, parsed_calendar_date)


HistoryArchiveClock = Callable[[], datetime]
HistoryArchiveTimerFactory = Callable[
    [timedelta, Callable[[], None]], Callable[[], None]
]


def _default_history_timer_factory(
    delay: timedelta, callback: Callable[[], None]
) -> Callable[[], None]:
    """Schedule one archive wakeup on the active Home Assistant loop."""
    handle = asyncio.get_running_loop().call_later(delay.total_seconds(), callback)
    return handle.cancel


def _noop_history_timer_factory(
    delay: timedelta, callback: Callable[[], None]
) -> Callable[[], None]:
    """Keep non-Home Assistant unit doubles free of real event-loop timers."""
    del delay, callback
    return lambda: None


def _is_valid_archive_activation_date(value: object) -> bool:
    """Return whether a persisted activation date is canonical ISO format."""
    if not isinstance(value, str):
        return False
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        return False
    return parsed.isoformat() == value


def _persist_archive_enablement_transition(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Persist an Archive Enablement transition and return its entry data."""
    options = getattr(entry, "options", None)
    enabled = isinstance(options, Mapping) and bool(
        options.get(CONF_ARCHIVE_ENABLED, False)
    )
    raw_data = getattr(entry, "data", {})
    data = dict(raw_data) if isinstance(raw_data, Mapping) else {}
    original = dict(data)
    was_enabled = data.get(CONF_ARCHIVE_PREVIOUSLY_ENABLED) is True

    if enabled and not was_enabled:
        # A persisted boundary, even a malformed one, is evidence of prior
        # state. Preserve it so the enabled path can fail closed below.
        if (
            CONF_ARCHIVE_ACTIVATION_DATE not in data
            or _is_valid_archive_activation_date(data[CONF_ARCHIVE_ACTIVATION_DATE])
        ):
            data[CONF_ARCHIVE_ACTIVATION_DATE] = (
                dt_util.as_local(dt_util.utcnow()).date().isoformat()
            )
        data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] = True
    elif not enabled and was_enabled:
        data[CONF_ARCHIVE_PREVIOUSLY_ENABLED] = False

    if data != original:
        hass.config_entries.async_update_entry(entry, data=data)
    return data


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
        """Validate symbols, signatures, and a progress-aware queue barrier."""
        try:
            from homeassistant.const import __version__ as home_assistant_version

            if not _has_supported_home_assistant_version(home_assistant_version):
                return RecorderCompatibilityResult.incompatible_result(
                    "unsupported_home_assistant_version"
                )

            from homeassistant.components.recorder.core import Recorder
            from homeassistant.components.recorder.db_schema import Statistics
            from homeassistant.components.recorder.models import (
                StatisticData,
                StatisticMetaData,
            )
            from homeassistant.helpers.recorder import get_instance

            _require_recorder_task_contract()
            _require_durable_import_statistics_contract()
            _require_parameters(
                Recorder.async_import_statistics,
                ("self", "metadata", "stats", "table"),
            )
            _require_parameters(Recorder.queue_task, ("self", "task"))
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
            await async_confirm_recorder_queue(recorder)
        except asyncio.CancelledError:
            raise
        except (AttributeError, ImportError, TypeError, ValueError):
            return RecorderCompatibilityResult.incompatible_result("recorder_signature")
        except _RecorderBarrierTimeoutError:
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


def _has_supported_home_assistant_version(version: str) -> bool:
    """Return whether Home Assistant meets the HACS minimum version policy."""
    if not isinstance(version, str):
        return False
    parts = version.split(".")
    if len(parts) != len(_RECORDER_MINIMUM_HOME_ASSISTANT_VERSION) or any(
        not part.isascii()
        or not part.isdecimal()
        or (len(part) > 1 and part.startswith("0"))
        for part in parts
    ):
        return False
    parsed = tuple(int(part) for part in parts)
    return parsed >= _RECORDER_MINIMUM_HOME_ASSISTANT_VERSION


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
        clock: HistoryArchiveClock | None = None,
        timer_factory: HistoryArchiveTimerFactory | None = None,
    ) -> None:
        """Initialize an archive without doing I/O or creating tasks."""
        self._hass = hass
        self._entry = entry
        self._recorder_checker = recorder_checker or HomeAssistantRecorderCompatibility(hass)
        self._store_factory = store_factory
        self._source_factory = source_factory
        self._recorder_factory = recorder_factory
        self._clock = clock
        self._timer_factory = timer_factory or (
            _default_history_timer_factory
            if isinstance(hass, HomeAssistant)
            else _noop_history_timer_factory
        )
        self._store: Any | None = None
        self._status_listeners: set[Callable[[], None]] = set()
        self._started = False
        self._tasks: set[asyncio.Task[Any]] = set()
        self._first_sync_task: asyncio.Task[Any] | None = None
        self._status = HistoryStatus(HistoryArchiveState.IDLE)
        self._completed_dates: set[str] = set()
        self._sync_lock = asyncio.Lock()
        self._runtime_sync_failure = False
        self._recorder_adapter: Any | None = None
        self._hrv_summaries: dict[str, dict[str, Any]] = {}
        self._numeric_source_calendar_dates_by_year: dict[str, dict[str, dict[str, str]]] = {}
        self._numeric_source_date_stores: dict[str, Any] = {}
        self._numeric_source_date_years: set[str] = set()
        self._numeric_source_date_dirty_years: set[str] = set()
        self._numeric_source_date_year_dates: dict[str, set[str]] = {}
        self._numeric_source_date_pending: dict[str, set[str]] = {}
        self._numeric_source_date_outbox: dict[str, dict[str, dict[str, str]]] = {}
        self._numeric_source_date_confirmed: dict[str, dict[str, dict[str, str]]] = {}
        self._numeric_source_date_tombstones: dict[str, set[str]] = {}
        self._numeric_source_date_replay_dates: set[str] = set()
        self._numeric_source_date_replay_state_dirty = False
        self._sleep_sessions: dict[str, dict[str, dict[str, Any]]] = {}
        self._health_events: dict[str, dict[str, dict[str, Any]]] = {}
        self._activities: dict[str, dict[str, dict[str, Any]]] = {}
        self._fit_archives: dict[str, dict[str, dict[str, Any]]] = {}
        self._fit_partition_quarantine: dict[str, Any] = {}
        self._fit_queue: dict[str, dict[str, str]] = {}
        self._fit_queue_quarantine: list[Any] = []
        self._fit_queue_error: str | None = None
        self._fit_last_eligible_download: datetime | None = None
        self._sleep_partition_stores: dict[str, Any] = {}
        self._presence: dict[str, dict[str, str]] = {}
        self._reconciliation_family_presence: dict[str, dict[str, str]] = {}
        self._reconciliation: dict[str, _ReconciliationEntry] = {}
        self._last_reconciliation_observation: dict[str, _ReconciliationObservation] = {}
        self._archive_enabled = False
        self._activation_date: date | None = None
        self._account_key_value: str | None = None
        self._owner_fingerprint_value: str | None = None
        self._archive_backoff_until: datetime | None = None
        self._archive_last_success: datetime | None = None
        self._archive_next_eligible_run: datetime | None = None
        self._archive_reauth_requested = False
        self._backfill: BackfillScheduler | None = None
        self._backfill_task: asyncio.Task[Any] | None = None
        self._cycle_timer_cancel: Callable[[], None] | None = None
        self._cycle_task: asyncio.Task[Any] | None = None
        self._cycle_pending = False

    @property
    def status(self) -> HistoryStatus:
        """Return the immutable, privacy-safe current status."""
        return self._status

    @property
    def _status(self) -> HistoryStatus:
        return self._status_value

    @_status.setter
    def _status(self, value: HistoryStatus) -> None:
        previous = getattr(self, "_status_value", None)
        self._status_value = value
        if previous is None or previous == value:
            return
        for listener in tuple(self._status_listeners):
            try:
                listener()
            except Exception:  # pragma: no cover - defensive entity boundary
                _LOGGER.warning("Garmin history status listener failed", exc_info=True)

    def add_status_listener(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Register a synchronous callback for public status changes."""
        self._status_listeners.add(listener)

        def remove() -> None:
            self._status_listeners.discard(listener)

        return remove

    @property
    def archive_enabled(self) -> bool:
        """Return whether prospective automatic archival is enabled."""
        return self._archive_enabled

    @property
    def activation_date(self) -> date | None:
        """Return the persisted Archive Activation Date, if established."""
        return self._activation_date

    def _backfill_status_fields(self) -> dict[str, Any]:
        archive_backoff = self._archive_backoff_until
        archive_backoff_value = (
            archive_backoff.astimezone(UTC).isoformat()
            if (
                self._archive_enabled
                and archive_backoff is not None
                and self._utc_now() < archive_backoff
            )
            else None
        )
        next_eligible_run = None
        if self._archive_enabled:
            next_eligible_run = archive_backoff_value or (
                self._archive_next_eligible_run.astimezone(UTC).isoformat()
                if self._archive_next_eligible_run is not None
                else self._status.next_eligible_run
            )
        return {
            "activation_date": self._activation_date.isoformat() if self._activation_date else None,
            "queued_count": self._status.queued_count,
            "completed_count": self._status.completed_count,
            "next_eligible_run": next_eligible_run,
            "last_success": (
                self._archive_last_success.astimezone(UTC).isoformat()
                if self._archive_last_success is not None
                else self._status.last_success
            ),
            "backoff_until": archive_backoff_value or self._status.backoff_until,
            "safe_error_class": self._status.safe_error_class,
        }

    def _archive_backoff_active(self) -> bool:
        """Return whether archive work is inside its persisted cooldown."""
        return (
            self._archive_backoff_until is not None
            and self._utc_now() < self._archive_backoff_until
        )

    def _archive_next_delay(self) -> timedelta:
        """Return the next archive wakeup without shortening backoff."""
        backoff_until = self._archive_backoff_until
        if backoff_until is not None and self._utc_now() < backoff_until:
            return max(backoff_until - self._utc_now(), timedelta())
        return _PROSPECTIVE_CYCLE_INTERVAL

    def _fit_base_directory(self) -> Path:
        """Return the shared FIT root without mixing account files."""
        return Path(self._hass.config.path("garmin_connect", "fit"))

    def _fit_directory(self) -> Path:
        """Return this account's private FIT directory."""
        return self._fit_base_directory() / self._account_key()

    def _fit_path_for_record(self, path_name: str) -> Path | None:
        """Return an account-owned FIT, copying an indexed legacy file once.

        The old layout put files directly in the shared FIT root.  A file is
        eligible for migration only when the caller has already validated a
        FIT record from this account's annual partition.  Unindexed files are
        deliberately left untouched because their account ownership cannot be
        proven.
        """
        account_directory = ensure_private_fit_directory(
            self._fit_directory(), create=False
        )
        if account_directory is not None:
            account_path = account_directory / path_name
            if os.path.lexists(account_path):
                return account_path

        legacy_directory = ensure_private_fit_directory(
            self._fit_base_directory(), create=False
        )
        if legacy_directory is None:
            return None
        legacy_path = legacy_directory / path_name
        validate_private_fit_file(legacy_path)

        if account_directory is None:
            account_directory = ensure_private_fit_directory(
                self._fit_directory(), create=True
            )
        if account_directory is None:
            return None
        account_path = account_directory / path_name
        if os.path.lexists(account_path):
            validate_private_fit_file(account_path)
            return account_path

        # The legacy root is shared and does not encode account ownership.
        # Copying leaves it available to another account that references the
        # same logical ID; moving it would let startup order steal the file.
        temporary_path = account_directory / f".{path_name}.migration"
        try:
            with legacy_path.open("rb") as source, temporary_path.open(
                "xb", buffering=0
            ) as target:
                while chunk := source.read(1024 * 1024):
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, account_path)
        finally:
            temporary_path.unlink(missing_ok=True)
        validate_private_fit_file(account_path)
        fsync_directory(account_directory)
        return account_path

    def _remove_corrupt_account_fit(self, path: Path | None) -> None:
        """Remove only a corrupt private FIT copy so the queue can retry.

        Legacy migration deliberately copies the shared source. If decoding
        that copy fails, leaving the account-owned 0600 file in place makes
        ``async_archive_fit`` treat it as an existing file forever. Do not
        remove symlinks, non-regular files, or files outside this account's
        directory; those remain isolated/quarantined for explicit repair.
        """
        if path is None or path.parent != self._fit_directory():
            return
        try:
            file_stat = os.lstat(path)
        except FileNotFoundError:
            return
        if (
            stat.S_ISREG(file_stat.st_mode)
            and stat.S_IMODE(file_stat.st_mode) == 0o600
        ):
            try:
                path.unlink()
                fsync_directory(path.parent)
            except OSError:
                _LOGGER.warning("Corrupt Garmin FIT copy could not be removed")

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
        result = {
            key: dict(value)
            for key, value in self._presence.items()
            if start_date <= date.fromisoformat(key) <= end_date
        }
        return result

    async def async_start(self) -> None:
        """Initialize identity, Store catalog, and Recorder compatibility."""
        if self._started:
            return
        self._started = True

        try:
            await self._async_open_store()
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
            account_key = await self._async_ensure_account_key()
            self._async_update_enablement_state()
        except asyncio.CancelledError:
            self._started = False
            await self.async_stop()
            raise
        except _InvalidArchiveActivationDateError:
            self._set_failed("activation_date_invalid")
            return
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
                HistoryArchiveState.FAILED,
                **self._backfill_status_fields(),
                error_type=compatibility.error_type or "recorder_incompatible",
            )
            _LOGGER.warning(
                "Garmin history archive disabled for entry %s: %s",
                self._entry.entry_id,
                self._status.error_type,
            )
            return

        self._status = HistoryStatus(
            (
                HistoryArchiveState.BACKOFF
                if self._archive_enabled and self._archive_backoff_active()
                else HistoryArchiveState.IDLE
                if self._archive_enabled
                else HistoryArchiveState.DISABLED
            ),
            error_type="rate_limited"
            if self._archive_enabled and self._archive_backoff_active()
            else None,
            **self._backfill_status_fields(),
        )

        if self._archive_enabled:
            if self._archive_backoff_active():
                self._schedule_next_cycle()
                await self._async_persist_archive_status_best_effort(
                    "Garmin archive startup schedule could not be persisted"
                )
            else:
                first_sync = self._async_run_first_sync_in_background()
                if isinstance(self._hass, HomeAssistant):
                    self._first_sync_task = self._hass.async_create_task(first_sync)
                else:
                    self._first_sync_task = asyncio.create_task(first_sync)

    async def _async_run_first_sync_in_background(self) -> None:
        """Run first synchronization without delaying current-value setup."""
        try:
            await self._async_run_first_sync()
        except asyncio.CancelledError:
            raise
        except Exception:
            self._set_failed("first_sync")
            _LOGGER.warning(
                "Garmin history archive first synchronization failed for entry %s",
                self._entry.entry_id,
            )
        finally:
            if self._first_sync_task is asyncio.current_task():
                self._first_sync_task = None

    async def _async_run_first_sync(self) -> None:
        """Import one bounded batch for the current Home Assistant local date."""
        if self._archive_backoff_active():
            self._status = HistoryStatus(
                HistoryArchiveState.BACKOFF,
                error_type="rate_limited",
                **self._backfill_status_fields(),
            )
            return
        runtime_data = getattr(self._entry, "runtime_data", None)
        client = getattr(getattr(runtime_data, "core", None), "client", None)
        if client is None:
            # Direct archive construction is also used by the lifecycle seam
            # before config-entry runtime data is attached.  The real setup
            # path attaches it before starting the archive.
            return

        target_date = self._current_local_date()
        async with self._sync_lock:
            report = await self._async_sync_range(
                target_date,
                target_date,
                fit_limit=_FIRST_SYNC_FIT_LIMIT,
                force_date=target_date,
                process_fit=True,
            )
            await self._async_update_reconciliation_state(
                target_date, report, is_current_date=True
            )
        if report.outcome != "written":
            if self._status.state not in {
                HistoryArchiveState.FAILED,
                HistoryArchiveState.BACKOFF,
            }:
                self._set_failed(report.error_type or "first_sync")
            return
        self._schedule_next_cycle()
        await self._async_persist_archive_status_best_effort(
            "Garmin archive schedule could not be persisted"
        )

    def _utc_now(self) -> datetime:
        """Return the injected or Home Assistant UTC clock value."""
        return self._clock() if self._clock is not None else dt_util.utcnow()

    def _current_local_date(self) -> date:
        """Return the current Home Assistant local calendar date."""
        return dt_util.as_local(self._utc_now()).date()

    async def _async_refresh_reconciliation_state(self) -> None:
        """Reload only the private reconciliation ledger before old-date work."""
        if self._store is None:
            return
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            return
        self._reconciliation = self._parse_reconciliation_state(
            catalog.get("reconciliation", {})
        )

    async def _async_save_archive_backoff(self) -> None:
        """Persist only the archive cooldown boundary."""
        if self._store is None:
            raise ValueError("Store catalog is unavailable")
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            raise ValueError("Store catalog is unavailable")
        updated = dict(catalog)
        updated["archive_backoff_until"] = (
            self._archive_backoff_until.astimezone(UTC).isoformat()
            if self._archive_backoff_until is not None
            else None
        )
        await self._store.async_save(updated)

    async def _async_save_archive_status(self) -> None:
        """Persist the public lifecycle timestamps without private ledgers."""
        if self._store is None:
            raise ValueError("Store catalog is unavailable")
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            raise ValueError("Store catalog is unavailable")
        updated = dict(catalog)
        updated["archive_last_success"] = (
            self._archive_last_success.astimezone(UTC).isoformat()
            if self._archive_last_success is not None
            else None
        )
        updated["archive_next_eligible_run"] = (
            self._archive_next_eligible_run.astimezone(UTC).isoformat()
            if self._archive_next_eligible_run is not None
            else None
        )
        await self._store.async_save(updated)

    async def _async_persist_archive_status_best_effort(self, message: str) -> None:
        """Persist lifecycle timestamps without masking archive data results."""
        try:
            await self._async_save_archive_status()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning(message, exc_info=True)

    async def _async_enter_archive_backoff(self) -> bool:
        """Persist a 24-hour archive-only rate-limit cooldown."""
        now = self._utc_now()
        candidate = now + _ARCHIVE_RATE_LIMIT_BACKOFF
        if self._archive_backoff_until is None or candidate > self._archive_backoff_until:
            self._archive_backoff_until = candidate
        try:
            await self._async_save_archive_backoff()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning(
                "Garmin archive rate-limit cooldown could not be persisted",
                exc_info=True,
            )
            self._status = HistoryStatus(
                HistoryArchiveState.FAILED,
                error_type="store_unavailable",
                **self._backfill_status_fields(),
            )
            return False
        self._status = HistoryStatus(
            HistoryArchiveState.BACKOFF,
            error_type="rate_limited",
            **self._backfill_status_fields(),
        )
        if self._cycle_timer_cancel is not None:
            self._cycle_timer_cancel()
            self._cycle_timer_cancel = None
        if self._started and self._archive_enabled:
            self._schedule_next_cycle()
            await self._async_persist_archive_status_best_effort(
                "Garmin archive rate-limit schedule could not be persisted"
            )
        return True

    async def _async_clear_archive_backoff(self) -> None:
        """Forget an expired cooldown after a successful archive attempt."""
        if self._archive_backoff_until is None:
            return
        if self._archive_backoff_active():
            return
        self._archive_backoff_until = None
        try:
            await self._async_save_archive_backoff()
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning(
                "Expired Garmin archive cooldown could not be cleared",
                exc_info=True,
            )
        if self._cycle_timer_cancel is None:
            self._archive_next_eligible_run = None
            self._status = replace(
                self._status,
                next_eligible_run=None,
                backoff_until=None,
                safe_error_class=None,
                error_type=None,
            )
        else:
            self._status = replace(
                self._status,
                next_eligible_run=(
                    self._archive_next_eligible_run.astimezone(UTC).isoformat()
                    if self._archive_next_eligible_run is not None
                    else self._status.next_eligible_run
                ),
                backoff_until=None,
                safe_error_class=None,
                error_type=None,
            )
        await self._async_persist_archive_status_best_effort(
            "Expired Garmin archive lifecycle state could not be persisted"
        )

    async def _async_request_archive_reauth_once(self) -> None:
        """Use account reauthentication once for a genuine archive auth failure."""
        if self._archive_reauth_requested:
            return
        self._archive_reauth_requested = True
        await self._async_backfill_reauth()

    @staticmethod
    def _parse_reconciliation_state(raw: Any) -> dict[str, _ReconciliationEntry]:
        """Validate the bounded private Open/Settled ledger."""
        if raw is None:
            return {}
        if not isinstance(raw, Mapping):
            raise ValueError("Store reconciliation state is invalid")
        parsed: dict[str, _ReconciliationEntry] = {}
        for key, value in raw.items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                raise ValueError("Store reconciliation state is invalid")
            target = date.fromisoformat(key)
            if target.isoformat() != key or target < _HISTORY_MIN_DATE:
                raise ValueError("Store reconciliation state is invalid")
            state = value.get("state")
            if state not in {"open", "settled", "gap"}:
                raise ValueError("Store reconciliation state is invalid")
            fingerprint = value.get("fingerprint")
            if fingerprint is not None and (
                not isinstance(fingerprint, str) or len(fingerprint) != 64
            ):
                raise ValueError("Store reconciliation state is invalid")
            has_records = value.get("has_records", False)
            if not isinstance(has_records, bool):
                raise ValueError("Store reconciliation state is invalid")
            raw_outcome = value.get("outcome")
            if raw_outcome is None:
                raw_outcome = "continuity_gap" if state == "gap" and not has_records else (
                    "records" if has_records else "incomplete"
                )
            if raw_outcome not in _RECONCILIATION_OUTCOMES:
                raise ValueError("Store reconciliation state is invalid")
            parsed[key] = _ReconciliationEntry(
                "settled" if state == "gap" else cast(Literal["open", "settled"], state),
                fingerprint,
                has_records,
                cast(Literal["records", "empty", "incomplete", "failed", "continuity_gap"], raw_outcome),
            )
        return parsed

    def _eligible_reconciliation_dates(self, current: date) -> tuple[date, ...]:
        """Return durable Open Archive Dates still inside the nominal window."""
        if self._activation_date is None:
            return ()
        eligible: list[date] = []
        for key, record in self._reconciliation.items():
            target = date.fromisoformat(key)
            age = current - target
            if (
                target < self._activation_date
                or target >= current
                or age >= _RECONCILIATION_WINDOW
                or record.state != "open"
            ):
                continue
            eligible.append(target)
        return tuple(sorted(eligible))

    async def _async_update_reconciliation_state(
        self,
        target: date,
        report: HistorySyncReport,
        *,
        is_current_date: bool = False,
        confirmation_fingerprint: str | None = None,
    ) -> None:
        """Persist one automatic observation without exposing its ledger."""
        key = target.isoformat()
        previous = self._reconciliation.get(key)
        if previous is not None and previous.state == "settled":
            return
        observation = self._last_reconciliation_observation.get(key)
        prior_fingerprint = previous.fingerprint if previous else None
        prior_has_records = previous.has_records if previous else False
        if observation is None:
            outcome = "failed" if report.outcome != "written" else "incomplete"
            if previous is not None and previous.outcome in {"failed", "incomplete"}:
                outcome = previous.outcome
            self._reconciliation[key] = _ReconciliationEntry(
                "open", prior_fingerprint, prior_has_records,
                cast(Literal["records", "empty", "incomplete", "failed", "continuity_gap"], outcome),
            )
        else:
            has_records = observation.has_records or prior_has_records
            if report.outcome != "written":
                outcome = "failed"
            elif (
                not observation.complete
                or (not observation.explicit_empty and not observation.has_records)
            ):
                outcome = "incomplete"
            elif observation.explicit_empty:
                outcome = "empty"
            else:
                outcome = "records"
            if previous is not None:
                if not (
                    outcome == "records"
                    and previous.outcome in {"failed", "incomplete"}
                ):
                    outcome = _merge_reconciliation_evidence(previous.outcome, outcome)
            evidence_requires_open = outcome in {"failed", "incomplete"}
            if (
                report.outcome != "written"
                or is_current_date
                or evidence_requires_open
                or not observation.complete
                or observation.explicit_empty
                or not observation.has_records
            ):
                state = "open"
            elif (
                confirmation_fingerprint is not None
                and previous is not None
                and previous.state == "open"
                and confirmation_fingerprint == observation.fingerprint
            ):
                state = "settled"
            else:
                state = "open"
            self._reconciliation[key] = _ReconciliationEntry(
                cast(Literal["open", "settled"], state),
                observation.fingerprint or prior_fingerprint,
                has_records,
                cast(Literal["records", "empty", "incomplete", "failed", "continuity_gap"], outcome),
            )
        await self._async_save_reconciliation_state()

    def _remember_date_reconciliation_observation(
        self,
        target_key: str,
        accumulator: _FamilyObservationAccumulator,
    ) -> None:
        """Store current completeness while retaining prior failure evidence."""
        previous = self._reconciliation_family_presence.get(target_key, {})
        current = accumulator.presence
        merged = dict(previous)
        for family, state in current.items():
            merged[family] = _merge_reconciliation_evidence(
                previous.get(family), state
            )
        self._reconciliation_family_presence[target_key] = merged
        self._last_reconciliation_observation[target_key] = _date_reconciliation_observation(
            accumulator
        )

    async def _async_checkpoint_observation(
        self,
        target: date,
        target_key: str,
        accumulator: _FamilyObservationAccumulator,
        checkpoint: _StructuredCheckpoint,
        *,
        outcome: str = "written",
    ) -> None:
        """Publish one observation and the structured records it made durable."""
        self._remember_date_reconciliation_observation(target_key, accumulator)
        await self._async_persist_observed_structured_records(checkpoint)
        if outcome != "written":
            await self._async_update_reconciliation_state(
                target,
                HistorySyncReport(outcome=outcome),
                is_current_date=target == self._current_local_date(),
            )

    async def _async_fetch_structured_family(
        self,
        fetch: Callable[..., Any] | None,
        target: date,
        target_key: str,
        family: str,
        accumulator: _FamilyObservationAccumulator,
        checkpoint: _StructuredCheckpoint,
    ) -> _FamilyObservation:
        """Fetch one structured family and checkpoint its bounded observation."""
        observation = await _async_observe_family(
            fetch, target, family, accumulator
        )
        await self._async_checkpoint_observation(
            target,
            target_key,
            accumulator,
            checkpoint,
            outcome="failed" if observation.presence == "failed" else "written",
        )
        return observation

    async def _async_expire_empty_reconciliation_dates(self, current: date) -> None:
        """Settle empty Open Archive Dates that reached the window boundary."""
        changed = False
        if self._activation_date is None:
            return
        for key, record in self._reconciliation.items():
            target = date.fromisoformat(key)
            if (
                target >= self._activation_date
                and record.state == "open"
                and record.outcome == "empty"
                and not record.has_records
                and isinstance(record.fingerprint, str)
                and set(self._reconciliation_family_presence.get(key, {}))
                == set(_FROZEN_ARCHIVE_FAMILIES)
                and all(
                    self._reconciliation_family_presence[key][family]
                    in _RECONCILIATION_EXPLICIT_EMPTY_STATES
                    for family in _FROZEN_ARCHIVE_FAMILIES
                )
                and current - target >= _RECONCILIATION_WINDOW
            ):
                record.state = "settled"
                record.outcome = "continuity_gap"
                changed = True
        if changed:
            await self._async_save_reconciliation_state()

    async def _async_save_reconciliation_state(
        self,
        *,
        presence: Mapping[str, Mapping[str, str]] | None = None,
        hrv_summaries: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        """Atomically persist the private reconciliation ledger."""
        if self._store is None:
            return
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            raise ValueError("Store catalog is unavailable")
        updated = dict(catalog)
        updated["reconciliation"] = _reconciliation_records(self._reconciliation)
        updated["reconciliation_family_presence"] = {
            key: dict(value)
            for key, value in self._reconciliation_family_presence.items()
        }
        if hrv_summaries is not None:
            updated["hrv_summaries"] = {
                key: dict(value) for key, value in hrv_summaries.items()
            }
        if presence is not None:
            updated["presence"] = {
                key: dict(value) for key, value in presence.items()
            }
        await self._store.async_save(updated)

    def _schedule_next_cycle(self) -> None:
        """Arm one nominal cadence wakeup after a successful first sync."""
        if (
            not self._started
            or not self._archive_enabled
            or self._cycle_timer_cancel is not None
        ):
            return
        try:
            delay = self._archive_next_delay()
            next_eligible_run = (self._utc_now() + delay).astimezone(UTC)
            self._cycle_timer_cancel = self._timer_factory(
                delay, self._async_cycle_tick
            )
            self._archive_next_eligible_run = next_eligible_run
            self._status = replace(
                self._status,
                next_eligible_run=next_eligible_run.isoformat(),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            self._set_failed("schedule")
            _LOGGER.warning(
                "Garmin history archive cadence could not be scheduled for entry %s",
                self._entry.entry_id,
            )

    def _async_cycle_tick(self) -> None:
        """Handle one timer tick without allowing overlapping cycles."""
        self._cycle_timer_cancel = None
        if not self._started or not self._archive_enabled:
            return
        if self._cycle_task is not None and not self._cycle_task.done():
            self._cycle_pending = True
        else:
            self._cycle_task = self._create_cycle_task()
        self._schedule_next_cycle()

    def _create_cycle_task(self) -> asyncio.Task[Any]:
        """Create and retain one prospective cycle task."""
        coroutine = self._async_run_cycle()
        if isinstance(self._hass, HomeAssistant):
            task = self._hass.async_create_task(coroutine)
        else:
            task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _async_run_cycle(self) -> None:
        """Synchronize current and durable Open Archive Dates in the background."""
        try:
            await self._async_persist_archive_status_best_effort(
                "Garmin archive schedule could not be persisted"
            )
            if self._archive_backoff_active():
                self._status = HistoryStatus(
                    HistoryArchiveState.BACKOFF,
                    error_type="rate_limited",
                    **self._backfill_status_fields(),
                )
                return
            target_date = self._current_local_date()
            async with self._sync_lock:
                current_report = await self._async_sync_range(
                    target_date,
                    target_date,
                    fit_limit=_PROSPECTIVE_CYCLE_FIT_LIMIT,
                    force_date=target_date,
                    process_fit=True,
                )
                await self._async_update_reconciliation_state(
                    target_date, current_report, is_current_date=True
                )
                await self._async_refresh_reconciliation_state()
                await self._async_expire_empty_reconciliation_dates(target_date)
                reports = [current_report]
                for reconciliation_date in self._eligible_reconciliation_dates(
                    target_date
                ):
                    reconciliation_key = reconciliation_date.isoformat()
                    previous_reconciliation = self._reconciliation.get(
                        reconciliation_key
                    )
                    confirmation_fingerprint = (
                        previous_reconciliation.fingerprint
                        if previous_reconciliation is not None
                        else None
                    )
                    reconciliation_report = await self._async_sync_range(
                        reconciliation_date,
                        reconciliation_date,
                        fit_limit=_PROSPECTIVE_CYCLE_FIT_LIMIT,
                        force_date=reconciliation_date,
                        process_fit=True,
                    )
                    await self._async_update_reconciliation_state(
                        reconciliation_date,
                        reconciliation_report,
                        confirmation_fingerprint=confirmation_fingerprint,
                    )
                    reports.append(reconciliation_report)
            if (
                any(report.outcome != "written" for report in reports)
                and self._status.state
                not in {HistoryArchiveState.FAILED, HistoryArchiveState.BACKOFF}
            ):
                failed_report = next(
                    report for report in reports if report.outcome != "written"
                )
                self._set_failed(failed_report.error_type or "cycle")
        except asyncio.CancelledError:
            raise
        except Exception:
            self._set_failed("cycle")
            _LOGGER.warning(
                "Garmin history archive cycle failed for entry %s",
                self._entry.entry_id,
            )
        finally:
            if self._cycle_task is asyncio.current_task():
                self._cycle_task = None
            if self._cycle_pending and self._started and self._archive_enabled:
                self._cycle_pending = False
                self._cycle_task = self._create_cycle_task()

    async def async_stop(self) -> None:
        """Stop archive tasks and leave no background work behind."""
        self._started = False
        self._cycle_pending = False
        if self._cycle_timer_cancel is not None:
            self._cycle_timer_cancel()
            self._cycle_timer_cancel = None
        first_sync_task = self._first_sync_task
        if first_sync_task is not None and first_sync_task is not asyncio.current_task():
            first_sync_task.cancel()
            await asyncio.gather(first_sync_task, return_exceptions=True)
            self._first_sync_task = None
        cycle_task = self._cycle_task
        if cycle_task is not None and cycle_task is not asyncio.current_task():
            cycle_task.cancel()
            await asyncio.gather(cycle_task, return_exceptions=True)
            self._cycle_task = None
        if self._backfill is not None:
            self._backfill.stop()
        if self._backfill_task is not None:
            self._backfill_task.cancel()
            await asyncio.gather(self._backfill_task, return_exceptions=True)
            self._backfill_task = None
        tasks = tuple(self._tasks)
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def async_abort_startup(self, error_type: str) -> None:
        """Fail closed and release archive resources after bounded startup fails."""
        self._set_failed(error_type)
        await self.async_stop()

    async def _async_load_backfill_state(self) -> Any:
        if self._store is None:
            return None
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            return None
        state = dict(catalog.get("backfill", {})) if isinstance(catalog.get("backfill", {}), Mapping) else {}
        completed = set(state.get("completed_dates", [])) | self._completed_dates
        state["completed_dates"] = sorted(completed)
        return state

    async def _async_save_backfill_state(self, state: Mapping[str, Any]) -> None:
        if self._store is None:
            return
        async with self._sync_lock:
            catalog = await self._store.async_load()
            if not isinstance(catalog, Mapping):
                return
            updated = dict(catalog)
            updated["backfill"] = dict(state)
            await self._store.async_save(updated)
        parsed = BackfillState.from_record(state)
        queued = count_uncompleted_dates(parsed, date.today())
        self._status = replace(self._status, queued_count=queued, completed_count=len(parsed.completed_dates), next_eligible_run=parsed.next_run.isoformat() if parsed.next_run else None, last_success=parsed.last_success.isoformat() if parsed.last_success else None, backoff_until=parsed.backoff_until.isoformat() if parsed.backoff_until else None, safe_error_class=parsed.error_type)

    async def _async_backfill_reauth(self) -> None:
        callback = getattr(self._entry, "async_start_reauth", None)
        if callable(callback):
            result = callback(self._hass)
            if inspect.isawaitable(result):
                await result

    async def async_sync_range(self, start_date: date, end_date: date, *, fit_limit: int | None = None, include_training_status: bool = True) -> HistorySyncReport:
        """Manually fetch and import the supported metrics for an inclusive range."""
        validation_error = _validate_sync_range(start_date, end_date)
        if validation_error:
            return HistorySyncReport(outcome="invalid", error_type=validation_error)
        if self._archive_backoff_active():
            return HistorySyncReport(outcome="disabled", error_type="rate_limited")
        if self._status.state is HistoryArchiveState.BACKOFF:
            self._status = HistoryStatus(
                self._resting_state(),
                **self._backfill_status_fields(),
            )
        if self._status.state not in {
            HistoryArchiveState.IDLE,
            HistoryArchiveState.DISABLED,
        }:
            if not (self._status.state is HistoryArchiveState.FAILED and self._runtime_sync_failure):
                return HistorySyncReport(outcome="disabled", error_type=self._status.error_type)
        if self._sync_lock.locked():
            return HistorySyncReport(outcome="busy", error_type="sync_in_progress")

        async with self._sync_lock:
            forced_dates = tuple(_iter_inclusive_dates(start_date, end_date))
            report = await self._async_sync_range(
                start_date,
                end_date,
                fit_limit=fit_limit,
                include_training_status=include_training_status,
                force_dates=frozenset(forced_dates),
                # Manual Repair is an explicit operator request.  Archive
                # disablement stops automatic cycles, but must not suppress
                # the FIT work discovered by this repair.
                process_fit=True,
            )
            for target, target_report in report.date_results:
                # One Manual Repair observation must not settle a date without
                # the later unchanged confirmation used by automatic work.
                await self._async_update_reconciliation_state(target, target_report)
            return report

    async def _async_fetch_numeric_detail(
        self,
        source: GarminHistorySource,
        target: date,
        metric: str,
    ) -> _NormalizedDetailRecord:
        """Fetch one numeric response and resolve its normalized dispatch tag once."""
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
                if inspect.isawaitable(candidate) or _normalized_detail_adapter(candidate).supported:
                    details = candidate
        if inspect.isawaitable(details):
            details = await details
        detail_record = _normalized_detail_record(details)
        if detail_record.detail_type is None:
            detail_record = _normalized_detail_record(
                await source.async_fetch(target, metric)
            )
        return detail_record

    async def _async_import_numeric_metric(
        self,
        source: GarminHistorySource,
        recorder: Any,
        target: date,
        target_key: str,
        metric: str,
        metadata: Any,
        presence: dict[str, dict[str, str]],
        health_events: list[NormalizedHealthEvent],
    ) -> _NumericImportResult:
        """Import one numeric family without deciding the date checkpoint."""
        detail_record = await self._async_fetch_numeric_detail(source, target, metric)
        details = detail_record.details
        detail_adapter = detail_record.adapter
        family_observation = _FamilyObservation.from_details(details)

        inserted = updated = skipped = 0
        if detail_adapter.metric_presence is not None:
            presence.setdefault(target_key, {})[metric] = detail_adapter.metric_presence
        for total_key, state in detail_adapter.total_presence.items():
            presence.setdefault(target_key, {})[f"{metric}:{total_key}"] = state
        if detail_adapter.summary is not None:
            self._hrv_summaries[target_key] = {
                "status": detail_adapter.summary.status,
                "last_night_avg": detail_adapter.summary.last_night_avg,
                "last_night_5_min_high": detail_adapter.summary.last_night_5_min_high,
                "weekly_avg": detail_adapter.summary.weekly_avg,
                "baseline": detail_adapter.summary.baseline,
            }
        health_events.extend(detail_adapter.events)
        snapshot_fields = detail_adapter.fields
        if snapshot_fields is not None:
            for field, (state, _value) in snapshot_fields.items():
                presence.setdefault(target_key, {})[f"{metric}:{field}"] = state
            snapshot_metadata = {
                "abnormal_heart_rate_alerts": DAILY_ABNORMAL_HR_METADATA,
                "acute_load": TRAINING_ACUTE_LOAD_METADATA,
                "chronic_load": TRAINING_CHRONIC_LOAD_METADATA,
                "load_balance": TRAINING_LOAD_BALANCE_METADATA,
                "acwr": TRAINING_ACWR_METADATA,
                "vo2_max": TRAINING_VO2_MAX_METADATA,
                "fitness_trend": TRAINING_FITNESS_TREND_METADATA,
                "recovery_time": TRAINING_RECOVERY_TIME_METADATA,
            }
            for field, (state, value) in snapshot_fields.items():
                metadata_for_field = snapshot_metadata.get(field)
                if state != "present" or value is None or metadata_for_field is None:
                    continue
                snapshot = NormalizedSample(
                    details.timestamp,
                    details.calendar_date or target,
                    details.raw_timestamp,
                    value,
                )
                statistic_id = statistic_id_for(self._account_key(), metadata_for_field.key)
                await self._async_prepare_numeric_source_dates(statistic_id, (snapshot,))
                snapshot_outcome = await recorder.async_write(
                    statistic_id,
                    metadata_for_field,
                    (snapshot,),
                )
                if snapshot_outcome.outcome != "written":
                    raise _NumericFamilyError(
                        snapshot_outcome.error_type or "sync_failed",
                        write_failure=True,
                        observation=family_observation,
                    )
                await self._async_confirm_numeric_source_dates(statistic_id, (snapshot,))
                inserted += getattr(snapshot_outcome, "inserted_count", snapshot_outcome.accepted_count)
                updated += getattr(snapshot_outcome, "updated_count", 0)
                skipped += getattr(snapshot_outcome, "skipped_count", 0)
            return _NumericImportResult(
                inserted,
                updated,
                skipped,
                family_observation,
            )
        elif metadata is None and not detail_adapter.write_samples:
            return _NumericImportResult(
                0,
                0,
                0,
                family_observation,
            )
        samples = detail_adapter.samples

        statistic_id = statistic_id_for(self._account_key(), metric)
        await self._async_prepare_numeric_source_dates(statistic_id, samples)
        outcome = await recorder.async_write(statistic_id, metadata, samples)
        if outcome.outcome != "written":
            raise _NumericFamilyError(
                outcome.error_type or "sync_failed",
                write_failure=True,
                observation=family_observation,
            )
        await self._async_confirm_numeric_source_dates(statistic_id, samples)

        total_values = detail_adapter.totals
        if total_values:
            total_metadata = {
                ("steps", "totalSteps"): STEPS_DAILY_TOTAL_METADATA,
                ("floors", "floorsAscended"): FLOORS_ASCENDED_DAILY_METADATA,
                ("floors", "floorsDescended"): FLOORS_DESCENDED_DAILY_METADATA,
                ("floors", "floorsAscendedInMeters"): FLOORS_ASCENDED_METERS_DAILY_METADATA,
                ("floors", "floorsDescendedInMeters"): FLOORS_DESCENDED_METERS_DAILY_METADATA,
                ("floors", "totalFloors"): FLOORS_TOTAL_DAILY_METADATA,
                ("intensity_moderate", "moderateIntensityMinutes"): MODERATE_INTENSITY_DAILY_METADATA,
                ("intensity_moderate", "vigorousIntensityMinutes"): VIGOROUS_INTENSITY_DAILY_METADATA,
                ("intensity_vigorous", "vigorousIntensityMinutes"): VIGOROUS_INTENSITY_DAILY_METADATA,
                ("intensity_moderate", "totalIntensityMinutes"): INTENSITY_TOTAL_DAILY_METADATA,
                ("intensity_vigorous", "moderateIntensityMinutes"): MODERATE_INTENSITY_DAILY_METADATA,
                ("intensity_vigorous", "totalIntensityMinutes"): INTENSITY_TOTAL_DAILY_METADATA,
            }
            for total_key, total_value in total_values.items():
                total_metric = total_metadata.get((metric, total_key))
                if total_metric is None:
                    continue
                total_sample = NormalizedSample(
                    datetime.combine(
                        target, time.min, tzinfo=_DATE_SUMMARY_BUCKET_TIME_ZONE
                    ).astimezone(UTC),
                    target,
                    target.isoformat(),
                    total_value,
                )
                total_statistic_id = statistic_id_for(self._account_key(), total_metric.key)
                await self._async_prepare_numeric_source_dates(total_statistic_id, (total_sample,))
                total_outcome = await recorder.async_write(
                    total_statistic_id, total_metric, (total_sample,)
                )
                if total_outcome.outcome != "written":
                    raise _NumericFamilyError(
                        total_outcome.error_type or "sync_failed",
                        write_failure=True,
                        observation=family_observation,
                    )
                await self._async_confirm_numeric_source_dates(
                    total_statistic_id, (total_sample,)
                )
                inserted += getattr(total_outcome, "inserted_count", total_outcome.accepted_count)
                updated += getattr(total_outcome, "updated_count", 0)
                skipped += getattr(total_outcome, "skipped_count", 0)

        inserted += getattr(outcome, "inserted_count", outcome.accepted_count)
        updated += getattr(outcome, "updated_count", 0)
        skipped += getattr(outcome, "skipped_count", 0)
        return _NumericImportResult(
            inserted,
            updated,
            skipped,
            family_observation,
        )

    async def _async_sync_range(
        self,
        start_date: date,
        end_date: date,
        *,
        fit_limit: int | None = None,
        include_training_status: bool = True,
        force_date: date | None = None,
        force_dates: Collection[date] = (),
        process_fit: bool = False,
    ) -> HistorySyncReport:
        """Run one serialized, checkpointed sync.

        ``force_date`` is used by automatic current-day work. ``force_dates``
        is used by Manual Repair so settled checkpoints cannot suppress an
        explicitly requested bounded range.
        """

        if self._archive_backoff_active():
            return HistorySyncReport(outcome="disabled", error_type="rate_limited")

        runtime_data = getattr(self._entry, "runtime_data", None)
        client = getattr(getattr(runtime_data, "core", None), "client", None)
        request_executor = _runtime_request_executor(runtime_data)
        if client is None:
            self._runtime_sync_failure = True
            self._status = HistoryStatus(HistoryArchiveState.FAILED, error_type="integration_not_loaded", **self._backfill_status_fields())
            return HistorySyncReport(outcome="failed", error_type="integration_not_loaded")
        # The loaded runtime adapter owns the account gate and persists any
        # token refresh before releasing it. Test-only source factories and
        # legacy injected runtimes can still receive a bare gate.
        source = (self._source_factory or GarminHistorySource)(client, request_executor)
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
            self._status = HistoryStatus(HistoryArchiveState.FAILED, error_type="store_unavailable", **self._backfill_status_fields())
            return HistorySyncReport(outcome="failed", error_type="store_unavailable")
        processed: list[date] = []
        date_results: list[tuple[date, HistorySyncReport]] = []

        def build_failed_date_result(
            target_date: date, error_type: str
        ) -> tuple[date, HistorySyncReport]:
            return target_date, HistorySyncReport(
                outcome="failed", error_type=error_type
            )

        skipped = 0
        inserted = 0
        updated = 0
        fit_archived_count = 0
        health_events: list[NormalizedHealthEvent] = []
        checkpoint = _StructuredCheckpoint(
            presence={key: dict(value) for key, value in self._presence.items()},
            sessions_by_year={
                year: dict(records) for year, records in self._sleep_sessions.items()
            },
            events_by_year={
                year: dict(records) for year, records in self._health_events.items()
            },
            activities_by_year={
                year: dict(records) for year, records in self._activities.items()
            },
            dirty_years=set(),
        )
        presence = checkpoint.presence
        sleep_sessions = checkpoint.sessions_by_year
        structured_dirty_years = checkpoint.dirty_years
        self._status = HistoryStatus(HistoryArchiveState.SYNCING, **self._backfill_status_fields())
        for target in _iter_inclusive_dates(start_date, end_date):
            target_key = target.isoformat()
            if (
                target_key in self._completed_dates
                and target != force_date
                and target not in force_dates
            ):
                skipped += 1
                processed.append(target)
                date_results.append(
                    (target, HistorySyncReport(outcome="written", skipped_count=1))
                )
                self._status = HistoryStatus(HistoryArchiveState.SYNCING, current_date=target_key, processed_dates=len(processed), record_count=inserted + updated, **self._backfill_status_fields())
                continue
            if target in force_dates:
                entry = self._reconciliation.get(target_key)
                if entry is not None and entry.state == "settled":
                    entry.state = "open"
                    await self._async_save_reconciliation_state()
            self._status = HistoryStatus(HistoryArchiveState.SYNCING, current_date=target_key, processed_dates=len(processed), record_count=inserted + updated, **self._backfill_status_fields())
            try:
                presence[target_key] = {}
                metrics: tuple[tuple[str, Any], ...] = _NUMERIC_FAMILY_METADATA
                if not include_training_status:
                    metrics = tuple(item for item in metrics if item[0] != "training_status")
                failed_families: set[str] = set()
                failed_family_error = "sync_failed"
                numeric_write_failed = False
                family_observations = _FamilyObservationAccumulator.create()
                events_by_year = {
                    year: dict(records) for year, records in self._health_events.items()
                }
                activities_by_year = {
                    year: dict(records) for year, records in self._activities.items()
                }
                checkpoint.events_by_year = events_by_year
                checkpoint.activities_by_year = activities_by_year
                for metric, metadata in metrics:
                    try:
                        metric_result = await self._async_import_numeric_metric(
                            source, recorder, target, target_key, metric, metadata, presence, health_events
                        )
                    except asyncio.CancelledError:
                        raise
                    except _NumericFamilyError as error:
                        failed_families.add(metric)
                        failed_family_error = error.error_type
                        presence.setdefault(target_key, {})[metric] = "failed"
                        if error.observation is not None:
                            family_observations.record(metric, error.observation)
                        family_observations.record_failure(metric, error.error_type)
                        _LOGGER.warning(
                            "Garmin numeric family failed for %s (%s: %s)",
                            target_key,
                            metric,
                            failed_family_error,
                        )
                        if error.write_failure:
                            numeric_write_failed = True
                            await self._async_checkpoint_observation(
                                target,
                                target_key,
                                family_observations,
                                checkpoint,
                                outcome="failed",
                            )
                            break
                        await self._async_checkpoint_observation(
                            target,
                            target_key,
                            family_observations,
                            checkpoint,
                            outcome="failed",
                        )
                        continue
                    except (
                        GarminConnectError,
                        AttributeError,
                        ImportError,
                        OSError,
                        TypeError,
                        ValueError,
                        RuntimeError,
                    ) as error:
                        error_type = _safe_family_error_type(error)
                        if error_type in {"rate_limited", "reauth_required"}:
                            raise _ArchivePolicyError(error_type) from error
                        failed_families.add(metric)
                        presence.setdefault(target_key, {})[metric] = "failed"
                        failed_family_error = error_type
                        family_observations.record_failure(metric, failed_family_error)
                        _LOGGER.warning(
                            "Garmin numeric family failed for %s (%s: %s)",
                            target_key,
                            metric,
                            failed_family_error,
                        )
                        await self._async_checkpoint_observation(
                            target,
                            target_key,
                            family_observations,
                            checkpoint,
                            outcome="failed",
                        )
                        continue
                    inserted += metric_result.inserted_count
                    updated += metric_result.updated_count
                    skipped += metric_result.skipped_count
                    family_observations.record(metric, metric_result.observation)
                    await self._async_checkpoint_observation(
                        target, target_key, family_observations, checkpoint
                    )
                try:
                    structured_descriptor = inspect.getattr_static(source, "async_fetch_details")
                except AttributeError:
                    structured_descriptor = None
                structured_fetch = getattr(source, "async_fetch_details", None)
                sleep_observation = await self._async_fetch_structured_family(
                    structured_fetch
                    if callable(structured_descriptor) and callable(structured_fetch)
                    else None,
                    target,
                    target_key,
                    "sleep_sessions",
                    family_observations,
                    checkpoint,
                )
                sleep_details = sleep_observation.details
                if sleep_observation.presence in (
                    _RECONCILIATION_UNAVAILABLE_STATES - {"unknown"}
                ):
                    sleep_details = ()
                    if sleep_observation.presence == "failed":
                        failed_families.add("sleep_sessions")
                        failed_family_error = sleep_observation.error_type or "sync_failed"
                        family_observations.record_failure(
                            "sleep_stream", failed_family_error
                        )
                    else:
                        family_observations.record(
                            "sleep_stream",
                            _FamilyObservation(
                                (),
                                sleep_observation.presence,
                                _fingerprint_details(
                                    {
                                        "family": "sleep_stream",
                                        "presence": sleep_observation.presence,
                                    }
                                ),
                                False,
                            ),
                        )
                elif not isinstance(sleep_details, tuple) or any(
                    not isinstance(item, SleepSession) for item in sleep_details
                ):
                    failed_families.add("sleep_sessions")
                    failed_family_error = "sync_failed"
                    family_observations.record_failure("sleep_sessions", failed_family_error)
                    family_observations.record_failure("sleep_stream", failed_family_error)
                    sleep_details = ()
                    await self._async_checkpoint_observation(
                        target,
                        target_key,
                        family_observations,
                        checkpoint,
                        outcome="failed",
                    )
                else:
                    family_observations.record(
                        "sleep_stream", _sleep_stream_observation(sleep_details)
                    )
                invalid_sleep_streams: set[tuple[str, str]] = set()
                for session in sleep_details:
                    for stream in session.streams:
                        sentinels = _SLEEP_NEGATIVE_SENTINELS.get(stream.metric, frozenset())
                        if any(
                            point.value is not None
                            and point.value < 0
                            and point.value not in sentinels
                            for point in stream.points
                        ):
                            invalid_sleep_streams.add((session.logical_id, stream.metric))
                            failed_families.add("sleep_stream")
                            failed_family_error = "sleep_stream_invalid"
                invalid_sleep_sessions = {
                    session_id for session_id, _metric in invalid_sleep_streams
                }
                if invalid_sleep_streams:
                    family_observations.record_failure(
                        "sleep_stream", "sleep_stream_invalid"
                    )
                observed_sleep_sessions: dict[str, dict[str, dict[str, Any]]] = {}
                for session in sleep_details:
                    if session.logical_id in invalid_sleep_sessions:
                        continue
                    year = str(session.start.year)
                    structured_dirty_years.add(year)
                    record = session_record(session)
                    sleep_sessions.setdefault(year, {})[session.logical_id] = record
                    observed_sleep_sessions.setdefault(year, {})[session.logical_id] = record
                    for stream in session.streams:
                        metadata_for_stream = _SLEEP_STREAM_METADATA.get(stream.metric)
                        if metadata_for_stream is None:
                            family_observations.record_failure(
                                "sleep_stream", "sync_failed"
                            )
                            raise SleepSchemaError("sleep stream metric is unsupported")
                        samples = tuple(
                            NormalizedSample(point.timestamp, session.calendar_date, point.raw_timestamp, point.value)
                            for point in stream.points
                            if point.value is not None
                            and point.value not in _SLEEP_NEGATIVE_SENTINELS.get(
                                stream.metric, frozenset()
                            )
                        )
                        stream_statistic_id = statistic_id_for(
                            self._account_key(),
                            f"{metadata_for_stream.key}:{session.logical_id}",
                        )
                        await self._async_prepare_numeric_source_dates(stream_statistic_id, samples)
                        try:
                            stream_outcome = await recorder.async_write(
                                stream_statistic_id, metadata_for_stream, samples
                            )
                        except asyncio.CancelledError:
                            raise
                        except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
                            failed_families.add("sleep_stream")
                            failed_family_error = "sync_failed"
                            family_observations.record_failure(
                                "sleep_stream", failed_family_error
                            )
                            numeric_write_failed = True
                            continue
                        if stream_outcome.outcome != "written":
                            failed_families.add("sleep_stream")
                            failed_family_error = stream_outcome.error_type or "sleep_stream_write_failed"
                            family_observations.record_failure(
                                "sleep_stream", failed_family_error
                            )
                            numeric_write_failed = True
                            continue
                        await self._async_confirm_numeric_source_dates(
                            stream_statistic_id, samples
                        )
                        inserted += getattr(stream_outcome, "inserted_count", stream_outcome.accepted_count)
                        updated += getattr(stream_outcome, "updated_count", 0)
                        skipped += getattr(stream_outcome, "skipped_count", 0)
                date_presence = presence.setdefault(target_key, {})
                for key in tuple(date_presence):
                    if key.startswith(_SLEEP_PRESENCE_PREFIX):
                        del date_presence[key]
                date_presence.update(_aggregate_sleep_presence(observed_sleep_sessions, target))
                for event in health_events:
                    year = str((event.start or event.occurrence or datetime.combine(event.calendar_date, time.min, tzinfo=UTC)).year)
                    structured_dirty_years.add(year)
                    events_by_year.setdefault(year, {})[event.logical_id] = health_event_record(event)
                health_events.clear()
                await self._async_checkpoint_observation(
                    target,
                    target_key,
                    family_observations,
                    checkpoint,
                    outcome="failed" if "sleep_stream" in failed_families else "written",
                )
                for event_metric in ("health_events_daily", "health_events_body_battery"):
                    event_observation = await _async_observe_family(
                        structured_fetch
                        if callable(structured_descriptor) and callable(structured_fetch)
                        else None,
                        target,
                        event_metric,
                        family_observations,
                    )
                    event_details = event_observation.details
                    if event_observation.presence == "failed":
                        failed_families.add(event_metric)
                        failed_family_error = event_observation.error_type or "sync_failed"
                        await self._async_checkpoint_observation(
                            target,
                            target_key,
                            family_observations,
                            checkpoint,
                            outcome="failed",
                        )
                        continue
                    if not isinstance(event_details, tuple) or any(not isinstance(item, NormalizedHealthEvent) for item in event_details):
                        failed_families.add(event_metric)
                        failed_family_error = "health_event_schema"
                        family_observations.record_failure(event_metric, failed_family_error)
                        await self._async_checkpoint_observation(
                            target,
                            target_key,
                            family_observations,
                            checkpoint,
                            outcome="failed",
                        )
                        continue
                    for event in event_details:
                        year = str((event.start or event.occurrence or datetime.combine(event.calendar_date, time.min, tzinfo=UTC)).year)
                        structured_dirty_years.add(year)
                        events_by_year.setdefault(year, {})[event.logical_id] = health_event_record(event)
                    await self._async_checkpoint_observation(
                        target, target_key, family_observations, checkpoint
                    )
                activity_observation = await self._async_fetch_structured_family(
                    structured_fetch
                    if callable(structured_descriptor) and callable(structured_fetch)
                    else None,
                    target,
                    target_key,
                    "timed_activities",
                    family_observations,
                    checkpoint,
                )
                activity_details = activity_observation.details
                if activity_observation.presence == "failed":
                    failed_families.add("timed_activities")
                    failed_family_error = activity_observation.error_type or "sync_failed"
                    activity_details = ()
                if not isinstance(activity_details, tuple) or any(not isinstance(item, NormalizedActivity) for item in activity_details):
                    failed_families.add("timed_activities")
                    failed_family_error = "activity_schema"
                    family_observations.record_failure("timed_activities", failed_family_error)
                    activity_details = ()
                    await self._async_checkpoint_observation(
                        target,
                        target_key,
                        family_observations,
                        checkpoint,
                        outcome="failed",
                    )
                for activity in activity_details:
                    if activity.calendar_date != target:
                        continue
                    year = str(activity.calendar_date.year)
                    for previous_year, previous_records in activities_by_year.items():
                        if previous_year == year or activity.logical_id not in previous_records:
                            continue
                        del previous_records[activity.logical_id]
                        structured_dirty_years.add(previous_year)
                    structured_dirty_years.add(year)
                    activities_by_year.setdefault(year, {})[activity.logical_id] = {
                        "logical_id": activity.logical_id, "activity_id": activity.activity_id, "revision": activity.revision, "calendar_date": activity.calendar_date.isoformat(),
                        "activity_type": activity.activity_type, "name": activity.name, "start": activity.start.isoformat(),
                        "end": activity.end.isoformat() if activity.end else None, "duration_seconds": activity.duration_seconds,
                        "training_effect": activity.training_effect, "load": activity.load, "recovery": activity.recovery,
                    }
                    queued = self._fit_queue.get(activity.logical_id)
                    if queued is not None:
                        self._fit_queue[activity.logical_id] = _fit_queue_entry(
                            activity.logical_id,
                            activity.activity_id,
                            year,
                            activity.calendar_date,
                        )
                    if (
                        callable(getattr(client, "download_activity", None))
                        and activity.activity_id.isdecimal()
                        and str(int(activity.activity_id)) == activity.activity_id
                        and all(
                            queued.get("activity_id") != activity.activity_id
                            for queued in self._fit_queue.values()
                        )
                    ):
                        if (
                            activity.logical_id not in self._fit_archives.get(year, {})
                            and activity.logical_id not in self._fit_queue
                        ):
                            self._fit_queue[activity.logical_id] = _fit_queue_entry(
                                activity.logical_id,
                                activity.activity_id,
                                year,
                                activity.calendar_date,
                            )
                await self._async_checkpoint_observation(
                    target,
                    target_key,
                    family_observations,
                    checkpoint,
                    outcome="failed" if "sleep_stream" in failed_families else "written",
                )
                self._remember_date_reconciliation_observation(target_key, family_observations)
                if failed_families:
                    await self._async_persist_numeric_recovery_state(
                        include_partition=not numeric_write_failed
                    )
                    await self._async_save_sleep_partitions(
                        sleep_sessions,
                        events_by_year,
                        activities_by_year,
                        self._fit_archives,
                        years=structured_dirty_years,
                    )
                    await store.async_save(
                        self._catalog_record(
                            completed_dates=self._completed_dates,
                            presence=presence,
                            sessions_by_year=sleep_sessions,
                            events_by_year=events_by_year,
                            activities_by_year=activities_by_year,
                        )
                    )
                    self._presence = presence
                    self._runtime_sync_failure = True
                    self._status = HistoryStatus(
                        HistoryArchiveState.FAILED,
                        current_date=target_key,
                        processed_dates=len(processed),
                        record_count=inserted + updated,
                        error_type=failed_family_error,
                        **self._backfill_status_fields(),
                    )
                    date_results.append(
                        build_failed_date_result(target, failed_family_error)
                    )
                    return HistorySyncReport(
                        tuple(processed), inserted, updated, skipped,
                        outcome="failed", error_type=failed_family_error,
                        date_results=tuple(date_results),
                    )
                for year, pending_dates in self._numeric_source_date_pending.items():
                    if target_key not in pending_dates or self._numeric_source_date_is_repaired(
                        year, target_key
                    ):
                        continue
                    self._numeric_source_date_tombstones.setdefault(year, set()).add(target_key)
                    self._numeric_source_date_year_dates.setdefault(year, set()).add(target_key)
                    self._numeric_source_date_dirty_years.add(year)
                processed.append(target)
                completed_dates = self._completed_dates | {target_key}
                numeric_checkpoint_years = set(self._numeric_source_date_dirty_years)
                await self._async_save_numeric_source_manifest()
                await self._async_save_numeric_source_partitions()
                await self._async_save_sleep_partitions(
                    sleep_sessions,
                    events_by_year,
                    activities_by_year,
                    self._fit_archives,
                    years=structured_dirty_years,
                )
                self._sleep_sessions = sleep_sessions
                self._health_events = events_by_year
                self._activities = activities_by_year
                # Publish the catalog checkpoint only after every affected annual
                # partition is durable. A failed partition save must be replayed.
                committed_dates_by_year: dict[str, set[str]] = {}
                for year, dates in self._numeric_source_date_pending.items():
                    if target_key in dates:
                        committed_dates_by_year.setdefault(year, set()).add(target_key)
                for year, statistics in self._numeric_source_date_outbox.items():
                    if year not in numeric_checkpoint_years:
                        continue
                    if any(
                        source_date == target_key
                        for instants in statistics.values()
                        for source_date in instants.values()
                    ):
                        committed_dates_by_year.setdefault(year, set()).add(target_key)
                committed_pending = {
                    year: set(dates) - committed_dates_by_year.get(year, set())
                    for year, dates in self._numeric_source_date_pending.items()
                }
                committed_outbox = {
                    year: {
                        statistic_id: {
                            instant: source_date
                            for instant, source_date in instants.items()
                            if source_date not in committed_dates_by_year.get(year, set())
                        }
                        for statistic_id, instants in statistics.items()
                        if any(
                            source_date not in committed_dates_by_year.get(year, set())
                            for source_date in instants.values()
                        )
                    }
                    for year, statistics in self._numeric_source_date_outbox.items()
                    if statistics
                }
                confirmed_after_partition = {
                    year: statistics
                    for year, statistics in self._numeric_source_date_confirmed.items()
                    if year not in numeric_checkpoint_years
                }
                await store.async_save(
                    self._catalog_record(
                        completed_dates=completed_dates,
                        presence=presence,
                        sessions_by_year=sleep_sessions,
                        events_by_year=events_by_year,
                        activities_by_year=activities_by_year,
                        numeric_source_date_pending=committed_pending,
                        numeric_source_date_outbox=committed_outbox,
                        numeric_source_date_confirmed=confirmed_after_partition,
                    )
                )
                self._numeric_source_date_confirmed = confirmed_after_partition
                for year, source_dates in committed_dates_by_year.items():
                    pending = self._numeric_source_date_pending.get(year)
                    if pending is not None:
                        pending.difference_update(source_dates)
                        if not pending:
                            self._numeric_source_date_pending.pop(year, None)
                    outbox_statistics = self._numeric_source_date_outbox.get(year)
                    if outbox_statistics is not None:
                        for statistic_id, instants in tuple(outbox_statistics.items()):
                            for instant, source_date in tuple(instants.items()):
                                if source_date in source_dates:
                                    del instants[instant]
                            if not instants:
                                del outbox_statistics[statistic_id]
                        if not outbox_statistics:
                            self._numeric_source_date_outbox.pop(year, None)
                self._numeric_source_date_replay_dates.difference_update(
                    source_date
                    for source_dates in committed_dates_by_year.values()
                    for source_date in source_dates
                )
                self._numeric_source_date_dirty_years.difference_update(numeric_checkpoint_years)
                self._completed_dates = completed_dates
                self._presence = presence
                self._remember_date_reconciliation_observation(target_key, family_observations)
                if process_fit:
                    fit_archived_count += await self._async_process_fit_queue(
                        client,
                        limit=fit_limit,
                    )
                date_results.append(
                    (
                        target,
                        HistorySyncReport(
                            outcome="written", fit_count=fit_archived_count
                        ),
                    )
                )
            except asyncio.CancelledError:
                self._remember_date_reconciliation_observation(
                    target_key, family_observations
                )
                try:
                    await asyncio.shield(
                        self._async_persist_numeric_recovery_state(
                            include_partition=not numeric_write_failed
                        )
                    )
                    await asyncio.shield(
                        self._async_persist_observed_structured_records(checkpoint)
                    )
                    await asyncio.shield(
                        self._async_update_reconciliation_state(
                            target,
                            HistorySyncReport(
                                outcome="failed", error_type="sync_cancelled"
                            ),
                        )
                    )
                except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
                    _LOGGER.warning(
                        "Garmin reconciliation could not be checkpointed after cancellation for %s",
                        target_key,
                    )
                self._status = HistoryStatus(self._resting_state(), current_date=target_key, processed_dates=len(processed), record_count=inserted + updated, **self._backfill_status_fields())
                raise
            except _NumericFamilyError as error:
                self._remember_date_reconciliation_observation(target_key, family_observations)
                await self._async_persist_numeric_recovery_state(
                    include_partition=not error.write_failure
                )
                self._runtime_sync_failure = True
                date_results.append(
                    build_failed_date_result(target, error.error_type)
                )
                self._status = HistoryStatus(
                    HistoryArchiveState.FAILED,
                    current_date=target_key,
                    processed_dates=len(processed),
                    record_count=inserted,
                    error_type=error.error_type,
                    **self._backfill_status_fields(),
                )
                return HistorySyncReport(
                    tuple(processed), inserted, updated, skipped,
                    outcome="failed", error_type=error.error_type,
                    date_results=tuple(date_results),
                )
            except _ArchivePolicyError as error:
                self._remember_date_reconciliation_observation(
                    target_key, family_observations
                )
                try:
                    await self._async_persist_numeric_recovery_state(
                        include_partition=not numeric_write_failed
                    )
                    await self._async_persist_observed_structured_records(checkpoint)
                except (
                    AttributeError,
                    ImportError,
                    OSError,
                    TypeError,
                    ValueError,
                    RuntimeError,
                ):
                    _LOGGER.warning(
                        "Garmin archive recovery could not be checkpointed after %s for %s",
                        error.error_type,
                        target_key,
                    )
                try:
                    await self._async_update_reconciliation_state(
                        target,
                        HistorySyncReport(
                            outcome="failed", error_type=error.error_type
                        ),
                    )
                except (
                    AttributeError,
                    ImportError,
                    OSError,
                    TypeError,
                    ValueError,
                    RuntimeError,
                ):
                    _LOGGER.warning(
                        "Garmin reconciliation could not be checkpointed after %s for %s",
                        error.error_type,
                        target_key,
                    )
                self._runtime_sync_failure = True
                public_error = error.error_type
                if error.error_type == "rate_limited":
                    if not await self._async_enter_archive_backoff():
                        public_error = "store_unavailable"
                else:
                    try:
                        await self._async_request_archive_reauth_once()
                    except Exception:
                        _LOGGER.warning(
                            "Garmin archive reauthentication request failed",
                            exc_info=True,
                        )
                date_results.append(build_failed_date_result(target, public_error))
                if self._status.state is not HistoryArchiveState.BACKOFF:
                    self._status = HistoryStatus(
                        HistoryArchiveState.FAILED,
                        current_date=target_key,
                        processed_dates=len(processed),
                        record_count=inserted,
                        error_type=public_error,
                        **self._backfill_status_fields(),
                    )
                return HistorySyncReport(
                    tuple(processed),
                    inserted,
                    updated,
                    skipped,
                    outcome="failed",
                    error_type=public_error,
                    date_results=tuple(date_results),
                )
            except (GarminConnectError, AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError) as error:
                self._remember_date_reconciliation_observation(target_key, family_observations)
                await self._async_persist_numeric_recovery_state(
                    include_partition=not numeric_write_failed
                )
                try:
                    await self._async_persist_observed_structured_records(checkpoint)
                except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
                    _LOGGER.warning(
                        "Garmin structured history could not be checkpointed after %s for %s",
                        type(error).__name__,
                        target_key,
                    )
                try:
                    await self._async_update_reconciliation_state(
                        target,
                        HistorySyncReport(outcome="failed", error_type="sync_failed"),
                    )
                except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
                    _LOGGER.warning(
                        "Garmin reconciliation could not be checkpointed after %s for %s",
                        type(error).__name__,
                        target_key,
                    )
                self._runtime_sync_failure = True
                error_type = _archive_error_type(error)
                date_results.append(build_failed_date_result(target, error_type))
                self._status = HistoryStatus(HistoryArchiveState.FAILED, current_date=target_key, processed_dates=len(processed), record_count=inserted, error_type=error_type, **self._backfill_status_fields())
                return HistorySyncReport(
                    tuple(processed),
                    inserted,
                    updated,
                    skipped,
                    outcome="failed",
                    error_type=error_type,
                    date_results=tuple(date_results),
                )
        await self._async_clear_archive_backoff()
        self._archive_reauth_requested = False
        self._runtime_sync_failure = False
        self._archive_last_success = self._utc_now().astimezone(UTC)
        await self._async_persist_archive_status_best_effort(
            "Garmin archive last-success state could not be persisted"
        )
        self._status = HistoryStatus(self._resting_state(), current_date=end_date.isoformat(), processed_dates=len(processed), record_count=inserted + updated, **self._backfill_status_fields())
        return HistorySyncReport(
            tuple(processed),
            inserted,
            updated,
            skipped,
            fit_archived_count,
            outcome="written",
            date_results=tuple(date_results),
        )

    def _fit_queue_records(
        self, queue: Mapping[str, Mapping[str, str]] | None = None
    ) -> list[dict[str, str]]:
        """Serialize pending FIT work in deterministic order."""
        selected = self._fit_queue if queue is None else queue
        return serialize_fit_queue(selected)

    def _fit_last_eligible_record(self) -> str | None:
        """Serialize the durable pacing checkpoint."""
        return serialize_fit_pacing(self._fit_last_eligible_download)

    async def _async_save_fit_state(
        self,
        *,
        queue: Mapping[str, Mapping[str, str]] | None = None,
        last_eligible_download: datetime | None | object = ...,
    ) -> None:
        """Persist queue and pacing without changing public archive status."""
        if self._store is None:
            raise FitArchiveError("FIT queue Store unavailable")
        if last_eligible_download is not ...:
            self._fit_last_eligible_download = cast(
                datetime | None, last_eligible_download
            )
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            raise FitArchiveError("FIT queue catalog unavailable")
        updated = dict(catalog)
        updated["fit_queue"] = self._fit_queue_records(queue)
        updated["fit_queue_quarantine"] = list(self._fit_queue_quarantine)[
            :_FIT_QUEUE_QUARANTINE_MAX_ITEMS
        ]
        updated["fit_queue_error"] = self._fit_queue_error
        updated["fit_last_eligible_download"] = self._fit_last_eligible_record()
        await self._store.async_save(updated)

    async def _async_mark_fit_error(self, error_type: str) -> None:
        """Record FIT-only failure state while retaining pending queue work."""
        self._fit_queue_error = error_type
        try:
            await asyncio.shield(self._async_save_fit_state())
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning("Garmin FIT failure state could not be persisted")

    async def _async_existing_fit_record(
        self, logical_id: str
    ) -> dict[str, Any] | None:
        """Validate an already-owned local FIT without spending pacing budget."""
        path: Path | None = None
        try:
            directory = ensure_private_fit_directory(self._fit_directory(), create=False)
            if directory is None:
                return None
            path = directory / fit_file_name(logical_id)
            validate_private_fit_file(path)
        except (FileNotFoundError, OSError, RuntimeError, TypeError, ValueError, FitArchiveError):
            return None
        try:
            inspected = await asyncio.to_thread(inspect_fit, path, 0o600)
            safe_summary = validated_fit_summary(inspected)
        except (FileNotFoundError, OSError, RuntimeError, TypeError, ValueError, FitArchiveError):
            self._remove_corrupt_account_fit(path)
            return None
        return {
            "logical_id": logical_id,
            "path": path.name,
            "summary": safe_summary,
        }

    async def _async_process_fit_queue(
        self, client: Any, *, limit: int | None
    ) -> int:
        """Archive eligible pending FIT work while isolating every FIT error."""
        download_activity = getattr(client, "download_activity", None)
        if not callable(download_activity):
            return 0
        max_downloads = 1 if limit is None else max(0, limit)
        downloaded = 0
        runtime_data = getattr(self._entry, "runtime_data", None)
        request_executor = _runtime_request_executor(runtime_data)

        async def gated_download(activity_id: int, file_format: str) -> bytes:
            async def request() -> bytes:
                return cast(bytes, await download_activity(activity_id, file_format))

            if request_executor is None or not callable(
                getattr(request_executor, "async_request", None)
            ):
                return await request()
            return cast(
                bytes,
                await request_executor.async_request(
                    GarminRequestPriority.BACKGROUND, request
                ),
            )

        fit_client = SimpleNamespace(download_activity=gated_download)
        for logical_id, queued in sorted(self._fit_queue.items()):
            year = queued["year"]
            try:
                await self._async_load_sleep_partitions({year})
            except asyncio.CancelledError:
                raise
            except Exception as error:
                error_type = _archive_error_type(
                    error.__cause__ if error.__cause__ is not None else error
                )
                if error_type in {"rate_limited", "reauth_required"}:
                    raise _ArchivePolicyError(error_type) from error
                _LOGGER.warning("Garmin FIT partition load failed")
                await self._async_mark_fit_error("fit_partition_schema")
                return downloaded
            existing = self._fit_archives.get(year, {}).get(logical_id)
            if existing is None:
                existing = await self._async_existing_fit_record(logical_id)
            if existing is not None:
                try:
                    await self._async_save_fit_partition_and_queue(
                        queued, existing, client_sessions=self._sleep_sessions,
                        client_events=self._health_events,
                        client_activities=self._activities,
                    )
                except Exception as error:
                    error_type = _archive_error_type(
                        error.__cause__ if error.__cause__ is not None else error
                    )
                    if error_type in {"rate_limited", "reauth_required"}:
                        raise _ArchivePolicyError(error_type) from error
                    _LOGGER.warning("Garmin FIT completion checkpoint failed")
                    await self._async_mark_fit_error("fit_checkpoint_failed")
                    return downloaded
                continue
            if downloaded >= max_downloads:
                break
            now = self._utc_now().astimezone(UTC)
            if (
                self._fit_last_eligible_download is not None
                and now < self._fit_last_eligible_download + _FIT_PACING_INTERVAL
            ):
                break
            try:
                await asyncio.shield(
                    self._async_save_fit_state(last_eligible_download=now)
                )
                fit_result = await async_archive_fit(
                    client=fit_client,
                    activity_id=queued["activity_id"],
                    logical_id=logical_id,
                    directory=self._fit_directory(),
                    inspect=inspect_fit,
                )
                await self._async_save_fit_partition_and_queue(
                    queued, fit_result, client_sessions=self._sleep_sessions,
                    client_events=self._health_events,
                    client_activities=self._activities,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                error_type = _archive_error_type(
                    error.__cause__ if error.__cause__ is not None else error
                )
                if error_type in {"rate_limited", "reauth_required"}:
                    raise _ArchivePolicyError(error_type) from error
                _LOGGER.warning("Garmin FIT archive attempt failed")
                await self._async_mark_fit_error("fit_archive_failed")
                return downloaded
            downloaded += 1
        return downloaded

    async def _async_save_fit_partition_and_queue(
        self,
        queued: Mapping[str, str],
        fit_result: Mapping[str, Any],
        *,
        client_sessions: Mapping[str, Mapping[str, dict[str, Any]]],
        client_events: Mapping[str, Mapping[str, dict[str, Any]]],
        client_activities: Mapping[str, Mapping[str, dict[str, Any]]],
    ) -> None:
        """Checkpoint one accepted FIT and remove only that queue item."""
        logical_id = queued["logical_id"]
        year = queued["year"]
        activities = client_activities.get(year, {})
        if logical_id not in activities:
            raise FitArchiveError("FIT activity record is unavailable")
        parsed_fit = fit_record(fit_result)
        self._fit_archives.setdefault(year, {})[logical_id] = parsed_fit
        await self._async_save_sleep_partitions(
            client_sessions,
            client_events,
            client_activities,
            self._fit_archives,
            years={year},
        )
        self._fit_queue_error = None
        remaining = {
            key: value for key, value in self._fit_queue.items() if key != logical_id
        }
        await self._async_save_fit_state(queue=remaining)
        self._fit_queue.pop(logical_id, None)

    def _catalog_record(
        self,
        *,
        completed_dates: Collection[str],
        presence: Mapping[str, Any],
        sessions_by_year: Mapping[str, Mapping[str, dict[str, Any]]],
        events_by_year: Mapping[str, Mapping[str, dict[str, Any]]],
        activities_by_year: Mapping[str, Mapping[str, dict[str, Any]]],
        numeric_source_date_pending: Mapping[str, Collection[str]] | None = None,
        numeric_source_date_outbox: Mapping[str, Mapping[str, Mapping[str, str]]] | None = None,
        numeric_source_date_confirmed: Mapping[str, Mapping[str, Mapping[str, str]]] | None = None,
        reconciliation: Mapping[str, _ReconciliationEntry] | None = None,
        fit_queue: Mapping[str, Mapping[str, str]] | None = None,
        fit_last_eligible_download: datetime | None | object = ...,
    ) -> dict[str, Any]:
        """Build a bounded catalog record after partitions are durable."""
        return {
            "schema_version": HISTORY_STORE_VERSION,
            "sleep_schema_version": _SLEEP_SCHEMA_VERSION,
            "account_key": self._account_key(),
            **(
                {HISTORY_OWNER_FINGERPRINT: self._owner_fingerprint_value}
                if self._owner_fingerprint_value is not None
                else {}
            ),
            "archive_backoff_until": (
                self._archive_backoff_until.astimezone(UTC).isoformat()
                if self._archive_backoff_until is not None
                else None
            ),
            "archive_last_success": (
                self._archive_last_success.astimezone(UTC).isoformat()
                if self._archive_last_success is not None
                else None
            ),
            "archive_next_eligible_run": (
                self._archive_next_eligible_run.astimezone(UTC).isoformat()
                if self._archive_next_eligible_run is not None
                else None
            ),
            "completed_dates": sorted(completed_dates),
            "reconciliation": _reconciliation_records(
                self._reconciliation if reconciliation is None else reconciliation
            ),
            "hrv_summaries": self._hrv_summaries,
            "numeric_source_date_index": sorted(self._numeric_source_date_years),
            "numeric_source_date_dates": {
                year: sorted(dates)
                for year, dates in self._numeric_source_date_year_dates.items()
            },
            "numeric_source_date_pending": {
                year: sorted(dates)
                for year, dates in (
                    self._numeric_source_date_pending
                    if numeric_source_date_pending is None
                    else numeric_source_date_pending
                ).items()
                if dates
            },
            "numeric_source_date_tombstones": {
                year: sorted(dates)
                for year, dates in self._numeric_source_date_tombstones.items()
                if dates
            },
            "numeric_source_date_outbox": {
                year: {
                    statistic_id: dict(instants)
                    for statistic_id, instants in statistics.items()
                    if instants
                }
                for year, statistics in (
                    self._numeric_source_date_outbox
                    if numeric_source_date_outbox is None
                    else numeric_source_date_outbox
                ).items()
                if statistics
            },
            "numeric_source_date_confirmed": {
                year: {
                    statistic_id: dict(instants)
                    for statistic_id, instants in statistics.items()
                    if instants
                }
                for year, statistics in (
                    self._numeric_source_date_confirmed
                    if numeric_source_date_confirmed is None
                    else numeric_source_date_confirmed
                ).items()
                if statistics
            },
            "presence": presence,
            "reconciliation_family_presence": {
                key: dict(value)
                for key, value in self._reconciliation_family_presence.items()
            },
            "sleep_index": {year: sorted(records) for year, records in sessions_by_year.items()},
            "event_index": {year: sorted(records) for year, records in events_by_year.items()},
            "activity_index": {year: sorted(records) for year, records in activities_by_year.items()},
            "fit_queue": self._fit_queue_records(fit_queue),
            "fit_queue_quarantine": list(self._fit_queue_quarantine)[
                :_FIT_QUEUE_QUARANTINE_MAX_ITEMS
            ],
            "fit_queue_error": self._fit_queue_error,
            "fit_last_eligible_download": (
                self._fit_last_eligible_record()
                if fit_last_eligible_download is ...
                else (
                    fit_last_eligible_download.astimezone(UTC).isoformat()
                    if isinstance(fit_last_eligible_download, datetime)
                    else None
                )
            ),
        }

    def _remember_numeric_source_dates(
        self, statistic_id: str, samples: Collection[NormalizedSample]
    ) -> None:
        """Persist pre-Recorder source-date intent without confirming provenance."""
        if not samples:
            return
        for sample in samples:
            if sample.timestamp.tzinfo is None or sample.timestamp.utcoffset() is None:
                raise ValueError("numeric sample timestamp is naive")
            if not isinstance(sample.request_date, date):
                raise ValueError("numeric source calendar date is invalid")
            instant = sample.timestamp.astimezone(UTC).isoformat()
            year = str(sample.timestamp.astimezone(UTC).year)
            self._numeric_source_date_years.add(year)
            self._numeric_source_date_dirty_years.add(year)
            source_date = sample.request_date.isoformat()
            self._numeric_source_date_year_dates.setdefault(year, set()).add(source_date)
            self._numeric_source_date_pending.setdefault(year, set()).add(source_date)
            self._numeric_source_date_outbox.setdefault(year, {}).setdefault(
                statistic_id, {}
            )[instant] = source_date

    def _confirm_numeric_source_dates(
        self, statistic_id: str, samples: Collection[NormalizedSample]
    ) -> None:
        """Confirm provenance only after Recorder has crossed its write barrier."""
        for sample in samples:
            instant = sample.timestamp.astimezone(UTC).isoformat()
            year = str(sample.timestamp.astimezone(UTC).year)
            self._numeric_source_calendar_dates_by_year.setdefault(year, {}).setdefault(
                statistic_id, {}
            )[instant] = sample.request_date.isoformat()
            self._numeric_source_date_confirmed.setdefault(year, {}).setdefault(
                statistic_id, {}
            )[instant] = sample.request_date.isoformat()

    async def _async_confirm_numeric_source_dates(
        self, statistic_id: str, samples: Collection[NormalizedSample]
    ) -> None:
        """Durably mark provenance after Recorder has crossed its write barrier."""
        self._confirm_numeric_source_dates(statistic_id, samples)
        await self._async_save_numeric_source_manifest()

    def _numeric_source_date_is_repaired(self, year: str, source_date: str) -> bool:
        """Return whether an annual provenance partition retains this date."""
        if source_date in self._numeric_source_date_tombstones.get(year, set()):
            return True
        return any(
            source_date in instants.values()
            for instants in self._numeric_source_calendar_dates_by_year.get(year, {}).values()
        )

    async def _async_prepare_numeric_source_dates(
        self, statistic_id: str, samples: Collection[NormalizedSample]
    ) -> None:
        """Durably enqueue provenance before a Recorder write is attempted."""
        if not samples:
            return
        self._remember_numeric_source_dates(statistic_id, samples)
        await self._async_save_numeric_source_manifest()

    async def _async_save_numeric_source_manifest(self) -> None:
        """Publish numeric partition intent before writing an annual partition."""
        if not self._numeric_source_date_dirty_years or self._store is None:
            return
        catalog = await self._store.async_load()
        if not isinstance(catalog, Mapping):
            raise ValueError("Store catalog is unavailable")
        updated = dict(catalog)
        updated["numeric_source_date_index"] = sorted(self._numeric_source_date_years)
        updated["numeric_source_date_dates"] = {
            year: sorted(dates)
            for year, dates in self._numeric_source_date_year_dates.items()
        }
        updated["numeric_source_date_pending"] = {
            year: sorted(dates)
            for year, dates in self._numeric_source_date_pending.items()
            if dates
        }
        updated["numeric_source_date_tombstones"] = {
            year: sorted(dates)
            for year, dates in self._numeric_source_date_tombstones.items()
            if dates
        }
        updated["numeric_source_date_outbox"] = {
            year: {
                statistic_id: dict(instants)
                for statistic_id, instants in statistics.items()
                if instants
            }
            for year, statistics in self._numeric_source_date_outbox.items()
            if statistics
        }
        updated["numeric_source_date_confirmed"] = {
            year: {
                statistic_id: dict(instants)
                for statistic_id, instants in statistics.items()
                if instants
            }
            for year, statistics in self._numeric_source_date_confirmed.items()
            if statistics
        }
        await self._store.async_save(updated)

    async def _async_persist_numeric_recovery_state(
        self, *, include_partition: bool = True
    ) -> None:
        """Persist saveable numeric state after a checkpoint exception."""
        if not self._numeric_source_date_dirty_years:
            return
        if include_partition:
            try:
                await self._async_save_numeric_source_partitions()
            except asyncio.CancelledError:
                raise
            except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
                _LOGGER.warning(
                    "Garmin numeric source-date partition could not be checkpointed during recovery"
                )
        try:
            await self._async_save_numeric_source_manifest()
        except asyncio.CancelledError:
            raise
        except (AttributeError, ImportError, OSError, TypeError, ValueError, RuntimeError):
            _LOGGER.warning(
                "Garmin numeric source-date manifest could not be checkpointed during recovery"
            )

    async def _async_save_numeric_source_partitions(self) -> None:
        """Persist source-date provenance in lazy annual private Stores."""
        if not self._numeric_source_date_dirty_years:
            return
        store_factory = self._store_factory
        if store_factory is None:
            from homeassistant.helpers.storage import Store

            store_factory = Store
        for year in sorted(self._numeric_source_date_dirty_years):
            if year not in self._numeric_source_date_stores:
                self._numeric_source_date_stores[year] = store_factory(
                    self._hass,
                    HISTORY_STORE_VERSION,
                    f"{DOMAIN}.{self._entry.entry_id}.numeric_source_dates_{year}",
                    private=True,
                    atomic_writes=True,
                )
            dates = {
                statistic_id: dict(instants)
                for statistic_id, instants in self._numeric_source_calendar_dates_by_year.get(year, {}).items()
                if instants
            }
            await self._numeric_source_date_stores[year].async_save(
                {
                    "schema_version": HISTORY_STORE_VERSION,
                    "account_key": self._account_key(),
                    "year": year,
                    "dates": dates,
                    "tombstones": sorted(
                        self._numeric_source_date_tombstones.get(year, set())
                    ),
                }
            )

    async def _async_load_numeric_source_partitions(self, years: set[str]) -> None:
        """Load only indexed annual source-date provenance partitions."""
        store_factory = self._store_factory
        if store_factory is None:
            from homeassistant.helpers.storage import Store

            store_factory = Store
        for year in years:
            if year not in self._numeric_source_date_stores:
                self._numeric_source_date_stores[year] = store_factory(
                    self._hass,
                    HISTORY_STORE_VERSION,
                    f"{DOMAIN}.{self._entry.entry_id}.numeric_source_dates_{year}",
                    private=True,
                    atomic_writes=True,
                )
            # Some lightweight test adapters intentionally return one Store
            # object for every path; there is no separate partition to load.
            if self._numeric_source_date_stores[year] is self._store:
                continue
            affected_dates = set(self._numeric_source_date_year_dates.get(year, set()))
            confirmed_year = self._numeric_source_date_confirmed.get(year, {})
            affected_dates.update(
                item for item in self._completed_dates if item.startswith(f"{year}-")
            )
            affected_dates.difference_update(
                self._numeric_source_date_tombstones.get(year, set())
            )
            restored_year: dict[str, dict[str, str]] = {}
            repaired = False
            try:
                partition = await self._numeric_source_date_stores[year].async_load()
                if (
                    not isinstance(partition, Mapping)
                    or partition.get("schema_version") != HISTORY_STORE_VERSION
                    or partition.get("account_key") != self._account_key()
                    or partition.get("year") != year
                    or not isinstance(partition.get("dates", {}), Mapping)
                ):
                    raise ValueError("numeric source-date partition is invalid")
                raw_tombstones = partition.get("tombstones", [])
                if not isinstance(raw_tombstones, list):
                    raise ValueError("numeric source-date partition is invalid")
                for item in raw_tombstones:
                    parsed = date.fromisoformat(item)
                    if parsed.isoformat() != item or parsed < _HISTORY_MIN_DATE:
                        raise ValueError("numeric source-date partition is invalid")
                    self._numeric_source_date_tombstones.setdefault(year, set()).add(item)
                    affected_dates.discard(item)
                    self._completed_dates.add(item)
                    self._numeric_source_date_pending.get(year, set()).discard(item)
                    self._numeric_source_date_replay_state_dirty = True
                raw_dates = partition["dates"]
                for statistic_id, instants in partition["dates"].items():
                    if not isinstance(statistic_id, str) or not statistic_id or not isinstance(instants, Mapping):
                        repaired = True
                        continue
                    restored = restored_year.setdefault(statistic_id, {})
                    for instant, source_date in instants.items():
                        try:
                            if not isinstance(instant, str) or not isinstance(source_date, str):
                                raise ValueError
                            parsed_instant = datetime.fromisoformat(instant)
                            parsed_date = date.fromisoformat(source_date)
                            if (
                                parsed_instant.tzinfo is None
                                or parsed_instant.utcoffset() is None
                                or parsed_instant.astimezone(UTC).isoformat() != instant
                                or parsed_instant.astimezone(UTC).year != int(year)
                                or parsed_date.isoformat() != source_date
                                or parsed_date < _HISTORY_MIN_DATE
                            ):
                                raise ValueError
                        except (TypeError, ValueError):
                            repaired = True
                            continue
                        restored[instant] = source_date
                        affected_dates.discard(source_date)
                if not raw_dates:
                    repaired = True
                self._numeric_source_calendar_dates_by_year[year] = restored_year
            except asyncio.CancelledError:
                raise
            except Exception:
                repaired = True
                self._numeric_source_calendar_dates_by_year[year] = {}
                _LOGGER.warning(
                    "Garmin numeric source-date partition unavailable for %s; affected dates will replay",
                    year,
                )
            for statistic_id, instants in confirmed_year.items():
                restored_year.setdefault(statistic_id, {}).update(instants)
                affected_dates.difference_update(instants.values())
            if repaired:
                self._numeric_source_date_dirty_years.add(year)
            self._numeric_source_calendar_dates_by_year[year] = restored_year
            self._numeric_source_date_year_dates.setdefault(year, set()).update(
                source_date for instants in restored_year.values() for source_date in instants.values()
            )
            affected_dates.difference_update(
                self._numeric_source_date_tombstones.get(year, set())
            )
            if affected_dates:
                self._numeric_source_date_replay_dates.update(affected_dates)
                self._numeric_source_date_pending.setdefault(year, set()).update(affected_dates)
                self._completed_dates.difference_update(affected_dates)
                self._numeric_source_date_replay_state_dirty = True

    async def _async_save_sleep_partitions(
        self, sessions_by_year: Mapping[str, Mapping[str, dict[str, Any]]],
        events_by_year: Mapping[str, Mapping[str, dict[str, Any]]] | None = None,
        activities_by_year: Mapping[str, Mapping[str, dict[str, Any]]] | None = None,
        fits_by_year: Mapping[str, Mapping[str, dict[str, Any]]] | None = None,
        *,
        years: Collection[str] | None = None,
    ) -> None:
        """Atomically checkpoint only annual partitions changed by this sync."""
        store_factory = self._store_factory
        if store_factory is None:
            from homeassistant.helpers.storage import Store

            store_factory = Store
        changed_years = set(years) if years is not None else (
            set(sessions_by_year)
            | set(events_by_year or {})
            | set(activities_by_year or {})
            | set(fits_by_year or {})
        )
        for year in changed_years:
            records = sessions_by_year.get(year, {})
            if year not in self._sleep_partition_stores:
                self._sleep_partition_stores[year] = store_factory(
                    self._hass,
                    HISTORY_STORE_VERSION,
                    f"{DOMAIN}.{self._entry.entry_id}.sleep_{year}",
                    private=True,
                    atomic_writes=True,
                )
            fit_records = dict((fits_by_year or {}).get(year, {}))
            if year not in (fits_by_year or {}):
                existing_partition = await self._sleep_partition_stores[year].async_load()
                existing_fits = (
                    existing_partition.get("fits", {})
                    if isinstance(existing_partition, Mapping)
                    else {}
                )
                if isinstance(existing_fits, Mapping):
                    fit_records = dict(existing_fits)
            partition_record = {
                "schema_version": HISTORY_STORE_VERSION,
                "sleep_schema_version": _SLEEP_SCHEMA_VERSION,
                "account_key": self._account_key(),
                "year": year,
                "sessions": dict(records),
                "events": dict((events_by_year or {}).get(year, {})),
                "activities": dict((activities_by_year or {}).get(year, {})),
                "fits": fit_records,
            }
            if year in self._fit_partition_quarantine:
                partition_record["fit_quarantine"] = dict(
                    self._fit_partition_quarantine[year]
                )
            await self._sleep_partition_stores[year].async_save(
                partition_record
            )

    async def _async_persist_observed_structured_records(
        self, checkpoint: _StructuredCheckpoint
    ) -> None:
        """Persist structured records already observed before the next fetch."""
        if self._store is None:
            return
        self._sleep_sessions = {
            year: dict(records) for year, records in checkpoint.sessions_by_year.items()
        }
        self._health_events = {
            year: dict(records) for year, records in checkpoint.events_by_year.items()
        }
        self._activities = {
            year: dict(records) for year, records in checkpoint.activities_by_year.items()
        }
        if checkpoint.dirty_years:
            await self._async_save_sleep_partitions(
                checkpoint.sessions_by_year,
                checkpoint.events_by_year,
                checkpoint.activities_by_year,
                self._fit_archives,
                years=checkpoint.dirty_years,
            )
            await self._store.async_save(
                self._catalog_record(
                    completed_dates=self._completed_dates,
                    presence=checkpoint.presence,
                    sessions_by_year=checkpoint.sessions_by_year,
                    events_by_year=checkpoint.events_by_year,
                    activities_by_year=checkpoint.activities_by_year,
                )
            )
            return
        await self._async_save_reconciliation_state(
            presence=checkpoint.presence,
            hrv_summaries=self._hrv_summaries,
        )

    def _account_key(self) -> str:
        """Return the persisted opaque account key."""
        account_key = self._account_key_value or self._entry.data.get(
            CONF_HISTORY_ACCOUNT_KEY
        )
        if not isinstance(account_key, str) or not _is_valid_account_key(account_key):
            raise RuntimeError("account identity unavailable")
        return account_key

    def _resting_state(self) -> HistoryArchiveState:
        """Return the public non-running state after a manual operation."""
        return HistoryArchiveState.IDLE if self._archive_enabled else HistoryArchiveState.DISABLED

    async def async_get_calendar_events(
        self,
        calendar: str,
        start_date: date,
        end_date: date,
    ) -> tuple[HistoryCalendarEvent, ...]:
        """Return privacy-safe structured sleep and nap events."""
        if calendar not in {"sleep", "health", "activity"}:
            return ()
        if start_date > end_date:
            return ()
        await self._async_load_sleep_partitions(
            {str(year) for year in range(start_date.year - 1, end_date.year + 2)}
        )
        events: dict[tuple[str, datetime, datetime, str], HistoryCalendarEvent] = {}
        if calendar == "activity":
            for records in self._activities.values():
                for logical_id, record in records.items():
                    interval = project_activity_interval(record)
                    if interval is None:
                        continue
                    start, end = interval
                    summary = str(
                        record.get("name")
                        or record.get("activity_type")
                        or "Activity"
                    )[:64]
                    add_structured_calendar_event(
                        events,
                        logical_id=logical_id,
                        record=record,
                        start=start,
                        end=end,
                        summary=summary,
                        query_start_date=start_date,
                        query_end_date=end_date,
                    )
            return tuple(sorted(events.values(), key=lambda event: event.start))
        if calendar == "health":
            for records in self._health_events.values():
                for logical_id, record in records.items():
                    interval = project_health_interval(record)
                    if interval is None:
                        continue
                    health_start, health_end = interval
                    summary = str(
                        record.get("category")
                        or record.get("event_type")
                        or "Health event"
                    )[:64]
                    add_structured_calendar_event(
                        events,
                        logical_id=logical_id,
                        record=record,
                        start=health_start,
                        end=health_end,
                        summary=summary,
                        query_start_date=start_date,
                        query_end_date=end_date,
                    )
            return tuple(sorted(events.values(), key=lambda event: event.start))
        for records in self._sleep_sessions.values():
            for logical_id, record in records.items():
                start = datetime.fromisoformat(record["start"])
                end = datetime.fromisoformat(record["end"])
                if (
                    start.astimezone(UTC).date() <= end_date
                    and end.astimezone(UTC).date() >= start_date
                ):
                    summary = "Sleep" if record["kind"] == "main" else "Nap"
                    events[(logical_id, start, end, summary)] = HistoryCalendarEvent(start, end, summary)
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
            self._activities.pop(year, None)
            self._fit_archives.pop(year, None)
            self._fit_partition_quarantine.pop(year, None)
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
                    self._completed_dates = {value for value in self._completed_dates if value[:4] != year}
                    await self._async_invalidate_activity_index(year)
                    continue
                if (
                    not isinstance(partition, Mapping)
                    or partition.get("account_key") != self._account_key()
                    or partition.get("year") != year
                    or partition.get("sleep_schema_version", _SLEEP_SCHEMA_VERSION) != _SLEEP_SCHEMA_VERSION
                    or not isinstance(partition.get("sessions", {}), Mapping)
                    or not isinstance(partition.get("events", {}), Mapping)
                ):
                    self._completed_dates = {value for value in self._completed_dates if value[:4] != year}
                    await self._async_invalidate_activity_index(year)
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
                raw_activities = partition.get("activities", {})
                if not isinstance(raw_activities, Mapping):
                    raise SleepSchemaError("activity partition is invalid")
                parsed_activities: dict[str, dict[str, Any]] = {}
                for key, value in raw_activities.items():
                    restored_activity = activity_from_record(value)
                    if restored_activity.logical_id != key or str(restored_activity.calendar_date.year) != year:
                        raise SleepSchemaError("activity partition is invalid")
                    parsed_activities[key] = dict(value)
                self._activities[year] = parsed_activities
                await self._async_load_fit_records(
                    year, partition, parsed_activities
                )
            except (KeyError, TypeError, ValueError, OSError):
                self._sleep_sessions.pop(year, None)
                self._health_events.pop(year, None)
                self._activities.pop(year, None)
                self._fit_archives.pop(year, None)
                self._completed_dates = {value for value in self._completed_dates if value[:4] != year}
                await self._async_invalidate_activity_index(year)

    async def _async_load_fit_records(
        self,
        year: str,
        partition: Mapping[str, Any],
        activities: Mapping[str, Mapping[str, Any]],
    ) -> None:
        """Load optional FIT detail without invalidating structured records."""
        raw_fits = partition.get("fits", {})
        parsed_fits: dict[str, dict[str, Any]] = {}
        invalid_fit_records: dict[str, Any] = {}
        persisted_fit_quarantine = partition.get("fit_quarantine")
        quarantine = (
            dict(persisted_fit_quarantine)
            if isinstance(persisted_fit_quarantine, Mapping)
            else {}
        )
        if isinstance(persisted_fit_quarantine, Mapping):
            self._fit_partition_quarantine[year] = dict(persisted_fit_quarantine)
            self._fit_queue_error = self._fit_queue_error or "fit_partition_schema"
        if not isinstance(raw_fits, Mapping):
            invalid_fit_records["__schema__"] = repr(raw_fits)[:256]
            raw_fits = {}
        try:
            for key, value in raw_fits.items():
                fit_path: Path | None = None
                try:
                    restored_fit = fit_record(value)
                    if restored_fit["logical_id"] != key or key not in activities:
                        raise FitArchiveError("FIT activity association is invalid")
                    fit_path = self._fit_path_for_record(restored_fit["path"])
                    if fit_path is None:
                        raise FitArchiveError("FIT account directory is unavailable")
                    validate_private_fit_file(fit_path)
                    try:
                        inspected = await asyncio.to_thread(inspect_fit, fit_path, 0o600)
                        safe_summary = validated_fit_summary(inspected)
                    except (FileNotFoundError, OSError, RuntimeError, TypeError, ValueError, FitArchiveError):
                        self._remove_corrupt_account_fit(fit_path)
                        raise
                    parsed_fits[key] = fit_record(
                        {
                            "logical_id": key,
                            "path": fit_path.name,
                            "summary": safe_summary,
                        }
                    )
                except (FileNotFoundError, OSError, RuntimeError, TypeError, ValueError, FitArchiveError):
                    invalid_fit_records[key] = value
        except asyncio.CancelledError:
            raise
        except Exception:
            invalid_fit_records["__schema__"] = "FIT detail validation failed"
            parsed_fits = {}

        self._fit_archives[year] = parsed_fits
        if not invalid_fit_records:
            return
        quarantine["invalid_records"] = invalid_fit_records
        self._fit_partition_quarantine[year] = quarantine
        self._fit_queue_error = self._fit_queue_error or "fit_partition_schema"
        cleaned_partition = dict(partition)
        cleaned_partition["fits"] = parsed_fits
        cleaned_partition["fit_quarantine"] = quarantine
        try:
            await self._sleep_partition_stores[year].async_save(cleaned_partition)
        except (OSError, RuntimeError, TypeError, ValueError):
            pass

    async def _async_invalidate_activity_index(self, year: str) -> None:
        """Best-effort removal of an invalid annual activity index entry."""
        store = self._store
        if store is None:
            return
        try:
            catalog = await store.async_load()
            if not isinstance(catalog, Mapping):
                return
            raw_index = catalog.get("activity_index")
            if not isinstance(raw_index, Mapping) or year not in raw_index:
                return
            activity_index = {key: value for key, value in raw_index.items() if key != year}
            updated = dict(catalog)
            updated["activity_index"] = activity_index
            await store.async_save(updated)
        except (OSError, TypeError, ValueError):
            # The partition and checkpoint are already invalidated in memory;
            # a catalog write failure must not restore durability or escape.
            return

    async def _async_ensure_account_key(self) -> str:
        """Load, safely recover, or create the opaque archive identity."""
        await self._async_open_store()
        assert self._store is not None
        catalog = await self._store.async_load()
        current = self._entry.data.get(CONF_HISTORY_ACCOUNT_KEY)

        if isinstance(current, str) and _is_valid_account_key(current):
            self._account_key_value = current
            if catalog is not None:
                if (
                    not isinstance(catalog, Mapping)
                    or catalog.get("account_key") != current
                ):
                    raise ValueError("Store identity mismatch")
                await self._async_bind_catalog_owner(catalog, current)
            elif self._has_current_profile_source():
                self._owner_fingerprint_value = (
                    await self._async_owner_fingerprint_for_current_profile(current)
                )
            return current

        # A malformed explicit key is not a downgrade artifact. Never replace
        # it with a catalog value because that could adopt another archive.
        if current is not None:
            raise ValueError("account identity is invalid")

        if catalog is not None:
            account_key = await self._async_restore_account_key(catalog)
            self._account_key_value = account_key
            data = {**self._entry.data, CONF_HISTORY_ACCOUNT_KEY: account_key}
            self._hass.config_entries.async_update_entry(self._entry, data=data)
            return account_key

        account_key = secrets.token_urlsafe(24)
        self._account_key_value = account_key
        if self._has_current_profile_source():
            self._owner_fingerprint_value = (
                await self._async_owner_fingerprint_for_current_profile(account_key)
            )
        data = {**self._entry.data, CONF_HISTORY_ACCOUNT_KEY: account_key}
        self._hass.config_entries.async_update_entry(self._entry, data=data)
        return account_key

    async def _async_open_store(self) -> None:
        """Open the private catalog Store without creating or overwriting it."""
        if self._store is not None:
            return
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

    async def _async_bind_catalog_owner(
        self, catalog: Mapping[str, Any], account_key: str
    ) -> None:
        """Verify or safely upgrade a catalog owner binding.

        Every archive binding is derived from the profile returned by the
        current authenticated Garmin client.  The old v1 binding used
        ``entry.unique_id``; it is migratable only when a legacy numeric ID
        exactly matches that authenticated profile.  A username fallback has
        no profile proof and intentionally remains fail-closed.
        """
        stored = catalog.get(HISTORY_OWNER_FINGERPRINT)
        # Direct lifecycle seams without a Garmin client are intentionally
        # identity-neutral. Production setup always supplies this capability;
        # do not turn a no-client test adapter into a permissive recovery path.
        if stored is None and not self._has_current_profile_source():
            return
        profile_id = await self._async_current_profile_id()
        owner_fingerprint = self._owner_fingerprint(account_key, profile_id)
        if isinstance(stored, str) and _is_valid_owner_fingerprint(stored):
            if hmac.compare_digest(stored, owner_fingerprint):
                self._owner_fingerprint_value = stored
                return
            if self._legacy_binding_matches_profile(stored, account_key, profile_id):
                await self._async_save_owner_fingerprint(catalog, owner_fingerprint)
                return
        elif stored is None and self._legacy_entry_matches_profile(profile_id):
            # A pre-binding beta catalog is recoverable only where its config
            # entry's legacy numeric identity is authenticated again.
            await self._async_save_owner_fingerprint(catalog, owner_fingerprint)
            return
        raise ValueError("Store owner identity mismatch")

    async def _async_restore_account_key(self, catalog: Any) -> str:
        """Recover only an archive proven to belong to the current profile."""
        if not isinstance(catalog, Mapping):
            raise ValueError("Store identity is invalid")
        account_key = catalog.get("account_key")
        owner_fingerprint = catalog.get(HISTORY_OWNER_FINGERPRINT)
        if (
            not isinstance(account_key, str)
            or not _is_valid_account_key(account_key)
            or not isinstance(owner_fingerprint, str)
            or not _is_valid_owner_fingerprint(owner_fingerprint)
        ):
            raise ValueError("Store recovery identity is unavailable")

        profile_id = await self._async_current_profile_id()
        expected_fingerprint = self._owner_fingerprint(account_key, profile_id)
        if hmac.compare_digest(owner_fingerprint, expected_fingerprint):
            self._owner_fingerprint_value = owner_fingerprint
            return account_key
        if self._legacy_binding_matches_profile(
            owner_fingerprint, account_key, profile_id
        ):
            await self._async_save_owner_fingerprint(catalog, expected_fingerprint)
            return account_key
        raise ValueError("Store owner identity mismatch")

    async def _async_save_owner_fingerprint(
        self, catalog: Mapping[str, Any], owner_fingerprint: str
    ) -> None:
        """Persist a profile HMAC without retaining profile identity data."""
        updated = dict(catalog)
        updated[HISTORY_OWNER_FINGERPRINT] = owner_fingerprint
        assert self._store is not None
        await self._store.async_save(updated)
        self._owner_fingerprint_value = owner_fingerprint

    async def _async_owner_fingerprint_for_current_profile(self, account_key: str) -> str:
        """Return the v2 HMAC binding for the authenticated Garmin profile."""
        return self._owner_fingerprint(
            account_key, await self._async_current_profile_id()
        )

    async def _async_current_profile_id(self) -> str:
        """Read the authenticated Garmin profile through the request boundary."""
        runtime_data = getattr(self._entry, "runtime_data", None)
        client = getattr(getattr(runtime_data, "core", None), "client", None)
        get_user_profile = getattr(client, "get_user_profile", None)
        if not callable(get_user_profile):
            raise ValueError("current account identity is unavailable")

        async def request() -> Any:
            return await get_user_profile()

        request_executor = _runtime_request_executor(runtime_data)
        if request_executor is not None and callable(
            getattr(request_executor, "async_request", None)
        ):
            profile = await request_executor.async_request(
                GarminRequestPriority.FOREGROUND, request
            )
        else:
            profile = await request()
        profile_id = getattr(profile, "profile_id", None)
        if not isinstance(profile_id, (str, int)) or isinstance(profile_id, bool):
            raise ValueError("current account identity is unavailable")
        return str(profile_id)

    def _has_current_profile_source(self) -> bool:
        """Return whether this runtime can verify its authenticated profile."""
        runtime_data = getattr(self._entry, "runtime_data", None)
        client = getattr(getattr(runtime_data, "core", None), "client", None)
        try:
            getter = inspect.getattr_static(client, "get_user_profile")
        except AttributeError:
            return False
        return callable(getter)

    def _owner_identity(self) -> str | None:
        """Return the bounded immutable identity configured for this Garmin entry."""
        unique_id = getattr(self._entry, "unique_id", None)
        if not isinstance(unique_id, str) or not 1 <= len(unique_id) <= 256:
            return None
        return unique_id

    def _owner_fingerprint(self, account_key: str, profile_id: str) -> str:
        """Return a private v2 binding for one immutable Garmin profile."""
        return hmac.new(
            account_key.encode(),
            f"{DOMAIN}:history-owner:v2:{profile_id}".encode(),
            hashlib.sha256,
        ).hexdigest()

    def _legacy_binding_matches_profile(
        self, stored: str, account_key: str, profile_id: str
    ) -> bool:
        """Accept a v1 binding only after numeric entry/profile verification."""
        owner_identity = self._owner_identity()
        if (
            owner_identity is None
            or not owner_identity.isdecimal()
            or not hmac.compare_digest(owner_identity, profile_id)
        ):
            return False
        expected = hmac.new(
            account_key.encode(),
            f"{DOMAIN}:history-owner:v1:{owner_identity}".encode(),
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(stored, expected)

    def _legacy_entry_matches_profile(self, profile_id: str) -> bool:
        """Return whether a legacy numeric entry is current-profile verified."""
        owner_identity = self._owner_identity()
        return (
            owner_identity is not None
            and owner_identity.isdecimal()
            and hmac.compare_digest(owner_identity, profile_id)
        )

    def _async_update_enablement_state(self) -> None:
        """Persist Archive Enablement transitions without starting sync work."""
        raw_data = getattr(self._entry, "data", {})
        original = dict(raw_data) if isinstance(raw_data, Mapping) else {}
        data = _persist_archive_enablement_transition(self._hass, self._entry)
        if (
            self._account_key_value is not None
            and data != original
            and data.get(CONF_HISTORY_ACCOUNT_KEY) != self._account_key_value
        ):
            data[CONF_HISTORY_ACCOUNT_KEY] = self._account_key_value
            self._hass.config_entries.async_update_entry(self._entry, data=data)

        self._archive_enabled = bool(
            isinstance(self._entry.options, Mapping)
            and self._entry.options.get(CONF_ARCHIVE_ENABLED, False)
        )
        raw_activation_date = data.get(CONF_ARCHIVE_ACTIVATION_DATE)
        if self._archive_enabled:
            if not isinstance(raw_activation_date, str):
                raise _InvalidArchiveActivationDateError
            try:
                parsed_activation_date = date.fromisoformat(raw_activation_date)
            except ValueError as err:
                raise _InvalidArchiveActivationDateError from err
            if parsed_activation_date.isoformat() != raw_activation_date:
                raise _InvalidArchiveActivationDateError
            self._activation_date = parsed_activation_date
            return

        try:
            self._activation_date = (
                date.fromisoformat(raw_activation_date)
                if isinstance(raw_activation_date, str)
                else None
            )
        except ValueError:
            self._activation_date = None

    async def _async_initialize_store(self, account_key: str) -> None:
        """Create and validate the per-account Store catalog."""
        await self._async_open_store()
        assert self._store is not None
        catalog = await self._store.async_load()
        if catalog is None:
            initial_catalog = {
                    "schema_version": HISTORY_STORE_VERSION,
                    "account_key": account_key,
                    "archive_backoff_until": None,
                    "archive_last_success": None,
                    "archive_next_eligible_run": None,
                    "completed_dates": [],
                    "reconciliation": {},
                    "hrv_summaries": {},
                    "numeric_source_date_index": [],
                    "numeric_source_date_dates": {},
                    "numeric_source_date_pending": {},
                    "numeric_source_date_tombstones": {},
                    "numeric_source_date_outbox": {},
                    "numeric_source_date_confirmed": {},
                    "presence": {},
                    "reconciliation_family_presence": {},
                    "sleep_index": {},
                    "event_index": {},
                    "activity_index": {},
                    "fit_queue": [],
                    "fit_queue_quarantine": [],
                    "fit_queue_error": None,
                    "fit_last_eligible_download": None,
                }
            if self._owner_fingerprint_value is not None:
                initial_catalog[HISTORY_OWNER_FINGERPRINT] = (
                    self._owner_fingerprint_value
                )
            await self._store.async_save(initial_catalog)
            return
        if not isinstance(catalog, Mapping) or catalog.get("account_key") != account_key:
            raise ValueError("Store identity mismatch")
        raw_archive_backoff = catalog.get("archive_backoff_until")
        if raw_archive_backoff is not None:
            if not isinstance(raw_archive_backoff, str):
                raise ValueError("Archive backoff state is invalid")
            try:
                parsed_archive_backoff = datetime.fromisoformat(raw_archive_backoff)
            except ValueError as err:
                raise ValueError("Archive backoff state is invalid") from err
            if (
                parsed_archive_backoff.tzinfo is None
                or parsed_archive_backoff.utcoffset() is None
                or parsed_archive_backoff.astimezone(UTC).isoformat()
                != raw_archive_backoff
            ):
                raise ValueError("Archive backoff state is invalid")
            self._archive_backoff_until = parsed_archive_backoff.astimezone(UTC)
        for field_name, attribute_name in (
            ("archive_last_success", "_archive_last_success"),
            ("archive_next_eligible_run", "_archive_next_eligible_run"),
        ):
            raw_value = catalog.get(field_name)
            if raw_value is None:
                continue
            if not isinstance(raw_value, str):
                raise ValueError("Archive lifecycle state is invalid")
            try:
                parsed_value = datetime.fromisoformat(raw_value)
            except ValueError as err:
                raise ValueError("Archive lifecycle state is invalid") from err
            if (
                parsed_value.tzinfo is None
                or parsed_value.utcoffset() is None
                or parsed_value.astimezone(UTC).isoformat() != raw_value
            ):
                raise ValueError("Archive lifecycle state is invalid")
            setattr(self, attribute_name, parsed_value.astimezone(UTC))
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
        self._reconciliation = self._parse_reconciliation_state(
            catalog.get("reconciliation", {})
        )
        summaries = catalog.get("hrv_summaries", {})
        if isinstance(summaries, Mapping):
            self._hrv_summaries = {key: dict(value) for key, value in summaries.items() if isinstance(key, str) and isinstance(value, Mapping)}
        raw_numeric_dates = catalog.get("numeric_source_calendar_dates", {})
        if not isinstance(raw_numeric_dates, Mapping):
            raise ValueError("Store numeric source-date catalog is invalid")
        numeric_dates: dict[str, dict[str, str]] = {}
        for statistic_id, instants in raw_numeric_dates.items():
            if not isinstance(statistic_id, str) or not statistic_id or not isinstance(instants, Mapping):
                raise ValueError("Store numeric source-date catalog is invalid")
            parsed_instants: dict[str, str] = {}
            for instant, source_date in instants.items():
                if not isinstance(instant, str) or not isinstance(source_date, str):
                    raise ValueError("Store numeric source-date catalog is invalid")
                try:
                    parsed_instant = datetime.fromisoformat(instant)
                    parsed_date = date.fromisoformat(source_date)
                except ValueError as err:
                    raise ValueError("Store numeric source-date catalog is invalid") from err
                if (
                    parsed_instant.tzinfo is None
                    or parsed_instant.utcoffset() is None
                    or parsed_instant.astimezone(UTC).isoformat() != instant
                    or parsed_date.isoformat() != source_date
                    or parsed_date < _HISTORY_MIN_DATE
                ):
                    raise ValueError("Store numeric source-date catalog is invalid")
                parsed_instants[instant] = source_date
            numeric_dates[statistic_id] = parsed_instants
        raw_numeric_years = catalog.get("numeric_source_date_index", [])
        if not isinstance(raw_numeric_years, list) or any(
            not isinstance(year, str) or len(year) != 4 or not year.isdecimal()
            for year in raw_numeric_years
        ):
            raise ValueError("Store numeric source-date index is invalid")
        self._numeric_source_date_years = set(raw_numeric_years)
        for statistic_id, instants in numeric_dates.items():
            for instant, source_date in instants.items():
                year = str(datetime.fromisoformat(instant).astimezone(UTC).year)
                self._numeric_source_calendar_dates_by_year.setdefault(year, {}).setdefault(statistic_id, {})[instant] = source_date
                self._numeric_source_date_years.add(year)
                self._numeric_source_date_year_dates.setdefault(year, set()).add(source_date)
        if numeric_dates:
            # Legacy catalogs are read once and rewritten into annual Stores
            # on the next successful checkpoint.
            self._numeric_source_date_dirty_years.update(self._numeric_source_date_years)
        raw_outbox = catalog.get("numeric_source_date_outbox", {})
        if not isinstance(raw_outbox, Mapping):
            raise ValueError("Store numeric source-date outbox is invalid")
        for year, statistics in raw_outbox.items():
            if (
                not isinstance(year, str)
                or len(year) != 4
                or not year.isdecimal()
                or not isinstance(statistics, Mapping)
            ):
                raise ValueError("Store numeric source-date outbox is invalid")
            restored_statistics: dict[str, dict[str, str]] = {}
            for statistic_id, instants in statistics.items():
                if not isinstance(statistic_id, str) or not statistic_id or not isinstance(instants, Mapping):
                    raise ValueError("Store numeric source-date outbox is invalid")
                restored_instants: dict[str, str] = {}
                for instant, source_date in instants.items():
                    if not isinstance(instant, str) or not isinstance(source_date, str):
                        raise ValueError("Store numeric source-date outbox is invalid")
                    try:
                        parsed_instant = datetime.fromisoformat(instant)
                        parsed_date = date.fromisoformat(source_date)
                    except ValueError as err:
                        raise ValueError("Store numeric source-date outbox is invalid") from err
                    if (
                        parsed_instant.tzinfo is None
                        or parsed_instant.utcoffset() is None
                        or parsed_instant.astimezone(UTC).isoformat() != instant
                        or parsed_instant.astimezone(UTC).year != int(year)
                        or parsed_date.isoformat() != source_date
                        or parsed_date < _HISTORY_MIN_DATE
                    ):
                        raise ValueError("Store numeric source-date outbox is invalid")
                    restored_instants[instant] = source_date
                    self._numeric_source_date_year_dates.setdefault(year, set()).add(source_date)
                    self._numeric_source_date_pending.setdefault(year, set()).add(source_date)
                    self._numeric_source_date_replay_dates.add(source_date)
                restored_statistics[statistic_id] = restored_instants
            self._numeric_source_date_outbox[year] = restored_statistics
            self._numeric_source_date_years.add(year)
            self._numeric_source_date_dirty_years.add(year)
        raw_confirmed = catalog.get("numeric_source_date_confirmed", {})
        if not isinstance(raw_confirmed, Mapping):
            raise ValueError("Store confirmed numeric source-date state is invalid")
        for year, statistics in raw_confirmed.items():
            if (
                not isinstance(year, str)
                or len(year) != 4
                or not year.isdecimal()
                or not isinstance(statistics, Mapping)
            ):
                raise ValueError("Store confirmed numeric source-date state is invalid")
            confirmed_statistics: dict[str, dict[str, str]] = {}
            for statistic_id, instants in statistics.items():
                if not isinstance(statistic_id, str) or not statistic_id or not isinstance(instants, Mapping):
                    raise ValueError("Store confirmed numeric source-date state is invalid")
                confirmed_instants: dict[str, str] = {}
                for instant, source_date in instants.items():
                    if not isinstance(instant, str) or not isinstance(source_date, str):
                        raise ValueError("Store confirmed numeric source-date state is invalid")
                    try:
                        parsed_instant = datetime.fromisoformat(instant)
                        parsed_date = date.fromisoformat(source_date)
                    except ValueError as err:
                        raise ValueError("Store confirmed numeric source-date state is invalid") from err
                    if (
                        parsed_instant.tzinfo is None
                        or parsed_instant.utcoffset() is None
                        or parsed_instant.astimezone(UTC).isoformat() != instant
                        or parsed_instant.astimezone(UTC).year != int(year)
                        or parsed_date.isoformat() != source_date
                        or parsed_date < _HISTORY_MIN_DATE
                    ):
                        raise ValueError("Store confirmed numeric source-date state is invalid")
                    confirmed_instants[instant] = source_date
                    self._numeric_source_date_year_dates.setdefault(year, set()).add(source_date)
                confirmed_statistics[statistic_id] = confirmed_instants
            self._numeric_source_date_confirmed[year] = confirmed_statistics
            self._numeric_source_date_years.add(year)
            self._numeric_source_date_dirty_years.add(year)
        raw_numeric_date_dates = catalog.get("numeric_source_date_dates", {})
        if not isinstance(raw_numeric_date_dates, Mapping):
            raise ValueError("Store numeric source-date dates are invalid")
        for year, dates in raw_numeric_date_dates.items():
            if not isinstance(year, str) or year not in self._numeric_source_date_years or not isinstance(dates, list):
                raise ValueError("Store numeric source-date dates are invalid")
            parsed_source_dates: set[str] = set()
            for item in dates:
                try:
                    parsed = date.fromisoformat(item)
                except (TypeError, ValueError) as err:
                    raise ValueError("Store numeric source-date dates are invalid") from err
                if parsed.isoformat() != item or parsed < _HISTORY_MIN_DATE:
                    raise ValueError("Store numeric source-date dates are invalid")
                parsed_source_dates.add(item)
            self._numeric_source_date_year_dates.setdefault(year, set()).update(parsed_source_dates)
        for year in self._numeric_source_date_years:
            self._numeric_source_date_year_dates.setdefault(year, set()).update(
                item for item in self._completed_dates if item.startswith(f"{year}-")
            )
        raw_numeric_pending = catalog.get("numeric_source_date_pending", {})
        if not isinstance(raw_numeric_pending, Mapping):
            raise ValueError("Store numeric source-date pending state is invalid")
        for year, dates in raw_numeric_pending.items():
            if not isinstance(year, str) or year not in self._numeric_source_date_years or not isinstance(dates, list):
                raise ValueError("Store numeric source-date pending state is invalid")
            parsed_pending_dates: set[str] = set()
            for item in dates:
                try:
                    parsed = date.fromisoformat(item)
                except (TypeError, ValueError) as err:
                    raise ValueError("Store numeric source-date pending state is invalid") from err
                if parsed.isoformat() != item or parsed < _HISTORY_MIN_DATE:
                    raise ValueError("Store numeric source-date pending state is invalid")
                parsed_pending_dates.add(item)
            if len(parsed_pending_dates) != len(dates):
                raise ValueError("Store numeric source-date pending state is invalid")
            self._numeric_source_date_pending[year] = parsed_pending_dates
        raw_tombstones = catalog.get("numeric_source_date_tombstones", {})
        if not isinstance(raw_tombstones, Mapping):
            raise ValueError("Store numeric source-date tombstones are invalid")
        for year, dates in raw_tombstones.items():
            if not isinstance(year, str) or len(year) != 4 or not year.isdecimal() or not isinstance(dates, list):
                raise ValueError("Store numeric source-date tombstones are invalid")
            parsed_tombstones: set[str] = set()
            for item in dates:
                try:
                    parsed = date.fromisoformat(item)
                except (TypeError, ValueError) as err:
                    raise ValueError("Store numeric source-date tombstones are invalid") from err
                if parsed.isoformat() != item or parsed < _HISTORY_MIN_DATE:
                    raise ValueError("Store numeric source-date tombstones are invalid")
                parsed_tombstones.add(item)
            self._numeric_source_date_tombstones[year] = parsed_tombstones
            self._numeric_source_date_replay_dates.difference_update(parsed_tombstones)
            self._numeric_source_date_years.add(year)
            self._numeric_source_date_year_dates.setdefault(year, set()).update(parsed_tombstones)
            pending = self._numeric_source_date_pending.get(year)
            if pending is not None:
                pending.difference_update(parsed_tombstones)
            statistics = self._numeric_source_date_outbox.get(year)
            if statistics is not None:
                for statistic_id, instants in tuple(statistics.items()):
                    for instant, source_date in tuple(instants.items()):
                        if source_date in parsed_tombstones:
                            del instants[instant]
                    if not instants:
                        del statistics[statistic_id]
                if not statistics:
                    self._numeric_source_date_outbox.pop(year, None)
        for dates in self._numeric_source_date_tombstones.values():
            self._completed_dates.update(dates)
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
            if not isinstance(metrics, Mapping):
                raise ValueError("Store presence catalog is invalid")
            bounded: dict[str, str] = {}
            for metric, state in metrics.items():
                if not isinstance(metric, str) or len(metric) > 64 or state not in _PRESENCE_STATES:
                    raise ValueError("Store presence catalog is invalid")
                if any(metric.startswith(f"{metadata.key}:") for metadata in _SLEEP_STREAM_METADATA.values()):
                    continue
                bounded[metric] = state
            if len(bounded) > _MAX_PRESENCE_METRICS:
                raise ValueError("Store presence catalog is invalid")
            parsed_presence[key] = bounded
        self._presence = parsed_presence
        raw_reconciliation_presence = catalog.get("reconciliation_family_presence", {})
        if not isinstance(raw_reconciliation_presence, Mapping):
            raise ValueError("Store reconciliation family presence is invalid")
        parsed_reconciliation_presence: dict[str, dict[str, str]] = {}
        for key, families in raw_reconciliation_presence.items():
            if not isinstance(key, str) or not isinstance(families, Mapping):
                raise ValueError("Store reconciliation family presence is invalid")
            parsed_date = date.fromisoformat(key)
            if parsed_date.isoformat() != key or parsed_date < _HISTORY_MIN_DATE:
                raise ValueError("Store reconciliation family presence is invalid")
            bounded_families: dict[str, str] = {}
            for family, state in families.items():
                if (
                    not isinstance(family, str)
                    or family not in _RECONCILIATION_FAMILIES
                    or state not in _PRESENCE_STATES
                ):
                    raise ValueError("Store reconciliation family presence is invalid")
                bounded_families[family] = state
            parsed_reconciliation_presence[key] = bounded_families
        self._reconciliation_family_presence = parsed_reconciliation_presence
        raw_fit_queue = catalog.get("fit_queue", [])
        raw_fit_quarantine = catalog.get("fit_queue_quarantine", [])
        if not isinstance(raw_fit_quarantine, list):
            raw_fit_quarantine = []
            self._fit_queue_error = "fit_queue_schema"
        self._fit_queue_quarantine = [
            dict(item) if isinstance(item, Mapping) else {"raw": repr(item)[:256]}
            for item in raw_fit_quarantine
            if isinstance(item, Mapping) or item is not None
        ][:_FIT_QUEUE_QUARANTINE_MAX_ITEMS]
        raw_fit_error = catalog.get("fit_queue_error")
        if isinstance(raw_fit_error, str) and len(raw_fit_error) <= 64:
            self._fit_queue_error = raw_fit_error
        elif raw_fit_error is not None:
            self._fit_queue_error = "fit_queue_schema"
        if not isinstance(raw_fit_queue, list):
            self._fit_queue_quarantine.append({"raw": repr(raw_fit_queue)[:256]})
            self._fit_queue_error = "fit_queue_schema"
            raw_fit_queue = []
        seen_activity_ids: set[str] = set()
        for item in raw_fit_queue:
            try:
                if not isinstance(item, Mapping):
                    raise ValueError("FIT queue entry is not a mapping")
                parsed_entry = _parse_fit_queue_entry(item)
                logical_id = parsed_entry["logical_id"]
                if logical_id in self._fit_queue or parsed_entry["activity_id"] in seen_activity_ids:
                    raise ValueError("FIT queue contains a duplicate activity")
                self._fit_queue[logical_id] = parsed_entry
                seen_activity_ids.add(parsed_entry["activity_id"])
            except (TypeError, ValueError):
                self._fit_queue_quarantine.append(
                    dict(item) if isinstance(item, Mapping) else {"raw": repr(item)[:256]}
                )
                self._fit_queue_error = "fit_queue_schema"
        raw_fit_last = catalog.get("fit_last_eligible_download")
        if raw_fit_last is not None:
            try:
                if not isinstance(raw_fit_last, str):
                    raise ValueError("FIT pacing state is invalid")
                parsed_fit_last = datetime.fromisoformat(raw_fit_last)
                if (
                    parsed_fit_last.tzinfo is None
                    or parsed_fit_last.utcoffset() is None
                    or parsed_fit_last.astimezone(UTC).isoformat() != raw_fit_last
                ):
                    raise ValueError("FIT pacing state is invalid")
                self._fit_last_eligible_download = parsed_fit_last.astimezone(UTC)
            except ValueError:
                self._fit_queue_error = "fit_pacing_schema"
        raw_sleep = catalog.get("sleep_index", catalog.get("sleep_sessions", {}))
        raw_events = catalog.get("event_index", {})
        raw_activities = catalog.get("activity_index", {})
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
        if not isinstance(raw_activities, Mapping):
            raise ValueError("Activity catalog is invalid")
        for year, records in raw_activities.items():
            if not isinstance(year, str) or len(year) != 4 or not year.isdecimal() or not isinstance(records, list) or len(records) > 10000:
                raise ValueError("Activity catalog is invalid")
            sleep_years.add(year)
        self._sleep_sessions = {}
        await self._async_load_sleep_partitions(sleep_years)
        await self._async_load_numeric_source_partitions(self._numeric_source_date_years)
        for year, dates in self._numeric_source_date_pending.items():
            self._numeric_source_date_replay_dates.update(
                dates - self._numeric_source_date_tombstones.get(year, set())
            )
        self._completed_dates.difference_update(self._numeric_source_date_replay_dates)
        missing_years = sleep_years - set(self._sleep_sessions)
        if missing_years:
            self._completed_dates = {
                value for value in self._completed_dates
                if value[:4] not in missing_years
            }
        await self._async_persist_fit_recovery_state()
        await self._async_persist_numeric_replay_state()

    async def _async_persist_fit_recovery_state(self) -> None:
        """Durably record FIT-only repair work without failing startup."""
        if self._store is None or (
            self._fit_queue_error is None and not self._fit_queue_quarantine
        ):
            return
        try:
            catalog = await self._store.async_load()
            if not isinstance(catalog, Mapping):
                return
            updated = dict(catalog)
            updated["fit_queue"] = self._fit_queue_records()
            updated["fit_queue_quarantine"] = list(self._fit_queue_quarantine)[
                :_FIT_QUEUE_QUARANTINE_MAX_ITEMS
            ]
            updated["fit_queue_error"] = self._fit_queue_error
            updated["fit_last_eligible_download"] = self._fit_last_eligible_record()
            await self._store.async_save(updated)
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning("Garmin FIT queue repair state could not be persisted")

    async def _async_persist_numeric_replay_state(self) -> None:
        """Keep startup-detected provenance loss durable without aborting setup."""
        if not self._numeric_source_date_replay_state_dirty and not self._numeric_source_date_replay_dates:
            return
        if self._store is None:
            return
        try:
            catalog = await self._store.async_load()
            if not isinstance(catalog, Mapping):
                return
            updated = dict(catalog)
            updated["completed_dates"] = sorted(self._completed_dates)
            updated["numeric_source_date_index"] = sorted(self._numeric_source_date_years)
            updated["numeric_source_date_dates"] = {
                year: sorted(dates)
                for year, dates in self._numeric_source_date_year_dates.items()
            }
            updated["numeric_source_date_pending"] = {
                year: sorted(dates)
                for year, dates in self._numeric_source_date_pending.items()
                if dates
            }
            updated["numeric_source_date_tombstones"] = {
                year: sorted(dates)
                for year, dates in self._numeric_source_date_tombstones.items()
                if dates
            }
            updated["numeric_source_date_outbox"] = {
                year: {
                    statistic_id: dict(instants)
                    for statistic_id, instants in statistics.items()
                    if instants
                }
                for year, statistics in self._numeric_source_date_outbox.items()
                if statistics
            }
            updated["numeric_source_date_confirmed"] = {
                year: {
                    statistic_id: dict(instants)
                    for statistic_id, instants in statistics.items()
                    if instants
                }
                for year, statistics in self._numeric_source_date_confirmed.items()
                if statistics
            }
            await self._store.async_save(updated)
            self._numeric_source_date_replay_state_dirty = False
        except asyncio.CancelledError:
            raise
        except Exception:
            _LOGGER.warning(
                "Garmin numeric source-date replay marker could not be persisted"
            )

    def _set_failed(self, error_type: str) -> None:
        """Set a bounded startup failure without exposing exception details."""
        self._runtime_sync_failure = False
        self._status = HistoryStatus(
            HistoryArchiveState.FAILED,
            error_type=error_type,
            **self._backfill_status_fields(),
        )


def _is_valid_account_key(value: str) -> bool:
    """Accept only opaque URL-safe generated-key-shaped values."""
    return _ACCOUNT_KEY_MIN_LENGTH <= len(value) <= _ACCOUNT_KEY_MAX_LENGTH and not (
        set(value) - _ACCOUNT_KEY_ALPHABET
    )


def _is_valid_owner_fingerprint(value: str) -> bool:
    """Accept only the fixed-width private owner binding format."""
    return len(value) == 64 and not (set(value) - frozenset("0123456789abcdef"))


def _iter_inclusive_dates(start_date: date, end_date: date) -> Iterator[date]:
    """Yield every calendar date in an inclusive range."""
    for offset in range((end_date - start_date).days + 1):
        yield start_date.fromordinal(start_date.toordinal() + offset)


def _validate_sync_range(start_date: date, end_date: date) -> str | None:
    """Validate the bounded inclusive manual sync range."""
    if start_date > end_date:
        return "reversed_range"
    if start_date < _HISTORY_MIN_DATE or end_date < _HISTORY_MIN_DATE:
        return "date_before_minimum"
    if (end_date - start_date).days + 1 > _HISTORY_MAX_DAYS:
        return "range_too_large"
    return None
