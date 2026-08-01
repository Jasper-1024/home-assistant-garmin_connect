"""Authenticated Garmin intraday history source and payload normalization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from math import isfinite
from typing import TYPE_CHECKING, Any, Protocol

from .request_gate import GarminRequestGate, GarminRequestPriority
from .sleep_archive import SleepSession, parse_sleep_sessions

if TYPE_CHECKING:
    from ha_garmin import GarminClient


class GarminRequestExecutor(Protocol):
    """The account lifecycle required by a history cloud request."""

    async def async_request(
        self,
        priority: GarminRequestPriority,
        requester: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Run one request under an account-scoped priority slot."""


class HistorySchemaError(ValueError):
    """Raised when a known Garmin series has an incompatible shape."""


_MISSING = object()
_CALENDAR_BUCKET_TIME_ZONE = timezone(timedelta(hours=8))


@dataclass(frozen=True, slots=True)
class NormalizedSample:
    """One immutable, normalized intraday measurement."""

    timestamp: datetime
    request_date: date
    raw_timestamp: Any
    value: float


@dataclass(frozen=True, slots=True)
class HRVSummary:
    """Bounded HRV summary metadata, kept separate from raw readings."""

    status: str | None = None
    last_night_avg: float | None = None
    last_night_5_min_high: float | None = None
    weekly_avg: float | None = None
    baseline: dict[str, float] | None = None


@dataclass(frozen=True, slots=True)
class HRVData:
    """Raw HRV readings plus an optional, bounded summary."""

    readings: tuple[NormalizedSample, ...]
    summary: HRVSummary | None = None
    presence: str = "present"


@dataclass(frozen=True, slots=True)
class SegmentedData:
    """Raw time slices and separate bounded daily totals."""

    readings: tuple[NormalizedSample, ...]
    totals: dict[str, float] | None = None
    presence: str = "present"
    total_presence: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SourceSeries:
    """One source array and its bounded availability state."""

    readings: tuple[NormalizedSample, ...]
    presence: str


@dataclass(frozen=True, slots=True)
class SnapshotData:
    """Bounded numeric snapshot fields and their source presence states."""

    fields: dict[str, tuple[str, float | None]]
    timestamp: datetime
    raw_timestamp: Any
    events: tuple[NormalizedHealthEvent, ...] = ()
    calendar_date: date | None = None


@dataclass(frozen=True, slots=True)
class TrainingDeviceSnapshots:
    """One Garmin-computed training snapshot per returned device."""

    snapshots: dict[str, SnapshotData]


@dataclass(frozen=True, slots=True)
class NormalizedHealthEvent:
    """Sanitized Garmin event without health-value payloads."""

    logical_id: str
    revision: str
    calendar_date: date
    source: str | None
    event_type: str | None
    category: str | None
    start: datetime | None
    end: datetime | None
    occurrence: datetime | None


@dataclass(frozen=True, slots=True)
class NormalizedActivity:
    """Sanitized timed activity summary; route and FIT payloads excluded."""

    logical_id: str
    activity_id: str
    revision: str
    activity_type: str
    name: str | None
    start: datetime
    end: datetime | None
    duration_seconds: float | None
    training_effect: float | None
    load: float | None
    recovery: float | None
    calendar_date: date


def _activity_hashes(
    activity_type: str,
    activity_id: str,
    start: datetime,
    end: datetime | None,
    duration: float | None,
    name: str | None,
    training_effect: float | None,
    load: float | None,
    recovery: float | None,
    source_calendar_date: date,
) -> tuple[str, str]:
    logical_id = hashlib.sha256(f"{activity_id}:{activity_type}:{start.astimezone(UTC).isoformat()}".encode()).hexdigest()[:24]
    revision = hashlib.sha256(json.dumps((activity_type, activity_id, start.isoformat(), end.isoformat() if end else None, duration, name, training_effect, load, recovery, source_calendar_date.isoformat()), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    return logical_id, revision


def normalize_activities(payload: Any, target_date: date) -> tuple[NormalizedActivity, ...]:
    if isinstance(payload, dict):
        payload = next((payload[key] for key in ("activities", "activityList", "data") if isinstance(payload.get(key), list)), [payload])
    if not isinstance(payload, list) or len(payload) > 256:
        raise HistorySchemaError("activities have invalid type")
    result: dict[str, NormalizedActivity] = {}
    for item in payload:
        if not isinstance(item, dict):
            raise HistorySchemaError("activity has invalid type")
        raw_activity_type = item.get("activityType", item.get("activityTypeKey"))
        activity_type = (
            raw_activity_type.get("typeKey")
            if isinstance(raw_activity_type, Mapping)
            else raw_activity_type
        )
        family_markers = (
            item.get("eventType"),
            item.get("eventTypeKey"),
            item.get("sourceType"),
            item.get("source"),
            item.get("sourceName"),
        )
        families = tuple(str(marker).lower() for marker in family_markers if marker is not None)
        type_key = str(activity_type).lower() if activity_type is not None else ""
        non_timed_family = any(
            marker.replace("-", "_").replace(" ", "_") in {"move_iq", "moveiq", "daily", "daily_event"}
            or "move_iq" in marker.replace("-", "_").replace(" ", "_")
            or marker.startswith("daily")
            for marker in (*families, type_key)
        )
        has_event_fields = any(
            key in item
            for key in ("eventId", "eventTime", "eventCategory", "eventPayload", "eventData", "eventFields")
        )
        if non_timed_family or has_event_fields:
            continue
        start_aliases = ("startTime", "startTimeGMT", "startTimeLocal")
        start_raw, start = _timestamp_from_aliases(item, start_aliases)
        if start_raw is _MISSING:
            start_raw = None
        activity_id = item.get("activityId", item.get("activityUUID"))
        end_raw, end = _timestamp_from_aliases(item, ("endTimeGMT", "endTime"))
        if end_raw is _MISSING:
            end_raw = None
        duration_raw = _first_non_null(item, ("durationInSeconds", "duration"))
        if duration_raw is _MISSING:
            duration_raw = None
        if not isinstance(activity_type, str) or len(activity_type) > 64 or not isinstance(activity_id, (str, int)) or not isinstance(start_raw, (str, int, float, datetime)) or (end_raw is None and duration_raw is None):
            raise HistorySchemaError("activity identity has invalid type")
        if start is None:
            raise HistorySchemaError("activity timestamp is invalid")
        def numeric(item_data: dict[str, Any], *names: str) -> float | None:
            value = _first_non_null(item_data, names)
            if value is _MISSING:
                return None
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise HistorySchemaError("activity summary has invalid type")
            return float(value)
        duration = numeric(item, "durationInSeconds", "duration")
        if duration is not None and (not isfinite(duration) or duration < 0):
            raise HistorySchemaError("activity duration is invalid")
        if end is not None and end < start:
            raise HistorySchemaError("activity interval is reversed")
        activity_name = item.get("activityName") if isinstance(item.get("activityName"), str) else None
        training_effect = numeric(item, "trainingEffect", "aerobicTrainingEffect")
        load = numeric(item, "activityTrainingLoad", "trainingLoad")
        recovery = numeric(item, "recoveryTime")
        local_date = _activity_source_calendar_date(item, start)
        logical_id, revision = _activity_hashes(activity_type, str(activity_id), start, end, duration, activity_name, training_effect, load, recovery, local_date)
        result[logical_id] = NormalizedActivity(logical_id, str(activity_id), revision, activity_type, activity_name, start, end, duration, training_effect, load, recovery, local_date)
    return tuple(sorted(result.values(), key=lambda item: (item.start, item.logical_id)))


def activity_from_record(record: Mapping[str, Any]) -> NormalizedActivity:
    try:
        activity_type = record["activity_type"]
        activity_id = record["activity_id"]
        start = _timestamp(record["start"])
        end = _timestamp(record["end"]) if record.get("end") is not None else None
        calendar_date = date.fromisoformat(record["calendar_date"])
        values = (record.get("duration_seconds"), record.get("training_effect"), record.get("load"), record.get("recovery"))
        name = record.get("name")
        if not isinstance(activity_type, str) or len(activity_type) > 64 or not isinstance(activity_id, str) or (name is not None and (not isinstance(name, str) or len(name) > 128)) or start is None or (end is not None and end < start) or any(value is not None and (isinstance(value, bool) or not isinstance(value, int | float)) for value in values):
            raise HistorySchemaError("activity record is invalid")
        duration = values[0]
        if duration is not None and (not isfinite(float(duration)) or duration < 0):
            raise HistorySchemaError("activity record is invalid")
        logical_id, revision = _activity_hashes(activity_type, activity_id, start, end, values[0], name, values[1], values[2], values[3], calendar_date)
        if record.get("logical_id") != logical_id or record.get("revision") != revision:
            raise HistorySchemaError("activity record is inconsistent")
        return NormalizedActivity(logical_id, activity_id, revision, activity_type, name, start, end, values[0], values[1], values[2], values[3], calendar_date)
    except (KeyError, TypeError, ValueError) as err:
        raise HistorySchemaError("activity record is invalid") from err


def _health_identity_revision(event_type: str | None, source: str | None, category: str | None, start: datetime | None, end: datetime | None, occurrence: datetime | None) -> tuple[str, str]:
    identity = (event_type or "event", start.isoformat() if start else None, end.isoformat() if end else None, occurrence.isoformat() if occurrence else None)
    logical_id = hashlib.sha256(json.dumps(identity, separators=(",", ":")).encode()).hexdigest()[:24]
    revision = hashlib.sha256(json.dumps((source, event_type, category, identity), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
    return logical_id, revision


def health_event_record(event: NormalizedHealthEvent) -> dict[str, Any]:
    return {
        "logical_id": event.logical_id, "revision": event.revision,
        "calendar_date": event.calendar_date.isoformat(), "source": event.source,
        "event_type": event.event_type, "category": event.category,
        "start": event.start.isoformat() if event.start else None,
        "end": event.end.isoformat() if event.end else None,
        "occurrence": event.occurrence.isoformat() if event.occurrence else None,
    }


def health_event_from_record(record: Mapping[str, Any]) -> NormalizedHealthEvent:
    if not isinstance(record, Mapping):
        raise HistorySchemaError("health event record is invalid")
    source = record.get("source")
    event_type = record.get("event_type")
    category = record.get("category")
    strings = (source, event_type, category)
    if any(value is not None and (not isinstance(value, str) or len(value) > 64) for value in strings):
        raise HistorySchemaError("health event record is invalid")
    source = source if isinstance(source, str) else None
    event_type = event_type if isinstance(event_type, str) else None
    category = category if isinstance(category, str) else None
    logical_id, revision = record.get("logical_id"), record.get("revision")
    if not isinstance(logical_id, str) or len(logical_id) != 24 or any(char not in "0123456789abcdef" for char in logical_id):
        raise HistorySchemaError("health event record is invalid")
    if not isinstance(revision, str) or len(revision) != 16 or any(char not in "0123456789abcdef" for char in revision):
        raise HistorySchemaError("health event record is invalid")
    try:
        calendar_date = date.fromisoformat(record["calendar_date"])
        values = {
            key: _timestamp_from_aliases(record, (key,), reject_malformed=True)[1]
            for key in ("start", "end", "occurrence")
        }
    except (KeyError, TypeError, ValueError) as err:
        raise HistorySchemaError("health event record is invalid") from err
    expected_id, expected_revision = _health_identity_revision(event_type, source, category, values["start"], values["end"], values["occurrence"])
    if logical_id != expected_id or revision != expected_revision:
        raise HistorySchemaError("health event record is inconsistent")
    event = NormalizedHealthEvent(logical_id, revision, calendar_date, source, event_type, category, values["start"], values["end"], values["occurrence"])
    return event


HistorySeries = tuple[NormalizedSample, ...]
HistoryResult = HistorySeries | tuple[SleepSession, ...] | tuple[NormalizedHealthEvent, ...] | tuple[NormalizedActivity, ...]
HistoryDetails = HistorySeries | HRVData | SegmentedData | SourceSeries | SnapshotData | TrainingDeviceSnapshots | tuple[SleepSession, ...] | tuple[NormalizedHealthEvent, ...] | tuple[NormalizedActivity, ...]


def normalize_health_events(payload: Any, target_date: date) -> tuple[NormalizedHealthEvent, ...]:
    """Normalize explicit event identity/category/time fields only."""
    if payload is None:
        return ()
    if isinstance(payload, dict) and "abnormalHRValuesArray" in payload:
        values = payload["abnormalHRValuesArray"]
        if values is None:
            return ()
        if not isinstance(values, list) or len(values) > 512:
            raise HistorySchemaError("abnormal events have invalid type")
        payload = {
            "events": [
                {"source": "GARMIN", "type": "abnormalHeartRate", "category": "abnormal",
                 "occurrenceTime": row[0] if isinstance(row, list) and row else row.get("timestamp") if isinstance(row, dict) else None}
                for row in values
            ]
        }
    raw_events: Any = payload
    if isinstance(payload, dict):
        event_aliases = ("events", "dailyEvents", "bodyBatteryEvents", "eventList")
        raw_events = _MISSING
        for event_key in event_aliases:
            if event_key not in payload:
                continue
            candidate = payload[event_key]
            if candidate is None or (
                isinstance(candidate, (list, dict)) and not candidate
            ):
                continue
            if not isinstance(candidate, (list, dict)):
                raise HistorySchemaError("health events have invalid type")
            raw_events = candidate
            break
        if raw_events is _MISSING:
            if not any(key in payload for key in event_aliases):
                raw_events = payload
            else:
                return ()
    if isinstance(raw_events, dict):
        raw_events = [raw_events]
    if not isinstance(raw_events, list):
        raise HistorySchemaError("health events have invalid type")
    if len(raw_events) > 512:
        raise HistorySchemaError("health event batch exceeds bounded limit")
    result: dict[str, NormalizedHealthEvent] = {}
    for raw_event in raw_events[:512]:
        if not isinstance(raw_event, dict):
            raise HistorySchemaError("health event has invalid type")
        event = raw_event
        nested_event = raw_event.get("event")
        if nested_event is not None:
            if not isinstance(nested_event, dict):
                raise HistorySchemaError("health event envelope has invalid type")
            event = dict(nested_event)
            event["source"] = "GARMIN"
            if "eventStartTimeGmt" in event:
                event["startTimeGMT"] = event["eventStartTimeGmt"]
            duration = event.get("durationInMilliseconds")
            if duration is not None:
                if isinstance(duration, bool) or not isinstance(duration, int | float):
                    raise HistorySchemaError("health event duration has invalid type")
                _, event_start = _timestamp_from_aliases(
                    event, ("startTimeGMT",), reject_malformed=True
                )
                if event_start is not None:
                    event["endTimeGMT"] = (
                        event_start + timedelta(milliseconds=float(duration))
                    ).isoformat()
            if "feedbackType" in event and "category" not in event:
                event["category"] = event["feedbackType"]
        source = next((event[key] for key in ("source", "eventSource") if key in event), None)
        event_type = next((event[key] for key in ("type", "eventType") if key in event), None)
        category = next((event[key] for key in ("category", "eventCategory") if key in event), None)
        if any(value is not None and (not isinstance(value, str) or len(value) > 64) for value in (source, event_type, category)):
            raise HistorySchemaError("health event identity has invalid type")
        _, start = _timestamp_from_aliases(
            event, ("startTime", "startTimeGMT", "start"), reject_malformed=True
        )
        _, end = _timestamp_from_aliases(
            event, ("endTime", "endTimeGMT", "end"), reject_malformed=True
        )
        _, occurrence = _timestamp_from_aliases(
            event,
            ("occurrenceTime", "occurrenceTimeGMT", "eventTime", "timestamp", "time"),
            reject_malformed=True,
        )
        if all(value is None for value in (start, end, occurrence)):
            continue
        logical_id, revision = _health_identity_revision(event_type, source, category, start, end, occurrence)
        result[logical_id] = NormalizedHealthEvent(logical_id, revision, target_date, source, event_type, category, start, end, occurrence)
    return tuple(sorted(result.values(), key=lambda item: (item.start or item.occurrence or datetime.min.replace(tzinfo=UTC), item.logical_id)))


def normalize_snapshot(
    payload: Any,
    target_date: date,
    field_aliases: Mapping[str, tuple[str, ...]],
) -> SnapshotData:
    """Extract known numeric fields without inferring absent or null values."""
    if payload is None:
        return SnapshotData(
            dict.fromkeys(field_aliases, ("null", None)),
            _calendar_bucket(target_date),
            target_date.isoformat(),
            calendar_date=target_date,
        )
    if not isinstance(payload, dict):
        raise HistorySchemaError("snapshot payload is not an object")
    calendar_date = _snapshot_calendar_date(payload.get("calendarDate", _MISSING), target_date)
    timestamp_value = _first_non_null(payload, ("timestamp", "startTime"))
    if timestamp_value is _MISSING or timestamp_value is None:
        timestamp = _calendar_bucket(calendar_date)
        raw_timestamp = calendar_date.isoformat()
    else:
        source_instant = _timestamp(timestamp_value)
        if source_instant is None:
            raise HistorySchemaError("snapshot timestamp is invalid")
        timestamp = source_instant
        raw_timestamp = timestamp_value
    fields: dict[str, tuple[str, float | None]] = {}
    for name, aliases in field_aliases.items():
        found = _nested_value(payload, aliases)
        if found is None:
            fields[name] = ("absent", None)
            continue
        value = found[0]
        if value is None:
            fields[name] = ("null", None)
        elif isinstance(value, bool) or not isinstance(value, int | float):
            raise HistorySchemaError(f"{name} has an invalid type")
        else:
            fields[name] = ("present", float(value))
    events = normalize_health_events(payload, target_date) if "abnormalHRValuesArray" in payload or "abnormalHeartRateEvents" in payload else ()
    return SnapshotData(fields, timestamp, raw_timestamp, events, calendar_date)


DAILY_SUMMARY_FIELDS = {
    "abnormal_heart_rate_alerts": ("abnormalHeartRateAlertsCount",),
    "floors_ascended": ("floorsAscended",),
    "floors_descended": ("floorsDescended",),
    "floors_ascended_meters": ("floorsAscendedInMeters",),
    "floors_descended_meters": ("floorsDescendedInMeters",),
    "intensity_moderate": ("moderateIntensityMinutes",),
    "intensity_vigorous": ("vigorousIntensityMinutes",),
}
TRAINING_STATUS_FIELDS = {
    "acute_load": ("acuteLoad",), "chronic_load": ("chronicLoad",),
    "load_balance": ("loadBalance",), "acwr": ("acwr", "acuteChronicWorkloadRatio"),
    "vo2_max": ("vo2Max", "vo2MaxValue"), "fitness_trend": ("fitnessTrend",),
    "recovery_time": ("recoveryTime",),
}


def normalize_training_status(
    payload: Any, target_date: date
) -> SnapshotData | TrainingDeviceSnapshots:
    """Normalize flat legacy or current device-keyed training status payloads."""
    if not isinstance(payload, dict) or "mostRecentTrainingStatus" not in payload:
        return normalize_snapshot(payload, target_date, TRAINING_STATUS_FIELDS)
    status_container = payload.get("mostRecentTrainingStatus")
    if not isinstance(status_container, dict):
        raise HistorySchemaError("training status container has invalid type")
    status_by_device = status_container.get("latestTrainingStatusData")
    if status_by_device is None:
        return TrainingDeviceSnapshots({})
    if not isinstance(status_by_device, dict):
        raise HistorySchemaError("training status devices have invalid type")
    if len(status_by_device) > 32:
        raise HistorySchemaError("training status has too many devices")

    vo2_by_device: dict[str, Any] = {}
    vo2_container = payload.get("mostRecentVO2Max")
    if vo2_container is not None:
        if not isinstance(vo2_container, dict):
            raise HistorySchemaError("training VO2 container has invalid type")
        for candidate in vo2_container.values():
            if not isinstance(candidate, dict):
                continue
            device_id = candidate.get("deviceId")
            if isinstance(device_id, str | int) and not isinstance(device_id, bool):
                for key in ("vo2MaxValue", "vo2MaxPreciseValue"):
                    if key in candidate:
                        vo2_by_device[str(device_id)] = candidate[key]
                        break

    snapshots: dict[str, SnapshotData] = {}
    for map_device_id, item in status_by_device.items():
        if not isinstance(item, dict):
            raise HistorySchemaError("training device snapshot has invalid type")
        raw_device_id = item.get("deviceId", map_device_id)
        if (
            isinstance(raw_device_id, bool)
            or not isinstance(raw_device_id, str | int)
            or not str(raw_device_id)
            or len(str(raw_device_id)) > 64
        ):
            raise HistorySchemaError("training device identity has invalid type")
        device_id = str(raw_device_id)
        if device_id in snapshots:
            raise HistorySchemaError("training device identity is duplicated")
        acute = item.get("acuteTrainingLoadDTO")
        if acute is None:
            acute = {}
        if not isinstance(acute, dict):
            raise HistorySchemaError("training load snapshot has invalid type")
        flattened = {
            "calendarDate": item.get("calendarDate", target_date.isoformat()),
        }
        for source_key, target_key in (
            ("dailyTrainingLoadAcute", "acuteLoad"),
            ("dailyTrainingLoadChronic", "chronicLoad"),
            ("dailyAcuteChronicWorkloadRatio", "acwr"),
        ):
            if source_key in acute:
                flattened[target_key] = acute[source_key]
        if "fitnessTrend" in item:
            flattened["fitnessTrend"] = item["fitnessTrend"]
        if device_id in vo2_by_device:
            flattened["vo2MaxValue"] = vo2_by_device[device_id]
        snapshots[device_id] = normalize_snapshot(
            flattened, target_date, TRAINING_STATUS_FIELDS
        )
    return TrainingDeviceSnapshots(snapshots)


def _timestamp(value: Any, *, allow_date_only: bool = False) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return None
        return value.astimezone(UTC)
    if isinstance(value, int | float):
        number = float(value) / (1000 if abs(float(value)) >= 100_000_000_000 else 1)
        try:
            return datetime.fromtimestamp(number, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            if not allow_date_only or parsed.time() != datetime.min.time() or value != parsed.date().isoformat():
                return None
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


def _timestamp_as_utc(value: Any) -> datetime | None:
    """Parse a Garmin GMT field whose name supplies the UTC timezone."""
    parsed = _timestamp(value)
    if parsed is not None:
        return parsed
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
    return None


def _timestamp_from_aliases(
    mapping: Mapping[str, Any],
    aliases: tuple[str, ...],
    *,
    reject_malformed: bool = False,
) -> tuple[Any, datetime | None]:
    """Select a timestamp alias, optionally failing on non-empty bad values."""
    first_value: Any = _MISSING
    for alias in aliases:
        value = mapping.get(alias)
        if value is None or (isinstance(value, str) and not value):
            continue
        if first_value is _MISSING:
            first_value = value
        parsed = (
            _timestamp_as_utc(value)
            if alias.endswith("GMT")
            else _timestamp(value)
        )
        if parsed is not None:
            return value, parsed
        if reject_malformed:
            raise HistorySchemaError("health event timestamp is invalid")
    return first_value, None


def _activity_source_calendar_date(
    item: Mapping[str, Any], source_start: datetime
) -> date:
    """Prefer an activity's source-local date over its instant's UTC date."""
    local_start = item.get("startTimeLocal", _MISSING)
    if local_start is _MISSING or local_start is None or local_start == "":
        return source_start.date()
    if isinstance(local_start, datetime):
        return local_start.date()
    if isinstance(local_start, str):
        try:
            return datetime.fromisoformat(local_start.replace("Z", "+00:00")).date()
        except ValueError as err:
            raise HistorySchemaError("activity local timestamp is invalid") from err
    raise HistorySchemaError("activity local timestamp is invalid")


def _snapshot_calendar_date(value: Any, fallback: date) -> date:
    """Validate a snapshot calendar date without treating it as an instant."""
    if value is _MISSING or value is None:
        return fallback
    if not isinstance(value, str):
        raise HistorySchemaError("snapshot calendar date is invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as err:
        raise HistorySchemaError("snapshot calendar date is invalid") from err
    if parsed.isoformat() != value:
        raise HistorySchemaError("snapshot calendar date is invalid")
    return parsed


def _calendar_bucket(calendar_date: date) -> datetime:
    """Return the canonical UTC+08:00 instant for a date-summary bucket."""
    return datetime.combine(calendar_date, datetime.min.time(), tzinfo=_CALENDAR_BUCKET_TIME_ZONE).astimezone(UTC)


def _descriptors(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, int]:
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if raw is None or raw == []:
            continue
        if not isinstance(raw, list):
            raise HistorySchemaError(f"{key} is not a descriptor list")
        result: dict[str, int] = {}
        for item in raw:
            if not isinstance(item, dict):
                raise HistorySchemaError(f"{key} contains a malformed descriptor")
            descriptor_key = item.get("key")
            index = item.get("index")
            if descriptor_key is None and index is None:
                pairs = [
                    (name, name.removesuffix("Index") + "Key")
                    for name in item
                    if name.endswith("Index") and name.removesuffix("Index") + "Key" in item
                ]
                if len(pairs) == 1:
                    index_name, key_name = pairs[0]
                    index = item[index_name]
                    descriptor_key = item[key_name]
            if (
                not isinstance(descriptor_key, str)
                or not isinstance(index, int)
                or isinstance(index, bool)
            ):
                raise HistorySchemaError(f"{key} contains a malformed descriptor")
            if index < 0 or descriptor_key in result or index in result.values():
                raise HistorySchemaError(f"{key} contains an invalid descriptor index")
            result[descriptor_key] = index
        return result
    return {}


def normalize_pair_series(
    payload: dict[str, Any],
    *,
    values_key: str,
    descriptor_keys: tuple[str, ...],
    value_keys: tuple[str, ...],
    exclude_negative: bool = False,
    request_date: date | None = None,
) -> tuple[NormalizedSample, ...]:
    """Normalize a descriptor-driven Garmin ``[timestamp, value]`` series."""
    if not isinstance(payload, dict):
        raise HistorySchemaError("series payload is not an object")
    raw_points = payload.get(values_key)
    if raw_points is None:
        return ()
    if not isinstance(raw_points, list):
        raise HistorySchemaError(f"{values_key} is not an array")
    descriptor_present = any(key in payload for key in descriptor_keys)
    positions = _descriptors(payload, descriptor_keys)
    if descriptor_present and raw_points and (
        not any(key in positions for key in ("timestamp", "time"))
        or not any(key in positions for key in value_keys)
    ):
        raise HistorySchemaError("descriptor list lacks required fields")
    effective_date = request_date
    latest: dict[datetime, NormalizedSample] = {}
    for point in raw_points:
        if point is None:
            continue
        if not isinstance(point, (list, tuple)):
            raise HistorySchemaError(f"{values_key} point has an invalid type")
        raw_time = _first_non_null_sequence(
            point, positions, ("timestamp", "time"), fallback_index=0
        )
        raw_value = _first_non_null_sequence(
            point, positions, value_keys, fallback_index=1
        )
        if raw_time is not None and not isinstance(raw_time, str | int | float):
            raise HistorySchemaError("timestamp has an invalid type")
        if raw_value is not None and (isinstance(raw_value, bool) or not isinstance(raw_value, int | float)):
            raise HistorySchemaError("value has an invalid type")
        if raw_time is None or raw_value is None:
            continue
        parsed = _timestamp(raw_time)
        if parsed is None:
            raise HistorySchemaError("timestamp has an invalid value")
        if effective_date is None:
            if isinstance(raw_time, str):
                try:
                    local_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                except ValueError:
                    local_time = parsed
                effective_date = local_time.date()
            else:
                effective_date = parsed.date()
        if exclude_negative and raw_value < 0:
            continue
        latest[parsed] = NormalizedSample(parsed, effective_date, raw_time, float(raw_value))
    return tuple(latest[key] for key in sorted(latest))


def _object_series(
    payload: Any,
    target_date: date,
    value_keys: tuple[str, ...],
    list_keys: tuple[str, ...],
    *,
    exclude_negative: bool = False,
) -> tuple[NormalizedSample, ...]:
    if payload is None or payload == []:
        return ()
    if isinstance(payload, list):
        payload = {"data": payload}
    if not isinstance(payload, dict):
        raise HistorySchemaError("segmented payload is not an object")
    points = next((payload[key] for key in list_keys if key in payload), None)
    if points is None:
        return ()
    if not isinstance(points, list):
        raise HistorySchemaError("segmented values are not an array")
    result: dict[datetime, NormalizedSample] = {}
    for point in points:
        if point is None:
            continue
        if not isinstance(point, dict):
            raise HistorySchemaError("segmented point is not an object")
        timestamp_aliases = (
            "timestamp",
            "time",
            "startTime",
            "start",
            "readingTime",
            "readingTimeGMT",
            "startTimeGMT",
            "startGMT",
        )
        timestamp_key = next(
            (key for key in timestamp_aliases if point.get(key) is not None), None
        )
        raw_time = point[timestamp_key] if timestamp_key is not None else _MISSING
        raw_value = _first_non_null(point, value_keys)
        if raw_time is _MISSING or raw_value is _MISSING:
            raise HistorySchemaError("segmented point lacks required fields")
        if raw_time is None or raw_value is None:
            continue
        if not isinstance(raw_time, str | int | float) or isinstance(raw_time, bool):
            raise HistorySchemaError("segmented timestamp has an invalid type")
        if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
            raise HistorySchemaError("segmented value has an invalid type")
        parsed = (
            _timestamp_as_utc(raw_time)
            if timestamp_key is not None and timestamp_key.endswith("GMT")
            else _timestamp(raw_time)
        )
        if parsed is None:
            raise HistorySchemaError("segmented timestamp has an invalid value")
        if exclude_negative and raw_value < 0:
            continue
        result[parsed] = NormalizedSample(parsed, target_date, raw_time, float(raw_value))
    return tuple(result[key] for key in sorted(result))


def _nested_array_value(
    payload: Any, aliases: tuple[str, ...]
) -> tuple[list[Any] | None, str] | None:
    """Select the first non-null, non-empty supported array alias."""
    empty: tuple[list[Any] | None, str] | None = None
    null: tuple[list[Any] | None, str] | None = None
    for alias in aliases:
        found = _nested_value(payload, (alias,))
        if found is None:
            continue
        value, key = found
        if value is None:
            null = null or (None, key)
            continue
        if not isinstance(value, list):
            raise HistorySchemaError(f"{key} is not an array")
        if value:
            return value, key
        empty = empty or (value, key)
    return empty or null


def _has_non_null_series_value(
    values: list[Any],
    value_keys: tuple[str, ...],
    descriptor_payload: dict[str, Any],
    descriptor_keys: tuple[str, ...],
) -> bool:
    """Identify numeric values even when a quality filter removed them."""
    positions = _descriptors(descriptor_payload, descriptor_keys)
    for row in values:
        if row is None:
            continue
        if isinstance(row, dict):
            raw_value = _first_non_null(row, value_keys)
        elif isinstance(row, (list, tuple)):
            raw_value = _first_non_null_sequence(
                row, positions, value_keys, fallback_index=1
            )
        else:
            continue
        if raw_value is not _MISSING and raw_value is not None:
            return True
    return False


def _normalized_presence(
    presence: str,
    values: list[Any] | None,
    readings: tuple[NormalizedSample, ...],
    value_keys: tuple[str, ...],
    descriptor_payload: dict[str, Any],
    descriptor_keys: tuple[str, ...],
) -> str:
    """Add the all-null state without reclassifying sparse or zero data."""
    if (
        presence == "present"
        and values
        and not readings
        and not _has_non_null_series_value(
            values, value_keys, descriptor_payload, descriptor_keys
        )
    ):
        return "all-null"
    return presence


def _nested_value(payload: Any, aliases: tuple[str, ...], depth: int = 0) -> tuple[Any, str] | None:
    if depth > 3 or not isinstance(payload, dict):
        return None
    null_value: tuple[Any, str] | None = None
    for alias in aliases:
        if alias in payload:
            value = payload[alias], alias
            if value[0] is not None:
                return value
            if null_value is None:
                null_value = value
    for container in ("data", "report", "summary", "result", "trainingStatus", "dailySummary"):
        if container in payload:
            found = _nested_value(payload[container], aliases, depth + 1)
            if found is not None and found[0] is not None:
                return found
            if null_value is None and found is not None:
                null_value = found
    return null_value


def _first_non_null(mapping: Mapping[str, Any], aliases: tuple[str, ...]) -> Any:
    """Select the first available alias whose value is not null."""
    null_found = False
    for alias in aliases:
        if alias not in mapping:
            continue
        value = mapping[alias]
        if value is not None:
            return value
        null_found = True
    return None if null_found else _MISSING


def _first_non_null_sequence(
    values: list[Any] | tuple[Any, ...],
    positions: Mapping[str, int],
    aliases: tuple[str, ...],
    *,
    fallback_index: int,
) -> Any:
    """Select the first non-null descriptor column without hiding bad shapes."""
    if not positions:
        if fallback_index >= len(values):
            raise HistorySchemaError("series point is narrower than its descriptors")
        return values[fallback_index]
    found_null = False
    for alias in aliases:
        index = positions.get(alias)
        if index is None:
            continue
        if index >= len(values):
            raise HistorySchemaError("series point is narrower than its descriptors")
        value = values[index]
        if value is not None:
            return value
        found_null = True
    return None if found_null else _MISSING


def _classify_source_array(payload: Any, aliases: tuple[str, ...]) -> tuple[str, Any, str | None]:
    """Classify a source array and return its values when present."""
    if payload is None:
        return "null", None, None
    if isinstance(payload, list):
        return ("empty" if not payload else "present"), payload, None
    if not isinstance(payload, dict):
        raise HistorySchemaError("known numeric payload is not an object or array")
    marker = _nested_value(payload, ("presence", "state", "status", "availability"))
    if marker is not None and isinstance(marker[0], str) and marker[0].lower() == "returned-empty":
        return "returned-empty", None, None
    found = _nested_array_value(payload, aliases)
    if found is None:
        return "missing", None, None
    values, array_key = found
    if values is None:
        return "null", None, array_key
    if values == []:
        return "empty", values, array_key
    if not isinstance(values, list):
        raise HistorySchemaError(f"{array_key} is not an array")
    return "present", values, array_key


def _is_object_series(values: list[Any]) -> bool:
    """Recognize object points while ignoring null placeholders."""
    non_null = [value for value in values if value is not None]
    return bool(non_null) and all(isinstance(value, dict) for value in non_null)


def _normalize_source_series(
    payload: Any,
    target_date: date,
    array_aliases: tuple[str, ...],
    value_keys: tuple[str, ...],
    descriptor_aliases: tuple[str, ...],
    *,
    exclude_negative: bool = False,
) -> SourceSeries:
    presence, values, array_key = _classify_source_array(payload, array_aliases)
    if presence != "present":
        return SourceSeries((), presence)
    series_payload: dict[str, Any]
    if array_key is None:
        series_payload = {"data": values}
        if _is_object_series(values):
            readings = _object_series(
                payload,
                target_date,
                value_keys,
                ("data",),
                exclude_negative=exclude_negative,
            )
        else:
            readings = normalize_pair_series(
                series_payload,
                values_key="data",
                descriptor_keys=descriptor_aliases,
                value_keys=value_keys,
                exclude_negative=exclude_negative,
                request_date=target_date,
            )
        return SourceSeries(
            readings,
            _normalized_presence(
                presence, values, readings, value_keys, series_payload, descriptor_aliases
            ),
        )
    descriptor_found = _nested_array_value(payload, descriptor_aliases)
    series_payload = {array_key: values}
    if descriptor_found is not None:
        series_payload[descriptor_found[1]] = descriptor_found[0]
    if _is_object_series(values):
        readings = _object_series(
            series_payload,
            target_date,
            value_keys,
            (array_key,),
            exclude_negative=exclude_negative,
        )
    else:
        readings = normalize_pair_series(
            series_payload,
            values_key=array_key,
            descriptor_keys=descriptor_aliases,
            value_keys=value_keys,
            exclude_negative=exclude_negative,
            request_date=target_date,
        )
    return SourceSeries(
        readings,
        _normalized_presence(
            presence,
            values,
            readings,
            value_keys,
            series_payload,
            descriptor_aliases,
        ),
    )


def _array_presence(payload: Any, aliases: tuple[str, ...]) -> str:
    """Classify one source array without turning absence into an empty sample set."""
    return _classify_source_array(payload, aliases)[0]


def normalize_respiration(payload: Any, target_date: date, averages: bool = False) -> SourceSeries:
    aliases = ("respirationAveragesValuesArray",) if averages else ("respirationValuesArray",)
    return _normalize_source_series(payload, target_date, aliases, ("respiration", "respirationValue", "value"), ("respirationValueDescriptors", "respirationValueDescriptorsDTOList"))


def normalize_spo2(payload: Any, target_date: date, variant: str) -> SourceSeries:
    configs = {
        "single": (("spO2SingleValues", "spo2SingleValues", "singleValues"), ("spO2", "spo2", "spO2Reading", "spo2Reading", "value")),
        "continuous": (("continuousReadingDTOList", "spO2ContinuousValues", "spo2ContinuousValues", "continuousValues"), ("spO2", "spo2", "spO2Reading", "spo2Reading", "reading", "value")),
        "hourly": (("spO2HourlyAverages", "spo2HourlyAverages", "hourlyAverages"), ("spO2", "spo2", "spO2Reading", "spo2Reading", "average", "value")),
    }
    if variant not in configs:
        raise ValueError("unsupported SpO2 variant")
    return _normalize_source_series(payload, target_date, *configs[variant], ("spO2ValueDescriptors", "spO2ValueDescriptorsDTOList"))


def _total_values(
    payload: Any, keys: tuple[str, ...]
) -> tuple[dict[str, float], dict[str, str]]:
    result: dict[str, float] = {}
    states = dict.fromkeys(keys, "absent")
    key_set = set(keys)

    def visit(value: Any, depth: int) -> None:
        if depth > 3 or value is None:
            return
        if isinstance(value, dict):
            for name, item in value.items():
                if name in key_set:
                    if item is None:
                        if states[name] != "present":
                            states[name] = "null"
                        continue
                    if isinstance(item, bool) or not isinstance(item, int | float):
                        raise HistorySchemaError("daily total has an invalid type")
                    result[name] = float(item)
                    states[name] = "present"
                elif name in {"report", "summary", "data", "daily", "totals", "metrics"} and isinstance(item, dict):
                    # Intraday point arrays are source readings, never daily totals.
                    visit(item, depth + 1)

    visit(payload, 0)
    return result, states


def _totals(payload: Any, keys: tuple[str, ...]) -> dict[str, float] | None:
    return _total_values(payload, keys)[0] or None


def _total_presence(payload: Any, keys: tuple[str, ...]) -> dict[str, str]:
    return _total_values(payload, keys)[1]


def _segmented_totals(
    payload: Any, keys: tuple[str, ...]
) -> tuple[dict[str, float] | None, dict[str, str]]:
    values, states = _total_values(payload, keys)
    return values or None, states


def _normalize_segmented(
    payload: Any,
    target_date: date,
    array_aliases: tuple[str, ...],
    value_keys: tuple[str, ...],
    descriptor_keys: tuple[str, ...],
    total_keys: tuple[str, ...],
) -> SegmentedData:
    presence, values, array_key = _classify_source_array(payload, array_aliases)
    totals, total_presence = _segmented_totals(payload, total_keys)
    if presence != "present":
        return SegmentedData((), totals, presence, total_presence)
    selected_key = array_key or "data"
    series_payload: dict[str, Any] = {selected_key: values}
    descriptor_found = _nested_array_value(payload, descriptor_keys)
    if descriptor_found is not None:
        descriptor_values, descriptor_key = descriptor_found
        series_payload[descriptor_key] = descriptor_values
    if _is_object_series(values):
        readings = _object_series(
            series_payload, target_date, value_keys, (selected_key,)
        )
    else:
        readings = normalize_pair_series(
            series_payload,
            values_key=selected_key,
            descriptor_keys=descriptor_keys,
            value_keys=value_keys,
            request_date=target_date,
        )
    return SegmentedData(
        readings,
        totals,
        _normalized_presence(
            presence, values, readings, value_keys, series_payload, descriptor_keys
        ),
        total_presence,
    )


def normalize_steps(payload: Any, target_date: date) -> SegmentedData:
    return _normalize_segmented(
        payload,
        target_date,
        ("stepsValues", "stepsValuesArray", "chartData", "data"),
        ("steps", "stepCount", "value"),
        ("stepsValueDescriptors", "stepsValueDescriptorsDTOList", "stepsValueDescriptorDTOList"),
        ("totalSteps",),
    )


def normalize_floors(payload: Any, target_date: date) -> SegmentedData:
    return _normalize_segmented(
        payload,
        target_date,
        ("floorsValues", "floorsValuesArray", "chartData", "data"),
        ("floors", "floorCount", "value"),
        ("floorsValueDescriptors", "floorsValueDescriptorsDTOList", "floorsValueDescriptorDTOList"),
        ("floorsAscended", "floorsDescended", "floorsAscendedInMeters", "floorsDescendedInMeters", "totalFloors"),
    )


def normalize_intensity(payload: Any, target_date: date, kind: str) -> SegmentedData:
    if kind not in {"moderate", "vigorous"}:
        raise ValueError("unsupported intensity kind")
    keys = (f"{kind}IntensityMinutes", f"{kind}Minutes", "value")
    return _normalize_segmented(
        payload,
        target_date,
        ("imValuesArray", "intensityValues", "intensityValuesArray", "chartData", "data"),
        keys,
        (
            "imValueDescriptorsDTOList",
            "intensityValueDescriptors",
            "intensityValueDescriptorsDTOList",
            "intensityValueDescriptorDTOList",
        ),
        ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes"),
    )


def _descriptor_segment(payload: Any, target_date: date, values_keys: tuple[str, ...], value_keys: tuple[str, ...], descriptor_keys: tuple[str, ...]) -> tuple[NormalizedSample, ...] | None:
    if not isinstance(payload, dict) or not any(key in payload for key in descriptor_keys):
        return None
    found = _nested_array_value(payload, values_keys)
    if found is None:
        return ()
    values, values_key = found
    series_payload = {values_key: values}
    descriptor_found = _nested_array_value(payload, descriptor_keys)
    if descriptor_found is not None:
        descriptor_values, descriptor_key = descriptor_found
        series_payload[descriptor_key] = descriptor_values
    return normalize_pair_series(
        series_payload,
        values_key=values_key,
        descriptor_keys=descriptor_keys,
        value_keys=value_keys,
        request_date=target_date,
    )


def _select_daily_report(payload: Any, target_date: date) -> dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        for key in ("bodyBatteryReports", "reports", "dailyReports"):
            if key in payload:
                if payload[key] is None:
                    return {}
                return _select_daily_report(payload[key], target_date)
        return payload
    if payload == []:
        return {}
    if not isinstance(payload, list):
        raise HistorySchemaError("body battery reports are not an array")
    for report in payload:
        if report is None:
            continue
        if not isinstance(report, dict):
            raise HistorySchemaError("body battery report is not an object")
        for key in ("calendarDate", "date", "reportDate"):
            value = report.get(key)
            if isinstance(value, str) and value[:10] == target_date.isoformat():
                return report
    return {}


def normalize_body_battery(payload: Any, target_date: date) -> tuple[NormalizedSample, ...]:
    """Normalize one daily body-battery report with descriptor-defined columns."""
    report = _select_daily_report(payload, target_date)
    return normalize_pair_series(
        report,
        values_key="bodyBatteryValuesArray",
        descriptor_keys=(
            "bodyBatteryValueDescriptorsDTOList",
            "bodyBatteryValueDescriptorsDtoList",
            "bodyBatteryValueDescriptorDTOList",
            "bodyBatteryValueDescriptors",
        ),
        value_keys=("bodyBatteryValue", "bodyBatteryLevel", "value"),
        request_date=target_date,
    )


def _body_battery_presence(payload: Any, target_date: date) -> str:
    """Classify the selected Body Battery report before numeric normalization."""
    if payload is None:
        return "null"
    if payload == []:
        return "empty"
    if isinstance(payload, dict):
        for key in ("bodyBatteryReports", "reports", "dailyReports"):
            if key not in payload:
                continue
            reports = payload[key]
            if reports is None:
                return "null"
            if reports == []:
                return "empty"
            break
    elif not isinstance(payload, list):
        return _array_presence(payload, ("bodyBatteryValuesArray",))
    report = _select_daily_report(payload, target_date)
    presence, values, array_key = _classify_source_array(
        report, ("bodyBatteryValuesArray",)
    )
    if presence != "present":
        return presence
    readings = normalize_body_battery(payload, target_date)
    descriptor_payload = report if array_key is not None else {"data": values}
    return _normalized_presence(
        presence,
        values,
        readings,
        ("bodyBatteryValue", "bodyBatteryLevel", "value"),
        descriptor_payload,
        (
            "bodyBatteryValueDescriptorsDTOList",
            "bodyBatteryValueDescriptorsDtoList",
            "bodyBatteryValueDescriptorDTOList",
            "bodyBatteryValueDescriptors",
        ),
    )


def _merge_source_series(primary: SourceSeries, supplemental: SourceSeries) -> SourceSeries:
    """Merge equal-timestamp source records without hiding conflicting values."""
    readings = {sample.timestamp: sample for sample in primary.readings}
    for sample in supplemental.readings:
        previous = readings.get(sample.timestamp)
        if previous is not None and previous.value != sample.value:
            raise HistorySchemaError("overlapping source series values conflict")
        readings[sample.timestamp] = previous or sample
    if readings:
        presence = "present"
    elif primary.presence == supplemental.presence:
        presence = primary.presence
    elif "failed" in {primary.presence, supplemental.presence}:
        presence = "failed"
    else:
        presence = primary.presence
    return SourceSeries(tuple(readings[key] for key in sorted(readings)), presence)


def _normalize_body_battery_event_series(
    payload: Any, target_date: date, metric: str
) -> SourceSeries:
    """Extract numeric records carried by live Body Battery event envelopes."""
    if payload is None:
        return SourceSeries((), "null")
    if not isinstance(payload, list):
        raise HistorySchemaError("body battery events have invalid type")
    if not payload:
        return SourceSeries((), "empty")
    if len(payload) > 512:
        raise HistorySchemaError("body battery event batch exceeds bounded limit")
    samples: dict[datetime, NormalizedSample] = {}
    for event in payload:
        if not isinstance(event, dict):
            raise HistorySchemaError("body battery event has invalid type")
        if metric == "body_battery":
            values_key = "bodyBatteryValuesArray"
            descriptor_keys = (
                "bodyBatteryValueDescriptorsDTOList",
                "bodyBatteryValueDescriptorDTOList",
            )
            value_keys = ("bodyBatteryLevel", "bodyBatteryValue", "value")
        elif metric == "stress":
            values_key = "stressValuesArray"
            descriptor_keys = (
                "stressValueDescriptorsDTOList",
                "stressValueDescriptorsDtoList",
            )
            value_keys = ("stressLevel", "stress", "value")
        else:
            raise ValueError("unsupported body battery event metric")
        if values_key not in event:
            continue
        normalized = normalize_pair_series(
            event,
            values_key=values_key,
            descriptor_keys=descriptor_keys,
            value_keys=value_keys,
            request_date=target_date,
        )
        for sample in normalized:
            previous = samples.get(sample.timestamp)
            if previous is not None and previous.value != sample.value:
                raise HistorySchemaError("body battery event values conflict")
            samples[sample.timestamp] = previous or sample
    return SourceSeries(
        tuple(samples[key] for key in sorted(samples)),
        "present" if samples else "missing",
    )


def parse_hrv_data(payload: Any, target_date: date) -> HRVData:
    """Parse HRV readings while tolerating absent summary fields."""
    if payload is None:
        return HRVData((), presence="null")
    if payload == []:
        return HRVData((), presence="empty")
    if not isinstance(payload, dict):
        raise HistorySchemaError("HRV payload is not an object")
    if "hrvReadings" not in payload:
        presence = "missing"
    elif payload["hrvReadings"] is None:
        presence = "null"
    elif payload["hrvReadings"] == []:
        presence = "empty"
    else:
        presence = "present"
    raw_readings = payload.get("hrvReadings", [])
    if raw_readings is None:
        raw_readings = []
    if not isinstance(raw_readings, list):
        raise HistorySchemaError("HRV readings are not an array")
    readings: list[NormalizedSample] = []
    for reading in raw_readings:
        if reading is None:
            continue
        if not isinstance(reading, dict):
            raise HistorySchemaError("HRV reading is not an object")
        timestamp_aliases = ("readingTimeGMT", "readingTimeGmt", "readingTime")
        timestamp_key = next(
            (key for key in timestamp_aliases if reading.get(key) is not None), None
        )
        raw_time = reading[timestamp_key] if timestamp_key is not None else _MISSING
        raw_value = _first_non_null(reading, ("hrvValue", "value"))
        if raw_time is _MISSING or raw_value is _MISSING:
            raise HistorySchemaError("HRV reading lacks required fields")
        if raw_time is not None and not isinstance(raw_time, str | int | float):
            raise HistorySchemaError("HRV timestamp has an invalid type")
        if raw_value is not None and (isinstance(raw_value, bool) or not isinstance(raw_value, int | float)):
            raise HistorySchemaError("HRV value has an invalid type")
        if raw_time is None or raw_value is None:
            continue
        parsed = (
            _timestamp_as_utc(raw_time)
            if timestamp_key is not None and timestamp_key.lower().endswith("gmt")
            else _timestamp(raw_time)
        )
        if parsed is None:
            raise HistorySchemaError("HRV timestamp has an invalid value")
        readings.append(NormalizedSample(parsed, target_date, raw_time, float(raw_value)))
    latest = {sample.timestamp: sample for sample in readings}
    if presence == "present" and raw_readings and not latest:
        if not _has_non_null_series_value(
            raw_readings,
            ("hrvValue", "value"),
            {},
            (),
        ):
            presence = "all-null"
    raw_summary = payload.get("hrvSummary")
    if raw_summary is not None and not isinstance(raw_summary, dict):
        raise HistorySchemaError("HRV summary is not an object")
    summary_data = raw_summary or {}

    def numeric(name: str) -> float | None:
        value = summary_data.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise HistorySchemaError("HRV summary value has an invalid type")
        return float(value)
    raw_baseline = summary_data.get("baseline")
    baseline: dict[str, float] | None = None
    if raw_baseline is not None:
        if not isinstance(raw_baseline, dict):
            raise HistorySchemaError("HRV baseline is not an object")
        baseline = {}
        for key, value in list(raw_baseline.items())[:8]:
            if not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int | float):
                raise HistorySchemaError("HRV baseline has an invalid type")
            baseline[key] = float(value)
    status = summary_data.get("status")
    if status is not None and not isinstance(status, str):
        raise HistorySchemaError("HRV status has an invalid type")
    if isinstance(status, str):
        status = status[:64]
    summary = HRVSummary(status, numeric("lastNightAvg"), numeric("lastNight5MinHigh"), numeric("weeklyAvg"), baseline) if summary_data else None
    return HRVData(tuple(latest[key] for key in sorted(latest)), summary, presence)


@dataclass(frozen=True, slots=True)
class _CachedPayloadFailure:
    """One shared endpoint failure retained only for this sync attempt."""

    error: Exception


class GarminHistorySource:
    """Small serialized adapter for Garmin intraday endpoints."""

    def __init__(
        self,
        client: GarminClient,
        request_gate: GarminRequestExecutor | None = None,
    ) -> None:
        self.client = client
        self.request_gate = request_gate or GarminRequestGate()
        self._payload_cache: dict[tuple[date, str], Any] = {}

    async def _async_cached_payload(
        self, target_date: date, key: str, request: Callable[[], Awaitable[Any]]
    ) -> Any:
        cache_key = (target_date, key)
        if cache_key not in self._payload_cache:
            try:
                self._payload_cache[cache_key] = await request()
            except Exception as err:
                self._payload_cache[cache_key] = _CachedPayloadFailure(err)
                raise
        cached = self._payload_cache[cache_key]
        if isinstance(cached, _CachedPayloadFailure):
            raise cached.error
        return cached

    async def async_fetch(self, target_date: date, metric: str) -> HistoryResult:
        """Fetch one metric, retaining the historical tuple return contract."""
        result = await self.async_fetch_details(target_date, metric)
        if isinstance(result, (HRVData, SegmentedData, SourceSeries)):
            return result.readings
        if isinstance(result, SnapshotData | TrainingDeviceSnapshots):
            return ()
        return result

    async def async_fetch_details(self, target_date: date, metric: str) -> HistoryDetails:
        """Fetch a metric and retain private details needed by the archive."""

        async def request() -> Any:
            base = self.client._base_url
            if metric == "daily_summary":
                return await self._async_cached_payload(
                    target_date,
                    "daily_summary",
                    lambda: self.client._get_user_summary_raw(target_date),
                )
            if metric == "training_status":
                return await self._async_cached_payload(
                    target_date,
                    "training_status",
                    lambda: self.client.get_training_status(target_date),
                )
            if metric == "sleep_sessions":
                return await self._async_cached_payload(
                    target_date,
                    "sleep",
                    lambda: self.client._get_sleep_data_raw(target_date),
                )
            if metric == "timed_activities":
                pages: list[Any] = []
                for offset in range(0, 500, 100):
                    page = await self.client.get_activities(offset, 100)
                    if not page:
                        break
                    page_items = page.get("activities", page) if isinstance(page, dict) else page
                    if not isinstance(page_items, list) or not page_items:
                        break
                    pages.extend(page_items)
                    page_dates = []
                    for item in page_items:
                        if not isinstance(item, dict):
                            continue
                        _, parsed_start = _timestamp_from_aliases(
                            item, ("startTime", "startTimeGMT", "startTimeLocal")
                        )
                        if parsed_start is not None:
                            page_dates.append(_activity_source_calendar_date(item, parsed_start))
                    if page_dates and all(page_date < target_date for page_date in page_dates):
                        break
                    if len(page_items) < 100:
                        break
                return {"activities": pages}
            if metric == "health_events_daily":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/dailyEvents", params={"calendarDate": target_date.isoformat()})
            if metric == "health_events_body_battery":
                return await self._async_cached_payload(
                    target_date,
                    "body_battery_events",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/bodyBattery/events/{target_date.isoformat()}",
                    ),
                )
            if metric == "heart_rate":
                profile = await self.client.get_user_profile()
                return await self.client._request(
                    "GET",
                    f"{base}/wellness-service/wellness/dailyHeartRate/{profile.display_name}",
                    params={"date": target_date.isoformat()},
                )
            if metric == "stress":
                primary = await self._async_cached_payload(
                    target_date,
                    "stress",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/dailyStress/{target_date.isoformat()}",
                    ),
                )
                events = await self._async_cached_payload(
                    target_date,
                    "body_battery_events",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/bodyBattery/events/{target_date.isoformat()}",
                    ),
                )
                return primary, events
            if metric == "body_battery":
                primary = await self._async_cached_payload(
                    target_date,
                    "body_battery",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/bodyBattery/reports/daily",
                        params={
                            "startDate": target_date.isoformat(),
                            "endDate": target_date.isoformat(),
                        },
                    ),
                )
                events = await self._async_cached_payload(
                    target_date,
                    "body_battery_events",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/bodyBattery/events/{target_date.isoformat()}",
                    ),
                )
                return primary, events
            if metric == "nightly_hrv":
                return await self._async_cached_payload(
                    target_date,
                    "hrv",
                    lambda: self.client._get_hrv_data_raw(target_date),
                )
            if metric == "steps":
                profile = await self.client.get_user_profile()
                return await self.client._request("GET", f"{base}/wellness-service/wellness/dailySummaryChart/{profile.display_name}", params={"date": target_date.isoformat()})
            if metric == "floors":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/floorsChartData/daily/{target_date.isoformat()}")
            if metric in {"intensity_moderate", "intensity_vigorous"}:
                return await self._async_cached_payload(
                    target_date,
                    "intensity",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/daily/im/{target_date.isoformat()}",
                    ),
                )
            if metric in {"respiration_raw", "respiration_average"}:
                return await self._async_cached_payload(
                    target_date,
                    "respiration",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/daily/respiration/{target_date.isoformat()}",
                    ),
                )
            if metric.startswith("spo2_"):
                return await self._async_cached_payload(
                    target_date,
                    "spo2",
                    lambda: self.client._request(
                        "GET",
                        f"{base}/wellness-service/wellness/daily/spo2/{target_date.isoformat()}",
                    ),
                )
            raise ValueError(f"unsupported history metric: {metric}")

        payload = await self.request_gate.async_request(GarminRequestPriority.BACKGROUND, request)
        if metric == "body_battery":
            primary_payload, event_payload = payload
            presence = _body_battery_presence(primary_payload, target_date)
            primary = SourceSeries(
                normalize_body_battery(primary_payload, target_date)
                if presence == "present"
                else (),
                presence,
            )
            return _merge_source_series(
                primary,
                _normalize_body_battery_event_series(
                    event_payload, target_date, "body_battery"
                ),
            )
        if metric == "nightly_hrv":
            return parse_hrv_data(payload, target_date)
        if metric == "steps":
            return normalize_steps(payload, target_date)
        if metric == "floors":
            return normalize_floors(payload, target_date)
        if metric in {"intensity_moderate", "intensity_vigorous"}:
            return normalize_intensity(payload, target_date, metric.removeprefix("intensity_"))
        if metric == "respiration_raw":
            return normalize_respiration(payload, target_date)
        if metric == "respiration_average":
            return normalize_respiration(payload, target_date, True)
        if metric.startswith("spo2_"):
            return normalize_spo2(payload, target_date, metric.removeprefix("spo2_"))
        if metric == "daily_summary":
            return normalize_snapshot(payload, target_date, DAILY_SUMMARY_FIELDS)
        if metric == "training_status":
            return normalize_training_status(payload, target_date)
        if metric == "sleep_sessions":
            return parse_sleep_sessions(payload, target_date)
        if metric == "timed_activities":
            return tuple(activity for activity in normalize_activities(payload, target_date) if activity.calendar_date == target_date)
        if metric in {"health_events_daily", "health_events_body_battery"}:
            return normalize_health_events(payload, target_date)
        if metric == "heart_rate":
            if isinstance(payload, (int, float, bool)):
                raise HistorySchemaError("heart-rate payload has invalid type")
            return _normalize_source_series(
                payload,
                target_date,
                ("heartRateValues",),
                ("heartRate", "heartrate", "heartRateValue", "value"),
                ("heartRateValueDescriptors",),
            )
        primary_payload, event_payload = payload
        return _merge_source_series(
            _normalize_source_series(
                primary_payload,
                target_date,
                ("stressValuesArray",),
                ("stressLevel", "stress", "value"),
                ("stressValueDescriptorsDTOList", "stressValueDescriptorsDtoList"),
            ),
            _normalize_body_battery_event_series(
                event_payload, target_date, "stress"
            ),
        )

    async def async_fetch_daily_status_payload(
        self, target_date: date, family: str
    ) -> Any:
        """Fetch one status payload while sharing this sync's endpoint cache."""
        requests: dict[str, Callable[[], Awaitable[Any]]] = {
            "stress": lambda: self.client._get_user_summary_raw(target_date),
            "training": lambda: self.client.get_training_status(target_date),
            "sleep": lambda: self.client._get_sleep_data_raw(target_date),
            "hrv": lambda: self.client._get_hrv_data_raw(target_date),
            "fitness_age": lambda: self.client.get_fitness_age(target_date),
        }
        cache_keys = {
            "stress": "daily_summary",
            "training": "training_status",
            "sleep": "sleep",
            "hrv": "hrv",
            "fitness_age": "fitness_age",
        }
        if family not in requests:
            raise ValueError(f"unsupported daily status family: {family}")

        async def request() -> Any:
            return await self._async_cached_payload(
                target_date, cache_keys[family], requests[family]
            )

        return await self.request_gate.async_request(
            GarminRequestPriority.BACKGROUND, request
        )


async def async_fetch_intraday(
    client: GarminClient,
    target_date: date,
    metric: str,
    request_gate: GarminRequestGate | None = None,
) -> HistoryResult:
    """Fetch one intraday series through a shared request gate."""
    return await GarminHistorySource(client, request_gate).async_fetch(target_date, metric)
