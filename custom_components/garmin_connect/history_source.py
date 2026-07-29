"""Authenticated Garmin intraday history source and payload normalization."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from .request_gate import GarminRequestGate, GarminRequestPriority
from .sleep_archive import SleepSession, parse_sleep_sessions

if TYPE_CHECKING:
    from ha_garmin import GarminClient


class HistorySchemaError(ValueError):
    """Raised when a known Garmin series has an incompatible shape."""


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


def _activity_hashes(activity_type: str, activity_id: str, start: datetime, end: datetime | None, duration: float | None, name: str | None, training_effect: float | None, load: float | None, recovery: float | None) -> tuple[str, str]:
    logical_id = hashlib.sha256(f"{activity_id}:{activity_type}:{start.astimezone(UTC).isoformat()}".encode()).hexdigest()[:24]
    revision = hashlib.sha256(json.dumps((activity_type, activity_id, start.isoformat(), end.isoformat() if end else None, duration, name, training_effect, load, recovery), sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:16]
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
        activity_type = item.get("activityType", item.get("activityTypeKey"))
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
        start_raw = item.get("startTime", item.get("startTimeGMT", item.get("startTimeLocal")))
        activity_id = item.get("activityId", item.get("activityUUID"))
        end_raw = item.get("endTimeGMT", item.get("endTime"))
        duration_raw = item.get("durationInSeconds", item.get("duration"))
        if not isinstance(activity_type, str) or len(activity_type) > 64 or not isinstance(activity_id, (str, int)) or not isinstance(start_raw, (str, int, float)) or (end_raw is None and duration_raw is None):
            raise HistorySchemaError("activity identity has invalid type")
        start = _timestamp(start_raw)
        if start is None:
            raise HistorySchemaError("activity timestamp is invalid")
        end = _timestamp(item.get("endTimeGMT", item.get("endTime"))) if item.get("endTimeGMT", item.get("endTime")) is not None else None
        def numeric(item_data: dict[str, Any], *names: str) -> float | None:
            value = next((item_data[name] for name in names if name in item_data), None)
            if value is None:
                return None
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise HistorySchemaError("activity summary has invalid type")
            return float(value)
        duration = numeric(item, "durationInSeconds", "duration")
        activity_name = item.get("activityName") if isinstance(item.get("activityName"), str) else None
        training_effect = numeric(item, "trainingEffect", "aerobicTrainingEffect")
        load = numeric(item, "activityTrainingLoad", "trainingLoad")
        recovery = numeric(item, "recoveryTime")
        logical_id, revision = _activity_hashes(activity_type, str(activity_id), start, end, duration, activity_name, training_effect, load, recovery)
        local_date = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).date() if isinstance(start_raw, str) and "T" in start_raw and "startTimeLocal" in item and "startTime" not in item and "startTimeGMT" not in item else start.date()
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
        if not isinstance(activity_type, str) or len(activity_type) > 64 or not isinstance(activity_id, str) or (name is not None and (not isinstance(name, str) or len(name) > 128)) or start is None or (end is not None and end <= start) or any(value is not None and (isinstance(value, bool) or not isinstance(value, int | float)) for value in values):
            raise HistorySchemaError("activity record is invalid")
        logical_id, revision = _activity_hashes(activity_type, activity_id, start, end, values[0], name, values[1], values[2], values[3])
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
            key: (_timestamp(record[key]) if record.get(key) is not None else None)
            for key in ("start", "end", "occurrence")
        }
    except (KeyError, TypeError, ValueError) as err:
        raise HistorySchemaError("health event record is invalid") from err
    if any(record.get(key) is not None and values[key] is None for key in values):
        raise HistorySchemaError("health event record is invalid")
    expected_id, expected_revision = _health_identity_revision(event_type, source, category, values["start"], values["end"], values["occurrence"])
    if logical_id != expected_id or revision != expected_revision:
        raise HistorySchemaError("health event record is inconsistent")
    event = NormalizedHealthEvent(logical_id, revision, calendar_date, source, event_type, category, values["start"], values["end"], values["occurrence"])
    return event


HistorySeries = tuple[NormalizedSample, ...]
HistoryResult = HistorySeries | tuple[SleepSession, ...] | tuple[NormalizedHealthEvent, ...] | tuple[NormalizedActivity, ...]
HistoryDetails = HistorySeries | HRVData | SegmentedData | SourceSeries | SnapshotData | tuple[SleepSession, ...] | tuple[NormalizedHealthEvent, ...] | tuple[NormalizedActivity, ...]


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
        raw_events = next((payload[key] for key in ("events", "dailyEvents", "bodyBatteryEvents", "eventList") if key in payload), payload)
    if isinstance(raw_events, dict):
        raw_events = [raw_events]
    if not isinstance(raw_events, list):
        raise HistorySchemaError("health events have invalid type")
    if len(raw_events) > 512:
        raise HistorySchemaError("health event batch exceeds bounded limit")
    result: dict[str, NormalizedHealthEvent] = {}
    for event in raw_events[:512]:
        if not isinstance(event, dict):
            raise HistorySchemaError("health event has invalid type")
        source = next((event[key] for key in ("source", "eventSource") if key in event), None)
        event_type = next((event[key] for key in ("type", "eventType") if key in event), None)
        category = next((event[key] for key in ("category", "eventCategory") if key in event), None)
        if any(value is not None and (not isinstance(value, str) or len(value) > 64) for value in (source, event_type, category)):
            raise HistorySchemaError("health event identity has invalid type")
        def event_time(event_data: dict[str, Any], names: tuple[str, ...]) -> datetime | None:
            value = next((event_data[key] for key in names if key in event_data), None)
            return _timestamp(value)
        start = event_time(event, ("startTime", "startTimeGMT", "start"))
        end = event_time(event, ("endTime", "endTimeGMT", "end"))
        occurrence = event_time(event, ("occurrenceTime", "occurrenceTimeGMT", "eventTime", "timestamp", "time"))
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
        return SnapshotData(dict.fromkeys(field_aliases, ("null", None)), datetime.combine(target_date, datetime.min.time(), tzinfo=UTC), target_date.isoformat())
    if not isinstance(payload, dict):
        raise HistorySchemaError("snapshot payload is not an object")
    timestamp_key = next((key for key in ("timestamp", "startTime", "calendarDate") if key in payload), None)
    timestamp_value = payload[timestamp_key] if timestamp_key is not None else target_date.isoformat()
    timestamp = _timestamp(timestamp_value)
    if timestamp is None:
        raise HistorySchemaError("snapshot timestamp is invalid")
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
    return SnapshotData(fields, timestamp, timestamp_value, events)


DAILY_SUMMARY_FIELDS = {"abnormal_heart_rate_alerts": ("abnormalHeartRateAlertsCount",)}
TRAINING_STATUS_FIELDS = {
    "acute_load": ("acuteLoad",), "chronic_load": ("chronicLoad",),
    "load_balance": ("loadBalance",), "acwr": ("acwr", "acuteChronicWorkloadRatio"),
    "vo2_max": ("vo2Max", "vo2MaxValue"), "fitness_trend": ("fitnessTrend",),
    "recovery_time": ("recoveryTime",),
}


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        number = float(value) / (1000 if abs(float(value)) >= 100_000_000_000 else 1)
        try:
            return datetime.fromtimestamp(number, tz=UTC)
        except OverflowError, OSError, ValueError:
            return None
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)
    return None


def _descriptors(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, int]:
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if not isinstance(raw, list):
            raise HistorySchemaError(f"{key} is not a descriptor list")
        result: dict[str, int] = {}
        for item in raw:
            if not isinstance(item, dict) or not isinstance(item.get("key"), str) or not isinstance(item.get("index"), int) or isinstance(item.get("index"), bool):
                raise HistorySchemaError(f"{key} contains a malformed descriptor")
            index = item["index"]
            if index < 0 or item["key"] in result or index in result.values():
                raise HistorySchemaError(f"{key} contains an invalid descriptor index")
            result[item["key"]] = index
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
    if descriptor_present and raw_points and ("timestamp" not in positions or not any(key in positions for key in value_keys)):
        raise HistorySchemaError("descriptor list lacks required fields")
    timestamp_index = positions.get("timestamp", 0)
    value_index = next((positions[key] for key in value_keys if key in positions), 1)
    effective_date = request_date
    latest: dict[datetime, NormalizedSample] = {}
    for point in raw_points:
        if point is None:
            continue
        if not isinstance(point, (list, tuple)):
            raise HistorySchemaError(f"{values_key} point has an invalid type")
        if timestamp_index >= len(point) or value_index >= len(point):
            raise HistorySchemaError(f"{values_key} point is narrower than its descriptors")
        raw_time, raw_value = point[timestamp_index], point[value_index]
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
        raw_time = next((point[key] for key in ("timestamp", "time", "startTime", "start", "readingTime", "readingTimeGMT") if key in point), None)
        raw_value = next((point[key] for key in value_keys if key in point), None)
        if raw_time is None or raw_value is None:
            continue
        if not isinstance(raw_time, str | int | float) or isinstance(raw_time, bool):
            raise HistorySchemaError("segmented timestamp has an invalid type")
        if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
            raise HistorySchemaError("segmented value has an invalid type")
        parsed = _timestamp(raw_time)
        if parsed is None:
            raise HistorySchemaError("segmented timestamp has an invalid value")
        if exclude_negative and raw_value < 0:
            continue
        result[parsed] = NormalizedSample(parsed, target_date, raw_time, float(raw_value))
    return tuple(result[key] for key in sorted(result))


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


def _classify_source_array(payload: Any, aliases: tuple[str, ...]) -> tuple[str, Any, str | None]:
    """Classify a source array and return its values when present."""
    if payload is None:
        return "null", None, None
    if isinstance(payload, list):
        return ("empty" if not payload else "present"), payload, None
    if not isinstance(payload, dict):
        return "unsupported", None, None
    marker = _nested_value(payload, ("presence", "state", "status", "availability"))
    if marker is not None and isinstance(marker[0], str) and marker[0].lower() == "returned-empty":
        return "returned-empty", None, None
    found_values = [
        found
        for alias in aliases
        if (found := _nested_value(payload, (alias,))) is not None
    ]
    if not found_values:
        return "missing", None, None
    values, array_key = next(
        (found for found in found_values if found[0] not in (None, [])),
        found_values[0],
    )
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
    if array_key is None:
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
                {"data": values},
                values_key="data",
                descriptor_keys=descriptor_aliases,
                value_keys=value_keys,
                exclude_negative=exclude_negative,
                request_date=target_date,
            )
        return SourceSeries(readings, "present")
    descriptor_found = _nested_value(payload, descriptor_aliases)
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
    return SourceSeries(readings, "present")


def _array_presence(payload: Any, aliases: tuple[str, ...]) -> str:
    """Classify one source array without turning absence into an empty sample set."""
    return _classify_source_array(payload, aliases)[0]


def normalize_respiration(payload: Any, target_date: date, averages: bool = False) -> SourceSeries:
    aliases = ("respirationAveragesValuesArray",) if averages else ("respirationValuesArray",)
    return _normalize_source_series(payload, target_date, aliases, ("respiration", "respirationValue", "value"), ("respirationValueDescriptors", "respirationValueDescriptorsDTOList"))


def normalize_spo2(payload: Any, target_date: date, variant: str) -> SourceSeries:
    configs = {
        "single": (("spO2SingleValues", "spo2SingleValues", "singleValues"), ("spO2", "spo2", "value")),
        "continuous": (("continuousReadingDTOList", "spO2ContinuousValues", "spo2ContinuousValues", "continuousValues"), ("spO2", "spo2", "reading", "value")),
        "hourly": (("spO2HourlyAverages", "spo2HourlyAverages", "hourlyAverages"), ("spO2", "spo2", "average", "value")),
    }
    if variant not in configs:
        raise ValueError("unsupported SpO2 variant")
    return _normalize_source_series(payload, target_date, *configs[variant], ("spO2ValueDescriptors", "spO2ValueDescriptorsDTOList"))


def _totals(payload: Any, keys: tuple[str, ...]) -> dict[str, float] | None:
    result: dict[str, float] = {}
    key_set = set(keys)

    def visit(value: Any, depth: int) -> None:
        if depth > 3 or value is None:
            return
        if isinstance(value, dict):
            for name, item in value.items():
                if name in key_set:
                    if item is None:
                        continue
                    if isinstance(item, bool) or not isinstance(item, int | float):
                        raise HistorySchemaError("daily total has an invalid type")
                    result[name] = float(item)
                elif name in {"report", "summary", "data", "daily", "totals", "metrics"}:
                    visit(item, depth + 1)
        elif isinstance(value, list):
            for item in value[:32]:
                visit(item, depth + 1)

    visit(payload, 0)
    return result or None


def normalize_steps(payload: Any, target_date: date) -> SegmentedData:
    presence = _array_presence(payload, ("stepsValues", "stepsValuesArray", "chartData", "data"))
    if presence != "present":
        return SegmentedData((), _totals(payload, ("totalSteps", "steps")), presence)
    readings = _descriptor_segment(payload, target_date, ("stepsValues", "stepsValuesArray", "chartData", "data"), ("steps", "stepCount", "value"), ("stepsValueDescriptors", "stepsValueDescriptorsDTOList", "stepsValueDescriptorDTOList"))
    return SegmentedData(readings if readings is not None else _object_series(payload, target_date, ("steps", "stepCount", "value"), ("steps", "stepsValues", "stepsValuesArray", "chartData", "data")), _totals(payload, ("totalSteps", "steps")), presence)


def normalize_floors(payload: Any, target_date: date) -> SegmentedData:
    presence = _array_presence(payload, ("floorsValues", "floorsValuesArray", "chartData", "data"))
    if presence != "present":
        return SegmentedData((), _totals(payload, ("floorsAscended", "floorsDescended", "floorsAscendedInMeters", "floorsDescendedInMeters", "totalFloors")), presence)
    readings = _descriptor_segment(payload, target_date, ("floorsValues", "floorsValuesArray", "chartData", "data"), ("floors", "floorCount", "value"), ("floorsValueDescriptors", "floorsValueDescriptorsDTOList", "floorsValueDescriptorDTOList"))
    return SegmentedData(readings if readings is not None else _object_series(payload, target_date, ("floors", "floorCount", "value"), ("floors", "floorValues", "floorsValuesArray", "chartData", "data")), _totals(payload, ("floorsAscended", "floorsDescended", "floorsAscendedInMeters", "floorsDescendedInMeters", "totalFloors")), presence)


def normalize_intensity(payload: Any, target_date: date, kind: str) -> SegmentedData:
    if kind not in {"moderate", "vigorous"}:
        raise ValueError("unsupported intensity kind")
    presence = _array_presence(payload, ("intensityValues", "intensityValuesArray", "chartData", "data"))
    if presence != "present":
        return SegmentedData((), _totals(payload, ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes")), presence)
    keys = (f"{kind}IntensityMinutes", f"{kind}Minutes", "value")
    readings = _descriptor_segment(payload, target_date, ("intensityValues", "intensityValuesArray", "chartData", "data"), keys, ("intensityValueDescriptors", "intensityValueDescriptorsDTOList", "intensityValueDescriptorDTOList"))
    return SegmentedData(readings if readings is not None else _object_series(payload, target_date, keys, (f"{kind}IntensityMinutes", f"{kind}Minutes", "intensityMinutes", "chartData", "data")), _totals(payload, ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes")), presence)


def _descriptor_segment(payload: Any, target_date: date, values_keys: tuple[str, ...], value_keys: tuple[str, ...], descriptor_keys: tuple[str, ...]) -> tuple[NormalizedSample, ...] | None:
    if not isinstance(payload, dict) or not any(key in payload for key in descriptor_keys):
        return None
    present = [key for key in values_keys if key in payload]
    values_key = next((key for key in present if payload[key] not in (None, [])), present[0] if present else values_keys[0])
    return normalize_pair_series(payload, values_key=values_key, descriptor_keys=descriptor_keys, value_keys=value_keys, request_date=target_date)


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
    return _array_presence(_select_daily_report(payload, target_date), ("bodyBatteryValuesArray",))


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
        if not isinstance(reading, dict):
            raise HistorySchemaError("HRV reading is not an object")
        raw_time = next((reading[key] for key in ("readingTimeGMT", "readingTimeGmt", "readingTime") if key in reading), None)
        raw_value = next((reading[key] for key in ("hrvValue", "value") if key in reading), None)
        if raw_time is not None and not isinstance(raw_time, str | int | float):
            raise HistorySchemaError("HRV timestamp has an invalid type")
        if raw_value is not None and (isinstance(raw_value, bool) or not isinstance(raw_value, int | float)):
            raise HistorySchemaError("HRV value has an invalid type")
        if raw_time is None or raw_value is None:
            continue
        parsed = _timestamp(raw_time)
        if parsed is None:
            raise HistorySchemaError("HRV timestamp has an invalid value")
        readings.append(NormalizedSample(parsed, target_date, raw_time, float(raw_value)))
    latest = {sample.timestamp: sample for sample in readings}
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


class GarminHistorySource:
    """Small serialized adapter for Garmin intraday endpoints."""

    def __init__(self, client: GarminClient, request_gate: GarminRequestGate | None = None) -> None:
        self.client = client
        self.request_gate = request_gate or GarminRequestGate()

    async def async_fetch(self, target_date: date, metric: str) -> HistoryResult:
        """Fetch one metric, retaining the historical tuple return contract."""
        result = await self.async_fetch_details(target_date, metric)
        if isinstance(result, (HRVData, SegmentedData, SourceSeries)):
            return result.readings
        if isinstance(result, SnapshotData):
            return ()
        return result

    async def async_fetch_details(self, target_date: date, metric: str) -> HistoryDetails:
        """Fetch a metric and retain private details needed by the archive."""

        async def request() -> Any:
            base = self.client._base_url
            if metric == "daily_summary":
                return await self.client._get_user_summary_raw(target_date)
            if metric == "training_status":
                return await self.client.get_training_status(target_date)
            if metric == "sleep_sessions":
                return await self.client._get_sleep_data_raw(target_date)
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
                        raw_start = item.get("startTime", item.get("startTimeGMT", item.get("startTimeLocal")))
                        parsed_start = _timestamp(raw_start)
                        if parsed_start is not None:
                            if "startTime" not in item and "startTimeGMT" not in item and isinstance(raw_start, str):
                                page_dates.append(datetime.fromisoformat(raw_start.replace("Z", "+00:00")).date())
                            else:
                                page_dates.append(parsed_start.date())
                    if page_dates and all(page_date < target_date for page_date in page_dates):
                        break
                    if len(page_items) < 100:
                        break
                return {"activities": pages}
            if metric == "health_events_daily":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/dailyEvents", params={"calendarDate": target_date.isoformat()})
            if metric == "health_events_body_battery":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/bodyBattery/events/{target_date.isoformat()}")
            if metric == "heart_rate":
                profile = await self.client.get_user_profile()
                return await self.client._request(
                    "GET",
                    f"{base}/wellness-service/wellness/dailyHeartRate/{profile.display_name}",
                    params={"date": target_date.isoformat()},
                )
            if metric == "stress":
                return await self.client._request(
                    "GET", f"{base}/wellness-service/wellness/dailyStress/{target_date.isoformat()}"
                )
            if metric == "body_battery":
                return await self.client._request(
                    "GET", f"{base}/wellness-service/wellness/bodyBattery/reports/daily",
                    params={"date": target_date.isoformat()},
                )
            if metric == "nightly_hrv":
                return await self.client._get_hrv_data_raw(target_date)
            if metric == "steps":
                profile = await self.client.get_user_profile()
                return await self.client._request("GET", f"{base}/wellness-service/wellness/dailySummaryChart/{profile.display_name}", params={"date": target_date.isoformat()})
            if metric == "floors":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/floorsChartData/daily/{target_date.isoformat()}")
            if metric in {"intensity_moderate", "intensity_vigorous"}:
                return await self.client._request("GET", f"{base}/wellness-service/wellness/daily/im/{target_date.isoformat()}")
            if metric in {"respiration_raw", "respiration_average"}:
                return await self.client._request("GET", f"{base}/wellness-service/wellness/daily/respiration/{target_date.isoformat()}")
            if metric.startswith("spo2_"):
                return await self.client._request("GET", f"{base}/wellness-service/wellness/daily/spo2/{target_date.isoformat()}")
            raise ValueError(f"unsupported history metric: {metric}")

        payload = await self.request_gate.async_request(GarminRequestPriority.BACKGROUND, request)
        if metric == "body_battery":
            presence = _body_battery_presence(payload, target_date)
            return SourceSeries(
                normalize_body_battery(payload, target_date) if presence == "present" else (),
                presence,
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
            return normalize_snapshot(payload, target_date, TRAINING_STATUS_FIELDS)
        if metric == "sleep_sessions":
            return parse_sleep_sessions(payload, target_date)
        if metric == "timed_activities":
            return tuple(activity for activity in normalize_activities(payload, target_date) if activity.calendar_date == target_date)
        if metric in {"health_events_daily", "health_events_body_battery"}:
            return normalize_health_events(payload, target_date)
        if not isinstance(payload, (dict, list)):
            return ()
        if metric == "heart_rate":
            return _normalize_source_series(
                payload,
                target_date,
                ("heartRateValues",),
                ("heartRate", "heartRateValue", "value"),
                ("heartRateValueDescriptors",),
            )
        return _normalize_source_series(
            payload,
            target_date,
            ("stressValuesArray",),
            ("stressLevel", "stress", "value"),
            ("stressValueDescriptorsDTOList", "stressValueDescriptorsDtoList"),
            exclude_negative=True,
        )


async def async_fetch_intraday(
    client: GarminClient,
    target_date: date,
    metric: str,
    request_gate: GarminRequestGate | None = None,
) -> HistoryResult:
    """Fetch one intraday series through a shared request gate."""
    return await GarminHistorySource(client, request_gate).async_fetch(target_date, metric)
