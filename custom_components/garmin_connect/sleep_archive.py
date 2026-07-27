"""Structured, privacy-bounded Garmin sleep session archive."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any


class SleepSchemaError(ValueError):
    """Raised when a known sleep session field changes type."""


@dataclass(frozen=True, slots=True)
class SleepStreamPoint:
    """One bounded raw sleep stream point."""

    timestamp: datetime
    raw_timestamp: Any
    value: float | None


@dataclass(frozen=True, slots=True)
class SleepStream:
    """One named high-resolution stream associated with a sleep session."""

    metric: str
    points: tuple[SleepStreamPoint, ...]


@dataclass(frozen=True, slots=True)
class SleepSession:
    """One logical sleep or nap interval, excluding numeric arrays."""

    logical_id: str
    kind: str
    start: datetime
    end: datetime
    calendar_date: date
    revision: str
    score: dict[str, Any]
    adjustments: tuple[Any, ...]
    feedback: tuple[Any, ...]
    restless_events: tuple[Any, ...]
    stages: tuple[Any, ...] = ()
    streams: tuple[SleepStream, ...] = ()


@dataclass(frozen=True, slots=True)
class SleepData:
    """Sleep sessions returned by one authenticated Garmin request."""

    sessions: tuple[SleepSession, ...]


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str):
        raise SleepSchemaError("sleep timestamp has invalid type")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as err:
        raise SleepSchemaError("sleep timestamp has invalid value") from err
    # Preserve Garmin's supplied offset for local-date, DST, and cross-midnight
    # calendar semantics; naive API values are treated as UTC.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _first(mapping: dict[str, Any], names: tuple[str, ...]) -> Any:
    return next((mapping[name] for name in names if name in mapping), None)


def _bounded_structured(value: Any, *, depth: int = 0) -> Any:
    """Copy bounded structured content while excluding numeric arrays."""
    if depth > 4:
        raise SleepSchemaError("sleep structured field is too deep")
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {
            str(key)[:64]: _bounded_structured(item, depth=depth + 1)
            for key, item in list(value.items())[:32]
        }
    if isinstance(value, list):
        if value and all(item is None or isinstance(item, (int, float, bool)) for item in value):
            raise SleepSchemaError("numeric sleep arrays are excluded")
        return [_bounded_structured(item, depth=depth + 1) for item in value[:64]]
    raise SleepSchemaError("sleep structured field has invalid type")


def _structured_list(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SleepSchemaError("sleep structured collection has invalid type")
    if value and all(item is None or isinstance(item, (int, float, bool)) for item in value):
        raise SleepSchemaError("numeric sleep arrays are excluded")
    return tuple(_bounded_structured(item) for item in value)


def _stage_list(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SleepSchemaError("sleep stages have invalid type")
    stages: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            raise SleepSchemaError("sleep stage has invalid type")
        if any(key not in item for key in ("startGMT", "activityLevel", "activityType")):
            raise SleepSchemaError("sleep stage has invalid shape")
        bounded = _bounded_structured(item)
        if not isinstance(bounded, dict):
            raise SleepSchemaError("sleep stage has invalid shape")
        stages.append(bounded)
    return tuple(stages)


_STREAM_FIELDS = {
    "heart_rate": ("sleepHeartRate", "sleepHeartRateValues", "heartRateValues"),
    "hrv": ("hrvData", "sleepHrv", "sleepHrvValues", "hrvReadings"),
    "body_battery": ("sleepBodyBattery", "sleepBodyBatteryValues", "bodyBatteryValuesArray", "bodyBattery"),
    "stress": ("sleepStress", "sleepStressValues", "stressValuesArray", "stress"),
    "respiration": ("sleepRespiration", "sleepRespirationValues", "respirationValuesArray", "respiration"),
    "spo2": ("sleepSpO2", "sleepSpo2", "sleepSpO2Values", "spO2ContinuousValues"),
    "movement": ("sleepMovement", "sleepMovementValues"),
}


def _sleep_streams(item: dict[str, Any]) -> tuple[SleepStream, ...]:
    result: list[SleepStream] = []
    for metric, aliases in _STREAM_FIELDS.items():
        key = next((alias for alias in aliases if alias in item), None)
        if key is None:
            continue
        values = item[key]
        if values is None:
            result.append(SleepStream(metric, ()))
            continue
        if not isinstance(values, list):
            raise SleepSchemaError("sleep stream has invalid type")
        points: list[SleepStreamPoint] = []
        for row in values[:4096]:
            if isinstance(row, dict):
                raw_time = next((row[name] for name in ("timestamp", "time", "startGMT", "readingTimeGMT") if name in row), None)
                raw_value = next((row[name] for name in ("value", metric, "heartRate", "hrvValue", "stressLevel", "spO2", "spo2") if name in row), None)
            elif isinstance(row, list) and len(row) >= 2:
                raw_time, raw_value = row[0], row[1]
            else:
                raise SleepSchemaError("sleep stream point has invalid type")
            parsed_time = _parse_time(raw_time)
            if raw_value is not None and (isinstance(raw_value, bool) or not isinstance(raw_value, int | float)):
                raise SleepSchemaError("sleep stream value has invalid type")
            points.append(SleepStreamPoint(parsed_time, raw_time, None if raw_value is None else float(raw_value)))
        result.append(SleepStream(metric, tuple(points)))
    return tuple(result)


def _score(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("sleepScores", payload.get("score", {}))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SleepSchemaError("sleep score has invalid type")
    result: dict[str, Any] = {}
    for key, value in list(raw.items())[:16]:
        try:
            result[key[:64]] = _bounded_structured(value)
        except SleepSchemaError:
            if isinstance(value, list) and all(
                item is None or isinstance(item, (int, float, bool)) for item in value
            ):
                continue
            raise
    return result


def parse_sleep_sessions(payload: Any, target_date: date) -> tuple[SleepSession, ...]:
    """Parse only interval/session structure from a Garmin sleep response."""
    if payload is None:
        return ()
    if not isinstance(payload, dict):
        raise SleepSchemaError("sleep payload has invalid type")
    candidates: list[tuple[str, dict[str, Any]]] = []
    for container in (payload, payload.get("sleepData"), payload.get("data")):
        if not isinstance(container, dict):
            continue
        candidates.append(("main", container))
        naps = container.get("napEvents", container.get("naps", []))
        if naps is not None:
            if not isinstance(naps, list):
                raise SleepSchemaError("sleep naps have invalid type")
            if any(not isinstance(item, dict) for item in naps):
                raise SleepSchemaError("sleep nap has invalid type")
            candidates.extend(("nap", item) for item in naps)
    sessions: dict[str, SleepSession] = {}
    for kind, item in candidates:
        start_raw = _first(item, ("sleepStartTimestampGMT", "startTimeGMT", "startTime", "start"))
        end_raw = _first(item, ("sleepEndTimestampGMT", "endTimeGMT", "endTime", "end"))
        if start_raw is None or end_raw is None:
            continue
        start, end = _parse_time(start_raw), _parse_time(end_raw)
        if end <= start:
            continue
        canonical_start = start.astimezone(UTC).isoformat()
        canonical_end = end.astimezone(UTC).isoformat()
        logical_id = hashlib.sha256(f"{kind}:{canonical_start}:{canonical_end}".encode()).hexdigest()[:24]
        revision_payload = {
            "score": item.get("sleepScores", item.get("score", {})),
            "adjustments": item.get("adjustments"),
            "feedback": item.get("feedback"),
            "restless_events": item.get("restlessEvents"),
            "stages": item.get("sleepLevels"),
            "streams": {key: item.get(key) for aliases in _STREAM_FIELDS.values() for key in aliases if key in item},
        }
        revision = hashlib.sha256(
            json.dumps(revision_payload, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()[:16]
        sessions[logical_id] = SleepSession(
            logical_id, kind, start, end, target_date, revision, _score(item),
            _structured_list(item.get("adjustments")), _structured_list(item.get("feedback")),
            _structured_list(item.get("restlessEvents")), _stage_list(item.get("sleepLevels")),
            _sleep_streams(item),
        )
    return tuple(sorted(sessions.values(), key=lambda item: (item.start, item.logical_id)))


def session_record(session: SleepSession) -> dict[str, Any]:
    """Return the bounded Store representation."""
    return {
        "logical_id": session.logical_id, "kind": session.kind,
        "start": session.start.isoformat(), "end": session.end.isoformat(),
        "calendar_date": session.calendar_date.isoformat(), "revision": session.revision,
        "score": session.score, "adjustments": list(session.adjustments),
        "feedback": list(session.feedback), "restless_events": list(session.restless_events),
        "stages": list(session.stages),
        "streams": {
            stream.metric: [
                {"timestamp": point.timestamp.isoformat(), "raw_timestamp": point.raw_timestamp, "value": point.value}
                for point in stream.points
            ]
            for stream in session.streams
        },
    }


def session_from_record(record: dict[str, Any]) -> SleepSession:
    """Restore one validated session from an annual Store partition."""
    try:
        logical_id = record["logical_id"]
        kind = record["kind"]
        start = datetime.fromisoformat(record["start"])
        end = datetime.fromisoformat(record["end"])
        calendar_date = date.fromisoformat(record["calendar_date"])
        revision = record["revision"]
        score = record["score"]
        if (
            not isinstance(logical_id, str)
            or len(logical_id) != 24
            or any(character not in "0123456789abcdef" for character in logical_id)
            or kind not in {"main", "nap"}
            or start.tzinfo is None
            or end.tzinfo is None
            or end <= start
            or not isinstance(revision, str)
            or len(revision) != 16
            or any(character not in "0123456789abcdef" for character in revision)
            or not isinstance(score, dict)
        ):
            raise SleepSchemaError("sleep Store record is invalid")
        expected_id = hashlib.sha256(
            f"{kind}:{start.astimezone(UTC).isoformat()}:{end.astimezone(UTC).isoformat()}".encode()
        ).hexdigest()[:24]
        if logical_id != expected_id:
            raise SleepSchemaError("sleep Store record is invalid")
        bounded_score = _bounded_structured(score)
        if not isinstance(bounded_score, dict):
            raise SleepSchemaError("sleep Store record is invalid")
        raw_streams = record.get("streams", {})
        if not isinstance(raw_streams, dict):
            raise SleepSchemaError("sleep Store record is invalid")
        streams: list[SleepStream] = []
        for metric, points in raw_streams.items():
            if metric not in _STREAM_FIELDS or not isinstance(points, list):
                raise SleepSchemaError("sleep Store record is invalid")
            restored_points = []
            for point in points:
                if not isinstance(point, dict):
                    raise SleepSchemaError("sleep Store record is invalid")
                timestamp = _parse_time(point["timestamp"])
                value = point["value"]
                if value is not None and (isinstance(value, bool) or not isinstance(value, int | float)):
                    raise SleepSchemaError("sleep Store record is invalid")
                restored_points.append(SleepStreamPoint(timestamp, point["raw_timestamp"], None if value is None else float(value)))
            streams.append(SleepStream(metric, tuple(restored_points)))
        return SleepSession(
            logical_id=logical_id, kind=kind, start=start, end=end,
            calendar_date=calendar_date, revision=revision, score=bounded_score,
            adjustments=_structured_list(record["adjustments"]),
            feedback=_structured_list(record["feedback"]),
            restless_events=_structured_list(record["restless_events"]),
            stages=_stage_list(record.get("stages")),
            streams=tuple(streams),
        )
    except (KeyError, TypeError, ValueError) as err:
        raise SleepSchemaError("sleep Store record is invalid") from err
