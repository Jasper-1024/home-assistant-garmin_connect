"""Structured, privacy-bounded Garmin sleep session archive."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any


class SleepSchemaError(ValueError):
    """Raised when a known sleep session field changes type."""


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
    adjustments: tuple[str, ...]
    feedback: tuple[str, ...]
    restless_events: tuple[str, ...]


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


def _text_list(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SleepSchemaError("sleep text collection has invalid type")
    return tuple(item[:120] for item in value if isinstance(item, str))


def _score(payload: dict[str, Any]) -> dict[str, Any]:
    raw = payload.get("sleepScores", payload.get("score", {}))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SleepSchemaError("sleep score has invalid type")
    result: dict[str, Any] = {}
    for key, value in list(raw.items())[:16]:
        if isinstance(value, (str, int, float, bool)) or value is None:
            result[key[:64]] = value
        elif isinstance(value, dict):
            result[key[:64]] = {
                str(child_key)[:64]: child_value
                for child_key, child_value in list(value.items())[:8]
                if isinstance(child_value, (str, int, float, bool)) or child_value is None
            }
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
            candidates.extend(("nap", item) for item in naps if isinstance(item, dict))
    sessions: dict[str, SleepSession] = {}
    for kind, item in candidates:
        start_raw = _first(item, ("sleepStartTimestampGMT", "startTimeGMT", "startTime", "start"))
        end_raw = _first(item, ("sleepEndTimestampGMT", "endTimeGMT", "endTime", "end"))
        if start_raw is None or end_raw is None:
            continue
        start, end = _parse_time(start_raw), _parse_time(end_raw)
        if end <= start:
            continue
        logical_id = hashlib.sha256(f"{kind}:{start.isoformat()}:{end.isoformat()}".encode()).hexdigest()[:24]
        revision = hashlib.sha256(repr((item.get("sleepScores", item.get("score", {})), item.get("adjustments"))).encode()).hexdigest()[:16]
        sessions[logical_id] = SleepSession(
            logical_id, kind, start, end, target_date, revision, _score(item),
            _text_list(item.get("adjustments")), _text_list(item.get("feedback")),
            _text_list(item.get("restlessEvents")),
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
    }


def session_from_record(record: dict[str, Any]) -> SleepSession:
    """Restore one validated session from an annual Store partition."""
    try:
        return SleepSession(
            logical_id=record["logical_id"],
            kind=record["kind"],
            start=datetime.fromisoformat(record["start"]),
            end=datetime.fromisoformat(record["end"]),
            calendar_date=date.fromisoformat(record["calendar_date"]),
            revision=record["revision"],
            score=record["score"],
            adjustments=tuple(record["adjustments"]),
            feedback=tuple(record["feedback"]),
            restless_events=tuple(record["restless_events"]),
        )
    except (KeyError, TypeError, ValueError) as err:
        raise SleepSchemaError("sleep Store record is invalid") from err
