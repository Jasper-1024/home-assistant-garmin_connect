"""Calendar projections for persisted structured Garmin Source Records."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from math import isfinite
from typing import Any


@dataclass(frozen=True, slots=True)
class HistoryCalendarEvent:
    """Safe Calendar result shape reserved for the future query interface."""

    start: datetime
    end: datetime
    summary: str


def _normalize_interval(
    start: datetime, end: datetime
) -> tuple[datetime, datetime] | None:
    """Reject reversed intervals and project zero-length intervals positively."""
    if end < start:
        return None
    return start, end if end > start else start + timedelta(seconds=1)


def project_activity_interval(
    record: Mapping[str, Any],
) -> tuple[datetime, datetime] | None:
    """Project an activity Source Record into a positive Calendar interval."""
    start = datetime.fromisoformat(record["start"])
    raw_end = record.get("end")
    end = datetime.fromisoformat(raw_end) if raw_end else None
    if end is None:
        duration = record.get("duration_seconds")
        if (
            isinstance(duration, int | float)
            and not isinstance(duration, bool)
            and isfinite(duration)
            and duration >= 0
        ):
            end = start + timedelta(seconds=max(duration, 1.0))
    if end is None:
        return None
    return _normalize_interval(start, end)


def project_health_interval(
    record: Mapping[str, Any],
) -> tuple[datetime, datetime] | None:
    """Project a health Source Record into a positive Calendar interval."""
    start = datetime.fromisoformat(record["start"]) if record.get("start") else None
    end = datetime.fromisoformat(record["end"]) if record.get("end") else None
    occurrence = (
        datetime.fromisoformat(record["occurrence"])
        if record.get("occurrence")
        else None
    )
    if start is None and end is None and occurrence is not None:
        start, end = occurrence, occurrence + timedelta(seconds=1)
    elif start is not None and end is None and occurrence is None:
        end = start + timedelta(seconds=1)
    elif start is None and end is not None and occurrence is None:
        start, end = end, end + timedelta(seconds=1)
    else:
        start = start or occurrence or end
        end = end or occurrence or start
    if start is None or end is None:
        return None
    return _normalize_interval(start, end)


def add_structured_calendar_event(
    events: dict[tuple[str, datetime, datetime, str], HistoryCalendarEvent],
    *,
    logical_id: str,
    record: Mapping[str, Any],
    start: datetime,
    end: datetime,
    summary: str,
    query_start_date: date,
    query_end_date: date,
) -> None:
    """Add one deduplicated structured event matching a date query."""
    source_calendar_date: date | None = None
    raw_source_calendar_date = record.get("calendar_date")
    if isinstance(raw_source_calendar_date, str):
        try:
            source_calendar_date = date.fromisoformat(raw_source_calendar_date)
        except ValueError:
            pass
    source_calendar_date_matches = (
        source_calendar_date is not None
        and query_start_date <= source_calendar_date <= query_end_date
    )
    source_instants_overlap = (
        start.date() <= query_end_date and end.date() >= query_start_date
    )
    if not source_calendar_date_matches and not source_instants_overlap:
        return
    events[(logical_id, start, end, summary)] = HistoryCalendarEvent(
        start, end, summary
    )
