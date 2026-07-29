"""Tests for Garmin Calendar adapter range handling."""

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from custom_components.garmin_connect.calendar import (
    GarminActivityCalendar,
    GarminHealthEventsCalendar,
)
from custom_components.garmin_connect.history import HistoryCalendarEvent


@pytest.mark.asyncio
async def test_activity_calendar_queries_source_instant_dates_for_display_time_range() -> None:
    """A Display Time query finds Source Instants on the prior UTC date."""
    event = HistoryCalendarEvent(
        datetime(2026, 7, 23, 16, 30, tzinfo=UTC),
        datetime(2026, 7, 23, 16, 45, tzinfo=UTC),
        "Evening run",
    )
    archive = MagicMock()

    async def get_events(
        _calendar: str, start_date: date, end_date: date
    ) -> tuple[HistoryCalendarEvent, ...]:
        return (event,) if start_date <= event.start.date() <= end_date else ()

    archive.async_get_calendar_events = AsyncMock(side_effect=get_events)
    calendar = GarminActivityCalendar(archive, "entry-1")
    local_time_zone = ZoneInfo("Asia/Taipei")

    events = await calendar.async_get_events(
        MagicMock(),
        datetime(2026, 7, 24, 0, 15, tzinfo=local_time_zone),
        datetime(2026, 7, 24, 1, 0, tzinfo=local_time_zone),
    )

    assert [item.summary for item in events] == ["Evening run"]
    archive.async_get_calendar_events.assert_awaited_once_with(
        "activity", date(2026, 7, 23), date(2026, 7, 23)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("calendar_type", "calendar_name"),
    [
        (GarminHealthEventsCalendar, "health"),
        (GarminActivityCalendar, "activity"),
    ],
)
async def test_calendar_adapter_excludes_source_calendar_date_candidate_outside_source_instant_range(
    calendar_type, calendar_name
) -> None:
    """A Source Calendar Date candidate must overlap the Source Instant range."""
    archive = MagicMock()
    archive.async_get_calendar_events = AsyncMock(
        return_value=(
            HistoryCalendarEvent(
                datetime(2026, 7, 23, 23, 30, tzinfo=UTC),
                datetime(2026, 7, 24, 0, 0, tzinfo=UTC),
                "source-date match",
            ),
            HistoryCalendarEvent(
                datetime(2026, 7, 24, 0, 30, tzinfo=UTC),
                datetime(2026, 7, 24, 0, 45, tzinfo=UTC),
                "within range",
            ),
            HistoryCalendarEvent(
                datetime(2026, 7, 25, 0, 0, tzinfo=UTC),
                datetime(2026, 7, 25, 0, 15, tzinfo=UTC),
                "starts at upper bound",
            ),
        )
    )
    calendar = calendar_type(archive, "entry-1")
    start = datetime(2026, 7, 24, tzinfo=UTC)
    end = datetime(2026, 7, 25, tzinfo=UTC)

    events = await calendar.async_get_events(MagicMock(), start, end)

    assert [event.summary for event in events] == ["within range"]
    archive.async_get_calendar_events.assert_awaited_once_with(
        calendar_name, date(2026, 7, 24), date(2026, 7, 25)
    )
