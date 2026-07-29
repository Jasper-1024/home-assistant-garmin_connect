"""Tests for Garmin Calendar adapter range handling."""

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.garmin_connect.calendar import (
    GarminActivityCalendar,
    GarminHealthEventsCalendar,
)
from custom_components.garmin_connect.history import HistoryCalendarEvent


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("calendar_type", "calendar_name"),
    [
        (GarminHealthEventsCalendar, "health"),
        (GarminActivityCalendar, "activity"),
    ],
)
async def test_calendar_adapter_excludes_source_date_match_outside_datetime_range(
    calendar_type, calendar_name
) -> None:
    """Source-date lookup cannot leak an event outside Home Assistant's range."""
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
