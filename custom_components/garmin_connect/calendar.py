"""Read-only Calendar entities backed by structured Garmin sleep sessions."""

from __future__ import annotations

from datetime import datetime

from homeassistant.components.calendar import CalendarEntity, CalendarEvent
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .history import GarminHistoryArchive


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up per-account Garmin structured Calendars."""
    archive = getattr(entry.runtime_data, "history_archive", None)
    if isinstance(archive, GarminHistoryArchive):
        async_add_entities([
            GarminSleepCalendar(archive, entry.entry_id),
            GarminHealthEventsCalendar(archive, entry.entry_id),
            GarminActivityCalendar(archive, entry.entry_id),
        ])


class GarminSleepCalendar(CalendarEntity):
    """Expose sleep and nap intervals without payload details."""

    _attr_has_entity_name = True
    _attr_name = "Sleep"

    def __init__(self, archive: GarminHistoryArchive, entry_id: str) -> None:
        self._archive = archive
        self._attr_unique_id = f"{entry_id}_sleep_calendar"

    async def async_get_events(
        self, hass: HomeAssistant, start_date: datetime, end_date: datetime
    ) -> list[CalendarEvent]:
        """Return only bounded interval events in the requested date range."""
        events = await self._archive.async_get_calendar_events(
            "sleep", start_date.date(), end_date.date()
        )
        return [
            CalendarEvent(
                summary=event.summary,
                start=event.start,
                end=event.end,
            )
            for event in events
        ]

    @property
    def event(self) -> CalendarEvent | None:
        """Calendar state is supplied by range queries."""
        return None


class GarminHealthEventsCalendar(GarminSleepCalendar):
    """Expose sanitized health event intervals only."""

    _attr_name = "Health events"

    def __init__(self, archive: GarminHistoryArchive, entry_id: str) -> None:
        super().__init__(archive, entry_id)
        self._attr_unique_id = f"{entry_id}_health_events_calendar"

    async def async_get_events(self, hass: HomeAssistant, start_date: datetime, end_date: datetime) -> list[CalendarEvent]:
        events = await self._archive.async_get_calendar_events("health", start_date.date(), end_date.date())
        return [CalendarEvent(summary=event.summary, start=event.start, end=event.end) for event in events]


class GarminActivityCalendar(GarminSleepCalendar):
    """Expose timed activity summaries without routes or raw streams."""

    _attr_name = "Activities"

    def __init__(self, archive: GarminHistoryArchive, entry_id: str) -> None:
        super().__init__(archive, entry_id)
        self._attr_unique_id = f"{entry_id}_activity_calendar"

    async def async_get_events(self, hass: HomeAssistant, start_date: datetime, end_date: datetime) -> list[CalendarEvent]:
        events = await self._archive.async_get_calendar_events("activity", start_date.date(), end_date.date())
        return [CalendarEvent(summary=event.summary, start=event.start, end=event.end) for event in events]
