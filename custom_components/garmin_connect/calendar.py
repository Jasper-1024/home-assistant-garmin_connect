"""Read-only Calendar entities backed by structured Garmin Source Records."""

from __future__ import annotations

from datetime import UTC, datetime

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


class GarminStructuredCalendar(CalendarEntity):
    """Common read-only Calendar behavior for structured Source Records."""

    _attr_has_entity_name = True
    _calendar_name: str
    _unique_id_suffix: str

    def __init__(self, archive: GarminHistoryArchive, entry_id: str) -> None:
        self._archive = archive
        self._attr_unique_id = f"{entry_id}_{self._unique_id_suffix}"

    async def async_get_events(
        self, hass: HomeAssistant, start_date: datetime, end_date: datetime
    ) -> list[CalendarEvent]:
        """Return candidates that exactly overlap the requested Source Instants."""
        if start_date >= end_date:
            return []
        events = await self._archive.async_get_calendar_events(
            self._calendar_name,
            start_date.astimezone(UTC).date(),
            end_date.astimezone(UTC).date(),
        )
        return [
            CalendarEvent(
                summary=event.summary,
                start=event.start,
                end=event.end,
            )
            for event in events
            if event.end > start_date and event.start < end_date
        ]

    @property
    def event(self) -> CalendarEvent | None:
        """Calendar state is supplied by range queries."""
        return None


class GarminSleepCalendar(GarminStructuredCalendar):
    """Expose sleep and nap intervals without payload details."""

    _attr_name = "Sleep"
    _calendar_name = "sleep"
    _unique_id_suffix = "sleep_calendar"


class GarminHealthEventsCalendar(GarminStructuredCalendar):
    """Expose sanitized health event intervals only."""

    _attr_name = "Health events"
    _calendar_name = "health"
    _unique_id_suffix = "health_events_calendar"


class GarminActivityCalendar(GarminStructuredCalendar):
    """Expose timed activity summaries without routes or raw streams."""

    _attr_name = "Activities"
    _calendar_name = "activity"
    _unique_id_suffix = "activity_calendar"
