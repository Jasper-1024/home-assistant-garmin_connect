"""Status sensor for the Garmin history archive."""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo

from .const import DOMAIN
from .history import GarminHistoryArchive
from .sensor import HISTORY_STATUS_SENSOR_DESCRIPTIONS


class GarminHistoryStatusSensor(SensorEntity):
    """Expose archive lifecycle without exposing account or health data."""

    _attr_has_entity_name = True
    _attr_icon = "mdi:archive-clock"
    _attr_should_poll = False
    entity_description = HISTORY_STATUS_SENSOR_DESCRIPTIONS[0]

    def __init__(self, archive: GarminHistoryArchive, entry_id: str) -> None:
        """Initialize the status sensor."""
        self._archive = archive
        self._attr_unique_id = f"{entry_id}_{self.entity_description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name="Garmin Connect",
            manufacturer="Garmin",
            entry_type=DeviceEntryType.SERVICE,
        )

    @property
    def native_value(self) -> str:
        """Return the archive state."""
        return self._archive.status.state.value

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return only bounded, privacy-safe status attributes."""
        return self._archive.status.as_attributes()
