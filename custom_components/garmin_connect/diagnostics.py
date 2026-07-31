"""Diagnostics support for Garmin Connect."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .coordinator import BaseGarminCoordinator, GarminConnectConfigEntry
from .history import GarminHistoryArchive

TO_REDACT = {
    "history_account_key",
    "token",
    "refresh_token",
    "access_token",
    "accessToken",
    "auth_token",
    "authToken",
    "client_id",
    "displayName",
    "fullName",
    "username",
    "userName",
    "user_name",
    "emailAddress",
    "email_address",
    "email",
    "profileImageUrlMedium",
    "profileImageUrlSmall",
    "profileImageUrlLarge",
}

_COORDINATOR_FIELDS = (
    "core",
    "activity",
    "training",
    "body",
    "goals",
    "gear",
    "blood_pressure",
    "menstrual",
    "nutrition",
)


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: GarminConnectConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator_info: dict[str, Any] = {}
    runtime_data = getattr(entry, "runtime_data", None)
    for field_name in _COORDINATOR_FIELDS:
        coordinator = getattr(runtime_data, field_name, None)
        if not isinstance(coordinator, BaseGarminCoordinator):
            continue
        data = coordinator.data or {}
        data_keys = list(data.keys())
        coordinator_info[field_name] = {
            "last_update_success": coordinator.last_update_success,
            "update_interval_seconds": (
                coordinator.update_interval.total_seconds() if coordinator.update_interval else None
            ),
            "data_keys_count": len(data_keys),
            "data_keys_sample": data_keys[:50] if len(data_keys) > 50 else data_keys,
        }

    history_archive = getattr(runtime_data, "history_archive", None)
    if isinstance(history_archive, GarminHistoryArchive):
        coordinator_info["history_archive"] = history_archive.status.as_attributes()

    return {
        "entry_data": async_redact_data(dict(entry.data), TO_REDACT),
        "coordinators": coordinator_info,
    }
