"""Tests for coordinator request-gate integration."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from ha_garmin.exceptions import GarminAuthError
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import UpdateFailed

from custom_components.garmin_connect.const import (
    CONF_CLIENT_ID,
    CONF_REFRESH_TOKEN,
    CONF_SCAN_INTERVAL,
    CONF_TOKEN,
    DEFAULT_SCAN_INTERVAL,
)
from custom_components.garmin_connect.coordinator import (
    ActivityCoordinator,
    BloodPressureCoordinator,
    BodyCoordinator,
    CoreCoordinator,
    GarminConnectCoordinators,
    GearCoordinator,
    GoalsCoordinator,
    MenstrualCoordinator,
    NutritionCoordinator,
    TrainingCoordinator,
)
from custom_components.garmin_connect.request_gate import (
    GarminRequestGate,
    GarminRequestPriority,
)

_COORDINATORS = (
    CoreCoordinator,
    ActivityCoordinator,
    TrainingCoordinator,
    BodyCoordinator,
    GoalsCoordinator,
    GearCoordinator,
    BloodPressureCoordinator,
    MenstrualCoordinator,
    NutritionCoordinator,
)


def _inputs() -> tuple[MagicMock, MagicMock, MagicMock, MagicMock]:
    """Build isolated coordinator dependencies."""
    hass = MagicMock()
    hass.config.time_zone = "UTC"
    entry = MagicMock()
    entry.data = {
        CONF_TOKEN: "token",
        CONF_REFRESH_TOKEN: "refresh-token",
        CONF_CLIENT_ID: "client-id",
    }
    entry.options = {}
    client = MagicMock()
    auth = MagicMock()
    auth.di_token = entry.data[CONF_TOKEN]
    auth.di_refresh_token = entry.data[CONF_REFRESH_TOKEN]
    auth.di_client_id = entry.data[CONF_CLIENT_ID]
    return hass, entry, client, auth


def test_all_current_coordinators_can_share_one_account_gate() -> None:
    """Every coordinator constructed for one entry uses the same gate."""
    hass, entry, client, auth = _inputs()
    gate = GarminRequestGate()

    coordinators = [constructor(hass, entry, client, auth, gate) for constructor in _COORDINATORS]

    assert all(coordinator.request_gate is gate for coordinator in coordinators)


def test_default_and_explicit_scan_intervals() -> None:
    """The new default applies without overriding an explicit valid option."""
    hass, entry, client, auth = _inputs()

    default_coordinator = CoreCoordinator(hass, entry, client, auth, GarminRequestGate())
    entry.options = {CONF_SCAN_INTERVAL: 600}
    explicit_coordinator = CoreCoordinator(hass, entry, client, auth, GarminRequestGate())

    assert DEFAULT_SCAN_INTERVAL == 900
    assert default_coordinator.update_interval.total_seconds() == DEFAULT_SCAN_INTERVAL
    assert explicit_coordinator.update_interval.total_seconds() == 600


async def test_coordinators_serialize_current_fetches_through_shared_gate() -> None:
    """Two coordinators for one entry never run their Garmin requests together."""
    hass, entry, client, auth = _inputs()
    gate = GarminRequestGate()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()

    async def first_fetch() -> dict:
        first_started.set()
        await release_first.wait()
        return {"source": "core"}

    async def second_fetch() -> dict:
        second_started.set()
        return {"source": "activity"}

    client.fetch_core_data = first_fetch
    client.fetch_activity_data = second_fetch
    core = CoreCoordinator(hass, entry, client, auth, gate)
    activity = ActivityCoordinator(hass, entry, client, auth, gate)

    first_task = asyncio.create_task(core._async_update_data())
    await first_started.wait()
    second_task = asyncio.create_task(activity._async_update_data())
    await asyncio.sleep(0)
    assert not second_started.is_set()

    release_first.set()
    assert await first_task == {"source": "core"}
    assert await second_task == {"source": "activity"}

    await gate.async_close()


async def test_token_update_stays_inside_current_request_slot() -> None:
    """A following coordinator cannot fetch before token persistence finishes."""
    hass, entry, client, auth = _inputs()
    gate = GarminRequestGate()
    token_update_started = asyncio.Event()
    release_token_update = asyncio.Event()
    second_started = asyncio.Event()

    async def update_tokens() -> None:
        token_update_started.set()
        await release_token_update.wait()

    async def second_fetch() -> dict:
        second_started.set()
        return {"source": "activity"}

    client.fetch_core_data = AsyncMock(return_value={"source": "core"})
    client.fetch_activity_data = second_fetch
    core = CoreCoordinator(hass, entry, client, auth, gate)
    activity = ActivityCoordinator(hass, entry, client, auth, gate)
    core._update_tokens_if_changed = update_tokens

    first_task = asyncio.create_task(core._async_update_data())
    await token_update_started.wait()
    second_task = asyncio.create_task(activity._async_update_data())
    await asyncio.sleep(0)
    assert not second_started.is_set()

    release_token_update.set()
    assert await first_task == {"source": "core"}
    assert await second_task == {"source": "activity"}
    await gate.async_close()


async def test_runtime_request_persists_service_token_refresh_inside_gate() -> None:
    """Direct service work uses the coordinator token persistence lifecycle."""
    hass, entry, client, auth = _inputs()
    gate = GarminRequestGate()
    core = CoreCoordinator(hass, entry, client, auth, gate)
    runtime = GarminConnectCoordinators(
        core=core,
        activity=core,
        training=core,
        body=core,
        goals=core,
        gear=core,
        blood_pressure=core,
        menstrual=core,
        nutrition=core,
        request_gate=gate,
    )

    async def service_request() -> str:
        auth.di_token = "refreshed-token"
        auth.di_refresh_token = "refreshed-refresh-token"
        return "written"

    assert (
        await runtime.async_request(GarminRequestPriority.FOREGROUND, service_request)
        == "written"
    )
    hass.config_entries.async_update_entry.assert_called_once_with(
        entry,
        data={
            CONF_TOKEN: "refreshed-token",
            CONF_REFRESH_TOKEN: "refreshed-refresh-token",
            CONF_CLIENT_ID: "client-id",
        },
    )
    await gate.async_close()


async def test_auth_errors_keep_config_entry_auth_semantics() -> None:
    """A Garmin auth failure remains a Home Assistant auth failure."""
    hass, entry, client, auth = _inputs()
    client.fetch_core_data = AsyncMock(side_effect=GarminAuthError("expired"))
    coordinator = CoreCoordinator(hass, entry, client, auth, GarminRequestGate())

    with pytest.raises(ConfigEntryAuthFailed):
        await coordinator._async_update_data()


async def test_fetch_errors_keep_update_failed_semantics() -> None:
    """A non-auth fetch failure remains an UpdateFailed."""
    hass, entry, client, auth = _inputs()
    client.fetch_core_data = AsyncMock(side_effect=RuntimeError("network down"))
    coordinator = CoreCoordinator(hass, entry, client, auth, GarminRequestGate())

    with pytest.raises(UpdateFailed, match="network down"):
        await coordinator._async_update_data()


async def test_training_coordinator_only_calls_supported_endpoint_families() -> None:
    """Known-empty readiness, score, recovery, and lactate calls stay dormant."""
    hass, entry, client, auth = _inputs()
    client.get_training_status = AsyncMock(
        return_value={
            "mostRecentTrainingStatus": {
                "latestTrainingStatusData": {
                    "watch": {"calendarDate": "2026-08-01", "trainingStatus": 7}
                }
            },
            "mostRecentVO2Max": {"generic": {"vo2MaxValue": 48}},
        }
    )
    client._get_hrv_data_raw = AsyncMock(
        return_value={
            "hrvSummary": {
                "status": "BALANCED",
                "weeklyAvg": 43,
                "baseline": {"lowUpper": 31},
            }
        }
    )
    client.get_power_to_weight = AsyncMock(
        return_value=[
            {
                "sport": "cycling",
                "powerToWeight": 2.94,
                "functionalThresholdPower": 208,
            }
        ]
    )
    client.fetch_training_data = AsyncMock()
    client.get_training_readiness = AsyncMock()
    client.get_morning_training_readiness = AsyncMock()
    client.get_lactate_threshold = AsyncMock()
    client.get_endurance_score = AsyncMock()
    client.get_hill_score = AsyncMock()
    coordinator = TrainingCoordinator(
        hass, entry, client, auth, GarminRequestGate()
    )

    result = await coordinator._async_update_data()

    assert result["trainingStatusPhrase"] == "Productive"
    assert result["hrvWeeklyAvg"] == 43
    assert result["vo2MaxValue"] == 48
    assert result["powerToWeight"][0]["functionalThresholdPower"] == 208
    client.get_training_status.assert_awaited_once()
    client._get_hrv_data_raw.assert_awaited_once()
    client.get_power_to_weight.assert_awaited_once()
    for unsupported in (
        client.fetch_training_data,
        client.get_training_readiness,
        client.get_morning_training_readiness,
        client.get_lactate_threshold,
        client.get_endurance_score,
        client.get_hill_score,
    ):
        unsupported.assert_not_awaited()
    await coordinator.request_gate.async_close()
