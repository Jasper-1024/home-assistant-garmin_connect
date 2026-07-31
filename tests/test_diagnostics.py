"""Tests for Garmin Connect diagnostics."""

from datetime import timedelta
from unittest.mock import MagicMock

from custom_components.garmin_connect.coordinator import (
    BaseGarminCoordinator,
    CoreCoordinator,
    GarminConnectCoordinators,
)
from custom_components.garmin_connect.diagnostics import (
    TO_REDACT,
    async_get_config_entry_diagnostics,
)
from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    HistoryStatus,
)
from custom_components.garmin_connect.request_gate import GarminRequestGate


def _runtime_data(
    core: BaseGarminCoordinator | None = None,
    history_archive: GarminHistoryArchive | None = None,
) -> GarminConnectCoordinators:
    """Build the production runtime dataclass with its heterogeneous fields."""
    return GarminConnectCoordinators(
        core=core,
        activity=None,
        training=None,
        body=None,
        goals=None,
        gear=None,
        blood_pressure=None,
        menstrual=None,
        nutrition=None,
        request_gate=GarminRequestGate(),
        history_archive=history_archive,
    )


def _core(data: dict[str, object], *, update_interval: timedelta | None) -> CoreCoordinator:
    """Build a concrete coordinator with diagnostics-relevant state."""
    core = object.__new__(CoreCoordinator)
    core.data = data
    core.last_update_success = True
    core.update_interval = update_interval
    return core


def _archive(status: HistoryStatus) -> GarminHistoryArchive:
    """Build an archive instance exposing a real public status snapshot."""
    archive = object.__new__(GarminHistoryArchive)
    archive._status = status
    return archive


async def test_diagnostics_handles_real_runtime_data_and_redacts_nested_sensitive_data() -> None:
    """Diagnostics safely handles the real heterogeneous runtime dataclass."""
    core = _core(
        {"totalSteps": 10000, "restingHeartRate": 60},
        update_interval=timedelta(seconds=300),
    )

    entry = MagicMock()
    entry.data = {
        "token": "secret_token",
        "refresh_token": "secret_refresh",
        "client_id": "secret_client_id",
        "history_account_key": "secret_account_key",
        "username": "legacy@example.com",
        "userName": "profile@example.com",
        "user_name": "older@example.com",
        "email": "email@example.com",
        "emailAddress": "older-email@example.com",
        "access_token": "secret_access_token",
        "nested": {
            "token": "nested_token",
            "refresh_token": "nested_refresh",
            "client_id": "nested_client_id",
            "accessToken": "nested_access_token",
            "history_account_key": "nested_account_key",
            "username": "nested_legacy@example.com",
            "userName": "nested_profile@example.com",
            "user_name": "nested_older@example.com",
            "email": "nested_email@example.com",
            "email_address": "nested_older_email@example.com",
        },
    }
    entry.runtime_data = _runtime_data(core)

    result = await async_get_config_entry_diagnostics(MagicMock(), entry)

    assert result["entry_data"] == {
        "token": "**REDACTED**",
        "refresh_token": "**REDACTED**",
        "client_id": "**REDACTED**",
        "history_account_key": "**REDACTED**",
        "username": "**REDACTED**",
        "userName": "**REDACTED**",
        "user_name": "**REDACTED**",
        "email": "**REDACTED**",
        "emailAddress": "**REDACTED**",
        "access_token": "**REDACTED**",
        "nested": {
            "token": "**REDACTED**",
            "refresh_token": "**REDACTED**",
            "client_id": "**REDACTED**",
            "accessToken": "**REDACTED**",
            "history_account_key": "**REDACTED**",
            "username": "**REDACTED**",
            "userName": "**REDACTED**",
            "user_name": "**REDACTED**",
            "email": "**REDACTED**",
            "email_address": "**REDACTED**",
        },
    }
    assert result["coordinators"] == {
        "core": {
            "last_update_success": True,
            "update_interval_seconds": 300,
            "data_keys_count": 2,
            "data_keys_sample": ["totalSteps", "restingHeartRate"],
        }
    }


async def test_diagnostics_includes_archive_public_status_only() -> None:
    """Diagnostics preserves the archive's public status contract."""
    entry = MagicMock()
    entry.data = {}
    entry.runtime_data = _runtime_data(
        history_archive=_archive(
            HistoryStatus(
                HistoryArchiveState.BACKOFF,
                activation_date="2026-01-01",
                last_success="2026-01-02T00:00:00+00:00",
                safe_error_class="rate_limited",
            )
        )
    )

    result = await async_get_config_entry_diagnostics(MagicMock(), entry)

    assert result["coordinators"] == {
        "history_archive": {
            "archive_state": "backoff",
            "activation_date": "2026-01-01",
            "last_success": "2026-01-02T00:00:00+00:00",
            "safe_error_class": "rate_limited",
        }
    }


async def test_diagnostics_handles_none_update_interval() -> None:
    """Diagnostics handles a coordinator without an update interval."""
    core = _core({}, update_interval=None)
    core.last_update_success = False

    entry = MagicMock()
    entry.data = {}
    entry.runtime_data = _runtime_data(core)

    result = await async_get_config_entry_diagnostics(MagicMock(), entry)

    assert result["coordinators"]["core"]["update_interval_seconds"] is None


async def test_diagnostics_handles_unloaded_entry_without_runtime_data() -> None:
    """Diagnostics for an unloaded entry returns only redacted persisted data."""
    entry = MagicMock()
    entry.data = {"token": "secret_token", "name": "Garmin"}
    entry.runtime_data = _runtime_data()
    del entry.runtime_data

    result = await async_get_config_entry_diagnostics(MagicMock(), entry)

    assert result == {
        "entry_data": {"token": "**REDACTED**", "name": "Garmin"},
        "coordinators": {},
    }


def test_to_redact_contains_expected_keys() -> None:
    """Test that the redaction set covers known sensitive config fields."""
    expected = {
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
    assert TO_REDACT == expected
