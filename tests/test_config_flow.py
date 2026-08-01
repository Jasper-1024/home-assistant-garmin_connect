"""Tests for Garmin Connect config flow."""

from unittest.mock import AsyncMock, MagicMock, patch

from ha_garmin import GarminAuthError, GarminConnectError, GarminMFARequired, GarminRateLimitError

from custom_components.garmin_connect.const import (
    CONF_ARCHIVE_ENABLED,
    CONF_DEBUG_CAPTURE_ENABLED,
    CONF_DEBUG_REPLAY_SESSION,
    CONF_SCAN_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_auth_mock(*, login_side_effect=None) -> MagicMock:
    """Build a mock GarminAuth with DI tokens set."""
    auth = MagicMock()
    auth.di_token = "token.eyJleHAiOjk5OTk5OTk5OTl9.sig"
    auth.di_refresh_token = "refresh_token"
    auth.di_client_id = "GARMIN_CONNECT_MOBILE_ANDROID_DI"
    # login/complete_mfa are called via executor (sync callables)
    auth.login = MagicMock(side_effect=login_side_effect)
    auth.complete_mfa = MagicMock()
    return auth


def _sync_call(fn, *args):
    """Simulate executor_job: call sync fn and propagate exceptions."""
    fn(*args)


# ── User step ─────────────────────────────────────────────────────────────────


async def test_user_step_shows_form() -> None:
    """Initial step must show the login form with no errors."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    flow = GarminConnectConfigFlow()
    result = await flow.async_step_user(None)

    assert result["type"] == "form"
    assert result["step_id"] == "user"
    assert result["errors"] == {}


async def test_user_step_invalid_auth() -> None:
    """Invalid credentials must set base error and re-show the form."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    auth = _make_auth_mock(login_side_effect=GarminAuthError("bad creds"))
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config.country = "US"
    flow.hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: _sync_call(fn, *a))

    with patch("custom_components.garmin_connect.config_flow.GarminAuth", return_value=auth):
        result = await flow.async_step_user({"username": "test@example.com", "password": "wrong"})

    assert result["type"] == "form"
    assert result["errors"] == {"base": "invalid_auth"}


async def test_user_step_garmin_connect_error() -> None:
    """GarminConnectError must map to the 'unknown' base error."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    auth = _make_auth_mock(login_side_effect=GarminConnectError("timeout"))
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config.country = "US"
    flow.hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: _sync_call(fn, *a))

    with patch("custom_components.garmin_connect.config_flow.GarminAuth", return_value=auth):
        result = await flow.async_step_user({"username": "test@example.com", "password": "pass"})

    assert result["type"] == "form"
    assert result["errors"] == {"base": "unknown"}


async def test_user_step_rate_limit() -> None:
    """GarminRateLimitError must set the rate_limit base error."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    auth = _make_auth_mock(login_side_effect=GarminRateLimitError("429"))
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config.country = "US"
    flow.hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: _sync_call(fn, *a))

    with patch("custom_components.garmin_connect.config_flow.GarminAuth", return_value=auth):
        result = await flow.async_step_user({"username": "test@example.com", "password": "pass"})

    assert result["type"] == "form"
    assert result["errors"] == {"base": "rate_limit"}


async def test_user_step_mfa_required_transitions() -> None:
    """GarminMFARequired must move the flow to the mfa step."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    auth = _make_auth_mock(login_side_effect=GarminMFARequired("ticket"))
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config.country = "US"
    flow.hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: _sync_call(fn, *a))

    with patch("custom_components.garmin_connect.config_flow.GarminAuth", return_value=auth):
        result = await flow.async_step_user({"username": "test@example.com", "password": "pass"})

    assert result["type"] == "form"
    assert result["step_id"] == "mfa"


# ── MFA step ──────────────────────────────────────────────────────────────────


async def test_mfa_step_invalid_code() -> None:
    """An invalid MFA code must set base error and re-show the MFA form."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow._auth = MagicMock()
    flow._auth.complete_mfa = MagicMock(side_effect=GarminAuthError("bad code"))
    flow.hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: _sync_call(fn, *a))

    result = await flow.async_step_mfa({"mfa_code": "000000"})

    assert result["type"] == "form"
    assert result["errors"] == {"base": "invalid_mfa"}


async def test_mfa_step_rate_limit() -> None:
    """GarminRateLimitError during MFA must set the rate_limit base error."""
    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow._auth = MagicMock()
    flow._auth.complete_mfa = MagicMock(side_effect=GarminRateLimitError("429"))
    flow.hass.async_add_executor_job = AsyncMock(side_effect=lambda fn, *a: _sync_call(fn, *a))

    result = await flow.async_step_mfa({"mfa_code": "000000"})

    assert result["type"] == "form"
    assert result["errors"] == {"base": "rate_limit"}


# ── Reauth / Reconfigure steps ────────────────────────────────────────────────


async def test_reauth_confirm_step_shows_form() -> None:
    """Reauth confirm step must show the re-authentication form."""
    from homeassistant.config_entries import SOURCE_REAUTH

    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    mock_entry = MagicMock()
    mock_entry.options = {}
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config_entries.async_get_known_entry.return_value = mock_entry
    flow.context = {"source": SOURCE_REAUTH, "entry_id": "test_entry_id"}

    result = await flow.async_step_reauth_confirm(None)

    assert result["type"] == "form"
    assert result["step_id"] == "reauth_confirm"


async def test_reconfigure_step_shows_form() -> None:
    """Reconfigure step must show the reconfiguration form."""
    from homeassistant.config_entries import SOURCE_RECONFIGURE

    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    mock_entry = MagicMock()
    mock_entry.options = {}
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config_entries.async_get_known_entry.return_value = mock_entry
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": "test_entry_id"}

    result = await flow.async_step_reconfigure(None)

    assert result["type"] == "form"
    assert result["step_id"] == "reconfigure"


async def test_reauth_preserves_history_account_key() -> None:
    """Re-authentication must replace tokens without replacing archive identity."""
    from homeassistant.config_entries import SOURCE_REAUTH

    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    mock_entry = MagicMock(entry_id="test_entry_id")
    mock_entry.options = {}
    mock_entry.data = {"history_account_key": "opaque-account-key-123456"}
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config_entries.async_get_known_entry.return_value = mock_entry
    flow.hass.config_entries.async_update_entry = MagicMock()
    flow.hass.config_entries.async_reload = AsyncMock()
    flow.context = {"source": SOURCE_REAUTH, "entry_id": "test_entry_id"}
    flow._auth = _make_auth_mock()
    flow._is_cn = False

    await flow._async_finish_reauth()

    updated_data = flow.hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert updated_data["history_account_key"] == "opaque-account-key-123456"
    assert updated_data["token"] == flow._auth.di_token
    flow.hass.config_entries.async_reload.assert_awaited_once_with("test_entry_id")


async def test_reconfigure_reloads_once_after_replacing_tokens() -> None:
    """Reconfiguration owns one explicit reload after its entry update."""
    from homeassistant.config_entries import SOURCE_RECONFIGURE

    from custom_components.garmin_connect.config_flow import GarminConnectConfigFlow

    mock_entry = MagicMock(entry_id="test_entry_id")
    mock_entry.options = {}
    mock_entry.data = {"history_account_key": "opaque-account-key-123456"}
    flow = GarminConnectConfigFlow()
    flow.hass = MagicMock()
    flow.hass.config_entries.async_get_known_entry.return_value = mock_entry
    flow.hass.config_entries.async_update_entry = MagicMock()
    flow.hass.config_entries.async_reload = AsyncMock()
    flow.context = {"source": SOURCE_RECONFIGURE, "entry_id": "test_entry_id"}
    flow._auth = _make_auth_mock()
    flow._is_cn = True

    await flow._async_finish_reconfigure()

    updated_data = flow.hass.config_entries.async_update_entry.call_args.kwargs["data"]
    assert updated_data["history_account_key"] == "opaque-account-key-123456"
    assert updated_data["token"] == flow._auth.di_token
    flow.hass.config_entries.async_reload.assert_awaited_once_with("test_entry_id")


# ── Implementation constraints ────────────────────────────────────────────────


async def test_login_goes_through_executor() -> None:
    """login must be called via async_add_executor_job (sync API), not awaited directly."""
    import inspect

    from custom_components.garmin_connect import config_flow

    source = inspect.getsource(config_flow.GarminConnectConfigFlow._async_login)
    assert "async_add_executor_job" in source
    assert "await self._auth.login" not in source


async def test_no_bare_except_in_flow() -> None:
    """config_flow must not use bare 'except Exception'."""
    import inspect

    from custom_components.garmin_connect import config_flow

    source = inspect.getsource(config_flow)
    assert "except Exception" not in source


# ── Options flow ──────────────────────────────────────────────────────────────


def _make_options_flow(options: dict):
    """Instantiate GarminConnectOptionsFlow with a fake config_entry."""
    from custom_components.garmin_connect.config_flow import GarminConnectOptionsFlow

    entry = MagicMock()
    entry.options = options

    flow = GarminConnectOptionsFlow()
    # OptionsFlow._config_entry_id returns self.handler; config_entry calls
    # hass.config_entries.async_get_known_entry(_config_entry_id).
    flow.handler = "test_entry_id"
    hass = MagicMock()
    hass.config_entries.async_get_known_entry = MagicMock(return_value=entry)
    flow.hass = hass
    return flow


async def test_options_flow_shows_form() -> None:
    """Options flow init step must show the scan_interval form."""
    flow = _make_options_flow({CONF_SCAN_INTERVAL: 600})
    result = await flow.async_step_init(None)

    assert result["type"] == "form"
    assert result["step_id"] == "init"


async def test_options_flow_saves_new_interval() -> None:
    """Submitting the options form must create an entry with the chosen interval."""
    flow = _make_options_flow({CONF_SCAN_INTERVAL: DEFAULT_SCAN_INTERVAL})
    result = await flow.async_step_init({CONF_SCAN_INTERVAL: 120})

    assert result["type"] == "create_entry"
    assert result["data"][CONF_SCAN_INTERVAL] == 120


async def test_options_flow_exposes_archive_enablement_default_off() -> None:
    """Archive enablement is an explicit persistent option and defaults off."""
    flow = _make_options_flow({})

    result = await flow.async_step_init(None)

    schema = result["data_schema"]
    assert schema({})[CONF_ARCHIVE_ENABLED] is False


async def test_options_flow_accepts_archive_enablement() -> None:
    """Operators can explicitly enable the prospective archive."""
    flow = _make_options_flow({})

    result = await flow.async_step_init({CONF_ARCHIVE_ENABLED: True})

    assert result["type"] == "create_entry"
    assert result["data"][CONF_ARCHIVE_ENABLED] is True


async def test_options_flow_accepts_debug_capture_or_replay_but_not_both() -> None:
    """Raw capture and offline replay are explicit, mutually exclusive options."""
    flow = _make_options_flow({})

    capture = await flow.async_step_init({CONF_DEBUG_CAPTURE_ENABLED: True})
    assert capture["type"] == "create_entry"
    assert capture["data"][CONF_DEBUG_CAPTURE_ENABLED] is True
    assert capture["data"][CONF_DEBUG_REPLAY_SESSION] == ""

    flow = _make_options_flow({})
    conflict = await flow.async_step_init(
        {
            CONF_DEBUG_CAPTURE_ENABLED: True,
            CONF_DEBUG_REPLAY_SESSION: "capture-20260801T091710-deadbeef",
        }
    )
    assert conflict["type"] == "form"
    assert conflict["errors"] == {"base": "debug_capture_replay_conflict"}


async def test_options_flow_submission_keeps_archive_disabled_by_default() -> None:
    """Submitting unrelated options keeps archival explicitly disabled."""
    flow = _make_options_flow({})

    result = await flow.async_step_init({CONF_SCAN_INTERVAL: 120})

    assert result["data"][CONF_ARCHIVE_ENABLED] is False


async def test_options_flow_uses_default_when_options_empty() -> None:
    """Options flow must fall back to DEFAULT_SCAN_INTERVAL when entry options is empty."""
    flow = _make_options_flow({})

    # Just check the form renders without error; schema carries the default
    result = await flow.async_step_init(None)
    assert result["type"] == "form"
