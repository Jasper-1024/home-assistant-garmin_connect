"""Constants for Garmin Connect integration."""

from typing import Final

DOMAIN: Final = "garmin_connect"

# Config entry keys
CONF_TOKEN: Final = "token"
CONF_REFRESH_TOKEN: Final = "refresh_token"
CONF_CLIENT_ID: Final = "client_id"
CONF_HISTORY_ACCOUNT_KEY: Final = "history_account_key"
HISTORY_OWNER_FINGERPRINT: Final = "history_owner_fingerprint"

# The history archive intentionally has a separate identity from Garmin
# credentials.  It is persisted in the config entry and never derived from
# an account identifier.
HISTORY_STORE_VERSION: Final = 1
RECORDER_COMPATIBILITY_TARGET: Final = (
    "Home Assistant Recorder capability contract (2026.7.4+)"
)

# Options
CONF_IS_CN: Final = "is_cn"
CONF_SCAN_INTERVAL: Final = "scan_interval"
CONF_ARCHIVE_ENABLED: Final = "archive_enabled"

# Archive lifecycle metadata. Enablement is an operator option; the
# activation date and last observed state are persisted with the config entry
# so reloads and restarts preserve the archive identity and transition history.
CONF_ARCHIVE_ACTIVATION_DATE: Final = "archive_activation_date"
# Keep the persisted key stable while using a name that describes its role:
# this records the previous automatic enablement state across reloads.
CONF_ARCHIVE_PREVIOUSLY_ENABLED: Final = "archive_last_enabled"
DEFAULT_ARCHIVE_ENABLED: Final = False
DEFAULT_SCAN_INTERVAL: Final = 900  # 15 minutes
MIN_SCAN_INTERVAL: Final = 60  # 1 minute
MAX_SCAN_INTERVAL: Final = 3600  # 1 hour
