"""Constants for Garmin Connect integration."""

from typing import Final

DOMAIN: Final = "garmin_connect"

# Config entry keys
CONF_TOKEN: Final = "token"
CONF_REFRESH_TOKEN: Final = "refresh_token"
CONF_CLIENT_ID: Final = "client_id"
CONF_HISTORY_ACCOUNT_KEY: Final = "history_account_key"

# The history archive intentionally has a separate identity from Garmin
# credentials.  It is persisted in the config entry and never derived from
# an account identifier.
HISTORY_STORE_VERSION: Final = 1
RECORDER_COMPATIBILITY_TARGET: Final = "Home Assistant 2026.7.4 Recorder path B"

# Options
CONF_IS_CN: Final = "is_cn"
CONF_SCAN_INTERVAL: Final = "scan_interval"
DEFAULT_SCAN_INTERVAL: Final = 900  # 15 minutes
MIN_SCAN_INTERVAL: Final = 60  # 1 minute
MAX_SCAN_INTERVAL: Final = 3600  # 1 hour
