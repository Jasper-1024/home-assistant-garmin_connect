# Garmin Connect 3.1.0-beta.12

This release isolates all Garmin cloud traffic from Home Assistant startup and
shutdown. The integration and its entities load immediately; while Garmin is
slow or unavailable, entities may temporarily report unavailable or unknown
and recover after a later poll without reloading Home Assistant.

Initial current-value refreshes, archive first synchronization, and recurring
archive cycles now run as config-entry-owned background work. Unloading the
entry cancels that work before closing the shared request gate. Training
power-to-weight and FTP entities are created when delayed training data first
arrives, so they no longer depend on a successful setup-time request.

A recoverable first archive failure now schedules another attempt after the
normal fifteen-minute cadence and exposes that time as `next_eligible_run`.
Rate limiting retains the existing durable twenty-four-hour backoff, while a
genuine authentication failure requests reauthentication and waits for new
credentials. Sibling metrics sharing one Garmin endpoint also reuse one failure
within a synchronization attempt, avoiding immediate duplicate requests.

Upgrade to `3.1.0-beta.12`, then restart Home Assistant from its control panel.
Home Assistant should finish startup independently of Garmin network health.
If Garmin is unavailable, History Status should show `failed` and a future
`next_eligible_run`; after connectivity returns it should recover to `idle`
without another restart.
