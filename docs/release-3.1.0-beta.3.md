# Garmin Connect 3.1.0-beta.3

> **Historical release record.** This document describes `3.1.0-beta.3` and is
> not current installation guidance. Use the
> [3.1.0-beta.14 release guide](release-3.1.0-beta.14.md) for the current beta.

This beta adds an operator-controlled full Garmin HTTP capture and offline
replay boundary for local diagnosis. It is intended to investigate archive
failures without repeatedly calling Garmin Connect.

## Upgrade and diagnose

1. Upgrade to `3.1.0-beta.3` and restart Home Assistant.
2. In Garmin Connect integration options, enable **Capture complete Garmin HTTP
   requests and responses**. The integration reloads automatically.
3. Reproduce the problem once. Capture sessions are stored in
   `<config>/tmp/garmin_connect/<entry-id>/capture-*/` and are never deleted
   automatically.
4. Disable capture, enter the selected `capture-*` directory name in **Replay
   captured HTTP session**, and allow the integration to reload.
5. Reproduce or test repeatedly. Matching requests now use the local capture;
   an unmatched request fails closed and never contacts Garmin.

The captures deliberately include full health payloads and are only for the
operator's local debugging environment. They do not include authentication
headers, cookies, or DI tokens. Enable
`custom_components.garmin_connect: debug` for request sequence and traceback
diagnostics.

Minimum Home Assistant Core version remains **2026.7.4**. The prospective
archive remains opt-in and does not initiate historical backfill during normal
setup.
