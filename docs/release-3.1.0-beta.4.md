# Garmin Connect 3.1.0-beta.4

> **Historical release record.** This document describes `3.1.0-beta.4` and is
> not current installation guidance. Use the
> [3.1.0-beta.14 release guide](release-3.1.0-beta.14.md) for the current beta.

This hotfix repairs the `500 Internal Server Error` shown when opening the
Garmin Connect Options Flow in `3.1.0-beta.3`. Home Assistant could not
serialize the replay-session regex validator for the frontend.

Upgrade to `3.1.0-beta.4`, restart Home Assistant, then open **Configure**.
The raw HTTP capture and offline replay controls work as documented in the
[beta.3 guide](release-3.1.0-beta.3.md): capture once under
`<config>/tmp/garmin_connect/<entry-id>/capture-*/`, then select that directory
name as the replay session to run without Garmin network requests.

The prospective archive remains opt-in and Home Assistant Core **2026.7.4**
remains the minimum supported version.
