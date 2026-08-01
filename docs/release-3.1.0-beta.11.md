# Garmin Connect 3.1.0-beta.11

> **Historical release record.** This document describes `3.1.0-beta.11` and is
> not current installation guidance. Use the
> [documentation index](README.md) to find the current release guide.

This release prevents optional Garmin archive initialization from delaying
Home Assistant startup. The archive must wait until Home Assistant has started
before confirming Recorder compatibility, but beta.10 registered that wait as
bootstrap work. Home Assistant therefore reported a five-minute setup timeout
before continuing normally.

Beta.11 runs archive initialization as a config-entry background task. It no
longer blocks bootstrap, remains tied to the config-entry lifecycle, and is
still cancelled during unload. Recorder compatibility checks, current-value
polling, first synchronization, the fifteen-minute archive cadence, and stored
history are unchanged.

Upgrade to `3.1.0-beta.11`, then restart Home Assistant from its control panel.
Home Assistant should finish startup without reporting a pending
`garmin_connect archive startup` task. History Status should subsequently move
through its normal synchronization states and return to `idle`.
