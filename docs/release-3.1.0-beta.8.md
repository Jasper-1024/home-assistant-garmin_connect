# Garmin Connect 3.1.0-beta.8

> **Historical release record.** This document describes `3.1.0-beta.8` and is
> not current installation guidance. Use the
> [documentation index](README.md) to find the current release guide.

This release fixes the Recorder startup ordering exposed by beta.7. Home
Assistant Recorder intentionally does not consume its work queue until the Core
has emitted `homeassistant_started`; the archive now waits for that event before
it submits its harmless queue-confirmation task.

The archive remains optional managed background work. Garmin current-value
sensors and Home Assistant startup therefore do not wait for Recorder, while
the history archive no longer fails with `recorder_barrier` merely because it
was initialized during Core startup.

Upgrade to `3.1.0-beta.8` and restart Home Assistant. After Core reports
`RUNNING`, History Status should leave `idle` and begin its normal initial sync.
