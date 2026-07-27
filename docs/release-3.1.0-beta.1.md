# Garmin Connect 3.1.0-beta.1 release gate

Release target: Home Assistant Core **2026.7.4**. This beta freezes the
privacy-safe history families and their read-only Calendar surfaces.

## Before enabling

1. Back up the Home Assistant configuration directory, Recorder database, and
   Garmin integration Store files.
2. Plan a Home Assistant Core restart during a maintenance window.
3. After restart, run the bounded **2026-07-24** canary and verify one FIT
   archive only. Do not start the full backfill until the canary passes.
4. Enable the full backfill only after checking the checklist below.

No release test or instruction in this document contacts Garmin production or
starts a full-year backfill.

## Canary checklist

- expected sample/event/activity counts and UTC/local timestamps;
- equal values, overlaps, revisions, and restart produce no duplicates;
- sleep, health-event, and activity Calendars contain only bounded summaries;
- restart preserves Store partitions, checkpoints, and FIT integrity;
- backfill pacing is at most one date batch and one FIT per hour;
- FIT files are valid and mode `0600`;
- logs, diagnostics, status, actions, and Calendar output contain no raw
  measurements, arrays, credentials, account identity, address, or GPS;
- Recorder/Store growth is bounded and expected.

## Rollback

Disable the history archive or restore the previous integration fork. Never
delete Home Assistant Statistics, Store files, Recorder data, or FIT files as
a rollback step.

## Optional private replay

Private replay is opt-in only. A local 0600 capture may be supplied through an
environment variable to the inspector tests; the path must be gitignored,
the test skips when absent, and scalar values are never printed or committed.
