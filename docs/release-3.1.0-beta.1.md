# Garmin Connect 3.1.0-beta.1 release gate

Release target: Home Assistant Core **2026.7.4**. This beta freezes the
privacy-safe history families and their read-only Calendar surfaces.

The product is a prospective archive. It does not start Historical Backfill
during normal setup and has no full-year enablement switch.

## Controlled upgrade and enablement

1. Create a Home Assistant backup that includes the configuration directory,
   Recorder database, and Garmin integration Store files.
2. Upgrade the integration to `3.1.0-beta.1` and restart Home Assistant during
   a maintenance window.
3. Before enabling the archive, verify that existing current-value Garmin
   entities refresh normally and that retained Home Assistant history remains
   queryable.
4. In the Garmin config-entry options, explicitly enable **Prospective
   Archive**. The first run is bounded to the current Home Assistant local
   date; it does not fetch a year of history.
5. Observe the privacy-safe Archive Status (`syncing`, then `idle` or
   `failed`) and validate the current-day result through the existing Archive
   Query Surfaces. A fifteen-minute freshness target, seven-day reconciliation
   window, and one-FIT-per-hour pacing are nominal policies, not completeness
   or real-time guarantees.
6. Restart Home Assistant once more. Verify that Archive Enablement, retained
   query results, account isolation, FIT files, and the status contract persist.

Archive Disablement is the reversible stop mechanism: disable the option to
cancel future automatic work. Disablement, reload, upgrade, and rollback do
not delete Recorder statistics, Store records, Calendar records, or FIT files.
This beta provides no archive deletion or automatic expiry action. Long-term
storage growth on the order of 1–2 GB is an accepted planning estimate.

Source Instants remain absolute aware timestamps; Source Calendar Date remains
separate provenance metadata; Home Assistant chooses Display Time from its
configured timezone. The archive does not rewrite source timestamps to the
operator's display timezone.

No release instruction in this document contacts Garmin production outside the
operator's explicitly enabled prospective archive, and none starts Historical
Backfill.

The executable gates are `tests/test_release_gates.py`,
`tests/test_history_recorder.py::test_release_gate_scratch_recorder_restart_revision_and_no_state_changed`,
and `tests/test_fit_archive.py::test_optional_private_captured_fit_replay`.
They assert fixture state coverage and provenance, privacy-safe diagnostics/
status/action/Calendar shapes, raw timestamp/count/revision behavior, and
optional private replay integrity without asserting fabricated live counts.

## Current-day validation checklist

- expected sample/event/activity counts and UTC/local timestamps from the
  current-day validation run;
- equal values, overlaps, revisions, and restart produce no duplicates;
- sleep, health-event, and activity Calendars contain only bounded summaries;
- restart preserves Store partitions, checkpoints, and FIT integrity;
- prospective cadence is nominally fifteen minutes, reconciliation is bounded
  to seven days, and FIT pacing is at most one file per hour;
- FIT files are valid and mode `0600`;
- logs, diagnostics, status, actions, and Calendar output contain no raw
  measurements, arrays, credentials, account identity, address, or GPS;
- Recorder/Store growth is bounded and expected.

The validation contract is structural: timestamps remain aware, counts are
consistent, equal values and revisions converge, and repeated/restarted work
does not create duplicate points or `state_changed` replay. Live counts must
be recorded from the current-day run, not copied from this document. Missing
Garmin data remains missing; the archive does not synthesize values or claim
forensic completeness.

## Rollback

Disable the Prospective Archive or restore the previous integration fork.
Never delete Home Assistant Statistics, Store files, Recorder data, or FIT
files as a rollback step.

## Optional private replay

Private replay is opt-in only. A local 0600 capture may be supplied through an
environment variable to the inspector tests; the path must be gitignored,
the test skips when absent, and scalar values are never printed or committed.
