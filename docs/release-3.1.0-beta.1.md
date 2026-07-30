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

The executable release-gate command is the following single pytest invocation
(the matrix is also declared in `tests/test_release_gates.py`):

```text
pytest -q \
  tests/test_release_gates.py \
  tests/test_history.py::test_reconciliation_failure_stays_open_at_window_boundary \
  tests/test_history.py::test_settled_date_stays_terminal_after_confirmation_restart_and_next_scheduler \
  tests/test_history.py::test_manual_repair_accepts_one_and_31_days_but_rejects_32_before_requests \
  tests/test_history.py::test_manual_repair_reopens_settled_date_while_archive_is_disabled \
  tests/test_fit_queue.py::test_fit_queue_paces_downloads_and_recovers_pending_work_after_restart \
  tests/test_fit_queue.py::test_valid_local_fit_completes_queue_without_download \
  tests/test_fit_queue.py::test_fit_queue_isolated_by_account_and_background_gate \
  tests/test_fit_archive.py::test_fit_archive_validates_privately_and_atomically \
  tests/test_history.py::test_archive_rate_limit_enters_durable_backoff_without_cadence \
  tests/test_history.py::test_archive_rate_limit_backoff_survives_restart_and_expires_once \
  tests/test_history.py::test_first_sync_rate_limit_retries_after_expiry_without_restart \
  tests/test_history.py::test_archive_auth_classification_requires_genuine_account_failure \
  tests/test_history.py::test_archive_cycle_failure_does_not_break_foreground_request \
  tests/test_history.py::test_malformed_structured_record_fails_archive_without_blocking_foreground_work \
  tests/test_history.py::test_start_keeps_historical_backfill_dormant \
  tests/test_init.py::test_real_config_entry_lifecycle_keeps_backfill_dormant_and_surfaces_visible \
  tests/test_release_gates.py::test_release_privacy_snapshots_cover_diagnostics_status_action_and_calendars \
  tests/test_history.py::test_status_sensor_exposes_only_privacy_safe_placeholders \
  tests/test_history_recorder.py::test_release_gate_scratch_recorder_restart_revision_and_no_state_changed \
  tests/test_fit_archive.py::test_optional_private_captured_fit_replay
```

The matrix explicitly covers seven-day reconciliation, Manual Repair, FIT
pacing/restart/valid-file skip/account isolation, archive-only 429 backoff
including restart and expiry, genuine reauthentication and failure isolation,
dormant Historical Backfill, and exact status/privacy output. The fixture,
Recorder, and optional private replay gates remain part of the same command;
the replay skips without a local 0600 capture.

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
