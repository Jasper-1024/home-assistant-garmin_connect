# Garmin Connect 3.1.0-beta.2 release gate

Minimum Home Assistant Core version: **2026.7.4**. Newer patch and minor
releases are accepted only after the Recorder capability contract passes; an
incompatible Recorder API fails closed before history writes. This beta freezes
the privacy-safe history families and their read-only Calendar surfaces.

> This is the current candidate guide. 3.1.0-beta.1 is a historical release
> record whose annotated tag resolves to 5e25218; it must never be used as the
> tag or Release version for this beta.2 candidate.

## Package source and feedback

Install and report feedback for this beta at the maintained fork:
[Jasper-1024/home-assistant-garmin_connect](https://github.com/Jasper-1024/home-assistant-garmin_connect).
For HACS, add that URL as a custom repository in the **Integration** category;
the [HACS redirect](https://my.home-assistant.io/redirect/hacs_repository/?owner=Jasper-1024&repository=home-assistant-garmin_connect&category=integration)
opens the same repository. GitHub Issues and Discussions are disabled on this
fork. To submit public feedback, fork the repository, create a branch, and add
a redacted feedback file at docs/feedback/<topic>.md using the [feedback
template](feedback/README.md). Then open a [pull request](https://github.com/Jasper-1024/home-assistant-garmin_connect/pulls)
with Jasper-1024/home-assistant-garmin_connect as the base. Do not include
passwords, tokens, or raw health data. Maintainers use Plane only for internal
triage and archival; it is not a public feedback endpoint.

This fork originated from
[cyberjunky/home-assistant-garmin_connect](https://github.com/cyberjunky/home-assistant-garmin_connect).
The [ha-garmin](https://github.com/cyberjunky/ha-garmin) dependency remains its
upstream Garmin API library and is credited as such.

The product is a prospective archive. It does not start Historical Backfill
during normal setup and has no full-year enablement switch.

## Controlled upgrade and enablement

1. Create a Home Assistant backup that includes the configuration directory,
   Recorder database, and Garmin integration Store files.
2. Upgrade the integration to 3.1.0-beta.2 and restart Home Assistant during a
   maintenance window.
3. Before enabling the archive, verify that existing current-value Garmin
   entities refresh normally and that retained Home Assistant history remains
   queryable.
4. In the Garmin config-entry options, explicitly enable **Prospective
   Archive**. The first run is bounded to the current Home Assistant local
   date; it does not fetch a year of history.
5. Observe the privacy-safe Archive Status (syncing, then idle or failed) and
   validate the current-day result through the existing Archive Query Surfaces.
   Current entity polling defaults to **900 seconds (15 minutes)**; it is a
   polling cadence, not a device-sync or data-availability guarantee. A
   fifteen-minute freshness target, seven-day reconciliation window, and
   one-FIT-per-hour pacing are nominal policies, not completeness or real-time
   guarantees.
6. Restart Home Assistant once more. Verify that Archive Enablement, retained
   query results, account isolation, FIT files, and the status contract persist.

Archive Disablement is the reversible stop mechanism: disable the option to
cancel future automatic work. Disablement, reload, upgrade, and rollback do
not delete Recorder statistics, Store records, Calendar records, or FIT files.
This beta provides no archive deletion or automatic expiry action. Long-term
archive records have no built-in retention period and remain until an
administrator manages storage outside this integration. Storage growth on the
order of 1–2 GB is an accepted planning estimate.

Source Instants remain absolute aware timestamps; Source Calendar Date remains
separate provenance metadata; Home Assistant chooses Display Time from its
configured timezone. The archive does not rewrite source timestamps to the
operator's display timezone. No release instruction in this document contacts
Garmin production outside the operator's explicitly enabled prospective archive,
and none starts Historical Backfill.

## Downgrade and re-authentication safety

Versions based on 3.0.14 can replace all config-entry data during re-authentication
or reconfiguration. Before downgrading, create a backup that includes the Garmin
private Store files and Recorder database. On re-upgrade, sign in with the same
Garmin account and allow the integration to restore its archive identity from its
private catalog. The catalog is bound to the configured Garmin profile and is not
adopted for another account.

Do not re-authenticate a downgraded entry with a different Garmin account. If the
Archive Status becomes failed after re-upgrade, stop there: do not delete or
replace Store files, FIT files, or Recorder statistics. Restore the backup or
re-authenticate with the original account. A missing, damaged, or owner-mismatched
catalog intentionally fails closed rather than creating a new archive identity.

## Release prerequisites and offline gate

The candidate must be a clean commit installed in an exact Home Assistant Core
**2026.7.4** environment. requirements.txt is the install input: it carries the
same exact pins as every manifest requirement, including ha-garmin and
garmin-fit-sdk. Run the following from the candidate checkout after creating
the candidate commit, but before creating or moving any remote state:

```text
scripts/lint
.venv/bin/pytest tests/ -q
.venv/bin/python scripts/release_gate.py --check-installed
.venv/bin/python scripts/release_gate.py --check-installed \
  --candidate-sha "$(git rev-parse HEAD)"
```

The identity command reads its default tag and Release version dynamically from
manifest.json; for this candidate both are 3.1.0-beta.2. A supplied tag or
Release version must equal that manifest prerelease. Its candidate SHA must be
a full 40- or 64-character commit SHA and must equal the clean checkout's HEAD.
It requires 3.1.0-beta.2 to be an annotated tag (not a lightweight tag)
resolving to that same commit, verifies the tag, manifest version, and supplied
or defaulted GitHub Release version are the same semantic prerelease, and
rejects noncanonical prerelease spellings such as beta1 or beta-1. hacs.json
intentionally has no integration-version field: HACS identifies a release from
the repository tag. The gate instead binds its Home Assistant floor to
requirements.txt and the Recorder floor.

GitHub Actions repeats the exact installation/import check in **Offline release
gates (Home Assistant 2026.7.4)** after the full pytest job. HACS validation
and Hassfest run in the separate **Validate** workflow. Their hosted actions,
scripts/lint, full pytest, and the release-gate command are all required before
publication. The HACS and Hassfest actions require GitHub Actions; the
metadata/import/identity checks above require no network. A passing local gate
does not publish beta.2 or refresh HACS.

## Known limitations and observed issues

- Archive Enablement is off by default; without explicit enablement this beta
  performs no Prospective Archive work and never starts Historical Backfill.
- Archive freshness is a nominal fifteen-minute polling target. Garmin upload
  delay, rate limits, network failures, and Home Assistant downtime can create
  a Continuity Gap; missing data is never synthesized.
- This repository has no authorized live GitHub Release or HACS refresh in the
  offline gate. A passing gate is not evidence that either external service has
  published or indexed the candidate.

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
  tests/test_fit_queue.py::test_fit_backlog_over_256_survives_restart_and_is_fully_consumed \
  tests/test_fit_queue.py::test_valid_local_fit_completes_queue_without_download \
  tests/test_fit_queue.py::test_fit_queue_isolated_by_account_and_background_gate \
  tests/test_fit_queue.py::test_runtime_corrupt_local_fit_is_removed_and_downloaded_again \
  tests/test_fit_archive.py::test_fit_archive_validates_privately_and_atomically \
  tests/test_fit_archive.py::test_fit_archive_degrades_when_directory_fsync_is_unsupported \
  tests/test_fit_archive.py::test_fit_archive_uses_path_chmod_without_fchmod \
  tests/test_fit_archive.py::test_fit_archive_uses_windows_safe_permission_fallback \
  tests/test_fit_archive.py::test_fit_archive_does_not_swallow_path_permission_errors \
  tests/test_history.py::test_archive_rate_limit_enters_durable_backoff_without_cadence \
  tests/test_history.py::test_archive_rate_limit_backoff_survives_restart_and_expires_once \
  tests/test_history.py::test_first_sync_rate_limit_retries_after_expiry_without_restart \
  tests/test_history.py::test_successful_archive_status_persists_and_restores_schedule \
  tests/test_history.py::test_archive_auth_classification_requires_genuine_account_failure \
  tests/test_history.py::test_archive_cycle_failure_does_not_break_foreground_request \
  tests/test_history.py::test_malformed_structured_record_fails_archive_without_blocking_foreground_work \
  tests/test_history.py::test_downgrade_reauth_recovers_bound_archive_identity \
  tests/test_history.py::test_missing_key_never_adopts_mismatched_or_unbound_archive \
  tests/test_history.py::test_numeric_legacy_owner_binding_migrates_after_profile_verification \
  tests/test_history.py::test_downgrade_reauth_preserves_recorder_calendar_and_valid_fit_artifacts \
  tests/test_history.py::test_start_keeps_historical_backfill_dormant \
  tests/test_init.py::test_real_config_entry_lifecycle_keeps_backfill_dormant_and_surfaces_visible \
  tests/test_release_gates.py::test_release_privacy_snapshots_cover_diagnostics_status_action_and_calendars \
  tests/test_history.py::test_status_sensor_exposes_only_privacy_safe_placeholders \
  tests/test_history.py::test_recorder_compatibility_uses_real_scratch_recorder \
  tests/test_history.py::test_has_supported_home_assistant_version \
  tests/test_history.py::test_recorder_compatibility_accepts_supported_versions_with_a_slow_queue_task \
  tests/test_history.py::test_recorder_compatibility_rejects_versions_below_the_hacs_minimum \
  tests/test_history.py::test_recorder_compatibility_rejects_missing_durable_import_seam \
  tests/test_init.py::test_stalled_recorder_check_starts_archive_in_background_and_can_cancel \
  tests/test_history_recorder.py::test_release_gate_scratch_recorder_restart_revision_and_no_state_changed \
  tests/test_fit_archive.py::test_optional_private_captured_fit_replay
```

The matrix explicitly covers seven-day reconciliation, Manual Repair, FIT
pacing/restart/valid-file skip/account isolation, runtime recovery from a
corrupt local FIT, portable directory fsync and file permission fallback, the
>256-item FIT backlog across restart and continued consumption, archive-only
429 backoff including restart and expiry, genuine reauthentication and failure
isolation, dormant Historical Backfill, exact status/privacy output, and
persistence/restoration of the archive success schedule. It also covers
3.0.14-style token-data replacement: same-profile restoration retains Recorder,
Calendar, and valid 0600 FIT artifacts, while a different authenticated Garmin
account cannot adopt the retained archive. The Recorder capability contract uses
a real scratch Recorder, canonical version parsing, supported patch/minor
versions, a below-floor fail-closed check, a durable-seam mismatch, and the
five-second archive-startup isolation boundary. These tests are executable
members of the single command, not documentation-only claims. The fixture,
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
- FIT files are valid and mode 0600;
- logs, diagnostics, status, actions, and Calendar output contain no raw
  measurements, arrays, credentials, account identity, address, or GPS;
- Recorder/Store growth is bounded and expected.

The validation contract is structural: timestamps remain aware, counts are
consistent, equal values and revisions converge, and repeated/restarted work
does not create duplicate points or state_changed replay. Live counts must be
recorded from the current-day run, not copied from this document. Missing Garmin
data remains missing; the archive does not synthesize values or claim forensic
completeness.

## Rollback

Disable the Prospective Archive or restore the previous integration fork.
Never delete Home Assistant Statistics, Store files, Recorder data, or FIT
files as a rollback step.

## Maintainer publication checklist

This list is deliberately manual for remote operations. It must be performed
by a maintainer with the relevant GitHub/HACS authority; this repository does
not create tags, push commits, create Releases, or refresh HACS itself.

1. Confirm the candidate SHA and annotated 3.1.0-beta.2 tag with the final
   offline command above. Create/push neither from this gate.
2. In GitHub, create or inspect the Release named 3.1.0-beta.2, attached to
   that tag; record its URL and displayed target SHA in the release evidence.
3. Run/confirm green **Validate** (Hassfest and HACS validation) and **Tests**
   workflows for the candidate SHA.
4. After HACS refreshes, verify its integration entry resolves the semantic
   version 3.1.0-beta.2 and the 2026.7.4 Home Assistant floor. Record the
   observed time and URL; do not substitute a commit SHA for the version.
5. Perform the controlled upgrade/enablement validation above and record only
   redacted observations. Use the fork/branch/feedback-file pull-request path
   described in Package source and feedback for public feedback; do not open an
   empty pull request.

## Optional private replay

Private replay is opt-in only. A local 0600 capture may be supplied through an
environment variable to the inspector tests; the path must be gitignored, the
test skips when absent, and scalar values are never printed or committed.
