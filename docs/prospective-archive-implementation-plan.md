# Prospective Archive implementation plan

Status: accepted design; implementation pending.

## Outcome

Replace automatic historical backfill as the normal product behavior with an
explicitly enabled prospective archive. Preserve every valid record returned
for the frozen beta catalog, make current-day data normally queryable within
about fifteen minutes, and keep occasional source or transport gaps explicit.

The existing tested historical backfill implementation remains dormant. It is
not exposed by the beta and cannot start during normal integration setup.

## Delivery slices

### 1. Persistent enablement

- Add a per-config-entry `Enable prospective archive` option, defaulting to
  disabled for new and upgraded entries.
- On a disabled-to-enabled transition, persist the current Home Assistant local
  date as the new archive activation date.
- On disable, stop automatic archive tasks without deleting Recorder, Store, or
  FIT data.
- On re-enable, establish a new activation date and do not automatically fill
  the disabled interval.
- Keep statistics, Calendars, FIT files, and privacy-safe status queryable while
  automatic archival is disabled.

### 2. Archive lifecycle and scheduling

- Initialize archive identity, Store, Recorder compatibility, query surfaces,
  and manual repair regardless of automatic enablement.
- Start no historical-backfill task from normal config-entry setup.
- When enabled, run one immediate bounded current-day synchronization, followed
  by a non-overlapping nominal fifteen-minute cycle.
- Coalesce missed timer ticks; never run concurrent cycles for one account.
- Keep current-value coordinator work higher priority than archive work.
- Keep the tested historical backfill scheduler unreachable from the beta
  product path.

### 3. Open and settled dates

- Treat the current Home Assistant local date as open.
- Keep a nominal seven-day reconciliation window for dates that remain empty,
  failed, or otherwise incomplete.
- Check local family presence before requesting an older date. Do not request a
  locally settled date automatically.
- When data first appears for an older date, persist it and require one later
  unchanged confirmation before settling the date.
- Settle a date that remains empty through the reconciliation window.
- A settled date receives no further automatic requests. Explicit manual repair
  may reopen and update it.
- Store Garmin Source Calendar Dates separately from normalized timestamp
  instants. Preserve Garmin's original `startTimeLocal` or an equivalent Source
  Calendar Date at the collection and conversion boundary, independently of
  the aware `startTime` Source Instant.
- Treat a request date as source provenance only for a date-scoped source whose
  contract makes it authoritative. Never stamp activities returned by an
  unscoped `get_activities()` feed with the synchronization `target_date`.
- Only when activity source-local date provenance is genuinely absent, use the
  aware `startTime` UTC date as an explicit conservative degradation. For
  date-only `daily_summary` and `training_status`, use the explicit 00:00
  UTC+08:00 canonical date-summary bucket, normalized to UTC; do not label that
  derived identity a Garmin Source Instant.

### 4. Capture and persistence

- Preserve every valid source record returned for the frozen beta catalog;
  perform no intentional thinning or representative sampling.
- Keep raw numeric samples separate from Garmin-computed summaries.
- Preserve an aware Garmin timestamp's absolute Source Instant for Recorder and
  let Home Assistant choose the display timezone. Use the canonical date-summary
  bucket only when no aware timestamp is supplied.
- Preserve missing data as missing, never zero or synthesized data.
- Maintain deterministic timestamp/logical-ID/revision upserts.
- Keep structured Store records, statistics IDs, account isolation, Calendars,
  and FIT ownership stable across restart and option reload.

### 5. FIT pacing

- Discover activities during normal archive synchronization.
- Archive no more than one pending FIT per account per hour.
- Never redownload an existing valid FIT.
- Persist the FIT queue across restart.
- Do not let FIT failure or backlog block other data families.

### 6. Manual repair and dormant backfill

- Keep `garmin_connect.sync_history` available while automatic archival is
  disabled.
- Allow explicit ranges of at most 31 inclusive days, including dates before
  the current activation date.
- Manual repair never changes enablement, activation date, or starts an
  automatic backfill.
- Retain the historical backfill implementation and its tests, but expose no
  beta option or action that starts it.

### 7. Failure isolation

- Archive endpoint, schema, Recorder, Store, FIT, network, and archive-originated
  rate-limit failures affect only archival.
- Preserve a conservative archive-only 24-hour backoff after an archive 429.
- Keep existing current-value coordinators running through archive failures.
- Escalate only genuine account authentication failure to account-wide
  reauthentication.
- Continue to fail closed before history writes when Recorder compatibility is
  not satisfied.

### 8. Minimal status contract

Expose only:

- state: `disabled`, `idle`, `syncing`, `backoff`, or `failed`;
- activation date;
- last successful synchronization;
- next eligible run;
- safe error type when applicable.

Open-date fingerprints, confirmation attempts, family presence details, and
FIT queue bookkeeping remain internal.

### 9. Release and documentation

- Keep the unreleased version at `3.1.0-beta.1`.
- Replace the fixed `2026-07-24` and post-canary full-backfill instructions
  with backup, upgrade, explicit enablement, first-current-day validation, and
  reversible disablement.
- State that fifteen minutes and seven days are nominal operating targets, not
  hard guarantees.
- Document that disablement and rollback never delete archived data.
- Provide no archive deletion action and no custom export API.

## Required verification

- Options-flow tests for default-off, first enable, disable, and re-enable.
- Lifecycle tests proving normal setup never starts historical backfill.
- Deterministic-clock tests for immediate first sync, nominal fifteen-minute
  cycles, single-flight behavior, and current-value priority.
- Open-date tests for empty retry, delayed appearance, unchanged confirmation,
  settlement, seven-day expiry, restart, and manual reopen.
- Tests proving settled local dates cause no automatic Garmin request.
- Capture tests proving all returned frozen-catalog records are persisted
  without thinning and missing data remains missing.
- Archive-only 429/error isolation tests proving current coordinators continue.
- Manual repair tests while automatic archival is disabled and for dates before
  activation.
- FIT one-per-hour, valid-file skip, failure isolation, and restart tests.
- Timestamp tests for source UTC+2, HA UTC+8 display semantics, source calendar
  date separation, midnight, DST, and travel offsets.
- Activity provenance tests across Garmin/`ha-garmin` conversion, archive
  normalization and persistence, and Calendar query: preserve
  `startTimeLocal` or an equivalent Source Calendar Date beside the aware
  `startTime` Source Instant; reject `target_date` stamping for unscoped
  `get_activities()` results; and use `startTime` UTC date only when local
  provenance is genuinely absent.
- Existing Recorder, Store, Calendar, privacy, config-flow, migration,
  coordinator, sensor, action, diagnostics, entity-ID, and multi-account
  regression suites remain green.
- Release gate proves the new options/status/docs contract and the absence of an
  automatic historical-backfill startup path.

## Release readiness

The current branch is not release-ready until every slice above is implemented
and reviewed. Afterward, deployment consists of a Home Assistant backup,
integration upgrade and restart, verification that existing entities still
refresh, explicit archive enablement, first-current-day validation, a second
restart to prove persistence, and continued observation of privacy-safe status
and storage growth.
