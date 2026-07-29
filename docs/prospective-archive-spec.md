# Prospective Archive Specification

## Problem Statement

Garmin Connect exposes useful raw measurements, structured records, activities,
FIT files, and Garmin-computed summaries, but the integration's existing
current-value entities do not provide a durable longitudinal record suitable
for later analysis. The existing archive implementation also starts an
automatic Historical Backfill during normal setup. That behavior creates
unbounded private-endpoint traffic, storage growth, and operational risk merely
because an operator upgrades or restarts the integration.

The operator does not need forensic reconstruction of all prior Garmin
history, strict real-time delivery, or perfect continuity. The operator needs a
best-effort Prospective Archive from an explicitly chosen activation point,
normally fresh within about fifteen minutes, that retains every valid Source
Record Garmin returns for the Frozen Archive Catalog. Occasional unavoidable
Continuity Gaps are acceptable, but deliberate thinning, fabricated values,
silent automatic backfill, and archive failures that disrupt established
current-value entities are not.

## Solution

Provide an explicitly enabled, per-account Prospective Archive. It remains off
for new and upgraded entries until the operator enables it. Archive Enablement
records the current Home Assistant local calendar date as the Archive
Activation Date, performs one bounded current-day synchronization, and then
runs non-overlapping synchronization cycles on a nominal fifteen-minute
schedule.

The archive preserves every valid Source Record returned for the Frozen Archive
Catalog. Numeric Source Records become Home Assistant statistics, structured
summaries remain available through read-only Calendars, and activity details
are retained as FIT files. Raw measurements and Garmin-computed summaries are
both preserved without turning the integration into an analysis engine.

A nominal seven-day Reconciliation Window handles delayed Garmin uploads.
Before automatically requesting an older date, the integration checks its
local archive state. An Open Archive Date remains eligible while data is
missing, incomplete, or awaiting one unchanged confirmation. A Settled Archive
Date is not requested automatically. Manual Repair can explicitly synchronize
up to 31 inclusive days, including dates before the Archive Activation Date,
and can reopen a Settled Archive Date without enabling automatic backfill.

Archive Disablement stops automatic collection only. Existing statistics,
Calendar records, FIT files, status, and Manual Repair remain available.
Disabling, upgrading, reloading, rolling back, or re-enabling never deletes
archived data. Archive failures remain isolated from current-value polling,
except that a genuine account authentication failure may trigger the existing
account-wide reauthentication flow.

## User Stories

1. As a Home Assistant operator, I want prospective archival to be disabled by default, so that an installation or upgrade never silently increases Garmin requests or storage use.
2. As a Home Assistant operator, I want to enable archival explicitly for each Garmin account, so that collection begins only with my consent.
3. As a Home Assistant operator, I want Archive Enablement to persist across reloads and restarts, so that normal collection resumes without repeated configuration.
4. As a Home Assistant operator, I want the Archive Activation Date to be the Home Assistant local date on which I enable archival, so that the automatic collection boundary is understandable.
5. As a Home Assistant operator, I want enabling the archive to trigger a bounded current-day synchronization, so that newly available data starts becoming useful immediately.
6. As a Home Assistant operator, I want current-day archive cycles to run about every fifteen minutes, so that recently synchronized Garmin data becomes available soon enough for health review.
7. As a Home Assistant operator, I want the fifteen-minute interval to be a freshness target rather than a real-time guarantee, so that Garmin delays, rate limits, downtime, and network failure are represented honestly.
8. As a Home Assistant operator, I want missed timer ticks to coalesce, so that downtime or a slow cycle does not create a burst of duplicate archive jobs.
9. As a Home Assistant operator, I want only one archive cycle per account to run at a time, so that requests and writes cannot overlap unpredictably.
10. As a Home Assistant operator, I want current-value coordinators to take priority over archive work, so that established Garmin entities continue to refresh responsively.
11. As a Home Assistant operator, I want normal setup to avoid all Historical Backfill, so that a restart cannot unexpectedly request months of private Garmin history.
12. As a Home Assistant operator, I want automatic archival to begin no earlier than the current Archive Activation Date, so that earlier history is never fetched without an explicit Manual Repair request.
13. As a Home Assistant operator, I want disabling archival to stop future automatic archive requests, so that I can suspend collection immediately.
14. As a Home Assistant operator, I want disabling archival to preserve all Source Records already stored, so that pausing collection does not destroy longitudinal history.
15. As a Home Assistant operator, I want statistics, Calendars, and FIT files to remain queryable while archival is disabled, so that collection state does not control access to existing history.
16. As a Home Assistant operator, I want Manual Repair to remain available while automatic archival is disabled, so that I can repair a known gap without resuming continuous collection.
17. As a Home Assistant operator, I want re-enabling archival to establish a new Archive Activation Date, so that the disabled interval is not fetched automatically.
18. As a Home Assistant operator, I want re-enabling to preserve the existing archive identity and data, so that the archive remains continuous where records exist.
19. As a Home Assistant operator, I want entity enablement to affect presentation only, so that disabling an entity does not unexpectedly start or stop data collection.
20. As a longitudinal-data consumer, I want every valid Source Record returned for the Frozen Archive Catalog retained, so that downstream analysis is not biased by intentional thinning.
21. As a longitudinal-data consumer, I want raw heart rate, stress, Body Battery, HRV, and sleep-stream samples retained, so that I can analyze their original time series externally.
22. As a longitudinal-data consumer, I want segmented steps, floors, and intensity records retained, so that intraday movement remains available for later analysis.
23. As a longitudinal-data consumer, I want respiration and SpO2 records retained, so that respiratory and oxygen trends remain available.
24. As a longitudinal-data consumer, I want daily and training snapshots retained, so that Garmin's own computed context is available beside raw measurements.
25. As a longitudinal-data consumer, I want sleep, activity, and health structures retained, so that structured Garmin events remain queryable without flattening away their meaning.
26. As a longitudinal-data consumer, I want activity FIT files retained, so that detailed activity records remain available outside the summarized Home Assistant entities.
27. As a longitudinal-data consumer, I want Garmin-computed summaries preserved without being treated as raw samples, so that their provenance and semantics remain clear.
28. As a longitudinal-data consumer, I want missing Source Records to remain missing, so that a Continuity Gap is never converted into zero or synthesized data.
29. As a longitudinal-data consumer, I want stable statistic identities and deterministic upserts, so that retries and revisions do not create uncontrolled duplicates.
30. As a multi-account operator, I want archive identities, statistics, Store records, and FIT files isolated by Garmin account, so that one account cannot contaminate another.
31. As a Home Assistant operator, I want archive identity and ownership to remain stable across restart and option reload, so that enabling or disabling does not fork the archive.
32. As a Home Assistant operator, I want the current Home Assistant local date to remain an Open Archive Date, so that data uploaded later on the same day can still be captured.
33. As a Home Assistant operator, I want recent older dates checked only when local state says they are open, so that reconciliation avoids unnecessary Garmin requests.
34. As a Home Assistant operator, I want an older date to remain open when expected families are absent or a prior attempt failed, so that delayed data has a bounded chance to arrive.
35. As a Home Assistant operator, I want newly appeared data saved immediately and checked once more later, so that a date can settle after one unchanged confirmation.
36. As a Home Assistant operator, I want an unchanged confirmation to settle an older date, so that stable data stops generating automatic requests.
37. As a Home Assistant operator, I want a date that remains empty through the nominal seven-day Reconciliation Window to settle as missing, so that permanent gaps do not cause indefinite requests.
38. As a Home Assistant operator, I want a Settled Archive Date to cause no automatic Garmin request, so that local knowledge meaningfully limits private-endpoint traffic.
39. As a Home Assistant operator, I want reconciliation state to survive restart, so that a restart does not reopen every recent date or lose stability confirmations.
40. As a Home Assistant operator, I want the seven-day window to be a nominal operating policy rather than a completeness guarantee, so that occasional delayed or absent Garmin data remains acceptable.
41. As a Home Assistant operator, I want Manual Repair to target a single date or a range of at most 31 inclusive days, so that repairs are useful but bounded.
42. As a Home Assistant operator, I want Manual Repair to accept dates before Archive Activation, so that I can explicitly recover a known earlier period.
43. As a Home Assistant operator, I want Manual Repair to reopen a Settled Archive Date, so that an explicit repair can retrieve data that appeared after automatic reconciliation ended.
44. As a Home Assistant operator, I want Manual Repair never to change archive enablement or the Archive Activation Date, so that a repair does not silently change future behavior.
45. As a Home Assistant operator, I want Manual Repair never to start automatic Historical Backfill, so that its effects remain limited to the requested dates.
46. As a Home Assistant operator, I want valid local FIT files skipped, so that restart and repair do not redownload existing activity files.
47. As a Home Assistant operator, I want at most one pending FIT file archived per account per hour, so that detailed activity downloads remain conservatively paced.
48. As a Home Assistant operator, I want pending FIT work persisted across restart, so that pacing does not lose discovered activities or restart from an unsafe state.
49. As a Home Assistant operator, I want the first enabled archive synchronization to download at most one FIT file, so that initial activation remains bounded.
50. As a Home Assistant operator, I want FIT failure or backlog isolated from other data families, so that activity-file problems do not block health and summary records.
51. As a Home Assistant operator, I want archive endpoint, schema, network, Recorder, Store, and FIT failures to affect only archival, so that current-value entities remain usable.
52. As a Home Assistant operator, I want an archive-originated rate limit to pause archive work for 24 hours, so that the integration responds conservatively without disabling current-value polling.
53. As a Home Assistant operator, I want only genuine account authentication failure to escalate into reauthentication, so that ordinary archive failures do not log me out.
54. As a Home Assistant operator, I want history writes to fail closed when Recorder compatibility is not satisfied, so that incompatible storage cannot corrupt the archive.
55. As a Home Assistant operator, I want archive status to distinguish disabled, idle, syncing, backoff, and failed states, so that I can understand the archive without seeing internal ledgers.
56. As a Home Assistant operator, I want status to expose the activation date, last success, next eligible run, and a safe error type, so that I can verify operation without exposing private health data.
57. As a Home Assistant operator, I want open-date fingerprints, confirmation counts, family-presence details, and FIT queue bookkeeping kept private, so that status stays simple and privacy-safe.
58. As a Home Assistant operator, I want archived numeric Source Records available through Home Assistant statistics, so that normal Home Assistant history tools can query them.
59. As a Home Assistant operator, I want structured sleep, activity, and health summaries available through read-only Calendars, so that their date-based structure remains useful.
60. As a Home Assistant operator, I want FIT files available through the established archive location, so that detailed activity data can be consumed without a new export service.
61. As a timezone-changing user, I want every aware Garmin timestamp stored as its Source Instant, so that travel or source offsets do not change the measurement's absolute identity.
62. As a timezone-changing user, I want Home Assistant to project Source Instants into my browser or server timezone, so that display follows Home Assistant conventions.
63. As a longitudinal-data consumer, I want Source Calendar Date stored separately from Source Instant, so that a Garmin request day is not confused with a UTC or display date.
64. As a longitudinal-data consumer, I want cross-midnight, daylight-saving, and travel-offset samples to retain their absolute instant, so that time-series ordering remains correct.
65. As a Home Assistant operator, I want the beta to avoid a per-sample source-offset sidecar, so that the archive uses Home Assistant's existing timestamp model without redundant metadata.
66. As a Home Assistant operator, I want Archive Retention without automatic expiry, so that long-term analysis is not truncated by a rolling deletion policy.
67. As a Home Assistant operator, I want no archive deletion or bulk-clear action in this beta, so that accidental destructive operations are impossible through the integration.
68. As a Home Assistant operator, I want upgrades, disablement, and rollback to preserve Recorder records, Store documents, summaries, and FIT files, so that operational changes are reversible.
69. As a downstream analyst, I want the integration to synchronize Source Records without interpreting health state, so that analytical policy remains in my separate program.
70. As a downstream analyst, I want no integration-generated trend, correlation, baseline, or health assessment, so that Garmin source data is not mixed with new model conclusions.
71. As a Home Assistant operator, I want no custom export API or direct private Store access in this release, so that the product surface stays small and uses established Home Assistant query mechanisms.
72. As a Home Assistant operator, I want Deferred Data Families described as deferred rather than unsupported, so that the beta makes no inaccurate claim about Garmin or future integration capability.
73. As a release operator, I want activation and first-current-day validation to replace the old fixed-date canary, so that release verification matches the prospective product.
74. As a release operator, I want release guidance to include backup, upgrade, restart, explicit enablement, persistence verification, and reversible disablement, so that deployment is controlled.
75. As a release operator, I want release gates to prove that no normal startup path can construct or start automatic Historical Backfill, so that the old product behavior cannot regress.
76. As a Home Assistant operator, I want recurring synchronization to begin only after the first bounded current-day synchronization passes its startup checks, so that a failed activation cannot create an uncontrolled request loop.

## Implementation Decisions

- The Prospective Archive is a per-config-entry capability controlled by a
  persistent option. It defaults to disabled for both new and upgraded entries.
- A disabled-to-enabled transition persists the current Home Assistant local
  calendar date as the Archive Activation Date. A later re-enable replaces it
  with a new date; the disabled interval never becomes automatically eligible.
- Option changes reload the config entry while preserving the account's archive
  identity, statistics, Store documents, Calendar records, and FIT files.
- Archive infrastructure, compatibility checks, query surfaces, status, and
  Manual Repair initialize independently of automatic enablement. Only the
  recurring prospective synchronization task is conditional on enablement.
- Normal config-entry setup cannot create or start the legacy Historical
  Backfill scheduler. The existing scheduler and its tests remain dormant for a
  possible future capability, with no beta option, service, or startup path
  exposing it.
- Enabling runs one immediate, bounded current-day synchronization and then
  schedules cycles at a nominal fifteen-minute interval only after that first
  synchronization passes compatibility and persistence checks. A failed first
  synchronization enters the appropriate failed or backoff state without
  starting the recurring cadence. Cycles are single-flight per account, and
  missed ticks coalesce instead of queuing.
- Archive work uses the shared request-control mechanism at background
  priority. Existing current-value coordinator requests retain foreground
  priority.
- The current Home Assistant local date is always open. Older eligible dates
  are considered only within a nominal seven-day Reconciliation Window and
  only from the current Archive Activation Date forward.
- Reconciliation is local-first. Before requesting an older date, the archive
  checks persisted family presence and settlement state. A Settled Archive Date
  produces no automatic Garmin request.
- An older date remains open while it is empty, incomplete, failed, or awaiting
  stability confirmation. When data first appears, the archive persists it and
  requires one later unchanged confirmation before settling the date.
- A date that remains empty through its Reconciliation Window settles with an
  explicit gap. Missing values are never converted to zero and Source Records
  are never synthesized.
- Internal fingerprints, family-presence records, stability confirmation state,
  reconciliation state, and FIT queue state persist across restart. They are
  implementation bookkeeping and are not public status fields.
- Manual Repair uses the existing synchronization action. It accepts one date
  or a range of no more than 31 inclusive days, can target dates before the
  Archive Activation Date, can reopen a Settled Archive Date, and remains
  callable while automatic archival is disabled.
- Manual Repair does not enable the Prospective Archive, change the Archive
  Activation Date, start a recurring task, or expose the dormant Historical
  Backfill scheduler.
- The Frozen Archive Catalog for this beta consists of raw heart-rate, stress,
  Body Battery, HRV, and sleep streams; segmented steps, floors, and intensity;
  respiration and SpO2; daily and training snapshots; structured sleep,
  activity, and health records; and activity FIT files.
- Every valid Source Record returned for the Frozen Archive Catalog is retained.
  Best-effort continuity permits unavoidable source and transport gaps but does
  not permit intentional thinning, representative sampling, or dropping valid
  records for convenience.
- Raw numeric measurements and Garmin-computed summaries retain distinct
  semantics. Both are archived; neither is interpreted as a new integration
  health conclusion.
- Numeric Source Records use Home Assistant statistics. Structured summaries
  use the existing read-only Calendar surfaces. Activity detail uses archived
  FIT files. No custom export API or direct private Store interface is added.
- Timestamped records preserve the Source Instant represented by an aware
  Garmin timestamp. Statistics identity is based on the normalized absolute
  instant, not the original offset spelling.
- Source Calendar Date remains separate request and checkpoint metadata. It is
  not derived from UTC date or Display Time. When `daily_summary` or
  `training_status` supplies only a calendar date and no aware Garmin timestamp,
  Recorder identity uses the explicit canonical date-summary bucket instant:
  00:00 at UTC+08:00, normalized to UTC. This is not a Garmin-provided Source
  Instant; Source Calendar Date remains the separate provenance value. An aware
  Garmin timestamp always retains its actual Source Instant.
- Home Assistant owns Display Time and projects Source Instants into the user's
  browser or server timezone. The beta adds no per-sample original-offset
  sidecar.
- Persistence uses stable account isolation, deterministic timestamp or logical
  identities, and revision-aware upserts so retries and changed source records
  do not create uncontrolled duplicates.
- Activity discovery can add pending FIT work during normal synchronization.
  No more than one pending FIT per account is archived in any hour, including
  no more than one FIT during the first enabled synchronization.
- FIT queue and pacing state survive restart. A valid existing local FIT is
  skipped. FIT failure or backlog does not fail synchronization of other data
  families or prevent their persistence.
- Archive-originated endpoint, schema, network, Recorder, Store, FIT, and rate
  limit failures are isolated from current-value polling. Existing entities
  continue refreshing whenever their own path is healthy.
- An archive-originated HTTP 429 establishes a conservative archive-only
  24-hour backoff. It does not pause foreground current-value requests.
- Only a genuine account authentication failure may use the existing
  account-wide reauthentication behavior. Ordinary authorization-like endpoint
  errors must not be misclassified without evidence that account credentials
  are invalid.
- Recorder compatibility is checked before history writes. An incompatible or
  unavailable Recorder path fails closed for archival without blocking the
  current-value integration.
- The public Archive Status state is exactly one of `disabled`, `idle`,
  `syncing`, `backoff`, or `failed`. Its only additional contract fields are
  Archive Activation Date, last successful synchronization, next eligible run,
  and a privacy-safe error type when applicable.
- Archive Status changes notify its Home Assistant entity. Internal retry
  counts, open-date lists, fingerprints, family details, confirmation counts,
  request counts, pending FIT entries, and health payloads remain private.
- Archive Disablement cancels future automatic work but does not delete data,
  hide query surfaces, disable Manual Repair, or alter individual entity
  enablement.
- Archive Retention has no automatic expiry. Disablement, reload, upgrade, and
  rollback preserve Recorder statistics, Store records, Calendar summaries,
  and FIT files.
- This beta includes no archive deletion, bulk-clear, or automatic cleanup
  action. Entity disablement controls presentation only and is not a retention
  mechanism.
- Training-readiness history, weight and body composition, hydration, blood
  pressure, nutrition, menstrual data, ECG, skin temperature, and glucose are
  Deferred Data Families for this beta. Deferral does not assert that Garmin,
  the connected device, or a future integration version cannot support them.
- The integration synchronizes and archives records only. External Analysis
  owns longitudinal trends, correlations, health-state interpretation, derived
  baselines, and model-generated conclusions.
- Fifteen-minute freshness, seven-day reconciliation, and one-hour FIT pacing
  are operating policies rather than guarantees of Garmin availability,
  network success, or uninterrupted Home Assistant runtime.
- The beta version remains `3.1.0-beta.1` until these behaviors and release
  gates are implemented and reviewed.

## Testing Decisions

- Tests assert externally observable behavior rather than scheduler internals,
  private ledger shapes, method call order, or implementation-specific
  fingerprints. The preferred assertion is what Garmin was requested, what
  became queryable or durable, what status the operator sees, and whether
  current-value entities continued.
- There are exactly two high-level behavioral test seams. The primary seam is
  the `GarminHistoryArchive` interface. The secondary seam is the Home Assistant
  config-entry lifecycle interface. Internal adapters are injection points, not
  additional public test surfaces.
- Through the primary archive seam, tests drive archive start, deterministic
  time advancement, automatic cycles, Manual Repair, archive stop, status, and
  archive queries. They observe synchronization reports, persisted outcomes,
  Garmin requests, statistics, Calendar records, FIT files, and status
  transitions.
- The archive seam receives a private deterministic clock and timer adapter for
  tests. It controls Home Assistant local date, immediate first execution,
  fifteen-minute wakeups, seven-day reconciliation expiry, 24-hour rate-limit
  backoff, one-hour FIT pacing, cancellation, and missed-tick coalescing.
- Source, Recorder, Store, FIT, compatibility, and request-control adapters
  remain internal to the archive deep module. Tests can substitute them to
  produce deterministic external outcomes without exposing a scheduler or
  settlement ledger as a separate interface.
- Archive lifecycle tests cover disabled startup, immediate enabled sync,
  first-sync failure without recurring cadence, nominal cycles, single-flight
  behavior, coalesced ticks, local-date rollover, stop and cancellation, and
  restart recovery.
- Reconciliation tests cover empty retry, delayed first appearance, persistence
  of newly returned records, one later unchanged confirmation, settlement,
  empty-through-window settlement, failed and incomplete dates, restart, and
  explicit Manual Repair reopening.
- A release-gate test proves that a Settled Archive Date produces no automatic
  Garmin request and that no normal setup path constructs or starts the dormant
  Historical Backfill scheduler.
- Capture-completeness tests provide Source Records for every Frozen Archive
  Catalog family and prove that every valid returned record reaches its
  statistics, Store, Calendar, or FIT destination without thinning.
- Missing-data tests prove absence remains absence, sparse responses remain
  sparse, and null, missing, and zero retain distinct meanings.
- Persistence tests cover stable account identity, deterministic upsert,
  source revision, annual Store partitions, restart, option reload, corrupt
  local metadata isolation, and multi-account separation.
- FIT tests cover discovery, the one-per-hour per-account limit, the first-sync
  limit, valid-file skip, durable pending work, restart pacing, cancellation,
  malformed or failed download, and isolation from all non-FIT families.
- Failure tests cover endpoint failure, schema drift, network failure, Recorder
  incompatibility, Recorder write failure, Store failure, FIT failure, archive
  HTTP 429, and genuine authentication failure. Each test observes the public
  Archive Status and verifies the intended current-value behavior.
- One test uses the real shared request-control implementation to prove that a
  foreground current-value request gains priority between archive requests
  during prospective work. Tests do not require preemption inside a single
  already-running Garmin request.
- Timestamp tests cover an aware source timestamp at UTC+2 displayed by Home
  Assistant at UTC+8, the UTC+08:00 date-summary bucket across a UTC-year
  boundary, distinct Source Calendar Date metadata, cross-midnight samples,
  daylight-saving boundaries, travel offsets, equivalent-offset spellings of
  one instant, and rejection or explicit handling of naive values.
- Recorder verification includes a scratch Recorder query through the archive
  for absolute instant identity, revision upsert, restart, and provenance
  confirmation, without requiring a live Home Assistant instance.
  seam, proving that archived numeric values remain queryable by their absolute
  instant across restart and upsert.
- Calendar verification uses archive and Home Assistant Calendar behavior to
  prove that structured sleep, activity, and health records remain queryable
  when automatic archival is disabled.
- Status tests assert the exact five states, allowed public fields, transition
  notification, safe error types, and absence of health payloads or internal
  reconciliation and FIT bookkeeping.
- Manual Repair tests prove that exactly 31 inclusive days are accepted, 32 are
  rejected, pre-activation dates are allowed, settled dates can be reopened,
  operation while disabled succeeds, and no enablement or activation state
  changes.
- Through the secondary config-entry lifecycle seam, thin tests exercise
  options submission, entry reload, setup, unload, and restart. They observe
  default-off behavior, first activation, disablement, re-enablement with a new
  date, retained archive identity, conditional prospective task lifecycle, and
  the absence of Historical Backfill startup.
- Thin Home Assistant adapter tests cover the existing Manual Repair service,
  Archive Status entity, read-only Calendars, diagnostics privacy, and retained
  query visibility while automatic archival is disabled. These adapters do not
  become a third high-level seam.
- Existing test patterns for archive lifecycle adapters, fake Store and
  Recorder compatibility, persistent synchronization harnesses, deterministic
  request clocks, statistics normalization, Calendar queries, FIT atomic
  writes, service validation, diagnostics redaction, frozen fixtures, and
  release privacy snapshots are reused.
- Existing dormant Historical Backfill scheduler tests remain green but do not
  define prospective product behavior. Existing current-value coordinator,
  sensor, migration, entity-ID, action, diagnostics, privacy, and multi-account
  regression suites also remain green.

## Out of Scope

- Automatic retrieval of data before the Archive Activation Date.
- Automatic filling of an interval during which Archive Enablement was off.
- Any beta option, startup path, or operator action that starts the dormant
  Historical Backfill scheduler.
- Forensic completeness, guaranteed gap-free history, or guaranteed recovery
  of Garmin data uploaded after a date has settled.
- Hard real-time delivery or a strict service-level guarantee for the nominal
  fifteen-minute freshness target.
- Interpretation of health state, longitudinal trend analysis, correlation,
  anomaly detection, reproduction of Garmin baselines, or any other External
  Analysis.
- Intentional downsampling, representative sampling, or thinning of valid
  Source Records in the Frozen Archive Catalog.
- Training-readiness history, weight and body composition, hydration, blood
  pressure, nutrition, menstrual data, ECG, skin temperature, and glucose.
- A custom archive export API, direct exposure of private Store documents, or a
  replacement for Home Assistant statistics and Calendars.
- A per-sample original timezone-offset sidecar or custom archive display
  timezone.
- Automatic expiry, storage quotas, archive compaction based on age, archive
  deletion, or a bulk-clear action.
- Changing Garmin source records, editing archived Garmin data through Home
  Assistant, or fabricating values to conceal Continuity Gaps.
- Making internal reconciliation fingerprints, family-presence state,
  confirmation attempts, request counts, or FIT queue details public.
- Removing the existing tested Historical Backfill implementation solely to
  simplify the beta.
- Publishing a release, creating a tag, or changing the beta version as part of
  this specification.

## Further Notes

- This specification supersedes GARMIN-1, the old automatic-backfill and fixed
  `2026-07-24` canary specification. Release documentation must no longer
  direct operators from a fixed canary into full automatic backfill.
- The replacement release flow is: create a Home Assistant backup, upgrade and
  restart the integration, verify existing current-value entities, explicitly
  enable the Prospective Archive, validate the bounded current-day result,
  restart again to verify persistence, and observe privacy-safe status and
  storage growth. Disablement is the reversible stop mechanism.
- Existing automatic-backfill code and tests may remain, but release readiness
  requires proof that the capability is unreachable from normal beta product
  paths.
- The archive is intended to run for years. Estimated storage on the order of
  one to two gigabytes is acceptable and does not justify automatic expiry for
  this beta.
- Garmin upload timing is outside the integration's control. The reconciliation
  model assumes uploads are often batch-like, while accepting that some data
  will never arrive or may arrive after settlement.
- The archive's responsibility ends when faithful Source Records and
  Garmin-computed summaries become durable and queryable. Separate programs
  consume those records for long-term health review.
- Home Assistant stores statistics by absolute timestamp and chooses Display
  Time using the browser or server timezone preference. For example, a Source
  Instant expressed as 10:00 at UTC+2 is the same instant as 08:00 UTC and is
  displayed as 16:00 when Home Assistant uses UTC+8.
- The current implementation is not release-ready because normal startup still
  starts legacy Historical Backfill and lacks Archive Enablement, Archive
  Activation Date, Open and Settled Archive Dates, the accepted status
  contract, hourly FIT pacing, and prospective failure isolation.
- No prototype was required. The accepted design can be verified through the
  two agreed high-level seams without adding a new public archive interface.
