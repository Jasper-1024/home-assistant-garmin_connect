# Garmin Connect Archive

This context defines how Garmin data becomes durable Home Assistant history
without turning normal operation into an unbounded historical import.

## Language

**Archive Activation Date**:
The local calendar date on which an account most recently enabled or resumed
automatic archival. Earlier disabled intervals never become automatically
eligible.
_Avoid_: Backfill start, installation date

**Archive Enablement**:
The operator action that starts or resumes a Prospective Archive and
establishes a new Archive Activation Date.
_Avoid_: Upgrade, integration startup

**Archive Disablement**:
The operator action that stops future automatic archive requests without
deleting Source Records, disabling Manual Repair, or changing the visibility
setting of individual entities.
_Avoid_: Entity disablement, archive deletion

**Prospective Archive**:
The best-effort durable record of supported Garmin data from the Archive
Activation Date forward, intended for longitudinal analysis rather than
forensic completeness.
_Avoid_: Full backfill, historical import

**Frozen Archive Catalog**:
The explicitly supported set of data families whose capture, normalization,
storage, and privacy behavior are release-gated together.
_Avoid_: All Garmin data, every available metric

**Capture Completeness**:
Every valid source record returned for the Frozen Archive Catalog is retained
without intentional thinning; unavoidable source or transport gaps remain
explicit.
_Avoid_: Best sample, representative subset

**Source Record**:
A timestamped measurement, structured event, activity, FIT file, or
Garmin-computed summary returned for the Frozen Archive Catalog.
_Avoid_: Analysis result

**Source Instant**:
The absolute moment represented by an aware Garmin timestamp. Its UTC identity
is preserved even when Home Assistant displays another timezone, while the
source offset spelling is not part of statistics identity.
_Avoid_: Display time, wall-clock text

**Source Calendar Date**:
The source-local calendar date associated with a Source Record, stored
separately from its Source Instant. It comes from an explicit Garmin calendar
date or source-local timestamp such as `startTimeLocal`. A request date may
supply it only for a date-scoped source whose contract makes that date
authoritative. An unscoped feed must never stamp records with its caller's
target date. If source-local date provenance is genuinely absent, the explicit
conservative degradation is the UTC date of the aware Source Instant.
_Avoid_: Display date, target date stamped onto an unscoped feed

**Canonical Date-Summary Bucket Instant**:
The deterministic instant assigned only to a daily summary or training-status
record that supplies a calendar date but no aware Garmin timestamp: 00:00 at
UTC+08:00, normalized to UTC for Recorder identity. It is not a Source Instant;
the Source Calendar Date remains separate provenance.
_Avoid_: Garmin-provided timestamp, inferred intraday measurement

**Display Time**:
Home Assistant's user-facing projection of a Source Instant into the selected
browser or server timezone.
_Avoid_: Source timestamp

**Deferred Data Family**:
A Garmin data family outside the Frozen Archive Catalog for the current
release, without implying that Garmin or the integration can never support it.
_Avoid_: Unsupported metric

**Archive Freshness**:
The elapsed time between data becoming available in Garmin Connect and that
data becoming queryable in Home Assistant history. The normal target is about
fifteen minutes; it is not measurement-to-cloud latency or a guarantee
during Garmin delay, rate limiting, network failure, or Home Assistant downtime.
_Avoid_: Real-time sync, sensor latency

**Continuity Gap**:
An interval for which no source measurement was archived. A gap remains
missing and is never converted into zero or a fabricated value.
_Avoid_: Zero reading, failed trend

**External Analysis**:
Interpretation, trend detection, correlation, or model-derived health insight
performed outside this integration using archived Source Records.
_Avoid_: Archive synchronization

**Archive Query Surfaces**:
Home Assistant statistics for numeric Source Records, read-only Calendars for
structured summaries, and archived FIT files for activity detail.
_Avoid_: Custom export API, direct Store access

**Archive Retention**:
Source Records remain durable without automatic expiry until an operator
explicitly removes them outside normal archival operation.
_Avoid_: Rolling retention, automatic cleanup

**Archive Failure Isolation**:
An archive-source, storage, schema, or rate-limit failure that pauses archival
without stopping existing current-value entities; account authentication
failure is the exception.
_Avoid_: Integration outage

**Archive Status**:
The minimal operator-facing archive state plus activation date, last success,
next run, and a safe error type. Internal settlement and queue bookkeeping are
not part of this public contract.
_Avoid_: Debug ledger, health-data report

**Historical Backfill**:
Retrieval of Garmin data dated before the Archive Activation Date. It is never
started automatically as part of normal prospective archival.
_Avoid_: Initial sync

**Reconciliation Window**:
The nominal rolling seven-day period in which an unsettled Garmin date remains
eligible for automatic checking so delayed uploads can arrive.
_Avoid_: Backfill range, retry history

**Open Archive Date**:
A date still eligible for automatic Garmin requests because it is current,
missing expected families, or awaiting one stability confirmation.
_Avoid_: Incomplete row

**Settled Archive Date**:
A date that no longer receives automatic Garmin requests after one unchanged
confirmation, or after remaining empty through its Reconciliation Window.
_Avoid_: Completed date, immutable date

**Manual Repair**:
An operator-requested synchronization of a specific date or a range of at most
31 days, including dates before Archive Activation, which can reopen a Settled
Archive Date without enabling automatic backfill.
_Avoid_: Automatic backfill
