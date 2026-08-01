# Documentation index

Use this index to distinguish current operating guidance from research and
historical design records.

## Current user documentation

- [Project README](../README.md) — installation, configuration, normal use,
  and troubleshooting.
- [Garmin health data integration guide](garmin-health-data-integration.md) —
  current data model, storage behavior, and implementation status (Chinese).
- [3.1.0-beta.14 release guide](release-3.1.0-beta.14.md) — current beta
  upgrade and validation notes.
- [Public feedback](feedback/README.md) — privacy-safe feedback by pull request.

## Architecture and decisions

- [Domain context](../CONTEXT.md) — archive terminology and invariants.
- Architecture decision records:
  [prospective archiving](adr/0001-prefer-prospective-archiving.md),
  [explicit enablement](adr/0002-require-explicit-archive-enablement.md),
  [external analysis](adr/0003-separate-archival-from-analysis.md),
  [gated backfill](adr/0004-retain-but-gate-historical-backfill.md),
  [failure isolation](adr/0005-isolate-archive-failures.md),
  [retention](adr/0006-retain-archive-until-explicit-deletion.md), and
  [timestamp handling](adr/0007-store-source-instants-and-delegate-display-timezone.md).

## Research evidence

- [First-batch probe result](garmin-first-batch-probe-result.md)
- [Garmin rules and forum feedback](garmin-forum-feedback.md)
- [High-resolution Home Assistant history research](ha-high-resolution-history-research.md)
- [Timezone, Recorder, and Garmin timestamp research](ha-timezone-storage-display-research.md)

These documents preserve evidence and experiments. For current behavior, use
the health data integration guide above.

## Historical design records

- [Prospective Archive specification](prospective-archive-spec.md)
- [Prospective Archive implementation plan](prospective-archive-implementation-plan.md)

These records explain why the archive was designed this way. The design is now
implemented; they are not current installation or release instructions.

## Historical release notes

- [beta.1](release-3.1.0-beta.1.md),
  [beta.2](release-3.1.0-beta.2.md),
  [beta.3](release-3.1.0-beta.3.md),
  [beta.4](release-3.1.0-beta.4.md),
  [beta.5](release-3.1.0-beta.5.md),
  [beta.6](release-3.1.0-beta.6.md),
  [beta.7](release-3.1.0-beta.7.md),
  [beta.8](release-3.1.0-beta.8.md),
  [beta.9](release-3.1.0-beta.9.md),
  [beta.10](release-3.1.0-beta.10.md),
  [beta.11](release-3.1.0-beta.11.md), and
  [beta.12](release-3.1.0-beta.12.md).

Each file records its own release only. Use the beta.14 guide for the current
candidate.

## Contributor documentation

- [Domain documentation workflow](agents/domain.md)
- [Plane issue tracker](agents/issue-tracker.md)
- [Triage labels](agents/triage-labels.md)
- [HACS repository metadata](garmin_connect.markdown)
