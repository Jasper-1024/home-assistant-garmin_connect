# Garmin Connect 3.1.0-beta.10

> **Historical release record.** This document describes `3.1.0-beta.10` and is
> not current installation guidance. Use the
> [documentation index](README.md) to find the current release guide.

This release fixes the beta.8-to-beta.9 Store migration. Beta.8 catalogs may
contain reconciliation bookkeeping for the retired segmented `floors` family.
Beta.9 rejected that otherwise valid catalog before issuing any archive
requests and reported `store_initialization`.

Beta.10 recognizes that specific retired family, removes its bookkeeping on
the next Store save, and preserves all Recorder statistics, structured archive
records, FIT files, and active-family reconciliation state. Unknown family
names remain fail-closed.

Upgrade to `3.1.0-beta.10`, then restart Home Assistant from its control panel.
History Status should progress beyond `store_initialization` and begin the
normal archive cycle. The beta.9 response-shape repairs are unchanged.
