# Garmin Connect 3.1.0-beta.6

> **Historical release record.** This document describes `3.1.0-beta.6` and is
> not current installation guidance. Use the
> [3.1.0-beta.14 release guide](release-3.1.0-beta.14.md) for the current beta.

This release removes the archive-startup timeout that could mark History Status
as failed while Home Assistant Recorder was still completing its normal queue
confirmation.

Current-value coordinators now finish setup immediately. The optional archive
then completes its Recorder check and initial sync in a managed background task,
even when that takes several minutes. Config-entry reload and unload cancel and
clean up this task, so an obsolete archive cannot continue after settings change.

Upgrade to `3.1.0-beta.6` and restart Home Assistant. The expected startup log
contains the normal Garmin coordinator setup; it must not contain `Garmin history
archive startup timed out`. If a later archive error appears, enable debug
capture and retain the local replay session under
`<config>/tmp/garmin_connect/<entry-id>/capture-*/` for diagnosis without extra
Garmin requests.
