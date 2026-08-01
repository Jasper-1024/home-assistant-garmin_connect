# Garmin Connect 3.1.0-beta.7

> **Historical release record.** This document describes `3.1.0-beta.7` and is
> not current installation guidance. Use the
> [3.1.0-beta.14 release guide](release-3.1.0-beta.14.md) for the current beta.

This release extends the optional Recorder queue-confirmation window for Home
Assistant startup recovery. After an unclean shutdown, Recorder may need more
than one minute before it dispatches queued work; the archive now permits five
minutes without queue progress and fifteen minutes in total.

Current-value Garmin coordinators remain independent of this wait. The archive
starts in the managed background task introduced in beta.6, so normal sensors
and Home Assistant startup are not delayed.

Upgrade to `3.1.0-beta.7` and restart Home Assistant. A recovering Recorder may
take several minutes before History Status becomes ready, but it must not change
to `recorder_barrier` after only one minute.
