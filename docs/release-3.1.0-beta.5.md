# Garmin Connect 3.1.0-beta.5

> **Historical release record.** This document describes `3.1.0-beta.5` and is
> not current installation guidance. Use the
> [documentation index](README.md) to find the current release guide.

This release repairs Garmin response shapes captured from a live Home Assistant
instance without requesting more Garmin data. History import now accepts the
current Heart Rate, HRV, Steps, and SpO2 payload forms, and Body Battery uses
the upstream endpoint's required `startDate` and `endDate` parameters.

Archive startup may now take up to 60 seconds while Garmin profile and Recorder
initialization complete. It remains an optional background archive and does not
change the normal coordinator polling interval.

Upgrade to `3.1.0-beta.5`, restart Home Assistant, and open **Configure** only
if you need to change capture or replay options. To validate without Garmin
network requests, select the existing capture directory under
`<config>/tmp/garmin_connect/<entry-id>/capture-*/` as the replay session.
