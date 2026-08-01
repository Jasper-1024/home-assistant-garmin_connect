# Garmin Connect 3.1.0-beta.9

This release repairs history parsing against the complete local HTTP capture
produced by beta.8. Development and regression tests replay sanitized captured
shapes and do not add Garmin requests.

Changes:

- unwrap the current `dailySleepDTO` response and retain all seven captured
  sleep streams;
- merge Body Battery event samples into Body Battery and stress history while
  reusing the same response for the health-event Calendar;
- retain every returned training-status device under its direct Garmin device
  ID;
- archive Garmin's daily floors ascended/descended, vertical distances, and
  moderate/vigorous intensity summaries;
- stop requesting the fixed-bucket floors chart during archive sync;
- cache shared same-date intensity, respiration, SpO2, and Body Battery event
  responses within one archive sync.

Upgrade to `3.1.0-beta.9`, then restart Home Assistant from its control panel.
Do not delete the configured Garmin integration or its existing archive.
After Home Assistant reaches `RUNNING`, verify that History Status leaves a
failure state on the next archive cycle. Raw capture/replay remains available
under `<config>/tmp/garmin_connect/<entry-id>/capture-*/` for local diagnosis.
