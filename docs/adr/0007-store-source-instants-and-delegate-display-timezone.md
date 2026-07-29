# Store source instants and delegate display timezone

Garmin aware timestamps are normalized without changing their absolute instant,
and Garmin calendar dates remain separate source metadata. Recorder statistics
store the instant as Unix time rather than preserving the original offset
spelling; Home Assistant then renders that instant in the user's selected
browser or server timezone.

`daily_summary` and `training_status` can supply a calendar date without an
aware Garmin timestamp. In that case, the archive uses the explicit canonical
date-summary bucket instant of 00:00 UTC+08:00, normalized to UTC for Recorder
identity. It is derived archive identity, not a Garmin Source Instant, and the
Source Calendar Date is retained separately. When Garmin supplies an aware
timestamp, it always remains the Source Instant. The beta does not add a
per-sample offset sidecar, because it would duplicate Home Assistant time
handling without changing point identity.
