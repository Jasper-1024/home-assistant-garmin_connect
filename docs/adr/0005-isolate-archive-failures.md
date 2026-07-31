# Isolate archive failures from current-value polling

Prospective archival is optional and must not make established Garmin entities
stale. Archive rate limits, endpoint failures, schema drift, Recorder problems,
and Store failures therefore pause or disable only archival even though both
paths share serialized request control; only account authentication failure
may escalate to account-wide reauthentication.
