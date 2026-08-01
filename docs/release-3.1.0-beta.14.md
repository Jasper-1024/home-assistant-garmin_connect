# Garmin Connect 3.1.0-beta.14

This release adds Source Calendar Date storage for prospective Garmin history.
HRV, training status and workload, sleep scores and sleep need, fitness age,
and daily stress Source Records are checkpointed in private annual
Home Assistant Store partitions before their numeric fields are projected to
Recorder statistics. A failed projection is retried from Store without another
Garmin request. Empty later responses cannot erase a valid Source Record.

Compared with beta.13, this candidate preserves the live Garmin response's
independent sleep-need, VO2-max, and per-device load-balance sources. Missing,
null, empty, malformed, and present source states remain distinct. Per-source
metrics have independent Recorder statistic IDs, and Store updates are
copy-on-write so a failed save cannot expose an undurable revision. Training
polls use Home Assistant's configured local date.

Current-value entities remain useful as latest-value views. Daily snapshot
entities no longer claim sync-time long-term statistics where Source Calendar
Date statistics are now canonical, and large training/HRV attributes are bounded.
Eight endpoint families that this device/account does not support remain
available for compatibility but are disabled by default for new installations.
Existing entity-registry choices are not changed automatically.

The training coordinator now requests only training status, HRV, and
power-to-weight/FTP data. It no longer polls readiness, morning readiness,
lactate threshold, endurance score, or hill score endpoints. The Prospective
Archive cadence and existing UTC+8 Canonical Date-Summary Bucket Instant remain
unchanged.

Upgrade to `3.1.0-beta.14`, then restart Home Assistant from its control panel.
Enable Prospective Archive if it is not already enabled. After Garmin has
synced and one archive cycle has completed, verify that Archive Status returns
to `idle`, supported current entities retain values, and Recorder statistics
whose IDs begin with `garmin_connect:` contain Source Calendar Date HRV, sleep,
stress, fitness-age, and training points.

Daily status Source Records are retained in files named
`garmin_connect.<entry-id>.daily_status_<year>` under Home Assistant private
storage. This release intentionally provides no public REST, WebSocket, or
service query over those private records.
