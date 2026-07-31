# Prefer prospective archiving over automatic historical backfill

The product goal is a best-effort longitudinal Garmin record from the moment
archival is enabled, not forensic completeness or reconstruction of an
arbitrary earlier period. Normal operation therefore archives from a persisted
activation date forward and never automatically requests older data, accepting
occasional explicit gaps in exchange for bounded private-endpoint load. This
best-effort boundary never permits intentional thinning of valid records
returned for the frozen catalog.
