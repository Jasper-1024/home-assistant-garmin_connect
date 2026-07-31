# Require explicit archive enablement

Upgrading the integration must not silently increase private Garmin requests,
Recorder growth, or FIT storage. Prospective archival therefore remains off
until the operator explicitly enables it, at which point the activation date
is persisted and normal archival begins. Disabling preserves all archived data
but stops requests; enabling again establishes a new activation date and never
automatically fills the disabled interval. Enablement is a persistent config
entry option rather than a one-shot action, and changing it reloads the entry
without deleting Recorder, Store, or FIT data. Individual entity enablement
controls presentation only and never starts or stops archival.
Archive Disablement stops collection only: existing statistics, Calendar
records, FIT files, and privacy-safe status remain queryable.
