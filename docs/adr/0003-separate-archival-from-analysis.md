# Separate archival from analysis

The integration faithfully synchronizes and stores every valid raw record and
Garmin-computed summary in the frozen catalog, while preserving explicit gaps.
It does not interpret health state, reproduce Garmin baselines, or implement
long-term analytical models; downstream programs own that analysis so the
archive remains a stable source record rather than an opinionated health model.
Consumers use Home Assistant statistics, the read-only Calendars, and archived
FIT files; this release does not add a parallel export API or expose private
Store documents.
