# Retain but gate historical backfill

The tested backfill scheduler remains in the codebase to avoid deletion-driven
regressions and preserve a future capability, but the beta product exposes no
automatic historical-backfill entry and normal integration startup can never
start it. Operators retain only bounded manual repair; prospective archival is
the sole automatic synchronization mode. Manual repair may explicitly target
up to 31 days, including dates before archive activation, without changing the
activation date or enabling the scheduler. Manual repair remains available
while prospective archival is disabled, subject to the same compatibility,
request-gate, backoff, and privacy safeguards.
