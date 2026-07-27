# AGENTS.md

Guidance for Codex and other coding agents working in this repository.

## Communication style

Use terse, direct communication. Fragments are acceptable. Keep technical terms
exact and code unchanged. Prefer: `[thing] [action] [reason]. [next step].`

## Commands

```bash
scripts/setup       # Install dependencies and pre-commit hooks
scripts/test        # Run pytest with coverage (pass extra args after --)
scripts/lint        # Run pre-commit + vulture dead-code check
scripts/develop     # Start local Home Assistant with the integration loaded
```

Run a single test file:

```bash
pytest tests/test_sensor.py -v
```

## Architecture

This is a Home Assistant custom integration that polls the Garmin Connect cloud
API via the [`ha-garmin`](https://github.com/cyberjunky/ha-garmin) library. The
library is the existing Garmin API dependency.

### Data flow

Each data domain has its own `DataUpdateCoordinator` subclass in
`custom_components/garmin_connect/coordinator.py`. All coordinators share the
same `GarminClient` and `GarminAuth` instances, and each calls one
`client.fetch_*_data()` method per poll:

| Coordinator | Data |
| --- | --- |
| `CoreCoordinator` | Daily summary, steps, sleep, HR, stress, SpO2, body battery (~50 sensors) |
| `ActivityCoordinator` | Last activity, last 10 activities, polyline, workouts (~5 sensors) |
| `TrainingCoordinator` | Readiness, VO2max, HRV, training status, scores (~11 sensors) |
| `BodyCoordinator` | Weight, BMI, hydration, fitness age, body composition (~17 sensors) |
| `GoalsCoordinator` | Badges, points, active goals (~6 sensors) |
| `GearCoordinator` | Gear stats (dynamic sensors per item), alarms |
| `BloodPressureCoordinator` | Latest BP reading (~3 sensors) |
| `MenstrualCoordinator` | Menstrual data (~9 sensors, disabled by default) |
| `NutritionCoordinator` | Consumed macros, goals, per-meal breakdown (~11 sensors, disabled by default, Connect+) |

### Sensor definitions

All sensors are declared as `GarminConnectSensorEntityDescription` tuples in
`custom_components/garmin_connect/sensor.py`, grouped by coordinator. Each
description has:

- `coordinator_type` — coordinator that feeds it
- `value_fn` — extracts state from coordinator data; falls back to `key`
- `attributes_fn` — extracts extra state attributes
- `preserve_value=True` — retains the last non-`None` value

### Key data facts from `ha-garmin`

- `startTimeLocal` is dropped by the library; use `startTime` (UTC datetime).
- `activityType` is simplified to a plain string such as `running` or `cycling`.
- `polyline` lives on `lastActivityRoute`, not `lastActivity`.

### Custom Lovelace card

`www/garmin-polyline-card.js` renders activity routes using Leaflet. Users must
copy all three files from `www/` to `<config>/www/`: the card JavaScript,
`leaflet.js`, and `leaflet.css`.

### Entity unique IDs

Format: `{entry_id}_{sensor_key}`. The v1-to-v2 migration in
`custom_components/garmin_connect/__init__.py` rewrites unique IDs from the old
`{email}_{key}` format and triggers reauthentication because tokens are
incompatible between versions.

## Agent skills

### Issue tracker

Work is tracked in Plane project `GARMIN` using the `plane-ops` skill. See
`docs/agents/issue-tracker.md`.

### Triage labels

Plane uses the five canonical triage labels without renaming. See
`docs/agents/triage-labels.md`.

### Domain docs

This is a single-context repository using root `CONTEXT.md` and `docs/adr/`.
See `docs/agents/domain.md`.
