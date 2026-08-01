[![GitHub Release][releases-shield]][releases]
[![GitHub Activity][commits-shield]][commits]
[![License][license-shield]](LICENSE)
![Project Maintenance][maintenance-shield]

# Garmin Connect

A maintained Home Assistant custom integration for Garmin Connect, developed at
[Jasper-1024/home-assistant-garmin_connect](https://github.com/Jasper-1024/home-assistant-garmin_connect).
It originated from
[cyberjunky/home-assistant-garmin_connect](https://github.com/cyberjunky/home-assistant-garmin_connect)
and continues to use the upstream
[`ha-garmin`](https://github.com/cyberjunky/ha-garmin) API library.

The integration polls Garmin Connect for current health, activity, training,
body, goal, gear, blood-pressure, menstrual, and nutrition data. An optional
Prospective Archive stores Garmin's source-timestamped history for long-term use
in Home Assistant.

> Upgrading from v1 requires Garmin re-authentication because its tokens are not
> compatible with the current library. The migration preserves existing entity
> IDs where possible.

Current documentation:

- [Documentation index](docs/README.md)
- [Garmin health data model and implementation notes (Chinese)](docs/garmin-health-data-integration.md)
- [3.1.0-beta.14 release guide](docs/release-3.1.0-beta.14.md)

![Garmin Connect integration](https://github.com/Jasper-1024/home-assistant-garmin_connect/blob/main/screenshots/garmin_connect.png?raw=true)

## Installation

### HACS (recommended)

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=Jasper-1024&repository=home-assistant-garmin_connect&category=integration)

If the fork is not listed automatically:

1. Open **HACS → Integrations → menu → Custom repositories**.
2. Add `https://github.com/Jasper-1024/home-assistant-garmin_connect` as an
   **Integration** repository.
3. Find **Garmin Connect**, select the required release, and download it.
4. Restart Home Assistant.
5. Open **Settings → Devices & services → Add integration**, then add
   **Garmin Connect**.

Do not keep two different Garmin Connect custom repositories installed at the
same time. Existing configured accounts belong to the integration domain, not
to a particular HACS repository entry.

### Manual installation

Copy `custom_components/garmin_connect` into
`<config>/custom_components/garmin_connect`, restart Home Assistant, and add the
integration under **Settings → Devices & services**.

### Account setup

Enter the Garmin Connect email and password. Complete MFA when prompted. For an
account registered on Garmin's Chinese platform, enable **Use Garmin Connect
China (cn.garmin.com)** during setup.

## Configuration options

Use the integration's **Configure** button after setup.

| Option | Default | Behavior |
| --- | --- | --- |
| Update interval | 900 seconds (15 minutes) | Polling interval, from 60 to 3600 seconds |
| Use Garmin Connect China | Account setting | Uses `cn.garmin.com`; enable only for a China-region account |
| Enable prospective archive | Off | Starts durable collection from the activation date forward |
| Capture complete requests and responses | Off | Saves full local HTTP sessions for debugging |
| Replay captured HTTP session | Empty | Replays one saved session without Garmin network requests |

Capture and replay cannot be enabled together. Changing account region or
credentials may require re-authentication.

## What Home Assistant receives

Garmin data has several different shapes. The integration does not force all of
them into ordinary entity history.

| Surface | Purpose | Examples |
| --- | --- | --- |
| Sensors | Latest value, today's total, or current Garmin summary | Current Body Battery, steps, HRV status, sleep score, training status |
| Recorder statistics | Long-term numeric history at Garmin source times | Heart rate, stress, Body Battery, HRV, respiration, SpO2, steps, daily status metrics |
| Calendar and FIT | Structured sessions, intervals, and activity detail | Sleep sessions/stages, activities, health events, archived FIT files |
| Private Store | Durable canonical records and projection checkpoints | Annual sleep, activity, daily-status, reconciliation, and FIT metadata |

Sensor history records when Home Assistant observed a latest value. It is not a
replacement for Garmin's older source-timestamped samples. Archive statistics
are written separately so a delayed watch or phone sync can retain the original
measurement time without changing the current sensor state.

Private Store files are implementation storage. There is no public REST,
WebSocket, service, or direct query API for them. Use entities, Calendar,
Recorder statistics, and archived FIT files as the supported Home Assistant
surfaces.

## Data coverage

Current-value sensors include these broad families when the account and device
provide them:

- Daily activity: steps, distance, floors, calories, intensity, and solar data
- Health: heart rate, stress, Body Battery, respiration, SpO2, sleep, and HRV
- Body: weight, BMI, body composition, hydration, and fitness age
- Activities: recent activities, routes, workouts, alarms, gear, goals, badges,
  and points
- Training: training status, HRV, VO2 max, power-to-weight, and FTP
- Optional data: blood pressure, menstrual tracking, and Connect+ nutrition

Menstrual and nutrition entities are disabled by default. Availability always
depends on the Garmin account, device capabilities, subscriptions, and whether
the device has synced to Garmin Connect.

### Training compatibility entities

Normal Training polling intentionally requests only:

- training status, including Garmin's available load and VO2 summaries;
- HRV status and nightly/baseline summaries;
- power-to-weight and FTP data.

Training readiness, morning training readiness, recovery time, lactate
threshold, endurance score, and hill score entities remain for compatibility.
They are disabled by default for new installations and their known-empty
endpoint families are not requested during normal polling. Existing entity
registry choices are not changed automatically.

## Data updates

The default poll interval is 900 seconds (15 minutes). This is a cadence, not a
freshness guarantee: the watch must first sync to Garmin Connect, and Garmin
must make the data available. Independent coordinators isolate current-value
domains, so a failure in one domain does not erase valid values from another.

| Coordinator | Active polling |
| --- | --- |
| Core | Daily summary, heart rate, stress, sleep, Body Battery, SpO2, respiration, and intensity |
| Activity | Latest and recent activities, route, and workouts |
| Training | Training status, HRV, VO2 max, power-to-weight, and FTP |
| Body | Weight, body composition, hydration, and fitness age |
| Goals | Goals, badges, and points |
| Gear | Gear statistics and alarms |
| Blood Pressure | Latest blood-pressure measurement |
| Menstrual | Menstrual tracking when available |
| Nutrition | Connect+ nutrition data; entities disabled by default |

## Prospective Archive

Prospective Archive is **off by default**. Enabling it creates an activation
date, performs one bounded current-day sync, and then runs nominally every 15
minutes. Archive work uses background priority and does not block current-value
coordinators or Home Assistant startup. A failed archive run is reported by the
Archive Status entity and retried/backed off without deleting current values.

Archive behavior:

- Collection starts at enablement and proceeds forward. It does not
  automatically import the previous year or any other full historical range.
- The current Home Assistant local date remains open. Older eligible dates are
  reconciled only inside a nominal seven-day window and never before activation.
- Reconciliation checks local persisted state first. A settled date does not
  generate another automatic Garmin request.
- Empty, incomplete, or failed dates can be revisited during the window. A real
  zero remains zero; missing data is never synthesized.
- Every valid returned source record is retained. The archive does not
  intentionally thin or sample data.
- Disabling the option stops future automatic work. It does not delete Recorder
  statistics, Store records, Calendar records, or FIT files.
- The integration provides no archive-deletion action and no built-in retention period.
  Administrators must manage storage outside the integration.

Use `garmin_connect.sync_history` for a manual repair of one date or an
inclusive range of at most 31 days. Manual repair can target a date before
activation and can reopen a settled date. It does not enable Prospective
Archive, change its activation date, or start recurring work.

Create a Home Assistant backup that includes the configuration directory and
Recorder database before first enabling the archive. See the
[beta.14 release guide](docs/release-3.1.0-beta.14.md) for upgrade, validation,
storage, and downgrade guidance.

## Diagnostic and history actions

These read/repair actions are separate from actions that write data to Garmin.
All support response data and can be called from **Developer tools → Actions**.

### `garmin_connect.sync_history`

Imports the supported Frozen Archive Catalog for one date or a bounded range,
including numeric series, structured records, daily status, and eligible FIT
work returned for that range. Supply either `date`, or both `start_date` and
`end_date`; a range is limited to 31 inclusive days.

```yaml
action: garmin_connect.sync_history
data:
  start_date: "2026-07-25"
  end_date: "2026-08-01"
response_variable: result
```

Returned inserted/updated/skipped values are adapter bookkeeping, not an
authoritative Recorder database audit.

### `garmin_connect.probe_intraday`

Reads metadata and boundary samples for heart rate, stress, Body Battery, HRV,
or all four. It does not persist the returned data.

```yaml
action: garmin_connect.probe_intraday
data:
  date: "2026-08-01"
  metric: all
response_variable: probe
```

### `garmin_connect.probe_capability`

Makes exactly one read-only Garmin request for a selected data family. It
returns field structure and collection sizes, not scalar health values. This is
for compatibility diagnosis, not routine polling.

```yaml
action: garmin_connect.probe_capability
data:
  probe: sleep
  date: "2026-08-01"
response_variable: capability
```

With multiple Garmin accounts, select one with `entity_id`. Otherwise the
action requires an unambiguous loaded account.

## Raw HTTP capture and offline replay

For local debugging, enable **Capture complete Garmin HTTP requests and
responses**, reproduce the issue once, then disable it. Sessions are stored at:

```text
<config>/tmp/garmin_connect/<entry-id>/capture-*/
```

Each session contains a JSONL manifest, complete decoded JSON responses, and
binary downloads such as FIT files. Authentication headers, cookies, and DI
tokens are not captured, but the files still contain private health data. Keep
them on the Home Assistant host, restrict access, and redact them before sharing.
There is no automatic expiry or deletion.

To debug without further Garmin requests, enter one `capture-*` directory name
in **Replay captured HTTP session**. Replay fails closed if code requests an
operation absent from the recording. Disable capture while replaying. Enable
`custom_components.garmin_connect: debug` to log capture/replay sequencing.

## Activity route map

The `Last Activity Route` sensor provides a `polyline` attribute when the most
recent activity has GPS data. To use the included Lovelace card:

1. Copy `garmin-polyline-card.js`, `leaflet.js`, and `leaflet.css` from `www/`
   to `<config>/www/`.
2. Add `/local/garmin-polyline-card.js` as a JavaScript Module under
   **Settings → Dashboards → Resources**.
3. Hard-refresh the browser and add:

```yaml
type: custom:garmin-polyline-card
entity: sensor.YOUR_PREFIX_last_activity_route
attribute: polyline
title: Last Activity Route
height: 400px
color: "#FF5722"
```

Find the actual entity ID under the Garmin Connect device; account-based entity
prefixes may differ.

## Actions that write to Garmin

These actions modify Garmin Connect data. The optional `entity_id` selects the
account when more than one is configured.

| Action | Purpose |
| --- | --- |
| `set_active_gear` | Set or unset default gear for an activity type |
| `add_gear_to_activity` | Associate gear with an existing activity |
| `add_body_composition` | Upload weight and optional body-composition fields |
| `add_blood_pressure` | Upload systolic, diastolic, pulse, timestamp, and notes |
| `add_hydration` | Add or subtract hydration in millilitres |
| `add_nutrition_log` | Add a Connect+ Quick Add nutrition entry |
| `create_activity` | Create a manual Garmin activity |
| `upload_activity` | Upload a FIT, GPX, or TCX activity file |
| `download_activity` | Download an activity file to the Home Assistant host |

### Body composition

`weight` is required in kilograms. Optional fields include `timestamp`, `bmi`,
`percent_fat`, `percent_hydration`, `visceral_fat_mass`, `bone_mass`,
`muscle_mass`, `basal_met`, `active_met`, `physique_rating`, `metabolic_age`,
and `visceral_fat_rating`.

```yaml
action: garmin_connect.add_body_composition
data:
  entity_id: sensor.garmin_connect_weight
  weight: 82.3
  percent_fat: 23.6
  muscle_mass: 35.5
```

### Blood pressure

```yaml
action: garmin_connect.add_blood_pressure
data:
  entity_id: sensor.garmin_connect_blood_pressure_systolic
  systolic: 120
  diastolic: 80
  pulse: 60
  notes: "Morning measurement"
```

### Hydration and nutrition

Use a negative `value_in_ml` to subtract hydration from today's total.
Nutrition Quick Add requires Garmin Connect+ and nutrition setup in the app.

```yaml
action: garmin_connect.add_hydration
data:
  entity_id: sensor.garmin_connect_hydration
  value_in_ml: 250
```

```yaml
action: garmin_connect.add_nutrition_log
data:
  entity_id: sensor.garmin_connect_calories
  calories: 650
  carbs: 80
  protein: 35
  fat: 20
  name: "Lunch"
```

### Manual activity and files

```yaml
action: garmin_connect.create_activity
data:
  entity_id: sensor.garmin_connect_last_activity
  activity_name: "Morning Run"
  activity_type: running
  duration_min: 30
  distance_km: 5.0
```

```yaml
action: garmin_connect.upload_activity
data:
  entity_id: sensor.garmin_connect_last_activity
  file_path: "activities/morning_run.fit"
```

`download_activity` supports `fit`, `original`, `tcx`, `gpx`, `kml`, and `csv`.
Custom destinations must be in `allowlist_external_dirs`; the default is
`<config>/garmin_activities/activity_<id>.<format>`.

```yaml
action: garmin_connect.download_activity
data:
  activity_id: 23545484677
  file_format: gpx
response_variable: download
```

### Gear

`set_active_gear` accepts either `gear_uuid` or a gear sensor `entity_id` plus
an `activity_type`. `add_gear_to_activity` accepts an `activity_id` and either
gear selector.

```yaml
action: garmin_connect.set_active_gear
data:
  entity_id: sensor.garmin_connect_my_running_shoes
  activity_type: running
  setting: "set this as default, unset others"
```

```yaml
action: garmin_connect.add_gear_to_activity
data:
  activity_id: 12345678901
  entity_id: sensor.garmin_connect_my_running_shoes
```

## Migration from v1

The v1-to-v2 migration rewrites entity unique IDs from the old email-based
format to config-entry-based IDs. It then requests re-authentication because
the old OAuth tokens are incompatible. Complete re-authentication for every
configured account. Review renamed or removed entities before relying on old
dashboards and automations.

## Troubleshooting

### Unknown or unavailable values

- Confirm the watch has synced and the Garmin app or website shows the value.
- Wait for the next poll; a 15-minute integration interval starts only after
  Garmin receives the device data.
- Confirm the account/device supports the data family. Missing data is not
  converted to zero.
- For training data, verify the entity is part of the supported polling set;
  compatibility entities may remain unavailable by design.
- Check the Garmin coordinator and archive log messages independently. Archive
  failure does not imply that all current-value coordinators failed.

### Authentication and rate limiting

Use **Reconfigure** to enter credentials and MFA again. For HTTP 429 or login
rate-limit errors, stop manual reloads and wait before retrying. A 60-second
minimum scan interval is available but increases rate-limit risk.

### Debug logging

```yaml
logger:
  default: info
  logs:
    custom_components.garmin_connect: debug
```

Or use **Enable debug logging** from the integration page. Reproduce the issue,
then disable debug logging. Full request/response data appears only when the
separate capture option is enabled.

## Support

GitHub Issues and Discussions are disabled on this fork. Plane is not a public
support endpoint. Submit public feedback through a pull request to
[Jasper-1024/home-assistant-garmin_connect](https://github.com/Jasper-1024/home-assistant-garmin_connect/pulls).
Fork this repository, create a branch, and add a redacted
`docs/feedback/<topic>.md` file using the
[feedback template](docs/feedback/README.md). Do not include passwords, tokens,
or raw health data, and never attach raw capture sessions publicly.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).

[releases-shield]: https://img.shields.io/github/release/Jasper-1024/home-assistant-garmin_connect.svg?style=for-the-badge
[releases]: https://github.com/Jasper-1024/home-assistant-garmin_connect/releases
[commits-shield]: https://img.shields.io/github/commit-activity/y/Jasper-1024/home-assistant-garmin_connect.svg?style=for-the-badge
[commits]: https://github.com/Jasper-1024/home-assistant-garmin_connect/commits/main
[license-shield]: https://img.shields.io/github/license/Jasper-1024/home-assistant-garmin_connect.svg?style=for-the-badge
[maintenance-shield]: https://img.shields.io/badge/maintainer-Jasper--1024-blue.svg?style=for-the-badge
