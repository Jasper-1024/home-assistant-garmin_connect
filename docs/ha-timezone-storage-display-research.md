# Home Assistant timezone, Recorder, and Garmin timestamp research

Research date: 2026-07-28
Target: Home Assistant Core 2026.7.4 and current Home Assistant frontend source

## Conclusions

1. Aware timestamps with offsets should be normalized to an absolute instant before importing into Recorder statistics. Home Assistant’s own time helper converts aware datetimes to UTC with `astimezone(UTC)` and converts timestamps with the datetime timestamp value.
2. Recorder statistics store the instant, not the source zone/offset. In Core 2026.7.4, statistics rows use floating-point `start_ts`; `Statistics.from_stats()` writes `stats["start"].timestamp()`. The original `+02:00` offset is therefore not a statistics-row field.
3. Home Assistant’s API/internal time convention is UTC. Frontend formatting then chooses either the user’s browser-local IANA timezone or the configured server timezone, according to the user’s timezone preference.
4. Garmin’s `calendarDate` is a source-local calendar concept, not a substitute for an aware sample timestamp. Use the requested Garmin date for endpoint selection/checkpoint identity, but preserve and normalize each sample’s actual aware timestamp.

## Evidence: Core time handling

### Aware parsing and UTC conversion — fact

Core 2026.7.4 `homeassistant.util.dt.parse_datetime()` explicitly supports timezone offsets and says offset-bearing input produces a timezone-aware datetime with a fixed offset. `as_utc()` returns an aware datetime in UTC, attaching the configured default timezone only when input is naive. `as_timestamp()` ultimately returns the Unix timestamp for the parsed datetime. [Core 2026.7.4 `homeassistant/util/dt.py`](https://raw.githubusercontent.com/home-assistant/core/2026.7.4/homeassistant/util/dt.py#L137-L157) [Core 2026.7.4 `homeassistant/util/dt.py`](https://raw.githubusercontent.com/home-assistant/core/2026.7.4/homeassistant/util/dt.py#L198-L231)

Home Assistant’s official time documentation states that state timestamps such as `last_changed` and `last_updated` are stored in UTC. It also distinguishes configured-local `now()` from UTC `utcnow()`. [Working with dates and times](https://www.home-assistant.io/docs/templating/dates-and-times/#time-zones)

### Recorder statistics storage — fact

The official Recorder data documentation describes `statistics_short_term` and `statistics` as the 5-minute and hourly statistics tables. Their timestamp field is `start_ts`, documented as a `DOUBLE_TYPE`; long-term statistics are hourly aggregates. [Long- and short-term statistics](https://data.home-assistant.io/docs/statistics/#statistics-data-tables)

Core 2026.7.4 defines `TIMESTAMP_TYPE` as a floating-point type and maps `StatisticsBase.start_ts` to that type. `Statistics.from_stats()` stores `stats["start"].timestamp()` in `start_ts`. This is direct source evidence that statistics persist a Unix-time instant rather than a datetime object carrying the original offset. [Core 2026.7.4 `db_schema.py`](https://raw.githubusercontent.com/home-assistant/core/2026.7.4/homeassistant/components/recorder/db_schema.py#L190-L210) [Core 2026.7.4 `db_schema.py`](https://raw.githubusercontent.com/home-assistant/core/2026.7.4/homeassistant/components/recorder/db_schema.py#L587-L624)

The statistics table has a uniqueness index on `(metadata_id, start_ts)`, reinforcing that the time identity of a statistics point is the normalized instant associated with that statistic. [Core 2026.7.4 `db_schema.py`](https://raw.githubusercontent.com/home-assistant/core/2026.7.4/homeassistant/components/recorder/db_schema.py#L649-L663)

### Is the original offset preserved? — fact plus bounded inference

For Recorder statistics: no, not as a source offset. The row stores `start_ts` as a float Unix timestamp, and the schema exposes no per-statistics `utc_offset` column. The original offset may still be present in an integration’s transient input or external archive, but it cannot be reconstructed from the statistics row alone when two offset representations identify the same instant. This conclusion is an inference from the 2026.7.4 schema and write path, not a claim that every other Home Assistant table has identical storage.

Home Assistant’s historical UTC-awareness note documents a `utc_offset` migration for events and states, but explicitly says that information was not used at that time. That note concerns events/states, not the statistics schema; it should not be used to conclude that Recorder statistics retain Garmin’s original offset. [UTC & time zone awareness](https://www.home-assistant.io/blog/2015/05/09/utc-time-zone-awareness/#compatibility)

## Frontend and API display behavior

### API/internal convention — fact

Home Assistant’s official UTC-awareness documentation says internal communication uses UTC and that the API sends times in UTC. Consumers outside the frontend are instructed to handle that convention explicitly. [UTC & time zone awareness](https://www.home-assistant.io/blog/2015/05/09/utc-time-zone-awareness/#backwards-incompatible-stuff)

### Frontend conversion — fact

The current frontend `formatDateTime` functions pass an absolute JavaScript `Date` to `Intl.DateTimeFormat` with a `timeZone` selected by `resolveTimeZone(locale.time_zone, config.time_zone)`. The frontend’s timezone preference has two values: `local` and `server`. [Current frontend `format_date_time.ts`](https://raw.githubusercontent.com/home-assistant/frontend/dev/src/common/datetime/format_date_time.ts#L8-L23) [Current frontend translation/timezone preferences](https://raw.githubusercontent.com/home-assistant/frontend/dev/src/data/translation.ts#L13-L22)

`resolveTimeZone()` uses the browser’s resolved IANA timezone when the user preference is `local`; otherwise it uses the server/configured timezone. If the browser cannot provide a recognized IANA zone, it falls back to the server timezone. [Current frontend `resolve-time-zone.ts`](https://raw.githubusercontent.com/home-assistant/frontend/dev/src/common/datetime/resolve-time-zone.ts#L3-L23)

The frontend also has an explicit `formatDateTimeWithBrowserDefaults()` path that delegates to `Intl.DateTimeFormat` without a `timeZone` option, which means browser defaults apply. The normal HA formatter path is the user-preference-aware path above. [Current frontend `format_date_time.ts`](https://raw.githubusercontent.com/home-assistant/frontend/dev/src/common/datetime/format_date_time.ts#L25-L36)

The frontend developer documentation says formatted entity values are localized using user profile settings, including timezone. [Frontend data — entity state formatting](https://developers.home-assistant.io/docs/frontend/data/#entity-state-formatting)

## Worked UTC+8 / source UTC+2 example

Assume Garmin supplies an aware sample:

```text
2026-07-24T10:00:00+02:00
```

That represents `2026-07-24T08:00:00Z`. Recorder statistics receive the aware datetime and persist its Unix timestamp, so the stored identity is the UTC instant `08:00Z`; the `+02:00` spelling is not a separate statistics field.

If the HA frontend is configured to display in UTC+8, the same instant is displayed as:

```text
2026-07-24 16:00:00+08:00
```

If the user selects frontend `local` and the browser resolves to UTC+8, the result is also 16:00. If the browser resolves to a different zone, the local-preference display follows that browser zone. This example is arithmetic applied to the documented UTC storage/API and frontend timezone rules; it is an inference/example, not a captured HA UI screenshot.

## Garmin integration guidance

### Facts

- Garmin endpoints commonly identify the requested calendar day with `calendarDate` or a date parameter.
- Home Assistant internally/API-facing timestamps are UTC, while Core’s datetime parser accepts offset-bearing ISO 8601 values.
- Recorder statistics identify a point by statistic metadata plus its timestamp instant; duplicate offset spellings of the same instant are not distinct statistics points.

### Recommended integration behavior — inference from those facts

- Treat the Garmin requested `calendarDate` as request/checkpoint metadata only. Do not manufacture a local-midnight timestamp from it for an intraday sample.
- Parse Garmin ISO timestamps with their supplied offset. Keep the aware timestamp through normalization, then pass it to Recorder; Core’s `.timestamp()` path normalizes it to the instant.
- Preserve the original raw timestamp in the integration’s private/debug model only if needed for provenance. Do not expect Recorder statistics or the HA API to preserve that original offset spelling.
- Keep `request_date`/`calendarDate` separate from `timestamp.date()`: a cross-midnight sample can belong to the Garmin request day while its UTC date differs.
- For daily totals intentionally anchored to a calendar day, choose and document an explicit HA timezone boundary; do not assume the Garmin source offset and HA display timezone are the same.
- Avoid comparing naive and aware datetimes. Core’s official guidance requires both sides of a datetime comparison to use the same timezone representation. [Working with dates and times](https://www.home-assistant.io/docs/templating/dates-and-times/#time-zones)

## Scope and limitations

This note covers Home Assistant Core 2026.7.4’s datetime/statistics paths and the current frontend formatter source. It does not assert behavior of third-party dashboard cards, CSV consumers, SQL drivers, or Garmin’s private cloud semantics beyond the integration boundary. Where the source shows storage as a Unix timestamp, the conclusion is specifically about Recorder statistics, not every timestamp-bearing Home Assistant table.
