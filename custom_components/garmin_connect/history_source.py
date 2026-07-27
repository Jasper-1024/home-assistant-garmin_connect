"""Authenticated Garmin intraday history source and payload normalization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from .request_gate import GarminRequestGate, GarminRequestPriority

if TYPE_CHECKING:
    from ha_garmin import GarminClient


class HistorySchemaError(ValueError):
    """Raised when a known Garmin series has an incompatible shape."""


@dataclass(frozen=True, slots=True)
class NormalizedSample:
    """One immutable, normalized intraday measurement."""

    timestamp: datetime
    request_date: date
    raw_timestamp: Any
    value: float


@dataclass(frozen=True, slots=True)
class HRVSummary:
    """Bounded HRV summary metadata, kept separate from raw readings."""

    status: str | None = None
    last_night_avg: float | None = None
    last_night_5_min_high: float | None = None
    weekly_avg: float | None = None
    baseline: dict[str, float] | None = None


@dataclass(frozen=True, slots=True)
class HRVData:
    """Raw HRV readings plus an optional, bounded summary."""

    readings: tuple[NormalizedSample, ...]
    summary: HRVSummary | None = None


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        number = float(value) / (1000 if abs(float(value)) >= 100_000_000_000 else 1)
        try:
            return datetime.fromtimestamp(number, tz=UTC)
        except OverflowError, OSError, ValueError:
            return None
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)).astimezone(UTC)
    return None


def _descriptors(payload: dict[str, Any], keys: tuple[str, ...]) -> dict[str, int]:
    for key in keys:
        if key not in payload:
            continue
        raw = payload[key]
        if not isinstance(raw, list):
            raise HistorySchemaError(f"{key} is not a descriptor list")
        result: dict[str, int] = {}
        for item in raw:
            if not isinstance(item, dict) or not isinstance(item.get("key"), str) or not isinstance(item.get("index"), int) or isinstance(item.get("index"), bool):
                raise HistorySchemaError(f"{key} contains a malformed descriptor")
            index = item["index"]
            if index < 0 or item["key"] in result or index in result.values():
                raise HistorySchemaError(f"{key} contains an invalid descriptor index")
            result[item["key"]] = index
        return result
    return {}


def normalize_pair_series(
    payload: dict[str, Any],
    *,
    values_key: str,
    descriptor_keys: tuple[str, ...],
    value_keys: tuple[str, ...],
    exclude_negative: bool = False,
    request_date: date | None = None,
) -> tuple[NormalizedSample, ...]:
    """Normalize a descriptor-driven Garmin ``[timestamp, value]`` series."""
    if not isinstance(payload, dict):
        raise HistorySchemaError("series payload is not an object")
    raw_points = payload.get(values_key)
    if raw_points is None:
        return ()
    if not isinstance(raw_points, list):
        raise HistorySchemaError(f"{values_key} is not an array")
    descriptor_present = any(key in payload for key in descriptor_keys)
    positions = _descriptors(payload, descriptor_keys)
    if descriptor_present and raw_points and ("timestamp" not in positions or not any(key in positions for key in value_keys)):
        raise HistorySchemaError("descriptor list lacks required fields")
    timestamp_index = positions.get("timestamp", 0)
    value_index = next((positions[key] for key in value_keys if key in positions), 1)
    effective_date = request_date
    latest: dict[datetime, NormalizedSample] = {}
    for point in raw_points:
        if (
            not isinstance(point, (list, tuple))
            or timestamp_index >= len(point)
            or value_index >= len(point)
        ):
            continue
        raw_time, raw_value = point[timestamp_index], point[value_index]
        if raw_time is not None and not isinstance(raw_time, str | int | float):
            raise HistorySchemaError("timestamp has an invalid type")
        if raw_value is not None and (isinstance(raw_value, bool) or not isinstance(raw_value, int | float)):
            raise HistorySchemaError("value has an invalid type")
        if raw_time is None or raw_value is None:
            continue
        parsed = _timestamp(raw_time)
        if parsed is None:
            raise HistorySchemaError("timestamp has an invalid value")
        if effective_date is None:
            if isinstance(raw_time, str):
                try:
                    local_time = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                except ValueError:
                    local_time = parsed
                effective_date = local_time.date()
            else:
                effective_date = parsed.date()
        if exclude_negative and raw_value < 0:
            continue
        latest[parsed] = NormalizedSample(parsed, effective_date, raw_time, float(raw_value))
    return tuple(latest[key] for key in sorted(latest))


def _select_daily_report(payload: Any, target_date: date) -> dict[str, Any]:
    if isinstance(payload, dict):
        for key in ("bodyBatteryReports", "reports", "dailyReports"):
            if key in payload:
                return _select_daily_report(payload[key], target_date)
        return payload
    if not isinstance(payload, list):
        raise HistorySchemaError("body battery reports are not an array")
    for report in payload:
        if not isinstance(report, dict):
            raise HistorySchemaError("body battery report is not an object")
        for key in ("calendarDate", "date", "reportDate"):
            value = report.get(key)
            if isinstance(value, str) and value[:10] == target_date.isoformat():
                return report
    return {}


def normalize_body_battery(payload: Any, target_date: date) -> tuple[NormalizedSample, ...]:
    """Normalize one daily body-battery report with descriptor-defined columns."""
    report = _select_daily_report(payload, target_date)
    return normalize_pair_series(
        report,
        values_key="bodyBatteryValuesArray",
        descriptor_keys=(
            "bodyBatteryValueDescriptorsDTOList",
            "bodyBatteryValueDescriptorsDtoList",
            "bodyBatteryValueDescriptorDTOList",
            "bodyBatteryValueDescriptors",
        ),
        value_keys=("bodyBatteryValue", "bodyBatteryLevel", "value"),
        request_date=target_date,
    )


def parse_hrv_data(payload: Any, target_date: date) -> HRVData:
    """Parse HRV readings while tolerating absent summary fields."""
    if not isinstance(payload, dict):
        raise HistorySchemaError("HRV payload is not an object")
    raw_readings = payload.get("hrvReadings", [])
    if raw_readings is None:
        raw_readings = []
    if not isinstance(raw_readings, list):
        raise HistorySchemaError("HRV readings are not an array")
    readings: list[NormalizedSample] = []
    for reading in raw_readings:
        if not isinstance(reading, dict):
            raise HistorySchemaError("HRV reading is not an object")
        raw_time = next((reading[key] for key in ("readingTimeGMT", "readingTimeGmt", "readingTime") if key in reading), None)
        raw_value = next((reading[key] for key in ("hrvValue", "value") if key in reading), None)
        if raw_time is not None and not isinstance(raw_time, str | int | float):
            raise HistorySchemaError("HRV timestamp has an invalid type")
        if raw_value is not None and (isinstance(raw_value, bool) or not isinstance(raw_value, int | float)):
            raise HistorySchemaError("HRV value has an invalid type")
        if raw_time is None or raw_value is None:
            continue
        parsed = _timestamp(raw_time)
        if parsed is None:
            raise HistorySchemaError("HRV timestamp has an invalid value")
        readings.append(NormalizedSample(parsed, target_date, raw_time, float(raw_value)))
    latest = {sample.timestamp: sample for sample in readings}
    raw_summary = payload.get("hrvSummary")
    if raw_summary is not None and not isinstance(raw_summary, dict):
        raise HistorySchemaError("HRV summary is not an object")
    summary_data = raw_summary or {}
    def numeric(name: str) -> float | None:
        value = summary_data.get(name)
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise HistorySchemaError("HRV summary value has an invalid type")
        return float(value)
    raw_baseline = summary_data.get("baseline")
    baseline: dict[str, float] | None = None
    if raw_baseline is not None:
        if not isinstance(raw_baseline, dict):
            raise HistorySchemaError("HRV baseline is not an object")
        baseline = {}
        for key, value in list(raw_baseline.items())[:8]:
            if not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int | float):
                raise HistorySchemaError("HRV baseline has an invalid type")
            baseline[key] = float(value)
    status = summary_data.get("status")
    if status is not None and not isinstance(status, str):
        raise HistorySchemaError("HRV status has an invalid type")
    if isinstance(status, str):
        status = status[:64]
    summary = HRVSummary(status, numeric("lastNightAvg"), numeric("lastNight5MinHigh"), numeric("weeklyAvg"), baseline) if summary_data else None
    return HRVData(tuple(latest[key] for key in sorted(latest)), summary)


class GarminHistorySource:
    """Small serialized adapter for Garmin intraday endpoints."""

    def __init__(self, client: GarminClient, request_gate: GarminRequestGate | None = None) -> None:
        self.client = client
        self.request_gate = request_gate or GarminRequestGate()

    async def async_fetch(self, target_date: date, metric: str) -> tuple[NormalizedSample, ...]:
        """Fetch one metric, retaining the historical tuple return contract."""
        result = await self.async_fetch_details(target_date, metric)
        return result.readings if isinstance(result, HRVData) else result

    async def async_fetch_details(self, target_date: date, metric: str) -> tuple[NormalizedSample, ...] | HRVData:
        """Fetch a metric and retain private details needed by the archive."""

        async def request() -> Any:
            profile = await self.client.get_user_profile()
            base = self.client._base_url
            if metric == "heart_rate":
                return await self.client._request(
                    "GET",
                    f"{base}/wellness-service/wellness/dailyHeartRate/{profile.display_name}",
                    params={"date": target_date.isoformat()},
                )
            if metric == "stress":
                return await self.client._request(
                    "GET", f"{base}/wellness-service/wellness/dailyStress/{target_date.isoformat()}"
                )
            if metric == "body_battery":
                return await self.client._request(
                    "GET", f"{base}/wellness-service/wellness/bodyBattery/reports/daily",
                    params={"date": target_date.isoformat()},
                )
            if metric == "nightly_hrv":
                return await self.client._get_hrv_data_raw(target_date)
            raise ValueError(f"unsupported history metric: {metric}")

        payload = await self.request_gate.async_request(GarminRequestPriority.BACKGROUND, request)
        if metric == "body_battery":
            return normalize_body_battery(payload, target_date)
        if metric == "nightly_hrv":
            return parse_hrv_data(payload, target_date)
        if not isinstance(payload, dict):
            return ()
        if metric == "heart_rate":
            return normalize_pair_series(
                payload,
                values_key="heartRateValues",
                descriptor_keys=("heartRateValueDescriptors",),
                value_keys=("heartRate",),
                request_date=target_date,
            )
        return normalize_pair_series(
            payload,
            values_key="stressValuesArray",
            descriptor_keys=("stressValueDescriptorsDTOList", "stressValueDescriptorsDtoList"),
            value_keys=("stressLevel",),
            exclude_negative=True,
            request_date=target_date,
        )


async def async_fetch_intraday(
    client: GarminClient,
    target_date: date,
    metric: str,
    request_gate: GarminRequestGate | None = None,
) -> tuple[NormalizedSample, ...]:
    """Fetch one intraday series through a shared request gate."""
    return await GarminHistorySource(client, request_gate).async_fetch(target_date, metric)
