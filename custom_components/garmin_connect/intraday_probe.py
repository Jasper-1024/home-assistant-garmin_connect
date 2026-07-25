"""Read-only probe for Garmin Connect intraday health series.

This module intentionally returns metadata and small boundary samples only.
It never logs or returns authentication material and does not persist health data.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from statistics import median
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ha_garmin import GarminClient

METRICS = ("heart_rate", "stress", "body_battery", "hrv")


def _timestamp_ms_to_iso(value: Any) -> str | None:
    """Convert a Garmin epoch timestamp to UTC ISO format."""
    if not isinstance(value, int | float) or isinstance(value, bool):
        return None
    timestamp = float(value)
    if timestamp > 100_000_000_000:
        timestamp /= 1000
    try:
        return datetime.fromtimestamp(timestamp, tz=UTC).isoformat()
    except OSError, OverflowError, ValueError:
        return None


def _parse_garmin_datetime(value: Any) -> datetime | None:
    """Parse a Garmin ISO timestamp as UTC when it has no offset."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _interval_summary(timestamps: list[float]) -> dict[str, float | int | None]:
    """Summarize adjacent timestamp intervals."""
    ordered = sorted(set(timestamps))
    intervals = [
        (current - previous) / 1000
        for previous, current in zip(ordered, ordered[1:], strict=False)
        if current > previous
    ]
    if not intervals:
        return {
            "interval_count": 0,
            "minimum_interval_seconds": None,
            "median_interval_seconds": None,
            "maximum_interval_seconds": None,
        }
    return {
        "interval_count": len(intervals),
        "minimum_interval_seconds": min(intervals),
        "median_interval_seconds": median(intervals),
        "maximum_interval_seconds": max(intervals),
    }


def _summarize_pair_series(
    payload: dict[str, Any],
    *,
    values_key: str,
    descriptor_keys: tuple[str, ...],
    exclude_negative: bool = False,
) -> dict[str, Any]:
    """Summarize a Garmin ``[[epoch_ms, value], ...]`` series."""
    raw_points = payload.get(values_key)
    points = raw_points if isinstance(raw_points, list) else []
    timestamps: list[float] = []
    valid_values: list[float] = []
    null_count = 0
    negative_count = 0
    first_sample: list[Any] | None = None
    last_sample: list[Any] | None = None

    for point in points:
        if not isinstance(point, list | tuple) or len(point) < 2:
            continue
        timestamp, value = point[0], point[1]
        if isinstance(timestamp, int | float) and not isinstance(timestamp, bool):
            timestamps.append(float(timestamp))
        if first_sample is None:
            first_sample = [timestamp, value]
        last_sample = [timestamp, value]

        if value is None:
            null_count += 1
        elif isinstance(value, int | float) and not isinstance(value, bool):
            if value < 0:
                negative_count += 1
                if exclude_negative:
                    continue
            valid_values.append(float(value))

    descriptors: Any = None
    descriptor_key: str | None = None
    for key in descriptor_keys:
        if key in payload:
            descriptor_key = key
            descriptors = payload.get(key)
            break

    result: dict[str, Any] = {
        "ok": True,
        "payload_keys": sorted(payload),
        "values_key": values_key,
        "descriptor_key": descriptor_key,
        "descriptors": descriptors,
        "point_count": len(points),
        "valid_value_count": len(valid_values),
        "null_value_count": null_count,
        "negative_value_count": negative_count,
        "first_sample": first_sample,
        "last_sample": last_sample,
        "first_timestamp_utc": (_timestamp_ms_to_iso(first_sample[0]) if first_sample else None),
        "last_timestamp_utc": (_timestamp_ms_to_iso(last_sample[0]) if last_sample else None),
        "value_min": min(valid_values) if valid_values else None,
        "value_max": max(valid_values) if valid_values else None,
    }
    result.update(_interval_summary(timestamps))
    return result


def _summarize_hrv(payload: dict[str, Any]) -> dict[str, Any]:
    """Summarize Garmin sleep HRV readings."""
    raw_readings = payload.get("hrvReadings")
    readings_key = "hrvReadings"
    if not isinstance(raw_readings, list):
        raw_readings = payload.get("hrv")
        readings_key = "hrv"
    readings = raw_readings if isinstance(raw_readings, list) else []

    timestamps: list[float] = []
    valid_values: list[float] = []
    normalized_samples: list[dict[str, Any]] = []
    null_count = 0

    for reading in readings:
        if not isinstance(reading, dict):
            continue
        time_value = (
            reading.get("readingTimeGMT")
            or reading.get("readingTimeGmt")
            or reading.get("readingTime")
        )
        value = reading.get("hrvValue")
        if value is None:
            value = reading.get("value")
        parsed_time = _parse_garmin_datetime(time_value)
        if parsed_time is not None:
            timestamps.append(parsed_time.timestamp() * 1000)
        if value is None:
            null_count += 1
        elif isinstance(value, int | float) and not isinstance(value, bool):
            valid_values.append(float(value))
        normalized_samples.append(
            {
                "timestamp_utc": parsed_time.isoformat() if parsed_time else None,
                "value": value,
            }
        )

    result: dict[str, Any] = {
        "ok": True,
        "payload_keys": sorted(payload),
        "readings_key": readings_key,
        "summary_keys": sorted(payload.get("hrvSummary") or {}),
        "point_count": len(readings),
        "valid_value_count": len(valid_values),
        "null_value_count": null_count,
        "first_sample": normalized_samples[0] if normalized_samples else None,
        "last_sample": normalized_samples[-1] if normalized_samples else None,
        "value_min": min(valid_values) if valid_values else None,
        "value_max": max(valid_values) if valid_values else None,
    }
    result.update(_interval_summary(timestamps))
    return result


def _safe_error(err: Exception) -> dict[str, Any]:
    """Return a bounded error without authentication material."""
    message = str(err).replace("\r", " ").replace("\n", " ")
    return {
        "ok": False,
        "error_type": type(err).__name__,
        "error": message[:300],
    }


async def _probe_heart_rate(client: GarminClient, target_date: date) -> dict[str, Any]:
    profile = await client.get_user_profile()
    url = f"{client._base_url}/wellness-service/wellness/dailyHeartRate/{profile.display_name}"
    payload = await client._request("GET", url, params={"date": target_date.isoformat()})
    if not isinstance(payload, dict):
        return {"ok": True, "response_type": type(payload).__name__, "point_count": 0}
    return _summarize_pair_series(
        payload,
        values_key="heartRateValues",
        descriptor_keys=("heartRateValueDescriptors",),
    )


async def _probe_stress(client: GarminClient, target_date: date) -> dict[str, Any]:
    url = f"{client._base_url}/wellness-service/wellness/dailyStress/{target_date.isoformat()}"
    payload = await client._request("GET", url)
    if not isinstance(payload, dict):
        return {"ok": True, "response_type": type(payload).__name__, "point_count": 0}
    return _summarize_pair_series(
        payload,
        values_key="stressValuesArray",
        descriptor_keys=(
            "stressValueDescriptorsDTOList",
            "stressValueDescriptorsDtoList",
        ),
        exclude_negative=True,
    )


async def _probe_body_battery(client: GarminClient, target_date: date) -> dict[str, Any]:
    url = f"{client._base_url}/wellness-service/wellness/bodyBattery/reports/daily"
    payload = await client._request(
        "GET",
        url,
        params={
            "startDate": target_date.isoformat(),
            "endDate": target_date.isoformat(),
        },
    )
    entries = payload if isinstance(payload, list) else []
    matching = next(
        (
            item
            for item in entries
            if isinstance(item, dict) and item.get("date") == target_date.isoformat()
        ),
        None,
    )
    if matching is None:
        matching = next((item for item in entries if isinstance(item, dict)), None)
    if matching is None:
        return {
            "ok": True,
            "response_type": type(payload).__name__,
            "entry_count": len(entries),
            "point_count": 0,
        }
    result = _summarize_pair_series(
        matching,
        values_key="bodyBatteryValuesArray",
        descriptor_keys=(
            "bodyBatteryValueDescriptorDTOList",
            "bodyBatteryValueDescriptorsDTOList",
        ),
    )
    result["entry_count"] = len(entries)
    result["entry_date"] = matching.get("date")
    return result


async def _probe_hrv(client: GarminClient, target_date: date) -> dict[str, Any]:
    payload = await client._get_hrv_data_raw(target_date)
    if not isinstance(payload, dict):
        return {"ok": True, "response_type": type(payload).__name__, "point_count": 0}
    return _summarize_hrv(payload)


async def async_probe_intraday(
    client: GarminClient, target_date: date, metric: str
) -> dict[str, Any]:
    """Probe one or all Garmin intraday endpoints with an authenticated client."""
    requested = METRICS if metric == "all" else (metric,)
    probes = {
        "heart_rate": _probe_heart_rate,
        "stress": _probe_stress,
        "body_battery": _probe_body_battery,
        "hrv": _probe_hrv,
    }
    results: dict[str, Any] = {}
    for name in requested:
        try:
            results[name] = await probes[name](client, target_date)
        except Exception as err:  # Probe each endpoint independently.
            results[name] = _safe_error(err)
    return {
        "date": target_date.isoformat(),
        "requested_metric": metric,
        "results": results,
    }
