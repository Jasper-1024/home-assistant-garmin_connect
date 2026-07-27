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


def _timestamp(value: Any) -> datetime | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        number = float(value) / (1000 if abs(float(value)) >= 100_000_000_000 else 1)
        try:
            return datetime.fromtimestamp(number, tz=UTC)
        except (OverflowError, OSError, ValueError):
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
            if not isinstance(item, dict) or not isinstance(item.get("key"), str) or not isinstance(item.get("index"), int):
                continue
            result[item["key"]] = item["index"]
        return result
    return {}


def normalize_pair_series(
    payload: dict[str, Any], *, values_key: str, descriptor_keys: tuple[str, ...],
    value_keys: tuple[str, ...], exclude_negative: bool = False,
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
    positions = _descriptors(payload, descriptor_keys)
    timestamp_index = positions.get("timestamp", 0)
    value_index = next((positions[key] for key in value_keys if key in positions), 1)
    effective_date = request_date
    latest: dict[datetime, NormalizedSample] = {}
    for point in raw_points:
        if not isinstance(point, (list, tuple)) or timestamp_index >= len(point) or value_index >= len(point):
            continue
        raw_time, raw_value = point[timestamp_index], point[value_index]
        parsed = _timestamp(raw_time)
        if parsed is None or isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
            continue
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


class GarminHistorySource:
    """Small serialized adapter for Garmin intraday endpoints."""

    def __init__(self, client: GarminClient, request_gate: GarminRequestGate | None = None) -> None:
        self.client = client
        self.request_gate = request_gate or GarminRequestGate()

    async def async_fetch(self, target_date: date, metric: str) -> tuple[NormalizedSample, ...]:
        """Fetch and normalize one supported metric at low request priority."""
        async def request() -> Any:
            profile = await self.client.get_user_profile()
            base = self.client._base_url
            if metric == "heart_rate":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/dailyHeartRate/{profile.display_name}", params={"date": target_date.isoformat()})
            if metric == "stress":
                return await self.client._request("GET", f"{base}/wellness-service/wellness/dailyStress/{target_date.isoformat()}")
            raise ValueError(f"unsupported history metric: {metric}")
        payload = await self.request_gate.async_request(GarminRequestPriority.BACKGROUND, request)
        if not isinstance(payload, dict):
            return ()
        if metric == "heart_rate":
            return normalize_pair_series(payload, values_key="heartRateValues", descriptor_keys=("heartRateValueDescriptors",), value_keys=("heartRate",), request_date=target_date)
        return normalize_pair_series(payload, values_key="stressValuesArray", descriptor_keys=("stressValueDescriptorsDTOList", "stressValueDescriptorsDtoList"), value_keys=("stressLevel",), exclude_negative=True, request_date=target_date)


async def async_fetch_intraday(client: GarminClient, target_date: date, metric: str, request_gate: GarminRequestGate | None = None) -> tuple[NormalizedSample, ...]:
    """Fetch one intraday series through a shared request gate."""
    return await GarminHistorySource(client, request_gate).async_fetch(target_date, metric)
