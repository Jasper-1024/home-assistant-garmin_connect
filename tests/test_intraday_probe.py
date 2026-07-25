"""Tests for the read-only Garmin intraday probe."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

from custom_components.garmin_connect.intraday_probe import (
    _summarize_capability_payload,
    _summarize_pair_series,
    async_probe_capability,
    async_probe_intraday,
)


def test_summarize_stress_series_filters_negative_quality_codes() -> None:
    """Negative Garmin stress codes must not become valid measurements."""
    payload = {
        "stressValueDescriptorsDTOList": [
            {"key": "timestamp", "index": 0},
            {"key": "stressLevel", "index": 1},
        ],
        "stressValuesArray": [
            [1688191200000, 23],
            [1688191380000, -1],
            [1688191560000, 20],
            [1688191740000, None],
        ],
    }

    result = _summarize_pair_series(
        payload,
        values_key="stressValuesArray",
        descriptor_keys=("stressValueDescriptorsDTOList",),
        exclude_negative=True,
    )

    assert result["point_count"] == 4
    assert result["valid_value_count"] == 2
    assert result["negative_value_count"] == 1
    assert result["null_value_count"] == 1
    assert result["median_interval_seconds"] == 180
    assert result["value_min"] == 20
    assert result["value_max"] == 23


async def test_probe_intraday_calls_existing_authenticated_client() -> None:
    """Probe must use the supplied GarminClient and return summaries only."""
    client = SimpleNamespace()
    client._base_url = "https://connectapi.garmin.com"
    client.get_user_profile = AsyncMock(return_value=SimpleNamespace(display_name="profile-name"))
    client._request = AsyncMock(
        side_effect=[
            {
                "heartRateValueDescriptors": [
                    {"key": "timestamp", "index": 0},
                    {"key": "heartRate", "index": 1},
                ],
                "heartRateValues": [
                    [1688191200000, 60],
                    [1688191320000, 64],
                ],
            },
            {
                "stressValueDescriptorsDTOList": [],
                "stressValuesArray": [[1688191200000, 23]],
            },
            [
                {
                    "date": "2026-07-24",
                    "bodyBatteryValueDescriptorDTOList": [],
                    "bodyBatteryValuesArray": [[1688191200000, 55]],
                }
            ],
        ]
    )
    client._get_hrv_data_raw = AsyncMock(
        return_value={
            "hrvSummary": {"lastNightAvg": 50},
            "hrvReadings": [
                {
                    "readingTimeGMT": "2026-07-24T01:00:00.0",
                    "hrvValue": 48,
                },
                {
                    "readingTimeGMT": "2026-07-24T01:05:00.0",
                    "hrvValue": 52,
                },
            ],
        }
    )

    result = await async_probe_intraday(client, date(2026, 7, 24), "all")

    assert result["results"]["heart_rate"]["point_count"] == 2
    assert result["results"]["heart_rate"]["median_interval_seconds"] == 120
    assert result["results"]["stress"]["point_count"] == 1
    assert result["results"]["body_battery"]["point_count"] == 1
    assert result["results"]["hrv"]["point_count"] == 2
    assert result["results"]["hrv"]["median_interval_seconds"] == 300
    assert client._request.await_count == 3
    client._get_hrv_data_raw.assert_awaited_once_with(date(2026, 7, 24))


def test_capability_summary_omits_scalar_health_values() -> None:
    """Capability summaries expose structure and sizes, never scalar values."""
    result = _summarize_capability_payload(
        {
            "sleepScores": {"overall": {"value": 82}},
            "skinTempDataExists": True,
            "sleepLevels": [
                {
                    "startGMT": "2026-07-24T01:00:00",
                    "activityLevel": 2,
                    "activityType": "deepSleep",
                }
            ],
        }
    )

    shape = result["shape"]
    assert shape["fields"]["sleepLevels"]["length"] == 1
    assert shape["fields"]["sleepScores"]["fields"]["overall"]["fields"]["value"] == {
        "type": "number",
        "non_null": True,
    }
    assert "82" not in str(result)
    assert "2026-07-24T01:00:00" not in str(result)
    assert result["availability_flags"] == {"skinTempDataExists": [True]}
    assert result["categories"] == {"activityType": ["deepSleep"]}


async def test_capability_probe_makes_one_request() -> None:
    """One capability invocation must make exactly one Garmin data request."""
    client = SimpleNamespace()
    client._base_url = "https://connectapi.garmin.com"
    client._request = AsyncMock(
        return_value={
            "respirationValuesArray": [[1688191200000, 14.5]],
            "respirationAveragesValuesArray": [],
        }
    )

    result = await async_probe_capability(
        client,
        "respiration",
        date(2026, 7, 24),
    )

    assert result["result"]["ok"] is True
    assert result["result"]["shape"]["fields"]["respirationValuesArray"]["length"] == 1
    client._request.assert_awaited_once()
