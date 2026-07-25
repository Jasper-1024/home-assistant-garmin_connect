"""Tests for the read-only Garmin intraday probe."""

from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock

from custom_components.garmin_connect.intraday_probe import (
    _summarize_pair_series,
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
