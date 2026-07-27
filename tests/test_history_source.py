"""Tests for captured-shape Garmin history normalization."""

from datetime import UTC, datetime

import pytest

from custom_components.garmin_connect.history_source import (
    HistorySchemaError,
    normalize_pair_series,
)


def test_normalize_pair_series_uses_descriptors_and_keeps_ordered_equal_values() -> None:
    """Descriptor positions, timestamps, zeros, and equal samples are preserved."""
    payload = {
        "heartRateValueDescriptors": [
            {"key": "heartRate", "index": 0},
            {"key": "timestamp", "index": 1},
            {"key": "newField", "index": 2},
        ],
        "heartRateValues": [
            [62, "2026-07-24T01:04:00+02:00", "ignored"],
            [0, 1_784_852_400_000, "ignored"],
            [59, "2026-07-24T01:02:00", "ignored"],
            [61, "2026-07-24T01:04:00+02:00", "revision"],
        ],
        "unknownField": {"shape": "tolerated"},
    }

    samples = normalize_pair_series(
        payload,
        values_key="heartRateValues",
        descriptor_keys=("heartRateValueDescriptors",),
        value_keys=("heartRate",),
    )

    assert [(sample.timestamp, sample.value) for sample in samples] == [
        (datetime(2026, 7, 23, 23, 4, tzinfo=UTC), 61.0),
        (datetime(2026, 7, 24, 0, 20, tzinfo=UTC), 0.0),
        (datetime(2026, 7, 24, 1, 2, tzinfo=UTC), 59.0),
    ]
    assert samples[0].request_date.isoformat() == "2026-07-24"
    assert samples[0].raw_timestamp == "2026-07-24T01:04:00+02:00"
    assert samples[1].raw_timestamp == 1_784_852_400_000


def test_normalize_stress_excludes_negative_quality_codes_but_keeps_zero_and_null() -> None:
    """Negative stress codes are quality values, not measurements."""
    payload = {
        "stressValueDescriptorsDTOList": [
            {"key": "timestamp", "index": 0},
            {"key": "stressLevel", "index": 1},
        ],
        "stressValuesArray": [
            [1_784_852_400_000, -1],
            [1_784_852_460_000, 0],
            [1_784_852_520_000, None],
        ],
    }

    samples = normalize_pair_series(
        payload,
        values_key="stressValuesArray",
        descriptor_keys=("stressValueDescriptorsDTOList",),
        value_keys=("stressLevel",),
        exclude_negative=True,
    )

    assert len(samples) == 1
    assert samples[0].value == 0


def test_normalize_pair_series_rejects_incompatible_known_series() -> None:
    """A changed known field fails only this family/date."""
    with pytest.raises(HistorySchemaError):
        normalize_pair_series(
            {"heartRateValues": {"not": "an array"}},
            values_key="heartRateValues",
            descriptor_keys=("heartRateValueDescriptors",),
            value_keys=("heartRate",),
        )
