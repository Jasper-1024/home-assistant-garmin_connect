"""Tests for captured-shape Garmin history normalization."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.garmin_connect.history_source import (
    DAILY_SUMMARY_FIELDS,
    TRAINING_STATUS_FIELDS,
    GarminHistorySource,
    HistorySchemaError,
    normalize_body_battery,
    normalize_floors,
    normalize_intensity,
    normalize_pair_series,
    normalize_respiration,
    normalize_snapshot,
    normalize_spo2,
    normalize_steps,
    parse_hrv_data,
)


class _ImmediateGate:
    async def async_request(self, priority, request):
        return await request()


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


def test_body_battery_selects_daily_report_and_revises_irregular_times() -> None:
    payload = [
        {"calendarDate": "2026-07-25", "bodyBatteryValuesArray": []},
        {
            "calendarDate": "2026-07-24",
            "bodyBatteryValueDescriptorsDTOList": [
                {"key": "bodyBatteryValue", "index": 2}, {"key": "timestamp", "index": 0}, {"key": "unknown", "index": 1}
            ],
            "bodyBatteryValuesArray": [
                ["2026-07-24T23:59:00Z", "x", 0],
                ["2026-07-24T01:02:03Z", "x", 44],
                ["2026-07-24T01:02:03Z", "revision", 45],
            ],
        },
    ]
    samples = normalize_body_battery(payload, date(2026, 7, 24))
    assert [(sample.value, sample.raw_timestamp) for sample in samples] == [(45.0, "2026-07-24T01:02:03Z"), (0.0, "2026-07-24T23:59:00Z")]


def test_hrv_raw_readings_tolerate_missing_summary_and_keep_zero() -> None:
    parsed = parse_hrv_data(
        {"hrvReadings": [
            {"readingTimeGMT": "2026-07-24T23:59:00Z", "hrvValue": 0, "unknown": True},
            {"readingTimeGmt": "2026-07-24T01:00:00Z", "value": 42},
            {"readingTime": "2026-07-24T01:00:00Z", "hrvValue": 43},
            {"readingTime": "2026-07-24T02:00:00Z", "hrvValue": None},
        ]}, date(2026, 7, 24)
    )
    assert [sample.value for sample in parsed.readings] == [43.0, 0.0]
    assert parsed.summary is None


def test_hrv_nested_summary_is_separate_and_typed() -> None:
    parsed = parse_hrv_data({"hrvReadings": [], "hrvSummary": {"status": "balanced", "lastNightAvg": 48, "lastNight5MinHigh": 72, "weeklyAvg": 50, "baseline": {"low": 40, "high": 60}}}, date(2026, 7, 24))
    assert parsed.readings == ()
    assert parsed.summary is not None
    assert parsed.summary.baseline == {"low": 40.0, "high": 60.0}


def test_body_battery_singular_descriptor_is_supported() -> None:
    samples = normalize_body_battery({"bodyBatteryValueDescriptorDTOList": [{"key": "timestamp", "index": 0}, {"key": "bodyBatteryValue", "index": 1}], "bodyBatteryValuesArray": [["2026-07-24T01:00:00Z", 0]]}, date(2026, 7, 24))
    assert samples[0].value == 0


def test_known_type_drift_is_rejected_but_null_is_missing() -> None:
    with pytest.raises(HistorySchemaError):
        normalize_pair_series({"heartRateValues": [["bad", 60]]}, values_key="heartRateValues", descriptor_keys=(), value_keys=("heartRate",))
    samples = normalize_pair_series({"heartRateValues": [["2026-07-24T01:00:00Z", None], ["2026-07-24T01:01:00Z", 0]]}, values_key="heartRateValues", descriptor_keys=(), value_keys=("heartRate",))
    assert [sample.value for sample in samples] == [0]


def test_segmented_steps_descriptor_reordering_preserves_revisions_and_totals() -> None:
    parsed = normalize_steps({
        "stepsValueDescriptors": [{"key": "steps", "index": 2}, {"key": "timestamp", "index": 0}, {"key": "activityLevel", "index": 1}],
        "stepsValuesArray": [["2026-07-24T02:00:00Z", "active", 10], ["2026-07-24T01:00:00Z", "rest", 0], ["2026-07-24T02:00:00Z", "revised", 12]],
        "totalSteps": 12,
    }, date(2026, 7, 24))
    assert [sample.value for sample in parsed.readings] == [0, 12]
    assert parsed.totals == {"totalSteps": 12.0}


def test_floors_and_intensity_keep_distinct_semantics() -> None:
    floors = normalize_floors({"data": [{"time": 0, "floors": 0}, {"time": 60, "floors": 2}]}, date(2026, 7, 24))
    moderate = normalize_intensity({"data": [{"start": 0, "moderateIntensityMinutes": 1}]}, date(2026, 7, 24), "moderate")
    vigorous = normalize_intensity({"data": [{"start": 0, "vigorousIntensityMinutes": 3}]}, date(2026, 7, 24), "vigorous")
    assert [sample.value for sample in floors.readings] == [0, 2]
    assert moderate.readings[0].value == 1
    assert vigorous.readings[0].value == 3


def test_fixture_like_nested_totals_and_singular_descriptor_spellings() -> None:
    steps = normalize_steps({
        "summary": {"totalSteps": 12345, "goal": 15000, "optional": None},
        "stepsValueDescriptorDTOList": [{"key": "timestamp", "index": 1}, {"key": "steps", "index": 0}, {"key": "activityLevel", "index": 2}],
        "stepsValuesArray": [[0, "2026-07-24T00:00:00Z", "rest"], [3, "2026-07-24T00:15:00Z", "active"]] * 8,
    }, date(2026, 7, 24))
    floors = normalize_floors({
        "report": {"totals": {"floorsAscended": 10, "floorsDescended": 4}},
        "floorsValueDescriptorDTOList": [{"key": "floorCount", "index": 1}, {"key": "timestamp", "index": 0}],
        "floorsValuesArray": [[0, 0], [60, 2], [120, None]],
    }, date(2026, 7, 24))
    intensity = normalize_intensity({
        "data": {"summary": {"moderateIntensityMinutes": 42, "vigorousIntensityMinutes": 8}, "intensityValues": []},
        "intensityValueDescriptorDTOList": [{"key": "moderateIntensityMinutes", "index": 0}, {"key": "timestamp", "index": 1}],
        "intensityValuesArray": [[1, 0], [2, 900000]],
    }, date(2026, 7, 24), "moderate")
    assert len(steps.readings) == 2
    assert steps.totals == {"totalSteps": 12345.0}
    assert floors.totals == {"floorsAscended": 10.0, "floorsDescended": 4.0}
    assert intensity.totals == {"moderateIntensityMinutes": 42.0, "vigorousIntensityMinutes": 8.0}


def test_descriptor_index_beyond_point_width_is_schema_error() -> None:
    with pytest.raises(HistorySchemaError):
        normalize_steps({
            "stepsValueDescriptorDTOList": [{"key": "timestamp", "index": 0}, {"key": "steps", "index": 2}],
            "stepsValuesArray": [["2026-07-24T00:00:00Z", 1]],
        }, date(2026, 7, 24))


def test_sanitized_captured_chart_fixture_preserves_nested_shapes() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_segmented_charts.json").read_text())
    steps = normalize_steps(fixture["steps"], date(2026, 7, 24))
    floors = normalize_floors(fixture["floors"], date(2026, 7, 24))
    intensity = normalize_intensity(fixture["intensity"], date(2026, 7, 24), "moderate")
    vigorous = normalize_intensity(fixture["intensity"], date(2026, 7, 24), "vigorous")
    assert len(steps.readings) == 8
    assert len(floors.readings) == 6
    assert len(intensity.readings) == 4
    assert len(vigorous.readings) == 4
    assert steps.totals == {"totalSteps": 12345.0}
    assert floors.totals == {"floorsAscended": 10.0, "floorsDescended": 4.0}
    assert intensity.totals == vigorous.totals
    assert [sample.value for sample in vigorous.readings] == [0.0, 1.0, 0.0, 2.0]


def test_respiration_and_spo2_fixture_variants_preserve_revisions_and_sparse_series() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_respiration_spo2.json").read_text())
    respiration = normalize_respiration(fixture["respiration"], date(2026, 7, 24))
    average = normalize_respiration(fixture["respiration"], date(2026, 7, 24), True)
    single = normalize_spo2(fixture["spo2"], date(2026, 7, 24), "single")
    continuous = normalize_spo2(fixture["spo2"], date(2026, 7, 24), "continuous")
    hourly = normalize_spo2(fixture["spo2"], date(2026, 7, 24), "hourly")
    assert len(respiration.readings) == 2
    assert average.readings[0].value == 12.5
    assert single.readings[0].value == 98
    assert continuous.readings[0].value == 96
    assert hourly.readings[0].value == 95
    assert respiration.presence == "present"
    assert average.presence == "present"
    assert normalize_respiration(None, date(2026, 7, 24)).presence == "null"
    assert normalize_respiration({"respirationValuesArray": None}, date(2026, 7, 24)).presence == "null"
    assert normalize_respiration({"respirationValuesArray": []}, date(2026, 7, 24)).presence == "empty"
    assert normalize_respiration({}, date(2026, 7, 24)).presence == "missing"
    assert normalize_respiration([], date(2026, 7, 24)).presence == "unsupported"
    assert normalize_respiration({"status": "returned-empty"}, date(2026, 7, 24)).presence == "returned-empty"


@pytest.mark.parametrize(
    ("normalizer", "payload"),
    [
        (normalize_respiration, {"respirationValuesArray": {"drift": True}}),
        (normalize_spo2, {"spO2SingleValues": "drift"}),
    ],
)
def test_recognized_respiration_and_spo2_array_type_drift_raises(
    normalizer, payload
) -> None:
    """A known series changing its array type is a schema failure."""
    with pytest.raises(HistorySchemaError):
        if normalizer is normalize_spo2:
            normalizer(payload, date(2026, 7, 24), "single")
        else:
            normalizer(payload, date(2026, 7, 24))


def test_daily_summary_and_training_snapshot_presence_and_type_drift() -> None:
    """Snapshots preserve present/null/absent fields without readiness calls."""
    daily = normalize_snapshot(
        {"abnormalHeartRateAlertsCount": 2, "unknown": "ignored"},
        date(2026, 7, 24),
        DAILY_SUMMARY_FIELDS,
    )
    training = normalize_snapshot(
        {"acuteLoad": 42, "recoveryTime": None},
        date(2026, 7, 24),
        TRAINING_STATUS_FIELDS,
    )
    assert daily.fields["abnormal_heart_rate_alerts"] == ("present", 2.0)
    assert training.fields["recovery_time"] == ("null", None)
    assert training.fields["vo2_max"] == ("absent", None)
    with pytest.raises(HistorySchemaError):
        normalize_snapshot({"acuteLoad": "drift"}, date(2026, 7, 24), TRAINING_STATUS_FIELDS)


def test_snapshot_normalization_uses_sanitized_fixture() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_summary_training.json").read_text())
    daily = normalize_snapshot(fixture["daily_summary"], date(2026, 7, 24), DAILY_SUMMARY_FIELDS)
    training = normalize_snapshot(fixture["training_status"], date(2026, 7, 24), TRAINING_STATUS_FIELDS)
    assert fixture["_cardinality"]["training_status_fields"] == len(training.fields)
    assert daily.fields["abnormal_heart_rate_alerts"] == ("present", 2.0)
    assert training.fields["vo2_max"] == ("present", 47.2)
    assert training.fields["recovery_time"] == ("null", None)


@pytest.mark.asyncio
async def test_training_status_never_calls_training_readiness() -> None:
    client = MagicMock()
    client.get_training_status = AsyncMock(return_value={"acuteLoad": 1})
    client.get_training_readiness = AsyncMock(side_effect=AssertionError("readiness forbidden"))
    source = GarminHistorySource(client, _ImmediateGate())

    await source.async_fetch_details(date(2026, 7, 24), "training_status")

    client.get_training_status.assert_awaited_once()
    client.get_user_profile.assert_not_called()
    client.get_training_readiness.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_summary_uses_raw_client_method_without_profile() -> None:
    client = MagicMock()
    client._get_user_summary_raw = AsyncMock(return_value={"abnormalHeartRateAlertsCount": 1})
    source = GarminHistorySource(client, _ImmediateGate())

    await source.async_fetch_details(date(2026, 7, 24), "daily_summary")

    client._get_user_summary_raw.assert_awaited_once_with(date(2026, 7, 24))
    client.get_user_profile.assert_not_called()
