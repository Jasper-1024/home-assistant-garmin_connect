"""Tests for captured-shape Garmin history normalization."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from ha_garmin import GarminClient

from custom_components.garmin_connect.history_source import (
    DAILY_SUMMARY_FIELDS,
    TRAINING_STATUS_FIELDS,
    GarminHistorySource,
    HistorySchemaError,
    SourceSeries,
    TrainingDeviceSnapshots,
    health_event_from_record,
    health_event_record,
    normalize_activities,
    normalize_body_battery,
    normalize_floors,
    normalize_health_events,
    normalize_intensity,
    normalize_pair_series,
    normalize_respiration,
    normalize_snapshot,
    normalize_spo2,
    normalize_steps,
    normalize_training_status,
    parse_hrv_data,
)
from custom_components.garmin_connect.sleep_archive import parse_sleep_sessions


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
            [59, "2026-07-24T01:02:00+00:00", "ignored"],
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


def test_current_garmin_intraday_shapes_normalize() -> None:
    """Accept descriptor, GMT, and chart shapes captured from Garmin Connect."""
    target = date(2026, 8, 1)
    heart_rate = normalize_pair_series(
        {
            "heartRateValueDescriptors": [
                {"index": 0, "key": "timestamp"},
                {"index": 1, "key": "heartrate"},
            ],
            "heartRateValues": [[1_785_513_600_000, 61]],
        },
        values_key="heartRateValues",
        descriptor_keys=("heartRateValueDescriptors",),
        value_keys=("heartRate", "heartrate"),
        request_date=target,
    )
    hrv = parse_hrv_data(
        {"hrvReadings": [{"readingTimeGMT": "2026-07-31T15:15:45.0", "hrvValue": 42}]},
        target,
    )
    steps = normalize_steps(
        [{"startGMT": "2026-07-31T16:00:00.0", "endGMT": "2026-07-31T16:15:00.0", "steps": 12}],
        target,
    )
    spo2 = normalize_spo2(
        {
            "spO2ValueDescriptorsDTOList": [
                {"spo2ValueDescriptorIndex": 0, "spo2ValueDescriptorKey": "timestamp"},
                {"spo2ValueDescriptorIndex": 1, "spo2ValueDescriptorKey": "spo2Reading"},
            ],
            "spO2HourlyAverages": [[1_785_513_600_000, 91]],
        },
        target,
        "hourly",
    )

    assert heart_rate[0].value == 61.0
    assert hrv.readings[0].timestamp == datetime(2026, 7, 31, 15, 15, 45, tzinfo=UTC)
    assert steps.readings[0].timestamp == datetime(2026, 7, 31, 16, tzinfo=UTC)
    assert spo2.readings[0].value == 91.0


@pytest.mark.asyncio
async def test_body_battery_uses_date_range_parameters() -> None:
    """Use the upstream Body Battery endpoint's required date-range contract."""
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client._request = AsyncMock(return_value=[])

    result = await GarminHistorySource(client, _ImmediateGate()).async_fetch_details(
        date(2026, 8, 1), "body_battery"
    )

    assert isinstance(result, SourceSeries)
    assert client._request.await_args_list[0] == call(
        "GET",
        "https://garmin.example/wellness-service/wellness/bodyBattery/reports/daily",
        params={"startDate": "2026-08-01", "endDate": "2026-08-01"},
    )
    assert client._request.await_count == 2


def test_measurement_without_source_instant_offset_fails_closed() -> None:
    with pytest.raises(HistorySchemaError):
        normalize_pair_series(
            {"heartRateValues": [[60, "2026-07-24T01:00:00", "ignored"]]},
            values_key="heartRateValues",
            descriptor_keys=(),
            value_keys=("heartRate",),
        )


@pytest.mark.asyncio
async def test_fetch_stress_retains_negative_numeric_samples() -> None:
    """Ordinary intraday stress retains every valid numeric source record."""
    payload = {
        "stressValueDescriptorsDTOList": [
            {"key": "timestamp", "index": 0},
            {"key": "stressLevel", "index": 1},
        ],
        "stressValuesArray": [
            [1_784_852_400_000, -2],
            [1_784_852_460_000, 0],
            [1_784_852_520_000, None],
        ],
    }

    client = MagicMock()
    client._base_url = "https://garmin.example"
    client._request = AsyncMock(
        side_effect=lambda _method, url, **_kwargs: (
            [] if "/bodyBattery/events/" in url else payload
        )
    )
    result = await GarminHistorySource(client, _ImmediateGate()).async_fetch_details(
        date(2026, 7, 24), "stress"
    )

    assert isinstance(result, SourceSeries)
    assert result.presence == "present"
    assert [sample.value for sample in result.readings] == [-2.0, 0.0]


def test_normalize_health_events_preserves_explicit_fields_only() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_health_events.json").read_text())
    events = normalize_health_events(fixture, date(2026, 7, 24))
    assert len(events) == 2
    assert events[0].source == "MOVE_IQ"
    assert events[0].start is not None and events[0].end is not None
    assert events[1].event_type == "abnormalHeartRate"
    assert events[1].occurrence is not None


def test_normalize_health_events_uses_later_non_empty_alias() -> None:
    events = normalize_health_events(
        {
            "events": [],
            "dailyEvents": [
                {
                    "source": "MOVE_IQ",
                    "type": "walking",
                    "occurrenceTime": "2026-07-24T01:00:00Z",
                }
            ],
        },
        date(2026, 7, 24),
    )

    assert len(events) == 1
    assert events[0].event_type == "walking"


def test_activity_and_health_timestamp_aliases_keep_priority_and_timezone_rules() -> None:
    activity = normalize_activities(
        [
            {
                "activityId": 1,
                "activityType": "running",
                "startTime": "2026-07-24T01:00:00+02:00",
                "startTimeGMT": "2026-07-24T02:00:00.000",
                "startTimeLocal": "2026-07-24T03:00:00.000",
                "endTimeGMT": "2026-07-24T03:00:00.000",
                "endTime": "2026-07-24T04:00:00+02:00",
            }
        ],
        date(2026, 7, 24),
    )[0]
    health = normalize_health_events(
        {
            "events": [
                {
                    "startTime": "2026-07-24T01:00:00+02:00",
                    "startTimeGMT": "2026-07-24T02:00:00.000",
                    "endTimeGMT": "2026-07-24T03:00:00.000",
                    "endTime": "2026-07-24T04:00:00+02:00",
                    "occurrenceTimeGMT": "2026-07-24T02:30:00.000",
                }
            ]
        },
        date(2026, 7, 24),
    )[0]

    assert activity.start == datetime(2026, 7, 23, 23, tzinfo=UTC)
    assert activity.end == datetime(2026, 7, 24, 3, tzinfo=UTC)
    assert health.start == datetime(2026, 7, 23, 23, tzinfo=UTC)
    assert health.end == datetime(2026, 7, 24, 2, tzinfo=UTC)
    assert health.occurrence == datetime(2026, 7, 24, 2, 30, tzinfo=UTC)


def test_activity_timestamp_aliases_skip_unparseable_values_before_valid_aliases() -> None:
    activity = normalize_activities(
        [{
            "activityId": 2,
            "activityType": "running",
            "startTime": "not-a-timestamp",
            "startTimeGMT": "2026-07-24T02:00:00.000",
            "endTime": "also-not-a-timestamp",
            "endTimeGMT": "2026-07-24T03:00:00.000",
        }],
        date(2026, 7, 24),
    )[0]
    assert activity.start == datetime(2026, 7, 24, 2, tzinfo=UTC)
    assert activity.end == datetime(2026, 7, 24, 3, tzinfo=UTC)


@pytest.mark.parametrize("field", ["startTime", "endTime", "occurrenceTime"])
def test_health_event_rejects_non_empty_malformed_timestamp(field: str) -> None:
    with pytest.raises(HistorySchemaError):
        normalize_health_events(
            {"events": [{field: "not-a-timestamp"}]}, date(2026, 7, 24)
        )


@pytest.mark.parametrize("event", [{}, {"startTime": None}, {"startTime": ""}])
def test_health_event_allows_missing_null_and_empty_timestamp(event: dict[str, object]) -> None:
    assert normalize_health_events({"events": [event]}, date(2026, 7, 24)) == ()


def test_activity_normalization_rejects_reversed_source_interval() -> None:
    with pytest.raises(HistorySchemaError):
        normalize_activities(
            [{
                "activityId": 3,
                "activityType": "running",
                "startTime": "2026-07-24T03:00:00Z",
                "endTime": "2026-07-24T02:00:00Z",
            }],
            date(2026, 7, 24),
        )


@pytest.mark.asyncio
async def test_timed_activities_uses_pagination_and_deduplicates_overlap() -> None:
    client = MagicMock()
    client._base_url = "https://garmin.example"
    activity = {"activityId": 1, "activityType": "running", "startTime": "2026-07-24T23:00:00Z", "endTime": "2026-07-25T00:00:00Z"}
    client.get_activities = AsyncMock(side_effect=[
        [activity] * 100,
        [activity],
    ])
    source = GarminHistorySource(client, _ImmediateGate())
    result = await source.async_fetch_details(date(2026, 7, 24), "timed_activities")
    assert len(result) == 1
    assert result[0].activity_id == "1"
    assert client.get_activities.call_args_list[1].args == (100, 100)


@pytest.mark.asyncio
async def test_timed_activity_pagination_stops_on_older_page_and_excludes_move_iq() -> None:
    client = MagicMock()
    client.get_activities = AsyncMock(side_effect=[
        [{"activityId": 1, "activityType": "running", "startTime": "2026-07-24T10:00:00Z", "endTime": "2026-07-24T11:00:00Z"}] * 100,
        [{"activityId": 2 + index, "activityType": "walking", "source": "MOVE_IQ", "startTime": "2026-07-23T12:00:00Z", "durationInSeconds": 60} for index in range(100)],
    ])
    source = GarminHistorySource(client, _ImmediateGate())
    result = await source.async_fetch_details(date(2026, 7, 24), "timed_activities")
    assert len(result) == 1
    assert client.get_activities.await_count == 2
    assert client.get_activities.call_args_list[1].args == (100, 100)


@pytest.mark.asyncio
async def test_timed_activity_pagination_parses_naive_gmt_page_dates() -> None:
    fixture = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "garmin_activity_pagination.naive_gmt.json"
        ).read_text()
    )
    target_page = [fixture["target_activity"]] * 100
    older_page = [fixture["older_activity"]] * 100
    client = MagicMock()
    client.get_activities = AsyncMock(side_effect=[target_page, older_page, []])

    result = await GarminHistorySource(client, _ImmediateGate()).async_fetch_details(
        date(2026, 7, 24), "timed_activities"
    )

    assert [activity.activity_id for activity in result] == ["target-1"]
    assert [call.args for call in client.get_activities.call_args_list] == [
        (0, 100),
        (100, 100),
    ]


@pytest.mark.asyncio
async def test_timed_activity_pagination_uses_valid_gmt_when_start_time_is_bad() -> None:
    old_activity = {
        "activityId": "old-1",
        "activityType": "running",
        "startTime": "not-a-timestamp",
        "startTimeGMT": "2026-07-23T23:30:00.000",
        "endTimeGMT": "2026-07-24T00:30:00.000",
    }
    client = MagicMock()
    client.get_activities = AsyncMock(side_effect=[[old_activity] * 100])

    result = await GarminHistorySource(client, _ImmediateGate()).async_fetch_details(
        date(2026, 7, 24), "timed_activities"
    )

    assert result == ()
    assert client.get_activities.await_count == 1


@pytest.mark.asyncio
async def test_timed_activity_pagination_prioritizes_raw_local_calendar_date() -> None:
    fixture = json.loads(
        (
            Path(__file__).parent
            / "fixtures"
            / "garmin_activity_pagination.local_calendar.json"
        ).read_text()
    )
    first_page = [fixture["local_activity"]] * 100
    second_page = [fixture["following_activity"]] * 100
    client = MagicMock()
    client.get_activities = AsyncMock(side_effect=[first_page, second_page, []])

    result = await GarminHistorySource(client, _ImmediateGate()).async_fetch_details(
        date(2026, 1, 1), "timed_activities"
    )

    assert [activity.activity_id for activity in result] == [
        "local-day-1",
        "local-day-2",
    ]
    assert [call.args for call in client.get_activities.call_args_list] == [
        (0, 100),
        (100, 100),
        (200, 100),
    ]


def test_normalize_activities_rejects_explicit_event_families() -> None:
    payload = [
        {"activityId": 1, "activityType": "walking", "eventTypeKey": "dailyEvent", "startTime": "2026-07-24T10:00:00Z", "durationInSeconds": 60},
        {"activityId": 2, "activityType": "walking", "sourceName": "Garmin MOVE_IQ event", "startTime": "2026-07-24T10:00:00Z", "durationInSeconds": 60},
        {"activityId": 3, "activityType": "running", "eventTime": "2026-07-24T10:00:00Z", "startTime": "2026-07-24T10:00:00Z"},
        {"activityId": 5, "activityType": "running", "eventCategory": "daily", "startTime": "2026-07-24T10:00:00Z", "durationInSeconds": 60},
        {"activityId": 4, "activityType": "running", "source": "Garmin", "startTime": "2026-07-24T10:00:00Z", "endTime": "2026-07-24T11:00:00Z"},
    ]
    result = normalize_activities(payload, date(2026, 7, 24))
    assert [item.activity_id for item in result] == ["4"]


@pytest.mark.asyncio
async def test_timed_activity_preserves_source_calendar_date_and_source_instants_without_duration() -> None:
    client = GarminClient(MagicMock())
    client._request = AsyncMock(
        return_value=[
            {
                "activityId": 2,
                "activityType": {"typeKey": "walking"},
                "startTimeGMT": "2025-12-31T22:30:00.000",
                "startTimeLocal": "2026-01-01T00:30:00.000",
                "endTimeGMT": "2025-12-31T22:31:00.000",
            }
        ]
    )
    source = GarminHistorySource(client, _ImmediateGate())
    result = await source.async_fetch_details(date(2026, 1, 1), "timed_activities")
    assert len(result) == 1
    assert result[0].calendar_date == date(2026, 1, 1)
    assert result[0].start == datetime(2025, 12, 31, 22, 30, tzinfo=UTC)
    assert result[0].end == datetime(2025, 12, 31, 22, 31, tzinfo=UTC)


@pytest.mark.asyncio
async def test_unscoped_activity_without_source_calendar_date_uses_source_instant_utc_date() -> None:
    client = MagicMock()
    client.get_activities = AsyncMock(
        return_value=[
            {
                "activityId": 3,
                "activityType": "running",
                "startTime": datetime(2026, 1, 1, 23, 30, tzinfo=UTC),
                "durationInSeconds": 60,
            }
        ]
    )

    source = GarminHistorySource(client, _ImmediateGate())
    wrong_day = await source.async_fetch_details(
        date(2026, 1, 2), "timed_activities"
    )
    utc_day = await source.async_fetch_details(
        date(2026, 1, 1), "timed_activities"
    )

    assert wrong_day == ()
    assert len(utc_day) == 1
    assert utc_day[0].calendar_date == date(2026, 1, 1)


@pytest.mark.parametrize("local_start_state", ["missing", "null", "empty"])
def test_activity_empty_or_null_local_start_uses_aware_start_utc_date(
    local_start_state: str,
) -> None:
    item = {
        "activityId": 6,
        "activityType": "running",
        "startTime": "2026-01-01T23:30:00+02:00",
        "durationInSeconds": 60,
    }
    if local_start_state == "null":
        item["startTimeLocal"] = None
    elif local_start_state == "empty":
        item["startTimeLocal"] = ""

    activity = normalize_activities([item], date(2026, 1, 2))[0]

    assert activity.calendar_date == date(2026, 1, 1)


def test_activity_non_empty_malformed_local_start_fails() -> None:
    with pytest.raises(HistorySchemaError):
        normalize_activities(
            [
                {
                    "activityId": 7,
                    "activityType": "running",
                    "startTime": "2026-01-01T23:30:00+02:00",
                    "startTimeLocal": "not-a-timestamp",
                    "durationInSeconds": 60,
                }
            ],
            date(2026, 1, 2),
        )


def test_activity_calendar_date_revision_keeps_logical_identity_stable() -> None:
    first = normalize_activities(
        [{
            "activityId": 4,
            "activityType": "running",
            "startTime": "2026-12-31T22:30:00Z",
            "startTimeLocal": "2026-12-31T23:30:00+01:00",
            "durationInSeconds": 60,
        }],
        date(2026, 12, 31),
    )[0]
    corrected = normalize_activities(
        [{
            "activityId": 4,
            "activityType": "running",
            "startTime": "2026-12-31T22:30:00Z",
            "startTimeLocal": "2027-01-01T00:30:00+02:00",
            "durationInSeconds": 60,
        }],
        date(2027, 1, 1),
    )[0]

    assert first.logical_id == corrected.logical_id
    assert first.revision != corrected.revision
    assert first.calendar_date == date(2026, 12, 31)
    assert corrected.calendar_date == date(2027, 1, 1)


def test_health_event_revision_keeps_identity_and_rejects_bounds() -> None:
    first = normalize_health_events({"events": [{"source": "A", "type": "x", "category": "one", "occurrenceTime": "2026-07-24T00:00:00Z"}]}, date(2026, 7, 24))[0]
    revised = normalize_health_events({"events": [{"source": "B", "type": "x", "category": "two", "occurrenceTime": "2026-07-24T00:00:00Z"}]}, date(2026, 7, 24))[0]
    assert first.logical_id == revised.logical_id
    assert first.revision != revised.revision
    with pytest.raises(HistorySchemaError):
        normalize_health_events({"events": [{}] * 513}, date(2026, 7, 24))
    with pytest.raises(HistorySchemaError):
        normalize_health_events({"events": [{"source": "x" * 65}]}, date(2026, 7, 24))
    record = health_event_record(first)
    record["category"] = "changed"
    with pytest.raises(HistorySchemaError):
        health_event_from_record(record)


def test_normalize_health_events_keeps_empty_structures_absent() -> None:
    """An empty source structure is not converted into a synthetic event."""
    target = date(2026, 7, 24)
    assert normalize_health_events({"events": None}, target) == ()
    assert normalize_health_events({"events": []}, target) == ()
    assert normalize_health_events(
        {"events": [], "dailyEvents": None, "bodyBatteryEvents": {}}, target
    ) == ()
    assert normalize_health_events({}, target) == ()
    assert normalize_health_events({"events": [{}]}, target) == ()

    with pytest.raises(HistorySchemaError):
        normalize_health_events({"events": "malformed"}, target)
    with pytest.raises(HistorySchemaError):
        normalize_health_events({"events": [], "dailyEvents": "malformed"}, target)


@pytest.mark.parametrize(
    "values",
    [
        [None],
        [],
        [{"metadata": "only"}],
    ],
)
def test_abnormal_hr_without_source_instant_remains_absent(values: list) -> None:
    assert normalize_health_events(
        {"abnormalHRValuesArray": values}, date(2026, 7, 24)
    ) == ()


def test_normalize_pair_series_rejects_incompatible_known_series() -> None:
    """A changed known field fails only this family/date."""
    with pytest.raises(HistorySchemaError):
        normalize_pair_series(
            {"heartRateValues": {"not": "an array"}},
            values_key="heartRateValues",
            descriptor_keys=("heartRateValueDescriptors",),
            value_keys=("heartRate",),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric", "payload", "expected"),
    [
        ("heart_rate", [["2026-07-24T01:00:00Z", 61]], [61.0]),
        ("stress", [{"timestamp": "2026-07-24T01:00:00Z", "stressLevel": 14}], [14.0]),
    ],
)
async def test_top_level_heart_rate_and_stress_lists_are_normalized(
    metric: str, payload: list, expected: list[float]
) -> None:
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client.get_user_profile = AsyncMock(return_value=MagicMock(display_name="user"))
    client._request = AsyncMock(return_value=payload)
    source = GarminHistorySource(client, _ImmediateGate())

    result = await source.async_fetch_details(date(2026, 7, 24), metric)

    assert isinstance(result, SourceSeries)
    assert [sample.value for sample in result.readings] == expected
    assert result.presence == "present"


@pytest.mark.asyncio
@pytest.mark.parametrize("metric", ["heart_rate", "stress"])
async def test_null_intraday_payloads_remain_source_series(metric: str) -> None:
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client.get_user_profile = AsyncMock(return_value=MagicMock(display_name="user"))
    source = GarminHistorySource(client, _ImmediateGate())

    client._request = AsyncMock(return_value=None)
    result = await source.async_fetch_details(date(2026, 7, 24), metric)
    assert isinstance(result, SourceSeries)
    assert result.readings == ()
    assert result.presence == "null"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric", "payload"),
    [
        ("heart_rate", {"heartRateValues": "malformed"}),
        ("stress", {"stressValuesArray": 42}),
    ],
)
async def test_malformed_known_intraday_payload_fails_closed(metric: str, payload: dict) -> None:
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client.get_user_profile = AsyncMock(return_value=MagicMock(display_name="user"))
    client._request = AsyncMock(return_value=payload)
    source = GarminHistorySource(client, _ImmediateGate())

    with pytest.raises(HistorySchemaError):
        await source.async_fetch_details(date(2026, 7, 24), metric)


@pytest.mark.parametrize(
    ("normalizer", "args"),
    [
        (normalize_steps, ()),
        (normalize_floors, ()),
        (normalize_intensity, ("moderate",)),
        (normalize_respiration, ()),
        (normalize_spo2, ("single",)),
    ],
)
def test_known_numeric_scalar_payloads_fail_closed(normalizer, args) -> None:
    """A malformed scalar cannot become an unsupported completed family."""
    with pytest.raises(HistorySchemaError):
        normalizer("malformed", date(2026, 7, 24), *args)


def test_all_null_numeric_arrays_are_not_present() -> None:
    target = date(2026, 7, 24)

    pair = normalize_respiration(
        {"respirationValuesArray": [["2026-07-24T01:00:00Z", None], None]},
        target,
    )
    segmented = normalize_steps(
        {"stepsValuesArray": [["2026-07-24T01:00:00Z", None]]}, target
    )
    hrv = parse_hrv_data(
        {"hrvReadings": [{"readingTimeGMT": "2026-07-24T01:00:00Z", "hrvValue": None}]},
        target,
    )
    spo2 = normalize_spo2(
        {"spO2SingleValues": [{"readingTime": "2026-07-24T01:00:00Z", "spO2": None}]},
        target,
        "single",
    )
    assert pair.presence == segmented.presence == spo2.presence == hrv.presence == "all-null"


def test_snapshot_aliases_prefer_non_null_values() -> None:
    target = date(2026, 7, 24)

    snapshot = normalize_snapshot(
        {"vo2Max": None, "vo2MaxValue": 47.2}, target, TRAINING_STATUS_FIELDS
    )
    assert snapshot.fields["vo2_max"] == ("present", 47.2)
    assert normalize_snapshot(
        {"vo2Max": None, "vo2MaxValue": None}, target, TRAINING_STATUS_FIELDS
    ).fields["vo2_max"] == ("null", None)
    assert normalize_snapshot({}, target, TRAINING_STATUS_FIELDS).fields["vo2_max"] == (
        "absent",
        None,
    )


def test_snapshot_timestamp_aliases_prefer_non_null_values() -> None:
    snapshot = normalize_snapshot(
        {"timestamp": None, "startTime": "2026-07-24T01:00:00Z"},
        date(2026, 7, 24),
        DAILY_SUMMARY_FIELDS,
    )
    assert snapshot.timestamp == datetime(2026, 7, 24, 1, tzinfo=UTC)

    with pytest.raises(HistorySchemaError):
        normalize_snapshot(
            {"timestamp": "not-a-timestamp", "startTime": "2026-07-24T01:00:00Z"},
            date(2026, 7, 24),
            DAILY_SUMMARY_FIELDS,
        )


def test_snapshot_all_null_timestamp_aliases_fall_back_to_target_date() -> None:
    """Null timestamp aliases are calendar metadata, not a schema failure."""
    target = date(2026, 7, 24)

    snapshot = normalize_snapshot(
        {
            "timestamp": None,
            "startTime": None,
            "calendarDate": None,
            "acuteLoad": None,
        },
        target,
        TRAINING_STATUS_FIELDS,
    )

    assert snapshot.timestamp == datetime(2026, 7, 23, 16, tzinfo=UTC)
    assert snapshot.raw_timestamp == target.isoformat()
    assert snapshot.fields["acute_load"] == ("null", None)
    assert snapshot.fields["vo2_max"] == ("absent", None)


def test_object_series_timestamp_aliases_prefer_non_null_values() -> None:
    series = normalize_respiration(
        {
            "respirationValuesArray": [
                {
                    "timestamp": None,
                    "time": "2026-07-24T01:00:00Z",
                    "respirationValue": 12,
                }
            ]
        },
        date(2026, 7, 24),
    )
    assert series.readings[0].timestamp == datetime(2026, 7, 24, 1, tzinfo=UTC)

    with pytest.raises(HistorySchemaError):
        normalize_respiration(
            {
                "respirationValuesArray": [
                    {
                        "timestamp": "not-a-timestamp",
                        "time": "2026-07-24T01:00:00Z",
                        "respirationValue": 12,
                    }
                ]
            },
            date(2026, 7, 24),
        )


def test_descriptor_series_timestamp_and_value_aliases_prefer_non_null_values() -> None:
    series = normalize_respiration(
        {
            "respirationValuesArray": [
                {
                    "timestamp": None,
                    "time": "2026-07-24T01:00:00Z",
                    "respiration": None,
                    "respirationValue": 12,
                }
            ],
        },
        date(2026, 7, 24),
    )
    assert [(sample.timestamp, sample.value) for sample in series.readings] == [
        (datetime(2026, 7, 24, 1, tzinfo=UTC), 12.0)
    ]


def test_numeric_activity_records_choose_first_non_null_alias() -> None:
    activity = normalize_activities(
        [{
            "activityId": 1,
            "activityType": "running",
            "startTime": "2026-07-24T01:00:00Z",
            "durationInSeconds": None,
            "duration": 60,
            "trainingEffect": None,
            "aerobicTrainingEffect": 2.1,
        }],
        date(2026, 7, 24),
    )[0]

    assert activity.duration_seconds == 60.0
    assert activity.training_effect == 2.1


def test_mixed_null_object_series_preserve_respiration_and_spo2_samples() -> None:
    target = date(2026, 7, 24)
    respiration = normalize_respiration(
        {
            "respirationValuesArray": [
                None,
                {"timestamp": "2026-07-24T01:00:00Z", "respirationValue": 12.5},
            ]
        },
        target,
    )
    assert [sample.value for sample in respiration.readings] == [12.5]
    assert respiration.presence == "present"

    for variant, array_key, value_key, value in (
        ("single", "spO2SingleValues", "spO2", 98),
        ("continuous", "continuousReadingDTOList", "reading", 96),
        ("hourly", "spO2HourlyAverages", "average", 95),
    ):
        spo2 = normalize_spo2(
            {array_key: [None, {"readingTime": "2026-07-24T01:05:00Z", value_key: value}]},
            target,
            variant,
        )
        assert [sample.value for sample in spo2.readings] == [float(value)]
        assert spo2.presence == "present"


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


def test_hrv_measurement_aliases_prefer_non_null_values() -> None:
    parsed = parse_hrv_data(
        {
            "hrvReadings": [{
                "readingTimeGMT": None,
                "readingTimeGmt": "2026-07-24T01:00:00Z",
                "hrvValue": None,
                "value": 42,
            }]
        },
        date(2026, 7, 24),
    )
    assert parsed.readings[0].value == 42.0
    assert parsed.readings[0].timestamp == datetime(2026, 7, 24, 1, tzinfo=UTC)

    with pytest.raises(HistorySchemaError):
        parse_hrv_data(
            {
                "hrvReadings": [{
                    "readingTimeGMT": "bad",
                    "readingTimeGmt": "2026-07-24T01:00:00Z",
                    "hrvValue": 42,
                }]
            },
            date(2026, 7, 24),
        )


def test_hrv_aliases_use_first_non_null_and_reject_first_non_null_malformed_values() -> None:
    parsed = parse_hrv_data(
        {
            "hrvReadings": [
                {
                    "readingTimeGMT": None,
                    "readingTimeGmt": "2026-07-24T01:00:00Z",
                    "hrvValue": None,
                    "value": 42,
                }
            ]
        },
        date(2026, 7, 24),
    )
    assert [sample.value for sample in parsed.readings] == [42.0]

    with pytest.raises(HistorySchemaError):
        parse_hrv_data(
            {
                "hrvReadings": [
                    {
                        "readingTimeGMT": "malformed",
                        "readingTimeGmt": "2026-07-24T01:00:00Z",
                        "hrvValue": 42,
                    }
                ]
            },
            date(2026, 7, 24),
        )


def test_descriptor_aliases_use_first_non_null_and_fail_closed_when_selected_alias_is_bad() -> None:
    payload = {
        "heartRateValueDescriptors": None,
        "heartRateValueDescriptorsDTOList": [
            {"key": "timestamp", "index": 0},
            {"key": "heartRate", "index": 1},
        ],
        "heartRateValues": [["2026-07-24T01:00:00Z", 61]],
    }
    assert normalize_pair_series(
        payload,
        values_key="heartRateValues",
        descriptor_keys=("heartRateValueDescriptors", "heartRateValueDescriptorsDTOList"),
        value_keys=("heartRate",),
    )[0].value == 61.0

    with pytest.raises(HistorySchemaError):
        normalize_pair_series(
            {
                "heartRateValueDescriptors": "malformed",
                "heartRateValueDescriptorsDTOList": payload["heartRateValueDescriptorsDTOList"],
                "heartRateValues": payload["heartRateValues"],
            },
            values_key="heartRateValues",
            descriptor_keys=("heartRateValueDescriptors", "heartRateValueDescriptorsDTOList"),
            value_keys=("heartRate",),
        )


def test_empty_descriptor_alias_does_not_mask_later_descriptor() -> None:
    samples = normalize_pair_series(
        {
            "heartRateValueDescriptors": [],
            "heartRateValueDescriptorsDTOList": [
                {"key": "timestamp", "index": 0},
                {"key": "heartRate", "index": 1},
            ],
            "heartRateValues": [["2026-07-24T01:00:00Z", 61]],
        },
        values_key="heartRateValues",
        descriptor_keys=("heartRateValueDescriptors", "heartRateValueDescriptorsDTOList"),
        value_keys=("heartRate",),
    )
    assert [sample.value for sample in samples] == [61.0]


def test_hrv_mixed_null_rows_preserve_valid_objects_and_reject_malformed_objects() -> None:
    parsed = parse_hrv_data(
        {
            "hrvReadings": [
                None,
                {"readingTime": "2026-07-24T01:00:00Z", "hrvValue": 43},
                {"readingTime": "2026-07-24T01:01:00Z", "hrvValue": None},
            ]
        },
        date(2026, 7, 24),
    )
    assert [sample.value for sample in parsed.readings] == [43.0]

    with pytest.raises(HistorySchemaError):
        parse_hrv_data(
            {"hrvReadings": [None, {"readingTime": "2026-07-24T01:00:00Z"}]},
            date(2026, 7, 24),
        )


def test_date_only_calendar_metadata_remains_supported() -> None:
    snapshot = normalize_snapshot(
        {"calendarDate": "2026-07-24", "vo2Max": 47.2},
        date(2026, 7, 24),
        TRAINING_STATUS_FIELDS,
    )
    assert snapshot.timestamp == datetime(2026, 7, 23, 16, tzinfo=UTC)


def test_hrv_nested_summary_is_separate_and_typed() -> None:
    parsed = parse_hrv_data({"hrvReadings": [], "hrvSummary": {"status": "balanced", "lastNightAvg": 48, "lastNight5MinHigh": 72, "weeklyAvg": 50, "baseline": {"low": 40, "high": 60}}}, date(2026, 7, 24))
    assert parsed.readings == ()
    assert parsed.summary is not None
    assert parsed.summary.baseline == {"low": 40.0, "high": 60.0}


def test_body_battery_singular_descriptor_is_supported() -> None:
    samples = normalize_body_battery({"bodyBatteryValueDescriptorDTOList": [{"key": "timestamp", "index": 0}, {"key": "bodyBatteryValue", "index": 1}], "bodyBatteryValuesArray": [["2026-07-24T01:00:00Z", 0]]}, date(2026, 7, 24))
    assert samples[0].value == 0


def test_top_level_list_segmented_payloads_are_present_and_normalized() -> None:
    target = date(2026, 7, 24)
    steps = normalize_steps([
        {"timestamp": "2026-07-24T01:00:00Z", "steps": 0},
        {"timestamp": "2026-07-24T01:15:00Z", "steps": 12},
    ], target)
    floors = normalize_floors([
        {"time": "2026-07-24T01:00:00Z", "floors": 2},
    ], target)
    intensity = normalize_intensity([
        {"start": "2026-07-24T01:00:00Z", "moderateIntensityMinutes": 1},
    ], target, "moderate")

    assert [sample.value for sample in steps.readings] == [0, 12]
    assert [sample.value for sample in floors.readings] == [2]
    assert [sample.value for sample in intensity.readings] == [1]
    assert steps.presence == floors.presence == intensity.presence == "present"


def test_segmented_object_records_choose_first_non_null_value_alias() -> None:
    parsed = normalize_steps(
        {"data": [{"timestamp": "2026-07-24T01:00:00Z", "steps": None, "value": 12}]},
        date(2026, 7, 24),
    )
    assert [sample.value for sample in parsed.readings] == [12.0]


@pytest.mark.asyncio
async def test_top_level_list_body_battery_is_present() -> None:
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client._request = AsyncMock(return_value=[{
        "calendarDate": "2026-07-24",
        "bodyBatteryValuesArray": [["2026-07-24T01:00:00Z", 42]],
    }])
    source = GarminHistorySource(client, _ImmediateGate())

    result = await source.async_fetch_details(date(2026, 7, 24), "body_battery")

    assert isinstance(result, SourceSeries)
    assert [sample.value for sample in result.readings] == [42]
    assert result.presence == "present"


@pytest.mark.asyncio
async def test_body_battery_all_null_array_is_not_present() -> None:
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client._request = AsyncMock(return_value=[{
        "calendarDate": "2026-07-24",
        "bodyBatteryValuesArray": [["2026-07-24T01:00:00Z", None]],
    }])
    source = GarminHistorySource(client, _ImmediateGate())

    result = await source.async_fetch_details(date(2026, 7, 24), "body_battery")

    assert isinstance(result, SourceSeries)
    assert result.presence == "all-null"


@pytest.mark.parametrize(
    ("payload", "expected_presence"),
    [
        (None, "null"),
        ([], "empty"),
        ({}, "missing"),
        ({"status": "returned-empty"}, "returned-empty"),
    ],
)
def test_segmented_payload_presence_states_are_bounded(payload, expected_presence) -> None:
    assert normalize_steps(payload, date(2026, 7, 24)).presence == expected_presence


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


def test_segmented_rows_are_not_daily_totals_and_total_presence_is_distinct() -> None:
    target = date(2026, 7, 24)
    segmented = normalize_steps(
        {"data": [{"timestamp": "2026-07-24T01:00:00Z", "steps": 12}]}, target
    )
    null_total = normalize_steps({"summary": {"totalSteps": None}}, target)
    zero_total = normalize_steps({"summary": {"totalSteps": 0}}, target)

    assert segmented.readings[0].value == 12.0
    assert segmented.totals is None
    assert segmented.total_presence == {"totalSteps": "absent"}
    assert null_total.totals is None
    assert null_total.total_presence == {"totalSteps": "null"}
    assert zero_total.totals == {"totalSteps": 0.0}
    assert zero_total.total_presence == {"totalSteps": "present"}


def test_known_segmented_totals_preserve_zero_null_and_revision_values() -> None:
    target = date(2026, 7, 24)
    first = normalize_floors({"summary": {"totalFloors": 0}}, target)
    intensity = normalize_intensity(
        {"summary": {"totalIntensityMinutes": None}}, target, "moderate"
    )
    revised = normalize_floors({"summary": {"totalFloors": 3}}, target)

    assert first.totals == {"totalFloors": 0.0}
    assert first.total_presence["totalFloors"] == "present"
    assert intensity.total_presence["totalIntensityMinutes"] == "null"
    assert revised.totals == {"totalFloors": 3.0}


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
    assert normalize_respiration([], date(2026, 7, 24)).presence == "empty"
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
    assert normalize_snapshot({"trainingStatus": {"recoveryTime": None}}, date(2026, 7, 24), TRAINING_STATUS_FIELDS).fields["recovery_time"] == ("null", None)
    assert normalize_snapshot({"trainingStatus": {}}, date(2026, 7, 24), TRAINING_STATUS_FIELDS).fields["recovery_time"] == ("absent", None)
    assert training.fields["vo2_max"] == ("absent", None)
    with pytest.raises(HistorySchemaError):
        normalize_snapshot({"acuteLoad": "drift"}, date(2026, 7, 24), TRAINING_STATUS_FIELDS)
    with pytest.raises(HistorySchemaError):
        normalize_snapshot(
            {"timestamp": "malformed", "acuteLoad": 1},
            date(2026, 7, 24),
            TRAINING_STATUS_FIELDS,
        )


def test_current_daily_summary_retains_floor_and_intensity_totals() -> None:
    """Live daily totals remain separate Garmin-computed snapshots."""
    summary = normalize_snapshot(
        {
            "calendarDate": "2026-08-01",
            "floorsAscended": 8.0,
            "floorsDescended": 7.0,
            "floorsAscendedInMeters": 24.0,
            "floorsDescendedInMeters": 21.0,
            "moderateIntensityMinutes": 32,
            "vigorousIntensityMinutes": 9,
        },
        date(2026, 8, 1),
        DAILY_SUMMARY_FIELDS,
    )

    assert {key: state for key, (state, _value) in summary.fields.items()} == {
        "abnormal_heart_rate_alerts": "absent",
        "floors_ascended": "present",
        "floors_descended": "present",
        "floors_ascended_meters": "present",
        "floors_descended_meters": "present",
        "intensity_moderate": "present",
        "intensity_vigorous": "present",
    }


def test_sanitized_beta8_capture_shapes_cover_repaired_families() -> None:
    """The repaired normalizers consume representative offline capture shapes."""
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "garmin_beta8_capture_shapes.json").read_text()
    )
    target = date(2026, 8, 1)

    sessions = parse_sleep_sessions(fixture["sleep"], target)
    events = normalize_health_events(fixture["body_battery_events"], target)
    summary = normalize_snapshot(
        fixture["daily_summary"], target, DAILY_SUMMARY_FIELDS
    )
    training = normalize_training_status(fixture["training_status"], target)

    assert len(sessions) == 1
    assert {stream.metric for stream in sessions[0].streams} == {
        "heart_rate",
        "hrv",
        "body_battery",
        "stress",
        "respiration",
        "spo2",
        "movement",
    }
    assert len(events) == 1
    assert summary.fields["floors_ascended"] == ("present", 8.0)
    assert isinstance(training, TrainingDeviceSnapshots)
    assert training.snapshots["101"].fields["acute_load"] == (
        "present",
        420.0,
    )


def test_current_training_status_retains_every_device_snapshot() -> None:
    """Aggregated training status is flattened per returned Garmin device."""
    payload = {
        "mostRecentTrainingStatus": {
            "latestTrainingStatusData": {
                "101": {
                    "deviceId": 101,
                    "calendarDate": "2026-08-01",
                    "primaryTrainingDevice": True,
                    "acuteTrainingLoadDTO": {
                        "dailyTrainingLoadAcute": 420,
                        "dailyTrainingLoadChronic": 560,
                        "dailyAcuteChronicWorkloadRatio": 0.75,
                    },
                    "fitnessTrend": 2,
                },
                "202": {
                    "deviceId": 202,
                    "calendarDate": "2026-08-01",
                    "primaryTrainingDevice": False,
                    "acuteTrainingLoadDTO": {
                        "dailyTrainingLoadAcute": 210,
                        "dailyTrainingLoadChronic": 350,
                        "dailyAcuteChronicWorkloadRatio": 0.6,
                    },
                    "fitnessTrend": 1,
                },
            }
        },
        "mostRecentVO2Max": {
            "generic": {"deviceId": 101, "vo2MaxValue": 47.2},
            "cycling": {"deviceId": 202, "vo2MaxValue": 51.0},
        },
    }

    result = normalize_training_status(payload, date(2026, 8, 1))

    assert isinstance(result, TrainingDeviceSnapshots)
    assert set(result.snapshots) == {"101", "202"}
    assert result.snapshots["101"].fields["acute_load"] == ("present", 420.0)
    assert result.snapshots["101"].fields["vo2_max"] == ("present", 47.2)
    assert result.snapshots["202"].fields["vo2_max"] == ("present", 51.0)
    assert result.snapshots["202"].fields["recovery_time"] == ("absent", None)


def test_date_only_snapshots_use_the_utc_plus_eight_calendar_bucket() -> None:
    """Summaries retain Source Calendar Date without inventing a Source Instant."""
    calendar_date = date(2027, 1, 1)

    bucketed = normalize_snapshot(
        {"calendarDate": calendar_date.isoformat(), "acuteLoad": 42},
        calendar_date,
        TRAINING_STATUS_FIELDS,
    )
    aware = normalize_snapshot(
        {
            "calendarDate": calendar_date.isoformat(),
            "startTime": "2027-01-01T00:00:00+09:00",
            "acuteLoad": 43,
        },
        calendar_date,
        TRAINING_STATUS_FIELDS,
    )

    assert bucketed.timestamp == datetime(2026, 12, 31, 16, tzinfo=UTC)
    assert bucketed.raw_timestamp == calendar_date.isoformat()
    assert bucketed.calendar_date == calendar_date
    assert aware.timestamp == datetime(2026, 12, 31, 15, tzinfo=UTC)
    assert aware.raw_timestamp == "2027-01-01T00:00:00+09:00"
    assert aware.calendar_date == calendar_date


def test_snapshot_normalization_uses_sanitized_fixture() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_summary_training.json").read_text())
    daily = normalize_snapshot(fixture["daily_summary"], date(2026, 7, 24), DAILY_SUMMARY_FIELDS)
    training = normalize_snapshot(fixture["training_status"], date(2026, 7, 24), TRAINING_STATUS_FIELDS)
    assert fixture["_cardinality"]["training_status_fields"] == len(training.fields)
    assert daily.fields["abnormal_heart_rate_alerts"] == ("present", 2.0)
    assert training.fields["vo2_max"] == ("present", 47.2)
    assert training.fields["acute_load"] == ("present", 42.0)
    assert training.fields["chronic_load"] == ("present", 56.0)
    assert training.fields["load_balance"] == ("present", -14.0)
    assert training.fields["acwr"] == ("present", 0.75)
    assert training.fields["fitness_trend"] == ("present", 1.5)
    assert training.fields["recovery_time"] == ("present", 3600.0)


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric", "family", "method", "payload"),
    (
        ("daily_summary", "stress", "_get_user_summary_raw", {}),
        ("training_status", "training", "get_training_status", {}),
        ("nightly_hrv", "hrv", "_get_hrv_data_raw", {}),
        (
            "sleep_sessions",
            "sleep",
            "_get_sleep_data_raw",
            {"dailySleepDTO": None},
        ),
    ),
)
async def test_daily_status_reuses_shared_endpoint_payload(
    metric: str, family: str, method: str, payload: dict
) -> None:
    client = MagicMock()
    request = AsyncMock(return_value=payload)
    setattr(client, method, request)
    source = GarminHistorySource(client, _ImmediateGate())
    target = date(2026, 7, 24)

    await source.async_fetch_details(target, metric)
    assert await source.async_fetch_daily_status_payload(target, family) is payload

    request.assert_awaited_once()


@pytest.mark.asyncio
async def test_current_body_battery_event_enriches_numeric_series_and_event() -> None:
    """One live-shaped event response feeds both statistics and Calendar data."""
    target = date(2026, 8, 1)
    event_payload = [
        {
            "event": {
                "eventStartTimeGmt": "2026-08-01T01:00:00.0",
                "durationInMilliseconds": 900_000,
                "eventType": "ACTIVITY",
                "feedbackType": "POSITIVE",
            },
            "bodyBatteryValueDescriptorsDTOList": [
                {
                    "bodyBatteryValueDescriptorIndex": 0,
                    "bodyBatteryValueDescriptorKey": "timestamp",
                },
                {
                    "bodyBatteryValueDescriptorIndex": 1,
                    "bodyBatteryValueDescriptorKey": "bodyBatteryLevel",
                },
            ],
            "bodyBatteryValuesArray": [
                [1_785_524_400_000, 70],
                [1_785_525_300_000, 72],
            ],
            "stressValueDescriptorsDTOList": [
                {"index": 0, "key": "timestamp"},
                {"index": 1, "key": "stressLevel"},
            ],
            "stressValuesArray": [
                [1_785_524_400_000, 20],
                [1_785_525_300_000, 18],
            ],
        }
    ]
    client = MagicMock(_base_url="https://connect.garmin.test/gc-api")

    async def request(_method, url, **_kwargs):
        if "/bodyBattery/events/" in url:
            return event_payload
        if "/dailyStress/" in url:
            return {
                "stressValueDescriptorsDTOList": [
                    {"index": 0, "key": "timestamp"},
                    {"index": 1, "key": "stressLevel"},
                ],
                "stressValuesArray": [[1_785_524_400_000, 20]],
            }
        if "/bodyBattery/reports/daily" in url:
            return [
                {
                    "calendarDate": target.isoformat(),
                    "bodyBatteryValueDescriptorDTOList": [
                        {"index": 0, "key": "timestamp"},
                        {"index": 1, "key": "bodyBatteryLevel"},
                    ],
                    "bodyBatteryValuesArray": [[1_785_524_400_000, 70]],
                }
            ]
        raise AssertionError(url)

    client._request = AsyncMock(side_effect=request)
    source = GarminHistorySource(client, _ImmediateGate())

    stress = await source.async_fetch_details(target, "stress")
    body_battery = await source.async_fetch_details(target, "body_battery")
    events = await source.async_fetch_details(target, "health_events_body_battery")

    assert isinstance(stress, SourceSeries)
    assert isinstance(body_battery, SourceSeries)
    assert len(stress.readings) == 2
    assert len(body_battery.readings) == 2
    assert len(events) == 1
    assert events[0].event_type == "ACTIVITY"
    assert events[0].start == datetime(2026, 8, 1, 1, tzinfo=UTC)
    assert events[0].end == datetime(2026, 8, 1, 1, 15, tzinfo=UTC)
    assert sum(
        "/bodyBattery/events/" in call.args[1]
        for call in client._request.await_args_list
    ) == 1


@pytest.mark.asyncio
async def test_shared_daily_endpoints_are_requested_once_per_source_instance() -> None:
    """Sibling metrics reuse one captured payload instead of polling Garmin again."""
    target = date(2026, 8, 1)
    client = MagicMock(_base_url="https://connect.garmin.test/gc-api")
    client._request = AsyncMock(return_value={})
    source = GarminHistorySource(client, _ImmediateGate())

    for metric in (
        "intensity_moderate",
        "intensity_vigorous",
        "respiration_raw",
        "respiration_average",
        "spo2_single",
        "spo2_continuous",
        "spo2_hourly",
    ):
        await source.async_fetch_details(target, metric)

    urls = [call.args[1] for call in client._request.await_args_list]
    assert sum("/daily/im/" in url for url in urls) == 1
    assert sum("/daily/respiration/" in url for url in urls) == 1
    assert sum("/daily/spo2/" in url for url in urls) == 1


@pytest.mark.asyncio
async def test_shared_daily_endpoint_failure_is_cached_for_one_source_instance() -> None:
    """Sibling metrics share one endpoint failure but a new sync source retries."""
    target = date(2026, 8, 1)
    client = MagicMock(_base_url="https://connect.garmin.test/gc-api")
    client._request = AsyncMock(side_effect=OSError("temporary network failure"))
    source = GarminHistorySource(client, _ImmediateGate())

    with pytest.raises(OSError, match="temporary network failure"):
        await source.async_fetch_details(target, "respiration_raw")
    with pytest.raises(OSError, match="temporary network failure"):
        await source.async_fetch_details(target, "respiration_average")

    assert client._request.await_count == 1

    restarted_source = GarminHistorySource(client, _ImmediateGate())
    with pytest.raises(OSError, match="temporary network failure"):
        await restarted_source.async_fetch_details(target, "respiration_raw")
    assert client._request.await_count == 2
