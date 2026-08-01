"""Tests for structured Garmin sleep sessions."""

import json
from datetime import date
from pathlib import Path

import pytest

from custom_components.garmin_connect.sleep_archive import (
    SleepSchemaError,
    parse_sleep_sessions,
    session_from_record,
    session_record,
)


def test_sleep_parser_keeps_main_nap_and_ignores_numeric_arrays() -> None:
    payload = {
        "sleepData": {
            "sleepStartTimestampGMT": "2026-07-23T22:00:00Z",
            "sleepEndTimestampGMT": "2026-07-24T06:00:00Z",
            "sleepScores": {"overall": {"value": 82}, "quality": "good"},
            "adjustments": ["late meal"],
            "feedback": ["consistent"],
            "restlessEvents": ["movement"],
            "sleepLevels": [{"startGMT": "2026-07-24T01:00:00Z", "activityLevel": 2, "activityType": "deepSleep"}],
            "napEvents": [
                {
                    "startTime": "2026-07-24T13:00:00Z",
                    "endTime": "2026-07-24T13:30:00Z",
                }
            ],
        }
    }
    sessions = parse_sleep_sessions(payload, date(2026, 7, 24))
    assert [session.kind for session in sessions] == ["main", "nap"]
    assert sessions[0].score == {"overall": {"value": 82}, "quality": "good"}
    assert sessions[0].adjustments == ("late meal",)
    assert sessions[0].restless_events == ("movement",)
    assert sessions[0].stages == ({"startGMT": "2026-07-24T01:00:00Z", "activityLevel": 2, "activityType": "deepSleep"},)
    assert session_from_record(session_record(sessions[0])) == sessions[0]


def test_sleep_parser_rejects_known_shape_drift() -> None:
    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({"sleepData": {"napEvents": "bad"}}, date(2026, 7, 24))
    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({"sleepData": {"napEvents": [{}, "bad"]}}, date(2026, 7, 24))


def test_logical_id_is_stable_across_score_revisions() -> None:
    base = {
        "sleepStartTimestampGMT": "2026-07-23T22:00:00Z",
        "sleepEndTimestampGMT": "2026-07-24T06:00:00Z",
    }
    first = parse_sleep_sessions({**base, "score": {"overall": 80}}, date(2026, 7, 24))[0]
    second = parse_sleep_sessions({**base, "score": {"overall": 81}}, date(2026, 7, 24))[0]
    assert first.logical_id == second.logical_id
    assert first.revision != second.revision


def test_sleep_parser_preserves_structured_event_fields_and_rejects_numeric_event_arrays() -> None:
    payload = {
        "sleepStartTimestampGMT": "2026-03-29T00:30:00+00:00",
        "sleepEndTimestampGMT": "2026-03-29T08:30:00+01:00",
        "sleepScores": {"overall": {"value": 82, "subparts": {"quality": "good"}}},
        "adjustments": [{"type": "late_meal", "minutes": 30}],
        "feedback": [{"category": "routine", "message": "consistent"}],
        "restlessEvents": [{"start": "2026-03-29T03:00:00Z", "duration": 12}],
    }
    session = parse_sleep_sessions(payload, date(2026, 3, 29))[0]
    assert session.adjustments[0]["type"] == "late_meal"
    assert session.feedback[0]["category"] == "routine"
    assert session.restless_events[0]["duration"] == 12
    assert session.start.tzinfo is not None and session.end.tzinfo is not None

    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({**payload, "restlessEvents": [[1, 2, 3]]}, date(2026, 3, 29))

    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({**payload, "sleepLevels": [1, 2]}, date(2026, 3, 29))

    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({**payload, "sleepLevels": [{"startGMT": "x"}]}, date(2026, 3, 29))


def test_sleep_record_restore_rejects_corrupt_identity_and_fields() -> None:
    session = parse_sleep_sessions(
        {"startTime": "2026-07-24T00:00:00Z", "endTime": "2026-07-24T08:00:00Z"},
        date(2026, 7, 24),
    )[0]
    record = session_record(session)
    for field, value in (("kind", "unknown"), ("revision", "bad"), ("stages", ["bad"]), ("score", [])):
        corrupt = {**record, field: value}
        with pytest.raises(SleepSchemaError):
            session_from_record(corrupt)


def test_sleep_record_restore_accepts_legacy_record_without_stages() -> None:
    session = parse_sleep_sessions(
        {"startTime": "2026-07-24T00:00:00Z", "endTime": "2026-07-24T08:00:00Z"},
        date(2026, 7, 24),
    )[0]
    legacy_record = session_record(session)
    del legacy_record["stages"]
    restored = session_from_record(legacy_record)
    assert restored.stages == ()
    assert restored.logical_id == session.logical_id


def test_sleep_parser_handles_leap_day_and_overlap_identity() -> None:
    first = parse_sleep_sessions(
        {"startTime": "2028-02-29T22:00:00Z", "endTime": "2028-03-01T06:00:00Z"},
        date(2028, 2, 29),
    )[0]
    second = parse_sleep_sessions(
        {"startTime": "2028-02-29T22:00:00Z", "endTime": "2028-03-01T06:00:00Z"},
        date(2028, 2, 29),
    )[0]
    assert first == second
    assert first.calendar_date == date(2028, 2, 29)


def test_sleep_logical_id_canonicalizes_equivalent_offsets() -> None:
    first = parse_sleep_sessions(
        {"startTime": "2026-07-23T22:00:00Z", "endTime": "2026-07-24T06:00:00Z"},
        date(2026, 7, 24),
    )[0]
    second = parse_sleep_sessions(
        {"startTime": "2026-07-24T00:00:00+02:00", "endTime": "2026-07-24T08:00:00+02:00"},
        date(2026, 7, 24),
    )[0]
    assert first.logical_id == second.logical_id
    assert first.start.isoformat() != second.start.isoformat()


def test_sleep_parser_preserves_sanitized_high_resolution_streams() -> None:
    point_rows = [["2026-07-24T23:55:00Z", 60], ["2026-07-25T00:05:00Z", None], ["2026-07-24T23:50:00Z", 60]]
    payload = {
        "startTime": "2026-07-24T23:45:00Z",
        "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRate": point_rows,
        "hrvData": point_rows,
        "sleepBodyBattery": point_rows,
        "sleepStress": [["2026-07-25T00:00:00Z", -1], ["2026-07-25T00:01:00Z", 0]],
        "sleepRespiration": point_rows,
        "sleepSpO2": point_rows,
        "sleepMovement": point_rows,
    }
    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]
    assert {stream.metric for stream in session.streams} == {
        "heart_rate", "hrv", "body_battery", "stress", "respiration", "spo2", "movement",
    }
    assert session.streams[0].points[0].timestamp.tzinfo is not None
    assert any(point.value is None for point in session.streams[0].points)
    assert session_record(session)["streams"]["heart_rate"]
    assert session_from_record(session_record(session)) == session


def test_current_garmin_sleep_payload_unwraps_session_and_streams() -> None:
    """The live dailySleepDTO envelope retains its session and raw streams."""
    payload = {
        "dailySleepDTO": {
            "calendarDate": "2026-08-01",
            "sleepStartTimestampGMT": 1_785_520_800_000,
            "sleepEndTimestampGMT": 1_785_549_600_000,
            "sleepScores": {"overall": {"value": 82}},
        },
        "sleepLevels": [
            {
                "startGMT": "2026-07-31T22:00:00.0",
                "endGMT": "2026-07-31T22:15:00.0",
                "activityLevel": 2,
            }
        ],
        "sleepHeartRate": [{"startGMT": 1_785_520_800_000, "value": 60}],
        "hrvData": [{"startGMT": 1_785_520_800_000, "value": 42}],
        "sleepBodyBattery": [{"startGMT": 1_785_520_800_000, "value": 70}],
        "sleepStress": [{"startGMT": 1_785_520_800_000, "value": 12}],
        "sleepMovement": [
            {
                "startGMT": "2026-07-31T22:00:00.0",
                "endGMT": "2026-07-31T22:00:30.0",
                "activityLevel": 1,
            }
        ],
        "wellnessEpochRespirationDataDTOList": [
            {"startTimeGMT": 1_785_520_800_000, "respirationValue": 14.0}
        ],
        "wellnessEpochSPO2DataDTOList": [
            {"epochTimestamp": "2026-07-31T22:00:00.0", "spo2Reading": 97}
        ],
    }

    sessions = parse_sleep_sessions(payload, date(2026, 8, 1))

    assert len(sessions) == 1
    assert sessions[0].calendar_date == date(2026, 8, 1)
    assert len(sessions[0].stages) == 1
    assert {
        stream.metric: (stream.presence, len(stream.points))
        for stream in sessions[0].streams
    } == {
        "heart_rate": ("present", 1),
        "hrv": ("present", 1),
        "body_battery": ("present", 1),
        "stress": ("present", 1),
        "respiration": ("present", 1),
        "spo2": ("present", 1),
        "movement": ("present", 1),
    }


def test_generic_naive_sleep_session_timestamp_remains_rejected() -> None:
    """Only Garmin fields explicitly labelled GMT may imply UTC."""
    with pytest.raises(SleepSchemaError, match="no offset"):
        parse_sleep_sessions(
            {
                "startTime": "2026-08-01T00:00:00",
                "endTime": "2026-08-01T01:00:00",
            },
            date(2026, 8, 1),
        )


def test_sleep_stream_presence_distinguishes_sparse_states() -> None:
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z",
            "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRate": None,
            "hrvData": [],
            "sleepStress": [["2026-07-25T00:00:00Z", None]],
            "sleepRespiration": [["2026-07-25T00:01:00Z", 14]],
        },
        date(2026, 7, 24),
    )[0]
    streams = {stream.metric: stream for stream in session.streams}

    assert streams["heart_rate"].presence == "null"
    assert streams["hrv"].presence == "empty"
    assert streams["stress"].presence == "all-null"
    assert streams["respiration"].presence == "present"
    assert session_from_record(session_record(session)) == session


def test_sanitized_stream_fixture_has_representative_batch_sizes() -> None:
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "garmin_sleep_streams.json").read_text()
    )
    session = parse_sleep_sessions(fixture, date(2026, 7, 24))[0]

    assert {stream.metric: len(stream.points) for stream in session.streams} == {
        "heart_rate": 32,
        "hrv": 32,
        "body_battery": 32,
        "stress": 32,
        "respiration": 32,
        "spo2": 32,
        "movement": 32,
    }


def test_sleep_stream_object_rows_use_stream_specific_values() -> None:
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
            "sleepBodyBattery": [{"timestamp": "2026-07-25T00:00:00Z", "bodyBattery": 70}],
            "sleepRespiration": [{"timestamp": "2026-07-25T00:01:00Z", "respirationValue": 14}],
            "sleepMovement": [{"timestamp": "2026-07-25T00:02:00Z", "movement": 1}],
        },
        date(2026, 7, 24),
    )[0]
    assert [stream.points[0].value for stream in session.streams] == [70.0, 14.0, 1.0]


def test_sleep_stream_mixed_null_objects_preserve_valid_points_and_reject_malformed_rows() -> None:
    payload = {
        "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRate": [
            None,
            {"timestamp": "2026-07-25T00:00:00Z", "heartRate": 60},
            {"timestamp": "2026-07-25T00:01:00Z", "heartRate": None},
        ],
    }
    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]
    assert [(point.timestamp.isoformat(), point.value) for point in session.streams[0].points] == [
        ("2026-07-25T00:00:00+00:00", 60.0),
        ("2026-07-25T00:01:00+00:00", None),
    ]

    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions(
            {
                **payload,
                "sleepHeartRate": [
                    None,
                    {"timestamp": "2026-07-25T00:00:00Z"},
                ],
            },
            date(2026, 7, 24),
        )


def test_sleep_parser_retains_negative_numeric_points_for_archive_validation() -> None:
    """The parser must not silently turn a documented value into a gap."""
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z",
            "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRate": [["2026-07-25T00:00:00Z", -2]],
        },
        date(2026, 7, 24),
    )[0]

    assert session.streams[0].points[0].value == -2.0


def test_sleep_measurement_timestamp_without_offset_fails_closed() -> None:
    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions(
            {
                "startTime": "2026-07-24T23:45:00Z",
                "endTime": "2026-07-25T07:15:00Z",
                "sleepHeartRate": [[60, "2026-07-25T00:00:00"]],
            },
            date(2026, 7, 24),
        )


def test_sleep_stream_descriptors_reorder_columns_and_deduplicate_timestamps() -> None:
    payload = {
        "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRateDescriptors": [
            {"key": "value", "index": 0}, {"key": "timestamp", "index": 1},
        ],
        "sleepHeartRate": [
            [60, "2026-07-25T00:01:00Z"], [61, "2026-07-25T00:01:00Z"],
            [59, "2026-07-25T00:00:00Z"],
        ],
    }
    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]
    points = session.streams[0].points
    assert [(point.timestamp.isoformat(), point.value) for point in points] == [
        ("2026-07-25T00:00:00+00:00", 59.0),
        ("2026-07-25T00:01:00+00:00", 61.0),
    ]

    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({**payload, "sleepHeartRate": [[60, "x"]]}, date(2026, 7, 24))

    long_stream = [
        [index, f"2026-07-25T{index // 3600:02d}:{index // 60 % 60:02d}:{index % 60:02d}Z"]
        for index in range(4097)
    ]
    long_session = parse_sleep_sessions(
        {**payload, "sleepHeartRate": long_stream}, date(2026, 7, 24)
    )[0]
    assert len(long_session.streams[0].points) == 4097


def test_sleep_stream_aliases_choose_first_non_null_field() -> None:
    payload = {
        "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRate": [{
            "timestamp": None,
            "time": "2026-07-25T00:00:00Z",
            "heartRate": None,
            "value": 60,
        }],
        "sleepHeartRateValues": [{
            "timestamp": "2026-07-25T00:01:00Z",
            "heartRate": 61,
        }],
    }

    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]

    assert [(point.timestamp.isoformat(), point.value) for point in session.streams[0].points] == [
        ("2026-07-25T00:00:00+00:00", 60.0),
    ]


def test_sleep_descriptor_rows_choose_first_non_null_timestamp_and_value_alias() -> None:
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z",
            "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRateDescriptors": [
                {"key": "timestamp", "index": 0},
                {"key": "time", "index": 1},
                {"key": "heartRate", "index": 2},
                {"key": "value", "index": 3},
            ],
            "sleepHeartRate": [[None, "2026-07-25T00:00:00Z", None, 60]],
        },
        date(2026, 7, 24),
    )[0]
    assert session.streams[0].points[0].value == 60.0


def test_sleep_stream_key_aliases_choose_first_non_null_array() -> None:
    payload = {
        "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRate": None,
        "sleepHeartRateValues": [["2026-07-25T00:01:00Z", 61]],
    }

    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]

    assert session.streams[0].points[0].value == 61.0


def test_empty_sleep_stream_alias_does_not_mask_later_valid_stream() -> None:
    payload = {
        "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRate": [],
        "sleepHeartRateValues": [["2026-07-25T00:01:00Z", 61]],
    }

    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]

    assert session.streams[0].presence == "present"
    assert session.streams[0].points[0].value == 61.0


def test_sleep_stream_numeric_epoch_timestamp_and_nested_descriptors() -> None:
    payload = {
        "sleepData": {
            "startTime": "2026-07-24T23:45:00Z", "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRateValueDescriptorsDTOList": [
                {"key": "heartRate", "index": 0}, {"key": "timestamp", "index": 1},
            ],
            "sleepHeartRate": [[60, 1_784_841_600_000]],
        }
    }
    session = parse_sleep_sessions(payload, date(2026, 7, 24))[0]
    point = session.streams[0].points[0]
    assert point.raw_timestamp == 1_784_841_600_000
    assert point.timestamp.tzinfo is not None
