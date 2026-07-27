"""Tests for structured Garmin sleep sessions."""

from datetime import date

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
