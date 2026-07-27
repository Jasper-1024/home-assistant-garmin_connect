"""Tests for structured Garmin sleep sessions."""

from datetime import UTC, date, datetime

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
            "sleepLevels": [[1, 2, 3]],
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
    assert sessions[0].score == {"quality": "good"}
    assert sessions[0].adjustments == ("late meal",)
    assert sessions[0].restless_events == ("movement",)
    assert session_from_record(session_record(sessions[0])) == sessions[0]


def test_sleep_parser_rejects_known_shape_drift() -> None:
    with pytest.raises(SleepSchemaError):
        parse_sleep_sessions({"sleepData": {"napEvents": "bad"}}, date(2026, 7, 24))


def test_logical_id_is_stable_across_score_revisions() -> None:
    base = {
        "sleepStartTimestampGMT": "2026-07-23T22:00:00Z",
        "sleepEndTimestampGMT": "2026-07-24T06:00:00Z",
    }
    first = parse_sleep_sessions({**base, "score": {"overall": 80}}, date(2026, 7, 24))[0]
    second = parse_sleep_sessions({**base, "score": {"overall": 81}}, date(2026, 7, 24))[0]
    assert first.logical_id == second.logical_id
    assert first.revision != second.revision
