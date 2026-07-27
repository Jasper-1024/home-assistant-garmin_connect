"""Conservative backfill policy tests."""

from datetime import UTC, date, datetime

from custom_components.garmin_connect.backfill import (
    BACKFILL_START,
    BACKOFF_429,
    BackfillState,
    classify_backfill_error,
    next_backfill_date,
)


def test_backfill_selects_first_uncompleted_date() -> None:
    state = BackfillState(frozenset({"2026-01-01", "2026-01-03"}))
    assert next_backfill_date(state, date(2026, 1, 3)) == date(2026, 1, 2)
    assert next_backfill_date(BackfillState(frozenset({"2026-01-01"})), date(2026, 1, 1)) == date(2026, 1, 2)
    assert BACKFILL_START == date(2026, 1, 1)


def test_backfill_state_round_trips_checkpoint_and_backoff() -> None:
    now = datetime(2026, 2, 1, tzinfo=UTC)
    state = BackfillState(frozenset({"2026-01-01"}), now, now + BACKOFF_429, now, "rate_limited")
    restored = BackfillState.from_record(state.as_record())
    assert restored == state


def test_backfill_error_classes_are_bounded() -> None:
    class ResponseError(Exception):
        status_code = 429

    class ForbiddenError(Exception):
        status = 403

    assert classify_backfill_error(ResponseError()) == "rate_limited"
    assert classify_backfill_error(ForbiddenError()) == "forbidden_path"
    assert classify_backfill_error(OSError()) == "network"
