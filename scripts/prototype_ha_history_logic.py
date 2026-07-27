"""PROTOTYPE — pure verdict logic for HA high-resolution history experiments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


@dataclass(frozen=True)
class Sample:
    """One Garmin-like historical sample."""

    timestamp: datetime
    value: float


SAMPLES = (
    Sample(datetime(2026, 7, 25, 1, 2, tzinfo=UTC), 62),
    Sample(datetime(2026, 7, 25, 1, 4, tzinfo=UTC), 62),
    Sample(datetime(2026, 7, 25, 1, 7, tzinfo=UTC), 59),
    Sample(datetime(2026, 7, 25, 1, 11, tzinfo=UTC), 61),
)


def expected_timestamps() -> list[float]:
    """Return the fixture timestamps."""
    return [sample.timestamp.timestamp() for sample in SAMPLES]


def assess_state_replay(
    *,
    rows_before_restart: list[dict[str, Any]],
    rows_after_restart: list[dict[str, Any]],
    emitted_events: int,
    current_state: str | None,
) -> dict[str, Any]:
    """Assess whether path A preserved the raw state samples."""
    expected_times = expected_timestamps()
    before_times = [row["timestamp"] for row in rows_before_restart]
    after_times = [row["timestamp"] for row in rows_after_restart]
    before_values = [float(row["value"]) for row in rows_before_restart]
    expected_values = [sample.value for sample in SAMPLES]
    checks = {
        "all_points_before_restart": len(rows_before_restart) == len(SAMPLES),
        "all_points_after_restart": len(rows_after_restart) == len(SAMPLES),
        "timestamps_exact": before_times == expected_times == after_times,
        "duplicate_value_preserved": before_values[:2] == [62, 62],
        "values_exact": before_values == expected_values,
        "state_changed_emitted": emitted_events == len(SAMPLES),
        "current_state_is_last_call": current_state == str(int(SAMPLES[-1].value)),
    }
    return {
        "path": "A — StateMachine timestamp replay",
        "passed": all(checks.values()),
        "checks": checks,
        "rows": rows_before_restart,
        "events": emitted_events,
        "current_state": current_state,
    }


def assess_long_term_statistics(
    *,
    rows_before_restart: list[dict[str, Any]],
    rows_after_update: list[dict[str, Any]],
    rows_after_restart: list[dict[str, Any]],
    updated_timestamp: float,
    updated_value: float,
    emitted_events: int,
) -> dict[str, Any]:
    """Assess whether path B preserved and idempotently updated raw statistics."""
    expected_times = expected_timestamps()
    before_times = [row["timestamp"] for row in rows_before_restart]
    restart_times = [row["timestamp"] for row in rows_after_restart]
    updated_rows = [
        row for row in rows_after_update if row["timestamp"] == updated_timestamp
    ]
    restarted_updated_rows = [
        row for row in rows_after_restart if row["timestamp"] == updated_timestamp
    ]
    checks = {
        "all_points_before_restart": len(rows_before_restart) == len(SAMPLES),
        "all_points_after_update": len(rows_after_update) == len(SAMPLES),
        "all_points_after_restart": len(rows_after_restart) == len(SAMPLES),
        "timestamps_exact": before_times == expected_times == restart_times,
        "same_timestamp_immediately_visible": (
            len(updated_rows) == 1 and updated_rows[0]["mean"] == updated_value
        ),
        "same_timestamp_persisted_not_duplicated": (
            len(restarted_updated_rows) == 1
            and restarted_updated_rows[0]["mean"] == updated_value
        ),
        "no_state_changed_events": emitted_events == 0,
    }
    storage_checks = {
        key: value
        for key, value in checks.items()
        if key != "same_timestamp_immediately_visible"
    }
    return {
        "path": "B — non-hour long-term statistics",
        "storage_passed": all(storage_checks.values()),
        "passed": all(checks.values()),
        "checks": checks,
        "rows_before_restart": rows_before_restart,
        "rows_after_update": rows_after_update,
        "rows_after_restart": rows_after_restart,
        "updated_timestamp": updated_timestamp,
        "updated_value": updated_value,
        "events": emitted_events,
    }
