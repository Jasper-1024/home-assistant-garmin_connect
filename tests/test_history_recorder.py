"""Tests for the Garmin Recorder statistics writer."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone

import pytest

from custom_components.garmin_connect.history_recorder import (
    HEART_RATE_METADATA,
    RESPIRATION_RAW_METADATA,
    SPO2_SINGLE_METADATA,
    STRESS_METADATA,
    GarminHistoryRecorder,
    statistic_id_for,
)
from custom_components.garmin_connect.history_source import NormalizedSample


class FakeRequester:
    def __init__(self) -> None:
        self.imports: list[tuple[object, list[object], object]] = []
        self.tasks: list[object] = []
        self.rows: dict[tuple[str, object], object] = {}

    def async_import_statistics(self, metadata, stats, table) -> None:
        self.imports.append((metadata, list(stats), table))
        statistic_id = metadata["statistic_id"]
        for row in stats:
            self.rows[(statistic_id, row["start"])] = row

    def queue_task(self, task) -> None:
        self.tasks.append(task)
        task.future.set_result(None)


@pytest.mark.asyncio
async def test_writer_preserves_aware_timestamps_and_equal_values_without_state_events() -> None:
    recorder = FakeRequester()
    writer = GarminHistoryRecorder(recorder)
    samples = (
        NormalizedSample(
            datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
            date(2026, 7, 24),
            "2026-07-24T01:02:00+00:00",
            60.0,
        ),
        NormalizedSample(
            datetime(2026, 7, 24, 1, 3, tzinfo=UTC),
            date(2026, 7, 24),
            "2026-07-24T01:03:00+00:00",
            60.0,
        ),
    )

    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"), HEART_RATE_METADATA, samples
    )

    assert result.accepted_count == 2
    metadata, rows, table = recorder.imports[0]
    assert metadata["statistic_id"].startswith("garmin_connect:")
    assert "opaque-account-key-123" not in metadata["statistic_id"]
    assert metadata["unit_of_measurement"] == "bpm"
    assert [row["start"] for row in rows] == [sample.timestamp for sample in samples]
    assert [row["mean"] for row in rows] == [60.0, 60.0]
    assert table.__name__ == "Statistics"
    assert len(recorder.tasks) == 1


@pytest.mark.asyncio
async def test_writer_uses_absolute_source_instant_for_equivalent_offsets() -> None:
    """Different offset spellings of one source instant share one statistics row."""
    recorder = FakeRequester()
    writer = GarminHistoryRecorder(recorder)
    source_instant = datetime(2026, 7, 24, 8, 0, tzinfo=UTC)

    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 10, 0, tzinfo=timezone(timedelta(hours=2))),
                date(2026, 7, 24),
                "2026-07-24T10:00:00+02:00",
                60.0,
            ),
        ),
    )

    assert result.accepted_count == 1
    queued_timestamp = recorder.imports[0][1][0]["start"]
    assert queued_timestamp == source_instant
    assert queued_timestamp.tzinfo is UTC


@pytest.mark.asyncio
async def test_writer_keeps_source_calendar_date_outside_recorder_statistics() -> None:
    recorder = FakeRequester()
    writer = GarminHistoryRecorder(recorder)
    source_date = date(2026, 7, 24)

    await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (NormalizedSample(datetime(2026, 7, 24, 23, 30, tzinfo=UTC), source_date, "source", 60.0),),
    )

    assert set(recorder.imports[0][1][0]) == {"start", "mean", "min", "max"}


@pytest.mark.asyncio
async def test_writer_rejects_naive_source_timestamps_before_history_write() -> None:
    recorder = FakeRequester()
    writer = GarminHistoryRecorder(recorder)

    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (NormalizedSample(datetime(2026, 7, 24, 8), date(2026, 7, 24), "naive", 60.0),),
    )

    assert result.outcome == "invalid"
    assert result.error_type == "timestamp"
    assert recorder.imports == []


@pytest.mark.asyncio
async def test_revisions_are_sent_idempotently() -> None:
    recorder = FakeRequester()
    writer = GarminHistoryRecorder(recorder)
    sample = NormalizedSample(datetime(2026, 7, 24, 1, 2, tzinfo=UTC), date(2026, 7, 24), 1, 60.0)

    statistic_id = statistic_id_for("opaque-account-key-123", "stress")
    await writer.async_write(statistic_id, STRESS_METADATA, (sample,))
    await writer.async_write(statistic_id, STRESS_METADATA, (replace(sample, value=61.0),))

    assert len(recorder.imports) == 2
    assert recorder.imports[1][1][0]["mean"] == 61.0

    result = await writer.async_write(statistic_id, STRESS_METADATA, (replace(sample, value=61.0),))
    assert (result.inserted_count, result.updated_count, result.skipped_count) == (0, 0, 1)

    result = await writer.async_write(statistic_id, STRESS_METADATA, (replace(sample, value=62.0),))
    assert (result.inserted_count, result.updated_count, result.skipped_count) == (0, 1, 0)


def test_new_series_have_distinct_stable_statistic_ids() -> None:
    """Respiration and SpO2 variants cannot collide in Recorder."""
    account_key = "opaque-account-key-123"
    ids = {
        statistic_id_for(account_key, RESPIRATION_RAW_METADATA.key),
        statistic_id_for(account_key, SPO2_SINGLE_METADATA.key),
    }
    assert len(ids) == 2
    assert all("opaque-account-key" not in value for value in ids)


@pytest.mark.asyncio
async def test_release_gate_scratch_recorder_restart_revision_and_no_state_changed() -> None:
    """Release contract: raw points converge without state_changed replay."""
    from homeassistant.components.recorder.tasks import SynchronizeTask

    requester = FakeRequester()
    samples = tuple(
        NormalizedSample(
            datetime(2026, 7, 24, 1, minute, tzinfo=UTC),
            date(2026, 7, 24),
            f"2026-07-24T01:{minute:02d}:00+00:00",
            60.0,
        )
        for minute in range(4)
    )
    statistic_id = statistic_id_for("opaque-account-key-123", "heart_rate")
    writer = GarminHistoryRecorder(requester)
    await writer.async_write(statistic_id, HEART_RATE_METADATA, samples)
    restarted = GarminHistoryRecorder(requester)
    replay = await restarted.async_write(statistic_id, HEART_RATE_METADATA, samples)
    overlap = await restarted.async_write(
        statistic_id,
        HEART_RATE_METADATA,
        (samples[1], replace(samples[2], value=61.0), samples[3]),
    )

    assert len(requester.imports[0][1]) == 4
    assert [row["start"] for row in requester.imports[0][1]] == [sample.timestamp for sample in samples]
    assert replay.accepted_count == 4
    assert len(requester.rows) == 4
    assert overlap.updated_count == 1
    assert overlap.skipped_count == 2
    assert requester.imports[2][1][1]["start"] == samples[2].timestamp
    assert requester.imports[2][1][1]["mean"] == 61.0
    assert requester.rows[(statistic_id, samples[2].timestamp)]["mean"] == 61.0
    assert all(isinstance(task, SynchronizeTask) for task in requester.tasks)
    assert len(requester.tasks) == 3
    assert all("state_changed" not in row for _, rows, _ in requester.imports for row in rows)
