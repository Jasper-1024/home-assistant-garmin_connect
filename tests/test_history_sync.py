"""Focused tests for the manual history synchronization slice."""

from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_recorder import RecorderWriteOutcome
from custom_components.garmin_connect.history_source import (
    HRVData,
    HRVSummary,
    NormalizedSample,
    SegmentedData,
    SourceSeries,
)


class _Store:
    def __init__(self, data=None):
        self.data = data or {"account_key": "opaque-account-key-1234567890", "schema_version": 1}
        self.saved = []

    async def async_load(self):
        return self.data

    async def async_save(self, data):
        self.saved.append(data)
        self.data = data


def _sync_archive(source, recorder, store):
    entry = MagicMock(data={"history_account_key": "opaque-account-key-1234567890"}, entry_id="e")
    entry.runtime_data = SimpleNamespace(core=SimpleNamespace(client=object()), request_gate=object())
    archive = GarminHistoryArchive(
        MagicMock(), entry,
        recorder_checker=SimpleNamespace(async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())),
        store_factory=lambda *args, **kwargs: store,
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )
    return archive


@pytest.mark.asyncio
async def test_invalid_range_does_not_fetch_or_write():
    source = MagicMock()
    recorder = MagicMock()
    entry = MagicMock(data={"history_account_key": "opaque-account-key-1234567890"}, entry_id="e")
    entry.runtime_data = SimpleNamespace(core=SimpleNamespace(client=object()), request_gate=object())
    archive = GarminHistoryArchive(
        MagicMock(), entry,
        recorder_checker=SimpleNamespace(async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())),
        store_factory=lambda *args, **kwargs: _Store(),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()

    report = await archive.async_sync_range(date(2025, 12, 31), date(2026, 1, 1))

    assert report.outcome == "invalid"
    source.async_fetch.assert_not_called()
    recorder.async_write.assert_not_called()


@pytest.mark.asyncio
async def test_sync_fetches_only_supported_metrics_and_writes_each_day():
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=(NormalizedSample(datetime(2026, 1, 1, tzinfo=UTC), date(2026, 1, 1), 1, 60.0),))
    source.async_fetch_details = None
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1))
    entry = MagicMock(data={"history_account_key": "opaque-account-key-1234567890"}, entry_id="e")
    entry.runtime_data = SimpleNamespace(core=SimpleNamespace(client=object()), request_gate=object())
    archive = GarminHistoryArchive(
        MagicMock(), entry,
        recorder_checker=SimpleNamespace(async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())),
        store_factory=lambda *args, **kwargs: _Store(),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 2))

    assert report.outcome == "written"
    assert source.async_fetch.await_args_list[0].args[1] == "heart_rate"
    assert {call.args[1] for call in source.async_fetch.await_args_list} == {
        "heart_rate",
        "stress",
        "body_battery",
        "nightly_hrv",
        "steps",
        "floors",
        "intensity_moderate",
        "intensity_vigorous",
        "respiration_raw",
        "respiration_average",
        "spo2_single",
        "spo2_continuous",
        "spo2_hourly",
    }
    assert recorder.async_write.await_count == 26
    assert archive.status.state is HistoryArchiveState.IDLE


@pytest.mark.asyncio
async def test_source_series_unwraps_and_presence_survives_restart():
    """Source metadata reaches the private catalog; Recorder receives samples only."""
    sample = NormalizedSample(datetime(2026, 1, 1, tzinfo=UTC), date(2026, 1, 1), 1, 14.0)

    class Source:
        async def async_fetch_details(self, target, metric):
            if metric == "respiration_raw":
                return SourceSeries((sample,), "present")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1))
    store = _Store()
    archive = _sync_archive(Source(), recorder, store)
    await archive.async_start()
    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "written"
    respiration_call = next(
        call for call in recorder.async_write.await_args_list
        if call.args[1].key == "respiration_raw"
    )
    assert respiration_call.args[2] == (sample,)
    assert store.data["presence"]["2026-01-01"]["respiration_raw"] == "present"

    restarted = _sync_archive(Source(), recorder, _Store(store.data))
    await restarted.async_start()
    assert restarted.get_history_presence(date(2026, 1, 1), date(2026, 1, 1)) == {
        "2026-01-01": {"respiration_raw": "present"}
    }


@pytest.mark.asyncio
async def test_presence_catalog_loads_all_bounded_states():
    """The private catalog preserves each availability classification."""
    states = dict(zip(
        ("a", "b", "c", "d", "e", "f"),
        ("null", "empty", "missing", "unsupported", "returned-empty", "present"),
        strict=True,
    ))
    store = _Store({
        "account_key": "opaque-account-key-1234567890",
        "schema_version": 1,
        "completed_dates": [],
        "hrv_summaries": {},
        "presence": {"2026-01-01": states},
    })
    archive = _sync_archive(MagicMock(), MagicMock(), store)
    await archive.async_start()

    assert archive.get_history_presence(date(2026, 1, 1), date(2026, 1, 1)) == {
        "2026-01-01": states
    }


@pytest.mark.asyncio
async def test_archive_aggregates_import_classification_counts():
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(side_effect=[
        RecorderWriteOutcome(2, inserted_count=1, updated_count=1),
        RecorderWriteOutcome(2, skipped_count=2),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
        RecorderWriteOutcome(0),
    ])
    archive = _sync_archive(source, recorder, _Store())
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert (report.inserted_count, report.updated_count, report.skipped_count) == (1, 1, 2)


@pytest.mark.asyncio
async def test_checkpoint_persists_each_date_and_restart_skips_it():
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    store = _Store()
    first = _sync_archive(source, recorder, store)
    await first.async_start()
    await first.async_sync_range(date(2026, 1, 1), date(2026, 1, 2))

    assert store.saved[-1]["completed_dates"] == ["2026-01-01", "2026-01-02"]
    source.reset_mock()
    second = _sync_archive(source, recorder, store)
    await second.async_start()
    report = await second.async_sync_range(date(2026, 1, 1), date(2026, 1, 2))

    assert report.skipped_count == 2
    assert report.processed_dates == (date(2026, 1, 1), date(2026, 1, 2))
    source.async_fetch.assert_not_called()


@pytest.mark.asyncio
async def test_failed_second_metric_does_not_checkpoint_date():
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(side_effect=[RecorderWriteOutcome(0), RecorderWriteOutcome(0, "failed", "writer")])
    store = _Store()
    archive = _sync_archive(source, recorder, store)
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "failed"
    assert "completed_dates" not in store.data or store.data["completed_dates"] == []


@pytest.mark.asyncio
async def test_runtime_failure_can_retry():
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(
        side_effect=[
            RecorderWriteOutcome(0, "failed", "writer"),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
            RecorderWriteOutcome(0),
        ]
    )
    archive = _sync_archive(source, recorder, _Store())
    await archive.async_start()

    first = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))
    second = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert first.outcome == "failed"
    assert second.outcome == "written"


@pytest.mark.asyncio
async def test_segmented_daily_totals_write_separate_statistic():
    class Source:
        async def async_fetch_details(self, target_date, metric):
            if metric == "steps":
                return SegmentedData((), {"totalSteps": 7.0})
            return ()

    source = Source()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _sync_archive(source, recorder, _Store())
    await archive.async_start()
    await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))
    statistic_ids = [call.args[0] for call in recorder.async_write.await_args_list]
    assert any(statistic_id.endswith(":steps") for statistic_id in statistic_ids)
    assert any(statistic_id.endswith(":steps_daily_total") for statistic_id in statistic_ids)


@pytest.mark.asyncio
async def test_hrv_summary_persists_only_with_date_checkpoint():

    class Source:
        async def async_fetch(self, target_date, metric):
            return ()

        async def async_fetch_details(self, target_date, metric):
            if metric == "nightly_hrv":
                return HRVData((), HRVSummary("balanced", 48.0, 72.0, 50.0, {"low": 40.0}))
            return ()

    source = Source()
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    store = _Store()
    entry = MagicMock(data={"history_account_key": "opaque-account-key-1234567890"}, entry_id="e")
    entry.runtime_data = SimpleNamespace(core=SimpleNamespace(client=object()), request_gate=object())
    archive = GarminHistoryArchive(MagicMock(), entry, recorder_checker=SimpleNamespace(async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())), store_factory=lambda *args, **kwargs: store, source_factory=lambda *args: source, recorder_factory=lambda: recorder)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))
    assert store.data["hrv_summaries"]["2026-01-01"]["status"] == "balanced"
    assert archive.get_hrv_summaries(date(2026, 1, 1), date(2026, 1, 1))[0][1].status == "balanced"
    restarted = _sync_archive(source, recorder, store)
    await restarted.async_start()
    assert restarted.get_hrv_summaries(date(2026, 1, 1), date(2026, 1, 1))[0][1].weekly_avg == 50.0
