"""Focused tests for the manual history synchronization slice."""

import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.garmin_connect.history import (
    GarminHistoryArchive,
    HistoryArchiveState,
    RecorderCompatibilityResult,
)
from custom_components.garmin_connect.history_recorder import (
    DAILY_ABNORMAL_HR_METADATA,
    TRAINING_ACUTE_LOAD_METADATA,
    TRAINING_ACWR_METADATA,
    TRAINING_CHRONIC_LOAD_METADATA,
    TRAINING_FITNESS_TREND_METADATA,
    TRAINING_LOAD_BALANCE_METADATA,
    TRAINING_RECOVERY_TIME_METADATA,
    TRAINING_VO2_MAX_METADATA,
    RecorderWriteOutcome,
    statistic_id_for,
)
from custom_components.garmin_connect.history_source import (
    DAILY_SUMMARY_FIELDS,
    TRAINING_STATUS_FIELDS,
    HRVData,
    HRVSummary,
    NormalizedSample,
    SegmentedData,
    SnapshotData,
    SourceSeries,
    normalize_health_events,
    normalize_snapshot,
    normalize_activities,
)
from custom_components.garmin_connect.sleep_archive import (
    SleepSession,
    parse_sleep_sessions,
    session_record,
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


class _NamedStore(_Store):
    def __init__(self, data=None, *, fail_save=False):
        self.data = data
        self.saved = []
        self.fail_save = fail_save

    async def async_save(self, data):
        if self.fail_save:
            raise OSError("partition unavailable")
        await super().async_save(data)


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


def _partition_archive(source, recorder, stores):
    entry = MagicMock(data={"history_account_key": "opaque-account-key-1234567890"}, entry_id="e")
    entry.runtime_data = SimpleNamespace(core=SimpleNamespace(client=object()), request_gate=object())
    return GarminHistoryArchive(
        MagicMock(), entry,
        recorder_checker=SimpleNamespace(async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())),
        store_factory=lambda _hass, _version, path, **kwargs: stores.setdefault(path, _NamedStore()),
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )


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
        "daily_summary",
        "training_status",
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
async def test_snapshot_archive_writes_present_fields_and_restarts_from_checkpoint():
    fixture = json.loads(
        (Path(__file__).parent / "fixtures" / "garmin_summary_training.json").read_text()
    )
    target = date(2026, 7, 24)
    daily = normalize_snapshot(fixture["daily_summary"], target, DAILY_SUMMARY_FIELDS)
    training = normalize_snapshot(fixture["training_status"], target, TRAINING_STATUS_FIELDS)

    class Source:
        async def async_fetch_details(self, request_date, metric):
            if metric == "daily_summary":
                return daily
            if metric == "training_status":
                return training
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1))
    store = _Store()
    archive = _sync_archive(Source(), recorder, store)
    await archive.async_start()
    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written"
    snapshot_calls = [
        call for call in recorder.async_write.await_args_list
        if call.args[1].key.startswith(("daily_", "training_"))
    ]
    assert {call.args[1].key for call in snapshot_calls} == {
        DAILY_ABNORMAL_HR_METADATA.key,
        TRAINING_ACUTE_LOAD_METADATA.key,
        TRAINING_CHRONIC_LOAD_METADATA.key,
        TRAINING_LOAD_BALANCE_METADATA.key,
        TRAINING_ACWR_METADATA.key,
        TRAINING_VO2_MAX_METADATA.key,
        TRAINING_FITNESS_TREND_METADATA.key,
        TRAINING_RECOVERY_TIME_METADATA.key,
    }
    assert all(call.args[2][0].timestamp == datetime(2026, 7, 24, tzinfo=UTC) for call in snapshot_calls)
    assert all(call.args[2][0].request_date == target for call in snapshot_calls)
    recovery_call = next(call for call in snapshot_calls if call.args[1].key == TRAINING_RECOVERY_TIME_METADATA.key)
    assert recovery_call.args[0] == statistic_id_for("opaque-account-key-1234567890", TRAINING_RECOVERY_TIME_METADATA.key)
    assert recovery_call.args[2][0].timestamp == datetime(2026, 7, 24, tzinfo=UTC)
    assert recovery_call.args[1].unit_of_measurement == "s"
    assert all(call.args[0] == statistic_id_for("opaque-account-key-1234567890", call.args[1].key) for call in snapshot_calls)
    assert store.data["presence"][target.isoformat()]["training_status:recovery_time"] == "present"

    restarted = _sync_archive(Source(), recorder, _Store(store.data))
    await restarted.async_start()
    assert restarted.get_history_presence(target, target)[target.isoformat()]["daily_summary:abnormal_heart_rate_alerts"] == "present"
    before = recorder.async_write.await_count
    await restarted.async_sync_range(target, target)
    assert recorder.async_write.await_count == before


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


def _sleep_session() -> SleepSession:
    return SleepSession(
        "sleep-id", "main",
        datetime(2026, 1, 1, 22, tzinfo=UTC),
        datetime(2026, 1, 2, 6, tzinfo=UTC),
        date(2026, 1, 1), "revision", {}, (), (), (),
    )


@pytest.mark.asyncio
async def test_sleep_partition_failure_does_not_publish_completed_checkpoint():
    session = _sleep_session()

    class Source:
        async def async_fetch_details(self, target, metric):
            return (session,) if metric == "sleep_sessions" else ()

    catalog = _NamedStore({"account_key": "opaque-account-key-1234567890", "schema_version": 1})
    stores = {"garmin_connect.e.history_catalog": catalog}
    stores["garmin_connect.e.sleep_2026"] = _NamedStore(fail_save=True)
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "failed"
    assert "completed_dates" not in catalog.data


@pytest.mark.asyncio
async def test_sleep_streams_write_distinct_statistics_and_calendar_stays_bounded():
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_sleep_streams.json").read_text())
    session = parse_sleep_sessions(fixture, date(2026, 7, 24))[0]
    assert {stream.metric: len(stream.points) for stream in session.streams} == {
        "heart_rate": 32, "hrv": 32, "body_battery": 32, "stress": 32,
        "respiration": 32, "spo2": 32, "movement": 32,
    }

    class Source:
        async def async_fetch_details(self, target, metric):
            return (session,) if metric == "sleep_sessions" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1, inserted_count=1))
    catalog = _NamedStore()
    stores = {"garmin_connect.e.history_catalog": catalog}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    report = await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))

    stream_ids = {
        call.args[0]
        for call in recorder.async_write.await_args_list
        if ":sleep_" in call.args[0]
    }
    assert stream_ids == {
        statistic_id_for("opaque-account-key-1234567890", f"sleep_{metric}:" + session.logical_id)
        for metric in ("heart_rate", "hrv", "body_battery", "stress", "respiration", "spo2", "movement")
    }
    assert report.inserted_count >= 7
    events = await archive.async_get_calendar_events("sleep", date(2026, 7, 24), date(2026, 7, 25))
    assert events[0].summary == "Sleep"
    assert not hasattr(events[0], "streams")


@pytest.mark.asyncio
async def test_sleep_stream_failure_does_not_checkpoint_and_retry_converges():
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_sleep_streams.json").read_text())
    session = parse_sleep_sessions(fixture, date(2026, 7, 24))[0]

    class Source:
        async def async_fetch_details(self, target, metric):
            return (session,) if metric == "sleep_sessions" else ()

    failed = False

    async def write(statistic_id, metadata, samples):
        nonlocal failed
        if ":sleep_" in statistic_id and not failed:
            failed = True
            return RecorderWriteOutcome(0, "failed", "writer")
        return RecorderWriteOutcome(len(samples), inserted_count=len(samples))

    recorder = MagicMock()
    recorder.async_write = AsyncMock(side_effect=write)
    catalog = _NamedStore()
    stores = {"garmin_connect.e.history_catalog": catalog}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    first = await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))
    second = await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))

    assert first.outcome == "failed"
    assert second.outcome == "written"
    assert catalog.data["completed_dates"] == ["2026-07-24"]


@pytest.mark.asyncio
async def test_restart_drops_missing_or_corrupt_sleep_partition_from_completed_index():
    session = _sleep_session()
    catalog_data = {
        "account_key": "opaque-account-key-1234567890",
        "schema_version": 1,
        "completed_dates": ["2026-01-01"],
        "sleep_schema_version": 1,
        "sleep_index": {"2026": [session.logical_id]},
        "hrv_summaries": {}, "presence": {},
    }

    for partition_data in (None, {"year": "2026", "sessions": {"bad": "record"}}):
        stores = {
            "garmin_connect.e.history_catalog": _NamedStore(catalog_data),
            "garmin_connect.e.sleep_2026": _NamedStore(partition_data),
        }
        archive = _partition_archive(MagicMock(), MagicMock(), stores)
        await archive.async_start()
        assert "2026-01-01" not in archive._completed_dates
        assert await archive.async_get_calendar_events("sleep", date(2026, 1, 1), date(2026, 1, 2)) == ()


@pytest.mark.asyncio
async def test_health_events_archive_before_checkpoint_and_calendar_restart():
    event = normalize_health_events(
        {"events": [{"source": "MOVE_IQ", "type": "walking", "category": "activity", "startTime": "2026-07-24T10:00:00Z", "endTime": "2026-07-24T10:15:00Z"}]},
        date(2026, 7, 24),
    )[0]

    class Source:
        async def async_fetch_details(self, target, metric):
            if metric == "daily_summary":
                return SnapshotData({}, datetime(2026, 7, 24, tzinfo=UTC), "2026-07-24", (event,))
            if metric in {"health_events_daily", "health_events_body_battery"}:
                return (event,) if metric == "health_events_daily" else ()
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    catalog = _NamedStore()
    stores = {"garmin_connect.e.history_catalog": catalog}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    report = await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))
    assert report.outcome == "written"
    assert catalog.data["event_index"]["2026"] == [event.logical_id]
    assert stores["garmin_connect.e.sleep_2026"].data["events"][event.logical_id]["source"] == "MOVE_IQ"
    restarted = _partition_archive(Source(), recorder, stores)
    await restarted.async_start()
    events = await restarted.async_get_calendar_events("health", date(2026, 7, 24), date(2026, 7, 24))
    assert len(events) == 1 and events[0].summary == "activity"
    assert await restarted.async_get_calendar_events("health", date(2027, 1, 1), date(2027, 1, 1)) == ()


@pytest.mark.asyncio
async def test_corrupt_or_other_account_event_partition_is_ignored():
    catalog_data = {"account_key": "opaque-account-key-1234567890", "schema_version": 1, "completed_dates": ["2026-07-24"], "hrv_summaries": {}, "presence": {}, "sleep_index": {}, "event_index": {"2026": ["a" * 24]}}
    for partition in (
        {"account_key": "other-account-key-1234567890", "year": "2026", "sessions": {}, "events": {}},
        {"account_key": "opaque-account-key-1234567890", "year": "2026", "sessions": {}, "events": {"a" * 24: {"bad": True}}},
    ):
        stores = {"garmin_connect.e.history_catalog": _NamedStore(catalog_data), "garmin_connect.e.sleep_2026": _NamedStore(partition)}
        archive = _partition_archive(MagicMock(), MagicMock(), stores)
        await archive.async_start()
        assert "2026-07-24" not in archive._completed_dates
        assert await archive.async_get_calendar_events("health", date(2026, 7, 24), date(2026, 7, 24)) == ()


@pytest.mark.asyncio
async def test_runtime_missing_event_partition_invalidates_and_refetches():
    catalog = _NamedStore({"account_key": "opaque-account-key-1234567890", "schema_version": 1, "completed_dates": ["2026-07-24"], "hrv_summaries": {}, "presence": {}, "sleep_index": {}, "event_index": {"2026": ["a" * 24]}})
    stores = {"garmin_connect.e.history_catalog": catalog, "garmin_connect.e.sleep_2026": _NamedStore(None)}
    calls = 0

    class Source:
        async def async_fetch_details(self, target, metric):
            nonlocal calls
            if metric == "health_events_daily":
                calls += 1
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    assert "2026-07-24" not in archive._completed_dates
    await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))
    assert calls == 1


@pytest.mark.asyncio
async def test_timed_activity_archive_and_calendar_excludes_open_interval():
    activity = normalize_activities([{"activityId": 123, "activityType": "running", "activityName": "Morning Run", "startTime": "2026-07-24T23:30:00+02:00", "endTime": "2026-07-25T00:30:00+02:00", "durationInSeconds": 3600}], date(2026, 7, 24))[0]

    class Source:
        async def async_fetch_details(self, target, metric):
            return (activity,) if metric == "timed_activities" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    catalog = _NamedStore()
    stores = {"garmin_connect.e.history_catalog": catalog}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))
    events = await archive.async_get_calendar_events("activity", date(2026, 7, 24), date(2026, 7, 25))
    assert len(events) == 1 and events[0].summary == "Morning Run"


@pytest.mark.asyncio
async def test_calendar_loads_prior_year_partition_for_cross_year_sleep():
    session = parse_sleep_sessions(
        {"startTime": "2025-12-31T22:00:00Z", "endTime": "2026-01-01T06:00:00Z"},
        date(2025, 12, 31),
    )[0]
    catalog = _NamedStore({
        "account_key": "opaque-account-key-1234567890", "schema_version": 1,
        "completed_dates": [], "hrv_summaries": {}, "presence": {},
        "sleep_schema_version": 1, "sleep_index": {"2025": [session.logical_id]},
    })
    stores = {
        "garmin_connect.e.history_catalog": catalog,
        "garmin_connect.e.sleep_2025": _NamedStore({
            "account_key": "opaque-account-key-1234567890", "schema_version": 1,
            "sleep_schema_version": 1, "year": "2025",
            "sessions": {session.logical_id: session_record(session)},
        }),
    }
    archive = _partition_archive(MagicMock(), MagicMock(), stores)
    await archive.async_start()
    events = await archive.async_get_calendar_events("sleep", date(2026, 1, 1), date(2026, 1, 1))
    assert len(events) == 1
    assert events[0].start.date() == date(2025, 12, 31)
