"""Focused tests for the manual history synchronization slice."""

import asyncio
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from ha_garmin.exceptions import GarminConnectError

from custom_components.garmin_connect import history as history_module
from custom_components.garmin_connect.calendar import (
    GarminActivityCalendar,
    GarminHealthEventsCalendar,
    GarminSleepCalendar,
)
from custom_components.garmin_connect.const import CONF_ARCHIVE_ENABLED
from custom_components.garmin_connect.fit_archive import fit_file_name
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
    GarminHistorySource,
    HistorySchemaError,
    HRVData,
    HRVSummary,
    NormalizedSample,
    SegmentedData,
    SnapshotData,
    SourceSeries,
    normalize_activities,
    normalize_body_battery,
    normalize_floors,
    normalize_health_events,
    normalize_intensity,
    normalize_pair_series,
    normalize_snapshot,
    normalize_steps,
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


class _ImmediateGate:
    async def async_request(self, priority, request):
        return await request()


_SPARSE_SLEEP_STREAM_PRESENCE = {
    "sleep_stream:heart_rate": "null",
    "sleep_stream:hrv": "empty",
    "sleep_stream:body_battery": "missing",
    "sleep_stream:stress": "all-null",
    "sleep_stream:respiration": "present",
    "sleep_stream:spo2": "missing",
    "sleep_stream:movement": "missing",
}

def _sync_archive(source, recorder, store, *, options=None):
    entry = MagicMock(data={"history_account_key": "opaque-account-key-1234567890"}, entry_id="e")
    entry.options = {CONF_ARCHIVE_ENABLED: False} if options is None else options
    entry.runtime_data = SimpleNamespace(core=SimpleNamespace(client=object()), request_gate=object())
    archive = GarminHistoryArchive(
        MagicMock(), entry,
        recorder_checker=SimpleNamespace(async_check=AsyncMock(return_value=RecorderCompatibilityResult.compatible_result())),
        store_factory=lambda *args, **kwargs: store,
        source_factory=lambda *args: source,
        recorder_factory=lambda: recorder,
    )
    return archive


def _partition_archive(source, recorder, stores, *, options=None, data=None):
    entry = MagicMock(
        data=data or {"history_account_key": "opaque-account-key-1234567890"},
        entry_id="e",
    )
    entry.options = {CONF_ARCHIVE_ENABLED: False} if options is None else options
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
    entry.options = {CONF_ARCHIVE_ENABLED: True}
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
async def test_manual_repair_remains_available_while_archive_is_disabled():
    """Disablement stops automatic work but does not disable Manual Repair."""
    source = MagicMock()
    source.async_fetch = AsyncMock(return_value=())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _sync_archive(
        source,
        recorder,
        _Store(),
        options={CONF_ARCHIVE_ENABLED: False},
    )
    await archive.async_start()

    assert archive.status.state is HistoryArchiveState.DISABLED
    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "written"
    assert archive.status.state is HistoryArchiveState.DISABLED
    source.async_fetch.assert_awaited()


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
async def test_numeric_family_presence_survives_archive_normalization():
    """Every numeric family keeps source availability separate from samples."""

    class Source:
        async def async_fetch_details(self, target, metric):
            if metric == "nightly_hrv":
                return HRVData((), None, "null")
            if metric in {
                "steps",
                "floors",
                "intensity_moderate",
                "intensity_vigorous",
            }:
                return SegmentedData((), None, "empty")
            if metric in {"heart_rate", "stress", "body_battery"}:
                return SourceSeries((), "missing")
            if metric.startswith("respiration") or metric.startswith("spo2"):
                return SourceSeries((), "returned-empty")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _sync_archive(Source(), recorder, _Store())
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "written"
    assert archive.get_history_presence(date(2026, 1, 1), date(2026, 1, 1)) == {
        "2026-01-01": {
            "heart_rate": "missing",
            "stress": "missing",
            "body_battery": "missing",
            "nightly_hrv": "null",
            "steps": "empty",
            "floors": "empty",
            "intensity_moderate": "empty",
            "intensity_vigorous": "empty",
            "respiration_raw": "returned-empty",
            "respiration_average": "returned-empty",
            "spo2_single": "returned-empty",
            "spo2_continuous": "returned-empty",
            "spo2_hourly": "returned-empty",
        }
    }


@pytest.mark.asyncio
async def test_list_shaped_numeric_payloads_write_samples_and_presence() -> None:
    target = date(2026, 1, 1)
    body_battery = SourceSeries(
        tuple(normalize_body_battery([
            {"calendarDate": target.isoformat(), "bodyBatteryValuesArray": [["2026-01-01T01:00:00Z", 42]]},
        ], target)),
        "present",
    )

    class Source:
        async def async_fetch_details(self, request_date, metric):
            if metric == "body_battery":
                return body_battery
            if metric == "steps":
                return normalize_steps([{"timestamp": "2026-01-01T01:00:00Z", "steps": 12}], request_date)
            if metric == "floors":
                return normalize_floors([{"time": "2026-01-01T01:00:00Z", "floors": 2}], request_date)
            if metric == "intensity_moderate":
                return normalize_intensity([{"start": "2026-01-01T01:00:00Z", "moderateIntensityMinutes": 1}], request_date, "moderate")
            if metric == "intensity_vigorous":
                return normalize_intensity([{"start": "2026-01-01T01:00:00Z", "vigorousIntensityMinutes": 3}], request_date, "vigorous")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1, inserted_count=1))
    store = _Store()
    archive = _sync_archive(Source(), recorder, store)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written"
    assert archive.get_history_presence(target, target)[target.isoformat()] == {
        "body_battery": "present",
        "steps": "present",
        "steps:totalSteps": "absent",
        "floors": "present",
        "floors:floorsAscended": "absent",
        "floors:floorsDescended": "absent",
        "floors:floorsAscendedInMeters": "absent",
        "floors:floorsDescendedInMeters": "absent",
        "floors:totalFloors": "absent",
        "intensity_moderate": "present",
        "intensity_moderate:moderateIntensityMinutes": "absent",
        "intensity_moderate:vigorousIntensityMinutes": "absent",
        "intensity_moderate:totalIntensityMinutes": "absent",
        "intensity_vigorous": "present",
        "intensity_vigorous:moderateIntensityMinutes": "absent",
        "intensity_vigorous:vigorousIntensityMinutes": "absent",
        "intensity_vigorous:totalIntensityMinutes": "absent",
    }
    written = {
        call.args[1].key: call.args[2][0].value
        for call in recorder.async_write.await_args_list
        if call.args[2]
    }
    assert {key: written[key] for key in ("body_battery", "steps", "floors", "intensity_moderate", "intensity_vigorous")} == {
        "body_battery": 42.0,
        "steps": 12.0,
        "floors": 2.0,
        "intensity_moderate": 1.0,
        "intensity_vigorous": 3.0,
    }


@pytest.mark.asyncio
async def test_top_level_heart_rate_and_stress_lists_persist_samples_and_presence() -> None:
    target = date(2026, 1, 1)
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client.get_user_profile = AsyncMock(return_value=SimpleNamespace(display_name="user"))
    client._get_user_summary_raw = AsyncMock(return_value={})
    client.get_training_status = AsyncMock(return_value={})
    client._get_sleep_data_raw = AsyncMock(return_value={})
    client._get_hrv_data_raw = AsyncMock(return_value={"hrvReadings": []})
    client.get_activities = AsyncMock(return_value=[])

    async def request(_method, url, **_kwargs):
        if "dailyHeartRate" in url:
            return [["2026-01-01T01:00:00Z", 61]]
        if "dailyStress" in url:
            return [{"timestamp": "2026-01-01T01:05:00Z", "stressLevel": 14}]
        if "dailyEvents" in url or "/events/" in url:
            return []
        return {}

    client._request = AsyncMock(side_effect=request)
    source = GarminHistorySource(client, _ImmediateGate())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1, inserted_count=1))
    store = _Store()
    archive = _sync_archive(source, recorder, store)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written", report.error_type
    assert store.data["presence"][target.isoformat()]["heart_rate"] == "present"
    assert store.data["presence"][target.isoformat()]["stress"] == "present"
    written = {
        call.args[1].key: call.args[2][0].value
        for call in recorder.async_write.await_args_list
        if call.args[2]
    }
    assert written["heart_rate"] == 61.0
    assert written["stress"] == 14.0


@pytest.mark.asyncio
async def test_null_intraday_payloads_checkpoint_presence_without_samples() -> None:
    target = date(2026, 1, 1)
    client = MagicMock()
    client._base_url = "https://garmin.example"
    client.get_user_profile = AsyncMock(return_value=SimpleNamespace(display_name="user"))
    client._get_user_summary_raw = AsyncMock(return_value={})
    client.get_training_status = AsyncMock(return_value={})
    client._get_sleep_data_raw = AsyncMock(return_value=None)
    client._get_hrv_data_raw = AsyncMock(return_value=None)
    client.get_activities = AsyncMock(return_value=[])
    client._request = AsyncMock(return_value=None)
    source = GarminHistorySource(client, _ImmediateGate())
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    store = _Store()
    archive = _sync_archive(source, recorder, store)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written"
    assert archive.get_history_presence(target, target)[target.isoformat()]["heart_rate"] == "null"
    assert archive.get_history_presence(target, target)[target.isoformat()]["stress"] == "null"
    assert store.data["completed_dates"] == [target.isoformat()]


@pytest.mark.asyncio
async def test_archive_captures_every_numeric_catalog_family_without_thinning():
    """All frozen numeric families reach distinct statistics identities."""
    target = date(2026, 1, 1)
    sample = NormalizedSample(
        datetime(2026, 1, 2, 0, 15, tzinfo=UTC), target, "2026-01-01T16:15:00-08:00", 0.0
    )
    raw_metrics = {
        "heart_rate",
        "stress",
        "body_battery",
        "respiration_raw",
        "respiration_average",
        "spo2_single",
        "spo2_continuous",
        "spo2_hourly",
    }
    writes = []

    class Source:
        async def async_fetch_details(self, request_date, metric):
            if metric in raw_metrics:
                return SourceSeries((sample,), "present")
            if metric == "nightly_hrv":
                return HRVData((sample,), presence="present")
            if metric in {"steps", "floors", "intensity_moderate", "intensity_vigorous"}:
                return SegmentedData((sample,), presence="present")
            if metric == "daily_summary":
                return SnapshotData(
                    {"abnormal_heart_rate_alerts": ("present", 0.0)},
                    sample.timestamp,
                    sample.raw_timestamp,
                )
            if metric == "training_status":
                return SnapshotData(
                    {
                        field: ("present", float(index))
                        for index, field in enumerate(TRAINING_STATUS_FIELDS)
                    },
                    sample.timestamp,
                    sample.raw_timestamp,
                )
            return ()

    recorder = MagicMock()

    async def write(statistic_id, metadata, samples):
        writes.append((statistic_id, metadata, tuple(samples)))
        return RecorderWriteOutcome(len(samples), inserted_count=len(samples))

    recorder.async_write = AsyncMock(side_effect=write)
    archive = _sync_archive(Source(), recorder, _Store())
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written"
    assert {metadata.key for _, metadata, _ in writes} == {
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
        "daily_abnormal_heart_rate_alerts",
        "training_acute_load",
        "training_chronic_load",
        "training_load_balance",
        "training_acwr",
        "training_vo2_max",
        "training_fitness_trend",
        "training_recovery_time",
    }
    assert len(writes) == 21
    assert all(samples[0].request_date == target for _, _, samples in writes)
    assert all(len(samples) == 1 for _, _, samples in writes)
    assert next(samples[0].value for _, metadata, samples in writes if metadata.key == "heart_rate") == 0.0


@pytest.mark.asyncio
async def test_malformed_numeric_record_fails_archive_date_observably():
    """A malformed known numeric record fails its date; it is not a gap or zero."""

    class Source:
        async def async_fetch_details(self, target, metric):
            if metric == "heart_rate":
                return SourceSeries(
                    normalize_pair_series(
                        {"heartRateValues": ["malformed"]},
                        values_key="heartRateValues",
                        descriptor_keys=(),
                        value_keys=("heartRate",),
                    ),
                    "present",
                )
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _sync_archive(Source(), recorder, _Store())
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "failed"
    assert report.error_type == "sync_failed"
    assert archive.status.state is HistoryArchiveState.FAILED


@pytest.mark.asyncio
async def test_numeric_family_failure_does_not_block_other_families_or_checkpoint() -> None:
    target = date(2026, 1, 1)
    sample = NormalizedSample(datetime(2026, 1, 1, 1, tzinfo=UTC), target, "raw", 12.0)

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            if metric == "heart_rate":
                raise ValueError("malformed heart-rate family")
            if metric in {"stress", "spo2_single", "spo2_continuous", "spo2_hourly"}:
                return SourceSeries((sample,), "present")
            if metric == "steps":
                return SegmentedData((sample,), {"totalSteps": 7.0}, "present", {"totalSteps": "present"})
            if metric == "daily_summary":
                return SnapshotData(
                    {"abnormal_heart_rate_alerts": ("present", 2.0)},
                    sample.timestamp,
                    sample.raw_timestamp,
                )
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1, inserted_count=1))
    store = _Store()
    archive = _sync_archive(Source(), recorder, store)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    assert report.error_type == "sync_failed"
    assert store.data.get("completed_dates", []) == []
    presence = archive.get_history_presence(target, target)[target.isoformat()]
    assert presence["heart_rate"] == "failed"
    assert presence["stress"] == "present"
    assert presence["steps:totalSteps"] == "present"
    assert presence["daily_summary:abnormal_heart_rate_alerts"] == "present"
    assert {call.args[1].key for call in recorder.async_write.await_args_list} >= {
        "stress",
        "spo2_single",
        "spo2_continuous",
        "spo2_hourly",
        "steps",
        "steps_daily_total",
        "daily_abnormal_heart_rate_alerts",
    }


@pytest.mark.asyncio
async def test_numeric_failure_still_persists_structured_records_for_calendars() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:00:00Z",
            "endTime": "2026-07-25T07:00:00Z",
        },
        target,
    )[0]
    health_event = normalize_health_events(
        {
            "events": [
                {
                    "source": "MOVE_IQ",
                    "type": "walking",
                    "category": "activity",
                    "startTime": "2026-07-24T10:00:00Z",
                    "endTime": "2026-07-24T10:15:00Z",
                }
            ]
        },
        target,
    )[0]
    activity = normalize_activities(
        [
            {
                "activityId": 321,
                "activityType": "running",
                "activityName": "Failure-isolated run",
                "startTime": "2026-07-24T12:00:00Z",
                "durationInSeconds": 1800,
            }
        ],
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            if metric == "heart_rate":
                raise ValueError("injected numeric family failure")
            if metric == "sleep_sessions":
                return (session,)
            if metric == "health_events_daily":
                return (health_event,)
            if metric == "timed_activities":
                return (activity,)
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores: dict[str, _NamedStore] = {}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    assert report.error_type == "sync_failed"
    partition = stores["garmin_connect.e.sleep_2026"].data
    assert session.logical_id in partition["sessions"]
    assert health_event.logical_id in partition["events"]
    assert activity.logical_id in partition["activities"]
    assert len(
        await GarminSleepCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 26, tzinfo=UTC),
        )
    ) == 1
    assert len(
        await GarminHealthEventsCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
        )
    ) == 1
    assert len(
        await GarminActivityCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
        )
    ) == 1


@pytest.mark.asyncio
async def test_recorder_numeric_failure_still_persists_structured_records_for_calendars() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:00:00Z",
            "endTime": "2026-07-25T07:00:00Z",
        },
        target,
    )[0]
    health_event = normalize_health_events(
        {
            "events": [
                {
                    "source": "MOVE_IQ",
                    "type": "walking",
                    "category": "activity",
                    "startTime": "2026-07-24T10:00:00Z",
                    "endTime": "2026-07-24T10:15:00Z",
                }
            ]
        },
        target,
    )[0]
    activity = normalize_activities(
        [
            {
                "activityId": 654,
                "activityType": "cycling",
                "activityName": "Recorder failure-isolated ride",
                "startTime": "2026-07-24T12:00:00Z",
                "durationInSeconds": 1800,
            }
        ],
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            if metric == "sleep_sessions":
                return (session,)
            if metric == "health_events_daily":
                return (health_event,)
            if metric == "timed_activities":
                return (activity,)
            return ()

    async def write(statistic_id, _metadata, _samples):
        if statistic_id.endswith(":heart_rate"):
            return RecorderWriteOutcome(0, "failed", "writer")
        return RecorderWriteOutcome(0)

    recorder = MagicMock()
    recorder.async_write = AsyncMock(side_effect=write)
    stores: dict[str, _NamedStore] = {}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    assert report.error_type == "writer"
    partition = stores["garmin_connect.e.sleep_2026"].data
    assert session.logical_id in partition["sessions"]
    assert health_event.logical_id in partition["events"]
    assert activity.logical_id in partition["activities"]
    assert len(
        await GarminSleepCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 26, tzinfo=UTC),
        )
    ) == 1
    assert len(
        await GarminHealthEventsCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
        )
    ) == 1
    assert len(
        await GarminActivityCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
        )
    ) == 1


@pytest.mark.asyncio
async def test_structured_records_are_saved_before_later_activity_failure() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:00:00Z",
            "endTime": "2026-07-25T07:00:00Z",
        },
        target,
    )[0]
    health_event = normalize_health_events(
        {
            "events": [
                {
                    "source": "MOVE_IQ",
                    "type": "walking",
                    "category": "activity",
                    "startTime": "2026-07-24T10:00:00Z",
                    "endTime": "2026-07-24T10:15:00Z",
                }
            ]
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            if metric == "sleep_sessions":
                return (session,)
            if metric == "health_events_daily":
                return (health_event,)
            if metric == "timed_activities":
                raise ValueError("injected activity schema failure")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores: dict[str, _NamedStore] = {}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    assert report.error_type == "sync_failed"
    partition = stores["garmin_connect.e.sleep_2026"].data
    assert session.logical_id in partition["sessions"]
    assert health_event.logical_id in partition["events"]
    assert len(
        await GarminSleepCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 26, tzinfo=UTC),
        )
    ) == 1
    assert len(
        await GarminHealthEventsCalendar(archive, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
        )
    ) == 1


@pytest.mark.asyncio
async def test_garmin_client_error_becomes_bounded_archive_failure() -> None:
    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            if metric == "heart_rate":
                raise GarminConnectError("private endpoint failed")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _sync_archive(Source(), recorder, _Store())
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    assert report.outcome == "failed"
    assert report.error_type == "garmin_client_error"
    assert archive.status.state is HistoryArchiveState.FAILED


@pytest.mark.asyncio
async def test_sleep_numeric_stream_presence_is_sparse_and_valid_points_are_written() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z",
            "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRate": None,
            "hrvData": [],
            "sleepStress": [["2026-07-25T00:00:00Z", None]],
            "sleepRespiration": [
                ["2026-07-25T00:01:00Z", None],
                ["2026-07-25T00:02:00Z", 14],
            ],
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            return (session,) if metric == "sleep_sessions" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1, inserted_count=1))
    stores = {}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "written"
    presence = archive.get_history_presence(target, target)[target.isoformat()]
    assert {
        key: value for key, value in presence.items() if key.startswith("sleep_stream:")
    } == _SPARSE_SLEEP_STREAM_PRESENCE
    respiration_write = next(
        call for call in recorder.async_write.await_args_list if call.args[1].key == "sleep_respiration"
    )
    assert [point.value for point in respiration_write.args[2]] == [14.0]

    restarted = _partition_archive(Source(), recorder, stores)
    await restarted.async_start()
    assert restarted.get_history_presence(target, target)[target.isoformat()] == presence


@pytest.mark.asyncio
async def test_negative_sleep_stream_value_fails_the_date_without_silent_filtering() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z",
            "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRate": [["2026-07-25T00:00:00Z", -2]],
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            return (session,) if metric == "sleep_sessions" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, {})
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    assert report.error_type == "sleep_stream_invalid"
    assert not any(
        call.args[1].key.startswith("sleep_")
        for call in recorder.async_write.await_args_list
    )


@pytest.mark.asyncio
async def test_invalid_sleep_session_is_excluded_from_store_and_presence() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "startTime": "2026-07-24T23:45:00Z",
            "endTime": "2026-07-25T07:15:00Z",
            "sleepHeartRate": [["2026-07-25T00:00:00Z", -2]],
            "sleepRespiration": [["2026-07-25T00:01:00Z", 14]],
        },
        target,
    )[0]
    health_event = normalize_health_events(
        {
            "events": [
                {
                    "type": "abnormalHeartRate",
                    "category": "health",
                    "occurrenceTime": "2026-07-24T01:00:00Z",
                }
            ]
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            if metric == "sleep_sessions":
                return (session,)
            if metric == "health_events_daily":
                return (health_event,)
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores: dict[str, _NamedStore] = {}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    assert report.error_type == "sleep_stream_invalid"
    partition = stores["garmin_connect.e.sleep_2026"].data
    assert partition["sessions"] == {}
    assert health_event.logical_id in partition["events"]
    assert not any(
        key.startswith("sleep_stream:")
        for key in stores["garmin_connect.e.history_catalog"].data["presence"][
            target.isoformat()
        ]
    )


@pytest.mark.asyncio
async def test_many_sleep_sessions_keep_stream_presence_in_annual_partition() -> None:
    target = date(2026, 7, 24)
    sessions_payload = {
        "startTime": "2026-07-24T23:45:00Z",
        "endTime": "2026-07-25T07:15:00Z",
        "sleepHeartRate": None,
        "hrvData": [],
        "sleepStress": [["2026-07-25T00:00:00Z", None]],
        "sleepRespiration": [["2026-07-25T00:01:00Z", 14]],
        "napEvents": [
            {
                "startTime": f"2026-07-24T{hour:02d}:00:00Z",
                "endTime": f"2026-07-24T{hour:02d}:30:00Z",
                "sleepHeartRate": None,
                "hrvData": [],
                "sleepStress": [[f"2026-07-24T{hour:02d}:01:00Z", None]],
                "sleepRespiration": [[f"2026-07-24T{hour:02d}:02:00Z", 14]],
            }
            for hour in range(10, 20)
        ],
    }
    sessions = parse_sleep_sessions(sessions_payload, target)

    class Source:
        async def async_fetch_details(self, request_date: date, metric: str) -> object:
            if metric == "sleep_sessions":
                return sessions
            if metric in {"health_events_daily", "health_events_body_battery", "timed_activities"}:
                return ()
            if metric == "nightly_hrv":
                return HRVData((), presence="empty")
            if metric in {"steps", "floors", "intensity_moderate", "intensity_vigorous"}:
                total_keys = {
                    "steps": ("totalSteps",),
                    "floors": ("floorsAscended", "floorsDescended", "floorsAscendedInMeters", "floorsDescendedInMeters", "totalFloors"),
                    "intensity_moderate": ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes"),
                    "intensity_vigorous": ("moderateIntensityMinutes", "vigorousIntensityMinutes", "totalIntensityMinutes"),
                }[metric]
                return SegmentedData((), presence="empty", total_presence=dict.fromkeys(total_keys, "absent"))
            if metric == "daily_summary":
                return SnapshotData(
                    {"abnormal_heart_rate_alerts": ("absent", None)},
                    datetime.combine(request_date, datetime.min.time(), tzinfo=UTC),
                    request_date.isoformat(),
                )
            if metric == "training_status":
                return SnapshotData(
                    dict.fromkeys(TRAINING_STATUS_FIELDS, ("absent", None)),
                    datetime.combine(request_date, datetime.min.time(), tzinfo=UTC),
                    request_date.isoformat(),
                )
            return SourceSeries((), "empty")

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores = {}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    assert (await archive.async_sync_range(target, target)).outcome == "written"
    catalog = stores["garmin_connect.e.history_catalog"]
    assert len(catalog.data["presence"][target.isoformat()]) == 40
    assert not any(
        key.startswith(("sleep_heart_rate:", "sleep_hrv:", "sleep_body_battery:", "sleep_stress:", "sleep_respiration:", "sleep_spo2:", "sleep_movement:"))
        for key in catalog.data["presence"][target.isoformat()]
    )
    assert catalog.data["presence"][target.isoformat()]["sleep_stream:heart_rate"] == "null"

    restarted = _partition_archive(Source(), recorder, stores)
    await restarted.async_start()
    assert restarted.status.state is HistoryArchiveState.DISABLED
    restored_presence = restarted.get_history_presence(target, target)[target.isoformat()]
    sleep_presence = {
        key: value for key, value in restored_presence.items() if key.startswith("sleep_stream:")
    }
    assert sleep_presence == _SPARSE_SLEEP_STREAM_PRESENCE
    assert not any(
        key.startswith(
            (
                "sleep_heart_rate:",
                "sleep_hrv:",
                "sleep_body_battery:",
                "sleep_stress:",
                "sleep_respiration:",
                "sleep_spo2:",
                "sleep_movement:",
            )
        )
        for key in restored_presence
    )


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
    assert all(call.args[2][0].timestamp == datetime(2026, 7, 23, 16, tzinfo=UTC) for call in snapshot_calls)
    assert all(call.args[2][0].request_date == target for call in snapshot_calls)
    recovery_call = next(call for call in snapshot_calls if call.args[1].key == TRAINING_RECOVERY_TIME_METADATA.key)
    assert recovery_call.args[0] == statistic_id_for("opaque-account-key-1234567890", TRAINING_RECOVERY_TIME_METADATA.key)
    assert recovery_call.args[2][0].timestamp == datetime(2026, 7, 23, 16, tzinfo=UTC)
    assert recovery_call.args[1].unit_of_measurement == "s"
    assert all(call.args[0] == statistic_id_for("opaque-account-key-1234567890", call.args[1].key) for call in snapshot_calls)
    assert store.data["presence"][target.isoformat()]["training_status:recovery_time"] == "present"

    restarted = _sync_archive(Source(), recorder, _Store(store.data))
    await restarted.async_start()
    assert restarted.get_history_presence(target, target)[target.isoformat()]["daily_summary:abnormal_heart_rate_alerts"] == "present"
    before = recorder.async_write.await_count
    repaired = await restarted.async_sync_range(target, target)
    assert repaired.outcome == "written"
    assert recorder.async_write.await_count > before


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
async def test_checkpoint_persists_each_date_and_manual_repair_retries_it():
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

    assert report.outcome == "written"
    assert report.skipped_count == 0
    assert report.processed_dates == (date(2026, 1, 1), date(2026, 1, 2))
    assert source.async_fetch.await_count == 30


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
async def test_segmented_total_presence_distinguishes_absent_null_and_zero() -> None:
    dates = {
        date(2026, 1, 1): normalize_steps({"summary": {}}, date(2026, 1, 1)),
        date(2026, 1, 2): normalize_steps({"summary": {"totalSteps": None}}, date(2026, 1, 2)),
        date(2026, 1, 3): normalize_steps({"summary": {"totalSteps": 0}}, date(2026, 1, 3)),
    }

    class Source:
        async def async_fetch_details(self, target_date, metric):
            if metric == "steps":
                return dates[target_date]
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    store = _Store()
    archive = _sync_archive(Source(), recorder, store)
    await archive.async_start()

    report = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 3))

    assert report.outcome == "written"
    presence = archive.get_history_presence(date(2026, 1, 1), date(2026, 1, 3))
    assert presence["2026-01-01"]["steps:totalSteps"] == "absent"
    assert presence["2026-01-02"]["steps:totalSteps"] == "null"
    assert presence["2026-01-03"]["steps:totalSteps"] == "present"
    assert [call.args[1].key for call in recorder.async_write.await_args_list].count("steps_daily_total") == 1


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


@pytest.mark.asyncio
@pytest.mark.parametrize("structured_outcome", ["cancelled", "failed"])
async def test_numeric_sidecars_survive_first_structured_checkpoint_restart(
    structured_outcome: str,
) -> None:
    target = date(2026, 1, 1)
    sample = NormalizedSample(
        datetime(2026, 1, 1, 3, tzinfo=UTC), target, "2026-01-01T03:00:00Z", 52.0
    )

    class Source:
        async def async_fetch_details(self, request_date: date, metric: str) -> object:
            if metric == "nightly_hrv":
                return HRVData(
                    (sample,), HRVSummary("balanced", 48.0, 72.0, 50.0, {"low": 40.0})
                )
            if metric == "daily_summary":
                return SnapshotData(
                    {"abnormal_heart_rate_alerts": ("present", 2.0)},
                    datetime.combine(request_date, datetime.min.time(), tzinfo=UTC),
                    request_date.isoformat(),
                    calendar_date=request_date,
                )
            if metric == "sleep_sessions":
                if structured_outcome == "cancelled":
                    raise asyncio.CancelledError
                raise ValueError("injected structured family failure")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    store = _Store()
    archive = _sync_archive(Source(), recorder, store)
    await archive.async_start()

    if structured_outcome == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await archive.async_sync_range(target, target)
    else:
        report = await archive.async_sync_range(target, target)
        assert report.outcome == "failed"

    restarted = _sync_archive(Source(), recorder, store)
    await restarted.async_start()

    summaries = restarted.get_hrv_summaries(target, target)
    assert summaries[0][1].weekly_avg == 50.0
    assert restarted.get_history_presence(target, target)[target.isoformat()][
        "daily_summary:abnormal_heart_rate_alerts"
    ] == "present"
    assert store.data["reconciliation"][target.isoformat()]["state"] == "open"
    assert store.data["reconciliation"][target.isoformat()]["outcome"] != "continuity_gap"


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
    assert catalog.data.get("completed_dates", []) == []


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
    stress_write = next(
        call
        for call in recorder.async_write.await_args_list
        if call.args[1].key == "sleep_stress"
    )
    assert [sample.value for sample in stress_write.args[2]] == list(range(31))
    assert report.inserted_count >= 7
    events = await archive.async_get_calendar_events("sleep", date(2026, 7, 24), date(2026, 7, 25))
    assert events[0].summary == "Sleep"
    assert not hasattr(events[0], "streams")


@pytest.mark.asyncio
async def test_sleep_calendar_prefilter_uses_utc_source_instant_dates() -> None:
    target = date(2026, 7, 24)
    session = parse_sleep_sessions(
        {
            "sleepStartTimestampGMT": "2026-07-24T23:30:00+14:00",
            "sleepEndTimestampGMT": "2026-07-25T01:00:00+14:00",
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            return (session,) if metric == "sleep_sessions" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, {})
    await archive.async_start()
    await archive.async_sync_range(target, target)

    assert len(await archive.async_get_calendar_events("sleep", target, target)) == 1
    assert (
        await archive.async_get_calendar_events(
            "sleep", date(2026, 7, 25), date(2026, 7, 25)
        )
        == ()
    )


@pytest.mark.asyncio
async def test_disabled_archive_keeps_retained_calendar_query_available():
    """Disablement leaves already persisted Calendar records queryable."""
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_sleep_streams.json").read_text())
    session = parse_sleep_sessions(fixture, date(2026, 7, 24))[0]

    class Source:
        async def async_fetch_details(self, target, metric):
            return (session,) if metric == "sleep_sessions" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(1, inserted_count=1))
    catalog = _NamedStore()
    stores = {"garmin_connect.e.history_catalog": catalog}
    enabled = _partition_archive(
        Source(),
        recorder,
        stores,
        options={CONF_ARCHIVE_ENABLED: True},
        data={
            "history_account_key": "opaque-account-key-1234567890",
            "archive_last_enabled": True,
            "archive_activation_date": "2026-07-24",
        },
    )
    await enabled.async_start()
    await enabled.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))

    disabled = _partition_archive(
        Source(),
        recorder,
        stores,
        options={CONF_ARCHIVE_ENABLED: False},
        data={
            "history_account_key": "opaque-account-key-1234567890",
            "archive_last_enabled": True,
            "archive_activation_date": "2026-07-24",
        },
    )
    await disabled.async_start()

    events = await disabled.async_get_calendar_events(
        "sleep", date(2026, 7, 24), date(2026, 7, 25)
    )
    assert events[0].summary == "Sleep"


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
async def test_structured_health_family_is_queryable_when_following_family_fails() -> None:
    """Each structured family is durable before the next family is fetched."""
    target = date(2026, 7, 24)
    event = normalize_health_events(
        {
            "events": [
                {
                    "source": "GARMIN",
                    "type": "walking",
                    "category": "activity",
                    "startTime": "2026-07-24T10:00:00Z",
                    "endTime": "2026-07-24T10:15:00Z",
                }
            ]
        },
        target,
    )[0]

    class Source:
        async def async_fetch_details(self, request_date, metric):
            if metric == "health_events_daily":
                return (event,)
            if metric == "health_events_body_battery":
                raise GarminConnectError("body battery endpoint failed")
            return ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()

    report = await archive.async_sync_range(target, target)

    assert report.outcome == "failed"
    events = await GarminHealthEventsCalendar(archive, "e").async_get_events(
        MagicMock(),
        datetime(2026, 7, 24, tzinfo=UTC),
        datetime(2026, 7, 25, tzinfo=UTC),
    )
    assert [item.summary for item in events] == ["activity"]

    restarted = _partition_archive(Source(), recorder, stores)
    await restarted.async_start()
    assert [
        item.summary
        for item in await GarminHealthEventsCalendar(restarted, "e").async_get_events(
            MagicMock(),
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
        )
    ] == ["activity"]


@pytest.mark.asyncio
async def test_occurrence_only_health_event_is_a_positive_calendar_interval() -> None:
    occurrence = datetime(2026, 7, 24, 10, 0, tzinfo=UTC)
    event = normalize_health_events(
        {
            "events": [
                {
                    "source": "GARMIN",
                    "type": "abnormalHeartRate",
                    "occurrenceTime": occurrence.isoformat(),
                }
            ]
        },
        occurrence.date(),
    )[0]

    class Source:
        async def async_fetch_details(self, target, metric):
            return (event,) if metric == "health_events_daily" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(occurrence.date(), occurrence.date())

    stored = stores["garmin_connect.e.sleep_2026"].data["events"][event.logical_id]
    assert stored["start"] is None
    assert stored["end"] is None
    assert stored["occurrence"] == occurrence.isoformat()

    calendar = GarminHealthEventsCalendar(archive, "e")
    events = await calendar.async_get_events(
        MagicMock(),
        datetime(2026, 7, 24, tzinfo=UTC),
        datetime(2026, 7, 25, tzinfo=UTC),
    )

    assert [(item.start, item.end) for item in events] == [
        (occurrence, occurrence + timedelta(seconds=1))
    ]


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
async def test_activity_calendar_derives_end_from_duration_without_changing_source_end():
    activity = normalize_activities([{"activityId": 123, "activityType": "running", "activityName": "Morning Run", "startTime": "2026-07-24T23:30:00+02:00", "durationInSeconds": 3600}], date(2026, 7, 24))[0]
    assert activity.end is None

    class Source:
        async def async_fetch(self, target, metric):
            return ()

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
    assert [(event.summary, event.start, event.end) for event in events] == [
        (
            "Morning Run",
            datetime(2026, 7, 24, 21, 30, tzinfo=UTC),
            datetime(2026, 7, 24, 22, 30, tzinfo=UTC),
        )
    ]
    assert stores["garmin_connect.e.sleep_2026"].data["activities"][activity.logical_id]["end"] is None


@pytest.mark.parametrize("duration", [-1.0, float("nan"), float("inf")])
def test_activity_normalization_rejects_negative_or_non_finite_duration(
    duration: float,
) -> None:
    with pytest.raises(HistorySchemaError):
        normalize_activities(
            [
                {
                    "activityId": 125,
                    "activityType": "running",
                    "startTime": "2026-07-24T10:00:00Z",
                    "durationInSeconds": duration,
                }
            ],
            date(2026, 7, 24),
        )


@pytest.mark.asyncio
async def test_activity_calendar_projects_zero_duration_without_changing_source_record() -> None:
    activity = normalize_activities(
        [
            {
                "activityId": 126,
                "activityType": "running",
                "startTime": "2026-07-24T10:00:00Z",
                "durationInSeconds": 0.0,
            }
        ],
        date(2026, 7, 24),
    )[0]
    assert activity.end is None

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            return (activity,) if metric == "timed_activities" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))

    stored = stores["garmin_connect.e.sleep_2026"].data["activities"][activity.logical_id]
    assert stored["end"] is None

    calendar = GarminActivityCalendar(archive, "e")
    events = await calendar.async_get_events(
        MagicMock(),
        datetime(2026, 7, 24, tzinfo=UTC),
        datetime(2026, 7, 25, tzinfo=UTC),
    )

    assert [(event.start, event.end) for event in events] == [
        (activity.start, activity.start + timedelta(seconds=1))
    ]


@pytest.mark.asyncio
async def test_zero_length_activity_record_survives_persistence_and_restart() -> None:
    activity = normalize_activities(
        [{
            "activityId": 127,
            "activityType": "running",
            "startTime": "2026-07-24T10:00:00Z",
            "endTime": "2026-07-24T10:00:00Z",
            "durationInSeconds": 0.0,
        }],
        date(2026, 7, 24),
    )[0]
    assert activity.end == activity.start

    class Source:
        async def async_fetch_details(self, _target: date, metric: str) -> object:
            return (activity,) if metric == "timed_activities" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))

    stored = stores["garmin_connect.e.sleep_2026"].data["activities"][activity.logical_id]
    assert stored["start"] == activity.start.isoformat()
    assert stored["end"] == activity.end.isoformat()
    assert stored["duration_seconds"] == 0.0
    first_events = await archive.async_get_calendar_events(
        "activity", date(2026, 7, 24), date(2026, 7, 24)
    )
    assert [(event.start, event.end) for event in first_events] == [
        (activity.start, activity.start + timedelta(seconds=1))
    ]

    restarted = _partition_archive(Source(), recorder, stores)
    await restarted.async_start()
    restarted_events = await restarted.async_get_calendar_events(
        "activity", date(2026, 7, 24), date(2026, 7, 24)
    )
    assert [(event.start, event.end) for event in restarted_events] == [
        (activity.start, activity.start + timedelta(seconds=1))
    ]


@pytest.mark.asyncio
async def test_activity_calendar_uses_persisted_source_calendar_date_across_utc_midnight():
    activity = normalize_activities(
        [{
            "activityId": 124,
            "activityType": "running",
            "activityName": "New Year Run",
            "startTime": "2025-12-31T22:30:00Z",
            "startTimeLocal": "2026-01-01T00:30:00+02:00",
            "endTime": "2025-12-31T22:45:00Z",
        }],
        date(2026, 1, 1),
    )[0]
    assert activity.calendar_date == date(2026, 1, 1)
    assert activity.start == datetime(2025, 12, 31, 22, 30, tzinfo=UTC)

    class Source:
        async def async_fetch(self, target, metric):
            return ()

        async def async_fetch_details(self, target, metric):
            return (activity,) if metric == "timed_activities" else ()

    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))

    events = await archive.async_get_calendar_events(
        "activity", date(2026, 1, 1), date(2026, 1, 1)
    )

    assert [(event.summary, event.start, event.end) for event in events] == [
        (
            "New Year Run",
            datetime(2025, 12, 31, 22, 30, tzinfo=UTC),
            datetime(2025, 12, 31, 22, 45, tzinfo=UTC),
        )
    ]


def test_activity_fixture_preserves_training_fields_and_excludes_raw_route() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_activity_archive.json").read_text())
    activity = next(item for item in normalize_activities(fixture["activities"], date(2026, 7, 24)) if item.activity_id == "12345")
    assert activity.training_effect == 3.2
    assert activity.load == 82.0
    assert activity.recovery is None
    assert not hasattr(activity, "polyline")


@pytest.mark.asyncio
async def test_activity_partition_restart_and_corruption_invalidate_checkpoint() -> None:
    activity = normalize_activities(
        [{"activityId": 987, "activityType": "running", "activityName": "Archive run",
          "startTime": "2026-07-24T10:00:00Z", "endTime": "2026-07-24T11:00:00Z"}],
        date(2026, 7, 24),
    )[0]

    class Source:
        async def async_fetch_details(self, target, metric):
            return (activity,) if metric == "timed_activities" else ()

    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 7, 24), date(2026, 7, 24))

    restarted = _partition_archive(Source(), recorder, stores)
    await restarted.async_start()
    assert len(await restarted.async_get_calendar_events("activity", date(2026, 7, 24), date(2026, 7, 24))) == 1

    partition = stores["garmin_connect.e.history_catalog"]
    assert "2026-07-24" in partition.data["completed_dates"]
    sleep_store = stores["garmin_connect.e.sleep_2026"]
    sleep_store.data["activities"] = {activity.logical_id: {"logical_id": activity.logical_id}}
    corrupted = _partition_archive(Source(), recorder, stores)
    await corrupted.async_start()
    assert "2026-07-24" not in corrupted._completed_dates
    assert "2026" not in stores["garmin_connect.e.history_catalog"].data["activity_index"]
    assert await corrupted.async_get_calendar_events("activity", date(2026, 7, 24), date(2026, 7, 24)) == ()


@pytest.mark.asyncio
async def test_activity_partition_account_mismatch_isolated() -> None:
    stores = {
        "garmin_connect.e.history_catalog": _NamedStore({
            "account_key": "opaque-account-key-1234567890", "schema_version": 1,
            "completed_dates": ["2026-07-24"], "hrv_summaries": {}, "presence": {},
            "sleep_index": {}, "event_index": {}, "activity_index": {"2026": ["bad"]},
        }),
        "garmin_connect.e.sleep_2026": _NamedStore({
            "account_key": "another-account", "schema_version": 1, "sleep_schema_version": 1,
            "year": "2026", "sessions": {}, "events": {}, "activities": {},
        }),
    }
    archive = _partition_archive(MagicMock(), MagicMock(), stores)
    await archive.async_start()
    assert "2026-07-24" not in archive._completed_dates
    assert "2026" not in stores["garmin_connect.e.history_catalog"].data["activity_index"]
    assert await archive.async_get_calendar_events("activity", date(2026, 7, 24), date(2026, 7, 24)) == ()


@pytest.mark.asyncio
async def test_activity_partition_uses_local_calendar_year_at_utc_year_boundary() -> None:
    activity = normalize_activities(
        [{"activityId": 998, "activityType": "running", "startTimeLocal": "2026-01-01T00:30:00+02:00", "durationInSeconds": 60}],
        date(2026, 1, 1),
    )[0]

    class Source:
        async def async_fetch(self, target, metric):
            return ()

        async def async_fetch_details(self, target, metric):
            return (activity,) if metric == "timed_activities" else ()

    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, stores)
    await archive.async_start()
    await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1))
    assert "garmin_connect.e.sleep_2026" in stores
    assert "garmin_connect.e.sleep_2025" not in stores


@pytest.mark.asyncio
async def test_activity_calendar_date_correction_moves_record_between_year_partitions() -> None:
    initial = normalize_activities(
        [{
            "activityId": 1001,
            "activityType": "running",
            "startTime": "2026-12-31T22:30:00Z",
            "startTimeLocal": "2026-12-31T23:30:00+01:00",
            "durationInSeconds": 60,
        }],
        date(2026, 12, 31),
    )[0]
    corrected = normalize_activities(
        [{
            "activityId": 1001,
            "activityType": "running",
            "startTime": "2026-12-31T22:30:00Z",
            "startTimeLocal": "2027-01-01T00:30:00+02:00",
            "durationInSeconds": 60,
        }],
        date(2027, 1, 1),
    )[0]

    class Source:
        activity = initial

        async def async_fetch(self, _target: date, _metric: str) -> tuple[()]:
            return ()

        async def async_fetch_details(self, _target: date, metric: str) -> object:
            return (self.activity,) if metric == "timed_activities" else ()

    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    source = Source()
    archive = _partition_archive(source, recorder, stores)
    await archive.async_start()

    await archive.async_sync_range(date(2026, 12, 31), date(2026, 12, 31))
    source.activity = corrected
    await archive.async_sync_range(date(2027, 1, 1), date(2027, 1, 1))
    retry = await archive.async_sync_range(date(2027, 1, 1), date(2027, 1, 1))

    assert retry.outcome == "written"
    assert retry.skipped_count == 0
    assert stores["garmin_connect.e.sleep_2026"].data["activities"] == {}
    assert set(stores["garmin_connect.e.sleep_2027"].data["activities"]) == {
        corrected.logical_id
    }
    assert stores["garmin_connect.e.history_catalog"].data["activity_index"] == {
        "2026": [],
        "2027": [corrected.logical_id],
    }
    events = await archive.async_get_calendar_events(
        "activity", date(2027, 1, 1), date(2027, 1, 1)
    )
    assert [(event.start, event.end) for event in events] == [
        (corrected.start, corrected.start + timedelta(seconds=60))
    ]


@pytest.mark.asyncio
async def test_bad_fit_is_removed_without_touching_same_year_activity_partition() -> None:
    activity = normalize_activities(
        [{"activityId": 999, "activityType": "running", "startTime": "2026-01-01T10:00:00Z", "durationInSeconds": 60}],
        date(2026, 1, 1),
    )[0]
    activity_record = {
        "logical_id": activity.logical_id, "activity_id": activity.activity_id, "revision": activity.revision,
        "calendar_date": activity.calendar_date.isoformat(), "activity_type": activity.activity_type,
        "name": activity.name, "start": activity.start.isoformat(), "end": None,
        "duration_seconds": activity.duration_seconds, "training_effect": activity.training_effect,
        "load": activity.load, "recovery": activity.recovery,
    }
    summary = json.loads((Path(__file__).parent / "fixtures" / "garmin_fit_structural_summary.json").read_text())["summary"]
    catalog = _NamedStore({
        "account_key": "opaque-account-key-1234567890", "schema_version": 1,
        "completed_dates": ["2026-01-01"], "hrv_summaries": {}, "presence": {},
        "sleep_index": {}, "event_index": {}, "activity_index": {"2026": [activity.logical_id]},
    })
    partition = _NamedStore({
        "account_key": "opaque-account-key-1234567890", "schema_version": 1,
        "sleep_schema_version": 1, "year": "2026", "sessions": {}, "events": {},
        "activities": {activity.logical_id: activity_record},
        "fits": {activity.logical_id: {"logical_id": activity.logical_id, "path": fit_file_name(activity.logical_id), "summary": summary}},
    })
    stores = {"garmin_connect.e.history_catalog": catalog, "garmin_connect.e.sleep_2026": partition}
    archive = _partition_archive(MagicMock(), MagicMock(), stores)
    await archive.async_start()
    assert partition.data["fits"] == {}
    assert activity.logical_id in partition.data["activities"]


@pytest.mark.asyncio
async def test_valid_fit_survives_restart_revalidation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    activity = normalize_activities(
        [{"activityId": 1000, "activityType": "running", "startTime": "2026-01-01T10:00:00Z", "durationInSeconds": 60}],
        date(2026, 1, 1),
    )[0]
    activity_record = {
        "logical_id": activity.logical_id, "activity_id": activity.activity_id, "revision": activity.revision,
        "calendar_date": activity.calendar_date.isoformat(), "activity_type": activity.activity_type,
        "name": activity.name, "start": activity.start.isoformat(), "end": None,
        "duration_seconds": activity.duration_seconds, "training_effect": activity.training_effect,
        "load": activity.load, "recovery": activity.recovery,
    }
    summary = json.loads((Path(__file__).parent / "fixtures" / "garmin_fit_structural_summary.json").read_text())["summary"]
    fit_path = tmp_path / fit_file_name(activity.logical_id)
    fit_path.write_bytes(b"private fit")
    fit_path.chmod(0o600)
    catalog = _NamedStore({"account_key": "opaque-account-key-1234567890", "schema_version": 1, "completed_dates": [], "hrv_summaries": {}, "presence": {}, "sleep_index": {}, "event_index": {}, "activity_index": {"2026": [activity.logical_id]}})
    partition = _NamedStore({"account_key": "opaque-account-key-1234567890", "schema_version": 1, "sleep_schema_version": 1, "year": "2026", "sessions": {}, "events": {}, "activities": {activity.logical_id: activity_record}, "fits": {activity.logical_id: {"logical_id": activity.logical_id, "path": fit_path.name, "summary": summary}}})
    stores = {"garmin_connect.e.history_catalog": catalog, "garmin_connect.e.sleep_2026": partition}
    archive = _partition_archive(MagicMock(), MagicMock(), stores)
    archive._hass.config.path.side_effect = lambda *parts: str(tmp_path.joinpath(*parts[2:]))
    inspected_paths: list[Path] = []

    def inspect_valid_fit(path: Path, mode: int) -> dict[str, object]:
        inspected_paths.append(path)
        assert path == fit_path
        assert mode == 0o600
        return {**summary, "file": {"integrity_ok": True, "decode_ok": True}}

    monkeypatch.setattr(history_module, "inspect_fit", inspect_valid_fit)
    await archive.async_start()
    assert inspected_paths == [fit_path]
    assert activity.logical_id in archive._fit_archives["2026"]
    assert partition.data["fits"][activity.logical_id]["logical_id"] == activity.logical_id


@pytest.mark.asyncio
async def test_background_fit_limit_defers_then_converges_across_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    activities = normalize_activities(
        [{"activityId": 1100, "activityType": "running", "startTime": "2026-01-01T10:00:00Z", "durationInSeconds": 60}, {"activityId": 1101, "activityType": "cycling", "startTime": "2026-01-01T12:00:00Z", "durationInSeconds": 60}],
        date(2026, 1, 1),
    )
    client = MagicMock()
    client.download_activity = AsyncMock(return_value=b"fit")

    class Source:
        async def async_fetch(self, target, metric):
            return ()

        async def async_fetch_details(self, target, metric):
            return activities if metric == "timed_activities" else ()

    summary = {"message_counts": {"record": 1}, "message_fields": {"record": ["timestamp"]}, "time_coverage": {"start": None, "end": None}, "presence": dict.fromkeys(("heart_rate", "temperature", "gps", "cadence", "speed", "power", "training_effect", "training_load", "recovery_time", "recovery"), False), "file": {"integrity_ok": True, "decode_ok": True}}
    monkeypatch.setattr(history_module, "inspect_fit", lambda path, mode: summary)
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, stores)
    archive._entry.runtime_data.core.client = client
    archive._hass.config.path.return_value = str(tmp_path)
    await archive.async_start()
    archive._archive_enabled = True
    first = await archive.async_sync_range(date(2026, 1, 1), date(2026, 1, 1), fit_limit=1, include_training_status=False)
    assert first.outcome == "written"
    assert first.fit_count == 1
    assert "2026-01-01" in stores["garmin_connect.e.history_catalog"].data["completed_dates"]
    assert len(stores["garmin_connect.e.sleep_2026"].data["fits"]) == 1
    assert stores["garmin_connect.e.history_catalog"].data["fit_queue"] == [
        {
            "logical_id": activities[1].logical_id,
            "activity_id": activities[1].activity_id,
            "year": "2026",
            "calendar_date": "2026-01-01",
        }
    ]
    assert client.download_activity.await_count == 1


@pytest.mark.asyncio
async def test_first_sync_downloads_at_most_one_fit_without_dropping_activity_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    activities = normalize_activities(
        [
            {"activityId": 1200, "activityType": "running", "startTime": "2026-08-04T10:00:00Z", "durationInSeconds": 60},
            {"activityId": 1201, "activityType": "cycling", "startTime": "2026-08-04T12:00:00Z", "durationInSeconds": 60},
            {"activityId": 1199, "activityType": "walking", "startTime": "2026-08-03T12:00:00Z", "durationInSeconds": 60},
        ],
        date(2026, 8, 4),
    )
    client = MagicMock()
    client.download_activity = AsyncMock(return_value=b"fit")

    class Source:
        async def async_fetch(self, target, metric):
            return ()

        async def async_fetch_details(self, target, metric):
            return activities if metric == "timed_activities" else ()

    summary = {
        "message_counts": {"record": 1},
        "message_fields": {"record": ["timestamp"]},
        "time_coverage": {"start": None, "end": None},
        "presence": {},
        "file": {"integrity_ok": True, "decode_ok": True},
    }
    monkeypatch.setattr(history_module, "inspect_fit", lambda path, mode: summary)
    monkeypatch.setattr(
        history_module,
        "async_archive_fit",
        AsyncMock(
            side_effect=lambda **kwargs: {
                "logical_id": kwargs["logical_id"],
                "path": fit_file_name(kwargs["logical_id"]),
                "summary": summary,
            }
        ),
    )
    stores = {"garmin_connect.e.history_catalog": _NamedStore()}
    recorder = MagicMock()
    recorder.async_write = AsyncMock(return_value=RecorderWriteOutcome(0))
    archive = _partition_archive(Source(), recorder, stores, options={CONF_ARCHIVE_ENABLED: True})
    archive._entry.runtime_data.core.client = client
    archive._hass.config.path.return_value = str(tmp_path)

    with patch(
        "custom_components.garmin_connect.history.dt_util.utcnow",
        return_value=datetime(2026, 8, 4, tzinfo=UTC),
    ):
        await archive.async_start()
        first_sync_task = archive._first_sync_task
        assert first_sync_task is not None
        await first_sync_task

    assert archive.status.state is HistoryArchiveState.IDLE
    assert history_module.async_archive_fit.await_count == 1
    assert set(stores["garmin_connect.e.history_catalog"].data["activity_index"]["2026"]) == {
        activity.logical_id
        for activity in activities
        if activity.calendar_date == date(2026, 8, 4)
    }


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
