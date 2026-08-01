"""Tests for bounded Garmin daily status records."""

from datetime import UTC, date, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.garmin_connect.daily_status import (
    DailyStatusStore,
    daily_status_from_record,
    daily_status_record,
    normalize_fitness_age_status,
    normalize_hrv_status,
    normalize_sleep_daily_records,
    normalize_sleep_daily_status,
    normalize_stress_daily_status,
    normalize_training_daily_records,
    normalize_training_daily_status,
    unavailable_daily_status,
)
from custom_components.garmin_connect.history import GarminHistoryArchive
from custom_components.garmin_connect.history_recorder import RecorderWriteOutcome
from custom_components.garmin_connect.history_source import HistorySchemaError

TARGET = date(2026, 7, 31)


class MemoryStore:
    records: dict[str, dict] = {}

    def __init__(self, _hass, _version, key, **_kwargs):
        self.key = key

    async def async_load(self):
        return self.records.get(self.key)

    async def async_save(self, value):
        self.records[self.key] = value


@pytest.fixture(autouse=True)
def clear_store():
    MemoryStore.records = {}


def metric_values(record):
    return {metric.key: metric.value for metric in record.metrics}


def test_hrv_status_preserves_zero_missing_and_revision():
    payload = {
        "hrvSummary": {
            "status": "BALANCED",
            "lastNightAvg": 0,
            "weeklyAvg": 45,
            "baseline": {"balancedLow": 35, "balancedUpper": 52},
            "createTimeStamp": "2026-07-31T06:00:00+00:00",
        }
    }
    record = normalize_hrv_status(payload, TARGET)
    assert metric_values(record)["hrv_last_night_average"] == 0
    assert record.field_presence["lastNight5MinHigh"] == "missing"
    assert daily_status_from_record(daily_status_record(record)) == record
    assert normalize_hrv_status(payload, TARGET).revision == record.revision


def test_training_status_archives_generic_vo2_without_device_id():
    record = normalize_training_daily_status(
        {
            "mostRecentTrainingStatus": {
                "latestTrainingStatusData": {
                    "123": {
                        "calendarDate": "2026-07-31",
                        "trainingStatus": 7,
                        "fitnessTrend": 1.5,
                        "acuteTrainingLoadDTO": {
                            "dailyTrainingLoadAcute": 400,
                            "dailyTrainingLoadChronic": 350,
                            "dailyAcuteChronicWorkloadRatio": 1.14,
                        },
                    }
                }
            },
            "mostRecentVO2Max": {"generic": {"vo2MaxValue": 48}},
        },
        TARGET,
    )
    values = metric_values(record)
    assert values["training_vo2_max_generic"] == 48
    assert any(key.startswith("training_acute_load_") for key in values)
    assert record.values["devices"]


def test_sleep_fitness_and_stress_normalization():
    sleep = normalize_sleep_daily_status(
        {
            "dailySleepDTO": {
                "sleepScores": {
                    "overall": {"value": 82, "qualifierKey": "GOOD"},
                    "deepPercentage": {"value": 24},
                },
                "sleepNeed": {"actual": 28800, "baseline": 27000},
                "nextSleepNeed": {"actual": 28000},
                "avgOvernightHrv": 42,
            }
        },
        TARGET,
    )
    assert metric_values(sleep)["sleep_score_overall"] == 82
    assert sleep.field_presence["sleepNeed.actual"] == "present"
    assert sleep.field_presence["nextSleepNeed.actual"] == "present"
    fitness = normalize_fitness_age_status(
        {
            "chronologicalAge": 35,
            "fitnessAge": 31.5,
            "rhr": {"value": 52, "potentialAge": 30},
            "lastUpdated": "2026-07-31T00:00:00+00:00",
        },
        TARGET,
    )
    assert metric_values(fitness)["fitness_age_fitness_age"] == 31.5
    stress = normalize_stress_daily_status(
        {
            "averageStressLevel": 28,
            "highStressDuration": 600,
            "stressQualifier": "BALANCED",
        },
        TARGET,
    )
    assert metric_values(stress)["stress_high_stress_duration_minutes"] == 10
    assert stress.source_timestamp is None
    assert stress.statistic_timestamp == datetime(2026, 7, 30, 16, tzinfo=UTC)


def test_invalid_known_numeric_field_is_rejected():
    with pytest.raises(HistorySchemaError):
        normalize_hrv_status({"hrvSummary": {"weeklyAvg": "45"}}, TARGET)


def test_source_calendar_date_and_absence_semantics_round_trip():
    record = normalize_hrv_status(
        {"hrvSummary": {"calendarDate": "2026-07-30", "weeklyAvg": 45}},
        TARGET,
    )
    assert record.calendar_date == date(2026, 7, 30)
    failed = unavailable_daily_status("hrv", TARGET, "failed")
    assert daily_status_from_record(daily_status_record(failed)).presence == "failed"


def test_training_and_sleep_subsources_keep_independent_identity_and_time():
    training = normalize_training_daily_records(
        {
            "mostRecentTrainingStatus": {
                "latestTrainingStatusData": {
                    "watch": {
                        "calendarDate": "2026-07-31",
                        "trainingStatus": 7,
                    }
                }
            },
            "mostRecentVO2Max": {
                "generic": {
                    "calendarDate": "2026-07-29",
                    "vo2MaxValue": 48,
                }
            },
            "mostRecentTrainingLoadBalance": {
                "calendarDate": "2026-07-28",
                "monthlyLoadAerobicLow": 100,
            },
        },
        TARGET,
    )
    assert {record.calendar_date for record in training} == {
        date(2026, 7, 29),
        date(2026, 7, 31),
        date(2026, 7, 28),
    }
    assert len({record.record_key for record in training}) == 3

    sleep = normalize_sleep_daily_records(
        {
            "dailySleepDTO": {
                "calendarDate": "2026-07-31",
                "sleepScores": {"overall": {"value": 82}},
                "sleepNeed": {
                    "calendarDate": "2026-07-31",
                    "timestampGmt": "2026-07-31T06:00:00+00:00",
                    "actual": 450,
                },
                "nextSleepNeed": {
                    "calendarDate": "2026-08-01",
                    "timestampGmt": "2026-07-31T20:00:00+00:00",
                    "actual": 440,
                },
            }
        },
        TARGET,
    )
    next_need = next(record for record in sleep if record.record_key == "sleep_next_sleep_need")
    assert next_need.calendar_date == date(2026, 8, 1)
    assert next_need.statistic_timestamp == datetime(2026, 7, 31, 20, tzinfo=UTC)
    assert "sleep_average_overnight_hrv" not in {
        metric.key for record in sleep for metric in record.metrics
    }


def test_malformed_subsource_does_not_discard_valid_siblings():
    training = normalize_training_daily_records(
        {
            "mostRecentTrainingStatus": {
                "latestTrainingStatusData": {
                    "valid": {"calendarDate": "2026-07-31", "trainingStatus": 7},
                    "bad": "invalid",
                }
            }
        },
        TARGET,
    )
    assert {record.presence for record in training} == {"present", "failed"}

    sleep = normalize_sleep_daily_records(
        {
            "dailySleepDTO": {
                "calendarDate": "2026-07-31",
                "sleepScores": {"overall": {"value": 82}},
                "nextSleepNeed": "invalid",
            }
        },
        TARGET,
    )
    assert {record.presence for record in sleep} == {"present", "failed"}


def test_nullable_status_fields_remain_distinct_from_missing():
    training = normalize_training_daily_status(
        {
            "mostRecentTrainingStatus": {
                "latestTrainingStatusData": {
                    "watch": {
                        "calendarDate": "2026-07-31",
                        "trainingPaused": None,
                    }
                }
            }
        },
        TARGET,
    )
    assert next(
        value
        for key, value in training.field_presence.items()
        if key.endswith("trainingPaused")
    ) == "null"
    fitness = normalize_fitness_age_status(
        {"physiqueRating": None, "visceralFat": 7}, TARGET
    )
    assert fitness.field_presence["physiqueRating"] == "null"
    assert metric_values(fitness)["fitness_age_visceral_fat"] == 7
    assert fitness.field_presence["components.rhr.stale"] == "missing"
    null_need = normalize_sleep_daily_records(
        {"dailySleepDTO": {"sleepNeed": None}}, TARGET
    )
    assert any(record.presence == "null" for record in null_need)
    null_stale = normalize_fitness_age_status({"rhr": {"stale": None}}, TARGET)
    assert null_stale.field_presence["components.rhr.stale"] == "null"


def test_live_training_and_sleep_status_shapes_normalize_without_failure():
    training = normalize_training_daily_records(
        {
            "mostRecentVO2Max": {
                "cycling": None,
                "generic": {
                    "calendarDate": "2026-08-01",
                    "maxMetCategory": 2,
                    "vo2MaxValue": 48,
                },
                "running": {
                    "calendarDate": "2026-08-01",
                    "vo2MaxValue": 49,
                },
                "userId": 123,
            },
            "mostRecentTrainingLoadBalance": {
                "metricsTrainingLoadBalanceDTOMap": {
                    "3417635870": {
                        "calendarDate": "2026-08-01",
                        "monthlyLoadAerobicLow": 358.3,
                        "monthlyLoadAerobicLowTargetMin": 200,
                        "primaryTrainingDevice": True,
                        "trainingBalanceFeedbackPhrase": "AEROBIC_HIGH_SHORTAGE",
                    },
                    "other": {
                        "calendarDate": "2026-08-01",
                        "monthlyLoadAerobicLow": 200,
                    },
                }
            },
        },
        TARGET,
    )
    assert not any(record.presence == "failed" for record in training)
    assert any(record.record_key.startswith("training_load_balance:") for record in training)
    assert any(
        metric.key.startswith("training_load_balance_monthly_load_aerobic_low_")
        for record in training
        for metric in record.metrics
    )
    load_records = {
        record.record_key
        for record in training
        if record.record_key.startswith("training_load_balance:")
        for metric in record.metrics
        if metric.key
        == "training_load_balance_monthly_load_aerobic_low_"
        + record.record_key.rsplit(":", 1)[1]
    }
    assert len(load_records) == 2
    vo2_keys = {
        metric.key
        for record in training
        if record.record_key.startswith("training_vo2:")
        for metric in record.metrics
        if "vo2_max" in metric.key
    }
    assert "training_vo2_max_generic" in vo2_keys
    assert "training_vo2_max_running" in vo2_keys

    metadata_null = normalize_training_daily_records(
        {"mostRecentVO2Max": {"userId": None}}, TARGET
    )
    assert not any("user_id" in record.record_key for record in metadata_null)
    null_balance = normalize_training_daily_records(
        {
            "mostRecentTrainingLoadBalance": {
                "metricsTrainingLoadBalanceDTOMap": {"watch": None}
            }
        },
        TARGET,
    )
    assert any(record.presence == "null" for record in null_balance)

    sleep = normalize_sleep_daily_records(
        {
            "dailySleepDTO": {
                "calendarDate": "2026-08-01",
                "sleepNeed": {
                    "actual": 450,
                    "baseline": 480,
                    "calendarDate": "2026-08-01",
                    "deviceId": 3417635870,
                    "displayedForTheDay": True,
                    "hrvAdjustment": "NO_CHANGE",
                    "napAdjustment": "DECREASING",
                    "preferredActivityTracker": True,
                    "sleepHistoryAdjustment": "NO_CHANGE",
                    "timestampGmt": "2026-08-01T01:06:15",
                },
            }
        },
        TARGET,
    )
    need = next(record for record in sleep if record.record_key == "sleep_sleep_need")
    assert need.presence == "present"
    assert need.values["sleepNeed"]["hrvAdjustment"] == "NO_CHANGE"
    assert need.values["sleepNeed"]["preferredActivityTracker"] is True


@pytest.mark.asyncio
async def test_store_is_idempotent_revisable_and_empty_cannot_erase():
    store = DailyStatusStore(object(), "entry", "account-key", MemoryStore)
    original = normalize_hrv_status({"hrvSummary": {"weeklyAvg": 45}}, TARGET)
    (saved,) = await store.async_upsert((original,))
    (same,) = await store.async_upsert((original,))
    assert same.revision == saved.revision

    empty = normalize_hrv_status({}, TARGET)
    (retained,) = await store.async_upsert((empty,))
    assert retained.revision == original.revision

    revised = normalize_hrv_status({"hrvSummary": {"weeklyAvg": 46}}, TARGET)
    await store.async_upsert((revised,))
    restored = DailyStatusStore(object(), "entry", "account-key", MemoryStore)
    assert (await restored.async_get_range(TARGET, TARGET))[0].revision == revised.revision


@pytest.mark.asyncio
async def test_projection_checkpoint_is_persisted_and_account_scoped():
    store = DailyStatusStore(object(), "entry", "account-key", MemoryStore)
    record = normalize_hrv_status({"hrvSummary": {"weeklyAvg": 45}}, TARGET)
    await store.async_upsert((record,))
    projected = await store.async_mark_projected(record)
    assert projected.projected_revision == record.revision

    wrong_account = DailyStatusStore(object(), "entry", "other-account", MemoryStore)
    with pytest.raises(HistorySchemaError):
        await wrong_account.async_get_range(TARGET, TARGET)


@pytest.mark.asyncio
async def test_archive_retries_store_projection_without_refetching_garmin():
    entry = MagicMock()
    entry.entry_id = "entry"
    archive = GarminHistoryArchive(MagicMock(), entry, store_factory=MemoryStore)
    archive._account_key_value = "account-key-long-enough-123"
    archive._daily_status_store = DailyStatusStore(
        object(), "entry", archive._account_key_value, MemoryStore
    )
    archive._async_prepare_numeric_source_dates = AsyncMock()
    archive._async_confirm_numeric_source_dates = AsyncMock()

    source = MagicMock()

    async def fetch(_target, family):
        if family == "hrv":
            return {"hrvSummary": {"weeklyAvg": 45}}
        return {}

    source.async_fetch_daily_status_payload = AsyncMock(side_effect=fetch)
    failed_recorder = MagicMock()
    failed_recorder.async_write = AsyncMock(
        return_value=RecorderWriteOutcome(outcome="failed")
    )

    await archive._async_sync_daily_status(
        source, failed_recorder, TARGET, include_training=True
    )
    persisted = await archive._daily_status_store.async_get_range(TARGET, TARGET)
    hrv = next(item for item in persisted if item.family == "hrv")
    assert hrv.projected_revision is None

    successful_recorder = MagicMock()
    successful_recorder.async_write = AsyncMock(
        return_value=RecorderWriteOutcome(
            accepted_count=1, inserted_count=1, outcome="written"
        )
    )
    result = await archive._async_project_daily_status(successful_recorder, TARGET)

    assert result == (1, 0, 0)
    assert source.async_fetch_daily_status_payload.await_count == 5
    projected = await archive._daily_status_store.async_get_range(TARGET, TARGET)
    assert all(item.projected_revision == item.revision for item in projected)
