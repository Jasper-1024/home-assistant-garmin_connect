"""Tests for bounded Garmin daily status records."""

from datetime import UTC, date, datetime

import pytest

from custom_components.garmin_connect.daily_status import (
    DailyStatusStore,
    daily_status_from_record,
    daily_status_record,
    normalize_fitness_age_status,
    normalize_hrv_status,
    normalize_sleep_daily_status,
    normalize_stress_daily_status,
    normalize_training_daily_status,
)
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
                "avgOvernightHrv": 42,
            }
        },
        TARGET,
    )
    assert metric_values(sleep)["sleep_score_overall"] == 82
    fitness = normalize_fitness_age_status(
        {
            "chronologicalAge": 35,
            "fitnessAge": 31.5,
            "rhr": {"value": 52, "potentialAge": 30},
            "lastUpdated": "2026-07-31T00:00:00+00:00",
        },
        TARGET,
    )
    assert metric_values(fitness)["fitness_age_fitnessAge"] == 31.5
    stress = normalize_stress_daily_status(
        {
            "averageStressLevel": 28,
            "highStressDuration": 600,
            "stressQualifier": "BALANCED",
        },
        TARGET,
    )
    assert metric_values(stress)["stress_highStressDuration_minutes"] == 10
    assert stress.source_timestamp == datetime(2026, 7, 30, 16, tzinfo=UTC)


def test_invalid_known_numeric_field_is_rejected():
    with pytest.raises(HistorySchemaError):
        normalize_hrv_status({"hrvSummary": {"weeklyAvg": "45"}}, TARGET)


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
