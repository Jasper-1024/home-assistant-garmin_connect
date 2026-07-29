"""Private, high-resolution Garmin statistics writer for Home Assistant Recorder."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from enum import StrEnum
from math import isfinite
from typing import Any, Protocol

from .history_source import NormalizedSample


class HistoryMetric(StrEnum):
    """Intraday metrics supported by this writer slice."""

    HEART_RATE = "heart_rate"
    STRESS = "stress"


@dataclass(frozen=True, slots=True)
class HistoryMetricMetadata:
    """Recorder metadata supplied by the metric owner."""

    key: str
    name: str
    unit_of_measurement: str
    unit_class: str | None = None


HEART_RATE_METADATA = HistoryMetricMetadata("heart_rate", "Heart rate", "bpm")
STRESS_METADATA = HistoryMetricMetadata("stress", "Stress", "unitless")
BODY_BATTERY_METADATA = HistoryMetricMetadata("body_battery", "Body Battery", "unitless")
NIGHTLY_HRV_METADATA = HistoryMetricMetadata("nightly_hrv", "Nightly HRV", "ms")
STEPS_METADATA = HistoryMetricMetadata("steps", "Steps", "steps")
STEPS_DAILY_TOTAL_METADATA = HistoryMetricMetadata("steps_daily_total", "Steps daily total", "steps")
FLOORS_METADATA = HistoryMetricMetadata("floors", "Floors", "floors")
FLOORS_ASCENDED_DAILY_METADATA = HistoryMetricMetadata("floors_ascended_daily_total", "Floors ascended daily total", "floors")
FLOORS_DESCENDED_DAILY_METADATA = HistoryMetricMetadata("floors_descended_daily_total", "Floors descended daily total", "floors")
FLOORS_ASCENDED_METERS_DAILY_METADATA = HistoryMetricMetadata("floors_ascended_meters_daily_total", "Floors ascended meters daily total", "m")
FLOORS_DESCENDED_METERS_DAILY_METADATA = HistoryMetricMetadata("floors_descended_meters_daily_total", "Floors descended meters daily total", "m")
MODERATE_INTENSITY_METADATA = HistoryMetricMetadata("intensity_moderate", "Moderate intensity minutes", "min")
VIGOROUS_INTENSITY_METADATA = HistoryMetricMetadata("intensity_vigorous", "Vigorous intensity minutes", "min")
MODERATE_INTENSITY_DAILY_METADATA = HistoryMetricMetadata("intensity_moderate_daily_total", "Moderate intensity minutes daily total", "min")
VIGOROUS_INTENSITY_DAILY_METADATA = HistoryMetricMetadata("intensity_vigorous_daily_total", "Vigorous intensity minutes daily total", "min")
RESPIRATION_RAW_METADATA = HistoryMetricMetadata("respiration_raw", "Respiration", "breaths/min")
RESPIRATION_AVERAGE_METADATA = HistoryMetricMetadata("respiration_average", "Respiration average", "breaths/min")
SPO2_SINGLE_METADATA = HistoryMetricMetadata("spo2_single", "SpO2 single", "%")
SPO2_CONTINUOUS_METADATA = HistoryMetricMetadata("spo2_continuous", "SpO2 continuous", "%")
SPO2_HOURLY_METADATA = HistoryMetricMetadata("spo2_hourly", "SpO2 hourly average", "%")
SLEEP_HEART_RATE_METADATA = HistoryMetricMetadata("sleep_heart_rate", "Sleep heart rate", "bpm")
SLEEP_HRV_METADATA = HistoryMetricMetadata("sleep_hrv", "Sleep HRV", "ms")
SLEEP_BODY_BATTERY_METADATA = HistoryMetricMetadata("sleep_body_battery", "Sleep Body Battery", "unitless")
SLEEP_STRESS_METADATA = HistoryMetricMetadata("sleep_stress", "Sleep stress", "unitless")
SLEEP_RESPIRATION_METADATA = HistoryMetricMetadata("sleep_respiration", "Sleep respiration", "breaths/min")
SLEEP_SPO2_METADATA = HistoryMetricMetadata("sleep_spo2", "Sleep SpO2", "%")
SLEEP_MOVEMENT_METADATA = HistoryMetricMetadata("sleep_movement", "Sleep movement", "unitless")
DAILY_ABNORMAL_HR_METADATA = HistoryMetricMetadata("daily_abnormal_heart_rate_alerts", "Abnormal heart-rate alerts", "alerts")
TRAINING_ACUTE_LOAD_METADATA = HistoryMetricMetadata("training_acute_load", "Training acute load", "load")
TRAINING_CHRONIC_LOAD_METADATA = HistoryMetricMetadata("training_chronic_load", "Training chronic load", "load")
TRAINING_LOAD_BALANCE_METADATA = HistoryMetricMetadata("training_load_balance", "Training load balance", "load")
TRAINING_ACWR_METADATA = HistoryMetricMetadata("training_acwr", "Training ACWR", "ratio")
TRAINING_VO2_MAX_METADATA = HistoryMetricMetadata("training_vo2_max", "Training VO2 Max", "mL/kg/min")
TRAINING_FITNESS_TREND_METADATA = HistoryMetricMetadata("training_fitness_trend", "Fitness trend", "unitless")
TRAINING_RECOVERY_TIME_METADATA = HistoryMetricMetadata("training_recovery_time", "Recovery time", "s")


@dataclass(frozen=True, slots=True)
class RecorderWriteOutcome:
    """Import classification; HA Recorder still performs the actual upsert."""

    accepted_count: int = 0
    outcome: str = "written"
    error_type: str | None = None
    inserted_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0


class _Recorder(Protocol):
    def async_import_statistics(self, metadata: Any, stats: Sequence[Any], table: type) -> None: ...
    def queue_task(self, task: Any) -> None: ...


def statistic_id_for(account_key: str, metric_key: str) -> str:
    """Build a stable, non-identifying statistic ID from an opaque account key."""
    digest = hashlib.sha256(account_key.encode("utf-8")).hexdigest()[:32]
    return f"garmin_connect:{digest}:{metric_key}"


class GarminHistoryRecorder:
    """Write normalized samples directly through Recorder's queued statistics path."""

    def __init__(self, recorder: _Recorder) -> None:
        self._recorder = recorder
        self._known_values: dict[tuple[str, Any], float] = {}

    async def async_write(
        self,
        statistic_id: str,
        metric: HistoryMetricMetadata,
        samples: Sequence[NormalizedSample],
    ) -> RecorderWriteOutcome:
        """Import samples, preserving timestamps and revisions, then await the queue barrier."""
        try:
            from homeassistant.components.recorder.db_schema import Statistics
            from homeassistant.components.recorder.models import StatisticMeanType
            from homeassistant.components.recorder.tasks import SynchronizeTask
        except ImportError, AttributeError, TypeError:
            return RecorderWriteOutcome(0, "incompatible", "recorder_symbols")
        if not statistic_id or not metric.key:
            return RecorderWriteOutcome(0, "invalid", "metric")
        normalized_samples: list[NormalizedSample] = []
        for sample in samples:
            if sample.timestamp.tzinfo is None or sample.timestamp.utcoffset() is None:
                return RecorderWriteOutcome(0, "invalid", "timestamp")
            if isinstance(sample.value, bool) or not isinstance(sample.value, int | float) or not isfinite(float(sample.value)):
                return RecorderWriteOutcome(0, "invalid", "value")
            normalized_samples.append(
                NormalizedSample(
                    sample.timestamp.astimezone(UTC),
                    sample.request_date,
                    sample.raw_timestamp,
                    float(sample.value),
                )
            )
        if tuple(inspect.signature(self._recorder.async_import_statistics).parameters) != (
            "metadata",
            "stats",
            "table",
        ):
            return RecorderWriteOutcome(0, "incompatible", "recorder_signature")
        try:
            metadata = {
                "mean_type": StatisticMeanType.ARITHMETIC,
                "has_sum": False,
                "name": f"Garmin {metric.name}",
                "source": "garmin_connect",
                "statistic_id": statistic_id,
                "unit_class": metric.unit_class,
                "unit_of_measurement": metric.unit_of_measurement,
            }
            stats = [
                {
                    "start": sample.timestamp,
                    "mean": sample.value,
                    "min": sample.value,
                    "max": sample.value,
                }
                for sample in normalized_samples
            ]
            self._recorder.async_import_statistics(metadata, stats, Statistics)
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            self._recorder.queue_task(SynchronizeTask(future))
            await future
        except asyncio.CancelledError:
            raise
        except AttributeError, ImportError, TypeError, ValueError, RuntimeError:
            return RecorderWriteOutcome(0, "failed", "recorder_unavailable")
        inserted = updated = skipped = 0
        for sample in normalized_samples:
            identity = (statistic_id, sample.timestamp)
            previous = self._known_values.get(identity)
            if previous is None:
                inserted += 1
            elif previous == sample.value:
                skipped += 1
            else:
                updated += 1
            self._known_values[identity] = sample.value
        return RecorderWriteOutcome(len(samples), inserted_count=inserted, updated_count=updated, skipped_count=skipped)
