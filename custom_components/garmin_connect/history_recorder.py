"""Private, high-resolution Garmin statistics writer for Home Assistant Recorder."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
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


@dataclass(frozen=True, slots=True)
class RecorderWriteOutcome:
    """Privacy-safe result of one import request."""

    accepted_count: int = 0
    outcome: str = "written"
    error_type: str | None = None


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
                for sample in samples
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
        return RecorderWriteOutcome(len(samples))
