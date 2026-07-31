"""Private, high-resolution Garmin statistics writer for Home Assistant Recorder."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import threading
from collections import OrderedDict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from enum import StrEnum
from math import isfinite
from typing import Any, Protocol

from .history_source import NormalizedSample

_RECORDER_CHUNK_SIZE = 1024
_RECENT_VALUE_CACHE_SIZE = 4096
# A task may wait behind Recorder's normal persistence queue. This is an idle
# timeout: every Recorder execution of the task renews it.
_RECORDER_BARRIER_TIMEOUT = 60
# Progress may be continuous while Recorder retries a database operation. Keep
# that healthy case alive, but never make a writer wait without a hard bound.
_RECORDER_BARRIER_MAX_TIMEOUT = 300


class _UnavailableRecorderTask:
    """Safe placeholder until the optional Recorder task API is needed."""

    commit_before = True


# history.py is imported as part of the current-value integration.  Do not
# import RecorderTask here: a missing optional Recorder dependency must only
# disable archival at its compatibility/write boundary.
_RecorderTaskBase: type[Any] = _UnavailableRecorderTask


class _RecorderBarrierTimeoutError(RuntimeError):
    """Raised when Recorder does not confirm an enqueued statistics write."""


class _RecorderUnavailableError(RuntimeError):
    """Private, non-diagnostic failure returned from a Recorder boundary."""


def _is_sqlalchemy_error(err: Exception) -> bool:
    """Return whether Recorder must receive the error for session recovery."""
    try:
        from sqlalchemy.exc import SQLAlchemyError
    except ImportError:
        return False
    return isinstance(err, SQLAlchemyError)


def _durable_import_statistics_job() -> Any:
    """Return Recorder's unwrapped statistics job with its commit boundary."""
    from homeassistant.components.recorder.statistics import import_statistics

    job = inspect.unwrap(import_statistics)
    if not callable(job):
        raise TypeError("Recorder statistics import job is unavailable")
    return job


def _require_durable_import_statistics_contract() -> None:
    """Raise unless Recorder exposes the transaction-owning import job."""
    from homeassistant.components.recorder.statistics import import_statistics
    from homeassistant.components.recorder.util import _is_retryable_error

    job = _durable_import_statistics_job()
    job_code = getattr(job, "__code__", None)
    retryable_error_code = getattr(_is_retryable_error, "__code__", None)
    if (
        getattr(import_statistics, "__wrapped__", None) is None
        or job is import_statistics
        or job_code is None
        or job_code.co_argcount != 4
        or job_code.co_kwonlyargcount != 0
        or job_code.co_flags & (inspect.CO_VARARGS | inspect.CO_VARKEYWORDS)
        or job_code.co_varnames[: job_code.co_argcount]
        != ("instance", "metadata", "statistics", "table")
        or "session_scope" not in job_code.co_names
        or retryable_error_code is None
        or retryable_error_code.co_argcount != 2
        or retryable_error_code.co_kwonlyargcount != 0
        or retryable_error_code.co_flags & (inspect.CO_VARARGS | inspect.CO_VARKEYWORDS)
        or retryable_error_code.co_varnames[: retryable_error_code.co_argcount]
        != ("instance", "err")
    ):
        raise TypeError("Recorder durable statistics import contract changed")


def _is_retryable_database_error(instance: Any, err: Exception) -> bool:
    """Classify a Recorder database error without exposing its details."""
    try:
        from homeassistant.components.recorder.util import _is_retryable_error
        from sqlalchemy.exc import OperationalError
    except ImportError:
        return False
    if not isinstance(err, OperationalError):
        return False
    try:
        return _is_retryable_error(instance, err)
    except (AttributeError, TypeError):
        return False


class _RecorderTaskProgress:
    """Thread-safe progress signal for one Recorder queue task."""

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        """Initialize the progress state on the Home Assistant event loop."""
        self._loop = loop
        self.event = asyncio.Event()
        self.last_progress = loop.time()

    def report(self) -> None:
        """Report that Recorder has started another attempt of this task."""
        self._loop.call_soon_threadsafe(self._report)

    def _report(self) -> None:
        """Record progress on the event loop that owns the waiter."""
        self.last_progress = self._loop.time()
        self.event.set()


async def _async_wait_for_recorder_confirmation(
    future: asyncio.Future[None], progress: _RecorderTaskProgress
) -> None:
    """Wait for confirmation while Recorder continues to make bounded progress."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _RECORDER_BARRIER_MAX_TIMEOUT
    while not future.done():
        remaining = deadline - loop.time()
        idle_remaining = progress.last_progress + _RECORDER_BARRIER_TIMEOUT - loop.time()
        if remaining <= 0 or idle_remaining <= 0:
            raise _RecorderBarrierTimeoutError
        progress_wait = loop.create_task(progress.event.wait())
        try:
            done, _ = await asyncio.wait(
                (future, progress_wait),
                timeout=min(remaining, idle_remaining),
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            progress_wait.cancel()
        if future in done:
            future.result()
            return
        if progress_wait in done:
            progress.event.clear()
            continue
        raise _RecorderBarrierTimeoutError


_ConfirmingRecorderTask: type[Any] = _UnavailableRecorderTask
_ConfirmingImportStatisticsTask: type[Any] = _UnavailableRecorderTask


def _load_recorder_task() -> type[Any]:
    """Load RecorderTask only at the archive compatibility/write boundary."""
    try:
        from homeassistant.components.recorder.tasks import RecorderTask
    except Exception as err:
        raise TypeError("RecorderTask is unavailable") from err
    return RecorderTask


def _recorder_task_classes() -> tuple[type[Any], type[Any], type[Any]]:
    """Build queue tasks against the RecorderTask type available at use time."""
    global _RecorderTaskBase, _ConfirmingRecorderTask, _ConfirmingImportStatisticsTask

    recorder_task = _load_recorder_task()
    if _RecorderTaskBase is recorder_task:
        return recorder_task, _ConfirmingRecorderTask, _ConfirmingImportStatisticsTask

    class ConfirmingRecorderTask(recorder_task):  # type: ignore[misc, valid-type]
        """Recorder queue task that confirms its own execution."""

        __slots__ = ("future", "progress")

        def __init__(
            self, future: asyncio.Future[None], progress: _RecorderTaskProgress
        ) -> None:
            self.future = future
            self.progress = progress

        def run(self, instance: Any) -> None:
            """Confirm execution on the future-owning event loop."""
            del instance
            self.progress.report()
            self.future.get_loop().call_soon_threadsafe(self._set_result_if_not_done)

        def _set_result_if_not_done(self) -> None:
            """Set the confirmation future exactly once on its owning event loop."""
            if not self.future.done():
                self.future.set_result(None)

    class ConfirmingImportStatisticsTask(recorder_task):  # type: ignore[misc, valid-type]
        """Recorder task that settles only after its statistics import succeeds."""

        __slots__ = (
            "metadata",
            "statistics",
            "table",
            "future",
            "progress",
            "abandoned",
            "terminal",
        )

        def __init__(
            self,
            metadata: Any,
            statistics: Sequence[Any],
            table: type,
            future: asyncio.Future[None],
            progress: _RecorderTaskProgress,
            abandoned: threading.Event,
        ) -> None:
            self.metadata = metadata
            self.statistics = statistics
            self.table = table
            self.future = future
            self.progress = progress
            self.abandoned = abandoned
            self.terminal = threading.Event()

        def abandon(self) -> None:
            """Prevent queued retries after the caller no longer awaits this import."""
            self.abandoned.set()
            self.terminal.set()

        def _fail(self) -> None:
            """Stop retries and safely report an internal terminal failure."""
            self.terminal.set()
            self.future.get_loop().call_soon_threadsafe(self._set_exception_if_active)

        def run(self, instance: Any) -> None:
            """Retry with Recorder ordering until the import itself completes."""
            if self.terminal.is_set():
                return
            self.progress.report()
            recorder_error = None
            try:
                # The public function is retryable_database_job-wrapped.  That
                # wrapper returns True after a permanent OperationalError, which
                # is not a durable import.  The unwrapped job exits session_scope
                # only after its transaction has committed.
                imported = _durable_import_statistics_job()(
                    instance, self.metadata, self.statistics, self.table
                )
            except asyncio.CancelledError:
                raise
            except Exception as err:
                if not _is_sqlalchemy_error(err):
                    self._fail()
                    return
                from sqlalchemy.exc import SQLAlchemyError

                if _is_retryable_database_error(instance, err) and not self.terminal.is_set():
                    try:
                        instance.queue_task(self)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        self._fail()
                else:
                    self._fail()
                recorder_error = SQLAlchemyError("Recorder statistics import failed")
            if recorder_error is not None:
                # Raise after the handler ends so Recorder receives no original exception
                # cause or context, which could expose database details in its diagnostics.
                raise recorder_error
            if not imported:
                if self.terminal.is_set():
                    return
                try:
                    instance.queue_task(self)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._fail()
                return
            if self.terminal.is_set():
                return
            self.future.get_loop().call_soon_threadsafe(self._set_result_if_not_done)

        def _set_result_if_not_done(self) -> None:
            """Set the confirmation future exactly once on its owning event loop."""
            if not self.abandoned.is_set() and not self.future.done():
                self.future.set_result(None)

        def _set_exception_if_active(self) -> None:
            """Return a safe task-boundary failure without Recorder exception details."""
            if not self.abandoned.is_set() and not self.future.done():
                self.future.set_exception(_RecorderUnavailableError())

    _RecorderTaskBase = recorder_task
    _ConfirmingRecorderTask = ConfirmingRecorderTask
    _ConfirmingImportStatisticsTask = ConfirmingImportStatisticsTask
    return recorder_task, ConfirmingRecorderTask, ConfirmingImportStatisticsTask


async def async_confirm_recorder_queue(recorder: _Recorder) -> None:
    """Confirm that Recorder executes a queued task with bounded waiting."""
    _, confirming_task, _ = _recorder_task_classes()
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    progress = _RecorderTaskProgress(loop)
    recorder.queue_task(confirming_task(future, progress))
    await _async_wait_for_recorder_confirmation(future, progress)


def _require_recorder_task_contract() -> None:
    """Raise unless queued tasks use Recorder's current concrete base contract."""
    recorder_task, confirming_task, import_task = _recorder_task_classes()
    run_code = getattr(recorder_task.run, "__code__", None)
    if (
        recorder_task.commit_before is not True
        or run_code is None
        or run_code.co_varnames[: run_code.co_argcount] != ("self", "instance")
        or not issubclass(confirming_task, recorder_task)
        or not issubclass(import_task, recorder_task)
    ):
        raise TypeError("RecorderTask contract changed")


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
FLOORS_TOTAL_DAILY_METADATA = HistoryMetricMetadata("floors_daily_total", "Floors daily total", "floors")
MODERATE_INTENSITY_METADATA = HistoryMetricMetadata("intensity_moderate", "Moderate intensity minutes", "min")
VIGOROUS_INTENSITY_METADATA = HistoryMetricMetadata("intensity_vigorous", "Vigorous intensity minutes", "min")
MODERATE_INTENSITY_DAILY_METADATA = HistoryMetricMetadata("intensity_moderate_daily_total", "Moderate intensity minutes daily total", "min")
VIGOROUS_INTENSITY_DAILY_METADATA = HistoryMetricMetadata("intensity_vigorous_daily_total", "Vigorous intensity minutes daily total", "min")
INTENSITY_TOTAL_DAILY_METADATA = HistoryMetricMetadata("intensity_daily_total", "Intensity minutes daily total", "min")
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
    def async_import_statistics(self, metadata: Any, stats: Sequence[Any], _table: type) -> None: ...
    def queue_task(self, task: Any) -> None: ...


def statistic_id_for(account_key: str, metric_key: str) -> str:
    """Build a stable, non-identifying statistic ID from an opaque account key."""
    digest = hashlib.sha256(account_key.encode("utf-8")).hexdigest()[:32]
    return f"garmin_connect:{digest}:{metric_key}"


class GarminHistoryRecorder:
    """Write normalized samples directly through Recorder's queued statistics path."""

    def __init__(self, recorder: _Recorder) -> None:
        self._recorder = recorder
        self._recent_values: OrderedDict[tuple[str, Any], float] = OrderedDict()

    async def async_write(
        self,
        statistic_id: str,
        metric: HistoryMetricMetadata,
        samples: Sequence[NormalizedSample],
    ) -> RecorderWriteOutcome:
        """Import samples and await completion, preserving timestamps and revisions."""
        try:
            from homeassistant.components.recorder.db_schema import Statistics
            from homeassistant.components.recorder.models import StatisticMeanType
        except (ImportError, AttributeError, TypeError):
            return RecorderWriteOutcome(0, "incompatible", "recorder_symbols")
        try:
            _, _, import_task = _recorder_task_classes()
        except TypeError:
            return RecorderWriteOutcome(0, "incompatible", "recorder_task")
        if not statistic_id or not metric.key:
            return RecorderWriteOutcome(0, "invalid", "metric")
        if not samples:
            return RecorderWriteOutcome(0)
        for sample in samples:
            if sample.timestamp.tzinfo is None or sample.timestamp.utcoffset() is None:
                return RecorderWriteOutcome(0, "invalid", "timestamp")
            if isinstance(sample.value, bool) or not isinstance(sample.value, int | float) or not isfinite(float(sample.value)):
                return RecorderWriteOutcome(0, "invalid", "value")
        try:
            signature = tuple(inspect.signature(self._recorder.async_import_statistics).parameters)
        except (AttributeError, TypeError, ValueError):
            return RecorderWriteOutcome(0, "incompatible", "recorder_signature")
        if signature != ("metadata", "stats", "table"):
            return RecorderWriteOutcome(0, "incompatible", "recorder_signature")
        metadata_record = {
            "mean_type": StatisticMeanType.ARITHMETIC,
            "has_sum": False,
            "name": f"Garmin {metric.name}",
            "source": "garmin_connect",
            "statistic_id": statistic_id,
            "unit_class": metric.unit_class,
            "unit_of_measurement": metric.unit_of_measurement,
        }
        inserted = updated = skipped = 0

        async def write_chunk(chunk: list[NormalizedSample]) -> tuple[int, int, int]:
            """Import one bounded batch while retaining all source samples."""
            stats = [
                {
                    "start": sample.timestamp.astimezone(UTC),
                    "mean": float(sample.value),
                    "min": float(sample.value),
                    "max": float(sample.value),
                }
                for sample in chunk
            ]
            loop = asyncio.get_running_loop()
            future = loop.create_future()
            progress = _RecorderTaskProgress(loop)
            task = import_task(
                metadata_record, stats, Statistics, future, progress, threading.Event()
            )
            try:
                self._recorder.queue_task(task)
            except asyncio.CancelledError:
                task.abandon()
                raise
            except Exception:
                task.abandon()
                raise _RecorderUnavailableError from None
            try:
                await _async_wait_for_recorder_confirmation(future, progress)
            except (asyncio.CancelledError, _RecorderBarrierTimeoutError):
                task.abandon()
                raise
            chunk_inserted = chunk_updated = chunk_skipped = 0
            for sample in chunk:
                normalized = sample.timestamp.astimezone(UTC)
                identity = (statistic_id, normalized)
                previous = self._recent_values.get(identity)
                if previous is None:
                    chunk_inserted += 1
                elif previous == float(sample.value):
                    chunk_skipped += 1
                else:
                    chunk_updated += 1
                self._recent_values[identity] = float(sample.value)
                self._recent_values.move_to_end(identity)
            while len(self._recent_values) > _RECENT_VALUE_CACHE_SIZE:
                self._recent_values.popitem(last=False)
            return chunk_inserted, chunk_updated, chunk_skipped

        try:
            chunk: list[NormalizedSample] = []
            for sample in samples:
                chunk.append(
                    NormalizedSample(
                        sample.timestamp.astimezone(UTC),
                        sample.request_date,
                        sample.raw_timestamp,
                        float(sample.value),
                    )
                )
                if len(chunk) == _RECORDER_CHUNK_SIZE:
                    counts = await write_chunk(chunk)
                    inserted += counts[0]
                    updated += counts[1]
                    skipped += counts[2]
                    chunk = []
            if chunk:
                counts = await write_chunk(chunk)
                inserted += counts[0]
                updated += counts[1]
                skipped += counts[2]
        except asyncio.CancelledError:
            raise
        except (AttributeError, ImportError, TypeError, ValueError, RuntimeError) as err:
            error_type = (
                "recorder_barrier"
                if isinstance(err, _RecorderBarrierTimeoutError)
                else "recorder_unavailable"
            )
            return RecorderWriteOutcome(0, "failed", error_type)
        return RecorderWriteOutcome(
            len(samples), inserted_count=inserted, updated_count=updated, skipped_count=skipped
        )
