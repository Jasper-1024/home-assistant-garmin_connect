"""Tests for the Garmin Recorder statistics writer."""

import functools
import gc
import threading
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from homeassistant.components.recorder.tasks import RecorderTask
from sqlalchemy.exc import DatabaseError, OperationalError, SQLAlchemyError

from custom_components.garmin_connect import history_recorder as history_recorder_module
from custom_components.garmin_connect.history_recorder import (
    HEART_RATE_METADATA,
    RESPIRATION_RAW_METADATA,
    SPO2_SINGLE_METADATA,
    STRESS_METADATA,
    GarminHistoryRecorder,
    async_confirm_recorder_queue,
    statistic_id_for,
)
from custom_components.garmin_connect.history_source import NormalizedSample


class FakeRequester:
    def __init__(self) -> None:
        self.imports: list[tuple[object, list[object], object]] = []
        self.tasks: list[object] = []
        self.rows: dict[tuple[str, object], object] = {}
        self.instance = SimpleNamespace(
            hass=SimpleNamespace(loop=None), requester=self, queue_task=self.queue_task
        )

    def async_import_statistics(self, metadata, stats, table) -> None:
        del metadata, stats, table

    def queue_task(self, task) -> None:
        self.tasks.append(task)
        self.instance.hass.loop = __import__("asyncio").get_running_loop()
        task.run(self.instance)


@pytest.fixture(autouse=True)
def _stub_import_statistics(monkeypatch: pytest.MonkeyPatch) -> None:
    """Provide only the Recorder state required by the real task implementation."""

    def import_statistics(instance, metadata, statistics, table) -> bool:
        requester = instance.requester
        rows = list(statistics)
        requester.imports.append((metadata, rows, table))
        statistic_id = metadata["statistic_id"]
        for row in rows:
            requester.rows[(statistic_id, row["start"])] = row
        return True

    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        import_statistics,
    )


class StalledRequester(FakeRequester):
    """Scratch Recorder whose queued barrier never settles."""

    def queue_task(self, task) -> None:
        self.tasks.append(task)


def test_recorder_barrier_defaults_allow_startup_recovery() -> None:
    """Recorder recovery after an unclean restart can take several minutes."""
    assert history_recorder_module._RECORDER_BARRIER_TIMEOUT == 300
    assert history_recorder_module._RECORDER_BARRIER_MAX_TIMEOUT == 900


class RetryingImportRequester:
    """Recorder queue that reorders a retrying statistics import behind a barrier."""

    def __init__(self, loop) -> None:
        self.instance = SimpleNamespace(hass=SimpleNamespace(loop=loop))
        self.instance.queue_task = self.queue_task
        self.tasks: list[object] = []

    def async_import_statistics(self, metadata, stats, table) -> None:
        del metadata, stats, table

    def queue_task(self, task) -> None:
        self.tasks.append(task)
        self.instance.hass.loop.call_soon(task.run, self.instance)


class DelayedRetryingImportRequester(RetryingImportRequester):
    """Recorder queue whose retries take longer than one idle interval in total."""

    def __init__(self, loop, delay: float) -> None:
        super().__init__(loop)
        self.delay = delay
        self.handles = []

    def queue_task(self, task) -> None:
        self.tasks.append(task)
        self.handles.append(
            self.instance.hass.loop.call_later(self.delay, task.run, self.instance)
        )

    def cancel_pending(self) -> None:
        """Stop simulated Recorder retries after a test completes."""
        for handle in self.handles:
            handle.cancel()


def _permanent_error_wrapper(job):
    """Model HA's public wrapper, which falsely returns success on a DB error."""

    @functools.wraps(job)
    def wrapper(*args, **kwargs) -> bool:
        del args, kwargs
        return True

    return wrapper


@pytest.mark.asyncio
async def test_startup_confirmation_cancellation_is_safe() -> None:
    """Cancelling startup confirmation leaves its one queued task harmless."""
    loop = __import__("asyncio").get_running_loop()

    class DelayedRecorder:
        def __init__(self) -> None:
            self.tasks: list[object] = []
            self.instance = SimpleNamespace(hass=SimpleNamespace(loop=loop))

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            loop.call_later(0.002, task.run, self.instance)

    recorder = DelayedRecorder()
    confirmation = __import__("asyncio").create_task(async_confirm_recorder_queue(recorder))
    await __import__("asyncio").sleep(0)
    confirmation.cancel()

    with pytest.raises(__import__("asyncio").CancelledError):
        await confirmation

    await __import__("asyncio").sleep(0.01)
    assert recorder.tasks[0].future.done()
    assert not recorder.tasks[0].future.cancelled()


@pytest.mark.asyncio
async def test_writer_confirms_a_retrying_import_before_reporting_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ImportStatisticsTask retry cannot settle behind an earlier barrier."""
    loop = __import__("asyncio").get_running_loop()
    recorder = RetryingImportRequester(loop)
    writer = GarminHistoryRecorder(recorder)
    attempts = 0

    def import_statistics(_instance, _metadata, _statistics, _table) -> bool:
        nonlocal attempts
        attempts += 1
        return attempts == 2

    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        import_statistics,
    )

    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert result.outcome == "written"
    assert attempts == 2
    assert len(recorder.tasks) == 2
    assert all(task is recorder.tasks[0] for task in recorder.tasks)
    assert recorder.tasks[0].future.done()


@pytest.mark.asyncio
async def test_writer_confirms_only_after_unwrapped_statistics_job_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retry wrapper itself is not a durable success boundary."""
    recorder = FakeRequester()
    calls: list[str] = []

    def durable_job(instance, metadata, statistics, table) -> bool:
        del instance, metadata, statistics, table
        calls.append("committed")
        return True

    wrapper = _permanent_error_wrapper(durable_job)
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics", wrapper
    )

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert result.outcome == "written"
    assert calls == ["committed"]


@pytest.mark.asyncio
async def test_permanent_operational_error_is_not_written_or_exposed(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The wrapped permanent-error success cannot confirm a statistics write."""
    secret = "INSERT private-garmin-value"
    loop = __import__("asyncio").get_running_loop()

    class RecoveringRecorder(RetryingImportRequester):
        def __init__(self) -> None:
            super().__init__(loop)
            self.instance.engine = SimpleNamespace(
                dialect=SimpleNamespace(name="sqlite")
            )
            self.recovered_errors: list[SQLAlchemyError] = []

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            loop.call_soon(self._run_task, task)

        def _run_task(self, task) -> None:
            try:
                task.run(self.instance)
            except SQLAlchemyError as err:
                self.recovered_errors.append(err)

    recorder = RecoveringRecorder()

    def durable_job(_instance, _metadata, _statistics, _table) -> bool:
        raise OperationalError(secret, {}, OSError(secret))

    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        _permanent_error_wrapper(durable_job),
    )

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert (result.outcome, result.error_type) == ("failed", "recorder_unavailable")
    assert len(recorder.recovered_errors) == 1
    assert str(recorder.recovered_errors[0]) == "Recorder statistics import failed"
    assert recorder.tasks[0].future.exception().__class__ is history_recorder_module._RecorderUnavailableError
    assert secret not in str(result)
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_retryable_operational_error_requeues_then_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A retryable DB failure is requeued after core session recovery."""
    from homeassistant.components.recorder.const import SupportedDialect

    secret = "retry-private-garmin-value"
    loop = __import__("asyncio").get_running_loop()

    class RetryableMySQLError(Exception):
        def __init__(self) -> None:
            super().__init__(1213, secret)

    class RecoveringRecorder(RetryingImportRequester):
        def __init__(self) -> None:
            super().__init__(loop)
            self.instance.engine = SimpleNamespace(
                dialect=SimpleNamespace(name=SupportedDialect.MYSQL)
            )
            self.recovered_errors: list[SQLAlchemyError] = []

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            loop.call_soon(self._run_task, task)

        def _run_task(self, task) -> None:
            try:
                task.run(self.instance)
            except SQLAlchemyError as err:
                self.recovered_errors.append(err)

    recorder = RecoveringRecorder()
    attempts = 0

    def durable_job(_instance, _metadata, _statistics, _table) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OperationalError(secret, {}, RetryableMySQLError())
        return True

    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        _permanent_error_wrapper(durable_job),
    )

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert result.outcome == "written"
    assert attempts == 2
    assert len(recorder.recovered_errors) == 1
    assert len(recorder.tasks) == 2
    assert recorder.tasks[0] is recorder.tasks[1]


@pytest.mark.asyncio
async def test_writer_extends_barrier_for_each_recorder_import_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retry progress renews the idle bound without removing the total bound."""
    loop = __import__("asyncio").get_running_loop()
    retry_delay = 0.05
    idle_timeout = 0.2
    max_timeout = 1.5
    attempts_to_success = 7
    recorder = DelayedRetryingImportRequester(loop, delay=retry_delay)
    writer = GarminHistoryRecorder(recorder)
    attempts = 0
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_TIMEOUT", idle_timeout)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_MAX_TIMEOUT", max_timeout)

    def import_statistics(_instance, _metadata, _statistics, _table) -> bool:
        nonlocal attempts
        attempts += 1
        return attempts == attempts_to_success

    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        import_statistics,
    )

    started_at = loop.time()
    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert result.outcome == "written"
    assert attempts == attempts_to_success
    assert len(recorder.tasks) == attempts_to_success
    assert idle_timeout < loop.time() - started_at < max_timeout


@pytest.mark.asyncio
async def test_writer_bounds_continuous_recorder_import_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Continuous retry progress cannot extend the writer beyond its total bound."""
    loop = __import__("asyncio").get_running_loop()
    recorder = DelayedRetryingImportRequester(loop, delay=0.001)
    writer = GarminHistoryRecorder(recorder)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_TIMEOUT", 0.01)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_MAX_TIMEOUT", 0.02)
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        lambda *_args: False,
    )

    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert result.outcome == "failed"
    assert result.error_type == "recorder_barrier"
    assert len(recorder.tasks) > 1
    assert recorder.tasks[0].abandoned.is_set()
    task_count_at_timeout = len(recorder.tasks)
    await __import__("asyncio").sleep(0.01)
    assert len(recorder.tasks) == task_count_at_timeout
    recorder.cancel_pending()


@pytest.mark.asyncio
async def test_writer_cancellation_stops_recorder_import_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancelling the writer cannot leave a Recorder retry chain behind."""
    loop = __import__("asyncio").get_running_loop()
    recorder = DelayedRetryingImportRequester(loop, delay=0.001)
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        lambda *_args: False,
    )
    write = __import__("asyncio").create_task(
        GarminHistoryRecorder(recorder).async_write(
            statistic_id_for("opaque-account-key-123", "heart_rate"),
            HEART_RATE_METADATA,
            (
                NormalizedSample(
                    datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                    date(2026, 7, 24),
                    "2026-07-24T01:02:00+00:00",
                    60.0,
                ),
            ),
        )
    )
    await __import__("asyncio").sleep(0.005)
    write.cancel()

    with pytest.raises(__import__("asyncio").CancelledError):
        await write

    assert recorder.tasks[0].abandoned.is_set()
    task_count_at_cancellation = len(recorder.tasks)
    await __import__("asyncio").sleep(0.01)
    assert len(recorder.tasks) == task_count_at_cancellation
    recorder.cancel_pending()


@pytest.mark.asyncio
async def test_writer_cancellation_abandons_unstarted_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cancelled caller abandons an import before its first Recorder run."""
    loop = __import__("asyncio").get_running_loop()
    recorder = DelayedRetryingImportRequester(loop, delay=0.002)
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        lambda *_args: True,
    )
    write = __import__("asyncio").create_task(
        GarminHistoryRecorder(recorder).async_write(
            statistic_id_for("opaque-account-key-123", "heart_rate"),
            HEART_RATE_METADATA,
            (
                NormalizedSample(
                    datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                    date(2026, 7, 24),
                    "2026-07-24T01:02:00+00:00",
                    60.0,
                ),
            ),
        )
    )
    await __import__("asyncio").sleep(0)
    write.cancel()

    with pytest.raises(__import__("asyncio").CancelledError):
        await write
    await __import__("asyncio").sleep(0.01)

    assert recorder.tasks[0].abandoned.is_set()
    assert not recorder.tasks[0].future.done()


@pytest.mark.asyncio
async def test_writer_hides_import_statistics_exception(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A Recorder task failure settles the writer without exposing its details."""
    secret = "token=garmin-secret"
    loop = __import__("asyncio").get_running_loop()
    recorder = RetryingImportRequester(loop)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_TIMEOUT", 0.1)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_MAX_TIMEOUT", 0.1)
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        lambda *_args: (_ for _ in ()).throw(OSError(secret)),
    )

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert (result.outcome, result.error_type) == ("failed", "recorder_unavailable")
    assert len(recorder.tasks) == 1
    assert recorder.tasks[0].terminal.is_set()
    assert not recorder.tasks[0].abandoned.is_set()
    assert secret not in str(result)
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_writer_hides_queue_task_oserror(caplog: pytest.LogCaptureFixture) -> None:
    """A failed Recorder enqueue returns a safe writer outcome."""
    secret = "token=garmin-secret"

    class FailingQueueRequester:
        def __init__(self) -> None:
            self.tasks: list[object] = []

        def async_import_statistics(self, metadata, stats, table) -> None:
            del metadata, stats, table

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            raise OSError(secret)

    recorder = FailingQueueRequester()
    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert (result.outcome, result.error_type) == ("failed", "recorder_unavailable")
    assert recorder.tasks[0].abandoned.is_set()
    assert secret not in str(result)
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_writer_hides_retry_enqueue_exception_without_requeueing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A retry enqueue failure is terminal and returns a safe writer outcome."""
    secret = "token=garmin-secret"
    loop = __import__("asyncio").get_running_loop()

    class FailingRetryQueueRequester:
        def __init__(self) -> None:
            self.instance = SimpleNamespace(hass=SimpleNamespace(loop=loop))
            self.instance.queue_task = self.queue_task
            self.tasks: list[object] = []

        def async_import_statistics(self, metadata, stats, table) -> None:
            del metadata, stats, table

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            if len(self.tasks) == 1:
                loop.call_soon(task.run, self.instance)
                return
            raise OSError(secret)

    recorder = FailingRetryQueueRequester()
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_TIMEOUT", 0.01)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_MAX_TIMEOUT", 0.01)
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        lambda *_args: False,
    )

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert (result.outcome, result.error_type) == ("failed", "recorder_unavailable")
    assert len(recorder.tasks) == 2
    assert recorder.tasks[0].terminal.is_set()
    assert secret not in str(result)
    assert secret not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    (
        SQLAlchemyError("token=garmin-secret"),
        DatabaseError("INSERT", {}, OSError("token=garmin-secret")),
    ),
    ids=("sqlalchemy", "database"),
)
async def test_sqlalchemy_error_reaches_recorder_recovery_with_safe_wrapper(
    error: SQLAlchemyError,
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Database failures retain recovery without reaching Recorder diagnostics."""
    secret = "token=garmin-secret"
    loop = __import__("asyncio").get_running_loop()

    class RecoveringRecorder:
        def __init__(self) -> None:
            self.instance = SimpleNamespace(hass=SimpleNamespace(loop=loop))
            self.instance.queue_task = self.queue_task
            self.recovered_errors: list[SQLAlchemyError] = []
            self.reopen_count = 0
            self.tasks: list[object] = []

        def async_import_statistics(self, metadata, stats, table) -> None:
            del metadata, stats, table

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            loop.call_soon(self._run_task, task)

        def _run_task(self, task) -> None:
            try:
                task.run(self.instance)
            except SQLAlchemyError as err:
                self.recovered_errors.append(err)
                self._reopen_event_session()

        def _reopen_event_session(self) -> None:
            self.reopen_count += 1

    recorder = RecoveringRecorder()
    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics",
        lambda *_args: (_ for _ in ()).throw(error),
    )

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert (result.outcome, result.error_type) == ("failed", "recorder_unavailable")
    assert len(recorder.recovered_errors) == 1
    recovered_error = recorder.recovered_errors[0]
    assert isinstance(recovered_error, SQLAlchemyError)
    assert str(recovered_error) == "Recorder statistics import failed"
    assert recovered_error.__cause__ is None
    assert recovered_error.__context__ is None
    assert recorder.reopen_count == 1
    future_error = recorder.tasks[0].future.exception()
    assert isinstance(future_error, history_recorder_module._RecorderUnavailableError)
    assert future_error.__cause__ is None
    assert future_error.__context__ is None
    assert secret not in str(result)
    assert secret not in str(recovered_error)
    assert secret not in str(future_error)
    assert secret not in caplog.text


@pytest.mark.asyncio
async def test_cancelled_writer_ignores_late_task_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller cancellation cannot create an unobserved late task exception."""
    secret = "token=garmin-secret"
    loop = __import__("asyncio").get_running_loop()
    started = threading.Event()
    release = threading.Event()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()

    class ThreadedRecorder:
        def __init__(self) -> None:
            self.instance = SimpleNamespace(hass=SimpleNamespace(loop=loop))
            self.instance.queue_task = self.queue_task
            self.tasks: list[object] = []
            self.thread: threading.Thread | None = None

        def async_import_statistics(self, metadata, stats, table) -> None:
            del metadata, stats, table

        def queue_task(self, task) -> None:
            self.tasks.append(task)
            self.thread = threading.Thread(target=task.run, args=(self.instance,))
            self.thread.start()

    recorder = ThreadedRecorder()

    def import_statistics(_instance, _metadata, _statistics, _table) -> bool:
        started.set()
        release.wait()
        raise OSError(secret)

    monkeypatch.setattr(
        "homeassistant.components.recorder.statistics.import_statistics", import_statistics
    )
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    write = __import__("asyncio").create_task(
        GarminHistoryRecorder(recorder).async_write(
            statistic_id_for("opaque-account-key-123", "heart_rate"),
            HEART_RATE_METADATA,
            (
                NormalizedSample(
                    datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                    date(2026, 7, 24),
                    "2026-07-24T01:02:00+00:00",
                    60.0,
                ),
            ),
        )
    )
    try:
        await __import__("asyncio").to_thread(started.wait)
        write.cancel()
        with pytest.raises(__import__("asyncio").CancelledError):
            await write

        release.set()
        assert recorder.thread is not None
        await __import__("asyncio").to_thread(recorder.thread.join)
        recorder.tasks.clear()
        gc.collect()
        await __import__("asyncio").sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert not loop_errors


@pytest.mark.asyncio
async def test_writer_skips_empty_statistic_import_and_confirmation() -> None:
    """Empty source families do not create a Recorder queue task."""
    recorder = FakeRequester()

    result = await GarminHistoryRecorder(recorder).async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (),
    )

    assert result == history_recorder_module.RecorderWriteOutcome(0)
    assert recorder.imports == []
    assert recorder.tasks == []


@pytest.mark.asyncio
async def test_writer_preserves_source_instants_and_equal_values_without_state_events() -> None:
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
    """Different offset spellings of one Source Instant share one statistics row."""
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
async def test_writer_rejects_naive_datetime_before_source_instant_write() -> None:
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
async def test_writer_times_out_a_stalled_recorder_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queued write without confirmation is a recoverable failed outcome."""
    recorder = StalledRequester()
    writer = GarminHistoryRecorder(recorder)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_TIMEOUT", 0)
    monkeypatch.setattr(history_recorder_module, "_RECORDER_BARRIER_MAX_TIMEOUT", 0)
    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        (
            NormalizedSample(
                datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
                date(2026, 7, 24),
                "2026-07-24T01:02:00+00:00",
                60.0,
            ),
        ),
    )

    assert result.outcome == "failed"
    assert result.error_type == "recorder_barrier"
    assert len(recorder.tasks) == 1


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
    assert all(
        isinstance(task, history_recorder_module._ConfirmingImportStatisticsTask)
        for task in requester.tasks
    )
    assert all(isinstance(task, RecorderTask) for task in requester.tasks)
    assert len(requester.tasks) == 3
    assert all("state_changed" not in row for _, rows, _ in requester.imports for row in rows)


@pytest.mark.asyncio
async def test_writer_chunks_large_series_without_dropping_samples() -> None:
    requester = FakeRequester()
    writer = GarminHistoryRecorder(requester)
    samples = tuple(
        NormalizedSample(
            datetime(2026, 7, 24, 0, 0, tzinfo=UTC) + timedelta(seconds=index),
            date(2026, 7, 24),
            index,
            float(index % 100),
        )
        for index in range(2500)
    )

    result = await writer.async_write(
        statistic_id_for("opaque-account-key-123", "heart_rate"),
        HEART_RATE_METADATA,
        samples,
    )

    assert result.accepted_count == len(samples)
    assert sum(len(rows) for _, rows, _ in requester.imports) == len(samples)
    assert max(len(rows) for _, rows, _ in requester.imports) < len(samples)
    assert len(writer._recent_values) <= 4096
