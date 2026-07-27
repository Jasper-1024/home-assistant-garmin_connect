"""Conservative backfill policy tests."""

import asyncio
from datetime import UTC, date, datetime

import pytest

from custom_components.garmin_connect.backfill import (
    BACKFILL_START,
    BACKOFF_429,
    BackfillScheduler,
    BackfillState,
    classify_backfill_error,
    count_uncompleted_dates,
    next_backfill_date,
)
from custom_components.garmin_connect.history import HistorySyncReport
from custom_components.garmin_connect.request_gate import GarminRequestGate, GarminRequestPriority


def test_backfill_selects_first_uncompleted_date() -> None:
    state = BackfillState(frozenset({"2026-01-01", "2026-01-03"}))
    assert next_backfill_date(state, date(2026, 1, 3)) == date(2026, 1, 2)
    assert next_backfill_date(BackfillState(frozenset({"2026-01-01"})), date(2026, 1, 1)) is None
    assert BACKFILL_START == date(2026, 1, 1)


def test_backfill_state_round_trips_checkpoint_and_backoff() -> None:
    now = datetime(2026, 2, 1, tzinfo=UTC)
    state = BackfillState(frozenset({"2026-01-01"}), now, now + BACKOFF_429, now, "rate_limited")
    restored = BackfillState.from_record(state.as_record())
    assert restored == state
    assert count_uncompleted_dates(BackfillState(frozenset({"2026-01-01"})), date(2026, 1, 3)) == 2


def test_backfill_error_classes_are_bounded() -> None:
    class ResponseError(Exception):
        status_code = 429

    class ForbiddenError(Exception):
        status = 403

    assert classify_backfill_error(ResponseError()) == "rate_limited"
    assert classify_backfill_error(ForbiddenError()) == "forbidden_path"
    assert classify_backfill_error(OSError()) == "network"


@pytest.mark.asyncio
async def test_401_reauth_once_and_403_disable_path() -> None:
    now = datetime(2026, 2, 1, tzinfo=UTC)
    reauth_calls: list[int] = []
    attempts: list[int] = []
    persisted: dict = {}

    class UnauthorizedError(Exception):
        status_code = 401

    async def sync_date(target: date) -> None:
        attempts.append(1)
        raise UnauthorizedError()

    async def reauth() -> None:
        reauth_calls.append(1)

    async def save_state(state: dict) -> None:
        persisted.update(state)

    async def load_state() -> dict:
        return persisted

    scheduler = BackfillScheduler(load_state=load_state, save_state=save_state, sync_date=sync_date, reauth=reauth, now=lambda: now)
    await scheduler.async_run_once()
    await scheduler.async_run_once()
    assert attempts == [1]
    assert reauth_calls == [1]
    assert "history" in scheduler.state.disabled_paths


@pytest.mark.asyncio
async def test_failed_archive_seam_does_not_checkpoint_then_restart_retries() -> None:
    persisted: dict = {}
    outcomes = iter((HistorySyncReport(outcome="failed", error_type="network"), HistorySyncReport(outcome="written")))

    async def load_state() -> dict:
        return persisted

    async def save_state(state: dict) -> None:
        persisted.clear()
        persisted.update(state)

    async def sync_date(target: date) -> HistorySyncReport:
        return next(outcomes)

    now = datetime(2026, 2, 1, tzinfo=UTC)
    first = BackfillScheduler(load_state=load_state, save_state=save_state, sync_date=sync_date, now=lambda: now)
    await first.async_run_once()
    assert "2026-01-01" not in persisted["completed_dates"]
    restarted = BackfillScheduler(load_state=load_state, save_state=save_state, sync_date=sync_date, now=lambda: now)
    await restarted.async_run_once()
    assert "2026-01-01" in persisted["completed_dates"]


@pytest.mark.asyncio
async def test_foreground_request_priority_preempts_background_batch() -> None:
    gate = GarminRequestGate()
    order: list[str] = []
    release_blocker = asyncio.Event()

    async def blocker() -> None:
        order.append("blocker")
        await release_blocker.wait()

    async def background() -> None:
        order.append("background")

    async def foreground() -> None:
        order.append("foreground")

    blocker_task = asyncio.create_task(gate.async_request(GarminRequestPriority.BACKGROUND, blocker))
    await asyncio.sleep(0)
    background_task = asyncio.create_task(gate.async_request(GarminRequestPriority.BACKGROUND, background))
    await asyncio.sleep(0)
    foreground_task = asyncio.create_task(gate.async_request(GarminRequestPriority.FOREGROUND, foreground))
    await asyncio.sleep(0)
    release_blocker.set()
    await asyncio.gather(blocker_task, background_task, foreground_task)
    assert order == ["blocker", "foreground", "background"]


@pytest.mark.asyncio
async def test_background_fit_limit_defers_date_until_remaining_fit_converges() -> None:
    persisted: dict = {}
    calls: list[int] = []
    now = datetime(2026, 2, 1, tzinfo=UTC)

    async def load_state() -> dict:
        return persisted

    async def save_state(state: dict) -> None:
        persisted.clear()
        persisted.update(state)

    async def sync_date(target: date) -> HistorySyncReport:
        calls.append(1)
        return HistorySyncReport(outcome="failed", error_type="fit_limit_pending") if len(calls) == 1 else HistorySyncReport(outcome="written")

    scheduler = BackfillScheduler(load_state=load_state, save_state=save_state, sync_date=sync_date, now=lambda: now)
    await scheduler.async_run_once()
    assert persisted["completed_dates"] == []
    await scheduler.async_run_once()
    assert persisted["completed_dates"] == ["2026-01-01"]
    assert len(calls) == 2
