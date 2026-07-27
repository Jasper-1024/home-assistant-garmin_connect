"""Tests for the account-scoped Garmin request gate seam."""

from __future__ import annotations

import asyncio

import pytest

from custom_components.garmin_connect.request_gate import (
    GarminRequestGate,
    GarminRequestPriority,
)


class ControllableClock:
    """Clock used by fake requesters without waiting on wall-clock time."""

    def __init__(self) -> None:
        self.ticks = 0

    def tick(self) -> int:
        """Advance and return the deterministic request-start time."""
        self.ticks += 1
        return self.ticks


class FakeRequester:
    """A request adapter whose completion is controlled by the test."""

    def __init__(self, clock: ControllableClock, name: str) -> None:
        self.clock = clock
        self.name = name
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.started_at: int | None = None

    async def __call__(self) -> str:
        self.started_at = self.clock.tick()
        self.started.set()
        await self.release.wait()
        return self.name


async def test_foreground_request_runs_before_waiting_history_request() -> None:
    """A current refresh queued behind history becomes the next request."""
    clock = ControllableClock()
    gate = GarminRequestGate()
    first = FakeRequester(clock, "first")
    history = FakeRequester(clock, "history")
    current = FakeRequester(clock, "current")

    first_task = asyncio.create_task(gate.async_request(GarminRequestPriority.FOREGROUND, first))
    await first.started.wait()
    history_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.BACKGROUND, history)
    )
    current_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.FOREGROUND, current)
    )
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    first.release.set()
    await current.started.wait()
    assert not history.started.is_set()

    current.release.set()
    await history.started.wait()
    history.release.set()

    assert await first_task == "first"
    assert await current_task == "current"
    assert await history_task == "history"
    assert current.started_at == 2
    assert history.started_at == 3

    await gate.async_close()


async def test_cancelled_waiter_is_removed_and_does_not_block_next_request() -> None:
    """Cancelling a queued request leaves the gate usable."""
    clock = ControllableClock()
    gate = GarminRequestGate()
    active = FakeRequester(clock, "active")
    cancelled = FakeRequester(clock, "cancelled")
    current = FakeRequester(clock, "current")

    active_task = asyncio.create_task(gate.async_request(GarminRequestPriority.FOREGROUND, active))
    await active.started.wait()
    cancelled_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.BACKGROUND, cancelled)
    )
    await asyncio.sleep(0)
    cancelled_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_task

    current_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.FOREGROUND, current)
    )
    active.release.set()
    await current.started.wait()
    assert not cancelled.started.is_set()
    current.release.set()

    assert await active_task == "active"
    assert await current_task == "current"
    await gate.async_close()


async def test_closing_gate_cancels_waiters_and_waits_for_active_request() -> None:
    """Unload cancels active and queued work without leaving the gate busy."""
    clock = ControllableClock()
    gate = GarminRequestGate()
    active = FakeRequester(clock, "active")
    waiting = FakeRequester(clock, "waiting")

    active_task = asyncio.create_task(gate.async_request(GarminRequestPriority.FOREGROUND, active))
    await active.started.wait()
    waiting_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.BACKGROUND, waiting)
    )
    await asyncio.sleep(0)

    close_task = asyncio.create_task(gate.async_close())
    await asyncio.sleep(0)

    with pytest.raises(asyncio.CancelledError):
        await waiting_task

    await close_task
    with pytest.raises(asyncio.CancelledError):
        await active_task
    assert not waiting.started.is_set()


async def test_cancelling_active_request_releases_the_gate() -> None:
    """A cancelled in-flight requester cannot strand the next waiter."""
    clock = ControllableClock()
    gate = GarminRequestGate()
    active = FakeRequester(clock, "active")
    next_request = FakeRequester(clock, "next")

    active_task = asyncio.create_task(gate.async_request(GarminRequestPriority.FOREGROUND, active))
    await active.started.wait()
    active_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await active_task

    next_task = asyncio.create_task(
        gate.async_request(GarminRequestPriority.FOREGROUND, next_request)
    )
    await next_request.started.wait()
    next_request.release.set()
    assert await next_task == "next"
    await gate.async_close()


async def test_separate_gates_do_not_block_each_other() -> None:
    """Two Garmin accounts can execute requests independently."""
    clock = ControllableClock()
    first_gate = GarminRequestGate()
    second_gate = GarminRequestGate()
    first = FakeRequester(clock, "first")
    second = FakeRequester(clock, "second")

    first_task = asyncio.create_task(
        first_gate.async_request(GarminRequestPriority.FOREGROUND, first)
    )
    second_task = asyncio.create_task(
        second_gate.async_request(GarminRequestPriority.FOREGROUND, second)
    )
    await asyncio.gather(first.started.wait(), second.started.wait())

    first.release.set()
    second.release.set()
    assert await first_task == "first"
    assert await second_task == "second"
    await asyncio.gather(first_gate.async_close(), second_gate.async_close())
