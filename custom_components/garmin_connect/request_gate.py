"""Account-scoped priority gate for serialized Garmin requests."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import IntEnum
from typing import Any, TypeVar


class GarminRequestPriority(IntEnum):
    """Ordering for work waiting to use one account's Garmin client."""

    BACKGROUND = 0
    FOREGROUND = 1


class GarminRequestGateClosedError(RuntimeError):
    """Raised when work is submitted after an account gate has closed."""


_RequestResult = TypeVar("_RequestResult")


@dataclass(slots=True)
class _Waiter:
    """Internal ownership handoff for one queued request."""

    priority: GarminRequestPriority
    sequence: int
    ready: asyncio.Future[None]
    task: asyncio.Task[Any]
    granted: bool = False


class GarminRequestGate:
    """Serialize account requests while prioritizing foreground refreshes.

    The gate owns only request admission.  A caller supplies a zero-argument
    async requester, which is invoked after admission and released on every
    exit path.  Queued requests are ordered by priority and then arrival;
    cancellation removes a waiter, and closing rejects future work and
    cancels any active request before waiting for its release.
    """

    def __init__(self) -> None:
        """Initialize an open gate with no active or waiting requests."""
        self._lock = asyncio.Lock()
        self._waiters: list[_Waiter] = []
        self._next_sequence = 0
        self._active = False
        self._active_task: asyncio.Task[Any] | None = None
        self._closed = False
        self._idle = asyncio.Event()
        self._idle.set()

    async def async_request(
        self,
        priority: GarminRequestPriority,
        requester: Callable[[], Awaitable[_RequestResult]],
    ) -> _RequestResult:
        """Run one request after acquiring this account's priority slot."""
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("Garmin requests must run in an asyncio task")
        waiter = await self._async_enqueue(priority, task)
        try:
            await waiter.ready
        except asyncio.CancelledError:
            await self._async_cancel_waiter(waiter)
            raise

        try:
            return await requester()
        finally:
            await self._async_release(waiter)

    async def async_close(self) -> None:
        """Reject queued work and cancel active work before returning."""
        async with self._lock:
            self._closed = True
            waiters = tuple(self._waiters)
            self._waiters.clear()
            for waiter in waiters:
                waiter.ready.cancel()
            active_task = self._active_task
            idle = self._idle

        if active_task is asyncio.current_task():
            return
        if active_task is not None:
            active_task.cancel()

        await idle.wait()

    async def _async_enqueue(
        self, priority: GarminRequestPriority, task: asyncio.Task[Any]
    ) -> _Waiter:
        """Queue a request and grant it immediately when the gate is idle."""
        loop = asyncio.get_running_loop()
        waiter = _Waiter(priority, self._next_sequence, loop.create_future(), task)

        async with self._lock:
            if self._closed:
                raise GarminRequestGateClosedError("Garmin request gate is closed")
            self._next_sequence += 1
            self._waiters.append(waiter)
            self._async_grant_next_locked()
        return waiter

    async def _async_cancel_waiter(self, waiter: _Waiter) -> None:
        """Remove a cancelled waiter or release a grant lost to cancellation."""
        async with self._lock:
            if waiter.granted:
                self._active = False
                self._active_task = None
                self._async_grant_next_locked()
                if not self._active:
                    self._idle.set()
                return

            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass
            self._async_grant_next_locked()

    async def _async_release(self, waiter: _Waiter) -> None:
        """Release the slot owned by a completed request."""
        async with self._lock:
            if not waiter.granted or not self._active:
                return
            self._active = False
            self._active_task = None
            self._async_grant_next_locked()
            if not self._active:
                self._idle.set()

    def _async_grant_next_locked(self) -> None:
        """Grant the highest-priority oldest waiter while the lock is held."""
        if self._active or not self._waiters:
            return

        index = max(
            range(len(self._waiters)),
            key=lambda position: (
                self._waiters[position].priority,
                -self._waiters[position].sequence,
            ),
        )
        waiter = self._waiters.pop(index)
        if waiter.ready.cancelled():
            self._async_grant_next_locked()
            return

        self._active = True
        self._active_task = waiter.task
        self._idle.clear()
        waiter.granted = True
        waiter.ready.set_result(None)
