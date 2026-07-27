"""Conservative, checkpointed background history backfill policy."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

BACKFILL_START = date(2026, 1, 1)
BACKFILL_INTERVAL = timedelta(hours=1)
BACKOFF_429 = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class BackfillState:
    completed_dates: frozenset[str] = frozenset()
    next_run: datetime | None = None
    backoff_until: datetime | None = None
    last_success: datetime | None = None
    error_type: str | None = None

    def as_record(self) -> dict[str, Any]:
        return {
            "completed_dates": sorted(self.completed_dates),
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "backoff_until": self.backoff_until.isoformat() if self.backoff_until else None,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "error_type": self.error_type,
        }

    @classmethod
    def from_record(cls, record: Any) -> BackfillState:
        if not isinstance(record, dict):
            return cls()
        completed = record.get("completed_dates", [])
        if not isinstance(completed, list) or any(not isinstance(value, str) for value in completed):
            return cls()
        def parsed(value: Any) -> datetime | None:
            if not isinstance(value, str):
                return None
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return cls(frozenset(completed), parsed(record.get("next_run")), parsed(record.get("backoff_until")), parsed(record.get("last_success")), record.get("error_type") if isinstance(record.get("error_type"), str) else None)


def classify_backfill_error(error: BaseException) -> str:
    status = getattr(error, "status", getattr(error, "status_code", None))
    if status == 429:
        return "rate_limited"
    if status == 401:
        return "reauth_required"
    if status == 403:
        return "forbidden_path"
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    if isinstance(error, (OSError, TimeoutError, ConnectionError)):
        return "network"
    return "sync_failed"


def next_backfill_date(state: BackfillState, today: date) -> date | None:
    end = max(today, BACKFILL_START)
    for offset in range((end - BACKFILL_START).days + 1):
        target = BACKFILL_START + timedelta(days=offset)
        if target.isoformat() not in state.completed_dates:
            return target
    return None


class BackfillScheduler:
    """One-account scheduler; all network work is delegated to the archive."""

    def __init__(self, *, load_state: Any, save_state: Any, sync_date: Any, now: Any = datetime.now, sleep: Any = asyncio.sleep) -> None:
        self._load_state = load_state
        self._save_state = save_state
        self._sync_date = sync_date
        self._now = now
        self._sleep = sleep
        self.state = BackfillState()
        self._stopped = False

    async def async_run_once(self) -> BackfillState:
        now = self._now()
        self.state = BackfillState.from_record(await self._load_state())
        if self.state.backoff_until and now < self.state.backoff_until:
            return self.state
        if self.state.next_run and now < self.state.next_run:
            return self.state
        target = next_backfill_date(self.state, now.date())
        if target is None:
            self.state = BackfillState(self.state.completed_dates, now + BACKFILL_INTERVAL, self.state.backoff_until, self.state.last_success, None)
            await self._save_state(self.state.as_record())
            return self.state
        try:
            report = await self._sync_date(target)
            if getattr(report, "outcome", None) != "written":
                raise RuntimeError(getattr(report, "error_type", None) or "sync_failed")
            self.state = BackfillState(self.state.completed_dates | {target.isoformat()}, now + BACKFILL_INTERVAL, None, now, None)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            error_type = classify_backfill_error(error)
            backoff = now + BACKOFF_429 if error_type == "rate_limited" else self.state.backoff_until
            self.state = BackfillState(self.state.completed_dates, now + BACKFILL_INTERVAL, backoff, self.state.last_success, error_type)
        await self._save_state(self.state.as_record())
        return self.state

    async def async_run(self) -> None:
        while not self._stopped:
            await self.async_run_once()
            await self._sleep(BACKFILL_INTERVAL.total_seconds())

    def stop(self) -> None:
        self._stopped = True
