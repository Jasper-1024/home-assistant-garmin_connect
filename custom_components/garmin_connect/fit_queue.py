"""Stable JSON serialization for the durable FIT work queue."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime


def serialize_fit_queue(
    queue: Mapping[str, Mapping[str, str]],
) -> list[dict[str, str]]:
    """Serialize pending FIT work in deterministic logical-ID order."""
    return [dict(queue[key]) for key in sorted(queue)]


def serialize_fit_pacing(value: datetime | None) -> str | None:
    """Serialize the optional UTC FIT pacing checkpoint."""
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()
