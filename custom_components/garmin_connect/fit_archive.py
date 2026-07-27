"""Private, validated FIT files and privacy-minimized structural summaries."""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast

from .fit_inspector import inspect_fit as _inspect_fit

_SUMMARY_KEYS = frozenset({"message_counts", "message_fields", "time_coverage", "presence"})
_PRESENCE_KEYS = frozenset({"heart_rate", "temperature", "gps", "cadence", "speed", "power", "training_effect", "training_load", "recovery_time", "recovery"})
_MAX_MESSAGES = 512
_MAX_FIELDS = 256
_MAX_NAME = 96


class FitArchiveError(ValueError):
    """FIT download, validation, or persistence failed."""


def inspect_fit(path: Path, required_mode: int = 0o600) -> dict[str, Any]:
    """Use the checked-in privacy-minimizing FIT inspector."""
    inspector = cast(Callable[[Path, int], dict[str, Any]], _inspect_fit)
    return inspector(path, required_mode)


def fit_file_name(logical_id: str) -> str:
    """Return a stable opaque filename for an archived activity FIT."""
    if not logical_id or any(character not in "0123456789abcdef" for character in logical_id):
        raise FitArchiveError("invalid activity identity")
    return f"activity_{logical_id}.fit"


def _summary_without_file(summary: Mapping[str, Any], *, require_integrity: bool = True) -> dict[str, Any]:
    """Keep only bounded structural inspector output."""
    file_info = summary.get("file")
    if require_integrity and (not isinstance(file_info, Mapping) or file_info.get("integrity_ok") is not True or file_info.get("decode_ok") is not True):
        raise FitArchiveError("FIT integrity or decode failed")
    if set(summary) - _SUMMARY_KEYS - {"file"}:
        raise FitArchiveError("FIT summary has unknown fields")
    allowed = set(_SUMMARY_KEYS)
    result = {key: summary[key] for key in allowed if key in summary}
    if set(result) != _SUMMARY_KEYS:
        raise FitArchiveError("FIT summary is incomplete")
    counts = result["message_counts"]
    fields = result["message_fields"]
    coverage = result["time_coverage"]
    if not isinstance(counts, Mapping) or len(counts) > _MAX_MESSAGES or any(
        not isinstance(name, str) or len(name) > _MAX_NAME or not isinstance(count, int) or isinstance(count, bool) or not 0 <= count <= 1_000_000
        for name, count in counts.items()
    ):
        raise FitArchiveError("FIT message counts are invalid")
    if not isinstance(fields, Mapping) or len(fields) > _MAX_MESSAGES or any(
        not isinstance(name, str) or len(name) > _MAX_NAME or not isinstance(names, list) or len(names) > _MAX_FIELDS
        or any(not isinstance(field, str) or len(field) > _MAX_NAME for field in names)
        for name, names in fields.items()
    ):
        raise FitArchiveError("FIT message fields are invalid")
    if not isinstance(coverage, Mapping) or set(coverage) != {"start", "end"} or any(
        value is not None and (not isinstance(value, str) or len(value) > 64)
        for value in coverage.values()
    ):
        raise FitArchiveError("FIT time coverage is invalid")
    for value in coverage.values():
        if value is not None:
            try:
                from datetime import datetime
                datetime.fromisoformat(value)
            except (TypeError, ValueError) as err:
                raise FitArchiveError("FIT time coverage is invalid") from err
    presence = result.get("presence")
    if require_integrity and isinstance(presence, Mapping) and "recovery" not in presence:
        presence = {**presence, "recovery": presence.get("recovery_time", False)}
        result["presence"] = presence
    if not isinstance(presence, Mapping) or set(presence) - _PRESENCE_KEYS or set(presence) != _PRESENCE_KEYS or any(not isinstance(value, bool) for value in presence.values()):
        raise FitArchiveError("FIT summary presence is invalid")
    result["presence"] = {
        key: bool(presence.get(key, False))
        for key in (
            "heart_rate", "temperature", "gps", "cadence", "speed",
            "power", "training_effect", "training_load", "recovery_time",
        )
    }
    result["presence"]["recovery"] = result["presence"]["recovery_time"]
    return result


async def async_archive_fit(
    *,
    client: Any,
    activity_id: str,
    logical_id: str,
    directory: Path,
    inspect: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    """Download, validate, and atomically archive one FIT file."""
    try:
        directory.mkdir(parents=True, exist_ok=True)
        final_path = directory / fit_file_name(logical_id)
        if final_path.exists():
            if final_path.stat().st_mode & 0o777 != 0o600:
                raise FitArchiveError("FIT permissions are invalid")
            existing_summary = await asyncio.to_thread(inspect, final_path, 0o600)
            if not isinstance(existing_summary, Mapping):
                raise FitArchiveError("FIT summary is invalid")
            return {"logical_id": logical_id, "path": final_path.name, "summary": _summary_without_file(existing_summary)}
        content = await client.download_activity(int(activity_id), "fit")
        if not isinstance(content, bytes) or not content:
            raise FitArchiveError("FIT download is empty")
        fd, temporary_name = tempfile.mkstemp(prefix=f".{logical_id}.", suffix=".fit", dir=directory)
        temporary_path = Path(temporary_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as temporary_file:
                temporary_file.write(content)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())
            summary = await asyncio.to_thread(inspect, temporary_path, 0o600)
            if not isinstance(summary, Mapping):
                raise FitArchiveError("FIT summary is invalid")
            safe_summary = _summary_without_file(summary)
            os.replace(temporary_path, final_path)
            os.chmod(final_path, 0o600)
            return {"logical_id": logical_id, "path": final_path.name, "summary": safe_summary}
        finally:
            temporary_path.unlink(missing_ok=True)
    except asyncio.CancelledError:
        raise
    except (OSError, RuntimeError, TypeError, ValueError) as err:
        raise FitArchiveError("FIT archive failed") from err


def fit_record(record: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a bounded persisted FIT record without exposing payload."""
    if not isinstance(record, Mapping):
        raise FitArchiveError("FIT record is invalid")
    logical_id = record.get("logical_id")
    path = record.get("path")
    summary = record.get("summary")
    if not isinstance(logical_id, str) or not isinstance(path, str) or path != fit_file_name(logical_id):
        raise FitArchiveError("FIT record is invalid")
    if not isinstance(summary, Mapping):
        raise FitArchiveError("FIT record is invalid")
    return {"logical_id": logical_id, "path": path, "summary": _summary_without_file(summary, require_integrity=False)}
