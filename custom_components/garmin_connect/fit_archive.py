"""Private, validated FIT files and privacy-minimized structural summaries."""

from __future__ import annotations

import asyncio
import importlib.util
import os
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Callable, cast


class FitArchiveError(ValueError):
    """FIT download, validation, or persistence failed."""


def inspect_fit(path: Path, required_mode: int = 0o600) -> dict[str, Any]:
    """Use the checked-in privacy-minimizing FIT inspector."""
    inspector_path = Path(__file__).parents[2] / "scripts" / "inspect_garmin_fit.py"
    spec = importlib.util.spec_from_file_location("garmin_fit_inspector", inspector_path)
    if spec is None or spec.loader is None:
        raise FitArchiveError("FIT inspector unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    inspector = cast(Callable[[Path, int], dict[str, Any]], module.inspect_fit)
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
    allowed = {"message_counts", "message_fields", "time_coverage", "presence"}
    result = {key: summary[key] for key in allowed if key in summary}
    presence = result.get("presence")
    if not isinstance(presence, Mapping):
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
