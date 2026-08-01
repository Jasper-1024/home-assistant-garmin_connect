"""Opt-in raw Garmin HTTP capture and offline replay for local debugging.

Capture files intentionally retain full Garmin request parameters and response
bodies.  They are for a single operator's local debugging session and live
under Home Assistant's configured ``tmp`` directory.  Authentication headers
and DI tokens are below this boundary and are never written here.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import uuid
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ha_garmin import GarminAuthError, GarminConnectError, GarminRateLimitError

_LOGGER = logging.getLogger(__name__)
_CAPTURE_SCHEMA_VERSION = 1
_WRAPPED_METHODS = (
    ("_request", False),
    ("_request_bytes", True),
    ("_post_request", False),
    ("_put_request", False),
    ("_delete_request", False),
)


class GarminDebugReplayError(RuntimeError):
    """A requested offline replay response is unavailable or malformed."""


def _json_value(value: Any) -> Any:
    """Encode arbitrary request values without losing replay identity."""
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, bytes):
        return {"__type__": "bytes", "base64": base64.b64encode(value).decode()}
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"__type__": "date", "value": value.isoformat()}
    if isinstance(value, Path):
        return {"__type__": "path", "value": str(value)}
    if isinstance(value, Mapping):
        return {str(key): _json_value(child) for key, child in value.items()}
    if isinstance(value, tuple | list):
        return [_json_value(item) for item in value]
    return {"__type__": type(value).__name__, "repr": repr(value)}


def _request_record(operation: str, args: tuple[Any, ...], kwargs: Mapping[str, Any]) -> dict[str, Any]:
    """Return the complete serializable request contract for one client call."""
    normalized_kwargs = dict(kwargs)
    if operation == "_request":
        # Retry calls are internal to ha-garmin.  The outer request captures the
        # final response while retaining a stable key for replay.
        normalized_kwargs.pop("_retry_count", None)
    return {
        "operation": operation,
        "args": _json_value(args),
        "kwargs": _json_value(normalized_kwargs),
    }


def _request_key(request: Mapping[str, Any]) -> str:
    """Build a stable lookup key from the exact captured request contract."""
    encoded = json.dumps(request, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _safe_session_name(value: str) -> str:
    """Accept one child directory name and reject traversal."""
    if not value or value in {".", ".."} or Path(value).name != value:
        raise ValueError("debug replay session must be a single directory name")
    return value


class GarminDebugCapture:
    """Install an opt-in per-client capture or replay boundary."""

    def __init__(
        self,
        root: Path,
        entry_id: str,
        *,
        capture_enabled: bool,
        replay_session: str | None,
    ) -> None:
        if capture_enabled and replay_session:
            raise ValueError("capture and replay cannot be enabled together")
        self._root = root / entry_id
        self._capture_enabled = capture_enabled
        self._replay_session = _safe_session_name(replay_session) if replay_session else None
        self._session_id = (
            f"capture-{datetime.now().astimezone().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
            if capture_enabled
            else None
        )
        self._sequence = 0
        self._replay_records: dict[str, list[dict[str, Any]]] | None = None
        self._replay_offsets: defaultdict[str, int] = defaultdict(int)

    @property
    def session_id(self) -> str | None:
        """Return the active capture session directory name."""
        return self._session_id

    def install(self, client: Any) -> None:
        """Wrap the private ha-garmin HTTP seam on this client instance."""
        if not self._capture_enabled and self._replay_session is None:
            return
        if getattr(client, "_garmin_debug_capture", None) is not None:
            return
        client._garmin_debug_capture = self
        for operation, returns_bytes in _WRAPPED_METHODS:
            original = getattr(client, operation, None)
            if not callable(original):
                continue
            setattr(client, operation, self._wrap(operation, original, returns_bytes))
        _LOGGER.debug(
            "Garmin HTTP debug %s enabled for client",
            "capture" if self._capture_enabled else f"replay={self._replay_session}",
        )

    def _wrap(
        self,
        operation: str,
        original: Callable[..., Awaitable[Any]],
        returns_bytes: bool,
    ) -> Callable[..., Awaitable[Any]]:
        async def wrapped(*args: Any, **kwargs: Any) -> Any:
            # ha-garmin retries _request by recursively calling self._request.
            # Capture only the outer call; replay also only needs that contract.
            retry_count = kwargs.get("_retry_count", args[3] if len(args) > 3 else 0)
            if operation == "_request" and retry_count:
                return await original(*args, **kwargs)
            request = _request_record(operation, args, kwargs)
            if self._replay_session is not None:
                return await self._async_replay(request, returns_bytes)
            if not self._capture_enabled:
                return await original(*args, **kwargs)
            return await self._async_capture(request, original, args, kwargs, returns_bytes)

        return wrapped

    async def _async_capture(
        self,
        request: dict[str, Any],
        original: Callable[..., Awaitable[Any]],
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
        returns_bytes: bool,
    ) -> Any:
        sequence = self._next_sequence()
        try:
            result = await original(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            await asyncio.to_thread(self._write_error, sequence, request, err)
            _LOGGER.debug("Captured Garmin request %s as %s", sequence, type(err).__name__)
            raise
        await asyncio.to_thread(self._write_response, sequence, request, result, returns_bytes)
        _LOGGER.debug("Captured Garmin request %s", sequence)
        return result

    async def _async_replay(self, request: dict[str, Any], returns_bytes: bool) -> Any:
        records = await self._async_replay_records()
        key = _request_key(request)
        candidates = records.get(key, [])
        if not candidates:
            raise GarminDebugReplayError(
                f"No captured response for {request['operation']} in {self._replay_session}"
            )
        offset = self._replay_offsets[key]
        record = candidates[min(offset, len(candidates) - 1)]
        self._replay_offsets[key] += 1
        result = await asyncio.to_thread(self._read_replay_result, record, returns_bytes)
        _LOGGER.debug("Replayed Garmin request %s from %s", record["sequence"], self._replay_session)
        return result

    async def _async_replay_records(self) -> dict[str, list[dict[str, Any]]]:
        if self._replay_records is None:
            self._replay_records = await asyncio.to_thread(self._load_replay_records)
        return self._replay_records

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence

    def _session_directory(self) -> Path:
        assert self._session_id is not None
        return self._root / self._session_id

    @staticmethod
    def _write_json(path: Path, value: Any) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as output:
            json.dump(value, output, ensure_ascii=False, indent=2, sort_keys=True)
            output.write("\n")
        os.chmod(temporary, 0o600)
        temporary.replace(path)

    def _write_manifest_record(self, record: Mapping[str, Any]) -> None:
        directory = self._session_directory()
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        manifest = directory / "manifest.jsonl"
        with manifest.open("a", encoding="utf-8") as output:
            output.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        os.chmod(manifest, 0o600)

    def _write_response(
        self,
        sequence: int,
        request: Mapping[str, Any],
        result: Any,
        returns_bytes: bool,
    ) -> None:
        directory = self._session_directory()
        prefix = f"{sequence:06d}"
        request_file = f"{prefix}.request.json"
        self._write_json(directory / request_file, request)
        record: dict[str, Any] = {
            "schema_version": _CAPTURE_SCHEMA_VERSION,
            "sequence": sequence,
            "request_key": _request_key(request),
            "request_file": request_file,
            "kind": "bytes" if returns_bytes else "json",
        }
        if returns_bytes:
            if not isinstance(result, bytes):
                raise TypeError("Garmin byte request returned a non-byte response")
            response_file = f"{prefix}.response.bin"
            path = directory / response_file
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_bytes(result)
            os.chmod(path, 0o600)
            record["response_file"] = response_file
            record["response_sha256"] = hashlib.sha256(result).hexdigest()
        else:
            response_file = f"{prefix}.response.json"
            self._write_json(directory / response_file, _json_value(result))
            record["response_file"] = response_file
        self._write_manifest_record(record)

    def _write_error(self, sequence: int, request: Mapping[str, Any], err: Exception) -> None:
        directory = self._session_directory()
        prefix = f"{sequence:06d}"
        request_file = f"{prefix}.request.json"
        error_file = f"{prefix}.error.json"
        self._write_json(directory / request_file, request)
        self._write_json(
            directory / error_file,
            {
                "exception_module": type(err).__module__,
                "exception_name": type(err).__name__,
                "message": str(err),
            },
        )
        self._write_manifest_record(
            {
                "schema_version": _CAPTURE_SCHEMA_VERSION,
                "sequence": sequence,
                "request_key": _request_key(request),
                "request_file": request_file,
                "kind": "error",
                "error_file": error_file,
            }
        )

    def _load_replay_records(self) -> dict[str, list[dict[str, Any]]]:
        assert self._replay_session is not None
        manifest = self._root / self._replay_session / "manifest.jsonl"
        if not manifest.is_file():
            raise GarminDebugReplayError(f"Replay manifest does not exist: {self._replay_session}")
        records: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for line in manifest.read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if not isinstance(record, dict) or record.get("schema_version") != _CAPTURE_SCHEMA_VERSION:
                raise GarminDebugReplayError("Replay manifest has an unsupported record")
            key = record.get("request_key")
            if not isinstance(key, str):
                raise GarminDebugReplayError("Replay manifest record is missing a request key")
            records[key].append(record)
        return dict(records)

    def _read_replay_result(self, record: Mapping[str, Any], returns_bytes: bool) -> Any:
        assert self._replay_session is not None
        directory = self._root / self._replay_session
        kind = record.get("kind")
        if kind == "error":
            error_file = record.get("error_file")
            if not isinstance(error_file, str):
                raise GarminDebugReplayError("Replay error record is malformed")
            error = json.loads((directory / error_file).read_text(encoding="utf-8"))
            message = str(error.get("message", "captured Garmin request failed"))
            error_name = error.get("exception_name")
            if error_name == "GarminRateLimitError":
                raise GarminRateLimitError(message)
            if error_name == "GarminAuthError":
                raise GarminAuthError(message)
            if error_name and "Garmin" in str(error_name):
                raise GarminConnectError(message)
            raise GarminDebugReplayError(f"Captured {error_name}: {message}")
        response_file = record.get("response_file")
        if not isinstance(response_file, str):
            raise GarminDebugReplayError("Replay response record is malformed")
        if kind == "bytes":
            if not returns_bytes:
                raise GarminDebugReplayError("Replay response type does not match request")
            return (directory / response_file).read_bytes()
        if kind != "json" or returns_bytes:
            raise GarminDebugReplayError("Replay response type does not match request")
        return json.loads((directory / response_file).read_text(encoding="utf-8"))
