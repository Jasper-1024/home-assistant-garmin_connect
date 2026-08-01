"""Tests for opt-in raw Garmin HTTP capture and offline replay."""

from __future__ import annotations

import json

import pytest
from ha_garmin import GarminConnectError

from custom_components.garmin_connect.debug_capture import (
    GarminDebugCapture,
    GarminDebugReplayError,
)


class _Client:
    """Small ha-garmin-shaped HTTP seam for capture tests."""

    def __init__(self) -> None:
        self.calls = 0

    async def _request(
        self, method: str, url: str, params: dict | None = None, _retry_count: int = 0
    ) -> dict:
        self.calls += 1
        return {"method": method, "url": url, "params": params, "calls": self.calls}

    async def _request_bytes(self, url: str) -> bytes:
        self.calls += 1
        return b"captured-fit-bytes"

    async def _post_request(self, url: str, payload: dict) -> dict:
        self.calls += 1
        return {"url": url, "payload": payload}

    async def _put_request(self, url: str, payload: dict) -> dict:
        self.calls += 1
        return {"url": url, "payload": payload}

    async def _delete_request(self, url: str) -> dict:
        self.calls += 1
        return {"url": url}


async def test_capture_persists_complete_request_and_response(tmp_path) -> None:
    """Capture writes replayable full JSON request/response records."""
    client = _Client()
    capture = GarminDebugCapture(
        tmp_path, "entry", capture_enabled=True, replay_session=None
    )
    capture.install(client)

    result = await client._request(
        "GET", "https://connectapi.garmin.com/example", {"date": "2026-08-01"}
    )

    assert result["calls"] == 1
    assert capture.session_id is not None
    directory = tmp_path / "entry" / capture.session_id
    manifest = [json.loads(line) for line in (directory / "manifest.jsonl").read_text().splitlines()]
    assert len(manifest) == 1
    request = json.loads((directory / manifest[0]["request_file"]).read_text())
    response = json.loads((directory / manifest[0]["response_file"]).read_text())
    assert request == {
        "args": ["GET", "https://connectapi.garmin.com/example", {"date": "2026-08-01"}],
        "kwargs": {},
        "operation": "_request",
    }
    assert response == result


async def test_replay_returns_capture_without_calling_network(tmp_path) -> None:
    """Replay returns a capture repeatedly and never invokes the original method."""
    captured_client = _Client()
    capture = GarminDebugCapture(tmp_path, "entry", capture_enabled=True, replay_session=None)
    capture.install(captured_client)
    expected = await captured_client._request("GET", "https://example.test/data", {"day": 1})

    replay_client = _Client()

    async def unexpected(*_args, **_kwargs):
        raise AssertionError("replay must not reach the network")

    replay_client._request = unexpected
    replay = GarminDebugCapture(
        tmp_path, "entry", capture_enabled=False, replay_session=capture.session_id
    )
    replay.install(replay_client)

    assert await replay_client._request("GET", "https://example.test/data", {"day": 1}) == expected
    assert await replay_client._request("GET", "https://example.test/data", {"day": 1}) == expected


async def test_replay_preserves_garmin_errors(tmp_path) -> None:
    """A captured Garmin transport failure retains its classification on replay."""
    client = _Client()

    async def fail(*_args, **_kwargs):
        raise GarminConnectError("captured API failure")

    client._request = fail
    capture = GarminDebugCapture(tmp_path, "entry", capture_enabled=True, replay_session=None)
    capture.install(client)
    with pytest.raises(GarminConnectError, match="captured API failure"):
        await client._request("GET", "https://example.test/fail")

    replay_client = _Client()
    replay = GarminDebugCapture(
        tmp_path, "entry", capture_enabled=False, replay_session=capture.session_id
    )
    replay.install(replay_client)
    with pytest.raises(GarminConnectError, match="captured API failure"):
        await replay_client._request("GET", "https://example.test/fail")


async def test_replay_preserves_binary_response_and_fails_closed_when_missing(tmp_path) -> None:
    """FIT-sized binary responses replay exactly and unknown calls never fall through."""
    client = _Client()
    capture = GarminDebugCapture(tmp_path, "entry", capture_enabled=True, replay_session=None)
    capture.install(client)
    assert await client._request_bytes("https://example.test/activity.fit") == b"captured-fit-bytes"

    replay_client = _Client()
    replay = GarminDebugCapture(
        tmp_path, "entry", capture_enabled=False, replay_session=capture.session_id
    )
    replay.install(replay_client)
    assert await replay_client._request_bytes("https://example.test/activity.fit") == b"captured-fit-bytes"
    with pytest.raises(GarminDebugReplayError, match="No captured response"):
        await replay_client._request("GET", "https://example.test/not-captured")
