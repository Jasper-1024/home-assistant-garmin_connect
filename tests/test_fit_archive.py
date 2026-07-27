"""FIT archive seam tests."""

import asyncio
import json
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from custom_components.garmin_connect.fit_archive import (
    FitArchiveError,
    async_archive_fit,
    fit_file_name,
    fit_record,
    inspect_fit,
    persisted_fit_summary,
    validated_fit_summary,
)


def _summary() -> dict[str, object]:
    return {
        "message_counts": {"record": 4},
        "message_fields": {"record": ["heart_rate", "timestamp"]},
        "time_coverage": {"start": "2026-07-24T00:00:00+00:00", "end": "2026-07-24T01:00:00+00:00"},
        "presence": dict.fromkeys(("heart_rate", "temperature", "gps", "cadence", "speed", "power", "training_effect", "training_load", "recovery_time", "recovery"), False),
        "file": {"size_bytes": 999, "integrity_ok": True, "decode_ok": True, "raw": "must not persist"},
    }


def _valid_inspector(path: Path, required_mode: int) -> dict[str, object]:
    return _summary()


def _persisted_summary() -> dict[str, object]:
    summary = _summary()
    del summary["file"]
    return summary


def _crc_failure(path: Path, required_mode: int) -> dict[str, object]:
    raise ValueError("CRC")


def _invalid_inspector(path: Path, required_mode: int) -> dict[str, object]:
    return {"file": {"integrity_ok": False, "decode_ok": True}}


@pytest.mark.asyncio
async def test_fit_archive_validates_privately_and_atomically(tmp_path: Path) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"
    record = await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="a" * 24,
        directory=tmp_path,
        inspect=_valid_inspector,
    )
    final = tmp_path / fit_file_name("a" * 24)
    assert final.read_bytes() == b"validated fit bytes"
    assert final.stat().st_mode & 0o777 == 0o600
    assert record["path"] == final.name
    assert "file" not in record["summary"]
    assert "raw" not in str(record)


@pytest.mark.asyncio
async def test_fit_archive_validation_failure_leaves_no_partial_file(tmp_path: Path) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"bad fit"
    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="b" * 24,
            directory=tmp_path,
            inspect=_crc_failure,
        )
    assert not (tmp_path / fit_file_name("b" * 24)).exists()
    assert not tuple(tmp_path.glob(".*.fit"))


@pytest.mark.asyncio
async def test_fit_archive_rejects_crc_or_decode_failure(tmp_path: Path) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"bad fit"
    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="c" * 24,
            directory=tmp_path,
            inspect=_invalid_inspector,
        )


@pytest.mark.asyncio
async def test_valid_existing_fit_is_reused_without_download(tmp_path: Path) -> None:
    client = AsyncMock()
    final = tmp_path / fit_file_name("d" * 24)
    final.write_bytes(b"existing")
    final.chmod(0o600)
    result = await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="d" * 24,
        directory=tmp_path,
        inspect=_valid_inspector,
    )
    client.download_activity.assert_not_awaited()
    assert result["logical_id"] == "d" * 24
    assert final.read_bytes() == b"existing"


@pytest.mark.asyncio
async def test_bad_existing_fit_is_not_replaced(tmp_path: Path) -> None:
    client = AsyncMock()
    final = tmp_path / fit_file_name("e" * 24)
    final.write_bytes(b"bad existing")
    final.chmod(0o600)
    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="e" * 24,
            directory=tmp_path,
            inspect=_invalid_inspector,
        )
    client.download_activity.assert_not_awaited()
    assert final.read_bytes() == b"bad existing"


@pytest.mark.asyncio
async def test_cancelled_download_leaves_no_temporary_or_target_file(tmp_path: Path) -> None:
    client = AsyncMock()
    client.download_activity.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="f" * 24,
            directory=tmp_path,
            inspect=_valid_inspector,
        )
    assert not (tmp_path / fit_file_name("f" * 24)).exists()
    assert not tuple(tmp_path.glob(".*.fit"))


def test_fit_record_keeps_opaque_activity_association_and_rejects_mismatch() -> None:
    record = fit_record({"logical_id": "a" * 24, "path": fit_file_name("a" * 24), "summary": _persisted_summary()})
    assert record["logical_id"] == "a" * 24
    assert "heart_rate" in record["summary"]["message_fields"]["record"]
    with pytest.raises(FitArchiveError):
        fit_record({"logical_id": "b" * 24, "path": fit_file_name("a" * 24), "summary": _persisted_summary()})


def test_fit_record_rejects_inspector_file_metadata() -> None:
    with pytest.raises(FitArchiveError):
        fit_record({"logical_id": "a" * 24, "path": fit_file_name("a" * 24), "summary": _summary()})


def test_inspector_summary_is_validated_then_stripped_for_persistence() -> None:
    summary = validated_fit_summary(_summary())
    assert "file" not in summary
    assert set(summary) == {"message_counts", "message_fields", "time_coverage", "presence"}
    with pytest.raises(FitArchiveError):
        persisted_fit_summary(_summary())


def test_fit_record_rejects_raw_or_unbounded_summary_fields() -> None:
    raw_summary = _summary()
    raw_summary["measurement"] = 42
    with pytest.raises(FitArchiveError):
        fit_record({"logical_id": "a" * 24, "path": fit_file_name("a" * 24), "summary": raw_summary})
    injected = _summary()
    injected["message_counts"] = {"record": "177"}
    with pytest.raises(FitArchiveError):
        fit_record({"logical_id": "a" * 24, "path": fit_file_name("a" * 24), "summary": injected})


def test_captured_structural_fixture_is_redacted_and_validated() -> None:
    fixture = json.loads((Path(__file__).parent / "fixtures" / "garmin_fit_structural_summary.json").read_text())
    assert fixture["provenance"]["redaction_version"] == "3.1.0-beta.1"
    summary = fixture["summary"]
    record = fit_record({"logical_id": "1" * 24, "path": fit_file_name("1" * 24), "summary": summary})
    assert set(record["summary"]) == {"message_counts", "message_fields", "time_coverage", "presence"}
    assert "heart_rate" in record["summary"]["message_fields"]["record"]
    assert "position_lat" in record["summary"]["message_fields"]["record"]
    assert not any(isinstance(value, (int, float)) for value in record["summary"]["message_fields"]["record"])
    assert "file" not in record["summary"]


def test_optional_private_captured_fit_replay() -> None:
    """Replay a private captured FIT when explicitly supplied by a developer."""
    raw_path = os.environ.get("GARMIN_SAKAMOTO13_FIT")
    if not raw_path:
        pytest.skip("GARMIN_SAKAMOTO13_FIT is not set")
    summary = inspect_fit(Path(raw_path), 0o600)
    assert summary["file"]["integrity_ok"] is True
    assert summary["file"]["decode_ok"] is True
    persisted = persisted_fit_summary(summary)
    assert set(persisted) == {"message_counts", "message_fields", "time_coverage", "presence"}
    assert {"file_id", "session", "lap", "record", "event", "time_in_zone", "device_info"}.intersection(persisted["message_counts"])
