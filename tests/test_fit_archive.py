"""FIT archive seam tests."""

import asyncio
import json
import os
import stat
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from custom_components.garmin_connect import fit_archive as fit_archive_module
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
async def test_fit_archive_hardens_account_directory_permissions(tmp_path: Path) -> None:
    directory = tmp_path / "account"
    directory.mkdir(mode=0o755)
    directory.chmod(0o755)
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"

    await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="a" * 24,
        directory=directory,
        inspect=_valid_inspector,
    )

    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert stat.S_IMODE((directory / fit_file_name("a" * 24)).stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_fit_archive_degrades_when_directory_fsync_is_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"

    original_open = os.open

    def unsupported_directory_open(path: Path, flags: int, *args: object) -> int:
        if Path(path) == tmp_path:
            raise OSError("directory handles are unsupported")
        return original_open(path, flags, *args)

    monkeypatch.setattr(fit_archive_module.os, "open", unsupported_directory_open)

    record = await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="a" * 24,
        directory=tmp_path,
        inspect=_valid_inspector,
    )

    assert record["path"] == fit_file_name("a" * 24)
    assert (tmp_path / record["path"]).read_bytes() == b"validated fit bytes"


@pytest.mark.asyncio
async def test_fit_archive_uses_path_chmod_without_fchmod(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"
    monkeypatch.delattr(fit_archive_module.os, "fchmod", raising=False)

    record = await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="7" * 24,
        directory=tmp_path,
        inspect=_valid_inspector,
    )

    final = tmp_path / record["path"]
    assert final.read_bytes() == b"validated fit bytes"
    assert final.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_fit_archive_uses_windows_safe_permission_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"
    monkeypatch.setattr(fit_archive_module, "_IS_WINDOWS", True)
    monkeypatch.delattr(fit_archive_module.os, "fchmod", raising=False)

    record = await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="8" * 24,
        directory=tmp_path,
        inspect=_valid_inspector,
    )

    assert (tmp_path / record["path"]).read_bytes() == b"validated fit bytes"


@pytest.mark.asyncio
async def test_fit_archive_does_not_swallow_path_permission_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"
    monkeypatch.delattr(fit_archive_module.os, "fchmod", raising=False)

    original_chmod = fit_archive_module.os.chmod

    def fail_file_chmod(path: Path, mode: int) -> None:
        if Path(path).name.startswith("."):
            raise OSError("permission update failed")
        original_chmod(path, mode)

    monkeypatch.setattr(fit_archive_module.os, "chmod", fail_file_chmod)

    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="9" * 24,
            directory=tmp_path,
            inspect=_valid_inspector,
        )

    assert not (tmp_path / fit_file_name("9" * 24)).exists()


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
@pytest.mark.parametrize("content", [b"\x0e\x20", b"\x0e\x10\x00\x00\x00"])
async def test_fit_archive_rejects_real_partial_fit_bytes(
    tmp_path: Path, content: bytes
) -> None:
    client = AsyncMock()
    client.download_activity.return_value = content

    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="2" * 24,
            directory=tmp_path,
            inspect=inspect_fit,
        )

    assert not (tmp_path / fit_file_name("2" * 24)).exists()


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
async def test_existing_fit_with_insecure_permissions_is_rejected(tmp_path: Path) -> None:
    client = AsyncMock()
    final = tmp_path / fit_file_name("0" * 24)
    final.write_bytes(b"existing")
    final.chmod(0o644)

    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="0" * 24,
            directory=tmp_path,
            inspect=_valid_inspector,
        )

    client.download_activity.assert_not_awaited()


@pytest.mark.asyncio
async def test_symlinked_fit_target_is_rejected_without_following_or_replacing(
    tmp_path: Path,
) -> None:
    client = AsyncMock()
    target = tmp_path / "outside.fit"
    target.write_bytes(b"outside")
    target.chmod(0o600)
    final = tmp_path / fit_file_name("1" * 24)
    final.symlink_to(target)

    with pytest.raises(FitArchiveError):
        await async_archive_fit(
            client=client,
            activity_id="12345",
            logical_id="1" * 24,
            directory=tmp_path,
            inspect=_valid_inspector,
        )

    client.download_activity.assert_not_awaited()
    assert final.is_symlink()
    assert target.read_bytes() == b"outside"


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


def test_optional_private_captured_fit_replay(capsys: pytest.CaptureFixture[str]) -> None:
    """Replay a private captured FIT when explicitly supplied by a developer."""
    raw_path = os.environ.get("GARMIN_SAKAMOTO13_FIT")
    if not raw_path:
        pytest.skip("GARMIN_SAKAMOTO13_FIT is not set")
    assert raw_path is not None
    capture_path = Path(raw_path)
    assert stat.S_IMODE(capture_path.stat().st_mode) == 0o600
    summary = inspect_fit(capture_path, 0o600)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert summary["file"]["integrity_ok"] is True
    assert summary["file"]["decode_ok"] is True
    persisted = persisted_fit_summary(summary)
    assert set(persisted) == {"message_counts", "message_fields", "time_coverage", "presence"}
    assert {"file_id", "session", "lap", "record", "event", "time_in_zone", "device_info"}.intersection(persisted["message_counts"])
