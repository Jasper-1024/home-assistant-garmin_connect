"""FIT archive seam tests."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from custom_components.garmin_connect.fit_archive import (
    FitArchiveError,
    async_archive_fit,
    fit_file_name,
)


def _summary() -> dict[str, object]:
    return {
        "message_counts": {"record": 4},
        "message_fields": {"record": ["heart_rate", "timestamp"]},
        "time_coverage": {"start": "2026-07-24T00:00:00+00:00", "end": "2026-07-24T01:00:00+00:00"},
        "presence": {"heart_rate": True, "gps": False, "temperature": False},
        "file": {"size_bytes": 999, "integrity_ok": True, "decode_ok": True, "raw": "must not persist"},
    }


@pytest.mark.asyncio
async def test_fit_archive_validates_privately_and_atomically(tmp_path: Path) -> None:
    client = AsyncMock()
    client.download_activity.return_value = b"validated fit bytes"
    record = await async_archive_fit(
        client=client,
        activity_id="12345",
        logical_id="a" * 24,
        directory=tmp_path,
        inspect=lambda path, required_mode: _summary(),
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
            inspect=lambda path, required_mode: (_ for _ in ()).throw(ValueError("CRC")),
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
            inspect=lambda path, required_mode: {"file": {"integrity_ok": False, "decode_ok": True}},
        )
