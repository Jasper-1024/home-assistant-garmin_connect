#!/usr/bin/env python3
"""Inspect a Garmin FIT file without emitting health values or GPS coordinates.

Runtime dependency:
    python -m pip install garmin-fit-sdk==21.208.0
"""

from __future__ import annotations

import argparse
import json
import stat
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

PRESENCE_RULES: dict[str, tuple[str, ...]] = {
    "heart_rate": ("heart_rate",),
    "temperature": ("temperature",),
    "gps": ("position_lat", "position_long"),
    "cadence": ("cadence",),
    "speed": ("speed",),
    "power": ("power",),
    "training_effect": ("training_effect",),
    "training_load": ("training_load", "exercise_load"),
    "recovery_time": ("recovery",),
}


def _message_name(message_number: int, number_to_name: dict[int, str]) -> str:
    """Return a stable lower-case message name."""
    return number_to_name.get(message_number, f"unknown_{message_number}").lower()


class FitMetadataCollector:
    """Collect structural metadata from decoded FIT messages."""

    def __init__(self, number_to_name: dict[int, str]) -> None:
        self._number_to_name = number_to_name
        self.message_counts: Counter[str] = Counter()
        self.message_fields: dict[str, set[str]] = defaultdict(set)
        self.timestamps: list[datetime] = []

    def __call__(self, message_number: int, message: dict[str, Any]) -> None:
        """Consume one decoded message without retaining field values."""
        name = _message_name(message_number, self._number_to_name)
        self.message_counts[name] += 1
        field_names = {str(field) for field in message}
        self.message_fields[name].update(field_names)

        for timestamp_key in ("timestamp", "start_time"):
            timestamp = message.get(timestamp_key)
            if isinstance(timestamp, datetime):
                self.timestamps.append(timestamp)
            elif isinstance(timestamp, date):
                self.timestamps.append(
                    datetime(timestamp.year, timestamp.month, timestamp.day)
                )

    def summary(self) -> dict[str, Any]:
        """Build a privacy-minimized structure summary."""
        all_fields = {
            field
            for message_field_names in self.message_fields.values()
            for field in message_field_names
        }
        presence = {
            category: (
                all(required in all_fields for required in rules)
                if category == "gps"
                else any(
                    rule in field
                    for field in all_fields
                    for rule in rules
                )
            )
            for category, rules in PRESENCE_RULES.items()
        }
        timestamps = sorted(self.timestamps)

        return {
            "message_counts": dict(sorted(self.message_counts.items())),
            "message_fields": {
                name: sorted(fields)
                for name, fields in sorted(self.message_fields.items())
            },
            "time_coverage": {
                "start": timestamps[0].isoformat() if timestamps else None,
                "end": timestamps[-1].isoformat() if timestamps else None,
            },
            "presence": presence,
        }


def _inspect_stream(
    *,
    stream_factory: Any,
    file_name: str,
    size_bytes: int,
    file_mode: int | None,
) -> dict[str, Any]:
    """Decode a FIT stream and return structural metadata only."""
    try:
        from garmin_fit_sdk import Decoder, Profile, Stream
    except ImportError as err:
        raise RuntimeError(
            "缺少 garmin-fit-sdk；请安装固定版本 21.208.0"
        ) from err

    integrity_ok = Decoder(stream_factory(Stream)).check_integrity()
    number_to_name = {
        message_number: name
        for name, message_number in Profile["mesg_num"].items()
    }
    collector = FitMetadataCollector(number_to_name)
    decoder = Decoder(stream_factory(Stream))
    _messages, errors = decoder.read(
        enable_crc_check=True,
        mesg_listener=collector,
    )
    summary = collector.summary()
    summary.update(
        {
            "file": {
                "name": file_name,
                "size_bytes": size_bytes,
                "mode": f"{file_mode:04o}" if file_mode is not None else None,
                "integrity_ok": integrity_ok,
                "decode_ok": not errors,
                "decode_error_types": sorted(
                    {type(error).__name__ for error in errors}
                ),
            }
        }
    )
    return summary


def inspect_fit(path: Path, required_mode: int | None = None) -> dict[str, Any]:
    """Decode one FIT file and return structural metadata only."""
    file_mode = stat.S_IMODE(path.stat().st_mode)
    if required_mode is not None and file_mode != required_mode:
        raise PermissionError(
            f"FIT 文件权限为 {file_mode:04o}，要求 {required_mode:04o}"
        )
    return _inspect_stream(
        stream_factory=lambda stream_type: stream_type.from_file(str(path)),
        file_name=path.name,
        size_bytes=path.stat().st_size,
        file_mode=file_mode,
    )


def inspect_fit_bytes(data: bytes, file_name: str = "<stdin>") -> dict[str, Any]:
    """Decode FIT bytes read from a pipe, without creating a duplicate file."""
    return _inspect_stream(
        stream_factory=lambda stream_type: stream_type.from_byte_array(bytearray(data)),
        file_name=file_name,
        size_bytes=len(data),
        file_mode=None,
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "fit_file",
        help="FIT path, or '-' to read FIT bytes from stdin",
    )
    parser.add_argument(
        "--require-mode",
        default="600",
        help="Required octal file mode; use 'none' to skip the check (default: 600)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write JSON to this file instead of stdout",
    )
    return parser.parse_args()


def main() -> int:
    """Run the FIT metadata inspector."""
    args = parse_args()
    required_mode = (
        None if args.require_mode.lower() == "none" else int(args.require_mode, 8)
    )
    if args.fit_file == "-":
        result = inspect_fit_bytes(sys.stdin.buffer.read())
    else:
        result = inspect_fit(Path(args.fit_file), required_mode)
    rendered = f"{json.dumps(result, ensure_ascii=False, indent=2)}\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
        args.output.chmod(0o600)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
