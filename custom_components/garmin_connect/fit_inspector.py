"""Privacy-minimized FIT decoder used at runtime by the integration."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any


_PRESENCE_RULES: dict[str, tuple[str, ...]] = {
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


def inspect_fit(path: Path, required_mode: int = 0o600) -> dict[str, Any]:
    """Decode and CRC-check a FIT file without retaining measurement values."""
    try:
        from garmin_fit_sdk import Decoder, Profile, Stream
    except ImportError as err:
        raise RuntimeError("garmin-fit-sdk unavailable") from err
    if path.stat().st_mode & 0o777 != required_mode:
        raise PermissionError("FIT permissions are invalid")
    number_to_name = {number: name for name, number in Profile["mesg_num"].items()}
    counts: Counter[str] = Counter()
    fields: dict[str, set[str]] = defaultdict(set)
    timestamps: list[datetime] = []

    def listener(message_number: int, message: dict[str, Any]) -> None:
        name = number_to_name.get(message_number, f"unknown_{message_number}").lower()
        counts[name] += 1
        fields[name].update(str(key) for key in message)
        for key in ("timestamp", "start_time"):
            value = message.get(key)
            if isinstance(value, datetime):
                timestamps.append(value)
            elif isinstance(value, date):
                timestamps.append(datetime(value.year, value.month, value.day))

    integrity_ok = Decoder(Stream.from_file(str(path))).check_integrity()
    _messages, errors = Decoder(Stream.from_file(str(path))).read(enable_crc_check=True, mesg_listener=listener)
    all_fields = {field for names in fields.values() for field in names}
    presence = {
        category: (
            all(required in all_fields for required in rules)
            if category == "gps"
            else any(rule in field for field in all_fields for rule in rules)
        )
        for category, rules in _PRESENCE_RULES.items()
    }
    ordered = sorted(timestamps)
    return {
        "message_counts": dict(sorted(counts.items())),
        "message_fields": {name: sorted(values) for name, values in sorted(fields.items())},
        "time_coverage": {"start": ordered[0].isoformat() if ordered else None, "end": ordered[-1].isoformat() if ordered else None},
        "presence": presence,
        "file": {"integrity_ok": integrity_ok, "decode_ok": not errors},
    }
