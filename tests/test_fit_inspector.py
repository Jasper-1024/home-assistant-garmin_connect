"""Tests for the privacy-minimized FIT metadata collector."""

from datetime import UTC, datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

SCRIPT_PATH = Path(__file__).parents[1] / "scripts" / "inspect_garmin_fit.py"
SPEC = spec_from_file_location("inspect_garmin_fit", SCRIPT_PATH)
assert SPEC is not None
assert SPEC.loader is not None
MODULE = module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
FitMetadataCollector = MODULE.FitMetadataCollector


def test_collector_reports_structure_without_values() -> None:
    """The summary must contain field names and counts but no measurements."""
    collector = FitMetadataCollector({20: "RECORD", 18: "SESSION"})
    collector(
        20,
        {
            "timestamp": datetime(2026, 7, 24, 1, 2, tzinfo=UTC),
            "heart_rate": 177,
            "position_lat": 123456,
            "position_long": 654321,
            "enhanced_speed": 3.5,
            "temperature": 29,
        },
    )
    collector(
        18,
        {
            "timestamp": datetime(2026, 7, 24, 2, 3, tzinfo=UTC),
            "total_training_effect": 3.2,
            "recovery_time": 18,
        },
    )

    summary = collector.summary()
    rendered = str(summary)

    assert summary["message_counts"] == {"record": 1, "session": 1}
    assert summary["presence"]["heart_rate"] is True
    assert summary["presence"]["gps"] is True
    assert summary["presence"]["speed"] is True
    assert summary["presence"]["training_effect"] is True
    assert summary["presence"]["recovery_time"] is True
    assert "177" not in rendered
    assert "123456" not in rendered
    assert "654321" not in rendered
    assert "3.5" not in rendered


def test_gps_requires_both_coordinate_fields() -> None:
    """A lone latitude or longitude field is not enough to report GPS."""
    collector = FitMetadataCollector({20: "RECORD"})
    collector(20, {"position_lat": 123456})

    assert collector.summary()["presence"]["gps"] is False
