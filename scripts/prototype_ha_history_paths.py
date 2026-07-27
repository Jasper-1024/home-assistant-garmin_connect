"""PROTOTYPE — validate two HA Recorder paths for Garmin raw history.

Question:
Can Home Assistant itself persist Garmin samples at their original non-hour
timestamps, survive restart, preserve repeated values, and expose them through
HA History/Statistics queries without using the production Recorder database?

Run:
    rtk .venv/bin/python scripts/prototype_ha_history_paths.py
    rtk .venv/bin/python scripts/prototype_ha_history_paths.py --run-all

Every experiment uses a temporary config directory and scratch SQLite database.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import tempfile
from typing import Any

from homeassistant import loader
from homeassistant.components.recorder import history, statistics
from homeassistant.components.recorder.db_schema import Statistics
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.tasks import SynchronizeTask
from homeassistant.config_entries import ConfigEntries
from homeassistant.const import EVENT_STATE_CHANGED, __version__
from homeassistant.core import Event, HomeAssistant, callback
from homeassistant.helpers.recorder import (
    async_initialize_recorder,
    get_instance,
)
from homeassistant.setup import async_setup_component

from prototype_ha_history_logic import (
    SAMPLES,
    assess_long_term_statistics,
    assess_state_replay,
)

ENTITY_ID = "sensor.garmin_prototype_raw_heart_rate"
STATISTIC_ID = "garmin_prototype:raw_heart_rate"
WINDOW_START = SAMPLES[0].timestamp - timedelta(minutes=1)
WINDOW_END = SAMPLES[-1].timestamp + timedelta(minutes=1)


async def start_scratch_hass(config_dir: str, db_path: Path) -> HomeAssistant:
    """Start a real HA Recorder against a disposable SQLite database."""
    hass = HomeAssistant(config_dir)
    hass.config_entries = ConfigEntries(hass, {})
    loader.async_setup(hass)
    async_initialize_recorder(hass)
    configured = await async_setup_component(
        hass,
        "recorder",
        {
            "recorder": {
                "db_url": f"sqlite:///{db_path}",
                "commit_interval": 0,
                "auto_purge": False,
            }
        },
    )
    if not configured:
        raise RuntimeError("Scratch Recorder setup failed")
    await hass.async_start()
    return hass


async def stop_scratch_hass(hass: HomeAssistant) -> None:
    """Flush and stop the scratch HA instance."""
    await wait_for_recorder(get_instance(hass))
    await hass.async_stop()


async def wait_for_recorder(recorder) -> None:
    """Wait behind every task already queued, even if one is currently running."""
    future = recorder.hass.loop.create_future()
    recorder.queue_task(SynchronizeTask(future))
    await future


async def query_history(hass: HomeAssistant) -> list[dict[str, Any]]:
    """Read exact raw rows through HA's History query layer."""
    result = await get_instance(hass).async_add_executor_job(
        history.get_significant_states,
        hass,
        WINDOW_START,
        WINDOW_END,
        [ENTITY_ID],
        None,
        False,
        False,
        False,
        True,
        False,
    )
    rows = result.get(ENTITY_ID, [])
    return [
        {
            "timestamp": state.last_updated_timestamp,
            "value": state.state,
        }
        for state in rows
    ]


async def query_statistics(hass: HomeAssistant) -> list[dict[str, Any]]:
    """Read non-hour rows through HA's public statistics query layer."""
    result = await get_instance(hass).async_add_executor_job(
        statistics.statistics_during_period,
        hass,
        WINDOW_START,
        WINDOW_END,
        {STATISTIC_ID},
        "hour",
        None,
        {"mean", "min", "max"},
    )
    return [
        {
            "timestamp": row["start"],
            "mean": row.get("mean"),
            "min": row.get("min"),
            "max": row.get("max"),
        }
        for row in result.get(STATISTIC_ID, [])
    ]


async def run_path_a() -> dict[str, Any]:
    """Run path A against a scratch Recorder and restart it."""
    with tempfile.TemporaryDirectory(prefix="ha-garmin-path-a-") as config_dir:
        db_path = Path(config_dir) / "PROTOTYPE-path-a.db"
        hass = await start_scratch_hass(config_dir, db_path)
        emitted_events = 0

        @callback
        def count_event(event: Event) -> None:
            nonlocal emitted_events
            if event.data.get("entity_id") == ENTITY_ID:
                emitted_events += 1

        remove_listener = hass.bus.async_listen(EVENT_STATE_CHANGED, count_event)
        for sample in SAMPLES:
            hass.states.async_set(
                ENTITY_ID,
                str(int(sample.value)),
                {
                    "friendly_name": "Garmin prototype raw heart rate",
                    "unit_of_measurement": "bpm",
                    "state_class": "measurement",
                },
                force_update=True,
                timestamp=sample.timestamp.timestamp(),
            )
        await wait_for_recorder(get_instance(hass))
        rows_before_restart = await query_history(hass)
        current = hass.states.get(ENTITY_ID)
        current_state = current.state if current else None
        remove_listener()
        await stop_scratch_hass(hass)

        restarted = await start_scratch_hass(config_dir, db_path)
        rows_after_restart = await query_history(restarted)
        await stop_scratch_hass(restarted)

        return assess_state_replay(
            rows_before_restart=rows_before_restart,
            rows_after_restart=rows_after_restart,
            emitted_events=emitted_events,
            current_state=current_state,
        )


def statistic_rows(samples=SAMPLES) -> list[StatisticData]:
    """Convert fixture samples to raw-value statistic rows."""
    return [
        StatisticData(
            start=sample.timestamp,
            mean=sample.value,
            min=sample.value,
            max=sample.value,
        )
        for sample in samples
    ]


async def run_path_b() -> dict[str, Any]:
    """Run path B against a scratch Recorder and restart it."""
    with tempfile.TemporaryDirectory(prefix="ha-garmin-path-b-") as config_dir:
        db_path = Path(config_dir) / "PROTOTYPE-path-b.db"
        hass = await start_scratch_hass(config_dir, db_path)
        emitted_events = 0

        @callback
        def count_event(event: Event) -> None:
            nonlocal emitted_events
            if event.data.get("entity_id") == ENTITY_ID:
                emitted_events += 1

        remove_listener = hass.bus.async_listen(EVENT_STATE_CHANGED, count_event)
        metadata = StatisticMetaData(
            mean_type=StatisticMeanType.ARITHMETIC,
            has_sum=False,
            name="Garmin prototype raw heart rate",
            source="garmin_prototype",
            statistic_id=STATISTIC_ID,
            unit_class=None,
            unit_of_measurement="bpm",
        )
        recorder = get_instance(hass)
        recorder.async_import_statistics(metadata, statistic_rows(), Statistics)
        await wait_for_recorder(recorder)
        rows_before_restart = await query_statistics(hass)

        updated_value = 77.0
        updated_sample = SAMPLES[1]
        recorder.async_import_statistics(
            metadata,
            [
                StatisticData(
                    start=updated_sample.timestamp,
                    mean=updated_value,
                    min=updated_value,
                    max=updated_value,
                )
            ],
            Statistics,
        )
        await wait_for_recorder(recorder)
        rows_after_update = await query_statistics(hass)
        remove_listener()
        await stop_scratch_hass(hass)

        restarted = await start_scratch_hass(config_dir, db_path)
        rows_after_restart = await query_statistics(restarted)
        await stop_scratch_hass(restarted)

        return assess_long_term_statistics(
            rows_before_restart=rows_before_restart,
            rows_after_update=rows_after_update,
            rows_after_restart=rows_after_restart,
            updated_timestamp=updated_sample.timestamp.timestamp(),
            updated_value=updated_value,
            emitted_events=emitted_events,
        )


def render(results: dict[str, dict[str, Any]]) -> None:
    """Render the complete prototype state."""
    print("\033[2J\033[H", end="")
    print("\033[1mHA Garmin raw-history persistence prototype\033[0m")
    print(f"\033[2mHome Assistant {__version__}; scratch SQLite only\033[0m\n")
    if not results:
        print("No experiment has run yet.")
    for key, result in results.items():
        print(f"\033[1m{key}: {result['path']}\033[0m")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print()
    print("\033[1m[a]\033[0m path A  \033[1m[b]\033[0m path B  "
          "\033[1m[r]\033[0m run both  \033[1m[q]\033[0m quit")


async def interactive() -> None:
    """Drive the prototype one action at a time."""
    results: dict[str, dict[str, Any]] = {}
    while True:
        render(results)
        choice = (await asyncio.to_thread(input, "> ")).strip().lower()
        if choice == "q":
            return
        if choice in {"a", "r"}:
            results["A"] = await run_path_a()
        if choice in {"b", "r"}:
            results["B"] = await run_path_b()


async def run_all() -> None:
    """Run both experiments non-interactively."""
    results = {"A": await run_path_a(), "B": await run_path_b()}
    render(results)
    if not all(result["passed"] for result in results.values()):
        raise SystemExit(1)


def main() -> None:
    """Run the prototype."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-all", action="store_true")
    args = parser.parse_args()
    with suppress(KeyboardInterrupt):
        asyncio.run(run_all() if args.run_all else interactive())


if __name__ == "__main__":
    main()
