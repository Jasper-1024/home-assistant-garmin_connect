"""Bounded daily Garmin status records and annual private storage."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta, timezone
from math import isfinite
from typing import Any

from .const import DOMAIN, HISTORY_STORE_VERSION
from .history_recorder import HistoryMetricMetadata
from .history_source import HistorySchemaError, NormalizedSample

DAILY_STATUS_SCHEMA_VERSION = 1
_DAY_TIME_ZONE = timezone(timedelta(hours=8))
_FAMILIES = frozenset({"hrv", "training", "sleep", "fitness_age", "stress"})
_PRESENCE = frozenset(
    {"present", "empty", "null", "missing", "unsupported", "failed"}
)


def _stat_key(value: str) -> str:
    """Return a stable lowercase Recorder statistic key component."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).replace("-", "_").lower()


@dataclass(frozen=True, slots=True)
class DailyStatusMetric:
    """One numeric status suitable for Recorder projection."""

    key: str
    name: str
    unit: str
    value: float

    @property
    def metadata(self) -> HistoryMetricMetadata:
        return HistoryMetricMetadata(self.key, self.name, self.unit)


@dataclass(frozen=True, slots=True)
class DailyStatusRecord:
    """One source-date status snapshot containing only known bounded fields."""

    family: str
    record_key: str
    calendar_date: date
    source_timestamp: datetime | None
    statistic_timestamp: datetime
    presence: str
    values: dict[str, Any]
    field_presence: dict[str, str]
    metrics: tuple[DailyStatusMetric, ...]
    revision: str
    projected_revision: str | None = None

    def samples(self) -> tuple[tuple[DailyStatusMetric, NormalizedSample], ...]:
        return tuple(
            (
                metric,
                NormalizedSample(
                    self.statistic_timestamp,
                    self.calendar_date,
                    (
                        self.source_timestamp.isoformat()
                        if self.source_timestamp is not None
                        else self.calendar_date.isoformat()
                    ),
                    metric.value,
                ),
            )
            for metric in self.metrics
        )


def _aware_timestamp(raw: Any = None) -> datetime | None:
    if isinstance(raw, datetime) and raw.tzinfo is not None:
        return raw.astimezone(UTC)
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is not None:
                return parsed.astimezone(UTC)
            if "T" in raw:
                return parsed.replace(tzinfo=UTC)
    if isinstance(raw, int | float) and not isinstance(raw, bool):
        seconds = float(raw) / (1000 if abs(float(raw)) >= 100_000_000_000 else 1)
        try:
            return datetime.fromtimestamp(seconds, UTC)
        except (OSError, OverflowError, ValueError):
            pass
    return None


def _statistic_timestamp(calendar_date: date, source: datetime | None) -> datetime:
    return source or datetime.combine(
        calendar_date, time.min, _DAY_TIME_ZONE
    ).astimezone(UTC)


def _calendar_date(source: Mapping[str, Any], fallback: date) -> date:
    raw = source.get("calendarDate")
    if raw is None:
        return fallback
    if not isinstance(raw, str):
        raise HistorySchemaError("calendarDate has an invalid type")
    try:
        return date.fromisoformat(raw)
    except ValueError as err:
        raise HistorySchemaError("calendarDate has an invalid value") from err


def _number(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise HistorySchemaError(f"{field_name} has an invalid type")
    result = float(value)
    if not isfinite(result):
        raise HistorySchemaError(f"{field_name} has an invalid value")
    return result


def _text(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise HistorySchemaError(f"{field_name} has an invalid type")
    return value


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise HistorySchemaError(f"{field_name} has an invalid type")
    return value


def _field(
    source: Mapping[str, Any],
    name: str,
    values: dict[str, Any],
    presence: dict[str, str],
    *,
    text_value: bool = False,
    presence_name: str | None = None,
) -> float | str | None:
    presence_key = presence_name or name
    if name not in source:
        presence[presence_key] = "missing"
        return None
    raw = source[name]
    if raw is None:
        presence[presence_key] = "null"
        return None
    value = _text(raw, name) if text_value else _number(raw, name)
    presence[presence_key] = "present"
    values[name] = value
    return value


def _metric(
    metrics: list[DailyStatusMetric], key: str, name: str, unit: str, value: Any
) -> None:
    if value is not None:
        metrics.append(DailyStatusMetric(key, name, unit, float(value)))


def _finish(
    family: str,
    calendar_date: date,
    values: dict[str, Any],
    presence: dict[str, str],
    metrics: Sequence[DailyStatusMetric],
    *,
    raw_timestamp: Any = None,
    record_presence: str | None = None,
    record_key: str | None = None,
) -> DailyStatusRecord:
    identity_key = record_key or family
    present = record_presence or ("present" if values or metrics else "empty")
    source_timestamp = _aware_timestamp(raw_timestamp)
    statistic_timestamp = _statistic_timestamp(calendar_date, source_timestamp)
    canonical = {
        "family": family,
        "record_key": identity_key,
        "calendar_date": calendar_date.isoformat(),
        "source_timestamp": source_timestamp.isoformat() if source_timestamp else None,
        "statistic_timestamp": statistic_timestamp.isoformat(),
        "presence": present,
        "values": values,
        "field_presence": presence,
        "metrics": [
            {"key": item.key, "name": item.name, "unit": item.unit, "value": item.value}
            for item in metrics
        ],
    }
    revision = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    return DailyStatusRecord(
        family,
        identity_key,
        calendar_date,
        source_timestamp,
        statistic_timestamp,
        present,
        values,
        presence,
        tuple(metrics),
        revision,
    )


def unavailable_daily_status(
    family: str, calendar_date: date, presence: str
) -> DailyStatusRecord:
    """Build a durable absence observation without inventing values."""
    if family not in _FAMILIES or presence not in _PRESENCE - {"present"}:
        raise HistorySchemaError("daily status availability is invalid")
    return _finish(
        family, calendar_date, {}, {}, (), record_presence=presence
    )


def _with_record_key(
    record: DailyStatusRecord, record_key: str
) -> DailyStatusRecord:
    return _finish(
        record.family,
        record.calendar_date,
        record.values,
        record.field_presence,
        record.metrics,
        raw_timestamp=record.source_timestamp,
        record_presence=record.presence,
        record_key=record_key,
    )


def normalize_hrv_status(payload: Any, target_date: date) -> DailyStatusRecord:
    """Normalize Garmin's bounded nightly HRV summary."""
    root = _mapping(payload, "HRV payload")
    summary = _mapping(root.get("hrvSummary", root.get("summary")), "HRV summary")
    values: dict[str, Any] = {}
    presence: dict[str, str] = {}
    metrics: list[DailyStatusMetric] = []
    for source, key, name in (
        ("lastNightAvg", "hrv_last_night_average", "HRV last-night average"),
        ("lastNight5MinHigh", "hrv_last_night_5_min_high", "HRV last-night 5-minute high"),
        ("weeklyAvg", "hrv_weekly_average", "HRV weekly average"),
    ):
        value = _field(summary, source, values, presence)
        _metric(metrics, key, name, "ms", value)
    _field(summary, "status", values, presence, text_value=True)
    _field(summary, "feedbackPhrase", values, presence, text_value=True)
    baseline = _mapping(summary.get("baseline"), "HRV baseline")
    baseline_values: dict[str, Any] = {}
    if baseline:
        values["baseline"] = baseline_values
    for source, key, name in (
        ("lowUpper", "hrv_baseline_low_upper", "HRV baseline low upper"),
        ("balancedLow", "hrv_baseline_balanced_low", "HRV baseline balanced low"),
        ("balancedUpper", "hrv_baseline_balanced_upper", "HRV baseline balanced upper"),
        ("markerValue", "hrv_baseline_marker", "HRV baseline marker"),
    ):
        value = _field(baseline, source, baseline_values, presence)
        _metric(metrics, key, name, "ms", value)
    return _finish(
        "hrv",
        _calendar_date(summary, target_date),
        values,
        presence,
        metrics,
        raw_timestamp=summary.get("createTimeStamp"),
    )


def _device_suffix(device_id: str) -> str:
    return hashlib.sha256(device_id.encode()).hexdigest()[:12]


def normalize_training_daily_status(payload: Any, target_date: date) -> DailyStatusRecord:
    """Normalize device training status, workload, and VO2 status."""
    root = _mapping(payload, "training payload")
    status_root = _mapping(root.get("mostRecentTrainingStatus"), "training status")
    devices = _mapping(status_root.get("latestTrainingStatusData"), "training devices")
    values: dict[str, Any] = {"devices": {}}
    presence: dict[str, str] = {}
    metrics: list[DailyStatusMetric] = []
    returned_dates: list[date] = []
    for raw_id, raw_item in devices.items():
        item = _mapping(raw_item, "training device")
        device_id = str(item.get("deviceId", raw_id))
        if not device_id or len(device_id) > 64:
            raise HistorySchemaError("training device identity is invalid")
        suffix = _device_suffix(device_id)
        device_values: dict[str, Any] = {}
        values["devices"][suffix] = device_values
        returned_dates.append(_calendar_date(item, target_date))
        for source in ("calendarDate", "sinceDate", "trainingStatusFeedbackPhrase"):
            _field(item, source, device_values, presence, text_value=True, presence_name=f"devices.{suffix}.{source}")
        for source in ("trainingStatus", "fitnessTrend"):
            value = _field(item, source, device_values, presence, presence_name=f"devices.{suffix}.{source}")
            if source == "fitnessTrend":
                _metric(metrics, f"training_fitness_trend_{suffix}", "Training fitness trend", "unitless", value)
        status_code = device_values.get("trainingStatus")
        if isinstance(status_code, float) and status_code.is_integer():
            mapped = {
                0: "no_status", 1: "peaking", 2: "maintaining",
                3: "recovering", 4: "unproductive", 5: "detraining",
                6: "peaking", 7: "productive", 8: "strained",
            }.get(int(status_code))
            if mapped is not None:
                device_values["mappedTrainingStatus"] = mapped
        if "trainingPaused" in item:
            paused = item["trainingPaused"]
            if paused is None:
                presence[f"devices.{suffix}.trainingPaused"] = "null"
            elif not isinstance(paused, bool):
                raise HistorySchemaError("trainingPaused has an invalid type")
            else:
                device_values["trainingPaused"] = paused
                presence[f"devices.{suffix}.trainingPaused"] = "present"
        else:
            presence[f"devices.{suffix}.trainingPaused"] = "missing"
        acute = _mapping(item.get("acuteTrainingLoadDTO"), "acute training load")
        acute_values: dict[str, Any] = {}
        if acute:
            device_values["acuteTrainingLoad"] = acute_values
        for source, key, name, unit in (
            ("dailyTrainingLoadAcute", "acute_load", "Training acute load", "load"),
            ("dailyTrainingLoadChronic", "chronic_load", "Training chronic load", "load"),
            ("dailyAcuteChronicWorkloadRatio", "acwr", "Training acute/chronic workload ratio", "ratio"),
            ("acwrPercent", "acwr_percent", "Training ACWR percent", "%"),
            ("minAcrChronicLoadRatio", "acwr_min_target", "Training ACWR minimum target", "ratio"),
            ("maxAcrChronicLoadRatio", "acwr_max_target", "Training ACWR maximum target", "ratio"),
            ("minTrainingLoadChronic", "chronic_load_min_target", "Training chronic-load minimum target", "load"),
            ("maxTrainingLoadChronic", "chronic_load_max_target", "Training chronic-load maximum target", "load"),
        ):
            value = _field(acute, source, acute_values, presence, presence_name=f"devices.{suffix}.acuteTrainingLoad.{source}")
            _metric(metrics, f"training_{key}_{suffix}", name, unit, value)
        for source in ("acwrStatus", "acwrStatusFeedback"):
            _field(acute, source, acute_values, presence, text_value=True, presence_name=f"devices.{suffix}.acuteTrainingLoad.{source}")

    vo2_root = _mapping(root.get("mostRecentVO2Max"), "training VO2")
    vo2_values: dict[str, Any] = {}
    for _raw_key, raw_item in vo2_root.items():
        item = _mapping(raw_item, "training VO2 item")
        raw_device = item.get("deviceId")
        suffix = _device_suffix(str(raw_device)) if raw_device is not None else "generic"
        item_values: dict[str, Any] = {}
        vo2_values[suffix] = item_values
        returned_dates.append(_calendar_date(item, target_date))
        for source in ("calendarDate", "maxMetCategory"):
            _field(item, source, item_values, presence, text_value=True, presence_name=f"vo2Max.{suffix}.{source}")
        for source, key, name in (
            ("vo2MaxValue", "vo2_max", "Training VO2 max"),
            ("vo2MaxPreciseValue", "vo2_max_precise", "Training precise VO2 max"),
        ):
            value = _field(item, source, item_values, presence, presence_name=f"vo2Max.{suffix}.{source}")
            _metric(metrics, f"training_{key}_{suffix}", name, "mL/kg/min", value)
    if vo2_values:
        values["vo2Max"] = vo2_values

    balance_root = _mapping(root.get("mostRecentTrainingLoadBalance"), "training load balance")
    balance_values: dict[str, Any] = {}
    load_balance_fields = frozenset(
        {
            "monthlyLoadAerobicLow", "monthlyLoadAerobicHigh",
            "monthlyLoadAnaerobic", "monthlyLoadAerobicLowTargetMin",
            "monthlyLoadAerobicLowTargetMax", "monthlyLoadAerobicHighTargetMin",
            "monthlyLoadAerobicHighTargetMax", "monthlyLoadAnaerobicTargetMin",
            "monthlyLoadAnaerobicTargetMax", "lowAerobicTargetMin",
            "lowAerobicTargetMax", "highAerobicTargetMin", "highAerobicTargetMax",
            "anaerobicTargetMin", "anaerobicTargetMax", "feedback",
            "trainingLoadBalanceFeedback", "calendarDate",
        }
    )
    if balance_root:
        returned_dates.append(_calendar_date(balance_root, target_date))
    for field_name in load_balance_fields:
        presence[f"loadBalance.{field_name}"] = (
            "missing" if field_name not in balance_root else "null"
            if balance_root[field_name] is None else "present"
        )
    for key, raw in balance_root.items():
        if key not in load_balance_fields:
            continue
        if isinstance(raw, str) or raw is None:
            if isinstance(raw, str):
                balance_values[str(key)] = raw
            continue
        if isinstance(raw, bool) or not isinstance(raw, int | float):
            continue
        value = _number(raw, str(key))
        balance_values[str(key)] = value
        _metric(metrics, f"training_load_balance_{_stat_key(str(key))}", f"Training load balance {key}", "load", value)
    if balance_values:
        values["loadBalance"] = balance_values
    if not values["devices"]:
        del values["devices"]
    presence.update(
        {
            "trainingReadiness": "unsupported",
            "morningTrainingReadiness": "unsupported",
            "recoveryTime": "unsupported",
            "enduranceScore": "unsupported",
            "hillScore": "unsupported",
            "lactateThreshold": "unsupported",
        }
    )
    return _finish(
        "training", max(returned_dates, default=target_date), values, presence, metrics
    )


def normalize_training_daily_records(
    payload: Any, target_date: date
) -> tuple[DailyStatusRecord, ...]:
    """Split training status, VO2, and load balance by source identity."""
    root = _mapping(payload, "training payload")
    records: list[DailyStatusRecord] = []
    try:
        status_root = _mapping(root.get("mostRecentTrainingStatus"), "training status")
        devices = _mapping(status_root.get("latestTrainingStatusData"), "training devices")
    except HistorySchemaError:
        records.append(
            _with_record_key(
                unavailable_daily_status("training", target_date, "failed"),
                "training_status",
            )
        )
        devices = {}
    for raw_id, item in devices.items():
        fallback_suffix = _device_suffix(str(raw_id))
        try:
            device_id = str(_mapping(item, "training device").get("deviceId", raw_id))
            suffix = _device_suffix(device_id)
            record = normalize_training_daily_status(
                {
                    "mostRecentTrainingStatus": {
                        "latestTrainingStatusData": {str(raw_id): item}
                    }
                },
                target_date,
            )
        except HistorySchemaError:
            record = unavailable_daily_status("training", target_date, "failed")
            suffix = fallback_suffix
        records.append(_with_record_key(record, f"training_status:{suffix}"))
    try:
        vo2 = _mapping(root.get("mostRecentVO2Max"), "training VO2")
    except HistorySchemaError:
        records.append(
            _with_record_key(
                unavailable_daily_status("training", target_date, "failed"),
                "training_vo2",
            )
        )
        vo2 = {}
    for raw_key, item in vo2.items():
        fallback_suffix = _device_suffix(str(raw_key))
        try:
            mapped = _mapping(item, "training VO2 item")
            raw_device = mapped.get("deviceId")
            suffix = _device_suffix(str(raw_device)) if raw_device is not None else "generic"
            record = normalize_training_daily_status(
                {"mostRecentVO2Max": {str(raw_key): item}}, target_date
            )
        except HistorySchemaError:
            record = unavailable_daily_status("training", target_date, "failed")
            suffix = fallback_suffix
        records.append(_with_record_key(record, f"training_vo2:{suffix}"))
    if root.get("mostRecentTrainingLoadBalance") is not None:
        try:
            record = normalize_training_daily_status(
                {"mostRecentTrainingLoadBalance": root["mostRecentTrainingLoadBalance"]},
                target_date,
            )
        except HistorySchemaError:
            record = unavailable_daily_status("training", target_date, "failed")
        records.append(_with_record_key(record, "training_load_balance"))
    return tuple(records) or (normalize_training_daily_status({}, target_date),)


def normalize_sleep_daily_status(payload: Any, target_date: date) -> DailyStatusRecord:
    """Normalize sleep scores, sleep need, and bounded daily insights."""
    root = _mapping(payload, "sleep payload")
    daily = _mapping(root.get("dailySleepDTO", root.get("sleepData", root)), "daily sleep")
    values: dict[str, Any] = {}
    presence: dict[str, str] = {}
    metrics: list[DailyStatusMetric] = []
    scores = _mapping(daily.get("sleepScores"), "sleep scores")
    score_values: dict[str, Any] = {}
    for component, raw_component in scores.items():
        component_data = _mapping(raw_component, f"sleep score {component}")
        normalized: dict[str, Any] = {}
        value = _field(component_data, "value", normalized, presence, presence_name=f"sleepScores.{component}.value")
        _metric(metrics, f"sleep_score_{_stat_key(str(component))}", f"Sleep score {component}", "unitless", value)
        for source in ("optimalStart", "optimalEnd", "idealStartInSeconds", "idealEndInSeconds"):
            range_value = _field(component_data, source, normalized, presence, presence_name=f"sleepScores.{component}.{source}")
            _metric(
                metrics,
                f"sleep_score_{_stat_key(str(component))}_{_stat_key(source)}",
                f"Sleep score {component} {source}",
                "s" if "Seconds" in source else "unitless",
                range_value,
            )
        _field(component_data, "qualifierKey", normalized, presence, text_value=True, presence_name=f"sleepScores.{component}.qualifierKey")
        if normalized:
            score_values[str(component)] = normalized
    if score_values:
        values["sleepScores"] = score_values
    for source in ("avgOvernightHrv", "bodyBatteryChange"):
        _field(daily, source, values, presence)
    for source in ("hrvStatus", "sleepScoreFeedback", "sleepScoreInsight", "sleepScorePersonalizedInsight"):
        _field(daily, source, values, presence, text_value=True)
    for need_name in ("sleepNeed", "nextSleepNeed"):
        need = _mapping(daily.get(need_name, root.get(need_name)), need_name)
        need_values: dict[str, Any] = {}
        for source, unit in (
            ("actual", "min"), ("baseline", "min"), ("hrvAdjustment", "min"),
            ("napAdjustment", "min"), ("sleepHistoryAdjustment", "min"),
            ("trainingAdjustment", "min"),
        ):
            value = _field(need, source, need_values, presence, presence_name=f"{need_name}.{source}")
            _metric(metrics, f"sleep_{_stat_key(need_name)}_{_stat_key(source)}", f"Sleep {need_name} {source}", unit, value)
        for source in ("calendarDate", "feedback", "trainingFeedback"):
            _field(need, source, need_values, presence, text_value=True, presence_name=f"{need_name}.{source}")
        for source in (
            "timestampGmt", "recommendedBedtimeStartTimestampGmt",
            "recommendedBedtimeEndTimestampGmt",
        ):
            _field(need, source, need_values, presence, text_value=True, presence_name=f"{need_name}.{source}")
        for source in (
            "recommendedBedtimeStartMins", "recommendedBedtimeEndMins"
        ):
            value = _field(need, source, need_values, presence, presence_name=f"{need_name}.{source}")
            _metric(
                metrics,
                f"sleep_{_stat_key(need_name)}_{_stat_key(source)}",
                f"Sleep {need_name} {source}",
                "min",
                value,
            )
        if "displayedForTheDay" in need:
            displayed = need["displayedForTheDay"]
            if displayed is None:
                presence[f"{need_name}.displayedForTheDay"] = "null"
            elif not isinstance(displayed, bool):
                raise HistorySchemaError("displayedForTheDay has an invalid type")
            else:
                need_values["displayedForTheDay"] = displayed
                presence[f"{need_name}.displayedForTheDay"] = "present"
        else:
            presence[f"{need_name}.displayedForTheDay"] = "missing"
        tracker = need.get("preferredActivityTracker")
        if "preferredActivityTracker" not in need:
            presence[f"{need_name}.preferredActivityTracker"] = "missing"
        elif tracker is None:
            presence[f"{need_name}.preferredActivityTracker"] = "null"
        else:
            if isinstance(tracker, bool) or not isinstance(tracker, str | int):
                raise HistorySchemaError("preferredActivityTracker has an invalid type")
            need_values["preferredActivityTracker"] = hashlib.sha256(
                str(tracker).encode()
            ).hexdigest()[:12]
            presence[f"{need_name}.preferredActivityTracker"] = "present"
        bedtime = need_values.get("recommendedBedtimeStartMins")
        actual = need_values.get("actual")
        if isinstance(bedtime, float) and isinstance(actual, float):
            need_values["derivedRecommendedWakeMins"] = bedtime + actual
        if need_values:
            values[need_name] = need_values
    return _finish(
        "sleep",
        _calendar_date(daily, target_date),
        values,
        presence,
        metrics,
        raw_timestamp=daily.get("sleepEndTimestampGMT", daily.get("calendarDate")),
    )


def normalize_sleep_daily_records(
    payload: Any, target_date: date
) -> tuple[DailyStatusRecord, ...]:
    """Split the sleep summary and both sleep-need snapshots."""
    root = _mapping(payload, "sleep payload")
    daily = dict(
        _mapping(root.get("dailySleepDTO", root.get("sleepData", root)), "daily sleep")
    )
    base = dict(daily)
    base.pop("sleepNeed", None)
    base.pop("nextSleepNeed", None)
    try:
        summary = normalize_sleep_daily_status({"dailySleepDTO": base}, target_date)
    except HistorySchemaError:
        summary = unavailable_daily_status("sleep", target_date, "failed")
    records = [_with_record_key(summary, "sleep_summary")]
    for need_name in ("sleepNeed", "nextSleepNeed"):
        if need_name in daily:
            raw_need = daily[need_name]
        elif need_name in root:
            raw_need = root[need_name]
        else:
            continue
        if raw_need is None:
            records.append(
                _with_record_key(
                    unavailable_daily_status("sleep", target_date, "null"),
                    f"sleep_{_stat_key(need_name)}",
                )
            )
            continue
        try:
            need = _mapping(raw_need, need_name)
            envelope = {
                "calendarDate": need.get("calendarDate", daily.get("calendarDate")),
                "sleepEndTimestampGMT": need.get("timestampGmt"),
                need_name: raw_need,
            }
            record = normalize_sleep_daily_status(
                {"dailySleepDTO": envelope}, target_date
            )
        except HistorySchemaError:
            record = unavailable_daily_status("sleep", target_date, "failed")
        records.append(_with_record_key(record, f"sleep_{_stat_key(need_name)}"))
    return tuple(records)


def normalize_fitness_age_status(payload: Any, target_date: date) -> DailyStatusRecord:
    """Normalize Garmin fitness-age values and known components."""
    root = _mapping(payload, "fitness age payload")
    values: dict[str, Any] = {}
    presence: dict[str, str] = {}
    metrics: list[DailyStatusMetric] = []
    for source in (
        "chronologicalAge", "fitnessAge", "achievableFitnessAge",
        "previousFitnessAge", "metabolicAge",
    ):
        value = _field(root, source, values, presence)
        _metric(metrics, f"fitness_age_{_stat_key(source)}", f"Fitness age {source}", "years", value)
    visceral_fat = _field(root, "visceralFat", values, presence)
    _metric(
        metrics, "fitness_age_visceral_fat", "Fitness age visceral fat",
        "unitless", visceral_fat,
    )
    _field(root, "lastUpdated", values, presence, text_value=True)
    physique = root.get("physiqueRating")
    if "physiqueRating" not in root:
        presence["physiqueRating"] = "missing"
    elif physique is None:
        presence["physiqueRating"] = "null"
    else:
        if isinstance(physique, bool) or not isinstance(physique, str | int):
            raise HistorySchemaError("physiqueRating has an invalid type")
        values["physiqueRating"] = str(physique)
        presence["physiqueRating"] = "present"
    components: dict[str, Any] = {}
    for component in ("bodyFat", "rhr", "vigorousDaysAvg", "vigorousMinutesAvg"):
        data = _mapping(root.get(component), f"fitness age {component}")
        normalized: dict[str, Any] = {}
        for source in ("value", "targetValue", "potentialAge", "improvement", "priority", "weeks"):
            value = _field(data, source, normalized, presence, presence_name=f"components.{component}.{source}")
            _metric(metrics, f"fitness_age_{_stat_key(component)}_{_stat_key(source)}", f"Fitness age {component} {source}", "unitless", value)
        for source in ("date",):
            _field(data, source, normalized, presence, text_value=True, presence_name=f"components.{component}.{source}")
        stale_key = f"components.{component}.stale"
        if "stale" not in data:
            presence[stale_key] = "missing"
        else:
            stale = data["stale"]
            if stale is None:
                presence[stale_key] = "null"
            elif not isinstance(stale, bool):
                raise HistorySchemaError("fitness age stale has an invalid type")
            else:
                normalized["stale"] = stale
                presence[stale_key] = "present"
        if normalized:
            components[component] = normalized
    if components:
        values["components"] = components
    return _finish("fitness_age", target_date, values, presence, metrics, raw_timestamp=root.get("lastUpdated"))


def normalize_stress_daily_status(payload: Any, target_date: date) -> DailyStatusRecord:
    """Normalize daily stress summary and convert source seconds to minutes."""
    root = _mapping(payload, "daily summary payload")
    values: dict[str, Any] = {}
    presence: dict[str, str] = {}
    metrics: list[DailyStatusMetric] = []
    for source, key, name in (
        ("averageStressLevel", "stress_daily_average", "Daily average stress"),
        ("maxStressLevel", "stress_daily_maximum", "Daily maximum stress"),
    ):
        value = _field(root, source, values, presence)
        _metric(metrics, key, name, "unitless", value)
    total_source = (
        "totalStressDuration" if "totalStressDuration" in root else "stressDuration"
    )
    total_seconds = _field(root, total_source, values, presence)
    _metric(
        metrics,
        "stress_total_stress_duration_minutes",
        "Stress total duration minutes",
        "min",
        None if total_seconds is None else float(total_seconds) / 60,
    )
    for source in (
        "restStressDuration", "activityStressDuration",
        "lowStressDuration", "mediumStressDuration", "highStressDuration",
        "uncategorizedStressDuration",
    ):
        seconds = _field(root, source, values, presence)
        _metric(metrics, f"stress_{_stat_key(source)}_minutes", f"Stress {source} minutes", "min", None if seconds is None else float(seconds) / 60)
    _field(root, "stressQualifier", values, presence, text_value=True)
    return _finish(
        "stress", _calendar_date(root, target_date), values, presence, metrics
    )


def daily_status_record(record: DailyStatusRecord) -> dict[str, Any]:
    """Serialize one validated daily status record."""
    return {
        "family": record.family,
        "record_key": record.record_key,
        "calendar_date": record.calendar_date.isoformat(),
        "source_timestamp": (
            record.source_timestamp.astimezone(UTC).isoformat()
            if record.source_timestamp is not None
            else None
        ),
        "statistic_timestamp": record.statistic_timestamp.astimezone(UTC).isoformat(),
        "presence": record.presence,
        "values": record.values,
        "field_presence": record.field_presence,
        "metrics": [
            {"key": item.key, "name": item.name, "unit": item.unit, "value": item.value}
            for item in record.metrics
        ],
        "revision": record.revision,
        "projected_revision": record.projected_revision,
    }


def daily_status_from_record(raw: Mapping[str, Any]) -> DailyStatusRecord:
    """Validate and restore a daily status record."""
    try:
        family = raw["family"]
        record_key = raw["record_key"]
        calendar_date = date.fromisoformat(raw["calendar_date"])
        source_timestamp = (
            datetime.fromisoformat(raw["source_timestamp"])
            if raw.get("source_timestamp") is not None
            else None
        )
        statistic_timestamp = datetime.fromisoformat(raw["statistic_timestamp"])
        values = raw["values"]
        field_presence = raw["field_presence"]
        raw_metrics = raw["metrics"]
        revision = raw["revision"]
        projected = raw.get("projected_revision")
    except (KeyError, TypeError, ValueError) as err:
        raise HistorySchemaError("daily status record is invalid") from err
    if (
        family not in _FAMILIES
        or not isinstance(record_key, str)
        or not record_key.startswith(family)
        or (source_timestamp is not None and source_timestamp.tzinfo is None)
        or (
            source_timestamp is not None
            and source_timestamp.astimezone(UTC).isoformat() != raw["source_timestamp"]
        )
        or statistic_timestamp.tzinfo is None
        or statistic_timestamp.astimezone(UTC).isoformat() != raw["statistic_timestamp"]
        or statistic_timestamp.astimezone(UTC)
        != _statistic_timestamp(
            calendar_date,
            source_timestamp.astimezone(UTC) if source_timestamp is not None else None,
        )
        or raw.get("presence") not in _PRESENCE
        or not isinstance(values, dict)
        or not isinstance(field_presence, dict)
        or not isinstance(raw_metrics, list)
        or not isinstance(revision, str)
        or len(revision) != 24
        or (projected is not None and projected != revision)
    ):
        raise HistorySchemaError("daily status record is invalid")
    metrics: list[DailyStatusMetric] = []
    try:
        for item in raw_metrics:
            value = _number(item["value"], "daily status metric")
            if value is None:
                raise HistorySchemaError("daily status metric is invalid")
            metrics.append(DailyStatusMetric(item["key"], item["name"], item["unit"], value))
    except (KeyError, TypeError) as err:
        raise HistorySchemaError("daily status record is invalid") from err
    restored = DailyStatusRecord(
        family,
        record_key,
        calendar_date,
        source_timestamp.astimezone(UTC) if source_timestamp is not None else None,
        statistic_timestamp.astimezone(UTC),
        raw["presence"],
        values,
        field_presence,
        tuple(metrics),
        revision,
        projected,
    )
    expected = _finish(
        family,
        calendar_date,
        values,
        field_presence,
        metrics,
        raw_timestamp=source_timestamp,
        record_presence=raw["presence"],
        record_key=record_key,
    )
    if expected.revision != revision:
        raise HistorySchemaError("daily status record is inconsistent")
    return restored


class DailyStatusStore:
    """Account-scoped annual Store partitions with idempotent revisions."""

    def __init__(self, hass: Any, entry_id: str, account_key: str, store_factory: Callable[..., Any] | None = None) -> None:
        self._hass = hass
        self._entry_id = entry_id
        self._account_key = account_key
        self._store_factory = store_factory
        self._stores: dict[str, Any] = {}
        self._records: dict[str, dict[str, dict[str, DailyStatusRecord]]] = {}

    def _store(self, year: str) -> Any:
        if year not in self._stores:
            factory = self._store_factory
            if factory is None:
                from homeassistant.helpers.storage import Store
                factory = Store
            self._stores[year] = factory(
                self._hass,
                HISTORY_STORE_VERSION,
                f"{DOMAIN}.{self._entry_id}.daily_status_{year}",
                private=True,
                atomic_writes=True,
            )
        return self._stores[year]

    async def async_load_year(self, year: int | str) -> None:
        year_key = str(year)
        if year_key in self._records:
            return
        payload = await self._store(year_key).async_load()
        if payload is None:
            self._records[year_key] = {}
            return
        if (
            not isinstance(payload, Mapping)
            or payload.get("account_key") != self._account_key
            or payload.get("year") != year_key
            or payload.get("daily_status_schema_version") != DAILY_STATUS_SCHEMA_VERSION
            or not isinstance(payload.get("dates"), Mapping)
        ):
            raise HistorySchemaError("daily status partition is invalid")
        dates: dict[str, dict[str, DailyStatusRecord]] = {}
        for date_key, families in payload["dates"].items():
            parsed_date = date.fromisoformat(date_key)
            if str(parsed_date.year) != year_key or not isinstance(families, Mapping):
                raise HistorySchemaError("daily status partition is invalid")
            parsed_families: dict[str, DailyStatusRecord] = {}
            for record_key, record in families.items():
                restored = daily_status_from_record(record)
                if (
                    restored.record_key != record_key
                    or restored.calendar_date != parsed_date
                ):
                    raise HistorySchemaError("daily status partition is invalid")
                parsed_families[record_key] = restored
            dates[date_key] = parsed_families
        self._records[year_key] = dates

    async def _async_save_year(self, year: str) -> None:
        dates = self._records.get(year, {})
        await self._store(year).async_save({
            "schema_version": HISTORY_STORE_VERSION,
            "daily_status_schema_version": DAILY_STATUS_SCHEMA_VERSION,
            "account_key": self._account_key,
            "year": year,
            "dates": {
                date_key: {family: daily_status_record(record) for family, record in families.items()}
                for date_key, families in dates.items()
            },
        })

    async def async_upsert(self, records: Sequence[DailyStatusRecord]) -> tuple[DailyStatusRecord, ...]:
        changed_years: set[str] = set()
        retained: list[DailyStatusRecord] = []
        for incoming in records:
            year = str(incoming.calendar_date.year)
            await self.async_load_year(year)
            date_key = incoming.calendar_date.isoformat()
            families = self._records[year].setdefault(date_key, {})
            existing = families.get(incoming.record_key)
            if existing is not None and existing.presence == "present" and incoming.presence != "present":
                retained.append(existing)
                continue
            if existing is not None and existing.revision == incoming.revision:
                retained.append(existing)
                continue
            families[incoming.record_key] = incoming
            changed_years.add(year)
            retained.append(incoming)
        for year in changed_years:
            await self._async_save_year(year)
        return tuple(retained)

    async def async_mark_projected(self, record: DailyStatusRecord) -> DailyStatusRecord:
        year = str(record.calendar_date.year)
        await self.async_load_year(year)
        current = self._records[year].get(record.calendar_date.isoformat(), {}).get(record.record_key)
        if current is None or current.revision != record.revision:
            raise HistorySchemaError("daily status projection revision changed")
        projected = replace(current, projected_revision=current.revision)
        self._records[year][record.calendar_date.isoformat()][record.record_key] = projected
        await self._async_save_year(year)
        return projected

    async def async_get_range(self, start: date, end: date) -> tuple[DailyStatusRecord, ...]:
        for year in range(start.year, end.year + 1):
            await self.async_load_year(year)
        result = [
            record
            for dates in self._records.values()
            for date_key, families in dates.items()
            if start <= date.fromisoformat(date_key) <= end
            for record in families.values()
        ]
        return tuple(
            sorted(result, key=lambda item: (item.calendar_date, item.record_key))
        )
