from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from radar.alerts.chart import render_anomaly_chart, render_spread_chart
from radar.alerts.models import AnomalyAlertDetails, FundingContext, SpreadAlertDetails
from radar.alerts.telegram import TelegramTransport
from radar.config import AnomalyV2Config
from radar.history.spread import (
    AnomalyConfirmationContext,
    HistoricalSpreadContext,
    SpreadHistory,
    WindowStats,
)
from radar.monitors.base import AlertRequest, JSONValue
from radar.storage.sqlite import SQLiteRuntimeStore

SUPPORTED_SIZES = frozenset({1_000, 5_000, 10_000})
LOGGER = logging.getLogger(__name__)
ANOMALY_CONTEXT_QUERY_ATTEMPTS = 3
ANOMALY_CONTEXT_QUERY_DELAY_SECONDS = 0.05
ANOMALY_NOTIFICATION_STATE_KEY = "anomaly_notifications_v2"
ANOMALY_NOTIFICATION_CLUSTERS_STATE_KEY = "anomaly_notification_clusters_v2"


def _notification_event_id(source_event_id: str, suffix: str) -> str:
    return f"{source_event_id}:anomaly_notification:{suffix}:{uuid4().hex}"


def _require_text(payload: Mapping[str, JSONValue], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty")
    return value


def _require_number(
    payload: Mapping[str, JSONValue],
    field_name: str,
    *,
    positive: bool = False,
    non_negative: bool = False,
) -> float:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    if positive and result <= 0:
        raise ValueError(f"{field_name} must be positive")
    if non_negative and result < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return result


def _optional_number(
    payload: Mapping[str, JSONValue],
    field_name: str,
    *,
    non_negative: bool = False,
) -> float | None:
    if payload.get(field_name) is None:
        return None
    return _require_number(
        payload,
        field_name,
        non_negative=non_negative,
    )


def _require_timestamp(payload: Mapping[str, JSONValue], field_name: str) -> datetime:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _optional_timestamp(
    payload: Mapping[str, JSONValue], field_name: str
) -> datetime | None:
    value = payload.get(field_name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a timestamp string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _require_duration(payload: Mapping[str, JSONValue], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field_name} must be an integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _parse_funding(
    value: JSONValue,
    *,
    expected_venue: str,
    expected_venue_symbol: str,
    expected_canonical_symbol: str,
) -> FundingContext | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise TypeError("funding context must be an object or null")
    if value.get("venue") != expected_venue:
        raise ValueError("funding venue does not match alert identity")
    if value.get("venue_symbol") != expected_venue_symbol:
        raise ValueError("funding venue symbol does not match alert identity")
    if value.get("canonical_symbol") != expected_canonical_symbol:
        raise ValueError("funding canonical symbol does not match alert identity")
    return FundingContext(
        effective_time=_require_timestamp(value, "effective_time"),
        observed_at=_require_timestamp(value, "observed_at"),
        funding_rate=_require_number(value, "funding_rate"),
        next_funding_time=_optional_timestamp(value, "next_funding_time"),
    )


def parse_spread_alert(alert: AlertRequest) -> SpreadAlertDetails:
    if alert.monitor != "spread":
        raise ValueError("alert monitor must be spread")
    payload = alert.payload
    canonical_symbol = _require_text(payload, "canonical_symbol")
    long_venue = _require_text(payload, "long_venue")
    long_venue_symbol = _require_text(payload, "long_venue_symbol")
    short_venue = _require_text(payload, "short_venue")
    short_venue_symbol = _require_text(payload, "short_venue_symbol")

    size = payload.get("primary_size_usd")
    if isinstance(size, bool) or not isinstance(size, int):
        raise TypeError("primary_size_usd must be an integer")
    if size not in SUPPORTED_SIZES:
        raise ValueError("primary_size_usd must be 1000, 5000, or 10000")

    funding_value = payload.get("funding_context")
    if funding_value is None:
        funding_payload: Mapping[str, JSONValue] = {}
    elif isinstance(funding_value, dict):
        funding_payload = funding_value
    else:
        raise TypeError("funding_context must be an object or null")

    return SpreadAlertDetails(
        canonical_symbol=canonical_symbol,
        long_venue=long_venue,
        long_venue_symbol=long_venue_symbol,
        short_venue=short_venue,
        short_venue_symbol=short_venue_symbol,
        primary_size_usd=size,
        long_buy_vwap=_require_number(payload, "long_buy_vwap", positive=True),
        short_sell_vwap=_require_number(payload, "short_sell_vwap", positive=True),
        raw_spread_bps=_require_number(payload, "raw_spread_bps"),
        long_fee_bps=_optional_number(payload, "long_fee_bps", non_negative=True),
        short_fee_bps=_optional_number(payload, "short_fee_bps", non_negative=True),
        net_spread_bps=_optional_number(payload, "net_spread_bps"),
        rolling_mean_bps=_require_number(payload, "rolling_mean_bps"),
        rolling_std_bps=_require_number(
            payload,
            "rolling_std_bps",
            non_negative=True,
        ),
        deviation_bps=_require_number(payload, "deviation_bps"),
        signal_duration_seconds=_require_duration(
            payload, "signal_duration_seconds"
        ),
        observed_at_skew_seconds=_require_number(
            payload,
            "observed_at_skew_seconds",
            non_negative=True,
        ),
        round_trip_fee_bps=_optional_number(
            payload,
            "round_trip_fee_bps",
            non_negative=True,
        ),
        theoretical_edge_bps=_optional_number(payload, "theoretical_edge_bps"),
        sample_time=_require_timestamp(payload, "sample_time"),
        candidate_duration_seconds=_require_duration(
            payload, "candidate_duration_seconds"
        ),
        alert_duration_seconds=_require_duration(payload, "alert_duration_seconds"),
        long_funding=_parse_funding(
            funding_payload.get("long"),
            expected_venue=long_venue,
            expected_venue_symbol=long_venue_symbol,
            expected_canonical_symbol=canonical_symbol,
        ),
        short_funding=_parse_funding(
            funding_payload.get("short"),
            expected_venue=short_venue,
            expected_venue_symbol=short_venue_symbol,
            expected_canonical_symbol=canonical_symbol,
        ),
    )


ANOMALY_EVENT_KINDS = frozenset(
    {"anomaly_initial", "anomaly_expansion", "anomaly_return", "anomaly_resolved"}
)


def parse_anomaly_alert(alert: AlertRequest) -> AnomalyAlertDetails:
    if alert.monitor != "spread":
        raise ValueError("alert monitor must be spread")
    payload = alert.payload
    event_kind = payload.get("event_kind")
    if event_kind not in ANOMALY_EVENT_KINDS:
        raise ValueError("unknown anomaly event kind")
    size = payload.get("primary_size_usd")
    if isinstance(size, bool) or not isinstance(size, int) or size not in SUPPORTED_SIZES:
        raise ValueError("primary_size_usd must be 1000, 5000, or 10000")
    raw_resolution_reason = payload.get("resolution_reason")
    resolution_reason = (
        raw_resolution_reason if isinstance(raw_resolution_reason, str) else None
    )
    return AnomalyAlertDetails(
        event_kind=event_kind,
        episode_id=_require_text(payload, "episode_id"),
        canonical_symbol=_require_text(payload, "canonical_symbol"),
        long_venue=_require_text(payload, "long_venue"),
        long_venue_symbol=_require_text(payload, "long_venue_symbol"),
        short_venue=_require_text(payload, "short_venue"),
        short_venue_symbol=_require_text(payload, "short_venue_symbol"),
        primary_size_usd=size,
        sample_time=_require_timestamp(payload, "sample_time"),
        candidate_started_at=_require_timestamp(payload, "candidate_started_at"),
        confirmed_at=_optional_timestamp(payload, "confirmed_at"),
        reference_mean_bps=_require_number(payload, "reference_mean_bps"),
        reference_std_bps=_require_number(
            payload, "reference_std_bps", non_negative=True
        ),
        confirmation_spread_bps=_optional_number(payload, "confirmation_spread_bps"),
        confirmation_deviation_bps=_optional_number(
            payload, "confirmation_deviation_bps"
        ),
        lifetime_peak_spread_bps=_require_number(
            payload, "lifetime_peak_spread_bps"
        ),
        lifetime_peak_deviation_bps=_require_number(
            payload, "lifetime_peak_deviation_bps"
        ),
        lifetime_peak_at=_require_timestamp(payload, "lifetime_peak_at"),
        post_confirmation_peak_spread_bps=_optional_number(
            payload, "post_confirmation_peak_spread_bps"
        ),
        post_confirmation_peak_deviation_bps=_optional_number(
            payload, "post_confirmation_peak_deviation_bps"
        ),
        post_confirmation_peak_at=_optional_timestamp(
            payload, "post_confirmation_peak_at"
        ),
        current_spread_bps=_require_number(payload, "current_spread_bps"),
        current_deviation_bps=_require_number(payload, "current_deviation_bps"),
        current_live_mean_bps=_optional_number(payload, "current_live_mean_bps"),
        current_live_std_bps=_optional_number(
            payload, "current_live_std_bps", non_negative=True
        ),
        ended_at=_optional_timestamp(payload, "ended_at"),
        end_spread_bps=_optional_number(payload, "end_spread_bps"),
        end_deviation_bps=_optional_number(payload, "end_deviation_bps"),
        resolution_reason=resolution_reason,
        long_buy_vwap=_require_number(payload, "long_buy_vwap", positive=True),
        short_sell_vwap=_require_number(payload, "short_sell_vwap", positive=True),
        raw_spread_bps=_require_number(payload, "raw_spread_bps"),
        long_fee_bps=_optional_number(payload, "long_fee_bps", non_negative=True),
        short_fee_bps=_optional_number(payload, "short_fee_bps", non_negative=True),
        net_spread_bps=_optional_number(payload, "net_spread_bps"),
        observed_at_skew_seconds=_require_number(
            payload, "observed_at_skew_seconds", non_negative=True
        ),
    )


def _format_funding(
    venue: str,
    funding: FundingContext | None,
) -> str:
    if funding is None:
        return f"{_display_venue(venue)} 不可用"
    return (
        f"{_display_venue(venue)} {funding.funding_rate * 100:+.4f}%"
        f" @ {_format_timestamp(funding.effective_time)}"
    )


def _display_venue(venue: str) -> str:
    return " ".join(part.capitalize() for part in venue.split("_"))


def _format_price(value: float) -> str:
    magnitude = abs(value)
    if magnitude >= 100:
        precision = 2
    elif magnitude >= 1:
        precision = 4
    elif magnitude >= 0.01:
        precision = 6
    else:
        precision = 8
    return f"{value:.{precision}f}"


def _format_bps(value: float | None, *, signed: bool = False) -> str:
    if value is None:
        return "不可用"
    return f"{value:+.2f}" if signed else f"{value:.2f}"


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_stats(label: str, stats: WindowStats) -> str:
    if stats.median_raw_spread_bps is None:
        return f"{label} 不可用（樣本數 {stats.sample_count}）"
    return (
        f"{label} {stats.median_raw_spread_bps:.2f} bps"
        f"（樣本數 {stats.sample_count}）"
    )


def format_spread_alert(
    details: SpreadAlertDetails,
    context: HistoricalSpreadContext,
    *,
    candidate_net_bps: float = 10.0,
    alert_net_bps: float = 20.0,
) -> str:
    long_venue = _display_venue(details.long_venue)
    short_venue = _display_venue(details.short_venue)
    return "\n\n".join(
        (
            "\n".join(
                (
                    f"價差警報 | {details.canonical_symbol} | ${details.primary_size_usd:,}",
                    f"做多: {long_venue} ({details.long_venue_symbol})",
                    f"做空: {short_venue} ({details.short_venue_symbol})",
                    (
                        f"原始價差: {details.raw_spread_bps:.2f} bps | "
                        f"淨價差: {_format_bps(details.net_spread_bps)} bps"
                    ),
                    (
                        f"24h 均值: {details.rolling_mean_bps:.2f} bps | "
                        f"Std: {details.rolling_std_bps:.2f} bps | "
                        f"偏離: {details.deviation_bps:+.2f} bps"
                    ),
                    (
                        f"訊號持續: {details.signal_duration_seconds}s | "
                        f"手續費: {long_venue} {_format_bps(details.long_fee_bps)} + "
                        f"{short_venue} {_format_bps(details.short_fee_bps)} bps"
                    ),
                    (
                        f"往返手續費: {_format_bps(details.round_trip_fee_bps)} bps | "
                        f"理論均值回歸邊際: "
                        f"{_format_bps(details.theoretical_edge_bps, signed=True)} bps"
                    ),
                )
            ),
            "\n".join(
                (
                    f"買入 VWAP: {_format_price(details.long_buy_vwap)} | "
                    f"賣出 VWAP: {_format_price(details.short_sell_vwap)}",
                    f"樣本時間: {_format_timestamp(details.sample_time)}",
                )
            ),
            "\n".join(
                (
                    f"觀測偏差: {details.observed_at_skew_seconds:.2f}s",
                    (
                        "資金費率: "
                        f"{_format_funding(details.long_venue, details.long_funding)} | "
                        f"{_format_funding(details.short_venue, details.short_funding)}"
                    ),
                )
            ),
            (
                "歷史原始價差中位數: "
                f"{_format_stats('7日', context.stats_7d)} | "
                f"{_format_stats('30日', context.stats_30d)} | "
                f"{_format_stats('90日', context.stats_90d)}"
            ),
        )
    )


def _format_duration(seconds: float | None) -> str:
    if seconds is None:
        return "—"
    total = max(0, int(seconds))
    minutes, remaining = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {remaining:02d}s"


def _anomaly_alignment(context: AnomalyConfirmationContext) -> float | None:
    values = [
        stats.mean_bps
        for stats in (
            context.stats_12h,
            context.stats_24h,
            context.stats_48h,
        )
    ]
    if any(value is None for value in values):
        return None
    numeric = [float(value) for value in values if value is not None]
    return max(numeric) - min(numeric)


def _format_anomaly_context(context: AnomalyConfirmationContext) -> str:
    return " / ".join(
        _format_bps(stats.mean_bps, signed=True)
        for stats in (context.stats_12h, context.stats_24h, context.stats_48h)
    )


def format_anomaly_alert(
    details: AnomalyAlertDetails,
    context: AnomalyConfirmationContext,
    *,
    event_kind: str | None = None,
) -> str:
    """Format one lifecycle event; no strategy decision is made here."""
    kind = details.event_kind if event_kind is None else event_kind
    long_venue = _display_venue(details.long_venue)
    short_venue = _display_venue(details.short_venue)
    duration = _format_duration(
        (
            (details.confirmed_at - details.candidate_started_at).total_seconds()
            if details.confirmed_at is not None
            else None
        )
    )
    alignment = _anomaly_alignment(context)
    alignment_text = _format_bps(alignment, signed=False)
    if kind == "anomaly_expansion":
        expansion = (
            (details.post_confirmation_peak_deviation_bps or 0.0)
            - (details.confirmation_deviation_bps or 0.0)
        )
        heading = f"🔺 {details.canonical_symbol} Basis Expanded · {duration}"
        body = (
            f"Initial deviation    {_format_bps(details.confirmation_deviation_bps, signed=True)} bps\n"
            f"Current deviation    {_format_bps(details.current_deviation_bps, signed=True)} bps\n"
            f"Post-confirm peak    {_format_bps(details.post_confirmation_peak_deviation_bps, signed=True)} bps\n"
            f"Expansion            {expansion:+.2f} bps"
        )
    elif kind == "anomaly_return":
        heading = f"🟢 {details.canonical_symbol} Returning to Mean"
        end_time = details.ended_at or details.sample_time
        body = (
            f"Reference mean        {_format_bps(details.reference_mean_bps, signed=True)} bps\n"
            f"Peak deviation        {_format_bps(details.post_confirmation_peak_deviation_bps, signed=True)} bps\n"
            f"Current deviation     {_format_bps(details.current_deviation_bps, signed=True)} bps\n"
            f"Episode duration      {_format_duration((end_time - details.candidate_started_at).total_seconds())}"
        )
    else:
        heading = f"🔴 {details.canonical_symbol} Basis Anomaly · {duration}"
        body = (
            f"Spread               {_format_bps(details.current_spread_bps, signed=True)} bps\n"
            f"Reference mean       {_format_bps(details.reference_mean_bps, signed=True)} bps\n"
            f"Deviation            {_format_bps(details.current_deviation_bps, signed=True)} bps\n\n"
            f"12h / 24h / 48h\n{_format_anomaly_context(context)} bps\n\n"
            f"Mean alignment       {alignment_text} bps\n"
            f"Reference std        {_format_bps(details.reference_std_bps)} bps\n"
            f"RT fee               {_format_bps((details.long_fee_bps or 0.0) + (details.short_fee_bps or 0.0))} bps"
        )
    return "\n\n".join(
        (
            heading,
            f"Long {long_venue} ({details.long_venue_symbol})\n"
            f"Short {short_venue} ({details.short_venue_symbol})",
            body,
            f"Buy VWAP {_format_price(details.long_buy_vwap)} · "
            f"Sell VWAP {_format_price(details.short_sell_vwap)}\n"
            f"Raw {_format_bps(details.raw_spread_bps, signed=True)} bps · "
            f"Net {_format_bps(details.net_spread_bps, signed=True)} bps\n"
            f"Sample {_format_timestamp(details.sample_time)}",
        )
    )


def _context_to_payload(context: AnomalyConfirmationContext) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {}
    for name, stats in (
        ("12h", context.stats_12h),
        ("24h", context.stats_24h),
        ("48h", context.stats_48h),
    ):
        payload.update(
            {
                f"{name}_sample_count": stats.sample_count,
                f"{name}_mean_bps": stats.mean_bps,
                f"{name}_std_bps": stats.std_bps,
                f"{name}_min_bps": stats.min_bps,
                f"{name}_max_bps": stats.max_bps,
                f"{name}_eligible": stats.eligible,
            }
        )
    return payload


def _context_from_payload(payload: Mapping[str, object]) -> AnomalyConfirmationContext:
    from radar.history.spread import AnomalyWindowStats

    stats: list[AnomalyWindowStats] = []
    for name in ("12h", "24h", "48h"):
        count = payload.get(f"{name}_sample_count")
        eligible = payload.get(f"{name}_eligible")
        if isinstance(count, bool) or not isinstance(count, int) or not isinstance(eligible, bool):
            raise ValueError("invalid persisted anomaly context")
        values: list[float | None] = []
        for field in ("mean_bps", "std_bps", "min_bps", "max_bps"):
            value = payload.get(f"{name}_{field}")
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError("invalid persisted anomaly context")
            values.append(None if value is None else float(value))
        stats.append(
            AnomalyWindowStats(
                sample_count=count,
                mean_bps=values[0],
                std_bps=values[1],
                min_bps=values[2],
                max_bps=values[3],
                eligible=eligible,
            )
        )
    return AnomalyConfirmationContext(*stats)


def _persisted_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


class AnomalyNotificationStore:
    """Small persisted delivery-state wrapper for v2 notifications."""

    def __init__(self, runtime_store: SQLiteRuntimeStore | None) -> None:
        self._runtime_store = runtime_store
        raw = (
            runtime_store.get_monitor_state("spread", ANOMALY_NOTIFICATION_STATE_KEY)
            if runtime_store is not None
            else None
        )
        self._state: dict[str, dict[str, JSONValue]] = {
            str(key): dict(value)
            for key, value in raw.items()
            if isinstance(raw, dict) and isinstance(value, dict)
        } if isinstance(raw, dict) else {}
        raw_clusters = (
            runtime_store.get_monitor_state(
                "spread", ANOMALY_NOTIFICATION_CLUSTERS_STATE_KEY
            )
            if runtime_store is not None
            else None
        )
        self._clusters: dict[str, dict[str, JSONValue]] = {
            str(key): dict(value)
            for key, value in raw_clusters.items()
            if isinstance(raw_clusters, dict) and isinstance(value, dict)
        } if isinstance(raw_clusters, dict) else {}

    def get(self, episode_id: str) -> dict[str, JSONValue]:
        return dict(self._state.get(episode_id, {}))

    def set(self, episode_id: str, value: Mapping[str, JSONValue], *, now: datetime) -> None:
        next_state = dict(self._state)
        next_state[episode_id] = dict(value)
        if self._runtime_store is not None:
            self._runtime_store.set_monitor_state(
                "spread",
                ANOMALY_NOTIFICATION_STATE_KEY,
                next_state,
                updated_at=now,
            )
        self._state = next_state

    def active_cooldown(
        self,
        symbol: str,
        *,
        now: datetime,
        cooldown_seconds: int,
    ) -> tuple[datetime, dict[str, JSONValue]] | None:
        if cooldown_seconds <= 0:
            return None
        entry = self._clusters.get(symbol)
        if entry is None:
            return None
        sent_at = _persisted_timestamp(entry.get("last_nonreturn_sent_at"))
        if sent_at is None:
            return None
        until = sent_at + timedelta(seconds=cooldown_seconds)
        if now >= until:
            return None
        return until, dict(entry)

    def record_suppression(
        self,
        episode_id: str,
        notification: Mapping[str, JSONValue],
        *,
        symbol: str,
        lifecycle_event_kind: str,
        delivery_kind: str,
        source_event_id: str,
        suppressing_episode_id: str | None,
        suppressing_event_id: str | None,
        suppressed_at: datetime,
        suppressed_until: datetime,
        event_id: str,
    ) -> None:
        next_notification = dict(notification)
        next_notification.update(
            {
                "suppression_reason": "symbol_cooldown",
                "suppressed_at": suppressed_at.isoformat(),
                "suppressed_until": suppressed_until.isoformat(),
                "suppressed_delivery_kind": delivery_kind,
                "suppressing_episode_id": suppressing_episode_id,
                "suppressing_event_id": suppressing_event_id,
            }
        )
        next_state = dict(self._state)
        next_state[episode_id] = next_notification
        event: dict[str, JSONValue] = {
            "source_event_id": source_event_id,
            "episode_id": episode_id,
            "symbol": symbol,
            "lifecycle_event_kind": lifecycle_event_kind,
            "requested_delivery_kind": delivery_kind,
            "reason": "symbol_cooldown",
            "suppressed_at": suppressed_at.isoformat(),
            "suppressed_until": suppressed_until.isoformat(),
            "suppressing_episode_id": suppressing_episode_id,
            "suppressing_event_id": suppressing_event_id,
        }
        if self._runtime_store is not None:
            self._runtime_store.set_monitor_state_and_append_opportunities(
                "spread",
                ANOMALY_NOTIFICATION_STATE_KEY,
                next_state,
                updated_at=suppressed_at,
                opportunities=(
                    (
                        event_id,
                        "anomaly_notification_suppressed",
                        event,
                        suppressed_at,
                    ),
                ),
            )
        self._state = next_state

    def commit_success(
        self,
        episode_id: str,
        notification: Mapping[str, JSONValue],
        *,
        symbol: str,
        lifecycle_event_kind: str,
        delivery_kind: str,
        completed_at: datetime,
        cooldown_seconds: int,
        source_event_id: str,
        attempt_event_id: str,
        result_event_id: str,
        delivery_channel: str,
    ) -> None:
        next_state = dict(self._state)
        next_state[episode_id] = dict(notification)
        next_clusters = self._pruned_clusters(
            completed_at,
            cooldown_seconds=cooldown_seconds,
        )
        if delivery_kind != "anomaly_return" and cooldown_seconds > 0:
            next_clusters[symbol] = {
                "last_nonreturn_sent_at": completed_at.isoformat(),
                "episode_id": episode_id,
                "event_id": source_event_id,
                "delivery_kind": delivery_kind,
            }
        success_event: dict[str, JSONValue] = {
            "source_event_id": source_event_id,
            "attempt_event_id": attempt_event_id,
            "episode_id": episode_id,
            "symbol": symbol,
            "lifecycle_event_kind": lifecycle_event_kind,
            "delivery_kind": delivery_kind,
            "delivery_channel": delivery_channel,
            "outcome": "success",
            "completed_at": completed_at.isoformat(),
        }
        if self._runtime_store is not None:
            self._runtime_store.set_monitor_state_and_append_opportunities(
                "spread",
                ANOMALY_NOTIFICATION_STATE_KEY,
                next_state,
                updated_at=completed_at,
                opportunities=(
                    (
                        result_event_id,
                        "anomaly_delivery_result",
                        success_event,
                        completed_at,
                    ),
                ),
                additional_states=(
                    (
                        "spread",
                        ANOMALY_NOTIFICATION_CLUSTERS_STATE_KEY,
                        next_clusters,
                        completed_at,
                    ),
                ),
            )
        self._state = next_state
        self._clusters = next_clusters

    def _pruned_clusters(
        self,
        now: datetime,
        *,
        cooldown_seconds: int,
    ) -> dict[str, dict[str, JSONValue]]:
        if cooldown_seconds <= 0:
            return {}
        return {
            symbol: entry
            for symbol, entry in self._clusters.items()
            if (
                (sent_at := _persisted_timestamp(entry.get("last_nonreturn_sent_at")))
                is not None
                and now < sent_at + timedelta(seconds=cooldown_seconds)
            )
        }


class SpreadAlertProcessor:
    def __init__(
        self,
        history: SpreadHistory,
        telegram: TelegramTransport,
        *,
        chart_renderer: Callable[
            [SpreadAlertDetails, HistoricalSpreadContext], bytes | None
        ] | None = None,
        candidate_net_bps: float = 10.0,
        alert_net_bps: float = 20.0,
        anomaly_v2_config: AnomalyV2Config | None = None,
        runtime_store: SQLiteRuntimeStore | None = None,
        stale_after_seconds: int = 30,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._history = history
        self._telegram = telegram
        self._candidate_net_bps = candidate_net_bps
        self._alert_net_bps = alert_net_bps
        self._anomaly_v2_config = (
            AnomalyV2Config(enabled=True)
            if anomaly_v2_config is None
            else anomaly_v2_config
        )
        self._anomaly_notifications = AnomalyNotificationStore(runtime_store)
        self._runtime_store = runtime_store
        self._clock = clock or (lambda: datetime.now(UTC))
        self._stale_after_seconds = stale_after_seconds
        if chart_renderer is None:
            self._chart_renderer = lambda details, context: render_spread_chart(
                details,
                context,
                candidate_net_bps=candidate_net_bps,
                alert_net_bps=alert_net_bps,
            )
        else:
            self._chart_renderer = chart_renderer

    async def process(self, alert: AlertRequest) -> None:
        if alert.payload.get("event_kind") in ANOMALY_EVENT_KINDS:
            await self._process_anomaly(alert)
            return
        details = parse_spread_alert(alert)
        try:
            context = await asyncio.to_thread(
                self._history.query,
                canonical_symbol=details.canonical_symbol,
                long_venue=details.long_venue,
                long_venue_symbol=details.long_venue_symbol,
                short_venue=details.short_venue,
                short_venue_symbol=details.short_venue_symbol,
                primary_size_usd=details.primary_size_usd,
                as_of=details.sample_time,
            )
        except Exception:  # noqa: BLE001
            LOGGER.error("history query failed for alert_id=%s", alert.event_id)
            context = HistoricalSpreadContext.empty()

        message = format_spread_alert(
            details,
            context,
            candidate_net_bps=self._candidate_net_bps,
            alert_net_bps=self._alert_net_bps,
        )
        chart_png: bytes | None = None
        try:
            chart_png = await asyncio.to_thread(
                self._chart_renderer,
                details,
                context,
            )
        except Exception:  # noqa: BLE001
            LOGGER.error("chart rendering failed for alert_id=%s", alert.event_id)

        if chart_png is None or len(message) > 1_024:
            await self._telegram.send_text(message)
        else:
            await self._telegram.send_chart(chart_png, message)

    async def _process_anomaly(self, alert: AlertRequest) -> None:
        details = parse_anomaly_alert(alert)
        current_time = alert.created_at
        notification = self._anomaly_notifications.get(details.episode_id)
        context: AnomalyConfirmationContext | None = None
        if notification.get("context") is not None:
            try:
                raw_context = notification["context"]
                if isinstance(raw_context, dict):
                    context = _context_from_payload(raw_context)
            except (TypeError, ValueError):
                context = None
        if context is None and details.confirmed_at is not None:
            context = await self._query_anomaly_context_with_retry(details)

        if context is None:
            context = AnomalyConfirmationContext.empty()
        eligible = all(
            stats.eligible
            for stats in (context.stats_12h, context.stats_24h, context.stats_48h)
        )
        alignment = _anomaly_alignment(context)
        reason = "eligible"
        if not eligible:
            reason = "context_unavailable"
            eligible = False
        elif alignment is None or alignment > self._anomaly_v2_config.mean_alignment_max_bps:
            reason = "mean_alignment"
            eligible = False
        if "eligibility" not in notification:
            notification.update(
                {
                    "eligibility": eligible,
                    "eligibility_reason": reason,
                    "mean_alignment_bps": alignment,
                    "context": _context_to_payload(context),
                }
            )
            self._anomaly_notifications.set(
                details.episode_id,
                notification,
                now=current_time,
            )
            if not eligible:
                return
        elif notification.get("eligibility") is not True:
            return
        elif not eligible:
            # A persisted eligible flag is not a substitute for a valid
            # confirmation context.  If that context was malformed or the
            # refresh failed, fail closed instead of delivering an alert.
            return
        if details.event_kind == "anomaly_return" and notification.get("initial_sent_at") is None:
            return

        initial_sent = notification.get("initial_sent_at") is not None
        delivery_kind = details.event_kind
        if details.event_kind == "anomaly_expansion" and not initial_sent:
            delivery_kind = "anomaly_initial"
        if details.event_kind == "anomaly_initial" and initial_sent:
            return
        if details.event_kind == "anomaly_expansion" and initial_sent:
            peak = details.post_confirmation_peak_deviation_bps
            last_peak = notification.get("last_expansion_peak_bps")
            if peak is None:
                return
            if isinstance(last_peak, (int, float)) and peak <= float(last_peak):
                return
        if details.event_kind == "anomaly_return" and notification.get("return_sent_at") is not None:
            return

        if details.event_kind != "anomaly_return":
            processing_time = self._now()
            cooldown = self._anomaly_notifications.active_cooldown(
                details.canonical_symbol,
                now=processing_time,
                cooldown_seconds=(
                    self._anomaly_v2_config.notification_symbol_cooldown_seconds
                ),
            )
            if cooldown is not None:
                suppressed_until, suppressing = cooldown
                suppressing_episode_id = suppressing.get("episode_id")
                suppressing_event_id = suppressing.get("event_id")
                self._anomaly_notifications.record_suppression(
                    details.episode_id,
                    notification,
                    symbol=details.canonical_symbol,
                    lifecycle_event_kind=details.event_kind,
                    delivery_kind=delivery_kind,
                    source_event_id=alert.event_id,
                    suppressing_episode_id=(
                        suppressing_episode_id
                        if isinstance(suppressing_episode_id, str)
                        else None
                    ),
                    suppressing_event_id=(
                        suppressing_event_id
                        if isinstance(suppressing_event_id, str)
                        else None
                    ),
                    suppressed_at=processing_time,
                    suppressed_until=suppressed_until,
                    event_id=_notification_event_id(alert.event_id, "suppressed"),
                )
                return

        message = format_anomaly_alert(details, context, event_kind=delivery_kind)
        chart_png: bytes | None = None
        try:
            historical = await asyncio.to_thread(
                self._history.query,
                canonical_symbol=details.canonical_symbol,
                long_venue=details.long_venue,
                long_venue_symbol=details.long_venue_symbol,
                short_venue=details.short_venue,
                short_venue_symbol=details.short_venue_symbol,
                primary_size_usd=details.primary_size_usd,
                as_of=details.sample_time,
            )
            chart_png = await asyncio.to_thread(
                render_anomaly_chart,
                details,
                historical,
            )
        except Exception:  # noqa: BLE001
            LOGGER.error("anomaly chart rendering failed for alert_id=%s", alert.event_id)
        delivery_channel = "text" if chart_png is None or len(message) > 1_024 else "chart"
        attempted_at = self._now()
        attempt_event_id = self._record_delivery_attempt(
            alert,
            details,
            delivery_kind=delivery_kind,
            delivery_channel=delivery_channel,
            attempted_at=attempted_at,
        )
        try:
            if delivery_channel == "text":
                await self._telegram.send_text(message)
            else:
                assert chart_png is not None
                await self._telegram.send_chart(chart_png, message)
        except Exception as error:  # noqa: BLE001
            try:
                self._record_delivery_result(
                    alert,
                    details,
                    delivery_kind=delivery_kind,
                    delivery_channel=delivery_channel,
                    attempt_event_id=attempt_event_id,
                    outcome="transport_error",
                    completed_at=self._now(),
                    error=error,
                )
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "anomaly delivery error result persistence failed for alert_id=%s",
                    alert.event_id,
                )
            raise

        completed_at = self._now()
        next_notification = dict(notification)
        if delivery_kind == "anomaly_initial":
            next_notification["initial_sent_at"] = completed_at.isoformat()
        elif delivery_kind == "anomaly_expansion":
            next_notification["last_expansion_peak_bps"] = (
                details.post_confirmation_peak_deviation_bps
            )
        elif delivery_kind == "anomaly_return":
            next_notification["return_sent_at"] = completed_at.isoformat()
        self._anomaly_notifications.commit_success(
            details.episode_id,
            next_notification,
            symbol=details.canonical_symbol,
            lifecycle_event_kind=details.event_kind,
            delivery_kind=delivery_kind,
            completed_at=completed_at,
            cooldown_seconds=(
                self._anomaly_v2_config.notification_symbol_cooldown_seconds
            ),
            source_event_id=alert.event_id,
            attempt_event_id=attempt_event_id,
            result_event_id=_notification_event_id(alert.event_id, "result"),
            delivery_channel=delivery_channel,
        )

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("alert clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    def _record_delivery_attempt(
        self,
        alert: AlertRequest,
        details: AnomalyAlertDetails,
        *,
        delivery_kind: str,
        delivery_channel: str,
        attempted_at: datetime,
    ) -> str:
        event_id = _notification_event_id(alert.event_id, "attempt")
        if self._runtime_store is not None:
            self._runtime_store.append_opportunity(
                "spread",
                event_id,
                "anomaly_delivery_attempt",
                {
                    "source_event_id": alert.event_id,
                    "episode_id": details.episode_id,
                    "symbol": details.canonical_symbol,
                    "lifecycle_event_kind": details.event_kind,
                    "delivery_kind": delivery_kind,
                    "delivery_channel": delivery_channel,
                    "attempted_at": attempted_at.isoformat(),
                },
                occurred_at=attempted_at,
            )
        return event_id

    def _record_delivery_result(
        self,
        alert: AlertRequest,
        details: AnomalyAlertDetails,
        *,
        delivery_kind: str,
        delivery_channel: str,
        attempt_event_id: str,
        outcome: str,
        completed_at: datetime,
        error: Exception | None = None,
    ) -> None:
        if self._runtime_store is None:
            return
        payload: dict[str, JSONValue] = {
            "source_event_id": alert.event_id,
            "attempt_event_id": attempt_event_id,
            "episode_id": details.episode_id,
            "symbol": details.canonical_symbol,
            "lifecycle_event_kind": details.event_kind,
            "delivery_kind": delivery_kind,
            "delivery_channel": delivery_channel,
            "outcome": outcome,
            "completed_at": completed_at.isoformat(),
        }
        if error is not None:
            payload["exception_class"] = type(error).__name__
            status_code = getattr(error, "status_code", None)
            if isinstance(status_code, int) and not isinstance(status_code, bool):
                payload["status_code"] = status_code
        self._runtime_store.append_opportunity(
            "spread",
            _notification_event_id(alert.event_id, "result"),
            "anomaly_delivery_result",
            payload,
            occurred_at=completed_at,
        )

    async def _query_anomaly_context_with_retry(
        self,
        details: AnomalyAlertDetails,
    ) -> AnomalyConfirmationContext:
        confirmed_at = details.confirmed_at
        if confirmed_at is None:
            raise ValueError("anomaly context requires confirmed_at")
        last_error: Exception | None = None
        for attempt in range(ANOMALY_CONTEXT_QUERY_ATTEMPTS):
            try:
                return await asyncio.to_thread(
                    self._history.query_anomaly_context,
                    canonical_symbol=details.canonical_symbol,
                    long_venue=details.long_venue,
                    long_venue_symbol=details.long_venue_symbol,
                    short_venue=details.short_venue,
                    short_venue_symbol=details.short_venue_symbol,
                    primary_size_usd=details.primary_size_usd,
                    confirmed_at=confirmed_at,
                    stale_after_seconds=self._stale_after_seconds,
                )
            except Exception as error:  # noqa: BLE001
                last_error = error
                if attempt + 1 < ANOMALY_CONTEXT_QUERY_ATTEMPTS:
                    await asyncio.sleep(ANOMALY_CONTEXT_QUERY_DELAY_SECONDS)
        LOGGER.error("anomaly context query failed after retries", exc_info=last_error)
        assert last_error is not None
        raise last_error
