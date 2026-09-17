from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

from radar.alerts.chart import render_spread_chart
from radar.alerts.models import FundingContext, SpreadAlertDetails
from radar.alerts.telegram import TelegramTransport
from radar.history.spread import HistoricalSpreadContext, SpreadHistory, WindowStats
from radar.monitors.base import AlertRequest, JSONValue

SUPPORTED_SIZES = frozenset({1_000, 5_000, 10_000})
LOGGER = logging.getLogger(__name__)


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
        long_fee_bps=_require_number(payload, "long_fee_bps", non_negative=True),
        short_fee_bps=_require_number(payload, "short_fee_bps", non_negative=True),
        net_spread_bps=_require_number(payload, "net_spread_bps"),
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


def _format_funding(
    venue: str,
    funding: FundingContext | None,
) -> str:
    if funding is None:
        return f"{venue.title()}: 不可用"
    next_time = (
        funding.next_funding_time.isoformat()
        if funding.next_funding_time is not None
        else "不可用"
    )
    return (
        f"{venue.title()}: {funding.funding_rate * 100:.4f}%"
        f"（有效 {funding.effective_time.isoformat()}，下次 {next_time}）"
    )


def _format_stats(label: str, stats: WindowStats) -> str:
    if stats.median_raw_spread_bps is None:
        return f"{label}: 不可用（樣本數 {stats.sample_count}）"
    return (
        f"{label}: 中位數 {stats.median_raw_spread_bps:.2f} bps"
        f"（樣本數 {stats.sample_count}）"
    )


def format_spread_alert(
    details: SpreadAlertDetails,
    context: HistoricalSpreadContext,
) -> str:
    return "\n".join(
        (
            f"價差警報 {details.canonical_symbol} ${details.primary_size_usd:,}",
            (
                f"做多 {details.long_venue.title()} ({details.long_venue_symbol}) "
                f"買入 VWAP {details.long_buy_vwap:.2f}"
            ),
            (
                f"做空 {details.short_venue.title()} ({details.short_venue_symbol}) "
                f"賣出 VWAP {details.short_sell_vwap:.2f}"
            ),
            f"樣本時間 {details.sample_time.isoformat()}",
            f"原始價差 {details.raw_spread_bps:.2f} bps",
            (
                f"手續費 {details.long_venue.title()} {details.long_fee_bps:.2f} bps"
                f" + {details.short_venue.title()} {details.short_fee_bps:.2f} bps"
            ),
            f"淨價差 {details.net_spread_bps:.2f} bps",
            (
                f"候選持續 {details.candidate_duration_seconds}s，"
                f"警報持續 {details.alert_duration_seconds}s"
            ),
            "資金費率",
            _format_funding(details.long_venue, details.long_funding),
            _format_funding(details.short_venue, details.short_funding),
            "歷史原始價差",
            _format_stats("7日", context.stats_7d),
            _format_stats("30日", context.stats_30d),
            _format_stats("90日", context.stats_90d),
        )
    )


class SpreadAlertProcessor:
    def __init__(
        self,
        history: SpreadHistory,
        telegram: TelegramTransport,
        *,
        chart_renderer: Callable[
            [SpreadAlertDetails, HistoricalSpreadContext], bytes | None
        ] = render_spread_chart,
    ) -> None:
        self._history = history
        self._telegram = telegram
        self._chart_renderer = chart_renderer

    async def process(self, alert: AlertRequest) -> None:
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

        message = format_spread_alert(details, context)
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
