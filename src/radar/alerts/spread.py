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
    ) -> None:
        self._history = history
        self._telegram = telegram
        self._candidate_net_bps = candidate_net_bps
        self._alert_net_bps = alert_net_bps
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
