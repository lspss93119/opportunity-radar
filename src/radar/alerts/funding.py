"""Telegram formatting for funding differential alerts (slow path)."""

from __future__ import annotations

import math
from datetime import datetime

from radar.alerts.telegram import TelegramTransport
from radar.monitors.base import AlertRequest, JSONValue

KIND_TITLES = {"funding_spike": "資金費飆升", "funding_carry": "資金費 Carry"}


def _display_venue(venue: str) -> str:
    return " ".join(part.capitalize() for part in venue.split("_"))


def _number(value: JSONValue) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _percent(value: JSONValue, *, digits: int = 2) -> str:
    number = _number(value)
    return "—" if number is None else f"{number * 100:+.{digits}f}%"


def _bps(value: JSONValue) -> str:
    number = _number(value)
    return "—" if number is None else f"{number:+.2f} bps"


def _usd(value: JSONValue) -> str:
    number = _number(value)
    if number is None:
        return "—"
    if number >= 1e6:
        return f"${number / 1e6:,.1f}M"
    if number >= 1e3:
        return f"${number / 1e3:,.0f}k"
    return f"${number:,.0f}"


def _time(value: JSONValue) -> str:
    if not isinstance(value, str):
        return "—"
    try:
        return datetime.fromisoformat(value).strftime("%m-%d %H:%M UTC")
    except ValueError:
        return "—"


def _duration(value: JSONValue) -> str:
    seconds = _number(value)
    if seconds is None:
        return "—"
    minutes = int(seconds // 60)
    return f"{minutes // 60}h{minutes % 60:02d}m"


def _interval(value: JSONValue) -> str:
    seconds = _number(value)
    return "—" if seconds is None else f"{seconds / 3600:g}h"


def _venue_label(venue: str, market: JSONValue) -> str:
    rfq = isinstance(market, dict) and market.get("price_source") == "rfq"
    return f"{_display_venue(venue)}{' ［RFQ 報價］' if rfq else ''}"


def _funding_line(venue: str, funding: JSONValue) -> str:
    if not isinstance(funding, dict):
        return f"{_display_venue(venue)}: 不可用"
    return (
        f"{_display_venue(venue)}: {_percent(funding.get('funding_rate'), digits=4)}"
        f"/{_interval(funding.get('interval_seconds'))}"
        f"（APR {_percent(funding.get('apr'))}）"
        f" 下次結算 {_time(funding.get('next_funding_time'))}"
    )


def _market_line(venue: str, market: JSONValue) -> str:
    if not isinstance(market, dict):
        return f"{_display_venue(venue)}: —"
    parts = [f"24h 量 {_usd(market.get('volume_24h_usd'))}"]
    if market.get("long_open_interest_usd") is not None or market.get(
        "short_open_interest_usd"
    ) is not None:
        parts.append(
            f"多方 OI {_usd(market.get('long_open_interest_usd'))}"
            f" / 空方 OI {_usd(market.get('short_open_interest_usd'))}"
        )
    else:
        parts.append(f"OI ≈ {_usd(market.get('open_interest_usd'))}")
    return f"{_display_venue(venue)}: " + " | ".join(parts)


def format_funding_alert(alert: AlertRequest) -> str:
    payload = alert.payload
    kind = payload.get("kind")
    if kind not in KIND_TITLES:
        raise ValueError("unknown funding alert kind")
    long_venue = str(payload["long_venue"])
    short_venue = str(payload["short_venue"])
    long_market = payload.get("long_market")
    short_market = payload.get("short_market")
    window = _number(payload.get("window_hours"))
    coverage = _number(payload.get("coverage"))
    horizon = _number(payload.get("horizon_hours"))
    size = _number(payload.get("primary_size_usd"))
    duration_label = "淨 APR ≥ 門檻已持續" if kind == "funding_spike" else "平均以上已持續"
    lines = [
        f"{KIND_TITLES[kind]} | {payload['canonical_symbol']}",
        f"做多: {_venue_label(long_venue, long_market)} ({payload['long_venue_symbol']})",
        f"做空: {_venue_label(short_venue, short_market)} ({payload['short_venue_symbol']})",
        "",
        f"淨 APR: {_percent(payload.get('net_apr'))}"
        f"（連續 {payload.get('spike_samples', 0)} 次採樣）",
        f"{window:g}h 平均淨 APR: {_percent(payload.get('avg_net_apr'))}"
        f"（覆蓋 {0 if coverage is None else coverage * 100:.0f}%）"
        if window is not None
        else f"平均淨 APR: {_percent(payload.get('avg_net_apr'))}",
        f"{duration_label}: {_duration(payload.get('condition_seconds'))}",
        "",
        "資金費（正值 = 多方付錢）:",
        _funding_line(long_venue, payload.get("long_funding")),
        _funding_line(short_venue, payload.get("short_funding")),
        "",
        f"${0 if size is None else size:,.0f} 可成交價差: {_bps(payload.get('entry_spread_bps'))}"
        f" | 來回成本: {_bps(payload.get('round_trip_cost_bps'))}",
        f"{0 if horizon is None else horizon:g}h 預期資金費: {_bps(payload.get('expected_funding_bps'))}",
        "",
        _market_line(long_venue, long_market),
        _market_line(short_venue, short_market),
    ]
    return "\n".join(lines)


class FundingAlertProcessor:
    def __init__(self, telegram: TelegramTransport) -> None:
        self._telegram = telegram

    async def process(self, alert: AlertRequest) -> None:
        if alert.monitor != "funding":
            raise ValueError("alert monitor must be funding")
        await self._telegram.send_text(format_funding_alert(alert))
