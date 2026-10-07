from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import math

from radar.monitors.base import AlertRequest
from radar.monitors.manual_opportunity import ManualOpportunityNotificationGate
from radar.monitors.spread.models import SpreadPairKey
from radar.alerts.telegram import TelegramTransport


@dataclass(frozen=True)
class ManualOpportunityAlertDetails:
    event_kind: str
    episode_id: str
    canonical_symbol: str
    long_venue: str
    long_venue_symbol: str
    short_venue: str
    short_venue_symbol: str
    signal_duration_seconds: int
    current_spread_bps: float
    reference_a_bps: float
    deviation_bps: float
    round_trip_fee_bps: float
    expected_net_at_a_bps: float
    mean_2h_bps: float | None
    mean_24h_bps: float | None
    mean_3d_bps: float | None
    baseline_range_bps: float | None
    long_volume_24h: float | None
    short_volume_24h: float | None
    route_volume_24h: float | None
    long_best_ask: float
    short_best_bid: float
    sample_time: datetime
    expansion_level_bps: float | None


class ManualOpportunityAlertProcessor:
    """Deliver Manual Opportunity alerts as text-only Telegram messages."""

    def __init__(
        self,
        telegram: TelegramTransport,
        *,
        notification_gate: ManualOpportunityNotificationGate | None = None,
    ) -> None:
        self._telegram = telegram
        self._notification_gate = notification_gate

    async def process(self, alert: AlertRequest) -> None:
        details = parse_manual_opportunity_alert(alert)
        try:
            await self._telegram.send_text(format_manual_opportunity_alert(details))
        except Exception:
            if (
                self._notification_gate is not None
                and details.event_kind == "manual_initial"
            ):
                self._notification_gate.mark_failed(
                    _route_key(details), details.episode_id
                )
            raise
        if (
            self._notification_gate is not None
            and details.event_kind == "manual_initial"
        ):
            self._notification_gate.mark_sent(
                _route_key(details),
                details.episode_id,
                sent_at=datetime.now(UTC),
            )


def _route_key(details: ManualOpportunityAlertDetails) -> SpreadPairKey:
    return SpreadPairKey(
        canonical_symbol=details.canonical_symbol,
        long_venue=details.long_venue,
        long_venue_symbol=details.long_venue_symbol,
        short_venue=details.short_venue,
        short_venue_symbol=details.short_venue_symbol,
    )


def parse_manual_opportunity_alert(
    alert: AlertRequest | Mapping[str, object],
) -> ManualOpportunityAlertDetails:
    payload: Mapping[str, object]
    if isinstance(alert, AlertRequest):
        if alert.monitor != "manual_opportunity":
            raise ValueError("alert is not a manual opportunity alert")
        payload = alert.payload
    else:
        payload = alert

    return ManualOpportunityAlertDetails(
        event_kind=_text(payload, "event_kind"),
        episode_id=_text(payload, "episode_id"),
        canonical_symbol=_text(payload, "canonical_symbol"),
        long_venue=_text(payload, "long_venue"),
        long_venue_symbol=_text(payload, "long_venue_symbol"),
        short_venue=_text(payload, "short_venue"),
        short_venue_symbol=_text(payload, "short_venue_symbol"),
        signal_duration_seconds=_integer(payload, "signal_duration_seconds"),
        current_spread_bps=_number(payload, "current_spread_bps"),
        reference_a_bps=_number(payload, "reference_a_bps"),
        deviation_bps=_number(payload, "deviation_bps"),
        round_trip_fee_bps=_number(payload, "round_trip_fee_bps"),
        expected_net_at_a_bps=_number(payload, "expected_net_at_a_bps"),
        mean_2h_bps=_optional_number(payload, "mean_2h_bps"),
        mean_24h_bps=_optional_number(payload, "mean_24h_bps"),
        mean_3d_bps=_optional_number(payload, "mean_3d_bps"),
        baseline_range_bps=_optional_number(payload, "baseline_range_bps"),
        long_volume_24h=_optional_number(payload, "long_volume_24h"),
        short_volume_24h=_optional_number(payload, "short_volume_24h"),
        route_volume_24h=_optional_number(payload, "route_volume_24h"),
        long_best_ask=_number(payload, "long_best_ask"),
        short_best_bid=_number(payload, "short_best_bid"),
        sample_time=_timestamp(payload, "sample_time"),
        expansion_level_bps=_optional_number(payload, "expansion_level_bps"),
    )


def format_manual_opportunity_alert(
    details: ManualOpportunityAlertDetails | Mapping[str, object] | AlertRequest,
) -> str:
    if not isinstance(details, ManualOpportunityAlertDetails):
        details = parse_manual_opportunity_alert(details)
    duration = _format_duration(details.signal_duration_seconds)
    heading = "Expansion" if details.event_kind == "manual_expansion" else "Manual Opportunity"
    expansion = (
        "\nExpansion level     "
        f"{details.expansion_level_bps:+.2f} bps"
        if details.expansion_level_bps is not None
        else ""
    )
    return "\n".join(
        (
            f"{details.canonical_symbol} {heading} · {duration}",
            f"Long  {details.long_venue} ({details.long_venue_symbol})",
            f"Short {details.short_venue} ({details.short_venue_symbol})",
            "",
            f"Current spread       {_bps(details.current_spread_bps)}",
            f"Normal basis a       {_bps(details.reference_a_bps)}",
            f"Deviation            {_bps(details.deviation_bps)}",
            "",
            f"4-leg taker fee      {_bps(details.round_trip_fee_bps)}",
            f"Expected net at a    {_bps(details.expected_net_at_a_bps)}",
            "",
            "Baseline",
            f"2h                  {_optional_bps(details.mean_2h_bps)}",
            f"24h                 {_optional_bps(details.mean_24h_bps)}",
            f"3d                  {_optional_bps(details.mean_3d_bps)}",
            f"Range               {_optional_bps(details.baseline_range_bps)}",
            "",
            "24h Volume",
            f"{details.long_venue:<20} {_usd(details.long_volume_24h)}",
            f"{details.short_venue:<20} {_usd(details.short_volume_24h)}",
            f"Route min            {_usd(details.route_volume_24h)}",
            "",
            f"Long best ask        {_price(details.long_best_ask)}",
            f"Short best bid       {_price(details.short_best_bid)}",
            f"Sample time          {details.sample_time.isoformat().replace('+00:00', 'Z')}",
            expansion,
        )
    ).rstrip()


def _text(payload: Mapping[str, object], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be non-empty text")
    return value


def _number(payload: Mapping[str, object], field_name: str) -> float:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be finite")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_number(payload: Mapping[str, object], field_name: str) -> float | None:
    value = payload.get(field_name)
    return None if value is None else _number(payload, field_name)


def _integer(payload: Mapping[str, object], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _timestamp(payload: Mapping[str, object], field_name: str) -> datetime:
    value = payload.get(field_name)
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO timestamp")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc
    offset = result.utcoffset()
    if result.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field_name} must be UTC")
    return result.astimezone(UTC)


def _bps(value: float) -> str:
    return f"{value:+.2f} bps"


def _optional_bps(value: float | None) -> str:
    return "n/a" if value is None else _bps(value)


def _price(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _usd(value: float | None) -> str:
    return "n/a" if value is None else f"${value:,.0f}"


def _format_duration(seconds: int) -> str:
    minutes, remainder = divmod(max(0, seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m {remainder:02d}s"
