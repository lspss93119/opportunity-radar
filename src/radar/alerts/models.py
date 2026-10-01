from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class FundingContext:
    effective_time: datetime
    observed_at: datetime
    funding_rate: float
    next_funding_time: datetime | None


@dataclass(frozen=True)
class SpreadAlertDetails:
    canonical_symbol: str
    long_venue: str
    long_venue_symbol: str
    short_venue: str
    short_venue_symbol: str
    primary_size_usd: int
    long_buy_vwap: float
    short_sell_vwap: float
    raw_spread_bps: float
    long_fee_bps: float | None
    short_fee_bps: float | None
    net_spread_bps: float | None
    rolling_mean_bps: float
    rolling_std_bps: float
    deviation_bps: float
    signal_duration_seconds: int
    observed_at_skew_seconds: float
    round_trip_fee_bps: float | None
    theoretical_edge_bps: float | None
    sample_time: datetime
    candidate_duration_seconds: int
    alert_duration_seconds: int
    long_funding: FundingContext | None
    short_funding: FundingContext | None


@dataclass(frozen=True)
class AnomalyAlertDetails:
    event_kind: str
    episode_id: str
    canonical_symbol: str
    long_venue: str
    long_venue_symbol: str
    short_venue: str
    short_venue_symbol: str
    primary_size_usd: int
    sample_time: datetime
    candidate_started_at: datetime
    confirmed_at: datetime | None
    reference_mean_bps: float
    reference_std_bps: float
    confirmation_spread_bps: float | None
    confirmation_deviation_bps: float | None
    lifetime_peak_spread_bps: float
    lifetime_peak_deviation_bps: float
    lifetime_peak_at: datetime
    post_confirmation_peak_spread_bps: float | None
    post_confirmation_peak_deviation_bps: float | None
    post_confirmation_peak_at: datetime | None
    current_spread_bps: float
    current_deviation_bps: float
    current_live_mean_bps: float | None
    current_live_std_bps: float | None
    ended_at: datetime | None
    end_spread_bps: float | None
    end_deviation_bps: float | None
    resolution_reason: str | None
    long_buy_vwap: float
    short_sell_vwap: float
    raw_spread_bps: float
    long_fee_bps: float | None
    short_fee_bps: float | None
    net_spread_bps: float | None
    observed_at_skew_seconds: float
