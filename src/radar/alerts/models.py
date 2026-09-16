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
    long_fee_bps: float
    short_fee_bps: float
    net_spread_bps: float
    sample_time: datetime
    candidate_duration_seconds: int
    alert_duration_seconds: int
    long_funding: FundingContext | None
    short_funding: FundingContext | None
