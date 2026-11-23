from __future__ import annotations

import math
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EXPLICIT_FEE_VENUES = frozenset(
    {"trade_xyz", "entropy", "arcus", "backpack", "lighter_robinhood", "ondo"}
)


class MarketConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    enabled: bool = True
    # Collection and Dashboard visibility are independent from executable
    # monitor participation.  Existing markets remain in monitor scope by
    # default; read-only feeds can opt out explicitly.
    monitor_enabled: bool = True
    # Set by market discovery (never in YAML): discovered feeds join only the
    # spread monitor, not the BBO-only manual lifecycle.
    discovered: bool = False
    # How often this feed feeds rolling history and Parquet; prices are still
    # evaluated every 10s. A pair uses the slower cadence of its two feeds.
    sample_seconds: Literal[10, 30, 60] = 10


DiscoveryVenue = Literal[
    "lighter", "lighter_robinhood", "hyperliquid", "trade_xyz", "variational"
]


class SymbolOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")

    venue: DiscoveryVenue
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)


class MarketRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)


class DiscoveryConfig(BaseModel):
    """Automatic market listing and canonical pairing for public venues."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    venues: list[DiscoveryVenue] = Field(
        default_factory=lambda: [
            "lighter",
            "lighter_robinhood",
            "hyperliquid",
            "trade_xyz",
            "variational",
        ]
    )
    min_volume_24h_usd: float = Field(default=100_000, ge=0, allow_inf_nan=False)
    # Same ticker, different asset (or unit) shows up as a large mark gap.
    max_mark_deviation: float = Field(default=0.2, gt=0, lt=1, allow_inf_nan=False)
    rescan_seconds: int = Field(default=6 * 60 * 60, ge=600)
    # Feeds below this 24h volume (and every RFQ feed, which refreshes about
    # once a minute) are long-tail and keep history at the slower cadence.
    full_rate_min_volume_usd: float = Field(default=5_000_000, ge=0, allow_inf_nan=False)
    long_tail_sample_seconds: Literal[30, 60] = 30
    symbol_overrides: list[SymbolOverride] = Field(
        default_factory=lambda: [
            SymbolOverride(
                venue="variational", venue_symbol="US500", canonical_symbol="SPY"
            )
        ]
    )
    # Pre-IPO contracts define a "share" differently on each venue.
    exclude_symbols: list[str] = Field(default_factory=lambda: ["ANT", "OPENAI"])
    exclude_markets: list[MarketRef] = Field(default_factory=list)


class AnomalyV2Config(BaseModel):
    """Configuration for the opt-in positive-deviation anomaly lifecycle."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    telegram_enabled: bool = True
    deviation_bps: float = Field(default=15.0, ge=0, allow_inf_nan=False)
    confirmation_seconds: int = Field(default=60, ge=0)
    return_band_bps: float = Field(default=5.0, ge=0, allow_inf_nan=False)
    max_gap_seconds: int = Field(default=20, gt=0)
    mean_alignment_max_bps: float = Field(default=5.0, ge=0, allow_inf_nan=False)
    expansion_notify_step_bps: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    notification_symbol_cooldown_seconds: int = Field(default=300, ge=0)


class ManualOpportunityConfig(BaseModel):
    """Configuration for the opt-in, BBO-only manual opportunity lifecycle."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    confirmation_seconds: int = Field(default=60, ge=0)
    baseline_range_max_bps: float = Field(default=5.0, ge=0, allow_inf_nan=False)
    expected_net_min_bps: float = Field(default=10.0, ge=0, allow_inf_nan=False)
    volume_24h_min_usd: float = Field(
        default=1_000_000.0, gt=0, allow_inf_nan=False
    )
    expansion_notify_step_bps: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    max_gap_seconds: int = Field(default=20, gt=0)
    telegram_rearm_quiet_seconds: int = Field(default=300, ge=0)


class SpreadMonitorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    interval_seconds: int = Field(default=10, gt=0)
    # Long-tail books are often too thin for $10k, so $1k is the evaluated size;
    # $5k/$10k spreads ride along in alerts as capacity context.
    primary_size_usd: Literal[1000, 5000, 10000] = 1000
    top_n: int = Field(default=3, gt=0)
    candidate_net_bps: float = Field(default=10.0, ge=0, allow_inf_nan=False)
    candidate_duration_seconds: int = Field(default=30, ge=0)
    alert_net_bps: float = Field(default=20.0, ge=0, allow_inf_nan=False)
    alert_duration_seconds: int = Field(default=120, ge=0)
    stale_after_seconds: int = Field(default=30, gt=0)
    # RFQ venues (Variational) refresh quotes about once a minute.
    rfq_stale_after_seconds: int = Field(default=120, gt=0)
    anomaly_v2: AnomalyV2Config = Field(default_factory=AnomalyV2Config)


class MonitorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spread: SpreadMonitorConfig = Field(default_factory=SpreadMonitorConfig)


class RadarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sampling_seconds: int = Field(default=10, gt=0)
    # Funding/OI/volume refresh period; venues settle hourly, so it must divide 1h.
    context_poll_seconds: int = Field(default=300, ge=60, le=3600)
    fees_bps: dict[str, float] = Field(default_factory=dict)
    maker_fees_bps: dict[str, float] = Field(default_factory=dict)
    markets: list[MarketConfig] = Field(default_factory=list)
    quoted_markets: list[MarketConfig] = Field(default_factory=list)
    monitors: MonitorConfig = Field(default_factory=MonitorConfig)
    manual_opportunity: ManualOpportunityConfig = Field(
        default_factory=ManualOpportunityConfig
    )
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)

    def monitor_market_identities(self) -> set[tuple[str, str, str]]:
        """Return enabled feeds allowed to form monitor routes."""
        return {
            (market.venue, market.venue_symbol, market.canonical_symbol)
            for market in self.markets
            if market.enabled and market.monitor_enabled and not market.discovered
        }

    def market_sample_seconds(self) -> dict[tuple[str, str, str], int]:
        return {
            (market.venue, market.venue_symbol, market.canonical_symbol): market.sample_seconds
            for market in (*self.markets, *self.quoted_markets)
            if market.enabled
        }

    def spread_monitor_market_identities(self) -> set[tuple[str, str, str]]:
        """Feeds the spread monitor may pair, including enabled RFQ quote feeds."""
        return {
            (market.venue, market.venue_symbol, market.canonical_symbol)
            for market in (*self.markets, *self.quoted_markets)
            if market.enabled and market.monitor_enabled
        }

    @field_validator("sampling_seconds")
    @classmethod
    def sampling_must_be_ten_seconds(cls, value: int) -> int:
        if value != 10:
            raise ValueError("sampling_seconds must be a 10-second interval")
        return value

    @field_validator("context_poll_seconds")
    @classmethod
    def context_poll_must_divide_hour(cls, value: int) -> int:
        if 3600 % value:
            raise ValueError("context_poll_seconds must evenly divide 3600")
        return value

    @field_validator("fees_bps")
    @classmethod
    def fees_must_be_non_negative(cls, value: dict[str, float]) -> dict[str, float]:
        if any(not math.isfinite(fee) or fee < 0 for fee in value.values()):
            raise ValueError("fees_bps must contain finite non-negative values")
        return value

    @field_validator("fees_bps", mode="before")
    @classmethod
    def fees_must_not_be_boolean(cls, value: object) -> object:
        if isinstance(value, dict) and any(isinstance(fee, bool) for fee in value.values()):
            raise ValueError("fees_bps values must be numeric, not boolean")
        return value

    @field_validator("maker_fees_bps")
    @classmethod
    def maker_fees_must_be_non_negative(
        cls, value: dict[str, float]
    ) -> dict[str, float]:
        if any(not math.isfinite(fee) or fee < 0 for fee in value.values()):
            raise ValueError("maker_fees_bps must contain finite non-negative values")
        return value

    @field_validator("maker_fees_bps", mode="before")
    @classmethod
    def maker_fees_must_not_be_boolean(cls, value: object) -> object:
        if isinstance(value, dict) and any(isinstance(fee, bool) for fee in value.values()):
            raise ValueError("maker_fees_bps values must be numeric, not boolean")
        return value

    @field_validator("quoted_markets")
    @classmethod
    def quoted_markets_must_be_variational(
        cls, value: list[MarketConfig]
    ) -> list[MarketConfig]:
        if any(market.venue.lower() != "variational" for market in value):
            raise ValueError("quoted_markets must contain only variational markets")
        # RFQ quotes refresh about once a minute; 10s history would only repeat
        # them. An explicit sample_seconds in YAML still wins.
        return [
            market
            if "sample_seconds" in market.model_fields_set
            else market.model_copy(update={"sample_seconds": 30})
            for market in value
        ]

    @model_validator(mode="after")
    def enabled_markets_require_explicit_fee(self) -> "RadarConfig":
        required_venues = {
            market.venue.lower()
            for market in self.markets
            if market.enabled and market.venue.lower() in EXPLICIT_FEE_VENUES
        }
        configured_venues = {venue.lower() for venue in self.fees_bps}
        missing_venues = sorted(required_venues - configured_venues)
        if missing_venues:
            raise ValueError(
                "enabled markets require explicit fees for: "
                + ", ".join(missing_venues)
            )
        return self


def load_config(path: Path) -> RadarConfig:
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    return RadarConfig.model_validate(raw)
