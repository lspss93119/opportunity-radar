from __future__ import annotations

import math
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EXPLICIT_FEE_VENUES = frozenset(
    {"trade_xyz", "entropy", "arcus", "backpack", "lighter_robinhood"}
)


class MarketConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    enabled: bool = True


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


class SpreadMonitorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    interval_seconds: int = Field(default=10, gt=0)
    primary_size_usd: Literal[1000, 5000, 10000] = 10000
    top_n: int = Field(default=3, gt=0)
    candidate_net_bps: float = Field(default=10.0, ge=0, allow_inf_nan=False)
    candidate_duration_seconds: int = Field(default=30, ge=0)
    alert_net_bps: float = Field(default=20.0, ge=0, allow_inf_nan=False)
    alert_duration_seconds: int = Field(default=120, ge=0)
    stale_after_seconds: int = Field(default=30, gt=0)
    anomaly_v2: AnomalyV2Config = Field(default_factory=AnomalyV2Config)


class MonitorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spread: SpreadMonitorConfig = Field(default_factory=SpreadMonitorConfig)


class RadarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sampling_seconds: int = Field(default=10, gt=0)
    fees_bps: dict[str, float] = Field(default_factory=dict)
    maker_fees_bps: dict[str, float] = Field(default_factory=dict)
    markets: list[MarketConfig] = Field(default_factory=list)
    quoted_markets: list[MarketConfig] = Field(default_factory=list)
    monitors: MonitorConfig = Field(default_factory=MonitorConfig)
    manual_opportunity: ManualOpportunityConfig = Field(
        default_factory=ManualOpportunityConfig
    )

    @field_validator("sampling_seconds")
    @classmethod
    def sampling_must_be_ten_seconds(cls, value: int) -> int:
        if value != 10:
            raise ValueError("sampling_seconds must be a 10-second interval")
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
        return value

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
