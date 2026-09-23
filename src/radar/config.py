from __future__ import annotations

import math
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EXPLICIT_FEE_VENUES = frozenset({"trade_xyz", "entropy", "arcus", "backpack"})


class MarketConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    enabled: bool = True


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


class MonitorConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    spread: SpreadMonitorConfig = Field(default_factory=SpreadMonitorConfig)


class RadarConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sampling_seconds: int = Field(default=10, gt=0)
    fees_bps: dict[str, float] = Field(default_factory=dict)
    markets: list[MarketConfig] = Field(default_factory=list)
    monitors: MonitorConfig = Field(default_factory=MonitorConfig)

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
