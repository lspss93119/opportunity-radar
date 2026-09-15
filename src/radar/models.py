from __future__ import annotations

from datetime import datetime, timedelta

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must use UTC")
    return value


class MarketSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sample_time: datetime
    observed_at: datetime
    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)

    best_bid: float = Field(gt=0)
    best_bid_size: float = Field(ge=0)
    best_ask: float = Field(gt=0)
    best_ask_size: float = Field(ge=0)

    mark_price: float | None = Field(default=None, gt=0)
    index_price: float | None = Field(default=None, gt=0)

    buy_1k_vwap: float | None = Field(default=None, gt=0)
    sell_1k_vwap: float | None = Field(default=None, gt=0)
    buy_5k_vwap: float | None = Field(default=None, gt=0)
    sell_5k_vwap: float | None = Field(default=None, gt=0)
    buy_10k_vwap: float | None = Field(default=None, gt=0)
    sell_10k_vwap: float | None = Field(default=None, gt=0)

    _sample_time_utc = field_validator("sample_time")(_require_utc)
    _observed_at_utc = field_validator("observed_at")(_require_utc)

    @model_validator(mode="after")
    def validate_bbo(self) -> "MarketSnapshot":
        if self.best_bid >= self.best_ask:
            raise ValueError("best_bid must be lower than best_ask")
        return self


class FundingSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    effective_time: datetime
    observed_at: datetime
    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    funding_rate: float = Field(allow_inf_nan=False)
    next_funding_time: datetime | None = None

    _effective_time_utc = field_validator("effective_time")(_require_utc)
    _observed_at_utc = field_validator("observed_at")(_require_utc)

    @field_validator("next_funding_time")
    @classmethod
    def next_funding_must_be_utc(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _require_utc(value)


class HourlyContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sample_time: datetime
    observed_at: datetime
    venue: str = Field(min_length=1)
    venue_symbol: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    open_interest: float | None = Field(default=None, ge=0)
    volume_24h: float | None = Field(default=None, ge=0)

    _sample_time_utc = field_validator("sample_time")(_require_utc)
    _observed_at_utc = field_validator("observed_at")(_require_utc)
