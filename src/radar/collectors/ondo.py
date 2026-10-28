from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from collections.abc import Callable

from radar.collectors.base import (
    CollectorBatch, CollectorErrorHandler, markets_for_venue,
    finite_float, positive_float, non_negative_float, report_collector_error,
)
from radar.collectors.http import request_json as default_request_json
from radar.config import MarketConfig
from radar.models import MarketSnapshot, FundingSnapshot, HourlyContext
from radar.vwap import BookLevel, buy_vwap, sell_vwap

UTC = timezone.utc


def _result(payload: object) -> object:
    if not isinstance(payload, dict) or payload.get("success") is not True or "result" not in payload:
        raise ValueError("Ondo response missing successful result envelope")
    return payload["result"]


def _time(value: object) -> datetime:
    if not isinstance(value, (str, int, float)):
        raise ValueError("invalid Ondo timestamp")
    if isinstance(value, str):
        try:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("invalid Ondo ISO timestamp") from exc
        if result.tzinfo is None:
            raise ValueError("Ondo timestamp missing timezone")
        return result.astimezone(UTC)
    seconds = finite_float(value, "timestamp")
    if seconds > 1e12:
        seconds /= 1000
    return datetime.fromtimestamp(seconds, tz=UTC)


def parse_ondo_contracts(payload: object) -> dict[str, dict]:
    rows = _result(payload)
    if not isinstance(rows, list):
        raise ValueError("Ondo contracts must be an array")
    contracts: dict[str, dict] = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("market"), str):
            raise ValueError("invalid Ondo contract")
        symbol = row["market"]
        if symbol in contracts:
            raise ValueError("duplicate Ondo contract")
        if row.get("disabled") is True or row.get("isClosed") is True:
            continue
        contracts[symbol] = row
    return contracts


def parse_ondo_book(payload: object, *, symbol: str) -> tuple[list[BookLevel], list[BookLevel]]:
    row = _result(payload)
    if not isinstance(row, dict) or row.get("market") != symbol:
        raise ValueError("Ondo book market mismatch")
    def side(name: str) -> list[BookLevel]:
        raw = row.get(name)
        if not isinstance(raw, list) or not raw:
            raise ValueError("empty Ondo book")
        values: list[BookLevel] = []
        for level in raw:
            if not isinstance(level, list) or len(level) != 2:
                raise ValueError("invalid Ondo book level")
            values.append(BookLevel(
                price=positive_float(level[0], "price"),
                base_size=positive_float(level[1], "base size"),
            ))
        values.sort(key=lambda x: x.price, reverse=name == "bids")
        return values
    bids, asks = side("bids"), side("asks")
    if bids[0].price >= asks[0].price:
        raise ValueError("crossed Ondo book")
    return bids, asks


def _optional_positive(value: object) -> float | None:
    return None if value is None else positive_float(value, "price")


def _optional_non_negative(value: object) -> float | None:
    return None if value is None else non_negative_float(value, "context")


class OndoCollector:
    """Public REST collector. Never authenticates or submits orders."""

    venue = "ondo"
    BASE_URL = "https://api.ondoperps.xyz"
    CONTRACTS_URL = BASE_URL + "/v1/perps/contracts"
    DEPTH_URL = BASE_URL + "/v1/perps/depth"
    FUNDING_URL = BASE_URL + "/v1/perps/funding_rates"

    def __init__(
        self,
        markets: list[MarketConfig] | tuple[MarketConfig, ...],
        *,
        latest_market_data=None,
        request_json=default_request_json,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        error_handler: CollectorErrorHandler | None = None,
    ) -> None:
        self._markets = markets_for_venue(markets, self.venue)
        self._request_json = request_json
        self._clock = clock
        self._error_handler = error_handler
        self._latest_market_data = latest_market_data

    async def _contracts(self) -> dict[str, dict]:
        raw = await self._request_json(
            self.CONTRACTS_URL, method="GET", params={"sparkline": "false"}
        )
        return parse_ondo_contracts(raw)

    async def collect(self, *, sample_time: datetime, include_hourly_context: bool) -> CollectorBatch:
        if not self._markets:
            return CollectorBatch()
        try:
            contracts = await self._contracts()
        except Exception as error:
            report_collector_error(self._error_handler, self.venue, error)
            return CollectorBatch()
        async def market_snapshot(market: MarketConfig) -> MarketSnapshot | None:
            contract = contracts.get(market.venue_symbol)
            if contract is None:
                if self._latest_market_data is not None:
                    self._latest_market_data.invalidate(venue=self.venue, venue_symbol=market.venue_symbol)
                return None
            try:
                raw = await self._request_json(self.DEPTH_URL, method="GET", params={
                    "market": market.venue_symbol, "depth": 100,
                })
                bids, asks = parse_ondo_book(raw, symbol=market.venue_symbol)
                now = self._clock()
                snapshot = MarketSnapshot(
                    sample_time=sample_time, observed_at=now, venue=self.venue,
                    venue_symbol=market.venue_symbol,
                    canonical_symbol=market.canonical_symbol,
                    best_bid=bids[0].price, best_bid_size=bids[0].base_size,
                    best_ask=asks[0].price, best_ask_size=asks[0].base_size,
                    mark_price=None,  # lastPrice is a trade price, not markPrice
                    index_price=_optional_positive(contract.get("indexPrice")),
                    buy_1k_vwap=buy_vwap(asks, 1000), sell_1k_vwap=sell_vwap(bids, 1000),
                    buy_5k_vwap=buy_vwap(asks, 5000), sell_5k_vwap=sell_vwap(bids, 5000),
                    buy_10k_vwap=buy_vwap(asks, 10000), sell_10k_vwap=sell_vwap(bids, 10000),
                )
                if self._latest_market_data is not None:
                    self._latest_market_data.update_book(
                        venue=self.venue, venue_symbol=market.venue_symbol,
                        bids=bids, asks=asks, observed_at=now,
                    )
                    self._latest_market_data.update_metadata(
                        venue=self.venue, venue_symbol=market.venue_symbol,
                        mark_price=snapshot.mark_price, index_price=snapshot.index_price,
                    )
                return snapshot
            except Exception as error:
                report_collector_error(self._error_handler, self.venue, error)
                if self._latest_market_data is not None:
                    self._latest_market_data.invalidate(
                        venue=self.venue, venue_symbol=market.venue_symbol,
                    )
                return None
        snapshots = await asyncio.gather(*(market_snapshot(m) for m in self._markets))
        hourly = await self._hourly(sample_time, contracts) if include_hourly_context else CollectorBatch()
        return CollectorBatch(
            market_snapshots=tuple(s for s in snapshots if s is not None),
            funding_snapshots=hourly.funding_snapshots,
            hourly_contexts=hourly.hourly_contexts,
        )

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        try:
            contracts = await self._contracts()
            return await self._hourly(sample_time, contracts)
        except Exception as error:
            report_collector_error(self._error_handler, self.venue, error)
            return CollectorBatch()

    async def _hourly(self, sample_time: datetime, contracts: dict[str, dict]) -> CollectorBatch:
        contexts = []
        funding = []
        now = self._clock()
        hourly_time = sample_time.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
        for market in self._markets:
            contract = contracts.get(market.venue_symbol)
            if contract is None:
                continue
            try:
                contexts.append(HourlyContext(
                    sample_time=hourly_time, observed_at=now,
                    venue=self.venue, venue_symbol=market.venue_symbol,
                    canonical_symbol=market.canonical_symbol,
                    open_interest=_optional_non_negative(contract.get("openInterest")),
                    volume_24h=_optional_non_negative(contract.get("usdVolume")),
                ))
                raw = await self._request_json(self.FUNDING_URL, method="GET", params={
                    "market": market.venue_symbol,
                })
                point = _result(raw)
                if not isinstance(point, dict) or point.get("market") != market.venue_symbol:
                    raise ValueError("Ondo funding market mismatch")
                rate = finite_float(point.get("rate"), "funding rate")
                if not point.get("intervalEnds"):
                    raise ValueError("Ondo funding interval end missing")
                effective = _time(point["intervalEnds"])
                funding.append(FundingSnapshot(
                    effective_time=effective, observed_at=self._clock(), venue=self.venue,
                    venue_symbol=market.venue_symbol, canonical_symbol=market.canonical_symbol,
                    funding_rate=rate,
                    next_funding_time=effective if effective > now else None,
                ))
            except Exception as error:
                report_collector_error(self._error_handler, self.venue, error)
        return CollectorBatch(funding_snapshots=tuple(funding), hourly_contexts=tuple(contexts))
