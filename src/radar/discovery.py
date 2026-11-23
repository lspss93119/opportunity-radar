"""Automatic market discovery and canonical pairing for public perp venues.

Discovery only decides which venue feeds to collect; it never evaluates
opportunities.  It fails closed: a feed whose identity cannot be trusted
(excluded, illiquid, no counterpart, or a mark that disagrees with the rest
of its canonical group) is left out and reported instead of guessed.
"""

from __future__ import annotations

import asyncio
import logging
import re
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from radar.collectors.base import finite_float, non_negative_float, positive_float
from radar.collectors.http import request_json as default_request_json
from radar.collectors.hyperliquid import (
    HyperliquidCollector,
    parse_hyperliquid_meta_and_asset_ctxs,
)
from radar.collectors.lighter import (
    LIGHTER_ROBINHOOD_BASE_URL,
    LighterCollector,
    parse_lighter_order_book_details,
)
from radar.collectors.variational import VARIATIONAL_STATS_URL, VARIATIONAL_VENUE
from radar.config import DiscoveryConfig, MarketConfig, RadarConfig

LOGGER = logging.getLogger(__name__)

# Hyperliquid quotes 1000x-denominated coins as kPEPE; Lighter/Var use 1000PEPE.
_THOUSAND_PREFIX = re.compile(r"^k([A-Z0-9]+)$")
_HYPERLIQUID_DEX = {"hyperliquid": None, "trade_xyz": "xyz"}


@dataclass(frozen=True)
class Listing:
    venue: str
    venue_symbol: str
    canonical_symbol: str
    mark_price: float
    volume_24h_usd: float


@dataclass(frozen=True)
class Exclusion:
    venue: str
    venue_symbol: str
    canonical_symbol: str
    reason: str


@dataclass(frozen=True)
class DiscoveryResult:
    markets: tuple[MarketConfig, ...]
    quoted_markets: tuple[MarketConfig, ...]
    exclusions: tuple[Exclusion, ...]


def canonical_symbol(
    venue: str,
    venue_symbol: str,
    overrides: Mapping[tuple[str, str], str],
) -> str:
    override = overrides.get((venue, venue_symbol))
    if override is not None:
        return override
    base = venue_symbol.split(":", 1)[1] if ":" in venue_symbol else venue_symbol
    thousand = _THOUSAND_PREFIX.fullmatch(base)
    return f"1000{thousand.group(1)}" if thousand else base


def _overrides(config: DiscoveryConfig) -> dict[tuple[str, str], str]:
    return {
        (override.venue, override.venue_symbol): override.canonical_symbol
        for override in config.symbol_overrides
    }


def parse_lighter_listings(
    payload: object, *, venue: str, overrides: Mapping[tuple[str, str], str]
) -> tuple[Listing, ...]:
    details = parse_lighter_order_book_details(payload)
    return tuple(
        Listing(
            venue=venue,
            venue_symbol=symbol,
            canonical_symbol=canonical_symbol(venue, symbol, overrides),
            mark_price=detail.mark_price,
            volume_24h_usd=detail.volume_24h,
        )
        for symbol, detail in sorted(details.items())
    )


def parse_hyperliquid_listings(
    payload: object, *, venue: str, overrides: Mapping[tuple[str, str], str]
) -> tuple[Listing, ...]:
    contexts = parse_hyperliquid_meta_and_asset_ctxs(payload)
    universe = payload[0]["universe"]  # shape validated by the parser above
    delisted = {
        market.get("name")
        for market in universe
        if isinstance(market, dict) and market.get("isDelisted") is True
    }
    return tuple(
        Listing(
            venue=venue,
            venue_symbol=name,
            canonical_symbol=canonical_symbol(venue, name, overrides),
            mark_price=context.mark_price,
            volume_24h_usd=context.volume_24h,
        )
        for name, context in sorted(contexts.items())
        if name not in delisted
    )


def parse_variational_listings(
    payload: object, *, overrides: Mapping[tuple[str, str], str]
) -> tuple[Listing, ...]:
    if not isinstance(payload, dict) or not isinstance(payload.get("listings"), list):
        raise ValueError("Variational stats response has invalid listings")
    listings: dict[str, Listing] = {}
    for raw in payload["listings"]:
        if not isinstance(raw, dict):
            continue
        ticker = raw.get("ticker")
        if not isinstance(ticker, str) or not ticker or ticker in listings:
            continue
        try:
            # Interval 0 marks non-perpetual swaps, which the radar does not pair.
            if finite_float(raw.get("funding_interval_s"), "funding_interval_s") <= 0:
                continue
            mark_price = positive_float(raw.get("mark_price"), "mark_price")
            volume = non_negative_float(raw.get("volume_24h"), "volume_24h")
        except (TypeError, ValueError):
            continue
        listings[ticker] = Listing(
            venue=VARIATIONAL_VENUE,
            venue_symbol=ticker,
            canonical_symbol=canonical_symbol(VARIATIONAL_VENUE, ticker, overrides),
            mark_price=mark_price,
            volume_24h_usd=volume,
        )
    return tuple(listings[ticker] for ticker in sorted(listings))


def select_universe(
    listings: Sequence[Listing], config: RadarConfig
) -> DiscoveryResult:
    """Choose discovered feeds that can be paired safely with another venue."""
    discovery = config.discovery
    configured = [
        market
        for market in (*config.markets, *config.quoted_markets)
        if market.enabled and not market.discovered
    ]
    configured_keys = {(market.venue, market.venue_symbol) for market in configured}
    excluded_markets = {
        (market.venue, market.venue_symbol) for market in discovery.exclude_markets
    }
    excluded_symbols = {symbol.upper() for symbol in discovery.exclude_symbols}

    exclusions: list[Exclusion] = []
    candidates: list[Listing] = []
    # Configured feeds on discovered venues still contribute their live mark to
    # the identity check, under the canonical symbol the YAML gives them.
    configured_canonical = {
        (market.venue, market.venue_symbol): market.canonical_symbol
        for market in configured
    }
    marks: dict[str, list[float]] = defaultdict(list)
    for listing in listings:
        key = (listing.venue, listing.venue_symbol)
        if key in configured_keys:
            marks[configured_canonical[key]].append(listing.mark_price)
            continue
        reason = None
        if key in excluded_markets:
            reason = "excluded_market"
        elif listing.canonical_symbol.upper() in excluded_symbols:
            reason = "excluded_symbol"
        if reason is not None:
            exclusions.append(_exclusion(listing, reason))
            continue
        # Illiquid listings still vote on what the ticker's asset is worth.
        marks[listing.canonical_symbol].append(listing.mark_price)
        if listing.volume_24h_usd < discovery.min_volume_24h_usd:
            exclusions.append(_exclusion(listing, "low_volume"))
            continue
        candidates.append(listing)

    trusted: list[Listing] = []
    for listing in candidates:
        median = statistics.median(marks[listing.canonical_symbol])
        if abs(listing.mark_price / median - 1.0) > discovery.max_mark_deviation:
            exclusions.append(_exclusion(listing, "mark_mismatch"))
        else:
            trusted.append(listing)

    venues_by_symbol: dict[str, set[str]] = defaultdict(set)
    for market in configured:
        venues_by_symbol[market.canonical_symbol].add(market.venue)
    for listing in trusted:
        venues_by_symbol[listing.canonical_symbol].add(listing.venue)

    markets: list[MarketConfig] = []
    quoted: list[MarketConfig] = []
    for listing in trusted:
        if len(venues_by_symbol[listing.canonical_symbol]) < 2:
            exclusions.append(_exclusion(listing, "no_counterpart"))
            continue
        long_tail = (
            listing.venue == VARIATIONAL_VENUE
            or listing.volume_24h_usd < discovery.full_rate_min_volume_usd
        )
        market = MarketConfig(
            venue=listing.venue,
            venue_symbol=listing.venue_symbol,
            canonical_symbol=listing.canonical_symbol,
            discovered=True,
            sample_seconds=discovery.long_tail_sample_seconds if long_tail else 10,
        )
        (quoted if listing.venue == VARIATIONAL_VENUE else markets).append(market)
    return DiscoveryResult(
        markets=tuple(markets),
        quoted_markets=tuple(quoted),
        exclusions=tuple(exclusions),
    )


def _exclusion(listing: Listing, reason: str) -> Exclusion:
    return Exclusion(
        venue=listing.venue,
        venue_symbol=listing.venue_symbol,
        canonical_symbol=listing.canonical_symbol,
        reason=reason,
    )


def merge_universe(config: RadarConfig, result: DiscoveryResult) -> RadarConfig:
    """Configured markets stay as written; discovered feeds are appended."""
    return config.model_copy(
        update={
            "markets": [
                *(market for market in config.markets if not market.discovered),
                *result.markets,
            ],
            "quoted_markets": [
                *(market for market in config.quoted_markets if not market.discovered),
                *result.quoted_markets,
            ],
        }
    )


async def _fetch_listings(
    venue: str,
    overrides: Mapping[tuple[str, str], str],
    request_json,
) -> tuple[Listing, ...]:
    if venue in ("lighter", "lighter_robinhood"):
        base_url = (
            LighterCollector.BASE_URL if venue == "lighter" else LIGHTER_ROBINHOOD_BASE_URL
        )
        payload = await request_json(
            f"{base_url}/api/v1/orderBookDetails",
            method="GET",
            params={"filter": "perp"},
        )
        return parse_lighter_listings(payload, venue=venue, overrides=overrides)
    if venue in _HYPERLIQUID_DEX:
        body: dict[str, object] = {"type": "metaAndAssetCtxs"}
        dex = _HYPERLIQUID_DEX[venue]
        if dex is not None:
            body["dex"] = dex
        payload = await request_json(
            HyperliquidCollector.INFO_URL, method="POST", json_body=body
        )
        return parse_hyperliquid_listings(payload, venue=venue, overrides=overrides)
    if venue == VARIATIONAL_VENUE:
        payload = await request_json(VARIATIONAL_STATS_URL, method="GET")
        return parse_variational_listings(payload, overrides=overrides)
    raise ValueError(f"unsupported discovery venue: {venue}")


async def discover_markets(
    config: RadarConfig, *, request_json=default_request_json
) -> DiscoveryResult:
    """List every enabled discovery venue; any venue failure aborts the scan.

    Aborting keeps callers on their last known-good universe instead of
    silently dropping a venue's markets because one request failed.
    """
    overrides = _overrides(config.discovery)
    results = await asyncio.gather(
        *(
            _fetch_listings(venue, overrides, request_json)
            for venue in dict.fromkeys(config.discovery.venues)
        )
    )
    listings = [listing for venue_listings in results for listing in venue_listings]
    result = select_universe(listings, config)
    reasons: dict[str, int] = defaultdict(int)
    for exclusion in result.exclusions:
        reasons[exclusion.reason] += 1
    LOGGER.info(
        "market discovery listings=%d markets=%d quoted_markets=%d exclusions=%s",
        len(listings),
        len(result.markets),
        len(result.quoted_markets),
        dict(sorted(reasons.items())),
    )
    for exclusion in result.exclusions:
        if exclusion.reason == "mark_mismatch":
            LOGGER.warning(
                "market discovery mark mismatch venue=%s venue_symbol=%s canonical=%s",
                exclusion.venue,
                exclusion.venue_symbol,
                exclusion.canonical_symbol,
            )
    return result
