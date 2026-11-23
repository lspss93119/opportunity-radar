import json
from pathlib import Path

import pytest

from radar.config import DiscoveryConfig, MarketConfig, MarketRef, RadarConfig
from radar.discovery import (
    Listing,
    canonical_symbol,
    discover_markets,
    merge_universe,
    parse_hyperliquid_listings,
    parse_lighter_listings,
    parse_variational_listings,
    select_universe,
)

FIXTURES = Path(__file__).parent / "fixtures"
OVERRIDES = {("variational", "US500"): "SPY"}


def load(*parts: str):
    return json.loads(FIXTURES.joinpath(*parts).read_text(encoding="utf-8"))


def listing(venue: str, symbol: str, *, mark: float = 100.0, volume: float = 1e6):
    return Listing(
        venue=venue,
        venue_symbol=symbol,
        canonical_symbol=canonical_symbol(venue, symbol, OVERRIDES),
        mark_price=mark,
        volume_24h_usd=volume,
    )


def config(**discovery: object) -> RadarConfig:
    return RadarConfig(discovery=DiscoveryConfig(enabled=True, **discovery))


@pytest.mark.parametrize(
    ("venue", "symbol", "expected"),
    [
        ("hyperliquid", "kPEPE", "1000PEPE"),  # 1000x unit on Hyperliquid
        ("lighter", "1000PEPE", "1000PEPE"),
        ("trade_xyz", "xyz:TSLA", "TSLA"),
        ("variational", "US500", "SPY"),  # YAML override: Var US500 is SPY
        ("lighter", "US500", "US500"),  # Lighter US500 is the index
        ("hyperliquid", "BTC", "BTC"),
    ],
)
def test_canonical_symbol_normalizes_units_namespaces_and_overrides(
    venue, symbol, expected
):
    assert canonical_symbol(venue, symbol, OVERRIDES) == expected


def test_venue_listing_parsers_read_marks_volumes_and_skip_non_perps():
    lighter = parse_lighter_listings(
        load("lighter", "order_book_details.json"), venue="lighter", overrides={}
    )
    assert [(item.venue_symbol, item.mark_price, item.volume_24h_usd) for item in lighter] == [
        ("BTC", 100.0, 456789.0),
        ("ETH", 2000.0, 567890.0),
        ("SOL", 100.0, 678901.0),
    ]

    variational = parse_variational_listings(
        load("variational", "stats.json"), overrides=OVERRIDES
    )
    # USOILP has funding_interval_s 0 (a swap), so it is not a pairable perp.
    assert [(item.venue_symbol, item.canonical_symbol) for item in variational] == [
        ("AAPL", "AAPL"),
        ("CL", "CL"),
        ("US500", "SPY"),
    ]


def test_hyperliquid_listing_parser_skips_delisted_and_keeps_dex_symbol():
    payload = [
        {
            "universe": [
                {"name": "xyz:TSLA"},
                {"name": "xyz:OLD", "isDelisted": True},
                {"name": "xyz:kBONK"},
            ]
        },
        [
            {"markPx": "400", "oraclePx": "400", "funding": "0", "openInterest": "1", "dayNtlVlm": "5"},
            {"markPx": "1", "oraclePx": "1", "funding": "0", "openInterest": "0", "dayNtlVlm": "0"},
            {"markPx": "0.02", "oraclePx": "0.02", "funding": "0", "openInterest": "1", "dayNtlVlm": "5"},
        ],
    ]

    listings = parse_hyperliquid_listings(payload, venue="trade_xyz", overrides={})

    assert [(item.venue_symbol, item.canonical_symbol) for item in listings] == [
        ("xyz:TSLA", "TSLA"),
        ("xyz:kBONK", "1000BONK"),
    ]


def test_select_universe_requires_liquidity_and_a_counterpart():
    result = select_universe(
        [
            listing("lighter", "VVV"),
            listing("variational", "VVV"),
            listing("hyperliquid", "THIN", volume=99_999),
            listing("variational", "THIN"),
            listing("variational", "LONELY"),
        ],
        config(min_volume_24h_usd=100_000),
    )

    assert [(m.venue, m.canonical_symbol, m.discovered) for m in result.markets] == [
        ("lighter", "VVV", True)
    ]
    assert [(m.venue, m.canonical_symbol) for m in result.quoted_markets] == [
        ("variational", "VVV")
    ]
    reasons = {(e.venue, e.venue_symbol): e.reason for e in result.exclusions}
    assert reasons == {
        ("hyperliquid", "THIN"): "low_volume",
        ("variational", "THIN"): "no_counterpart",
        ("variational", "LONELY"): "no_counterpart",
    }


def test_select_universe_drops_same_ticker_different_asset_by_mark():
    result = select_universe(
        [
            # Majority agrees: the outlier is a different asset with the same ticker.
            listing("lighter", "QNT", mark=147.8, volume=10),  # illiquid, still votes
            listing("trade_xyz", "xyz:QNT", mark=29.2),
            listing("variational", "QNT", mark=148.5),
            listing("hyperliquid", "QNT", mark=148.0),
            # Two venues that disagree: neither side can be trusted.
            listing("trade_xyz", "xyz:CVX", mark=209.0),
            listing("variational", "CVX", mark=1.18),
        ],
        config(),
    )

    kept = {(m.venue, m.venue_symbol) for m in (*result.markets, *result.quoted_markets)}
    assert kept == {("variational", "QNT"), ("hyperliquid", "QNT")}
    mismatched = {
        (e.venue, e.venue_symbol) for e in result.exclusions if e.reason == "mark_mismatch"
    }
    assert mismatched == {
        ("trade_xyz", "xyz:QNT"),
        ("trade_xyz", "xyz:CVX"),
        ("variational", "CVX"),
    }


def test_select_universe_honors_exclusions_and_keeps_yaml_markets_authoritative():
    radar_config = RadarConfig(
        fees_bps={"backpack": 4.0},
        markets=[MarketConfig(venue="backpack", venue_symbol="BTC_USDC_PERP", canonical_symbol="BTC")],
        quoted_markets=[
            MarketConfig(venue="variational", venue_symbol="US500", canonical_symbol="SPY")
        ],
        discovery=DiscoveryConfig(
            enabled=True,
            exclude_markets=[MarketRef(venue="lighter", venue_symbol="ETH")],
        ),
    )
    result = select_universe(
        [
            listing("lighter", "BTC"),  # pairs with the YAML Backpack feed
            listing("variational", "US500", mark=708.0),  # already in YAML
            listing("lighter", "SPY", mark=707.0),
            listing("lighter", "ETH"),
            listing("variational", "ANT"),  # pre-IPO default exclusion
            listing("lighter", "ANT"),
        ],
        radar_config,
    )

    assert [(m.venue, m.canonical_symbol) for m in result.markets] == [
        ("lighter", "BTC"),
        ("lighter", "SPY"),
    ]
    assert result.quoted_markets == ()
    reasons = {(e.venue, e.venue_symbol): e.reason for e in result.exclusions}
    assert reasons[("lighter", "ETH")] == "excluded_market"
    assert reasons[("variational", "ANT")] == "excluded_symbol"
    assert reasons[("lighter", "ANT")] == "excluded_symbol"


def test_merge_universe_replaces_previous_discovered_feeds_and_scopes_monitors():
    yaml_market = MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    base = RadarConfig(markets=[yaml_market])
    first = select_universe(
        [listing("hyperliquid", "BTC"), listing("variational", "BTC")], base
    )
    merged = merge_universe(base, first)
    again = merge_universe(merged, first)

    assert again.markets == merged.markets
    assert [m.venue for m in merged.markets] == ["lighter", "hyperliquid"]
    assert [m.venue for m in merged.quoted_markets] == ["variational"]
    # Discovered feeds join the spread monitor, not the BBO-only manual lifecycle.
    assert merged.monitor_market_identities() == {("lighter", "BTC", "BTC")}
    assert merged.spread_monitor_market_identities() == {
        ("lighter", "BTC", "BTC"),
        ("hyperliquid", "BTC", "BTC"),
        ("variational", "BTC", "BTC"),
    }


@pytest.mark.asyncio
async def test_discovery_aborts_when_any_venue_listing_fails():
    async def request_json(url, *, method, json_body=None, params=None):
        if "variational" in url:
            raise TimeoutError("venue down")
        if "hyperliquid" in url:
            return load("hyperliquid", "meta_and_asset_ctxs.json")
        return load("lighter", "order_book_details.json")

    with pytest.raises(TimeoutError):
        await discover_markets(
            config(venues=["lighter", "hyperliquid", "variational"]),
            request_json=request_json,
        )


@pytest.mark.asyncio
async def test_discovery_lists_each_venue_once_and_pairs_across_them():
    calls: list[tuple[str, object]] = []

    async def request_json(url, *, method, json_body=None, params=None):
        calls.append((url, json_body))
        if "hyperliquid" in url:
            return load("hyperliquid", "meta_and_asset_ctxs.json")
        return load("lighter", "order_book_details.json")

    result = await discover_markets(
        config(venues=["lighter", "hyperliquid"]), request_json=request_json
    )

    assert len(calls) == 2
    assert {(m.venue, m.canonical_symbol) for m in result.markets} == {
        (venue, symbol)
        for venue in ("lighter", "hyperliquid")
        for symbol in ("BTC", "ETH", "SOL")
    }
