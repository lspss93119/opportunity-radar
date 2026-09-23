from pathlib import Path

import pytest
from pydantic import ValidationError

from radar.config import MarketConfig, RadarConfig, load_config


def test_example_config_loads():
    cfg = load_config(Path("config/radar.example.yaml"))
    assert cfg.sampling_seconds == 10
    assert cfg.monitors.spread.primary_size_usd == 10_000
    assert cfg.fees_bps["trade_xyz"] == 9.0
    assert cfg.fees_bps["entropy"] == 9.0
    assert cfg.fees_bps["arcus"] == 2.25
    assert {m.venue for m in cfg.markets} == {
        "lighter",
        "hyperliquid",
        "trade_xyz",
        "entropy",
        "arcus",
    }
    assert {
        (market.venue, market.venue_symbol)
        for market in cfg.markets
    } == {
        (venue, symbol)
        for venue in ("lighter", "hyperliquid")
        for symbol in ("BTC", "ETH", "SOL")
    } | {
        ("lighter", symbol)
        for symbol in ("SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU")
    } | {
        ("trade_xyz", f"xyz:{symbol}")
        for symbol in ("SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU")
    } | {
        ("entropy", "io:SNDK")
    } | {
        ("arcus", f"{symbol}-USD")
        for symbol in ("SNDK", "NVDA", "TSLA", "HOOD", "GOOGL", "AAPL", "META", "MU")
    }


def test_primary_size_is_limited_to_fixed_supported_sizes():
    with pytest.raises(ValidationError):
        RadarConfig.model_validate(
            {
                "sampling_seconds": 10,
                "fees_bps": {"lighter": 4.5},
                "markets": [
                    {
                        "venue": "lighter",
                        "venue_symbol": "BTC",
                        "canonical_symbol": "BTC",
                        "enabled": True,
                    }
                ],
                "monitors": {
                    "spread": {
                        "enabled": True,
                        "interval_seconds": 10,
                        "primary_size_usd": 25_000,
                        "top_n": 3,
                        "candidate_net_bps": 10,
                        "candidate_duration_seconds": 30,
                        "alert_net_bps": 20,
                        "alert_duration_seconds": 120,
                        "stale_after_seconds": 30,
                    }
                },
            }
        )


def test_negative_fee_is_rejected():
    with pytest.raises(ValidationError):
        RadarConfig.model_validate(
            {
                "sampling_seconds": 10,
                "fees_bps": {"lighter": -1},
                "markets": [],
                "monitors": {"spread": {"enabled": False}},
            }
        )


@pytest.mark.parametrize("invalid_fee", [float("nan"), float("inf"), -float("inf")])
def test_non_finite_fee_is_rejected(invalid_fee):
    with pytest.raises(ValidationError):
        RadarConfig.model_validate(
            {
                "sampling_seconds": 10,
                "fees_bps": {"lighter": invalid_fee},
                "markets": [],
                "monitors": {"spread": {"enabled": False}},
            }
        )


def test_boolean_fee_is_rejected():
    with pytest.raises(ValidationError):
        RadarConfig.model_validate(
            {
                "sampling_seconds": 10,
                "fees_bps": {"lighter": True},
                "markets": [],
                "monitors": {"spread": {"enabled": False}},
            }
        )


@pytest.mark.parametrize("field_name", ["candidate_net_bps", "alert_net_bps"])
@pytest.mark.parametrize("invalid_threshold", [float("nan"), float("inf"), -float("inf"), -1.0])
def test_invalid_spread_threshold_is_rejected(field_name, invalid_threshold):
    with pytest.raises(ValidationError):
        RadarConfig.model_validate(
            {
                "sampling_seconds": 10,
                "fees_bps": {},
                "markets": [],
                "monitors": {"spread": {field_name: invalid_threshold}},
            }
        )


def test_sampling_interval_is_fixed_at_ten_seconds():
    with pytest.raises(ValidationError):
        RadarConfig.model_validate({"sampling_seconds": 5})


def test_enabled_trade_xyz_requires_an_explicit_fee():
    with pytest.raises(ValidationError, match="trade_xyz"):
        RadarConfig(
            fees_bps={"hyperliquid": 3.5},
            markets=[
                MarketConfig(
                    venue="trade_xyz",
                    venue_symbol="xyz:TSLA",
                    canonical_symbol="TSLA",
                )
            ],
        )


def test_enabled_trade_xyz_accepts_explicit_fee_case_insensitively():
    config = RadarConfig(
        fees_bps={"TRADE_XYZ": 9.0},
        markets=[
            MarketConfig(
                venue="trade_xyz",
                venue_symbol="xyz:TSLA",
                canonical_symbol="TSLA",
            )
        ],
    )

    assert config.fees_bps == {"TRADE_XYZ": 9.0}


@pytest.mark.parametrize("venue, venue_symbol", [("entropy", "io:SNDK"), ("arcus", "SNDK-USD")])
def test_enabled_new_venue_requires_an_explicit_fee(venue, venue_symbol):
    with pytest.raises(ValidationError, match=venue):
        RadarConfig(
            markets=[
                MarketConfig(
                    venue=venue,
                    venue_symbol=venue_symbol,
                    canonical_symbol="SNDK",
                )
            ]
        )
