from pathlib import Path

import pytest
from pydantic import ValidationError

from radar.config import RadarConfig, load_config


def test_example_config_loads():
    cfg = load_config(Path("config/radar.example.yaml"))
    assert cfg.sampling_seconds == 10
    assert cfg.monitors.spread.primary_size_usd == 10_000
    assert {m.venue for m in cfg.markets} == {"lighter", "hyperliquid"}
    assert {
        (market.venue, market.venue_symbol)
        for market in cfg.markets
    } == {
        (venue, symbol)
        for venue in ("lighter", "hyperliquid")
        for symbol in ("BTC", "ETH", "SOL")
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


def test_sampling_interval_is_fixed_at_ten_seconds():
    with pytest.raises(ValidationError):
        RadarConfig.model_validate({"sampling_seconds": 5})
