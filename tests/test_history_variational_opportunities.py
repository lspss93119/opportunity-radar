from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from radar.config import MarketConfig, RadarConfig
from radar.models import FundingSnapshot, MarketSnapshot, QuotedMarketSnapshot
from radar.storage.parquet import ParquetStorage

AS_OF = datetime(2026, 9, 25, 12, 0, tzinfo=UTC)


def make_config(
    *,
    fees: dict[str, float] | None = None,
    quoted_symbols: tuple[tuple[str, str], ...] = (("BTC", "BTC"),),
) -> RadarConfig:
    return RadarConfig(
        fees_bps={} if fees is None else fees,
        quoted_markets=[
            MarketConfig(
                venue="variational",
                venue_symbol=venue_symbol,
                canonical_symbol=canonical_symbol,
            )
            for venue_symbol, canonical_symbol in quoted_symbols
        ],
    )


def make_quote(
    quote_time: datetime,
    *,
    symbol: str = "BTC",
    bid_1k: float = 101.0,
    ask_1k: float = 100.0,
    fetched_at: datetime | None = None,
    funding_rate: float = 0.001,
    funding_interval_seconds: int = 28_800,
) -> QuotedMarketSnapshot:
    return QuotedMarketSnapshot(
        quote_time=quote_time,
        fetched_at=quote_time + timedelta(seconds=1)
        if fetched_at is None
        else fetched_at,
        venue="variational",
        venue_symbol=symbol,
        canonical_symbol=symbol,
        mark_price=100.0,
        bid_1k=bid_1k,
        ask_1k=ask_1k,
        bid_100k=99.0,
        ask_100k=101.0,
        funding_rate=funding_rate,
        funding_interval_seconds=funding_interval_seconds,
    )


def make_market(
    venue: str,
    sample_time: datetime,
    *,
    canonical_symbol: str = "BTC",
    venue_symbol: str | None = None,
    buy_1k_vwap: float | None = 100.0,
    sell_1k_vwap: float | None = 102.0,
    observed_at: datetime | None = None,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time if observed_at is None else observed_at,
        venue=venue,
        venue_symbol=venue if venue_symbol is None else venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=99.0,
        best_bid_size=10.0,
        best_ask=100.0,
        best_ask_size=10.0,
        buy_1k_vwap=buy_1k_vwap,
        sell_1k_vwap=sell_1k_vwap,
        buy_5k_vwap=1.0,
        sell_5k_vwap=1.0,
        buy_10k_vwap=1.0,
        sell_10k_vwap=1.0,
    )


def make_funding(
    venue: str,
    observed_at: datetime,
    *,
    canonical_symbol: str = "BTC",
    venue_symbol: str | None = None,
    funding_rate: float = 0.002,
) -> FundingSnapshot:
    return FundingSnapshot(
        effective_time=observed_at,
        observed_at=observed_at,
        venue=venue,
        venue_symbol=venue if venue_symbol is None else venue_symbol,
        canonical_symbol=canonical_symbol,
        funding_rate=funding_rate,
    )


def write_data(
    tmp_path: Path,
    *,
    quotes: list[QuotedMarketSnapshot],
    markets: list[MarketSnapshot],
    fundings: list[FundingSnapshot] | None = None,
) -> None:
    store = ParquetStorage(tmp_path / "data")
    for value in (*quotes, *markets, *(fundings or [])):
        store.append(value)
    timestamps = [
        *(quote.quote_time for quote in quotes),
        *(market.sample_time for market in markets),
        *(funding.effective_time for funding in (fundings or [])),
    ]
    store.flush(now=max(timestamps, default=AS_OF) + timedelta(days=1))


def build_report(tmp_path: Path, config: RadarConfig, **kwargs: object):
    from radar.history.variational_opportunities import (
        build_variational_opportunity_report,
    )

    return build_variational_opportunity_report(
        tmp_path / "data",
        config,
        **kwargs,
    )


def test_report_evaluates_both_directions_using_only_1k_and_other_fee(tmp_path):
    write_data(
        tmp_path,
        quotes=[make_quote(AS_OF)],
        markets=[make_market("arcus", AS_OF - timedelta(seconds=10))],
        fundings=[make_funding("arcus", AS_OF - timedelta(hours=1))],
    )

    report = build_report(tmp_path, make_config(fees={"arcus": 2.0}))

    assert len(report.observations) == 2
    long_variational = next(
        observation
        for observation in report.observations
        if observation.direction == "variational_long"
    )
    long_other = next(
        observation
        for observation in report.observations
        if observation.direction == "other_long"
    )
    assert long_variational.long_entry_price == 100.0
    assert long_variational.short_entry_price == 102.0
    assert long_variational.raw_spread_bps == pytest.approx(200.0)
    assert long_variational.indicative_net_bps == pytest.approx(198.0)
    assert long_other.long_entry_price == 100.0
    assert long_other.short_entry_price == 101.0
    assert long_other.indicative_net_bps == pytest.approx(98.0)
    assert long_variational.other_funding_rate == pytest.approx(0.002)
    assert long_variational.variational_funding_interval_seconds == 28_800


def test_matching_is_backward_only_and_requires_at_most_20_seconds(tmp_path):
    write_data(
        tmp_path,
        quotes=[make_quote(AS_OF)],
        markets=[
            make_market("future", AS_OF + timedelta(seconds=1)),
            make_market("stale", AS_OF - timedelta(seconds=21)),
            make_market("fresh", AS_OF - timedelta(seconds=20)),
        ],
    )

    report = build_report(
        tmp_path,
        make_config(fees={"future": 0.0, "stale": 0.0, "fresh": 0.0}),
    )

    assert {observation.other_venue for observation in report.observations} == {
        "fresh"
    }
    assert report.observations[0].market_age_at_quote_ms == pytest.approx(20_000.0)
    assert report.unmatched_observations == 2


def test_duplicate_variational_quote_time_is_counted_once_using_latest_fetch(tmp_path):
    write_data(
        tmp_path,
        quotes=[
            make_quote(AS_OF, bid_1k=101.0, fetched_at=AS_OF + timedelta(seconds=1)),
            make_quote(AS_OF, bid_1k=103.0, fetched_at=AS_OF + timedelta(seconds=2)),
        ],
        markets=[make_market("arcus", AS_OF - timedelta(seconds=10))],
    )

    report = build_report(tmp_path, make_config(fees={"arcus": 0.0}))

    assert report.total_quote_observations == 1
    assert len(report.observations) == 2
    other_long = next(
        observation
        for observation in report.observations
        if observation.direction == "other_long"
    )
    assert other_long.short_entry_price == 103.0


def test_min_net_filter_does_not_change_statistics_or_persistence_population(tmp_path):
    write_data(
        tmp_path,
        quotes=[
            make_quote(AS_OF, ask_1k=100.0),
            make_quote(AS_OF + timedelta(seconds=10), ask_1k=100.0),
        ],
        markets=[
            make_market("arcus", AS_OF - timedelta(seconds=10), sell_1k_vwap=100.5),
            make_market(
                "arcus",
                AS_OF + timedelta(seconds=10),
                sell_1k_vwap=102.5,
            ),
        ],
    )

    report = build_report(
        tmp_path,
        make_config(fees={"arcus": 2.0}),
        min_net_bps=200.0,
    )

    summary = next(
        summary
        for summary in report.pair_summaries
        if summary.direction == "variational_long"
    )
    assert report.display_min_net_bps == 200.0
    assert len(report.top_opportunities) == 1
    assert summary.sample_count == 2
    assert summary.median_net_bps == pytest.approx(148.0)
    assert summary.p90_net_bps == pytest.approx(228.0)
    assert summary.p95_net_bps == pytest.approx(238.0)
    assert summary.max_net_bps == pytest.approx(248.0)
    assert summary.positive_percent == pytest.approx(100.0)
    assert summary.count_ge_10 == 2
    assert summary.count_ge_20 == 2
    assert summary.count_ge_50 == 1
    threshold_10 = [
        episode
        for episode in report.persistence_episodes
        if episode.threshold_bps == 10.0
        and episode.direction == "variational_long"
    ]
    assert len(threshold_10) == 1
    assert threshold_10[0].quote_update_count == 2


def test_persistence_uses_quote_updates_not_intermediate_market_rows(tmp_path):
    quote_times = [AS_OF, AS_OF + timedelta(seconds=30)]
    write_data(
        tmp_path,
        quotes=[make_quote(quote_time) for quote_time in quote_times],
        markets=[
            make_market("arcus", AS_OF + timedelta(seconds=offset), sell_1k_vwap=102.0)
            for offset in (-10, 0, 10, 20, 30)
        ],
    )

    report = build_report(tmp_path, make_config(fees={"arcus": 0.0}))

    episodes = [
        episode
        for episode in report.persistence_episodes
        if episode.threshold_bps == 10.0
        and episode.direction == "variational_long"
    ]
    assert len(episodes) == 1
    assert episodes[0].quote_update_count == 2
    assert episodes[0].duration_seconds == 30.0


def test_unmatched_variational_quote_breaks_persistence_episode(tmp_path):
    quote_times = [
        AS_OF,
        AS_OF + timedelta(seconds=30),
        AS_OF + timedelta(seconds=60),
    ]
    write_data(
        tmp_path,
        quotes=[make_quote(quote_time) for quote_time in quote_times],
        markets=[
            make_market("arcus", AS_OF, sell_1k_vwap=102.0),
            make_market("arcus", AS_OF + timedelta(seconds=60), sell_1k_vwap=102.0),
        ],
    )

    report = build_report(tmp_path, make_config(fees={"arcus": 0.0}))

    episodes = [
        episode
        for episode in report.persistence_episodes
        if episode.threshold_bps == 10.0
        and episode.direction == "variational_long"
    ]
    assert len(episodes) == 2
    assert [episode.quote_update_count for episode in episodes] == [1, 1]


def test_symbol_venue_and_fee_filters_fail_closed(tmp_path):
    write_data(
        tmp_path,
        quotes=[
            make_quote(AS_OF, symbol="BTC"),
            make_quote(AS_OF, symbol="ETH"),
        ],
        markets=[
            make_market("arcus", AS_OF - timedelta(seconds=10), canonical_symbol="BTC"),
            make_market("backpack", AS_OF - timedelta(seconds=10), canonical_symbol="BTC"),
            make_market("unknown", AS_OF - timedelta(seconds=10), canonical_symbol="ETH"),
        ],
    )

    report = build_report(
        tmp_path,
        make_config(
            fees={"arcus": 0.0, "backpack": 1.0},
            quoted_symbols=(("BTC", "BTC"), ("ETH", "ETH")),
        ),
        symbol="btc",
        other_venue="arcus",
    )

    assert report.symbol_count == 1
    assert {observation.other_venue for observation in report.observations} == {
        "arcus"
    }
    assert all(observation.canonical_symbol == "BTC" for observation in report.observations)


def test_cli_formats_five_sections_without_runtime_or_live_dependencies(
    tmp_path, monkeypatch, capsys
):
    write_data(
        tmp_path,
        quotes=[make_quote(AS_OF)],
        markets=[make_market("arcus", AS_OF - timedelta(seconds=10))],
        fundings=[make_funding("arcus", AS_OF - timedelta(hours=1))],
    )
    config_path = tmp_path / "radar.yaml"
    config_path.write_text(
        """
fees_bps:
  arcus: 2
quoted_markets:
  - venue: variational
    venue_symbol: BTC
    canonical_symbol: BTC
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RADAR_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("RADAR_TELEGRAM_CHAT_ID", raising=False)

    from radar.app import main

    assert (
        main(
            [
                "variational-opportunities",
                "--config",
                str(config_path),
                "--hours",
                "24",
            ]
        )
        == 0
    )

    output = capsys.readouterr().out
    assert "VARIATIONAL $1K DISCOVERY REPORT" in output
    assert "1. CURRENT / LATEST" in output
    assert "2. TOP OPPORTUNITIES" in output
    assert "3. PAIR SUMMARY" in output
    assert "4. PERSISTENCE" in output
    assert "5. SUMMARY" in output
    assert "INDICATIVE NET SPREAD" in output
    assert "public metadata $1k quote" in output
    assert "0.001/28800s" in output
    assert "0.002/stored" in output
    assert not (tmp_path / "runtime.db").exists()
