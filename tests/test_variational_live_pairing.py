from datetime import UTC, datetime, timedelta

import pytest

from radar.collectors.base import CollectorBatch
from radar.collectors.variational import variational_market_snapshot
from radar.config import MarketConfig, RadarConfig
from radar.history.spread import SpreadHistory
from radar.models import MarketSnapshot, QuotedMarketSnapshot
from radar.monitors.manual_opportunity import ManualOpportunityMonitor
from radar.monitors.spread.models import build_spread_candidates, parse_price_source
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)


def make_quote(*, age_seconds: float = 30.0, bid: float = 99.0, ask: float = 100.0):
    quote_time = NOW - timedelta(seconds=age_seconds)
    return QuotedMarketSnapshot(
        quote_time=quote_time,
        fetched_at=quote_time,
        venue="variational",
        venue_symbol="BTC",
        canonical_symbol="BTC",
        mark_price=99.5,
        bid_1k=bid,
        ask_1k=ask,
        bid_100k=98.0,
        ask_100k=101.0,
        funding_rate=0.1095,
        funding_interval_seconds=28_800,
    )


def make_book(*, venue: str = "lighter", age_seconds: float = 1.0) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=NOW,
        observed_at=NOW - timedelta(seconds=age_seconds),
        venue=venue,
        venue_symbol="BTC",
        canonical_symbol="BTC",
        best_bid=101.0,
        best_bid_size=5.0,
        best_ask=101.5,
        best_ask_size=5.0,
        buy_1k_vwap=101.5,
        sell_1k_vwap=101.0,
    )


def test_rfq_quote_maps_to_the_same_size_to_price_interface():
    snapshot = variational_market_snapshot(make_quote(), sample_time=NOW)

    assert snapshot.price_source == "rfq"
    assert snapshot.buy_1k_vwap == 100.0
    assert snapshot.sell_1k_vwap == 99.0
    assert snapshot.best_ask == 100.0
    assert snapshot.best_bid == 99.0
    # Only $1k is quoted at a monitored size; larger sizes stay unavailable.
    assert snapshot.buy_5k_vwap is None
    assert snapshot.buy_10k_vwap is None
    # Staleness follows quotes.updated_at, not the radar sample time.
    assert snapshot.observed_at == NOW - timedelta(seconds=30)
    assert snapshot.sample_time == NOW


def test_crossed_rfq_quote_is_rejected():
    with pytest.raises(ValueError):
        variational_market_snapshot(make_quote(bid=100.0, ask=100.0), sample_time=NOW)


def test_rfq_staleness_uses_its_own_limit_and_tags_candidate_sources():
    rfq = variational_market_snapshot(make_quote(age_seconds=100), sample_time=NOW)
    candidates = build_spread_candidates(
        [rfq, make_book()],
        NOW,
        primary_size_usd=1_000,
        stale_after_seconds=30,
        rfq_stale_after_seconds=120,
        fees_bps={},
        require_fees=False,
    )

    by_long = {candidate.key.long_venue: candidate for candidate in candidates}
    assert by_long["variational"].long_price_source == "rfq"
    assert by_long["variational"].short_price_source == "order_book"
    assert by_long["lighter"].short_price_source == "rfq"

    expired = variational_market_snapshot(make_quote(age_seconds=121), sample_time=NOW)
    assert (
        build_spread_candidates(
            [expired, make_book()],
            NOW,
            primary_size_usd=1_000,
            stale_after_seconds=30,
            rfq_stale_after_seconds=120,
            fees_bps={},
            require_fees=False,
        )
        == ()
    )


def test_order_book_does_not_inherit_the_rfq_staleness_limit():
    candidates = build_spread_candidates(
        [
            variational_market_snapshot(make_quote(), sample_time=NOW),
            make_book(age_seconds=60),
        ],
        NOW,
        primary_size_usd=1_000,
        stale_after_seconds=30,
        rfq_stale_after_seconds=120,
        fees_bps={},
        require_fees=False,
    )
    assert candidates == ()


@pytest.mark.asyncio
async def test_pipeline_adds_only_fresh_rfq_quotes_to_state_and_storage(tmp_path):
    storage = ParquetStorage(tmp_path / "data")
    state = RadarState()
    pipeline = MarketDataPipeline(
        [], state, clock=lambda: NOW, storage=storage, rfq_stale_after_seconds=120
    )
    fresh = make_quote(age_seconds=60)
    stale = make_quote(age_seconds=130).model_copy(
        update={"venue_symbol": "ETH", "canonical_symbol": "ETH"}
    )
    pipeline._latest_quotes = {"BTC": fresh, "ETH": stale}

    batch = await pipeline.collect_once(now=NOW)

    assert [(s.venue, s.canonical_symbol, s.price_source) for s in batch.market_snapshots] == [
        ("variational", "BTC", "rfq")
    ]
    assert [s.venue for s in state.markets] == ["variational"]
    assert pipeline.flush_storage(now=NOW) == 1


def test_spread_scope_includes_rfq_feeds_but_manual_scope_does_not():
    config = RadarConfig(
        markets=[MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")],
        quoted_markets=[
            MarketConfig(venue="variational", venue_symbol="BTC", canonical_symbol="BTC"),
            MarketConfig(
                venue="variational",
                venue_symbol="ETH",
                canonical_symbol="ETH",
                monitor_enabled=False,
            ),
        ],
    )

    assert config.spread_monitor_market_identities() == {
        ("lighter", "BTC", "BTC"),
        ("variational", "BTC", "BTC"),
    }
    assert config.monitor_market_identities() == {("lighter", "BTC", "BTC")}


@pytest.mark.asyncio
async def test_manual_bbo_monitor_ignores_rfq_snapshots_even_without_scope():
    config = RadarConfig(manual_opportunity={"enabled": True})
    monitor = ManualOpportunityMonitor(config.manual_opportunity, {"lighter": 0.0})
    state = RadarState()
    state.apply(
        CollectorBatch(
            market_snapshots=(
                make_book(),
                variational_market_snapshot(make_quote(age_seconds=1), sample_time=NOW),
            )
        )
    )

    assert [s.venue for s in monitor._scoped_markets(state)] == ["lighter"]


def test_persisted_state_without_price_source_is_order_book():
    assert parse_price_source(None) == "order_book"
    assert parse_price_source("rfq") == "rfq"
    with pytest.raises(ValueError):
        parse_price_source("guess")


def test_history_hydration_keeps_rfq_rows_within_rfq_limit(tmp_path):
    storage = ParquetStorage(tmp_path / "data")
    sample_times = [NOW - timedelta(minutes=10) + timedelta(seconds=10 * i) for i in range(3)]
    for sample_time in sample_times:
        storage.append(
            variational_market_snapshot(
                make_quote().model_copy(
                    update={"quote_time": sample_time - timedelta(seconds=90)}
                ),
                sample_time=sample_time,
            )
        )
        storage.append(make_book().model_copy(update={"sample_time": sample_time, "observed_at": sample_time}))
    storage.flush(now=NOW)
    history = SpreadHistory(tmp_path / "data")

    with_rfq_limit = history.load_recent_pair_points(
        primary_size_usd=1_000,
        as_of=NOW,
        stale_after_seconds=30,
        rfq_stale_after_seconds=120,
    )
    book_limit_only = history.load_recent_pair_points(
        primary_size_usd=1_000, as_of=NOW, stale_after_seconds=30
    )

    assert {key.long_venue for key in with_rfq_limit} == {"lighter", "variational"}
    assert all(len(points) == 3 for points in with_rfq_limit.values())
    assert book_limit_only == {}


def test_persisted_spread_and_anomaly_state_round_trip_price_sources():
    from radar.monitors.spread.anomaly_v2 import _candidate_from_dict, _candidate_to_dict
    from radar.monitors.spread.monitor import SpreadEpisode, SpreadMonitor

    candidate = build_spread_candidates(
        [variational_market_snapshot(make_quote(), sample_time=NOW), make_book()],
        NOW,
        primary_size_usd=1_000,
        stale_after_seconds=30,
        rfq_stale_after_seconds=120,
        fees_bps={},
        require_fees=False,
    )[0]
    episode = SpreadEpisode(
        key=candidate.key,
        episode_id="episode",
        first_seen_at=NOW,
        last_seen_at=NOW,
        candidate=candidate,
    )

    restored = SpreadMonitor._deserialize_episode(SpreadMonitor._serialize_episode(episode))
    assert restored.candidate == candidate
    assert _candidate_from_dict(_candidate_to_dict(candidate)) == candidate

    legacy = SpreadMonitor._serialize_episode(episode)
    del legacy["candidate"]["long_price_source"]
    del legacy["candidate"]["short_price_source"]
    legacy_candidate = SpreadMonitor._deserialize_episode(legacy).candidate
    assert legacy_candidate.long_price_source == "order_book"
    assert legacy_candidate.short_price_source == "order_book"
