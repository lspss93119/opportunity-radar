from datetime import datetime, timedelta, timezone

import pytest

from radar.config import MarketConfig
from radar.market_data import LatestMarketData, LatestMarketView
from radar.vwap import BookLevel

UTC = timezone.utc
OBSERVED_AT = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)
SAMPLE_TIME = datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
NOW = datetime(2026, 9, 15, 10, 0, 19, tzinfo=UTC)


@pytest.fixture
def markets() -> tuple[MarketConfig, ...]:
    return (
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(
            venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"
        ),
    )


@pytest.fixture
def complete_book() -> tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]]:
    return (
        (BookLevel(98.0, 100.0), BookLevel(99.0, 200.0)),
        (BookLevel(102.0, 100.0), BookLevel(101.0, 200.0)),
    )


def seed_book(
    latest: LatestMarketData,
    *,
    venue: str = "lighter",
    bids: tuple[BookLevel, ...] | None = None,
    asks: tuple[BookLevel, ...] | None = None,
    observed_at: datetime = OBSERVED_AT,
) -> None:
    default_bids, default_asks = (
        (BookLevel(99.0, 200.0),),
        (BookLevel(101.0, 200.0),),
    )
    latest.update_book(
        venue=venue,
        venue_symbol="BTC",
        bids=default_bids if bids is None else bids,
        asks=default_asks if asks is None else asks,
        observed_at=observed_at,
    )


def test_build_batch_uses_source_observed_at_and_all_vwap_tiers(
    markets: tuple[MarketConfig, ...],
    complete_book: tuple[tuple[BookLevel, ...], tuple[BookLevel, ...]],
):
    bids, asks = complete_book
    latest = LatestMarketData()
    seed_book(latest, bids=bids, asks=asks)
    latest.update_metadata(
        venue="lighter",
        venue_symbol="BTC",
        mark_price=100.0,
        index_price=100.5,
    )

    batch = latest.build_batch(
        markets,
        sample_time=SAMPLE_TIME,
        now=NOW,
        stale_after_seconds=30,
    )

    assert len(batch.market_snapshots) == 1
    snapshot = batch.market_snapshots[0]
    assert snapshot.sample_time == SAMPLE_TIME
    assert snapshot.observed_at == OBSERVED_AT
    assert snapshot.best_bid == 99.0
    assert snapshot.best_ask == 101.0
    assert snapshot.buy_1k_vwap == pytest.approx(101.0)
    assert snapshot.sell_1k_vwap == pytest.approx(99.0)
    assert snapshot.buy_5k_vwap == pytest.approx(101.0)
    assert snapshot.sell_5k_vwap == pytest.approx(99.0)
    assert snapshot.buy_10k_vwap == pytest.approx(101.0)
    assert snapshot.sell_10k_vwap == pytest.approx(99.0)
    assert snapshot.mark_price == 100.0
    assert snapshot.index_price == 100.5


def test_build_batch_omits_book_with_missing_depth_side(
    markets: tuple[MarketConfig, ...],
):
    latest = LatestMarketData()

    with pytest.raises(ValueError, match="non-empty"):
        latest.update_book(
            venue="lighter",
            venue_symbol="BTC",
            bids=(),
            asks=(BookLevel(101.0, 200.0),),
            observed_at=OBSERVED_AT,
        )

    assert latest.build_batch(
        markets,
        sample_time=SAMPLE_TIME,
        now=NOW,
        stale_after_seconds=30,
    ).market_snapshots == ()


def test_invalidate_clears_ready_book_and_preserves_metadata(
    markets: tuple[MarketConfig, ...],
):
    latest = LatestMarketData()
    seed_book(latest)
    latest.update_metadata(
        venue="lighter",
        venue_symbol="BTC",
        mark_price=100.0,
        index_price=100.5,
    )

    latest.invalidate(venue="lighter", venue_symbol="BTC")

    view = latest._views[("lighter", "BTC")]
    assert isinstance(view, LatestMarketView)
    assert view.bids == ()
    assert view.asks == ()
    assert view.ready is False
    assert view.mark_price == 100.0
    assert view.index_price == 100.5
    assert latest.build_batch(
        markets,
        sample_time=SAMPLE_TIME,
        now=NOW,
        stale_after_seconds=30,
    ).market_snapshots == ()


@pytest.mark.parametrize(
    ("observed_at", "match"),
    [
        (datetime(2026, 9, 15, 10, 0, 8), "timezone-aware UTC"),
        (
            datetime(
                2026,
                9,
                15,
                18,
                0,
                8,
                tzinfo=timezone(timedelta(hours=8)),
            ),
            "timezone-aware UTC",
        ),
    ],
)
def test_update_book_rejects_non_utc_observation(observed_at: datetime, match: str):
    latest = LatestMarketData()
    with pytest.raises(ValueError, match=match):
        seed_book(latest, observed_at=observed_at)


def test_build_batch_omits_future_observation(markets: tuple[MarketConfig, ...]):
    latest = LatestMarketData()
    seed_book(
        latest,
        observed_at=datetime(2026, 9, 15, 10, 0, 20, tzinfo=UTC),
    )

    batch = latest.build_batch(
        markets,
        sample_time=SAMPLE_TIME,
        now=NOW,
        stale_after_seconds=30,
    )

    assert batch.market_snapshots == ()


def test_build_batch_omits_stale_observation(markets: tuple[MarketConfig, ...]):
    latest = LatestMarketData()
    seed_book(
        latest,
        observed_at=datetime(2026, 9, 15, 9, 59, 48, tzinfo=UTC),
    )

    batch = latest.build_batch(
        markets,
        sample_time=SAMPLE_TIME,
        now=NOW,
        stale_after_seconds=30,
    )

    assert batch.market_snapshots == ()


@pytest.mark.parametrize(
    ("bids", "asks"),
    [
        ((BookLevel(101.0, 200.0),), (BookLevel(101.0, 200.0),)),
        ((BookLevel(102.0, 200.0),), (BookLevel(101.0, 200.0),)),
    ],
)
def test_update_book_rejects_locked_or_crossed_bbo(
    bids: tuple[BookLevel, ...],
    asks: tuple[BookLevel, ...],
):
    latest = LatestMarketData()

    with pytest.raises(ValueError, match="crossed or locked"):
        latest.update_book(
            venue="lighter",
            venue_symbol="BTC",
            bids=bids,
            asks=asks,
            observed_at=OBSERVED_AT,
        )

    assert ("lighter", "BTC") not in latest._views


def test_ready_venues_requires_fresh_ready_full_10k_depth(
    markets: tuple[MarketConfig, ...],
):
    latest = LatestMarketData()
    seed_book(latest, venue="lighter")
    seed_book(
        latest,
        venue="hyperliquid",
        bids=(BookLevel(99.0, 1.0),),
        asks=(BookLevel(101.0, 200.0),),
    )

    assert latest.ready_venues(
        markets=markets,
        canonical_symbol="BTC",
        now=NOW,
        stale_after_seconds=30,
    ) == frozenset({"lighter"})


def test_latest_market_view_is_immutable():
    view = LatestMarketView(
        venue="lighter",
        venue_symbol="BTC",
        bids=(BookLevel(99.0, 200.0),),
        asks=(BookLevel(101.0, 200.0),),
        observed_at=OBSERVED_AT,
        ready=True,
        mark_price=None,
        index_price=None,
    )

    with pytest.raises(AttributeError):
        view.ready = False
