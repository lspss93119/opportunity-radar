from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from radar.alerts.funding import FundingAlertProcessor, format_funding_alert
from radar.alerts.worker import AlertRouter
from radar.collectors.base import CollectorBatch
from radar.collectors.variational import variational_funding_snapshot
from radar.config import FundingMonitorConfig
from radar.history.funding import load_recent_funding_points
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot, QuotedMarketSnapshot
from radar.monitors.funding import CARRY, SPIKE, FeedHistory, FundingMonitor
from radar.pipeline import MarketDataPipeline
from radar.state import RadarState
from radar.storage.parquet import ParquetStorage
from radar.storage.sqlite import SQLiteRuntimeStore

NOW = datetime(2026, 11, 23, 12, 0, tzinfo=UTC)
FEES = {"hyperliquid": 4.5, "variational": 0.0, "lighter": 0.0}


def funding(
    venue: str,
    apr: float,
    observed_at: datetime,
    *,
    symbol: str = "ACE",
    rate_kind: str = "predicted",
):
    # Hourly venue: one interval's rate is APR / 8760.
    return FundingSnapshot(
        effective_time=observed_at,
        observed_at=observed_at,
        venue=venue,
        venue_symbol=symbol,
        canonical_symbol=symbol,
        funding_rate=apr / 8760,
        interval_seconds=3600,
        next_funding_time=observed_at.replace(minute=0, second=0) + timedelta(hours=1),
        rate_kind=rate_kind,
    )


def book(venue: str, at: datetime, *, bid: float = 99.95, ask: float = 100.05, source="order_book"):
    return MarketSnapshot(
        sample_time=at,
        observed_at=at,
        venue=venue,
        venue_symbol="ACE",
        canonical_symbol="ACE",
        best_bid=bid,
        best_bid_size=100.0,
        best_ask=ask,
        best_ask_size=100.0,
        mark_price=100.0,
        buy_1k_vwap=ask,
        sell_1k_vwap=bid,
        price_source=source,
    )


def state_at(at: datetime, long_apr: float, short_apr: float, *, with_books: bool = True):
    state = RadarState()
    state.apply_context_batch(
        CollectorBatch(
            funding_snapshots=(
                funding("hyperliquid", long_apr, at),
                funding("variational", short_apr, at),
            ),
            hourly_contexts=(
                HourlyContext(
                    sample_time=at, observed_at=at, venue="hyperliquid", venue_symbol="ACE",
                    canonical_symbol="ACE", open_interest=1_000.0, volume_24h=14_000_000.0,
                ),
                HourlyContext(
                    sample_time=at, observed_at=at, venue="variational", venue_symbol="ACE",
                    canonical_symbol="ACE", volume_24h=460_000.0,
                    long_open_interest=120_000.0, short_open_interest=80_000.0,
                ),
            ),
        )
    )
    if with_books:
        state.apply_market_batch(
            CollectorBatch(
                market_snapshots=(book("hyperliquid", at), book("variational", at, source="rfq"))
            )
        )
    return state


def monitor(store=None, **overrides) -> FundingMonitor:
    return FundingMonitor(
        FundingMonitorConfig(enabled=True, **overrides),
        FEES,
        spread_stale_after_seconds=30,
        rfq_stale_after_seconds=120,
        runtime_store=store,
    )


def kinds(alerts, long_venue="hyperliquid"):
    return [a.payload["kind"] for a in alerts if a.payload["long_venue"] == long_venue]


def test_feed_window_is_time_weighted_and_counts_gaps():
    history = FeedHistory()
    start = NOW - timedelta(hours=4)
    history.record(start, 0.10)
    history.record(start + timedelta(hours=1), 0.30)  # holds for 3 hours
    window = history.window(start, NOW, timedelta(hours=2))

    # 0.10 for 1h, 0.30 for 2h (stale after the 2h gap limit), last hour missing.
    assert window.coverage == pytest.approx(0.75)
    assert window.mean_apr == pytest.approx((0.10 + 0.30 * 2) / 3)


@pytest.mark.asyncio
async def test_spike_needs_consecutive_new_samples_and_alerts_once():
    funding_monitor = monitor(carry_avg_net_apr=10.0)  # isolate the spike rule
    first = NOW
    # Long Hyperliquid pays -20 APR (longs receive), short Var pays +5: net +25.
    assert kinds(await funding_monitor.evaluate(first, state_at(first, -20.0, 5.0))) == []
    # Re-evaluating the same funding sample is not a new sample.
    assert kinds(await funding_monitor.evaluate(first + timedelta(seconds=60), state_at(first, -20.0, 5.0))) == []

    second = NOW + timedelta(minutes=5)
    alerts = await funding_monitor.evaluate(second, state_at(second, -20.0, 5.0))
    assert kinds(alerts) == [SPIKE]
    payload = next(a.payload for a in alerts if a.payload["long_venue"] == "hyperliquid")
    assert payload["net_apr"] == pytest.approx(25.0)
    assert payload["spike_samples"] == 2

    third = NOW + timedelta(minutes=10)
    assert kinds(await funding_monitor.evaluate(third, state_at(third, -20.0, 5.0))) == []
    # The reverse direction pays the spread, so it never alerts.
    assert kinds(alerts, long_venue="variational") == []


@pytest.mark.asyncio
async def test_spike_rearms_only_after_staying_below_threshold():
    funding_monitor = monitor(carry_avg_net_apr=10.0, rearm_seconds=600)
    times = [NOW + timedelta(minutes=5 * step) for step in range(8)]
    aprs = [25, 25, 0, 25, 0, 0, 0, 25]  # net APR per sample (alert at step 1 and 7 only)
    fired = []
    for at, net in zip(times, aprs):
        alerts = await funding_monitor.evaluate(at, state_at(at, -float(net), 0.0))
        fired.append(SPIKE in kinds(alerts))
    # Step 3's spike follows only 5 min below threshold: still inside rearm.
    assert fired == [False, True, False, False, False, False, False, False]
    eighth = NOW + timedelta(minutes=40)
    alerts = await funding_monitor.evaluate(eighth, state_at(eighth, -25.0, 0.0))
    assert SPIKE in kinds(alerts)


def seeded(funding_monitor: FundingMonitor, long_apr: float, short_apr: float, hours: int = 24):
    points = {
        venue_key: tuple(
            (NOW - timedelta(minutes=5 * step), apr) for step in range(1, hours * 12 + 1)
        )
        for venue_key, apr in (
            (("hyperliquid", "ACE", "ACE"), long_apr),
            (("variational", "ACE", "ACE"), short_apr),
        )
    }
    funding_monitor.hydrate_history(points)
    return funding_monitor


@pytest.mark.asyncio
async def test_carry_requires_average_coverage_and_yield_above_cost():
    funding_monitor = seeded(monitor(), 0.0, 0.20)
    alerts = await funding_monitor.evaluate(NOW, state_at(NOW, 0.0, 0.20))

    carry = [a.payload for a in alerts if a.payload["kind"] == CARRY]
    assert [p["long_venue"] for p in carry] == ["hyperliquid"]
    payload = carry[0]
    assert payload["avg_net_apr"] == pytest.approx(0.20)
    # 20% APR over the default 7-day horizon is ~38 bps; each book is ~10 bps
    # wide and Hyperliquid charges 4.5 bps per fill, so the round trip is ~29 bps.
    assert payload["expected_funding_bps"] == pytest.approx(0.20 * 168 / 8760 * 10_000)
    assert payload["round_trip_cost_bps"] == pytest.approx(2 * 4.5 + 19.99, abs=0.01)


@pytest.mark.asyncio
async def test_carry_is_suppressed_when_cost_exceeds_expected_funding():
    # Over one day, 20% APR earns ~5.5 bps: far below the ~29 bps round trip.
    one_day = seeded(monitor(carry_horizon_hours=24), 0.0, 0.20)
    assert CARRY not in kinds(await one_day.evaluate(NOW, state_at(NOW, 0.0, 0.20)))

    # Without executable books the cost is unknown, so carry fails closed.
    no_books = seeded(monitor(), 0.0, 0.20)
    assert CARRY not in kinds(
        await no_books.evaluate(NOW, state_at(NOW, 0.0, 0.20, with_books=False))
    )


@pytest.mark.asyncio
async def test_round_trip_cost_ignores_basis_between_venues():
    # A 10% price gap between venues must not make the round trip look free.
    funding_monitor = monitor()
    state = state_at(NOW, 0.0, 0.0)
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=(
                book("hyperliquid", NOW),
                book("variational", NOW, bid=109.95, ask=110.05, source="rfq"),
            )
        )
    )
    markets = {(s.venue, s.venue_symbol, s.canonical_symbol): s for s in state.markets}
    cost = funding_monitor._round_trip(
        ("hyperliquid", "ACE", "ACE"), ("variational", "ACE", "ACE"), markets, NOW
    )
    assert cost["round_trip_cost_bps"] == pytest.approx(0.2 / 100.05 * 10_000 + 9.0)
    assert cost["entry_spread_bps"] > 900  # Var trades 10% rich


@pytest.mark.asyncio
async def test_carry_needs_history_coverage():
    funding_monitor = seeded(monitor(), 0.0, 0.50, hours=12)
    assert CARRY not in kinds(await funding_monitor.evaluate(NOW, state_at(NOW, 0.0, 0.50)))


@pytest.mark.asyncio
async def test_stale_funding_fails_closed():
    funding_monitor = monitor(carry_avg_net_apr=10.0, stale_after_seconds=900)
    old = NOW - timedelta(minutes=20)
    await funding_monitor.evaluate(old, state_at(old, -20.0, 5.0))
    later = old + timedelta(minutes=5)
    await funding_monitor.evaluate(later, state_at(later, -20.0, 5.0))  # alerts here
    # Same snapshots seen 20 minutes later are stale: the pair is dropped.
    stale_alerts = await funding_monitor.evaluate(NOW + timedelta(minutes=10), state_at(later, -20.0, 5.0))
    assert stale_alerts == []
    assert all(pair.streak == 0 for pair in funding_monitor._pairs.values())


@pytest.mark.asyncio
async def test_alerted_state_survives_restart(tmp_path):
    store = SQLiteRuntimeStore(tmp_path / "runtime.sqlite3")
    first = monitor(store, carry_avg_net_apr=10.0)
    for step in range(2):
        at = NOW + timedelta(minutes=5 * step)
        await first.evaluate(at, state_at(at, -20.0, 5.0))

    restarted = monitor(store, carry_avg_net_apr=10.0)
    at = NOW + timedelta(minutes=10)
    assert kinds(await restarted.evaluate(at, state_at(at, -20.0, 5.0))) == []
    store.close()


@pytest.mark.asyncio
async def test_alert_payload_and_message_cover_required_context():
    funding_monitor = monitor(carry_avg_net_apr=10.0)
    for step in range(2):
        at = NOW + timedelta(minutes=5 * step)
        alerts = await funding_monitor.evaluate(at, state_at(at, -20.0, 5.0))
    alert = next(a for a in alerts if a.payload["long_venue"] == "hyperliquid")
    payload = alert.payload

    assert payload["long_funding"]["next_funding_time"] == "2026-11-23T13:00:00+00:00"
    assert payload["long_market"]["open_interest_usd"] == pytest.approx(100_000.0)
    assert payload["short_market"]["long_open_interest_usd"] == 120_000.0
    assert payload["short_market"]["price_source"] == "rfq"

    message = format_funding_alert(alert)
    assert message.startswith("資金費飆升 | ACE")
    assert "做空: Variational ［RFQ 報價］ (ACE)" in message
    assert "淨 APR: +2500.00%" in message
    assert "下次結算 11-23 13:00 UTC" in message
    assert "多方 OI $120k / 空方 OI $80k" in message
    assert "來回成本: +28.99 bps" in message


@pytest.mark.asyncio
async def test_router_sends_funding_alerts_through_telegram():
    sent: list[str] = []

    class Telegram:
        async def send_text(self, text: str) -> None:
            sent.append(text)

    funding_monitor = monitor(carry_avg_net_apr=10.0)
    for step in range(2):
        at = NOW + timedelta(minutes=5 * step)
        alerts = await funding_monitor.evaluate(at, state_at(at, -20.0, 5.0))
    router = AlertRouter(object(), object(), FundingAlertProcessor(Telegram()))  # type: ignore[arg-type]
    await router.process(alerts[0])
    assert sent and sent[0].startswith("資金費飆升")


def make_quote(at: datetime, *, interval: int = 14_400) -> QuotedMarketSnapshot:
    return QuotedMarketSnapshot(
        quote_time=at,
        fetched_at=at,
        venue="variational",
        venue_symbol="ACE",
        canonical_symbol="ACE",
        mark_price=0.07,
        bid_1k=0.0699,
        ask_1k=0.0701,
        bid_100k=0.069,
        ask_100k=0.071,
        funding_rate=-87.3,
        funding_interval_seconds=interval,
        volume_24h=459_000.0,
        long_open_interest=120_000.0,
        short_open_interest=80_000.0,
    )


def test_variational_next_funding_lands_on_interval_multiple():
    snapshot = variational_funding_snapshot(make_quote(NOW + timedelta(minutes=7)))
    assert snapshot.next_funding_time == datetime(2026, 11, 23, 12 + 4, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_pipeline_context_batch_includes_fresh_rfq_funding_and_oi():
    state = RadarState()
    pipeline = MarketDataPipeline([], state, clock=lambda: NOW, context_poll_seconds=300)
    pipeline._latest_quotes = {
        "ACE": make_quote(NOW - timedelta(seconds=30)),
        "OLD": make_quote(NOW - timedelta(seconds=600)).model_copy(
            update={"venue_symbol": "OLD", "canonical_symbol": "OLD"}
        ),
    }

    batch = await pipeline.collect_hourly_once(now=NOW + timedelta(seconds=61))

    assert [f.venue_symbol for f in batch.funding_snapshots] == ["ACE"]
    assert state.funding[0].apr == pytest.approx(-87.3)
    context = state.hourly_context[0]
    assert (context.long_open_interest, context.short_open_interest) == (120_000.0, 80_000.0)


def test_funding_history_hydration_skips_rows_without_interval(tmp_path):
    storage = ParquetStorage(tmp_path / "data")
    storage.append(funding("hyperliquid", 0.5, NOW - timedelta(hours=1)))
    storage.flush(now=NOW)
    legacy = tmp_path / "data" / "funding" / f"date={NOW.date()}"
    pq.write_table(
        pa.table(
            {
                "effective_time": [NOW - timedelta(hours=2)],
                "observed_at": [NOW - timedelta(hours=2)],
                "venue": ["lighter"],
                "venue_symbol": ["ACE"],
                "canonical_symbol": ["ACE"],
                "funding_rate": [0.001],
                "next_funding_time": pa.array([None], pa.timestamp("us", "UTC")),
            }
        ),
        legacy / "part-legacy.parquet",
    )

    points = load_recent_funding_points(
        tmp_path / "data", as_of=NOW, window=timedelta(hours=26)
    )

    assert set(points) == {("hyperliquid", "ACE", "ACE")}
    assert points[("hyperliquid", "ACE", "ACE")][0][1] == pytest.approx(0.5)



@pytest.mark.asyncio
async def test_spike_ignores_pairs_with_a_settled_leg():
    """A last-applied rate lags a moving market; comparing it to a live rate
    produced false spikes in the trial (Lighter settled vs xyz predicted)."""
    funding_monitor = monitor(carry_avg_net_apr=10.0)

    def lagged_state(at):
        state = state_at(at, -20.0, 5.0)
        state.apply_context_batch(
            CollectorBatch(
                funding_snapshots=(
                    funding("hyperliquid", -20.0, at, rate_kind="settled"),
                    funding("variational", 5.0, at),
                )
            )
        )
        return state

    for step in range(3):
        at = NOW + timedelta(minutes=5 * step)
        assert kinds(await funding_monitor.evaluate(at, lagged_state(at))) == []


@pytest.mark.asyncio
async def test_carry_still_uses_settled_legs():
    funding_monitor = seeded(monitor(), 0.0, 0.20)
    state = state_at(NOW, 0.0, 0.20)
    state.apply_context_batch(
        CollectorBatch(
            funding_snapshots=(
                funding("hyperliquid", 0.0, NOW, rate_kind="settled"),
                funding("variational", 0.20, NOW),
            ),
            hourly_contexts=state.hourly_context,
        )
    )
    assert CARRY in kinds(await funding_monitor.evaluate(NOW, state))
