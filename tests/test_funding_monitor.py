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
from radar.models import (
    FundingSnapshot,
    HourlyContext,
    MarketSnapshot,
    QuotedMarketSnapshot,
)
from radar.monitors.base import AlertRequest
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


def test_feed_history_prune_keeps_boundary_value_and_discards_older_points():
    history = FeedHistory()
    for hours, apr in enumerate((0.1, 0.2, 0.3, 0.4)):
        history.record(NOW - timedelta(hours=4 - hours), apr)

    history.prune(NOW - timedelta(hours=2, minutes=30))

    assert history.times == [
        NOW - timedelta(hours=3),
        NOW - timedelta(hours=2),
        NOW - timedelta(hours=1),
    ]
    assert history.aprs == [0.2, 0.3, 0.4]


def test_hydrated_funding_window_matches_online_history():
    key = ("hyperliquid", "ACE", "ACE")
    points = tuple(
        (NOW - timedelta(hours=4 - index), 0.1 + index * 0.1)
        for index in range(4)
    )
    online = FeedHistory()
    for observed_at, apr in points:
        online.record(observed_at, apr)

    funding_monitor = monitor()
    funding_monitor.hydrate_history({key: points})
    hydrated = funding_monitor._histories[key]

    assert hydrated.window(
        NOW - timedelta(hours=4), NOW, timedelta(hours=2)
    ) == online.window(NOW - timedelta(hours=4), NOW, timedelta(hours=2))


def settling(hour: int, minute: int = 45) -> datetime:
    """A moment ``hour`` hours after NOW, inside the pre-settlement window by default."""
    return NOW + timedelta(hours=hour, minutes=minute)


@pytest.mark.asyncio
async def test_spike_counts_consecutive_settlements_and_alerts_once():
    funding_monitor = monitor(carry_avg_net_apr=10.0)  # isolate the spike rule
    # Long Hyperliquid pays -20 APR (longs receive), short Var pays +5: net +25.
    first = settling(0)  # 12:45, settles 13:00
    assert (
        kinds(await funding_monitor.evaluate(first, state_at(first, -20.0, 5.0))) == []
    )
    # Another sample in the same window is the same settlement, not a new period.
    again = first + timedelta(minutes=10)
    assert (
        kinds(await funding_monitor.evaluate(again, state_at(again, -20.0, 5.0))) == []
    )

    second = settling(1)  # 13:45, settles 14:00
    alerts = await funding_monitor.evaluate(second, state_at(second, -20.0, 5.0))
    assert kinds(alerts) == [SPIKE]
    payload = next(a.payload for a in alerts if a.payload["long_venue"] == "hyperliquid")
    assert payload["net_apr"] == pytest.approx(25.0)
    assert payload["spike_periods"] == 2
    assert alerts[0].event_id.endswith(":funding_spike:2026-11-23T14:00:00+00:00")

    third = settling(2)
    assert (
        kinds(await funding_monitor.evaluate(third, state_at(third, -20.0, 5.0))) == []
    )
    # The reverse direction pays the spread, so it never alerts.
    assert kinds(alerts, long_venue="variational") == []


@pytest.mark.asyncio
async def test_spike_ignores_intra_interval_estimates():
    funding_monitor = monitor(carry_avg_net_apr=10.0)
    for hour in range(3):
        # Huge mid-hour estimates, but back to normal right before settlement.
        for minute in (0, 15, 30):
            at = settling(hour, minute)
            assert (
                kinds(await funding_monitor.evaluate(at, state_at(at, -20.0, 5.0)))
                == []
            )
        at = settling(hour)
        assert kinds(await funding_monitor.evaluate(at, state_at(at, 0.0, 0.0))) == []
    assert all(pair.streak == 0 for pair in funding_monitor._pairs.values())


@pytest.mark.asyncio
async def test_spike_streak_resets_on_a_skipped_or_low_settlement():
    funding_monitor = monitor(carry_avg_net_apr=10.0)
    await funding_monitor.evaluate(settling(0), state_at(settling(0), -20.0, 5.0))
    # No pre-settlement sample for 14:00; the next one seen is mid-hour 14:10.
    mid = settling(2, 10)
    await funding_monitor.evaluate(mid, state_at(mid, -20.0, 5.0))
    assert (
        kinds(
            await funding_monitor.evaluate(
                settling(2), state_at(settling(2), -20.0, 5.0)
            )
        )
        == []
    )

    low = monitor(carry_avg_net_apr=10.0)
    for hour, net in enumerate((25.0, 0.0, 25.0)):
        at = settling(hour)
        assert kinds(await low.evaluate(at, state_at(at, -net, 0.0))) == []


@pytest.mark.asyncio
async def test_spike_rearms_only_after_staying_below_threshold():
    quiet = monitor(carry_avg_net_apr=10.0, rearm_seconds=7_200)
    fired = []
    for hour, net in enumerate((25, 25, 0, 25, 25)):
        at = settling(hour)
        fired.append(
            SPIKE in kinds(await quiet.evaluate(at, state_at(at, -float(net), 0.0)))
        )
    # One hour below threshold is shorter than the 2h rearm: no second alert.
    assert fired == [False, True, False, False, False]

    rearmed = monitor(carry_avg_net_apr=10.0, rearm_seconds=3_600)
    fired = []
    for hour, net in enumerate((25, 25, 0, 0, 25, 25)):
        at = settling(hour)
        fired.append(
            SPIKE in kinds(await rearmed.evaluate(at, state_at(at, -float(net), 0.0)))
        )
    assert fired == [False, True, False, False, False, True]


def snapshot(venue, apr, at, *, interval=3600, settles=None, symbol="ACE"):
    return FundingSnapshot(
        effective_time=at,
        observed_at=at,
        venue=venue,
        venue_symbol=symbol,
        canonical_symbol=symbol,
        funding_rate=apr * interval / (365 * 24 * 3600),
        interval_seconds=interval,
        next_funding_time=settles,
        rate_kind="predicted",
    )


def funding_state(at, *snapshots):
    state = RadarState()
    state.apply_context_batch(CollectorBatch(funding_snapshots=snapshots))
    return state


def carry_state(at: datetime, aprs: dict[str, float]) -> RadarState:
    state = funding_state(
        at,
        *(snapshot(venue, apr, at) for venue, apr in aprs.items()),
    )
    state.apply_market_batch(
        CollectorBatch(
            market_snapshots=tuple(book(venue, at) for venue in aprs),
        )
    )
    return state


@pytest.mark.asyncio
async def test_longer_interval_leg_sets_the_settlement_period():
    """Hyperliquid pays hourly but Variational every 8h: only 8h settlements count."""
    funding_monitor = monitor(carry_avg_net_apr=10.0)
    eight_hourly = [
        datetime(2026, 11, 23, 16, tzinfo=UTC),
        datetime(2026, 11, 24, 0, tzinfo=UTC),
    ]

    def state(at, var_settles):
        hl_settles = at.replace(minute=0, second=0) + timedelta(hours=1)
        return funding_state(
            at,
            snapshot("hyperliquid", -20.0, at, settles=hl_settles),
            snapshot("variational", 5.0, at, interval=28_800, settles=var_settles),
        )

    fired = []
    for at, var_settles in (
        (datetime(2026, 11, 23, 15, 45, tzinfo=UTC), eight_hourly[0]),
        # Hourly pre-settlement samples between Var settlements do not count.
        *(
            (datetime(2026, 11, 23, hour, 45, tzinfo=UTC), eight_hourly[1])
            for hour in range(16, 23)
        ),
        (datetime(2026, 11, 23, 23, 45, tzinfo=UTC), eight_hourly[1]),
    ):
        fired.append(
            SPIKE in kinds(await funding_monitor.evaluate(at, state(at, var_settles)))
        )
    assert fired == [False] * 8 + [True]


@pytest.mark.asyncio
async def test_one_spike_alert_per_symbol_lists_other_pairs():
    funding_monitor = FundingMonitor(
        FundingMonitorConfig(enabled=True, carry_avg_net_apr=10.0),
        FEES,
        spread_stale_after_seconds=30,
        rfq_stale_after_seconds=120,
    )

    def state(at, hl_apr):
        settles = at.replace(minute=0, second=0) + timedelta(hours=1)
        return funding_state(
            at,
            snapshot("hyperliquid", hl_apr, at, settles=settles),
            snapshot("variational", 5.0, at, settles=settles),
            snapshot("lighter", 10.0, at, settles=settles),
        )

    await funding_monitor.evaluate(settling(0), state(settling(0), -20.0))
    alerts = await funding_monitor.evaluate(settling(1), state(settling(1), -20.0))

    spikes = [a for a in alerts if a.payload["kind"] == SPIKE]
    assert len(spikes) == 1
    lead = spikes[0].payload
    # HL->lighter (+30) leads; HL->Var (+25) and Var->lighter (+5) are listed.
    assert (lead["long_venue"], lead["short_venue"]) == ("hyperliquid", "lighter")
    assert sorted(o["net_apr"] for o in lead["other_pairs"]) == pytest.approx(
        [5.0, 25.0]
    )
    message = format_funding_alert(spikes[0])
    assert "同時達標的其他配對（2）" in message
    assert "做多 Hyperliquid / 做空 Variational: 淨 APR +2500.00%" in message

    # While the symbol's spike is active, a newly qualifying pair stays quiet.
    hotter = settling(2)
    await funding_monitor.evaluate(hotter, state(hotter, -40.0))
    assert [
        a
        for a in await funding_monitor.evaluate(settling(3), state(settling(3), -40.0))
        if a.payload["kind"] == SPIKE
    ] == []


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
async def test_one_carry_alert_per_symbol_lists_other_pairs_and_stays_in_cooldown():
    funding_monitor = monitor(carry_avg_net_apr=0.1)
    aprs = {"hyperliquid": 0.0, "variational": 0.3, "lighter": 0.5}
    funding_monitor.hydrate_history(
        {
            (venue, "ACE", "ACE"): tuple(
                (NOW - timedelta(minutes=5 * step), apr)
                for step in range(1, 24 * 12 + 1)
            )
            for venue, apr in aprs.items()
        }
    )

    alerts = await funding_monitor.evaluate(NOW, carry_state(NOW, aprs))
    carries = [alert for alert in alerts if alert.payload["kind"] == CARRY]

    assert len(carries) == 1
    payload = carries[0].payload
    assert (payload["long_venue"], payload["short_venue"]) == (
        "hyperliquid",
        "lighter",
    )
    assert sorted(pair["net_apr"] for pair in payload["other_pairs"]) == pytest.approx(
        [0.2, 0.3]
    )
    message = format_funding_alert(carries[0])
    assert "同時達標的其他配對（2）" in message
    assert "做多 Hyperliquid / 做空 Variational: 淨 APR +30.00%" in message

    later_alerts = await funding_monitor.evaluate(
        NOW + timedelta(minutes=5), carry_state(NOW + timedelta(minutes=5), aprs)
    )
    assert [alert for alert in later_alerts if alert.payload["kind"] == CARRY] == []


@pytest.mark.asyncio
async def test_spike_and_carry_aggregate_independently():
    at = settling(0)
    funding_monitor = monitor(carry_avg_net_apr=0.1, spike_min_periods=1)
    funding_monitor.hydrate_history(
        {
            key: tuple(
                (at - timedelta(minutes=5 * step), apr)
                for step in range(1, 24 * 12 + 1)
            )
            for key, apr in (
                (("hyperliquid", "ACE", "ACE"), -20.0),
                (("variational", "ACE", "ACE"), 5.0),
            )
        }
    )

    alerts = await funding_monitor.evaluate(at, state_at(at, -20.0, 5.0))

    assert {alert.payload["kind"] for alert in alerts} == {SPIKE, CARRY}


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
    for hour in range(2):
        at = settling(hour)
        alerts = await first.evaluate(at, state_at(at, -20.0, 5.0))
    assert SPIKE in kinds(alerts)

    restarted = monitor(store, carry_avg_net_apr=10.0)
    at = settling(2)
    assert kinds(await restarted.evaluate(at, state_at(at, -20.0, 5.0))) == []
    assert restarted._pairs["ACE|hyperliquid|ACE|variational|ACE"].streak == 3
    store.close()


@pytest.mark.asyncio
async def test_alert_payload_and_message_cover_required_context():
    funding_monitor = monitor(carry_avg_net_apr=10.0)
    for hour in range(2):
        at = settling(hour)
        alerts = await funding_monitor.evaluate(at, state_at(at, -20.0, 5.0))
    alert = next(a for a in alerts if a.payload["long_venue"] == "hyperliquid")
    payload = alert.payload

    assert payload["long_funding"]["next_funding_time"] == "2026-11-23T14:00:00+00:00"
    assert payload["long_market"]["open_interest_usd"] == pytest.approx(100_000.0)
    assert payload["short_market"]["long_open_interest_usd"] == 120_000.0
    assert payload["short_market"]["price_source"] == "rfq"

    message = format_funding_alert(alert)
    assert message.startswith("資金費飆升 | ACE")
    assert "做空: Variational ［RFQ 報價］ (ACE)" in message
    assert "淨 APR: +2500.00%" in message
    assert "下次結算 11-23 14:00 UTC" in message
    assert "淨 APR: +2500.00%（結算前，連續 2 個結算期）" in message
    assert "多方 OI $120k / 空方 OI $80k" in message
    assert "來回成本: +28.99 bps" in message


@pytest.mark.asyncio
async def test_router_sends_funding_alerts_through_telegram():
    sent: list[str] = []

    class Telegram:
        async def send_text(self, text: str) -> None:
            sent.append(text)

    funding_monitor = monitor(carry_avg_net_apr=10.0)
    for hour in range(2):
        at = settling(hour)
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

    for hour in range(3):
        at = settling(hour)
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


def test_funding_cadence_defaults_follow_hourly_settlement():
    from pydantic import ValidationError

    from radar.config import RadarConfig

    config = FundingMonitorConfig()
    assert (config.stale_after_seconds, config.spike_min_periods) == (1800, 2)
    assert config.spike_settlement_window_seconds == 1200
    assert RadarConfig().context_poll_seconds == 900
    with pytest.raises(ValidationError):
        FundingMonitorConfig(spike_min_samples=2)


def test_legacy_spike_payload_still_formats():
    alert = AlertRequest(
        monitor="funding",
        event_id="legacy",
        created_at=NOW,
        payload={
            "kind": SPIKE,
            "canonical_symbol": "ACE",
            "long_venue": "hyperliquid",
            "long_venue_symbol": "ACE",
            "short_venue": "variational",
            "short_venue_symbol": "ACE",
            "net_apr": 2.0,
            "spike_samples": 3,
        },
    )
    assert "（連續 3 次採樣）" in format_funding_alert(alert)
