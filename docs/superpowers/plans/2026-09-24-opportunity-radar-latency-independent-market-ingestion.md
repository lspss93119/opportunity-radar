# Latency-Independent Market Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decouple the aligned 10-second market sampler from venue network latency by maintaining fresh in-process market data for every integrated venue.

**Architecture:** Venue WebSocket and background REST tasks publish complete local order-book views and low-frequency metadata into one in-process `LatestMarketData` cache. `MarketDataPipeline.collect_once()` only reads that cache and emits the existing normalized market batch; one hourly coordinator separately gathers and applies funding/context batches. The existing RadarState, storage, SpreadMonitor, and alert boundaries remain intact.

**Tech Stack:** Python 3.13, asyncio, `websockets`, `httpx`, Pydantic models, existing VWAP helpers, PyArrow/Parquet, SQLite, pytest/pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-24-opportunity-radar-latency-independent-market-ingestion-design.md`

## Global Constraints

- The aligned 10-second sampler is the only production market sample clock.
- Background I/O must not delay, move, duplicate, backfill, or otherwise influence a market sample slot.
- `collect_once()` must perform no network I/O.
- Feed readiness and freshness are independent fail-closed gates.
- Applying an hourly batch must not clear or replace `RadarState.markets`.
- A metadata refresh failure must not invalidate an independently ready/fresh local book unless required identity metadata is unavailable.
- Lighter must treat a valid full `subscribed/order_book` snapshot as ready immediately; later deltas must remain nonce-contiguous.
- Hourly work uses one coordinator per UTC hour: gather, merge, apply context once, append once.
- Shutdown order is scheduler stop, awaited `pipeline.stop()`, final Parquet flush, alert-worker stop, runtime-store close.
- Backpack depth REST is limited to initial synchronization and sequence-gap recovery.
- Hyperliquid `l2Book` messages are complete snapshots and must replace local books atomically.
- Arcus remains REST-only, with no overlapping refresh cycles but permitted bounded concurrency within a refresh.
- Do not change normalized model schemas, Parquet schemas, storage layout, monitor thresholds, fees, sampling cadence, Telegram formatting, or existing venue semantics outside this ingestion boundary.
- Do not add Redis, Kafka, external queues, a broker, an ORM, a worker framework, a custom executor framework, or a generic plugin framework.
- Do not persist full L2 order books.
- Do not add trading or private API behavior.
- Use fixtures and injected clocks for deterministic tests; mark real network tests `pytest.mark.live`.
- Run Ruff and Mypy only when already configured and available; do not add them as dependencies.

## Review Focus

- **Sampler/network isolation:** a blocked REST or WebSocket operation must not delay the next aligned `sample_time`; pin with `test_collect_once_does_not_await_network_ingestion` in `tests/test_pipeline.py`.
- **Hourly merge atomicity:** venue results completing in different orders must produce one merged context batch and one state/storage application; pin with `test_hourly_coordinator_merges_before_single_apply` in `tests/test_pipeline.py`.
- **Readiness versus freshness:** a ready book can become stale, and a fresh-looking pre-disconnect book must not remain ready; pin with `test_ready_and_fresh_are_independent_gates` in `tests/test_market_data.py` and reconnect tests in each WS fixture suite.
- **Metadata isolation:** metadata failure must retain a ready/fresh book while missing required identity metadata omits only the affected feed; pin with `test_lighter_metadata_failure_preserves_ready_book` and the analogous Backpack/Hyperliquid tests.
- **Shutdown and smoke lifecycle:** no background append may race the final flush, and telegram-smoke must start/wait/sample/stop explicitly; pin with `test_application_shutdown_orders_pipeline_flush_worker_runtime` and `test_telegram_smoke_starts_waits_and_stops_pipeline` in `tests/test_app.py`.

---

### Task 1: Add the latest in-process market-data cache

**Files:**
- Create: `src/radar/market_data.py`
- Modify: `src/radar/collectors/base.py`
- Test: `tests/test_market_data.py`
- Test: `tests/test_pipeline.py` for any import/type seam changes

**Interfaces:**
- `LatestMarketView` is an immutable record containing `venue`, `venue_symbol`, sorted `bids`, sorted `asks`, `observed_at`, `ready`, `mark_price`, and `index_price`.
- `LatestMarketData.update_book(*, venue: str, venue_symbol: str, bids: Sequence[BookLevel], asks: Sequence[BookLevel], observed_at: datetime) -> None` stores one complete validated book and marks it ready.
- `LatestMarketData.invalidate(*, venue: str, venue_symbol: str) -> None` clears levels and marks the feed not ready.
- `LatestMarketData.update_metadata(*, venue: str, venue_symbol: str, mark_price: float | None, index_price: float | None) -> None` updates metadata without changing book readiness.
- `LatestMarketData.build_batch(markets: Sequence[MarketConfig], *, sample_time: datetime, now: datetime, stale_after_seconds: int) -> CollectorBatch` emits only enabled ready/fresh feeds and computes all six existing VWAP fields.
- `LatestMarketData.ready_venues(*, markets: Sequence[MarketConfig], canonical_symbol: str, now: datetime, stale_after_seconds: int) -> frozenset[str]` returns venues with ready/fresh `$10k` buy and sell VWAPs.
- `ManagedCollector` is a structural protocol with `venue`, `async start() -> None`, `async stop() -> None`, and `async collect_hourly(*, sample_time: datetime) -> CollectorBatch`; the existing `Collector` protocol and `CollectorBatch` remain available for compatibility.
- `CollectorLike = Collector | ManagedCollector` is the pipeline's structural input type; concrete production collectors implement both protocols, while test-only hourly stubs may implement `ManagedCollector` directly.

- [ ] **Step 1: Write failing cache and protocol tests.**

Add fixtures with two `MarketConfig` rows and a complete bid/ask book. Assert that a fresh ready book produces aligned-model fields, a missing depth side produces no snapshot, and the metadata values are preserved when present. Add tests for `invalidate`, future `observed_at`, stale `observed_at`, and readiness requiring both `$10k` VWAP sides for `ready_venues`.

```python
def test_build_batch_uses_source_observed_at_and_all_vwap_tiers():
    observed_at = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)
    sample_time = datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC)
    now = datetime(2026, 9, 15, 10, 0, 19, tzinfo=UTC)
    latest = LatestMarketData()
    latest.update_book(
        venue="lighter",
        venue_symbol="BTC",
        bids=(BookLevel(99.0, 200.0),),
        asks=(BookLevel(101.0, 200.0),),
        observed_at=observed_at,
    )
    latest.update_metadata(
        venue="lighter",
        venue_symbol="BTC",
        mark_price=100.0,
        index_price=100.5,
    )

    batch = latest.build_batch(
        (MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),),
        sample_time=sample_time,
        now=now,
        stale_after_seconds=30,
    )

    snapshot = batch.market_snapshots[0]
    assert snapshot.sample_time == sample_time
    assert snapshot.observed_at == observed_at
    assert snapshot.buy_1k_vwap == 101.0
    assert snapshot.sell_10k_vwap is None
    assert snapshot.mark_price == 100.0
```

- [ ] **Step 2: Run the focused tests and verify RED.**

Run: `uv run pytest tests/test_market_data.py -q`

Expected: collection/import or missing-method failures because the cache and managed-collector protocol do not exist yet.

- [ ] **Step 3: Implement the minimal cache.**

Use a dictionary keyed by `(venue, venue_symbol)`. Validate timezone-aware UTC observations, positive prices, non-negative sizes, non-empty books, and non-crossed BBO before storing. Keep `ready` in the stored record; compute freshness as `0 <= (now - observed_at).total_seconds() <= stale_after_seconds`. Use `buy_vwap` and `sell_vwap` from `radar.vwap` and never import monitor, storage, Telegram, or history modules.

Add the `ManagedCollector` protocol without changing the existing `Collector` method signature. Keep the protocol structural and small; do not add a registry or event bus.

- [ ] **Step 4: Run focused tests and verify GREEN.**

Run: `uv run pytest tests/test_market_data.py -q`

Expected: all cache/readiness/freshness tests pass.

- [ ] **Step 5: Run the existing model/VWAP regression tests.**

Run: `uv run pytest tests/test_models.py tests/test_vwap.py -q`

Expected: PASS with no model or VWAP behavior changes.

- [ ] **Step 6: Commit.**

```bash
git add src/radar/market_data.py src/radar/collectors/base.py tests/test_market_data.py tests/test_pipeline.py
git commit -m "Add latest market data cache"
```

### Task 2: Make the pipeline cache-only and separate hourly application

**Files:**
- Modify: `src/radar/state.py`
- Modify: `src/radar/pipeline.py`
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_app.py` only for constructor/fixture seams

**Interfaces:**
- `RadarState.apply_market_batch(batch: CollectorBatch) -> None` replaces only `_markets` from the supplied market batch.
- `RadarState.apply_context_batch(batch: CollectorBatch) -> None` replaces only funding and hourly-context mappings.
- Existing `RadarState.apply(batch, replace_context=False)` remains as a compatibility wrapper that calls both operations when requested.
- `MarketDataPipeline(collectors: Sequence[CollectorLike], state: RadarState, *, markets: Sequence[MarketConfig] = (), latest_market_data: LatestMarketData | None = None, sampling_seconds: int = 10, clock: Callable[[], datetime] = utc_now, storage: ParquetStorage | None = None, collector_error_handler: CollectorErrorHandler | None = None, stale_after_seconds: int = 30)` owns the cache and configured market list.
- `MarketDataPipeline.collect_once(*, now: datetime | None = None) -> CollectorBatch` only calls `LatestMarketData.build_batch`, applies market state, and appends the market batch.
- `MarketDataPipeline.collect_hourly_once(*, now: datetime | None = None) -> CollectorBatch | None` is the single hourly coordinator operation for one due hour.
- `MarketDataPipeline.wait_for_market_feeds(canonical_symbol: str, *, required_venues: int = 2, timeout_seconds: float = 30.0) -> None` polls only in-process readiness and raises `TimeoutError` on expiry.

- [ ] **Step 1: Write failing state-application tests.**

Add a market batch followed by a context-only batch and assert the market remains. Add a two-venue hourly test with one delayed collector and one immediate collector; assert the final state contains both context rows and the storage receives one merged batch rather than two partial batches.

```python
class HourlyCollectorStub:
    def __init__(self, venue: str, delay: float) -> None:
        self.venue = venue
        self.delay = delay

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def collect_hourly(self, *, sample_time: datetime) -> CollectorBatch:
        await asyncio.sleep(self.delay)
        return CollectorBatch(
            hourly_contexts=(make_hourly_context(self.venue, sample_time),)
        )


async def test_hourly_coordinator_merges_before_single_apply():
    first = HourlyCollectorStub("lighter", delay=0.02)
    second = HourlyCollectorStub("hyperliquid", delay=0.0)
    state = RadarState()
    configured_markets = (
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(venue="hyperliquid", venue_symbol="BTC", canonical_symbol="BTC"),
    )
    state.apply_market_batch(
        CollectorBatch(market_snapshots=(make_market("lighter"),))
    )
    pipeline = MarketDataPipeline(
        [first, second],
        state,
        markets=configured_markets,
        latest_market_data=LatestMarketData(),
    )

    batch = await pipeline.collect_hourly_once(
        now=datetime(2026, 9, 15, 10, 1, tzinfo=UTC)
    )

    assert batch is not None
    assert {context.venue for context in batch.hourly_contexts} == {
        "lighter",
        "hyperliquid",
    }
    assert {context.venue for context in state.hourly_context} == {
        "lighter",
        "hyperliquid",
    }
    assert state.get_market("lighter", "BTC") is not None
```

Define `make_hourly_context(venue, sample_time)` and `make_market(venue)` in
the test module using the repository's `HourlyContext` and `MarketSnapshot`
models and timezone-aware UTC values.

- [ ] **Step 2: Run focused tests and verify RED.**

Run: `uv run pytest tests/test_pipeline.py -q`

Expected: failures because the current pipeline awaits network collectors in `collect_once` and applies context through the market path.

- [ ] **Step 3: Implement separate RadarState methods and cache-only collection.**

Pass `config.markets` and `config.monitors.spread.stale_after_seconds` from `MarketDataPipeline.from_config`. `collect_once` must contain no `asyncio.gather` over collectors and no collector invocation. Preserve the aligned 10-second calculation and market batch authority: an empty batch clears current markets.

Implement the hourly loop around `collect_hourly_once`: calculate the current aligned hour, apply the existing +60-second grace rule, gather all managed collector hourly results with `return_exceptions=True`, report individual errors, call `merge_batches` once, call `state.apply_context_batch` once, append once, and advance `_last_hourly_sample` once. The loop must not begin another hourly cycle until the current one has finished.

Implement `start`/`stop` so `stop` first prevents a new hourly cycle, cancels and awaits the hourly task, then calls and awaits all collector `stop` hooks. Return from `stop` only after no pipeline-owned task can append storage.

Implement `wait_for_market_feeds` with `asyncio.timeout`/`asyncio.wait_for` around a small polling loop that reads `LatestMarketData.ready_venues`; it must not invoke `collect_once` or a collector network method.

- [ ] **Step 4: Run pipeline tests and verify GREEN.**

Run: `uv run pytest tests/test_pipeline.py -q`

Expected: aligned sample, stale/future omission, hourly +60-second grace, mid-hour startup, single merged context application, and no-network sampler tests pass.

- [ ] **Step 5: Run state and application regression tests.**

Run: `uv run pytest tests/test_pipeline.py tests/test_app.py -q`

Expected: existing application-cycle tests pass after their fixtures seed the cache instead of relying on collector network calls.

- [ ] **Step 6: Commit.**

```bash
git add src/radar/state.py src/radar/pipeline.py tests/test_pipeline.py tests/test_app.py
git commit -m "Decouple sampling from hourly collection"
```

### Task 3: Migrate Lighter and Lighter Robinhood to cache publication

**Files:**
- Modify: `src/radar/collectors/lighter.py`
- Modify: `tests/test_lighter_websocket.py`
- Modify: `tests/test_lighter_collector.py`
- Modify: `tests/test_pipeline.py` for configured managed collectors

**Interfaces:**
- `LighterOrderBookFeed(ws_url: str, market_ids: Sequence[int], *, on_book: Callable[[int, LighterOrderBookSnapshot], None] | None = None, on_invalidate: Callable[[int], None] | None = None, connect=websockets.connect, clock: Callable[[], datetime], venue: str, reconnect_delay_seconds: float = 1.0)` publishes complete local books and invalidates cache state on clear/reconnect.
- `async LighterCollector.start() -> None` performs initial market-ID discovery, starts the metadata refresh task and the persistent feed.
- `async LighterCollector.stop() -> None` cancels/awaits metadata and feed tasks.
- `async LighterCollector.collect_hourly(*, sample_time: datetime) -> CollectorBatch` retains current funding and hourly-context semantics without order-book REST calls.

- [ ] **Step 1: Write failing readiness/publication tests.**

Extend the WebSocket fixture so a valid `subscribed/order_book` snapshot is followed by no delta; assert `feed.snapshot(market_id)` is immediately non-`None` and the callback receives the book. Define a test-only `wait_until(predicate)` helper that checks a predicate up to 100 times with `await asyncio.sleep(0)`. Add a reconnect test asserting the invalidate callback fires and no pre-disconnect book is published as ready. Keep existing nonce-gap and zero-delete tests.

```python
async def test_full_lighter_snapshot_is_ready_without_followup_delta():
    published: list[LighterOrderBookSnapshot] = []
    feed = LighterOrderBookFeed(
        "wss://test.invalid/stream",
        [1],
        connect=connect_snapshot_only_fixture,
        on_book=lambda market_id, snapshot: published.append(snapshot),
    )

    try:
        await feed.start()
        await wait_until(lambda: feed.snapshot(1) is not None)
    finally:
        await feed.stop()

    assert len(published) == 1
    assert feed.snapshot(1) is not None
```

Define `connect_snapshot_only_fixture` as the injected async context-manager
fixture that sends the `connected` message, the subscription acknowledgement,
and one valid `subscribed/order_book` snapshot before ending the test stream.

Add a collector test where `orderBookDetails` fails after the feed has a valid book; assert the latest market cache still emits that feed with its source observation time. If initial discovery fails, the collector remains running with no subscribed IDs and the next low-frequency discovery can recover; it does not publish a synthetic book.

- [ ] **Step 2: Run focused tests and verify RED.**

Run: `uv run pytest tests/test_lighter_websocket.py tests/test_lighter_collector.py -q`

Expected: the snapshot-only readiness and callback/metadata-isolation tests fail against the current collector lifecycle.

- [ ] **Step 3: Implement callback publication and metadata lifecycle.**

Keep `LighterOrderBookState` nonce validation and VWAP behavior. Mark a state ready immediately after `apply_snapshot`; retain the snapshot nonce for later `begin_nonce == previous_nonce` validation. On every feed clear, call `on_invalidate` for configured IDs. On each valid snapshot or delta, call `on_book` with the complete sorted `LighterOrderBookSnapshot`.

Have `LighterCollector` map market IDs to configured markets and publish books/metadata into the shared `LatestMarketData`. Move `orderBookDetails` out of `collect_once`/the sampler into startup plus a sequential low-frequency metadata task. On refresh failure, keep the last parsed details and the WS cache. Keep the existing funding parser and hourly model construction unchanged.

- [ ] **Step 4: Run Lighter and funding regression tests.**

Run: `uv run pytest tests/test_lighter_websocket.py tests/test_lighter_collector.py tests/test_pipeline.py -q`

Expected: both normal Lighter and Robinhood parameterized cases pass, including funding normalization and fail-closed reconnect behavior.

- [ ] **Step 5: Commit.**

```bash
git add src/radar/collectors/lighter.py tests/test_lighter_websocket.py tests/test_lighter_collector.py tests/test_pipeline.py
git commit -m "Publish Lighter books to market cache"
```

### Task 4: Add the reusable Hyperliquid-family WebSocket feed

**Files:**
- Modify: `src/radar/collectors/hyperliquid.py`
- Modify: `tests/test_hyperliquid_collector.py`
- Create: `tests/test_hyperliquid_websocket.py`

**Interfaces:**
- `HyperliquidOrderBookFeed(ws_url: str, coins: Sequence[str], *, connect, clock, venue, on_book, on_invalidate, reconnect_delay_seconds=1.0)` maintains complete snapshots for exact coin identities.
- `async HyperliquidOrderBookFeed.start() -> None`, `async HyperliquidOrderBookFeed.stop() -> None`, `.snapshot(coin: str) -> HyperliquidOrderBookSnapshot | None` manage one logical domain feed.
- `async HyperliquidCollector.start() -> None` starts one feed plus metadata refresh; `async stop() -> None` awaits both; `async collect_hourly(*, sample_time: datetime) -> CollectorBatch` preserves funding/context behavior.

- [ ] **Step 1: Write failing WS fixtures.**

Create fake WebSocket messages for `BTC`, `xyz:TSLA`, and `io:SNDK`, including subscription acknowledgements and two complete `l2Book` snapshots. Assert each coin is isolated, the second snapshot replaces rather than merges the first, and an unrelated coin is rejected.

```python
async def test_hyperliquid_complete_snapshot_replaces_book_without_delta_logic():
    feed = HyperliquidOrderBookFeed(
        "wss://test.invalid/ws",
        ["BTC", "xyz:TSLA"],
        connect=connect_hyperliquid_fixture,
        clock=lambda: datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC),
        venue="trade_xyz",
    )
    try:
        await feed.start()
        await wait_until(lambda: feed.snapshot("xyz:TSLA") is not None)

        snapshot = feed.snapshot("xyz:TSLA")
        assert snapshot is not None
        assert snapshot.bids == (BookLevel(price=99.0, base_size=1.0),)
        assert feed.snapshot("BTC") is not None
    finally:
        await feed.stop()
```

Define `connect_hyperliquid_fixture` as the injected async context-manager
fixture that emits subscription acknowledgements and complete snapshots; define
the same 100-iteration `wait_until` helper locally in this test module. Its
`xyz:TSLA` bid must be `BookLevel(price=99.0, base_size=1.0)`.

- [ ] **Step 2: Run the fixture suite and verify RED.**

Run: `uv run pytest tests/test_hyperliquid_websocket.py tests/test_hyperliquid_collector.py -q`

Expected: import or missing-feed failures because the current collector still performs REST `l2Book` calls.

- [ ] **Step 3: Implement the complete-snapshot feed.**

Subscribe with `{"method": "subscribe", "subscription": {"type": "l2Book", "coin": coin}}` for every exact configured coin. Ignore subscription acknowledgements, parse only valid `l2Book` envelopes, verify the returned `coin`, and atomically publish complete sorted levels with the actual receive time. Clear all domain books on disconnect and reconnect, resubscribe, and publish no book until a new valid snapshot arrives.

Refactor `HyperliquidCollector` so metadata `metaAndAssetCtxs` runs in a low-frequency background task and updates optional market metadata. The sampler path must not call REST `l2Book`; the hourly path must continue to call only the existing funding endpoint.

- [ ] **Step 4: Run Hyperliquid-family regressions.**

Run: `uv run pytest tests/test_hyperliquid_websocket.py tests/test_hyperliquid_collector.py tests/test_pipeline.py -q`

Expected: mainnet, `trade_xyz`, and `entropy` parameterized cases pass without cross-dex state mixing or REST order-book calls.

- [ ] **Step 5: Commit.**

```bash
git add src/radar/collectors/hyperliquid.py tests/test_hyperliquid_websocket.py tests/test_hyperliquid_collector.py tests/test_pipeline.py
git commit -m "Use Hyperliquid WebSocket order books"
```

### Task 5: Add the Backpack snapshot-plus-incremental feed

**Files:**
- Modify: `src/radar/collectors/backpack.py`
- Modify: `tests/test_backpack_collector.py`
- Create: `tests/test_backpack_websocket.py`

**Interfaces:**
- `BackpackOrderBookState.seed(*, last_update_id: int, bids: Sequence[BookLevel], asks: Sequence[BookLevel], observed_at: datetime) -> None` establishes a REST snapshot but does not become ready until the documented bridging update is applied.
- `BackpackOrderBookState.apply_update(*, first_update_id: int, final_update_id: int, bids: Sequence[BookLevel], asks: Sequence[BookLevel], observed_at: datetime) -> Literal["buffered", "ready", "updated", "gap"]` applies absolute levels and enforces sequence continuity.
- `BackpackOrderBookFeed(symbols: Sequence[str], *, connect, snapshot_loader, clock, on_book, on_invalidate, error_handler)` owns one WS connection, per-symbol pending updates, and rebuild tasks.
- `async BackpackCollector.start() -> None`, `async BackpackCollector.stop() -> None`, and `async BackpackCollector.collect_hourly(*, sample_time: datetime) -> CollectorBatch` use the feed/cache while retaining existing metadata/funding parsing.

- [ ] **Step 1: Write failing parser/state tests.**

Add fixtures for a REST snapshot, a pre-snapshot update, a bridging update, consecutive updates, zero-size deletes, a sequence gap, and two symbols interleaved. Assert absolute updates do not add sizes, zero removes a level, symbols remain isolated, and a gap returns `"gap"` while clearing readiness.

```python
def test_backpack_update_requires_bridge_then_contiguous_ids():
    snapshot_bids = (BookLevel(99.0, 1.0),)
    snapshot_asks = (BookLevel(101.0, 1.0),)
    update_bids = ((99.0, 2.0),)
    update_asks = ((101.0, 0.0),)
    observed_at = datetime(2026, 9, 15, 10, 0, 8, tzinfo=UTC)
    state = BackpackOrderBookState()
    state.seed(last_update_id=10, bids=snapshot_bids, asks=snapshot_asks, observed_at=observed_at)
    assert state.apply_update(first_update_id=9, final_update_id=10, bids=(), asks=(), observed_at=observed_at) == "buffered"
    assert state.apply_update(first_update_id=10, final_update_id=11, bids=update_bids, asks=update_asks, observed_at=observed_at) == "ready"
    assert state.apply_update(first_update_id=13, final_update_id=13, bids=(), asks=(), observed_at=observed_at) == "gap"
    assert state.ready is False
```

- [ ] **Step 2: Run Backpack focused tests and verify RED.**

Run: `uv run pytest tests/test_backpack_websocket.py tests/test_backpack_collector.py -q`

Expected: missing feed/state behavior and no cache publication failures.

- [ ] **Step 3: Implement the per-symbol synchronized feed.**

Subscribe once with `SUBSCRIBE` parameters `depth.<venue_symbol>` for every configured symbol. Buffer updates received before the REST snapshot is seeded. Ignore updates with `u <= lastUpdateId`, bridge with `U <= lastUpdateId + 1 <= u`, then require `U == previous_u + 1`. Treat `b`/`a` values as absolute sizes and delete zeros. On gap or malformed data, invalidate only that symbol and schedule one REST rebuild for it. Track rebuild tasks so `stop()` cancels and awaits them.

Start a low-frequency metadata task for markets, mark prices, OI, and volume. Preserve the last valid active-market metadata on refresh errors. Use the shared cache callback for complete books; never request `/depth` from `collect_once`.

- [ ] **Step 4: Run Backpack tests including VWAP and failure isolation.**

Run: `uv run pytest tests/test_backpack_websocket.py tests/test_backpack_collector.py tests/test_market_data.py -q`

Expected: snapshot initialization, update continuity, recovery, reconnect, symbol isolation, metadata-failure preservation, and `$1k/$5k/$10k` VWAP tests pass.

- [ ] **Step 5: Commit.**

```bash
git add src/radar/collectors/backpack.py tests/test_backpack_websocket.py tests/test_backpack_collector.py tests/test_market_data.py
git commit -m "Use Backpack WebSocket depth feeds"
```

### Task 6: Move Arcus REST market ingestion behind the sampler boundary

**Files:**
- Modify: `src/radar/collectors/arcus.py`
- Modify: `tests/test_arcus_collector.py`
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- `async ArcusCollector.start() -> None` starts one non-overlapping refresh task.
- `async ArcusCollector.stop() -> None` cancels and awaits the refresh task.
- `async ArcusCollector.refresh_once() -> None` performs one metadata/depth refresh and publishes only successful complete books to `LatestMarketData`.
- `async ArcusCollector.collect_hourly(*, sample_time: datetime) -> CollectorBatch` preserves current funding and context semantics.

- [ ] **Step 1: Write failing slow-REST tests.**

Use an `asyncio.Event`-gated request fixture. Start the collector, leave its refresh blocked, call `pipeline.collect_once(now=datetime(2026, 9, 15, 10, 0, 10, tzinfo=UTC))`, and assert it returns within a short test timeout with no network request made by the sampler. Add a stale-cache test and assert the old observation is omitted.

- [ ] **Step 2: Run focused tests and verify RED.**

Run: `uv run pytest tests/test_arcus_collector.py tests/test_pipeline.py -q`

Expected: the current pipeline/collector coupling causes the sampler to wait for Arcus REST.

- [ ] **Step 3: Implement the background refresh loop.**

Keep one refresh task per Arcus collector. Await the whole refresh before sleeping for the next interval, preventing overlapping cycles. Within one refresh, retain concurrent or bounded-concurrent per-market requests using the existing `asyncio.gather` pattern. On an individual failure, omit that market from the cache update; on metadata failure, retain previous metadata and books. Publish actual REST observation times and leave stale filtering to the cache sampler.

- [ ] **Step 4: Run Arcus and pipeline tests.**

Run: `uv run pytest tests/test_arcus_collector.py tests/test_pipeline.py -q`

Expected: slow Arcus work no longer blocks aligned market sampling; existing parser, funding, context, partial-failure, and VWAP tests pass.

- [ ] **Step 5: Commit.**

```bash
git add src/radar/collectors/arcus.py tests/test_arcus_collector.py tests/test_pipeline.py
git commit -m "Move Arcus REST refresh off market sampler"
```

### Task 7: Wire application shutdown and telegram-smoke compatibility

**Files:**
- Modify: `src/radar/app.py`
- Modify: `tests/test_app.py`
- Modify: `src/radar/pipeline.py` only if the readiness-wait public method needs a final signature adjustment

**Interfaces:**
- `RadarApplication.run()` stops scheduling before entering shutdown, awaits `pipeline.stop()`, flushes Parquet, then cancels/awaits `AlertWorker`, then closes `SQLiteRuntimeStore`.
- `run_telegram_smoke(application, *, symbol="BTC", now=None, readiness_timeout_seconds=30.0)` starts the pipeline, waits for at least two ready/fresh `$10k` feeds for the symbol, samples once, stops the pipeline, performs the final flush, processes the synthetic alert, and closes runtime resources after cleanup.

- [ ] **Step 1: Write failing lifecycle tests.**

Extend `RecordingPipeline` and `FakeWorker` to record lifecycle events. Assert the application order is `pipeline.stop`, `flush`, `worker.stop`, `runtime.close`, and assert no pipeline append event occurs after the flush event. Add a smoke fake that records `start`, `wait_for_market_feeds`, `collect_once`, `flush`, and `stop`.

```python
async def test_telegram_smoke_starts_waits_samples_and_stops_pipeline():
    pipeline = SmokePipelineWithEvents()
    application = make_smoke_application(pipeline)
    await run_telegram_smoke(application, symbol="BTC", now=NOW)
    assert pipeline.events == [
        "start",
        "wait_for_market_feeds:BTC",
        "collect_once",
        "stop",
        "flush",
    ]
```

Define `SmokePipelineWithEvents` with the existing `start`,
`wait_for_market_feeds`, `collect_once`, `stop`, and `flush_storage` test
interfaces, and define `make_smoke_application` using the existing fake
runtime and processor fixtures. Keep `NOW` as the existing timezone-aware
app-test timestamp.

- [ ] **Step 2: Run app tests and verify RED.**

Run: `uv run pytest tests/test_app.py -q`

Expected: current shutdown and smoke paths have the old order and call cache-only `collect_once` without starting/waiting.

- [ ] **Step 3: Implement the explicit lifecycle order.**

Make the application loop exit before invoking `pipeline.stop`. Do not start a new scheduler task during cleanup. Await the pipeline barrier before `flush_now`. Stop the alert worker after the flush and await it before closing SQLite. Keep exception logging and `CancelledError` propagation intact.

Update `run_telegram_smoke` to call `pipeline.start`, `wait_for_market_feeds`, `collect_once`, and processing inside a `try`; in `finally`, await pipeline stop, perform the final flush after the barrier, and close the runtime store. Do not add network calls to `collect_once`.

- [ ] **Step 4: Run app and full smoke-path tests.**

Run: `uv run pytest tests/test_app.py tests/test_alert_processor.py tests/test_alert_worker.py -q`

Expected: lifecycle ordering, bounded readiness timeout, synthetic alert formatting, flush, and existing alert-worker behavior pass.

- [ ] **Step 5: Commit.**

```bash
git add src/radar/app.py tests/test_app.py src/radar/pipeline.py
git commit -m "Finish cache-only application lifecycle"
```

### Task 8: Integrate all collectors and complete deterministic regression coverage

**Files:**
- Modify: `src/radar/pipeline.py`
- Modify: `src/radar/collectors/lighter.py`
- Modify: `src/radar/collectors/hyperliquid.py`
- Modify: `src/radar/collectors/backpack.py`
- Modify: `src/radar/collectors/arcus.py`
- Modify: `tests/test_live_collectors.py` only to preserve `pytest.mark.live` around network tests
- Modify: existing collector tests where old one-shot calls need explicit lifecycle setup

**Interfaces:**
- `MarketDataPipeline.from_config()` constructs one shared `LatestMarketData`, passes it to every configured concrete collector, passes the spread monitor freshness limit, and preserves the current venue names/configured symbols.
- Every configured concrete collector publishes only complete local books and keeps funding/hourly collection on the hourly path.

- [ ] **Step 1: Add integration fixtures for all seven venues.**

Build a synthetic config with one market for Lighter, Robinhood, Hyperliquid, `trade_xyz`, `entropy`, Backpack, and Arcus. Seed the shared cache for selected feeds, leave others not-ready, call `collect_once`, and assert exactly the ready/fresh feeds are in the batch and `RadarState.markets`.

- [ ] **Step 2: Add negative integration tests.**

Assert no stale/future snapshot, missing `$10k` depth, mismatched Hyperliquid coin, Backpack gap, Lighter reconnect, Arcus timeout, or metadata failure creates a synthetic current sample. Assert other venues remain present.

- [ ] **Step 3: Update direct collector tests to use explicit lifecycle.**

Where tests exercise parser-only behavior, keep fixture parsers direct. Where tests exercise production collection, call `start`, wait for the injected feed/cache callback, call cache-only `collect_once`, and call `stop` in `finally`. Keep all public network tests marked `live` and do not make them part of the non-live suite.

- [ ] **Step 4: Run the complete non-live suite.**

Run: `uv run pytest -m "not live"`

Expected: all deterministic collector, pipeline, state, storage, monitor, alert, dashboard, and history tests pass.

- [ ] **Step 5: Commit integration regressions.**

```bash
git add src/radar tests/test_pipeline.py tests/test_lighter_collector.py tests/test_hyperliquid_collector.py tests/test_backpack_collector.py tests/test_arcus_collector.py tests/test_live_collectors.py
git commit -m "Integrate cache-only market ingestion"
```

### Task 9: Run live validation and quality gates

**Files:**
- No product-code changes are planned in this task.
- Create temporary validation output only outside the repository or in ignored runtime paths.

- [ ] **Step 1: Run focused live collector smoke tests.**

Run: `uv run pytest -m live tests/test_live_collectors.py -q` with network access. Record endpoint availability, warm-up time, per-venue counts, and any timeout/429/405 errors. If live access is unavailable, report the exact environment/network error and do not fabricate results.

- [ ] **Step 2: Run the real application scheduler for at least 30 minutes.**

Use the current expanded 127-feed configuration and the normal application scheduling path, not a back-to-back loop. Allow WS warm-up before steady-state accounting. Record scheduled slots, produced aligned `sample_time` values, missing slots, duplicate slots, total/per-venue completeness, sampler duration distribution, source book ages, reconnect counts, REST timeout/429 counts, hourly coordinator duration, and whether hourly work caused a missed market slot.

- [ ] **Step 3: Run quality gates only when available.**

Run:

```bash
uv run pytest -m "not live"
uv run ruff check .
uv run mypy src
uv run python -m compileall src
git diff --check
```

If Ruff or Mypy is not configured/available, record that fact and do not add dependencies or configuration. Compileall and diff-check must still run.

- [ ] **Step 4: Review the complete diff and commit only material improvement.**

Confirm the 10-second path contains no network call, no full L2 persistence was added, shutdown has no post-flush appends, and the live result materially improves the prior REST-fanout baseline. If a live blocker prevents proving improvement, leave implementation commits intact only if deterministic behavior is complete and report the blocker; do not add speculative optimizations.

- [ ] **Step 5: Create a final checkpoint only when needed.**

If all Task 9 changes are already committed by the per-task commits and the working tree is clean, do not create an empty commit. Otherwise commit only the remaining latency-independent market-ingestion changes with:

```bash
git add src/radar tests
git commit -m "Complete latency-independent market ingestion"
```

## Final report checklist

Report the starting SHA, ending SHA, all commits, changed files, exact WS endpoints/channels, REST calls removed from the 10-second path, snapshot/update semantics, reconnect behavior, before/after cadence metrics, steady-state missed slots, per-venue completeness and book ages, reconnect/errors, hourly duration and slot impact, non-live/live/quality results, known limitations, and `git status`. Do not push, merge, or open a pull request.
