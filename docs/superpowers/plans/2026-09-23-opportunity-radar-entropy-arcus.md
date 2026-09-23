# Entropy + Arcus Collector Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add verified public Entropy HIP-3 and Arcus equity-perpetual feeds to the existing normalized market-data pipeline without changing monitor, storage, alert, or existing-venue behavior.

**Architecture:** Entropy reuses `HyperliquidCollector` with the live HIP-3 dex namespace `io` and logical venue name `entropy`. Arcus gets one small native REST collector using the verified `api.arcus.xyz` endpoints; it parses market metadata, L2 snapshots, and funding history into the existing `CollectorBatch` models. `MarketDataPipeline.from_config()` conditionally adds collectors only when enabled markets are configured, so the existing `RadarState` and `SpreadMonitor` path remains unchanged.

**Tech Stack:** Python 3.13, asyncio, existing `urllib` JSON transport, Pydantic models, pytest/pytest-asyncio, YAML configuration.

**Spec:** User-provided Task “Add Entropy + Arcus to opportunity-radar in one implementation”, 2026-09-23; `AGENTS.md`; `docs/superpowers/plans/2026-09-15-opportunity-radar-macbook-v1.md`.

## Global Constraints

- Public/read-only market data only; no order placement, signing, private API, or trading code.
- Preserve collectors → normalized `RadarState` → existing `SpreadMonitor` → Telegram.
- Keep aligned 10-second samples, fail-closed missing data, no stale snapshot reuse, and concurrent venue collection.
- Compute executable `$1k`, `$5k`, and `$10k` VWAPs from in-memory live L2; never persist full L2.
- Do not add monitors, thresholds, storage changes, dashboard work, websocket code, frameworks, or dependencies.
- Only enable exact canonical equity matches: `SNDK`, `NVDA`, `TSLA`, `HOOD`, `GOOGL`, `AAPL`, `META`, `MU`; do not add proxy pairs or unproven pre-IPO equivalents.
- Require explicit non-zero verified venue fees in `fees_bps`; do not silently use zero.
- Run fixture/unit tests before live tests, then the full non-live suite and available quality gates.

## Review Focus

- Arcus order-book arrays are parsed as `[price, size]` levels and insufficient depth yields `None` VWAPs rather than fabricated prices; tests live in `tests/test_arcus_collector.py`.
- Arcus metadata status/type filtering omits offline/non-perpetual markets and preserves only configured exact symbols; tests live in `tests/test_arcus_collector.py`.
- Funding timestamps remain venue timestamps: Arcus microseconds from `/v1/fundingRates`, Entropy milliseconds from Hyperliquid `fundingHistory`; tests live in both collector test files.
- Per-symbol Arcus/Entropy request failures omit only that symbol and never reuse `RadarState` data; tests live in collector and pipeline tests.
- Missing Entropy/Arcus fee configuration is rejected before a venue can participate in spread candidates; tests live in `tests/test_config.py`.

### Task 1: Fixture contract tests and configuration expectations

**Files:**
- Create: `tests/fixtures/entropy/meta_and_asset_ctxs.json`
- Create: `tests/fixtures/entropy/l2_io_sndk.json`
- Create: `tests/fixtures/entropy/funding_io_sndk.json`
- Create: `tests/fixtures/arcus/markets.json`
- Create: `tests/fixtures/arcus/l2_sndk_usd.json`
- Create: `tests/fixtures/arcus/l2_nvda_usd.json`
- Create: `tests/fixtures/arcus/funding_sndk_usd.json`
- Create: `tests/test_arcus_collector.py`
- Modify: `tests/test_hyperliquid_collector.py`
- Modify: `tests/test_config.py`
- Modify: `tests/test_pipeline.py`

**Interfaces:**
- `parse_arcus_markets(payload: object) -> dict[str, ArcusMarketDetail]`.
- `parse_arcus_l2_order_book(payload: object) -> tuple[list[BookLevel], list[BookLevel]]`.
- `parse_arcus_funding_rates(payload: object, *, expected_market_id: int) -> ArcusFundingPoint | None`.
- Existing `HyperliquidCollector(..., venue="entropy", dex="io")` is the Entropy interface.

- [x] **Step 1: Add deterministic fixtures using the verified response shapes.**

  The Entropy fixture must contain `io:SNDK` and a non-enabled `io:ANTH` entry in the aligned `universe`/contexts arrays. The Arcus market fixture must include online perpetual records for `SNDK-USD` and `NVDA-USD`, with `markPrice`, `oraclePrice`, `fundingRate`, `nextFundingAt`, `volume24hNotional`, and `openInterest`. Arcus L2 fixtures must use `bids`/`asks` arrays of `[price, size]`; the SNDK fixture has enough depth for all three VWAP targets, while the NVDA fixture can be used for partial-failure coverage. Funding fixture entries use `marketId`, `marketDisplayName`, `fundingRate`, and microsecond `time`.

- [x] **Step 2: Write the failing Arcus parser/normalization tests.**

  Assert market metadata is keyed by `marketDisplayName`, offline and non-perpetual records are omitted, L2 bids sort descending and asks ascending, invalid array shapes raise `ValueError`, and funding `time` becomes a UTC `datetime` without synthesizing an effective timestamp. Add a fixture transport that records URL/method/params and returns the fixture by endpoint.

- [x] **Step 3: Write the failing Entropy HIP-3 normalization test.**

  Instantiate `HyperliquidCollector` with one `MarketConfig(venue="entropy", venue_symbol="io:SNDK", canonical_symbol="SNDK")`, `venue="entropy"`, and `dex="io"`. Assert the metadata request is `{"type": "metaAndAssetCtxs", "dex": "io"}`, book/funding requests use `io:SNDK` without a `dex` field, and the output contains market, funding, and hourly context with canonical `SNDK`.

- [x] **Step 4: Add failing configuration and pipeline tests.**

  Extend the example-config expectations to require one Entropy SNDK feed, eight Arcus equity feeds, and explicit `entropy: 9.0` and `arcus: 2.25` fees. Add validation tests that enabled Entropy/Arcus markets without their fee entry raise `ValidationError`. Add a pipeline construction test expecting `LighterCollector`, native `HyperliquidCollector`, `trade_xyz` HIP-3 when configured, Entropy HIP-3 when configured, and `ArcusCollector` when configured.

- [x] **Step 5: Run the focused tests and verify RED.**

  Run:

  ```bash
  uv run pytest tests/test_arcus_collector.py tests/test_hyperliquid_collector.py tests/test_config.py tests/test_pipeline.py
  ```

  Expected result: the new tests fail because Arcus parser/collector and the new pipeline/config wiring do not exist yet; existing tests must remain otherwise understandable and runnable.

### Task 2: Implement the native Arcus collector

**Files:**
- Create: `src/radar/collectors/arcus.py`
- Modify: `src/radar/collectors/__init__.py` only if a public export is needed by tests
- Test: `tests/test_arcus_collector.py`

**Interfaces:**
- `ArcusCollector(markets, *, request_json=default_request_json, clock=..., error_handler=None)` implements `Collector` and exposes `venue == "arcus"`.
- Constants are `MARKETS_URL = "https://api.arcus.xyz/v1/markets"`, `L2_ORDER_BOOK_URL = "https://api.arcus.xyz/v1/l2OrderBook"`, and `FUNDING_RATES_URL = "https://api.arcus.xyz/v1/fundingRates"`.

- [x] **Step 1: Implement the smallest parser dataclasses and parsers.**

  `ArcusMarketDetail` stores market display name, market id, mark/oracle prices, funding rate, next funding time, open interest, and 24-hour notional volume. Parse only `status == "ONLINE"` and `type == "PERPETUAL"`; reject malformed numeric fields with the existing `finite_float`/`positive_float`/`non_negative_float` helpers. Parse L2 `[price, size]` arrays into `BookLevel`s and sort them. Parse funding history entries for the expected market id/display name and choose the newest API timestamp.

- [x] **Step 2: Implement concurrent per-market collection and normalized models.**

  Fetch `/v1/markets` once. Use `asyncio.gather` over configured markets for L2 snapshots. For `include_hourly_context=True`, gather funding requests using `params={"market": market.venue_symbol}` and build `HourlyContext.sample_time` from the aligned hour while setting `observed_at` from the metadata receive clock. Set `FundingSnapshot.effective_time` only from the parsed API `time`, `next_funding_time` from `nextFundingAt`, and `observed_at` from the receive clock. Compute all six VWAP fields using `buy_vwap`/`sell_vwap`.

- [x] **Step 3: Preserve fail-closed behavior and run the focused tests.**

  Catch metadata failure as an empty `CollectorBatch`; catch each book/funding failure, report it through `report_collector_error`, and omit only the failed output. Do not cache a previous snapshot. Run:

  ```bash
  uv run pytest tests/test_arcus_collector.py -q
  ```

  Expected result: all Arcus parser, normalization, VWAP, funding, partial-failure, and metadata-failure tests pass.

### Task 3: Integrate Entropy and Arcus into config and pipeline

**Files:**
- Modify: `src/radar/config.py`
- Modify: `src/radar/pipeline.py`
- Modify: `config/radar.example.yaml`
- Modify: `tests/test_config.py`
- Modify: `tests/test_pipeline.py`
- Test: `tests/test_hyperliquid_collector.py`, `tests/test_arcus_collector.py`

**Interfaces:**
- `RadarConfig` requires an explicit fee for enabled `trade_xyz`, `entropy`, or `arcus` markets; fee lookup remains case-insensitive in the existing monitor code.
- `MarketDataPipeline.from_config()` appends `HyperliquidCollector(..., venue="entropy", dex="io")` and `ArcusCollector(...)` only when that venue has enabled markets.

- [x] **Step 1: Add explicit fee validation without changing existing venue semantics.**

  Generalize the existing `trade_xyz` validator to the set `{"trade_xyz", "entropy", "arcus"}` and retain the existing case-insensitive behavior and error style. Do not require new fees for unrelated disabled/unknown venues.

- [x] **Step 2: Add conditional collectors.**

  Import `ArcusCollector` alongside the existing local imports. Preserve the current first two collectors and trade_xyz behavior; append Entropy with dex `io` and Arcus only when `markets_for_venue` returns enabled markets. Pass through the existing request seam, clock, and error handler.

- [x] **Step 3: Update the example configuration with only live-confirmed exact matches.**

  Add `entropy: 9.0` and `arcus: 2.25`. Add `entropy/io:SNDK/SNDK`. Add Arcus `SNDK-USD`, `NVDA-USD`, `TSLA-USD`, `HOOD-USD`, `GOOGL-USD`, `AAPL-USD`, `META-USD`, and `MU-USD`, all mapped to the same canonical symbol. Do not add `io:ANTH`, proxies, or unverified symbols.

- [x] **Step 4: Run focused GREEN tests and the full non-live suite.**

  Run:

  ```bash
  uv run pytest tests/test_arcus_collector.py tests/test_hyperliquid_collector.py tests/test_config.py tests/test_pipeline.py
  uv run pytest -m "not live"
  ```

  Expected result: all tests pass and existing Lighter/Hyperliquid/trade_xyz behavior remains unchanged.

### Task 4: Add live smoke coverage and complete verification

**Files:**
- Modify: `tests/test_live_collectors.py`
- Modify: `tests/test_config.py` only if an assertion needs the final exact feed set

**Interfaces:**
- Live tests use public read-only endpoints through `HyperliquidCollector` and `ArcusCollector`; no credentials or order APIs.

- [x] **Step 1: Add marked live tests.**

  Add an Entropy SNDK smoke test asserting canonical mapping, BBO, available VWAP fields, funding, hourly context, and aligned sample time. Add an Arcus test over the eight enabled symbols asserting each is online/collected, canonical mappings are exact, BBO is valid, `$1k/$5k/$10k` VWAPs are either numeric or `None` only when live depth is insufficient, and funding/context are present from the verified endpoints.

- [x] **Step 2: Run the live validations after unit tests pass.**

  Run:

  ```bash
  uv run pytest -m live
  ```

  Record the actual collected symbols, VWAP availability, funding/context counts, and any transient public-API failures; do not convert a live failure into a stale fallback.

- [x] **Step 3: Run available quality gates.**

  Run:

  ```bash
  command -v ruff && ruff --version && uv run ruff check .
  command -v mypy && mypy --version && uv run mypy src
  uv run python -m compileall src
  git diff --check
  uv run pytest -m "not live"
  ```

  Ruff and Mypy are informational only when unavailable; do not add either dependency or configuration. Fix only violations introduced by this task.

- [x] **Step 4: Self-review, commit logical changes, and leave the tree clean.**

  Review the diff for API/schema guesses, stale-data reuse, accidental monitor/storage changes, and proxy/pre-IPO mappings. Create logical commits for the Arcus collector/tests and Entropy/config/pipeline integration (plus live test coverage if separate). Do not merge, open a PR, or push. Confirm `git status --short` is empty.
