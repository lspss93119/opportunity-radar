# Backpack Exchange Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a public/read-only Backpack collector for the verified exact-equity perpetual markets and route its normalized data through the existing radar pipeline.

**Architecture:** Add one native REST collector that follows the existing collector protocol and produces `MarketSnapshot`, `FundingSnapshot`, and `HourlyContext`. It will fetch Backpack metadata and bulk context endpoints, fetch configured order books concurrently, sort unsorted depth levels before using the existing executable VWAP helpers, and omit only failed symbols. `MarketDataPipeline` will instantiate it only when Backpack markets are configured; no monitor, storage, alert, or existing venue behavior changes.

**Tech Stack:** Python 3.13, asyncio, the repository's public JSON transport, Pydantic models, pytest/pytest-asyncio, and the existing VWAP helpers.

**Spec:** User request in the current task; official Backpack API documentation at `https://docs.backpack.exchange/` and the live public API at `https://api.backpack.exchange`.

## Global Constraints

- Public/read-only Backpack API only; no signed requests, credentials, order endpoints, or trading execution.
- Use only verified Backpack PERP symbols: `SNDK.US_USDC_PERP`, `NVDA.US_USDC_PERP`, `TSLA.US_USDC_PERP`, `HOOD.US_USDC_PERP`, `GOOGL.US_USDC_PERP`, `AAPL.US_USDC_PERP`, `META.US_USDC_PERP`, and `MU.US_USDC_PERP`.
- Canonical symbols remain `SNDK`, `NVDA`, `TSLA`, `HOOD`, `GOOGL`, `AAPL`, `META`, and `MU`; do not add proxy/index pairs or Backpack BTC/ETH/SOL feeds.
- Use explicit standard futures Tier 1 taker fee `0.0005 = 5.0` bps from the verified public `GET https://api.backpack.exchange/wapi/v1/feeTiers` response; do not use VIP/account-specific tiers.
- Keep fail-closed behavior: a metadata failure returns an empty Backpack batch; a symbol depth/context failure omits only that symbol/data point and never reuses a previous snapshot.
- Preserve aligned 10-second sampling, observed-at semantics, existing monitor thresholds, storage schemas, alerts, and all existing venue behavior.
- Do not persist L2 books, add a framework, add dependencies, or add a websocket path.

## Review Focus

- Unordered Backpack depth arrays must still produce the actual best bid/ask and correct executable VWAP; `tests/test_backpack_collector.py::test_backpack_depth_parser_sorts_levels` and the collector VWAP test pin this down.
- A listed market with no usable order book must be omitted rather than producing a stale/invalid snapshot; `test_backpack_collector_omits_only_symbol_when_depth_fails` covers the per-symbol fail-closed branch.
- API timestamps have different units/formats: depth/open-interest milliseconds or microseconds, and funding interval timestamps are ISO strings without a timezone; parser tests assert UTC normalization while retaining venue-provided effective time.
- Nullable normalized context must remain nullable when Backpack does not return a usable value; tests cover empty funding history and missing ticker volume.
- Exact config mappings must be enabled only for symbols returned by the verified PERP market list; config and pipeline integration tests assert all eight mappings and no crypto expansion.

### Task 1: Add Backpack parsing and normalized collection

**Files:**
- Create: `src/radar/collectors/backpack.py`
- Create: `tests/fixtures/backpack/markets.json`
- Create: `tests/fixtures/backpack/depth_sndk_us_usdc_perp.json`
- Create: `tests/fixtures/backpack/depth_nvda_us_usdc_perp.json`
- Create: `tests/fixtures/backpack/mark_prices.json`
- Create: `tests/fixtures/backpack/open_interest.json`
- Create: `tests/fixtures/backpack/funding_sndk_us_usdc_perp.json`
- Create: `tests/fixtures/backpack/tickers.json`
- Create: `tests/test_backpack_collector.py`

**Interfaces:**
- Consumes `MarketConfig`, `CollectorBatch`, the existing `request_json` seam, `buy_vwap`, and `sell_vwap`.
- Produces `BackpackCollector`, parser functions used by deterministic tests, and normalized batches with `venue == "backpack"`.

- [ ] **Step 1: Add representative live-shaped fixtures and failing parser/normalization tests.**

  Fixture payloads must preserve the verified live shapes:
  `markets` is a list with `marketType`, `orderBookState`, `visible`, `rwaMarketType`, `symbol`, and `baseSymbol`; depth is `{asks: [[price, size]], bids: [[price, size]], timestamp}`; mark prices are a list of `{symbol, markPrice, indexPrice, fundingRate, nextFundingTimestamp}`; open interest is a list of `{symbol, openInterest, timestamp}`; funding history is a list of `{symbol, fundingRate, intervalEndTimestamp}`; tickers are a list of `{symbol, volume, quoteVolume}`.

  Add tests for:
  - keeping only visible/open `PERP` markets with `rwaMarketType == "STOCK"`, rejecting duplicate symbols, and ignoring spot/inactive records;
  - sorting depth bids descending and asks ascending;
  - parsing mark/index/funding/next-funding and bulk OI/ticker maps;
  - selecting the newest funding history point using its API timestamp;
  - a full fixture collector call producing all six executable VWAP fields, mark/index, funding, hourly OI/volume, aligned hourly context, and local `observed_at`;
  - insufficient depth returning `None` for the affected VWAP fields;
  - one depth failure omitting only that symbol, metadata failure returning an empty batch, empty funding returning no funding snapshot, and missing volume returning `HourlyContext.volume_24h is None`.

  Use a fixture transport that records URL, method, and params. Assert the collector calls the verified public endpoints, uses `marketType=PERP` and `limit=1000` where applicable, and never calls an authenticated endpoint.

- [ ] **Step 2: Run the focused tests to verify the new behavior fails.**

  Run: `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run pytest tests/test_backpack_collector.py -q`

  Expected: FAIL because `radar.collectors.backpack` and its parser/collector interfaces do not exist yet.

- [ ] **Step 3: Implement the minimal Backpack collector.**

  Implement these constants and request shapes:

  ```python
  BASE_URL = "https://api.backpack.exchange"
  MARKETS_URL = f"{BASE_URL}/api/v1/markets"
  DEPTH_URL = f"{BASE_URL}/api/v1/depth"
  MARK_PRICES_URL = f"{BASE_URL}/api/v1/markPrices"
  OPEN_INTEREST_URL = f"{BASE_URL}/api/v1/openInterest"
  FUNDING_RATES_URL = f"{BASE_URL}/api/v1/fundingRates"
  TICKERS_URL = f"{BASE_URL}/api/v1/tickers"
  ```

  Fetch markets, mark prices, open interest, and `tickers?interval=1d` once per collection. Fetch each configured symbol's `depth?symbol=<actual>&limit=1000` concurrently. When hourly context is requested, fetch each configured symbol's `fundingRates?symbol=<actual>&limit=1` concurrently; the current market `fundingRate`/`nextFundingTimestamp` remains available from the mark-price response, while `FundingSnapshot.effective_time` comes only from the funding history `intervalEndTimestamp`.

  Parse API timestamps as UTC: numeric depth timestamps are microseconds for provenance only, numeric open-interest/next-funding timestamps are milliseconds, and a funding ISO timestamp without an offset is interpreted as UTC because the API field is an exchange interval boundary. Set `MarketSnapshot.observed_at` from the injected clock after each successful depth response; set metadata/context/funding observations from the injected clock at the corresponding response completion. Keep `HourlyContext.sample_time` equal to `sample_time` floored to the UTC hour.

  Sort levels before building the snapshot, pass them to the existing VWAP functions for `$1k`, `$5k`, and `$10k`, and return `None` for any target that cannot be fully filled. Do not retain the raw book after normalization. Catch per-symbol exceptions, call `report_collector_error`, and return `None` for that symbol; return an empty batch when the required market metadata request fails.

- [ ] **Step 4: Run the focused tests to verify they pass.**

  Run: `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run pytest tests/test_backpack_collector.py -q`

  Expected: all Backpack parser, VWAP, timestamp, nullable-field, and fail-closed tests PASS.

- [ ] **Step 5: Commit the collector and fixtures.**

  ```bash
  git add src/radar/collectors/backpack.py tests/test_backpack_collector.py tests/fixtures/backpack
  git commit -m "Add Backpack public market data collector"
  ```

### Task 2: Wire Backpack into config, pipeline, example config, and live smoke coverage

**Files:**
- Modify: `src/radar/config.py:EXPLICIT_FEE_VENUES`
- Modify: `src/radar/pipeline.py:MarketDataPipeline.from_config`
- Modify: `config/radar.example.yaml`
- Modify: `tests/test_config.py`
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_live_collectors.py`

**Interfaces:**
- Consumes `BackpackCollector` and the existing `MarketConfig`/`RadarConfig` validation.
- Produces automatic `BackpackCollector` construction only when enabled Backpack markets exist, eight example feed mappings, and a marked live smoke test.

- [ ] **Step 1: Add failing config and pipeline integration tests.**

  Add `backpack` to the explicit-fee venue set and tests that:
  - an enabled Backpack market without `backpack` in `fees_bps` is rejected;
  - the example config loads with `backpack: 5.0` and exactly the eight verified Backpack `*.US_USDC_PERP` mappings;
  - `MarketDataPipeline.from_config` appends `BackpackCollector` only when Backpack markets are configured and preserves the existing collector order before it;
  - a Backpack-only configured batch can flow through the existing pipeline request seam without modifying other venue constructors.

  Run: `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run pytest tests/test_config.py tests/test_pipeline.py -q`

  Expected: FAIL because Backpack is not currently an explicit-fee venue, is absent from the example config, and pipeline wiring is missing.

- [ ] **Step 2: Implement the minimal config/pipeline/example wiring.**

  Add `"backpack"` to `EXPLICIT_FEE_VENUES`. Import `BackpackCollector` locally in `from_config`, and append it after the existing Arcus conditional only when `markets_for_venue(config.markets, "backpack")` is non-empty. Add `backpack: 5.0` plus these enabled example markets, preserving each canonical symbol exactly:

  ```yaml
  - venue: backpack
    venue_symbol: SNDK.US_USDC_PERP
    canonical_symbol: SNDK
  - venue: backpack
    venue_symbol: NVDA.US_USDC_PERP
    canonical_symbol: NVDA
  - venue: backpack
    venue_symbol: TSLA.US_USDC_PERP
    canonical_symbol: TSLA
  - venue: backpack
    venue_symbol: HOOD.US_USDC_PERP
    canonical_symbol: HOOD
  - venue: backpack
    venue_symbol: GOOGL.US_USDC_PERP
    canonical_symbol: GOOGL
  - venue: backpack
    venue_symbol: AAPL.US_USDC_PERP
    canonical_symbol: AAPL
  - venue: backpack
    venue_symbol: META.US_USDC_PERP
    canonical_symbol: META
  - venue: backpack
    venue_symbol: MU.US_USDC_PERP
    canonical_symbol: MU
  ```

- [ ] **Step 3: Run integration tests to verify they pass.**

  Run: `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run pytest tests/test_config.py tests/test_pipeline.py tests/test_backpack_collector.py -q`

  Expected: all config, pipeline, and Backpack tests PASS, with no change to existing venue order or behavior.

- [ ] **Step 4: Add and run the marked live smoke test.**

  Add a `@pytest.mark.live` test using the example Backpack mappings and the real default transport. It must collect with `include_hourly_context=True`, assert at least one exact-equity market snapshot, assert mark/index and current funding values when returned, assert the eight canonical mappings are exact, and print/record each available `$1k/$5k/$10k` buy/sell VWAP without treating unavailable depth as a test failure. It must not call any authenticated endpoint.

  Run: `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run pytest -m live tests/test_live_collectors.py -q`

  Expected: Backpack live smoke succeeds for the currently available public markets, or records a clearly reported network/API failure without affecting non-live tests.

- [ ] **Step 5: Commit integration and live coverage.**

  ```bash
  git add src/radar/config.py src/radar/pipeline.py config/radar.example.yaml tests/test_config.py tests/test_pipeline.py tests/test_live_collectors.py
  git commit -m "Enable Backpack exact-equity feeds"
  ```

### Task 3: Full verification and bounded self-review

**Files:**
- Modify only files from Tasks 1-2 if a test-proven Backpack defect is found.

- [ ] **Step 1: Run the full non-live suite.**

  Run: `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run pytest -m "not live"`

  Expected: all non-live tests PASS and no existing venue tests regress.

- [ ] **Step 2: Run quality checks only if available.**

  Run: `ruff check .`; `mypy src`; `UV_CACHE_DIR=/tmp/opportunity-radar-uv-cache uv run python -m compileall src`; and `git diff --check`.

  Expected: Ruff and Mypy pass when installed, compileall exits 0, and diff check is clean. Do not add lint/type-check dependencies or unrelated cleanup.

- [ ] **Step 3: Review the diff for scope and fail-closed behavior.**

  Confirm no order/signing/private API code, no raw L2 persistence, no changes to existing venues/monitor/storage/alerts, no stale snapshot reuse, and no enabled Backpack symbols outside the eight verified exact-equity PERPs. Resolve only issues demonstrated by tests or the stated requirements.

- [ ] **Step 4: Leave the worktree clean and stop without merge, PR, or push.**

  Run: `git status --short --branch` and record the starting SHA, ending SHA, commit SHAs, API details, feed count, live results, skipped symbols/reasons, and known limitations.
