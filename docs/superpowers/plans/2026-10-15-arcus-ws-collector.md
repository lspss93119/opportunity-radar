# Arcus WebSocket Collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a tested, selectable Arcus `l2Orderbook` WebSocket collector with fail-closed recovery and non-live REST fallback while preserving REST production behavior.

**Architecture:** Extend the existing in-process asyncio collector/cache boundary. The WS feed writes complete normalized books into `LatestMarketData`; the existing fixed sampler remains the only path that creates `MarketSnapshot` rows. WS freshness controls live eligibility, while fallback REST books remain archival-only in WS mode.

**Tech Stack:** Python 3.13, asyncio, `websockets`, Pydantic, pytest/pytest-asyncio, existing VWAP and Parquet pipeline.

**Spec:** `docs/superpowers/specs/2026-10-15-arcus-ws-collector-design.md`

## Global Constraints

- Arcus REST remains the default and Production is not switched in this task.
- The Radar sampler remains a 10-second cadence and the existing Parquet/SQLite schemas remain unchanged.
- `observed_at` is local receive/response time; it is not treated as an exchange event timestamp.
- No orders, signing, credentials, Dashboard changes, or monitor strategy changes.
- Every production-code change is preceded by a failing regression test and followed by focused and full non-live verification.

## Review Focus

- A valid REST fallback book must be archived without becoming a Live signal; test through `LatestMarketData` and the pipeline.
- A reconnect must clear every Arcus book and require a new valid snapshot; test no stale reuse.
- A lower or malformed sequence must not replace a valid book; test both optional sequence fields and backwards values.
- A `degraded` notice or per-market silence must invalidate only readiness, not corrupt other configured markets; test multi-market isolation.
- Existing REST configuration and call counts must remain unchanged; run the existing Arcus and pipeline suites.

---

### Task 1: In-memory live eligibility and configuration

**Files:**
- Modify: `src/radar/market_data.py`
- Modify: `src/radar/config.py`
- Modify: `src/radar/pipeline.py`
- Test: `tests/test_market_data.py`, `tests/test_config.py`, `tests/test_pipeline.py`

**Interfaces:**
- `LatestMarketData.update_book(..., live_signal_eligible: bool = True)` stores an in-memory eligibility bit only.
- `MarketAvailabilityDiagnostic.live_signal_eligible: bool` lets the pipeline keep fresh fallback rows for storage while excluding them from live state.
- `RadarConfig.arcus.source` is `Literal["rest", "ws"]` with default `"rest"` and `ArcusCollector` receives the config.

- [ ] Write failing tests for an ineligible fresh book being returned in the archival candidate batch but excluded from `RadarState`, and for REST-default config/pipeline wiring.
- [ ] Run the focused tests and observe the expected missing-field/behavior failures.
- [ ] Implement the smallest cache/config/pipeline changes; do not change `MarketSnapshot` or storage schema.
- [ ] Run focused tests and verify they pass.
- [ ] Commit `feat: add Arcus source selection and live eligibility gate`.

### Task 2: Arcus WebSocket protocol and feed lifecycle

**Files:**
- Modify: `src/radar/collectors/arcus.py`
- Create: `tests/test_arcus_websocket.py`

**Interfaces:**
- `parse_arcus_ws_message(payload: object, configured_markets: frozenset[str]) -> ArcusWsSnapshot | None` parses configured `l2Orderbook` full snapshots and notices.
- `ArcusWebSocketFeed(...).start()`/`stop()` manage one reconnecting socket; callbacks receive a validated snapshot or invalidation.
- `ArcusWsSnapshot` carries market, normalized levels, local `observed_at`, optional raw venue timestamp, and optional sequence IDs.

- [ ] Write failing parser/feed tests for subscription payload, valid full snapshot/VWAP, invalid/empty/crossed book, old sequence, degraded/silence, disconnect, reconnect/resubscribe, and clean stop.
- [ ] Run only the new tests and observe failures because the feed/parser do not exist.
- [ ] Implement parser, monotonic guards, socket loop, bounded reconnect delay, silence invalidation, and task/socket cleanup using the existing `websockets` dependency.
- [ ] Run the new tests, then the Arcus collector tests, and verify all pass.
- [ ] Commit `feat: add Arcus l2Orderbook websocket feed`.

### Task 3: Arcus collector integration and fallback

**Files:**
- Modify: `src/radar/collectors/arcus.py`
- Modify: `src/radar/pipeline.py`
- Test: `tests/test_arcus_collector.py`, `tests/test_pipeline.py`, `tests/test_market_data.py`

**Interfaces:**
- `ArcusCollector` keeps constructor compatibility, adds `source="rest"|"ws"`, and in WS mode starts the feed plus existing metadata/funding refresh.
- WS books update the shared cache as eligible; REST fallback updates it as ineligible only after the WS book is absent/stale.

- [ ] Write failing integration tests for WS mode startup, cache updates, REST fallback archival-only behavior, WS recovery restoring eligibility, and unchanged REST mode request behavior.
- [ ] Run the focused tests and observe failures.
- [ ] Implement collector lifecycle and callback wiring without changing low-frequency context or storage schemas.
- [ ] Run Arcus/pipeline/market-data focused suites and verify all pass.
- [ ] Commit `feat: integrate selectable Arcus websocket source`.

### Task 4: Canary harness, documentation, and verification

**Files:**
- Modify: `docs/operations/arcus-shadow-probe.md` or add a focused operations note if the existing document is absent on the base.
- Create: isolated no-write canary output outside the repository; do not commit market data.

**Interfaces:**
- The canary uses the formal WS collector/parser and existing REST probe semantics; it never receives a Production storage path or Telegram transport.

- [ ] Run focused tests for all modified components.
- [ ] Run `uv run pytest -m "not live"`, `uv run python -m compileall -q src tests`, and `git diff --check` with a writable task-local `UV_CACHE_DIR`.
- [ ] Run a bounded real-feed canary for selected Arcus symbols when permitted by the current market/session; record REST/WS counts, interval percentiles, freshness, sequence/order errors, reconnects, depth/VWAP availability, CPU/RSS, and output paths. Do not start a Production writer.
- [ ] Update the operations note with the selectable config and fail-closed fallback semantics, then rerun the full suite.
- [ ] Commit `docs: document Arcus websocket canary and source selection`.
- [ ] Push the feature branch and verify the remote SHA and clean worktree.

## Completion Evidence

- Exact base and final SHA, changed files, focused/full test counts, compileall/diff-check output.
- Canary host, start/end time, no-write path, REST/WS measurements, and any unknowns.
- Production remains at its pre-task SHA and Arcus REST mode; no restart or configuration mutation.
