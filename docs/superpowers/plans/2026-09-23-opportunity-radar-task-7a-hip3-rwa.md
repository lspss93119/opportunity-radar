# Task 7A — Hyperliquid HIP-3 / trade[XYZ] RWA Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extend the existing Hyperliquid collector to normalize native Hyperliquid and `trade_xyz` HIP-3 markets without changing downstream models or monitor behavior.

**Architecture:** Keep one `HyperliquidCollector` implementation with instance-level `venue` and optional `dex`. Native Hyperliquid keeps its current requests; HIP-3 metadata requests add `dex="xyz"`, while `l2Book` and `fundingHistory` use the full prefixed coin because live probing shows those requests already identify the market by coin. The pipeline adds a `trade_xyz` collector only when enabled markets require it.

**Tech Stack:** Python 3.13, asyncio, Pydantic v2, PyYAML, existing HTTP collector seam, pytest/pytest-asyncio, existing `@pytest.mark.live` tests.

**Spec:**
- `docs/superpowers/specs/2026-09-15-opportunity-radar-requirements.md`
- `docs/superpowers/specs/2026-09-15-opportunity-radar-architecture.md`
- User-provided Task 7A — Hyperliquid HIP-3 / trade[XYZ] RWA collector support request.

## Global Constraints

- Read-only public API calls only; no authentication, signing, orders, or execution.
- Preserve `MarketSnapshot`, `FundingSnapshot`, `HourlyContext`, RadarState, Parquet, SQLite, monitor, and alert schemas.
- Preserve aligned 10-second sampling, concurrent collectors, fail-closed missing-data behavior, and no stale snapshot reuse.
- Do not add a new collector codebase, new venue beyond `trade_xyz`, websocket support, automatic discovery, or Task 7B semantics.
- Keep exact venue identities: native `hyperliquid`; HIP-3 `trade_xyz`; HIP-3 venue symbols remain prefixed, e.g. `xyz:NVDA`.
- Require a configured non-zero-or-zero-explicit `trade_xyz` fee whenever an enabled `trade_xyz` market exists; the example configuration uses `trade_xyz: 9.0` bps.

## Live API Findings

- `POST https://api.hyperliquid.xyz/info` with `{"type":"metaAndAssetCtxs"}` returns native metadata.
- The same request with `{"type":"metaAndAssetCtxs","dex":"xyz"}` returns prefixed HIP-3 names such as `xyz:TSLA`.
- Current `l2Book` and `fundingHistory` requests succeed with the full prefixed coin and no extra `dex` field; the implementation will not invent that field.
- The eight requested exact-equity candidates were present in both current Lighter active-perp metadata and current Hyperliquid `xyz` metadata: `SNDK`, `NVDA`, `TSLA`, `HOOD`, `GOOGL`, `AAPL`, `META`, and `MU`.

## Review Focus

- Native metadata must remain exactly `{"type":"metaAndAssetCtxs"}`; the native regression test asserts no `dex` field.
- HIP-3 context/book/funding identities must preserve `xyz:<SYMBOL>` as `venue_symbol` and `<SYMBOL>` as `canonical_symbol`; fixture normalization tests cover all three datasets.
- A failed HIP-3 book or funding request must omit only the unavailable normalized item and keep other symbols/contexts fail-closed; partial-failure tests cover this.
- A HIP-3 metadata failure must report logical venue `trade_xyz` and return an empty batch; the error callback test covers this.
- An enabled `trade_xyz` market without a configured fee must be rejected rather than silently becoming zero-cost; the config validation test covers this.

### Task 1: Generalize the Hyperliquid collector for HIP-3

**Files:**
- Modify: `src/radar/collectors/hyperliquid.py`
- Test: `tests/test_hyperliquid_collector.py`
- Add fixtures: `tests/fixtures/hyperliquid/hip3_meta_and_asset_ctxs.json`, `tests/fixtures/hyperliquid/hip3_l2_xyz_tsla.json`, `tests/fixtures/hyperliquid/hip3_funding_xyz_tsla.json`

**Interfaces:**
- Add constructor parameters `venue: str = "hyperliquid"` and `dex: str | None = None`.
- Select configured markets with `markets_for_venue(markets, self.venue)`.
- Add `dex` only to the `metaAndAssetCtxs` request when it is not `None`; keep `l2Book` and `fundingHistory` request bodies keyed by the full `venue_symbol` coin.
- Emit `self.venue` in market, funding, and hourly normalized objects and in `report_collector_error()`.

- [ ] Write fixture-based RED tests for native metadata request stability, HIP-3 metadata `dex`, HIP-3 `trade_xyz` identities, all executable VWAP sizes, prefixed funding/hourly context, per-symbol book failure, and logical-venue metadata failure.
- [ ] Run the focused Hyperliquid tests and verify the new tests fail because the collector does not accept/configure `venue` and `dex`.
- [ ] Implement the smallest instance-identity/request-body change; preserve existing parsers and per-symbol fail-closed handling.
- [ ] Run `UV_CACHE_DIR=/private/tmp/opportunity-radar-task7a-uv-cache uv run pytest tests/test_hyperliquid_collector.py -q` and verify all focused tests pass.
- [ ] Commit `Generalize Hyperliquid collector for HIP-3`.

### Task 2: Wire `trade_xyz` through pipeline and fee validation

**Files:**
- Modify: `src/radar/pipeline.py`
- Modify: `src/radar/config.py`
- Test: `tests/test_pipeline.py`
- Test: `tests/test_config.py`

**Interfaces:**
- `MarketDataPipeline.from_config()` continues to build Lighter and native Hyperliquid, and appends `HyperliquidCollector(config.markets, venue="trade_xyz", dex="xyz", ...)` only when enabled `trade_xyz` markets exist.
- `RadarConfig` rejects any enabled `trade_xyz` market when `fees_bps` has no case-insensitive `trade_xyz` key.

- [ ] Write RED tests proving the pipeline builds Lighter/native/`trade_xyz` collectors with the correct logical venues/dex, and proving missing `trade_xyz` fees are rejected while an explicit fee is accepted.
- [ ] Run the focused pipeline/config tests and verify failure before implementation.
- [ ] Implement conditional collector construction and the narrow model-level fee guard without changing existing fee behavior for other venues.
- [ ] Run `UV_CACHE_DIR=/private/tmp/opportunity-radar-task7a-uv-cache uv run pytest tests/test_pipeline.py tests/test_config.py -q` and verify all pass.
- [ ] Commit `Wire trade_xyz HIP-3 into the market pipeline`.

### Task 3: Add the verified exact-equity RWA universe and live coverage

**Files:**
- Modify: `config/radar.example.yaml`
- Modify: `tests/test_config.py`
- Modify: `tests/test_live_collectors.py`

**Interfaces:**
- Keep existing Lighter/Hyperliquid BTC/ETH/SOL entries unchanged.
- Add Lighter symbols `SNDK`, `NVDA`, `TSLA`, `HOOD`, `GOOGL`, `AAPL`, `META`, `MU` with matching canonical symbols.
- Add `trade_xyz` symbols `xyz:SNDK`, `xyz:NVDA`, `xyz:TSLA`, `xyz:HOOD`, `xyz:GOOGL`, `xyz:AAPL`, `xyz:META`, `xyz:MU` with unprefixed canonical symbols.
- Add `trade_xyz: 9.0` to `fees_bps`.

- [ ] Extend the config fixture assertion to pin the eight verified exact-equity mappings and conservative fee.
- [ ] Add a `pytest.mark.live` HIP-3 smoke covering at least `xyz:TSLA`, its funding/hourly context, executable VWAPs, and matching Lighter `TSLA` identity.
- [ ] Run the targeted config/live tests; do not make ordinary tests depend on network access.
- [ ] Commit `Add verified HIP-3 equity universe`.

### Task 4: Full verification and bounded live validation

**Files:**
- No planned production changes; inspect the final diff and live outputs.

- [ ] Run `UV_CACHE_DIR=/private/tmp/opportunity-radar-task7a-uv-cache uv run pytest -m "not live"`.
- [ ] Run `UV_CACHE_DIR=/private/tmp/opportunity-radar-task7a-uv-cache uv run ruff check .`.
- [ ] Run `UV_CACHE_DIR=/private/tmp/opportunity-radar-task7a-uv-cache uv run mypy src`.
- [ ] Run `UV_CACHE_DIR=/private/tmp/opportunity-radar-task7a-uv-cache uv run python -m compileall src`.
- [ ] Run `git diff --check` and verify `git status` contains only intentional committed state.
- [ ] Run the existing native/Lighter live smoke plus the new HIP-3 live smoke; record exact current symbols, VWAP availability, timestamps, funding/context results, and any skipped candidate.
- [ ] If a live API behavior fails, check official current API documentation/source before making any code change; record the observed status, source, root cause, minimal fix, and retest.
- [ ] Do not create an empty checkpoint commit; stop after Task 7A.
