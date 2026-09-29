# Opportunity Radar Read-Only Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing read-only dashboard into a persistent scanner,
pair-detail, and health UI without changing Radar collection, monitoring,
storage, or alert behavior.

**Architecture:** Run `python -m radar.dashboard` as a separate process. A
small `DashboardQueryService` reads YAML, Parquet, and SQLite in read-only
mode, computes only display/query results using exact production spread
semantics, and exposes bounded JSON endpoints through the existing
`ThreadingHTTPServer`. Vanilla HTML/CSS/JavaScript renders the three views.

**Tech Stack:** Python 3.13, existing DuckDB/PyArrow/SQLite/PyYAML runtime,
`http.server`, vanilla HTML/CSS/JavaScript, pytest/pytest-asyncio. No new
runtime dependency.

**Spec:** `docs/superpowers/specs/2026-09-29-opportunity-radar-dashboard-design.md`

## Global Constraints

- Dashboard is a separate read-only `python -m radar.dashboard` process.
- Parquet is the market/history source of truth; SQLite is the production episode, alert-lifecycle, and persisted signal-duration source of truth; YAML is the universe, fee, and trigger-configuration source of truth.
- Never call `SpreadMonitor.evaluate()` and never create a second strategy engine or shadow lifecycle.
- Preserve exact pair identity: `canonical_symbol`, long venue, long `venue_symbol`, short venue, short `venue_symbol`.
- Use only `buy_10k_vwap` for the long side and `sell_10k_vwap` for the short side.
- Raw spread is `(short_sell_vwap / long_buy_vwap - 1) * 10,000`.
- Match long and short observations on exact `sample_time`; use `observed_at` only for freshness and skew.
- Missing VWAP, missing fee, stale/future data, mismatched sample time, malformed identity, and unavailable basis coverage fail closed.
- Prior-only 24-hour arithmetic mean and population standard deviation exclude the current observation and require the existing full-window/80% coverage semantics.
- Do not interpolate, carry stale prices, synthesize prices, or perform look-ahead.
- Use a simple bounded process-local TTL cache of approximately 5–10 seconds; do not add Redis, a persistent cache, or a cache framework.
- Extend the existing `ThreadingHTTPServer`; use vanilla HTML/CSS/JavaScript; do not add React, Next.js, FastAPI, or a CDN dependency.
- Preserve `/api/status` behavior and safe local defaults; production binds only to the trading-mini Tailscale address on port 8787.
- Do not modify collectors, `RadarState`, `SpreadMonitor`, Parquet schemas, SQLite schemas, Telegram behavior, sampling cadence, fees, thresholds, or Radar launch behavior.
- The production benchmark is a temporary, separate read-only dashboard process on `trading-mini`; it may fast-forward the already-authorized `feature/radar-v1` branch but must never restart or replace Radar. Do not rsync an uncommitted source tree.
- The persistent LaunchDaemon must run as the actual normal `trading-mini` user with an explicit `UserName`, `WorkingDirectory`, executable, repository, config, data, runtime DB, stdout, and stderr paths. Deployment-specific values are discovered at install time and are not hardcoded into repository defaults; `~` and implicit `HOME` expansion are not valid plist paths.
- Signal duration and lifecycle are display-only values sourced from persisted SQLite episode state. Never reconstruct duration by replaying Parquet, and never convert a Parquet-only spread condition into an inferred active episode; when no matching persisted episode exists, show lifecycle/duration as inactive or unavailable rather than fabricating a zero-duration signal.
- Do not add delayed-entry PnL, MAE/MFE, historical backfill, Google Drive sync, collector-error persistence, Telegram health persistence, authentication, order entry, or automated trading.

## Review Focus

- Duplicate rows for one feed and `sample_time` must select the greatest `observed_at`; pin this in Task 1 query tests.
- Current observations must be excluded from prior-only basis statistics, with population standard deviation and 80%/full-window coverage; pin this in Task 1 basis tests.
- Missing/stale/future/mismatched/VWAP-null/fee-missing rows must never form an opportunity; pin this in Task 1 pair tests.
- Ambiguous venue-symbol mappings must return an identity error instead of guessing; pin this in Tasks 1–3 API tests.
- Missing or malformed SQLite/Parquet must produce explicit degraded output without creating or mutating runtime files; pin this in Task 1 and Task 2 status tests.

---

## Phase 1: Read-only query/data service

**Files:**

- Create: `src/radar/dashboard_data.py`
- Modify: `src/radar/dashboard.py` to retain `DashboardStatusService` as a compatibility facade over the new query service
- Create: `tests/test_dashboard_data.py`
- Modify: `tests/test_dashboard.py` for the facade and existing status regressions

**Interfaces:**

- Consumes: `RadarConfig`, enabled `MarketConfig` mappings, existing Parquet date partitions, SQLite `monitor_state`/`opportunity_log`, `SpreadPairKey`, `RollingBasis`, and pure raw-spread helpers.
- Produces: JSON-compatible results for `get_status()`, `get_opportunities()`, and `get_pair()`; no writes and no monitor lifecycle mutations.

Define these small query-layer types in `dashboard_data.py`:

```python
PairRange = Literal["1h", "6h", "24h", "3d", "7d", "all"]

@dataclass(frozen=True)
class OpportunitiesFilters:
    symbol: str | None = None
    long_venue: str | None = None
    short_venue: str | None = None
    max_std_bps: float | None = None
    min_deviation_bps: float | None = None
    min_duration_seconds: int | None = None
    active_only: bool = False
    limit: int = 200

class DashboardQueryService:
    def __init__(
        self,
        config: RadarConfig,
        *,
        data_root: Path = Path("data"),
        runtime_db: Path = Path("runtime/radar.sqlite3"),
        clock: Callable[[], datetime] = utc_now,
        cache_ttl_seconds: float = 5.0,
        max_cache_entries: int = 64,
    ) -> None: ...

    def get_status(self, *, now: datetime | None = None) -> dict[str, object]: ...
    def get_opportunities(
        self,
        filters: OpportunitiesFilters,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]: ...
    def get_pair(
        self,
        *,
        canonical_symbol: str,
        long_venue: str,
        long_venue_symbol: str,
        short_venue: str,
        short_venue_symbol: str,
        range_name: PairRange,
        now: datetime | None = None,
    ) -> dict[str, object]: ...
```

### Task 1.1: Write failing query-layer tests

- [ ] Add fixture helpers that write normalized `MarketSnapshot` rows through `ParquetStorage` and episode JSON through `SQLiteRuntimeStore` under `tmp_path`.
- [ ] Add `test_latest_feed_row_uses_newest_observed_at_for_duplicate_sample_time`.
- [ ] Add `test_opportunity_requires_exact_sample_time_and_distinct_venues`.
- [ ] Add `test_missing_vwap_stale_future_and_missing_fee_fail_closed`.
- [ ] Add `test_prior_only_basis_excludes_current_observation_and_uses_population_std` with generated aligned samples at the production minimum so the expected values are deterministic.
- [ ] Add `test_basis_requires_full_window_and_eighty_percent_coverage`.
- [ ] Add `test_data_as_of_is_latest_dataset_sample_time`.
- [ ] Add `test_persisted_episode_controls_signal_duration_and_alert_state_without_monitor_evaluation`.
- [ ] Add `test_sqlite_missing_or_unreadable_database_does_not_create_file`.
- [ ] Add `test_empty_parquet_and_temporary_partition_are_degraded_without_being_read_as_data`.
- [ ] Add `test_cache_key_separates_filters_and_expiry_recomputes_result`.
- [ ] Run `uv run pytest tests/test_dashboard_data.py -q` and confirm the new service/tests fail for the intended missing interfaces.

### Task 1.2: Implement bounded market and runtime reads

- [ ] Implement path selection for only the date partitions needed by a 24-hour scanner query; pair ranges use the selected range plus up to 24 hours of prior data for the rolling mean.
- [ ] Use DuckDB CTEs to rank rows by `(venue, venue_symbol, canonical_symbol, sample_time)` and retain the row with greatest `observed_at`.
- [ ] Reject `sample_time > now` and use `observed_at` against `config.monitors.spread.stale_after_seconds` for current freshness.
- [ ] Keep internal rows small: identity, sample/observed timestamps, primary VWAP, and any values needed for current skew.
- [ ] Read SQLite using `Path.resolve().as_uri() + "?mode=ro"`, a short timeout, and no initialization path. Parse only the persisted episode/event JSON needed for display.
- [ ] Treat malformed episode entries as degraded/unknown items; do not instantiate `SpreadMonitor` or invoke `evaluate()`.

### Task 1.3: Implement exact pair and basis calculations

- [ ] Group valid market rows by `(canonical_symbol, sample_time)` and build every ordered cross-venue pair with non-null primary VWAPs and explicit non-negative fees.
- [ ] Reuse `SpreadPairKey` and the pure raw-spread helper; do not import or mutate a monitor instance.
- [ ] For each current pair, select the latest exact matched sample and calculate raw spread, observed skew, freshness, fees, and display-only round-trip fee/theoretical edge.
- [ ] Feed prior pair values into the existing pure `RollingBasis` semantics or an equivalent history helper, using `stats_before(current_sample_time)` so the current observation is excluded.
- [ ] Require the existing `MIN_HISTORY_OBSERVATIONS`, 24-hour window, and 80% coverage semantics before returning mean/std/deviation as eligible. Return `None`/degraded metadata when basis is unavailable.
- [ ] Join the pair key to persisted SQLite episode state. Use `alert_condition_since`, `candidate_confirmed`, `alerted`, and `last_seen_at` only as persisted; if no matching persisted episode exists, return lifecycle and signal duration as inactive/unavailable. Never infer an episode or duration from Parquet observations.

### Task 1.4: Implement the small TTL cache and status aggregation

- [ ] Add a thread-safe cache keyed by operation plus normalized filters/pair/range, with a default 5-second TTL and a maximum of 64 entries. Release the lock before DuckDB/SQLite I/O.
- [ ] Implement `get_status()` using the existing heartbeat/feed health thresholds and persisted evidence. Show per-venue expected/latest/missing feeds, latest sample age, latest Parquet file mtime when available, SQLite readability, active episodes, recent events, and explicit errors.
- [ ] Mark unsupported deployed SHA, Telegram health, and collector-error counters as unavailable; never emit guessed zero values.
- [ ] Implement `get_opportunities()` with `deviation_bps` descending ordering and the bounded limit.
- [ ] Implement `get_pair()` with exact identity, whitelisted range, current summary, ordered history points, current point, and optional rolling-mean series. Downsample history to a fixed display maximum while preserving first/latest/local extrema; statistics remain based on full queried data.

### Task 1.5: Preserve the existing status facade and verify

- [ ] Make `DashboardStatusService` delegate to `DashboardQueryService.get_status()` while preserving its constructor and `get_status()` behavior used by existing tests.
- [ ] Run `uv run pytest tests/test_dashboard_data.py tests/test_dashboard.py -q`.
- [ ] Run `uv run ruff check src/radar/dashboard_data.py src/radar/dashboard.py tests/test_dashboard_data.py tests/test_dashboard.py` if Ruff is available/configured.
- [ ] Commit the complete phase as `Add read-only dashboard query service`.

Acceptance: temporary fixture data produces exact, finite, fail-closed scanner/pair/status payloads; the query layer never creates/writes SQLite or Parquet; existing dashboard status tests remain green.

## Phase 2: Opportunities scanner and Status API/UI

**Files:**

- Modify: `src/radar/dashboard.py`
- Modify: `tests/test_dashboard.py`

**Interfaces:**

- Consumes: `DashboardQueryService`, `OpportunitiesFilters`, and status payloads from Phase 1.
- Produces: `GET /api/opportunities`, extended `GET /api/status`, `/` and `/opportunities` scanner UI, `/status` UI, and URL-preserved filters.

### Task 2.1: Write failing API and scanner tests

- [ ] Add HTTP tests for `/api/opportunities` default ordering and required fields.
- [ ] Add tests for every filter: symbol, long venue, short venue, max std, minimum deviation, minimum duration, and `active_only`.
- [ ] Add tests for malformed numbers, negative values, invalid booleans, excessive/non-positive limits, and unknown filters; expect HTTP 400.
- [ ] Add tests proving response JSON is finite and includes `data_as_of`.
- [ ] Add tests for `/api/status` healthy, degraded, stale, missing-feed, and existing regression behavior.
- [ ] Pin the existing evidence-based status boundaries: heartbeat healthy at age `<=30s`, degraded above `30s` through `60s`, down above `60s`/missing; feed healthy at age `<=90s` with primary VWAP, degraded above `90s` through `180s` or missing primary VWAP, down above `180s`/missing. Overall status is down for a down heartbeat, otherwise degraded for degraded heartbeat/errors/non-healthy feeds, otherwise healthy.
- [ ] Add tests that scanner links contain the complete exact pair identity and do not guess venue symbols.
- [ ] Add tests that `/`, `/opportunities`, and `/status` return HTML containing the expected navigation/table/status anchors.
- [ ] Run the focused dashboard tests and observe failures before route/UI implementation.

### Task 2.2: Implement API request parsing and routes

- [ ] Add strict query parsing that normalizes symbols/venues, validates finite numeric bounds, validates `active_only`, caps `limit` at a small fixed maximum (for example 200), and rejects unknown/ambiguous identity input.
- [ ] Add `/api/opportunities` to call `DashboardQueryService.get_opportunities()` and return `application/json; charset=utf-8`, `Cache-Control: no-store`, and JSON-compatible errors.
- [ ] Preserve `/api/status` response compatibility while delegating to `get_status()`.
- [ ] Return HTTP 400 for malformed filters, HTTP 404 for unknown page paths/pair identities as appropriate, and a degraded JSON payload for storage-read failures rather than fabricating data.

### Task 2.3: Implement scanner and status UI

- [ ] Replace the single health-only page with a compact vanilla HTML/JS shell while retaining the existing dark, responsive style.
- [ ] Make Opportunities the default view with filters and columns: Symbol, Long, Short, Spread, 24h Mean, 24h Std, Deviation, Duration, RT Fee, Theo Edge, Skew, Freshness.
- [ ] Keep default sorting server-side by deviation descending; do not add a composite score.
- [ ] Persist scanner filters in the query string and refresh data without full-page navigation.
- [ ] Render exact pair links containing canonical symbol, venues, and both venue symbols.
- [ ] Add the Status view with Radar heartbeat, latest sample/age, configured/latest counts, per-venue expected/latest/missing/freshness, Parquet evidence, SQLite status, active episodes, recent events, and unavailable fields clearly labeled.
- [ ] Ensure transient partial cycles are shown as missing/degraded feed data rather than automatically rendering the whole Radar down.

### Task 2.4: Verify and commit

- [ ] Run `uv run pytest tests/test_dashboard.py tests/test_dashboard_data.py -q`.
- [ ] Run `uv run python -m compileall src/radar/dashboard.py src/radar/dashboard_data.py`.
- [ ] Review the diff for no writes, no monitor evaluation, no schema/config changes, and no external frontend dependency.
- [ ] Commit as `Add opportunities and status dashboard`.

Acceptance: the default browser page is a scanner table with working filters and exact pair links; `/api/status` retains existing behavior; no endpoint writes production files.

## Phase 3: Pair Detail and history chart

**Files:**

- Modify: `src/radar/dashboard.py`
- Modify: `src/radar/dashboard_data.py` only if the Phase 1 pair payload needs a narrowly scoped field adjustment
- Modify: `tests/test_dashboard.py`
- Modify: `tests/test_dashboard_data.py`

**Interfaces:**

- Consumes: exact pair identity from scanner links and `DashboardQueryService.get_pair()` from Phase 1.
- Produces: `GET /api/pair`, `/pair?...` UI, bounded historical raw-spread series, and readable current summary/chart.

### Task 3.1: Write failing pair API/chart tests

- [ ] Add tests for valid ranges `1h`, `6h`, `24h`, `3d`, `7d`, and `all`.
- [ ] Add tests rejecting non-whitelisted ranges and missing/ambiguous/unknown exact pair identity.
- [ ] Add tests for current summary fields: raw spread, prior-only mean/std/deviation, persisted signal duration, both `$10k` VWAPs, RT fee, theoretical edge, skew, freshness, and sample time.
- [ ] Add tests proving historical raw spread uses exact sample-time matching and the same raw formula.
- [ ] Add tests proving the current observation is excluded from the rolling mean and that unavailable 24-hour coverage remains unavailable.
- [ ] Add tests that long ranges return bounded display points and preserve first, latest, and significant extrema; no interpolation is introduced.
- [ ] Add tests that the response includes `data_as_of` from dataset `sample_time`.
- [ ] Run the focused tests and confirm intended failures.

### Task 3.2: Implement pair route and detail UI

- [ ] Parse `/api/pair` identity parameters as required exact values: `symbol`, `long_venue`, `long_venue_symbol`, `short_venue`, `short_venue_symbol`, and whitelisted `range`.
- [ ] Return pair JSON with exact identity, current summary, historical points, rolling 24-hour mean where available, and explicit degraded/unavailable fields.
- [ ] Add `/pair` route using the existing HTML shell and query-string state.
- [ ] Render the required summary fields and `Data as of` clearly.
- [ ] Render a self-contained SVG/canvas line chart with raw executable spread, rolling 24-hour mean, and current point. Omit std bands when readability would suffer.
- [ ] Provide range controls for `1h`, `6h`, `24h`, `3d`, `7d`, and `all`; range changes must not alter the statistics source.
- [ ] Do not add Matplotlib, a charting CDN, or a new backend service for the browser chart.

### Task 3.3: Verify and commit

- [ ] Run `uv run pytest tests/test_dashboard.py tests/test_dashboard_data.py -q`.
- [ ] Run `uv run python -m compileall src/radar/dashboard.py src/radar/dashboard_data.py`.
- [ ] Review that Pair Detail never calls `SpreadMonitor.evaluate()` and does not perform stale carry/interpolation/look-ahead.
- [ ] Commit as `Add dashboard pair detail history`.

Acceptance: a scanner row opens the exact pair, the required current fields are shown, chart requests stay bounded, and historical spread semantics match production.

## Phase 4: CLI, deployment documentation, and production performance validation

**Files:**

- Modify: `src/radar/dashboard.py` for `--host`, `--port`, `--config`, `--data-root`, and `--runtime-db` with safe local defaults
- Create: `docs/operations/opportunity-radar-dashboard.md`
- Create: `docs/operations/com.opportunity-radar.dashboard.plist.example`
- Modify: `README.md` with a short dashboard run/link and safety note
- Create or modify: `tests/test_dashboard.py` for CLI defaults and server host/port injection

**Interfaces:**

- Consumes: completed dashboard server and query layer.
- Produces: explicit-path `python -m radar.dashboard` invocation and a
  Tailscale-only macOS LaunchDaemon procedure. It does not alter the Radar
  launch process.

### Task 4.1: Write failing CLI/deployment tests

- [ ] Add parser tests for safe defaults: host `127.0.0.1`, port `8787`, existing relative data/runtime paths, and explicit overrides.
- [ ] Add a missing-config test proving startup fails before creating any runtime database or data files.
- [ ] Add a port-0 HTTP smoke test proving host injection works without opening a public listener.
- [ ] Add documentation checks for `LaunchDaemon`, `KeepAlive`, `RunAtLoad`, absolute paths, Tailscale-only binding, separate logs, and Radar isolation.

### Task 4.2: Implement CLI and deployment documentation

- [ ] Add the bounded CLI arguments and pass them to `DashboardStatusService`/`DashboardQueryService` and `create_dashboard_server()`.
- [ ] Preserve local loopback defaults; do not embed a machine-specific Tailscale IP in source control.
- [ ] Document how to determine the trading-mini Tailscale IPv4 at install time, how to install/uninstall the LaunchDaemon, inspect status, tail logs, and run a manual smoke request.
- [ ] Use explicit absolute Python/config/data/runtime/log paths in the plist example, with placeholders for deployment-specific values.
- [ ] Set `UserName` to the actual normal `trading-mini` user and document how to determine it at deployment time; do not rely on the default root user or implicit `HOME` expansion.
- [ ] State that the dashboard process is read-only and that Radar remains a separate process.

### Task 4.3: Run required production benchmark before acceptance

- [ ] On `trading-mini`, run the dashboard against production data without changing Radar, config, SQLite, or Parquet.
- [ ] Measure at least 30 cold `/api/opportunities` requests and at least 30 cached requests; record p50/p95/max latency.
- [ ] Measure `/api/pair` for `24h`, `7d`, and `all`; record p50/p95/max latency.
- [ ] Record dashboard RSS and CPU while idle, refreshing the scanner, and requesting pair charts.
- [ ] Compare Radar scheduler critical-path timing, missed/duplicate slots, cycle failures, and Parquet failures before and during dashboard load.
- [ ] Accept only with zero additional missed/duplicate slots and no material scheduler regression. If performance fails, report evidence and stop; do not add a larger cache or optimization framework in the same task.

### Task 4.4: Full verification and checkpoint

- [ ] Run `uv run pytest -m "not live"`.
- [ ] Run `uv run ruff check .` only if Ruff is already configured and available.
- [ ] Run `uv run mypy src` only if Mypy is already configured and available; do not add either dependency for this feature.
- [ ] Run `uv run python -m compileall src`.
- [ ] Run `git diff --check`.
- [ ] Review the complete diff for read-only behavior, exact identity, source-of-truth ownership, no monitor evaluation, no schema changes, and no Radar lifecycle changes.
- [ ] Commit documentation/CLI changes as `Document persistent read-only dashboard deployment`.

Acceptance: the dashboard has explicit-path persistent deployment documentation, production performance evidence, no material Radar impact, and all configured quality gates pass.

## Commit strategy

Use one meaningful commit per completed phase:

1. `Add read-only dashboard query service`
2. `Add opportunities and status dashboard`
3. `Add dashboard pair detail history`
4. `Document persistent read-only dashboard deployment`

Do not create empty checkpoint commits. The approved workflow pushes the plan
commit first, then pushes each completed Phase 1–3 commit before the
`trading-mini` benchmark; after successful benchmark and Phase 4 validation,
push the final Phase 4 commit. Do not merge or open a PR.

## Final handoff report

After implementation, report:

- starting/ending SHA and commits
- files changed by phase
- API contracts and exact identity behavior
- read-only source-of-truth behavior
- cold/cached opportunity latency
- pair-detail latency by range
- dashboard CPU/RSS
- Radar sampler impact and missed/duplicate slots
- tests and quality-gate results
- deployment paths/logs and LaunchDaemon status
- deferred items and known limitations
