# Manual Opportunity v1 Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing Manual Opportunity v1 lifecycle into live Radar, restore bounded three-day BBO history at startup, route text-only manual alerts, and disable only Anomaly v2 Telegram emission in production.

**Architecture:** `ManualOpportunityMonitor` will consume only the latest `RadarState` market/context view and keep one `BboRollingHistory` per exact directional route. A streaming history helper will hydrate those histories before the scheduler starts; `AlertRouter` will dispatch spread and manual requests to their existing slow-path processors. Anomaly v2 will keep its lifecycle and persistence but conditionally return no Telegram requests.

**Tech Stack:** Python 3.13, asyncio, DuckDB/PyArrow Parquet reads, Pydantic v2, SQLite runtime state, pytest/pytest-asyncio, existing Telegram transport.

**Spec:** `docs/superpowers/specs/2026-10-06-opportunity-radar-manual-opportunity-v1-phase-2-design.md`

## Global Constraints

- Read-only market monitoring; no orders, signing, private APIs, execution, or position management.
- Manual Opportunity uses best ask/bid only; never VWAP, funding, std, trajectory, ENME, or return/resolved notifications.
- Preserve `available_at = max(sample_time, long_observed_at, short_observed_at)` and 30-second BBO freshness.
- Preserve 60-second persistence, 5 bps baseline range, 10 bps expected-net minimum, $1m route-volume minimum, and 15/20/25/30... expansion levels.
- Monitor evaluation must remain network-/Parquet-/DuckDB-/Telegram-free.
- Use exact `(canonical_symbol, venue, venue_symbol)` identity and fail closed on missing/stale/invalid data.
- Do not add dependencies, frameworks, queues, plugin systems, or automated trading.
- Production example keeps Manual Opportunity disabled; production changes only enablement and Anomaly v2 Telegram suppression.

## Review Focus

- A late or stale BBO must not enter the live rolling basis or extend an episode: Task 3 tests temporal rejection before history mutation.
- A current live sample must not enter its own prior-only mean: Tasks 2 and 3 test `observe()` ordering and strict hydration bounds.
- A restored episode must not expand from stale state: Task 3 tests current freshness and continuity after SQLite restore.
- Anomaly v2 lifecycle events must persist while Telegram requests are suppressed: Task 1 tests state/event persistence and empty returned alerts.
- Routing must never send a manual alert through the chart/history path: Task 4 tests text-only manual processing and unchanged spread routing.

## File Map

- Modify `src/radar/config.py`: add backward-compatible `AnomalyV2Config.telegram_enabled`.
- Modify `config/radar.example.yaml`: keep Manual Opportunity disabled and document the Anomaly v2 Telegram switch.
- Modify `src/radar/history/manual_opportunity.py`: add bounded, streaming, strictly-prior BBO hydration from Parquet.
- Modify `src/radar/monitors/manual_opportunity.py`: add the live monitor around existing lifecycle/history primitives.
- Modify `src/radar/monitors/registry.py`: construct spread and manual monitors independently.
- Modify `src/radar/monitors/spread/monitor.py`: suppress only returned Anomaly v2 alert requests when configured, after persistence.
- Modify `src/radar/alerts/manual_opportunity.py`: add text-only processor and compact initial/expansion formatting if needed.
- Modify `src/radar/alerts/worker.py`: add a small monitor-name router while preserving worker error handling.
- Modify `src/radar/alerts/__init__.py`: export new alert processor/router symbols if the existing package convention requires it.
- Modify `src/radar/app.py`: assemble both monitors/processors, hydrate manual history at startup, and log hydration/RSS metrics.
- Modify tests adjacent to each component plus `tests/test_app.py` for integration wiring.

---

### Task 1: Configuration and Anomaly Telegram Gate

**Files:**
- Modify: `src/radar/config.py: AnomalyV2Config`
- Modify: `src/radar/monitors/spread/monitor.py: _evaluate_anomaly_v2`
- Modify: `config/radar.example.yaml: monitors.spread.anomaly_v2`
- Test: `tests/test_anomaly_v2_config.py`
- Test: `tests/test_anomaly_monitor_v2.py`

**Interfaces:**
- Produce `AnomalyV2Config.telegram_enabled: bool = True`.
- Preserve `SpreadMonitor._evaluate_anomaly_v2()` persistence and transaction behavior; only the returned alert list is gated.

- [ ] **Step 1: Write failing tests**

  Add tests proving the default is `True`, YAML/model validation accepts `false`, and an enabled Anomaly v2 monitor with `telegram_enabled=False` persists lifecycle state/events but returns no `AlertRequest`.

- [ ] **Step 2: Run the focused tests to verify RED**

  Run: `uv run pytest tests/test_anomaly_v2_config.py tests/test_anomaly_monitor_v2.py -q`

  Expected: failure because the config field and return suppression do not exist.

- [ ] **Step 3: Implement the minimum gate**

  Add the boolean with default `True`. In `_evaluate_anomaly_v2`, call the existing persistence path first and return `[]` only when the switch is false; do not change `AnomalyV2Lifecycle.evaluate()` or event payloads.

- [ ] **Step 4: Run focused tests to verify GREEN**

  Run the same command; expected: all focused tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/radar/config.py src/radar/monitors/spread/monitor.py config/radar.example.yaml tests/test_anomaly_v2_config.py tests/test_anomaly_monitor_v2.py
  git commit -m "Add Anomaly v2 Telegram gate"
  ```

### Task 2: Streaming Three-Day BBO Hydration

**Files:**
- Modify: `src/radar/history/manual_opportunity.py`
- Test: `tests/test_manual_opportunity_history.py`
- Test: `tests/test_manual_opportunity_replay.py` when shared fixture helpers are reused

**Interfaces:**
- Add:
  ```python
  def load_recent_bbo_history(
      data_root: Path,
      *,
      allowed_feeds: Collection[tuple[str, str, str]],
      as_of: datetime,
      window_seconds: int = 3 * 24 * 60 * 60,
  ) -> dict[SpreadPairKey, tuple[tuple[datetime, float], ...]]: ...
  ```
- Output contains exact route keys and route-local points sorted by `sample_time`; it excludes `sample_time >= as_of` and rows whose `observed_at > as_of`.

- [ ] **Step 1: Write failing fixture tests**

  Build synthetic market Parquet with duplicate feed/sample rows and assert:
  - only enabled exact feeds are included;
  - latest `observed_at` wins per feed/sample;
  - the `as_of` sample and future-observed rows are excluded;
  - directional BBO spread uses long `best_ask` and short `best_bid`;
  - output points are strictly prior, sorted, and route-isolated;
  - the loader reads only required columns and does not mutate source files.

- [ ] **Step 2: Run the history tests to verify RED**

  Run: `uv run pytest tests/test_manual_opportunity_history.py -q`

  Expected: failure because the loader is not defined.

- [ ] **Step 3: Implement the streaming loader**

  Use the existing DuckDB/Arrow batch-reading style. Order or batch the selected columns by sample/route, retain only the current sample-slot grouping and route-local three-day points, and reuse the replay's exact feed de-duplication and BBO spread formula. Do not load unrelated datasets or persist anything.

- [ ] **Step 4: Run focused history tests to verify GREEN**

  Run: `uv run pytest tests/test_manual_opportunity_history.py tests/test_manual_opportunity_replay.py -q`

  Expected: all pass, with the pre-existing replay tests unchanged.

- [ ] **Step 5: Commit**

  ```bash
  git add src/radar/history/manual_opportunity.py tests/test_manual_opportunity_history.py tests/test_manual_opportunity_replay.py
  git commit -m "Add streaming Manual Opportunity BBO hydration"
  ```

### Task 3: Live Manual Opportunity Monitor and Restart State

**Files:**
- Modify: `src/radar/monitors/manual_opportunity.py`
- Test: `tests/test_manual_opportunity.py`
- Create: `tests/test_manual_opportunity_monitor.py`

**Interfaces:**
- Add `ManualOpportunityMonitor(config: ManualOpportunityConfig, fees_bps: Mapping[str, float], *, runtime_store: SQLiteRuntimeStore | None = None, interval_seconds: int = 10, stale_after_seconds: int = 30)`.
- Add `hydrate_history(points_by_key: Mapping[SpreadPairKey, Sequence[tuple[datetime, float]]]) -> None`.
- Implement `async evaluate(now: datetime, state: RadarState) -> list[AlertRequest]`.

- [ ] **Step 1: Write failing monitor tests**

  Add deterministic tests proving:
  - all ordered exact cross-venue pairs are evaluated and same-venue pairs are excluded;
  - the current sample is not included in its own mean;
  - latest exact-key hourly volume is used;
  - missing, stale, future, invalid, insufficient-depth, fee-missing, and context-missing inputs fail closed;
  - a qualifying route produces `manual_initial` after exactly 60 seconds;
  - a frozen basis/fee produces the expected expansion level;
  - restored candidate/confirmed/frozen/watermark state behaves correctly;
  - stale restored state cannot produce an expansion and continuity gaps resolve it;
  - state persistence failure retains the Phase 1 rollback semantics.

- [ ] **Step 2: Run monitor tests to verify RED**

  Run: `uv run pytest tests/test_manual_opportunity.py tests/test_manual_opportunity_monitor.py -q`

  Expected: failure because `ManualOpportunityMonitor` is not defined.

- [ ] **Step 3: Implement the monitor**

  Group `state.markets` by canonical symbol, build exact directional route observations from BBO only, run temporal freshness checks before mutating route history, call `BboRollingHistory.observe()` for prior-only stats, resolve absent routes with `observe_gap()`, and delegate all eligibility/lifecycle persistence to `ManualOpportunityLifecycle`. Use exact context keys `(venue, venue_symbol, canonical_symbol)` and actual `now` for freshness.

- [ ] **Step 4: Run focused monitor tests to verify GREEN**

  Run the same command; expected: all monitor tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/radar/monitors/manual_opportunity.py tests/test_manual_opportunity.py tests/test_manual_opportunity_monitor.py
  git commit -m "Add live Manual Opportunity monitor"
  ```

### Task 4: Registry and Application Startup Hydration

**Files:**
- Modify: `src/radar/monitors/registry.py`
- Modify: `src/radar/app.py`
- Modify: `tests/test_monitors.py`
- Modify: `tests/test_app.py`

**Interfaces:**
- `build_enabled_monitors()` returns spread and/or manual monitors in stable order, independently controlled by their config flags.
- Application startup invokes `load_recent_bbo_history()` only when Manual Opportunity is enabled, hydrates the manual monitor before `pipeline.start()`, and logs `routes`, `observations`, `duration_ms`, `rss_before`, and `rss_after`.

- [ ] **Step 1: Write failing registry/app tests**

  Add tests for spread-only, manual-only, both, and both-disabled configurations. Add an application test proving startup hydrates manual history before the first collection cycle and logs/records bounded hydration metrics without network I/O. Add a test that the existing spread history hydration remains unchanged.

- [ ] **Step 2: Run wiring tests to verify RED**

  Run: `uv run pytest tests/test_monitors.py tests/test_app.py -q`

  Expected: failures for manual registry construction and startup hydration.

- [ ] **Step 3: Implement registry/app wiring**

  Add a manual factory using `config.sampling_seconds`, preserve monitor-name validation, and assemble manual history separately from VWAP `SpreadHistory`. Use standard-library RSS measurement and fail closed at startup if enabled manual hydration cannot be read; do not perform hydration in the 10-second loop.

- [ ] **Step 4: Run wiring tests to verify GREEN**

  Run the same command; expected: all app/registry tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/radar/monitors/registry.py src/radar/app.py tests/test_monitors.py tests/test_app.py
  git commit -m "Wire Manual Opportunity into Radar startup"
  ```

### Task 5: Alert Routing and Manual Text Delivery

**Files:**
- Modify: `src/radar/alerts/manual_opportunity.py`
- Modify: `src/radar/alerts/worker.py`
- Modify: `src/radar/alerts/__init__.py`
- Modify: `src/radar/app.py`
- Modify: `tests/test_manual_opportunity_alerts.py`
- Modify: `tests/test_alert_worker.py`
- Modify: `tests/test_alert_processor.py` if shared processor fixtures require it

**Interfaces:**
- Add `ManualOpportunityAlertProcessor(telegram: TelegramTransport)` with `async process(alert: AlertRequest) -> None` that sends exactly one text message.
- Add a small `AlertRouter` with `async process(alert: AlertRequest) -> None`, routing `spread` to `SpreadAlertProcessor` and `manual_opportunity` to `ManualOpportunityAlertProcessor`.
- Keep `AlertWorker`'s queue/error-handler behavior unchanged apart from accepting the common `.process()` interface.

- [ ] **Step 1: Write failing alert tests**

  Assert manual initial and expansion formatting contains the approved BBO/basis/fee/volume context and excludes VWAP/funding/std/trajectory/ENME/return fields. Assert the manual processor calls `send_text` and never `send_chart`; assert router dispatches manual and spread requests independently; assert unknown monitor errors reach existing worker handling.

- [ ] **Step 2: Run alert tests to verify RED**

  Run: `uv run pytest tests/test_manual_opportunity_alerts.py tests/test_alert_worker.py tests/test_alert_processor.py -q`

  Expected: failure for the processor/router behavior.

- [ ] **Step 3: Implement the minimal processor/router**

  Preserve the existing manual parser and formatting validation. Split initial and expansion presentation only as required for the approved compact format; construct the router in `build_application()` while leaving spread's slow history/chart behavior unchanged.

- [ ] **Step 4: Run alert tests to verify GREEN**

  Run the same command; expected: all alert tests pass.

- [ ] **Step 5: Commit**

  ```bash
  git add src/radar/alerts/manual_opportunity.py src/radar/alerts/worker.py src/radar/alerts/__init__.py src/radar/app.py tests/test_manual_opportunity_alerts.py tests/test_alert_worker.py tests/test_alert_processor.py
  git commit -m "Route Manual Opportunity Telegram alerts"
  ```

### Task 6: Integration Smoke, Example Config, and Full Validation

**Files:**
- Modify: `config/radar.example.yaml`
- Create or modify: `tests/test_manual_opportunity_integration.py`
- Modify existing tests only where shared app fixtures need the new router.

- [ ] **Step 1: Write the integration smoke first**

  Create a synthetic/in-memory integration test that hydrates one route, runs `MonitorRunner` with spread and manual monitors, produces one `manual_initial` and one expansion request, routes them through the manual processor with a fake Telegram transport, and asserts no network method/chart/trading method is invoked. Include a separate assertion that disabled Anomaly v2 still persists lifecycle state/events but queues nothing.

- [ ] **Step 2: Run the integration test to verify RED**

  Run: `uv run pytest tests/test_manual_opportunity_integration.py -q`

  Expected: failure until all live wiring is assembled.

- [ ] **Step 3: Update the development example config**

  Keep the existing Manual Opportunity block disabled with the exact approved values and add `telegram_enabled: true` under `monitors.spread.anomaly_v2` so old behavior is explicit in the example.

- [ ] **Step 4: Run integration test to verify GREEN**

  Run the same command; expected: pass with no real Telegram/API access.

- [ ] **Step 5: Run full deterministic quality gates**

  ```bash
  uv run pytest -m "not live"
  uv run ruff check .
  uv run mypy src
  uv run python -m compileall src
  git diff --check
  ```

  All commands must exit 0. Review `git diff` for signal/fee/threshold and architecture-boundary changes.

- [ ] **Step 6: Commit the completed implementation checkpoint**

  ```bash
  git add config/radar.example.yaml tests/test_manual_opportunity_integration.py
  git commit -m "Wire manual opportunity into live radar"
  ```

- [ ] **Step 7: Push the tested branch**

  ```bash
  git push origin feature/radar-v1
  ```

### Task 7: Controlled Production Deployment and Validation

**Files:**
- Production only: `/Users/lspss93119/.config/opportunity-radar/radar.yaml` after backup.

- [ ] **Step 1: Perform read-only production preflight**

  On `trading-mini`, record hostname, branch, HEAD, clean status, Radar/Dashboard PIDs, config SHA256, runtime SQLite status, and latest market/hourly-context timestamps. Stop if the checkout is not clean or the expected old HEAD is not present.

- [ ] **Step 2: Back up and fast-forward only**

  Create a timestamped config backup without printing secrets. Fetch origin and fast-forward production to the exact tested development SHA; do not reset, rebase, or create a merge commit.

- [ ] **Step 3: Apply and validate only production config changes**

  Set `manual_opportunity.enabled: true` and `monitors.spread.anomaly_v2.telegram_enabled: false`; leave all thresholds and fees byte-for-byte unchanged otherwise. Load the config with the production loader and show only a sanitized diff.

- [ ] **Step 4: Restart only Radar**

  Use the existing `radar` tmux session/pane ownership model, record old/new Radar PIDs, and do not restart Dashboard. Do not send a synthetic message unless transport health is otherwise unverified; if needed, send one explicitly labelled `TEST / SYNTHETIC` manual text only.

- [ ] **Step 5: Validate startup and three aligned cycles**

  Verify process health, hydration log, monitor names (`spread`, `manual_opportunity`), Anomaly v2 persistence with Telegram off, market/hourly/Parquet/SQLite health, stable queue, no monitor/state/freshness errors, and at least three 10-second cycles. Do not lower thresholds or manufacture an opportunity.

- [ ] **Step 6: Roll back on failure**

  Restore the timestamped config backup, return production to the prior known-good revision through a safe Git path, restart the existing Radar command, and verify health. Never leave a partially enabled deployment.

- [ ] **Step 7: Report and stop**

  Report development/production SHAs, commits, config backup, sanitized changes, PIDs, hydration RSS/duration, monitor registration, latest datasets, SQLite/Parquet health, Telegram modes, and clean status. Do not begin automated trading work.
