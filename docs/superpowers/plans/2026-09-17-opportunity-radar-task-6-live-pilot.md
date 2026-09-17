# Task 6 MacBook Live Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Compose the completed collectors, storage, monitor, slow alert path, and Telegram transport into a small read-only MacBook pilot.

**Architecture:** Add one application orchestration module only. It owns the aligned 10-second cycle, shared `MonitorRunner.queue`, periodic Parquet flush, shutdown cleanup, counters, and CLI; spread logic remains in `monitors/`, and DuckDB/chart work remains in the existing slow processor.

**Tech Stack:** Python 3.13, `asyncio`, `argparse`, existing Pydantic/Parquet/SQLite/DuckDB/Matplotlib/httpx components, pytest/pytest-asyncio.

**Spec:**
- `docs/superpowers/specs/2026-09-15-opportunity-radar-requirements.md`
- `docs/superpowers/specs/2026-09-15-opportunity-radar-architecture.md`
- `docs/superpowers/specs/2026-09-16-opportunity-radar-task-5b-slow-path-design.md`
- User-provided Task 6 MacBook Live Pilot requirements.

## Global Constraints

- Read-only only: no orders, signing, private keys, execution, or new venue.
- Preserve the aligned 10-second sampling, existing monitor semantics, and no-overlap behavior.
- Use one `asyncio.Queue[AlertRequest]` shared by `MonitorRunner` and `AlertWorker`.
- Keep DuckDB and Matplotlib off the asyncio event loop through existing `asyncio.to_thread()` calls.
- Keep Telegram secrets in `RADAR_TELEGRAM_BOT_TOKEN` and `RADAR_TELEGRAM_CHAT_ID`; never log or store them in YAML.
- Keep local pilot data in ignored `data/` and `runtime/` paths; never commit live data.
- Use 60-second application-owned Parquet flushes and one final graceful-shutdown flush.
- Do not add schedulers, worker/executor frameworks, services, dashboards, retries, or Task 7/production hardening.

---

### Task 1: Make collector failures observable without changing the Collector protocol

**Files:**
- Modify: `src/radar/collectors/base.py`
- Modify: `src/radar/collectors/hyperliquid.py`
- Modify: `src/radar/collectors/lighter.py`
- Modify: `src/radar/pipeline.py`
- Test: `tests/test_pipeline.py`
- Test: `tests/test_hyperliquid_collector.py`
- Test: `tests/test_lighter_collector.py`

**Interfaces:**
- Add an optional `CollectorErrorHandler = Callable[[str, Exception], None]` seam to pipeline construction and built-in collector construction; do not change `Collector.collect()`.
- Report `(venue, exception)` for gather-level failures and built-in collector request/parse failures, while retaining empty/partial fail-closed batches.

- [ ] Write a failing test proving a failed venue is reported while a successful venue batch still updates `RadarState`.
- [ ] Run that focused test and verify it fails because no error callback exists.
- [ ] Add the bounded callback/logger path and pass it from `MarketDataPipeline.from_config()` to both built-in collectors; isolate callback failures and preserve `gather(..., return_exceptions=True)` behavior.
- [ ] Run pipeline and collector tests; confirm existing partial-data and no-stale-data behavior remains unchanged.
- [ ] Commit: `Expose collector failures to the live pilot`.

### Task 2: Add the minimal application composition and live cycle

**Files:**
- Create: `src/radar/app.py`
- Modify: `.gitignore`
- Test: `tests/test_app.py`

**Interfaces:**
- `load_telegram_credentials(environ: Mapping[str, str]) -> tuple[str, str]` raises a clear non-secret error when either required variable is missing.
- `RadarApplication.collect_and_evaluate_once(now: datetime) -> CollectorBatch` calls pipeline collection, then `MonitorRunner.run_cycle(now)` on the same state.
- `RadarApplication.maybe_flush(now: datetime) -> int` flushes only when 60 seconds have elapsed since the last application flush.
- `RadarApplication.run(stop_event: asyncio.Event | None = None) -> None` starts one `AlertWorker` task on `runner.queue`, waits for the next aligned 10-second boundary, runs cycles, and cleans up in the required order.
- `build_application(config, *, data_root=Path("data"), runtime_db=Path("runtime/radar.sqlite3"), telegram_credentials, clock=utc_now) -> RadarApplication` composes the existing concrete components only.

- [ ] Write failing tests for collect → state/Parquet append → monitor cycle, one shared queue, 60-second flush cadence, missing Telegram environment, and shutdown flush/cancel/SQLite close.
- [ ] Run the focused application tests and verify they fail because `radar.app` does not exist.
- [ ] Add `RadarApplication`, simple pilot counters/log callbacks, default paths, aligned boundary waiting via `aligned_sample_time()`, `asyncio.to_thread(pipeline.flush_storage, ...)`, and graceful cleanup.
- [ ] Add `runtime/` to `.gitignore` and keep existing `data/` ignore behavior.
- [ ] Run application tests and the full non-live suite.
- [ ] Commit: `Add MacBook pilot application wiring`.

### Task 3: Add the deterministic CLI and Telegram smoke orchestration

**Files:**
- Modify: `src/radar/app.py`
- Test: `tests/test_app.py`

**Interfaces:**
- Support `python -m radar.app run --config PATH` using the real application composition.
- Support `python -m radar.app telegram-smoke --config PATH [--symbol BTC]`; collect/flush one current sample, build a synthetic read-only `AlertRequest` from current identities/prices without calculating opportunities in `app.py`, and pass it to the existing `SpreadAlertProcessor`.
- The smoke command must use real history/chart/Telegram components; test seams use fake history/transport only in non-live tests.

- [ ] Write failing parser/smoke tests for command dispatch, missing-secret failure without leakage, and processor-based smoke delivery.
- [ ] Run focused CLI/smoke tests and verify the intended failures.
- [ ] Implement only `argparse`, synthetic smoke payload construction, concise operational logging, and command dispatch; do not alter monitor thresholds or add i18n/config frameworks.
- [ ] Run all non-live tests and static checks.
- [ ] Commit: `Add pilot and Telegram smoke commands`.

### Task 4: Execute staged verification and live pilot

**Files:**
- No planned source changes; inspect and run the existing live tests.

- [ ] Run `uv run pytest -m "not live"`, Ruff, Mypy, Compileall, and `git diff --check` before live calls.
- [ ] Run the existing `pytest.mark.live` collector smoke for Hyperliquid and Lighter, BTC/ETH/SOL, and inspect BBO/mark/index/$10k VWAP/funding/Parquet output.
- [ ] Run the real Telegram text/chart smoke path after collecting enough exact samples.
- [ ] Run staged pilot validation: 2–3 cycles, smoke, then 10–15 minutes; record counters, failures, flushes, queue backlog, latest sample age, and responsiveness.
- [ ] For every live failure, check current official documentation/source first, record symptom/status/source/root cause/minimal fix/retest, then rerun required tests.
- [ ] Commit any bounded live-discovered fix separately; stop after Task 6 with no Mac mini/Task 7 work.
