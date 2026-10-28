# Radar Alert Delivery Issue 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Spread, Anomaly V2, and Manual Opportunity Telegram alerts durable across queue loss and process restart without changing monitor signal semantics.

**Architecture:** Keep the existing in-memory `asyncio.Queue` as a low-latency wake-up path, but make SQLite the durable source of pending notifications. Each monitor will insert its eligible `AlertRequest` into a SQLite outbox in the same transaction that commits its lifecycle state and opportunity events. A single worker will acknowledge successful sends, retain retryable failures with bounded backoff, and record permanent failures for inspection; delivery remains at-least-once rather than exactly-once.

**Tech Stack:** Python 3.13, asyncio, SQLite, pytest, existing Telegram transport and monitor state/event stores.

**Spec:** GitHub Issue #1, `Audit P0: durable Telegram alert delivery without changing monitor semantics`.

## Global Constraints

- Preserve Spread, Anomaly V2, and Manual Opportunity signal, deduplication, cooldown, and notification-gate semantics.
- Use the existing SQLite runtime database; do not add Redis, Celery, an external queue, or a new framework.
- `telegram_enabled=false` must not create sendable outbox rows.
- Telegram delivery is at-least-once; do not claim exactly-once behavior across a transport timeout or process crash.
- Do not modify Production configuration, restart services, send real Telegram messages, or deploy.

## Review Focus

- A lifecycle commit followed by a lost queue enqueue must leave one recoverable outbox row; test transaction atomicity and duplicate event IDs.
- A restart must process pending rows without requiring the original in-memory queue; test reopening SQLite and worker recovery.
- Transient and permanent delivery failures must remain inspectable and must not create an unbounded retry loop; test bounded retry scheduling and terminal failure.
- Anomaly V2 attempt/result persistence and cooldown must not conflict with outbox retries; test successful delivery deduplication and retry-safe state.
- Manual `reserve_initial`, `mark_sent`, `mark_failed`, quiet re-arm, and expansion gating must remain coherent across failure and restart.

### Task 1: SQLite notification outbox

**Files:**
- Modify: `src/radar/storage/sqlite.py`
- Test: `tests/test_storage_sqlite.py`

**Interfaces:**
- Add a backward-compatible `notification_outbox` table.
- Extend the existing atomic state/event commit methods with optional `AlertRequest` notifications.
- Expose read/ack/failure methods used by the single alert worker.

- [x] Write tests for atomic lifecycle/event/outbox commit, restart persistence, idempotent duplicate insertion, sent acknowledgement, retryable failure, and terminal failure.
- [x] Run the focused SQLite tests and verify the new tests fail before implementation.
- [x] Implement JSON serialization, UTC validation, pending-row selection, bounded retry metadata, and backward-compatible table creation.
- [x] Run the focused SQLite tests and verify they pass.

### Task 2: Persist eligible alerts at monitor commit boundaries

**Files:**
- Modify: `src/radar/monitors/spread/monitor.py`
- Modify: `src/radar/monitors/manual_opportunity.py`
- Modify: `src/radar/monitors/manual_opportunity.py` only to persist the existing failed reservation reset safely if required by the recovery tests.
- Test: `tests/test_spread_monitor.py`
- Test: `tests/test_anomaly_monitor_v2.py`
- Test: `tests/test_manual_opportunity_integration.py`
- Test: `tests/test_manual_notification_gate.py`

**Interfaces:**
- Pass only alerts that are already eligible under existing monitor gates into the existing SQLite state/event transaction.
- Do not enqueue Anomaly V2 alerts when `telegram_enabled` is false.

- [x] Add failing integration tests proving legacy Spread and Manual alerts are in the outbox with their lifecycle state/event in one commit.
- [x] Add a failing Anomaly V2 test proving disabled Telegram persists lifecycle/events but creates no sendable outbox row.
- [x] Add a Manual failure/restart test proving `mark_failed` does not strand a route reservation and re-arm/expansion semantics remain unchanged.
- [x] Implement the smallest call-site changes to pass alerts through existing persistence boundaries.
- [x] Run the focused monitor/gate tests and verify all pass.

### Task 3: Outbox-aware single worker

**Files:**
- Modify: `src/radar/alerts/worker.py`
- Modify: `src/radar/app.py`
- Test: `tests/test_alert_worker.py`
- Test: `tests/test_app.py`

**Interfaces:**
- Keep existing queue-only construction compatible with current tests and non-runtime callers.
- When given `SQLiteRuntimeStore`, process queued alerts with durable status checks and recover due pending rows after restart.

- [x] Add failing tests for queue-loss recovery, send-success deduplication, transient retry, permanent failure inspection, shutdown cancellation, and single-worker ordering.
- [x] Implement queue-first processing plus periodic outbox polling, success acknowledgement, bounded retry delay, and terminal failure status.
- [x] Wire the existing runtime store into the production `AlertWorker`; preserve one worker and existing error logging.
- [x] Run focused worker/application tests and verify all pass without real Telegram I/O.

### Task 4: Full verification and delivery

**Files:**
- Review all changed files; no unrelated refactor.

- [ ] Run focused alert/storage/monitor tests.
- [ ] Run complete non-Live pytest suite.
- [ ] Run `python -m compileall src tests` and `git diff --check`.
- [ ] Review the final diff for architecture and signal-semantic compatibility.
- [ ] Commit to `fix/radar-alert-delivery-20261017` and push only that branch.
