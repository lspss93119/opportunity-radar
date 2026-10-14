# OpportunityRadar Data Quality V1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify and minimally repair the existing low-depth retention, Lighter recovery, and Arcus REST/WS shadow-probe changes without changing Production behavior.

**Architecture:** Work only in an isolated checkout of `feature/radar-data-quality-v1`. Treat historical retention, Live Monitor eligibility, and shadow observation as separate contracts. Any code change must be driven by a reproducible fixture or live-protocol failure, with no Production deployment or trading capability.

**Tech Stack:** Python, `uv`, pytest, asyncio/websockets, Parquet-compatible pipeline code, Git worktree, SSH to `trading-mini` for read-only verification.

**Spec:** User-provided `OpportunityRadar Data Quality V1 — 驗證、修復與 Arcus WS 實測` in `/Users/liaoyuchen/.codex/attachments/826c8208-dfc8-436b-aa0b-11e87a1e08cb/貼上的文字.txt`.

## Global Constraints

- Use an independent Git worktree or research checkout.
- Do not modify the Production checkout, configuration, or historical data.
- Do not restart Radar, Dashboard, or any trading bot.
- Do not deploy changes automatically.
- Do not perform trading, signing, or private API operations.
- Do not use full historical backtests to test Collector correctness.
- Preserve the architecture boundaries in `AGENTS.md` and keep the change limited to this task.
- Commit and push only to `feature/radar-data-quality-v1`; do not merge to `feature/radar-v1`.

## Review Focus

- A quote with valid `$1k`/`$5k` depth but invalid `$10k` depth must be retained historically without becoming Live Monitor-eligible.
- A nonce gap must invalidate only the affected Lighter market, ignore deltas until a fresh snapshot, and leave no recovery-task leak.
- Arcus REST and WS observations must be compared only at causally comparable times and must preserve unknown event-time semantics.
- Snapshot, delta, REST response, and parser schema failures must be observable and must not be silently counted as valid quotes.
- Production service health and configuration must be checked from read-only evidence after all research activity.

---

### Task 1: Baseline and change review

**Files:**
- Read: `AGENTS.md`
- Read: `src/radar/pipeline.py`
- Read: `src/radar/market_data.py`
- Read: `src/radar/collectors/lighter.py`
- Read: `src/radar/collectors/arcus.py`
- Read: `src/radar/history/arcus_shadow_probe.py`
- Read: `tests/test_pipeline.py`
- Read: `tests/test_market_data.py`
- Read: `tests/test_lighter_websocket.py`
- Read: `tests/test_lighter_collector.py`
- Read: `tests/test_arcus_collector.py`
- Read: `tests/test_arcus_shadow_probe.py`
- Read: `docs/operations/arcus-shadow-probe.md`

**Interfaces:**
- Consumes: commits `b6d199d`, `f37c4ce`, and `744a54f`.
- Produces: a written baseline of current behavior, test failures, and exact candidate defects before any edit.

- [ ] **Step 1: Inspect the current branch and commit diffs**

Run `git status --short --branch`, `git log`, and `git diff b6d199d..HEAD` in the isolated worktree. Record whether HEAD is `744a54f` and whether the working tree is clean.

- [ ] **Step 2: Read the specified source, tests, and operations document completely**

Trace the data path from collector snapshot validation through pipeline persistence and dashboard-facing fields, and trace Lighter recovery and Arcus parser state transitions.

- [ ] **Step 3: Run the focused non-live baseline test command**

Run:

```bash
uv run pytest -m "not live" tests/test_pipeline.py tests/test_market_data.py tests/test_lighter_websocket.py tests/test_lighter_collector.py tests/test_arcus_collector.py tests/test_arcus_shadow_probe.py -q
```

Record the exact pass/fail count and every failure before proposing a fix.

### Task 2: Low-depth historical retention contract

**Files:**
- Inspect/modify only if a failing fixture proves a defect: `src/radar/pipeline.py`, `src/radar/market_data.py`
- Test: `tests/test_pipeline.py`, `tests/test_market_data.py`

**Interfaces:**
- Consumes: normalized `MarketSnapshot` validation and pipeline persistence behavior.
- Produces: fixture evidence that `$1k`/`$5k` valid snapshots persist, invalid VWAP fields remain null, and `$10k` Live eligibility is unchanged.

- [ ] **Step 1: Add or identify the smallest failing fixture for valid low-depth retention**

Exercise a market where `$1k` is valid and `$10k` is unavailable. Assert the persisted snapshot exists, the unavailable VWAP is null, and no fabricated price is emitted.

- [ ] **Step 2: Run only the new fixture and confirm the expected failure if behavior is missing**

Run the focused pytest node and verify it fails for the intended contract rather than due to fixture setup.

- [ ] **Step 3: Implement the minimal root-cause correction, if required**

Keep historical persistence separate from Live Monitor eligibility; do not relax BBO, stale, locked, or crossed-book validation.

- [ ] **Step 4: Verify the fixture and adjacent pipeline tests**

Run the focused pipeline and market-data test files and inspect schema/CSV compatibility assertions.

### Task 3: Lighter order-book recovery

**Files:**
- Inspect/modify only if a reproducible test proves a defect: `src/radar/collectors/lighter.py`
- Test: `tests/test_lighter_websocket.py`, `tests/test_lighter_collector.py`

**Interfaces:**
- Consumes: Lighter snapshot/delta message handling and subscription lifecycle.
- Produces: deterministic recovery behavior for both Lighter and Lighter Robinhood market streams.

- [ ] **Step 1: Cover normal snapshot-to-delta application**

Assert a complete snapshot is usable and a contiguous nonce delta updates only its market book.

- [ ] **Step 2: Cover nonce-gap invalidation and market isolation**

Assert the affected market is cleared immediately, another market remains usable, and deltas are ignored until a fresh snapshot.

- [ ] **Step 3: Cover resubscription, timeout, rejection, and cleanup**

Use controlled fake WS behavior to assert a fresh snapshot is required, timeout retries are bounded/controlled, rejected resubscription can recover through the intended reconnect path, and stop cleans all tasks.

- [ ] **Step 4: If a test fails, complete root-cause investigation before editing**

Compare the failing state transition with the existing working message path and protocol assumptions; write the smallest regression test first, then make one minimal fix.

- [ ] **Step 5: Run all Lighter-focused tests**

Run `uv run pytest -m "not live" tests/test_lighter_websocket.py tests/test_lighter_collector.py -q` and record task/exception warnings.

### Task 4: Arcus REST/WS shadow protocol validation

**Files:**
- Inspect/modify only if protocol evidence proves a defect: `src/radar/history/arcus_shadow_probe.py`
- Test: `tests/test_arcus_shadow_probe.py`
- Documentation: `docs/operations/arcus-shadow-probe.md`

**Interfaces:**
- Consumes: Arcus public REST `/v1/l2OrderBook/{market}` and WS `wss://api.arcus.xyz/v1/ws`, channel `l2Orderbook`.
- Produces: traceable REST/WS observations with explicit snapshot, sequence, timestamp, BBO, and VWAP availability semantics.

- [ ] **Step 1: Verify the actual enabled Arcus market symbol and endpoint inputs read-only**

Inspect Production configuration and/or public metadata without changing it or using private/trading permissions.

- [ ] **Step 2: Add or identify parser fixtures for subscription acknowledgement and L2 snapshots**

Assert the actual `contents`, `timestamp`, `lastSequenceId`, and `globalSequenceId` shapes are parsed without treating unknown fields as valid evidence.

- [ ] **Step 3: Run the parser/probe tests and fix only proven schema or lifecycle defects**

Preserve REST as the current Production path; do not modify `ArcusCollector` to use WS.

- [ ] **Step 4: Run the 5-minute Arcus smoke observation**

Use one enabled, liquid market and record command, start/end times, PID, output path, REST success rate, WS connection/subscription status, snapshots, parser errors, and timestamp provenance.

- [ ] **Step 5: Run the bounded 15-minute observation requested by the user**

After the local probe is stopped and analyzed, run the same read-only probe in a `trading-mini` isolated checkout for 15 minutes. Record REST/WS update intervals, request/receive latency, maximum no-update interval, valid BBO and `$1k`/`$5k`/`$10k` VWAP rates, errors/disconnects, and only time-comparable REST/WS price differences.

- [ ] **Step 6: Do not start a 24-hour observation in this narrowed scope**

Report that the bounded 15-minute result does not establish full-session or 24-hour behavior. Do not leave a long-running probe behind.

### Task 5: Timestamp semantics and deployment-safety checks

**Files:**
- Inspect/modify only if a causal timestamp defect is demonstrated: `src/radar/history/arcus_shadow_probe.py`, `src/radar/market_data.py`, or focused tests/docs.

**Interfaces:**
- Consumes: `sample_time`, `observed_at`, cycle-start, request-start/completion, and exchange-event timestamps.
- Produces: a documented causal interpretation and regression evidence against look-ahead or false event-time claims.

- [ ] **Step 1: Trace timestamp assignment at each collector and probe boundary**

Distinguish local receive/request completion from exchange event time and explain why `observed_at > sample_time` can be valid.

- [ ] **Step 2: Add a minimal causal alignment regression test if needed**

Assert data not received by the decision time cannot be used as a contemporaneous observation, while delayed-but-valid local receipt is not mislabeled as exchange event time.

- [ ] **Step 3: Verify Production health read-only**

Over SSH to `trading-mini`, capture Radar PID/status, Dashboard HTTP GET response, configured checkout branch/commit, configuration hash or read-only diff, and Production working-tree/data mtimes. Do not restart or mutate anything.

### Task 6: Full verification and delivery

**Files:**
- Modify only files justified by the preceding regression evidence.
- Add/update only focused tests and the Arcus operations document as needed.

**Interfaces:**
- Consumes: all focused tests, live observation artifacts, and Production read-only checks.
- Produces: pushed commit on `feature/radar-data-quality-v1` and Traditional Chinese evidence report in the final response.

- [ ] **Step 1: Run the required focused non-live suite**

Run the exact command from the spec and save the output.

- [ ] **Step 2: Run the complete non-live suite**

Run `uv run pytest -m "not live" -q`; record all tests, skips, warnings, and failures.

- [ ] **Step 3: Run syntax, diff, and status checks**

Run `python -m compileall` on changed Python files, `git diff --check`, and `git status --short`.

- [ ] **Step 4: Confirm no shadow probe remains uncontrolled**

List the research probe processes and verify only an intentionally documented 24-hour observation remains, or that all probes have exited cleanly.

- [ ] **Step 5: Commit and push the isolated branch**

Commit only task-scoped changes with a clear message and push `feature/radar-data-quality-v1` to its existing GitHub branch. Do not merge or deploy.

- [ ] **Step 6: Report evidence and unresolved non-critical issues**

Include final SHA, exact test counts, Arcus REST/WS observations, Lighter recovery evidence, timestamp findings, low-depth compatibility, Production health, and a direct deployment-readiness decision.

## Self-review checklist

- [ ] Every requested Task 1–6 has a corresponding verification step.
- [ ] No step permits modifying Production or adding trading capability.
- [ ] Any code change is preceded by a reproducible failing test or protocol observation.
- [ ] Live observations are reported as completed only when their duration actually elapsed.
- [ ] Full non-live tests, diff checks, and Production health checks are completed before any success claim.
