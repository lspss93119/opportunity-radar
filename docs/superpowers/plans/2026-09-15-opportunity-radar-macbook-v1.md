# Opportunity Radar — MacBook v1 Implementation Plan

> **For agentic workers:** Implement one task at a time. Use TDD for deterministic behavior. Do not continue into the next task until the current task passes its tests and has a clean checkpoint.

**Goal:** Build a small, extensible, read-only opportunity radar on MacBook before production deployment to Mac mini.

**Architecture:** Collectors normalize venue data into shared models. RadarState holds current data; Parquet stores rolling 90-day history. A cadence-aware Monitor Runner executes monitors on a fast path; AlertRequest crosses an asyncio queue to slow historical/chart/Telegram work.

**Tech Stack:** Python 3.13, uv, Pydantic v2, PyYAML, asyncio, httpx/websockets as needed, PyArrow, DuckDB, SQLite, matplotlib, pytest, pytest-asyncio.

**Specs:**
- `docs/superpowers/specs/2026-09-15-opportunity-radar-requirements.md`
- `docs/superpowers/specs/2026-09-15-opportunity-radar-architecture.md`

## Global constraints
- Read-only. No execution/private keys/trading permission.
- Fixed executable sizes: $1k/$5k/$10k; primary scanner size $10k.
- Aligned 10-second market sampling, UTC timestamps, concurrent collectors.
- Market/funding/hourly context retained for rolling 90 days.
- Fail closed on missing/stale/invalid data.
- Preserve the three architecture boundaries in `AGENTS.md`.

---

## Task 1 — Foundation
Create repo/tooling, config models, shared data models, and executable VWAP functions. Tests cover model/config validation and VWAP behavior.

**Exit:** `uv sync` works; config/models/VWAP tests pass.

## Task 2 — Market Data Pipeline
Add Collector Protocol, Hyperliquid and Lighter collectors, aligned scheduler, RadarState, and low-frequency Funding/OI/Volume capture. Use fixtures for parsers and limited `@pytest.mark.live` smoke tests.

**Exit:** MacBook continuously produces normalized BTC/ETH/SOL snapshots from both venues with aligned sample times and realistic executable prices.

## Task 3 — Storage
Add buffered Parquet datasets (`market`, `funding`, `hourly_context`), DuckDB-readable layout, rolling 90-day cleanup, and generic SQLite runtime/opportunity state.

**Exit:** buffered writes query correctly; retention boundaries pass; restart preserves data/state.

## Task 4 — Monitor Framework
Add a tiny Monitor Protocol, registry, cadence-aware runner, JSON-compatible AlertRequest, and asyncio alert queue. No plugin framework/event bus/DI framework.

**Exit:** dummy monitors with different cadences run independently; failures are isolated; AlertRequest reaches queue.

## Task 5 — Perp Spread Monitor
Implement Top-3 × Top-3 $10k scanner, fees, stale/NULL filtering, monitor-private candidate lifecycle, runtime persistence, exact-sample historical raw spread, 7/30/90d chart/context, and Telegram rendering on the slow path.

**Exit:** synthetic end-to-end data produces a correct alert without blocking monitor evaluation.

## Task 6 — Live MacBook Pilot
Wire Lighter + Hyperliquid, BTC/ETH/SOL, state/storage/monitor/alert worker/Telegram. Use temporary low thresholds to force the pipeline. Manually compare BBO/mark/index/$1k/$5k/$10k/funding to venue data. Test collector outage, Telegram failure, historical-view failure, and restart behavior.

**Exit:** full non-live suite passes; live pilot works; no fake spread from stale/missing data; restart does not duplicate an active alert episode.

## Deferred to Mac mini
launchd, boot auto-start, production reconnect hardening, disk-space warnings, external SSD layout, backups, log rotation, power/sleep tuning, full health monitoring, 7-day soak test, production secret management.
