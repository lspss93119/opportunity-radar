# Ondo Read-Only Production Deployment Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:verification-before-completion before claiming completion.

**Goal:** Enable only the three verified Ondo public feeds (BTC-USD.P, ETH-USD.P, and NVDA-USD.P) in the existing Radar production pipeline for read-only collection and Dashboard display, while excluding them from executable spread/anomaly/manual opportunity monitoring and Telegram notifications.

**Architecture:** Preserve the existing collector, Parquet, SQLite, Dashboard, and monitor architecture. Add only an explicit per-market monitor participation flag with a safe default of enabled; collectors and Dashboard use enabled markets, while monitors use the filtered market set. Ondo fee values remain research estimates only and no signing or order path is added.

**Tech Stack:** Python, Pydantic/YAML configuration, pytest, compileall, SQLite/Parquet, existing tmux/launch procedure on `trading-mini`.

**Spec:** `docs/research/ondo-readonly-feed.md`, repository `AGENTS.md`, and the supplied Production deployment requirements.

## Global Constraints

- Never add order placement, signing, private credentials, or automatic trading.
- Do not modify Recorder, unrelated venues, existing alert behavior, or Production data history.
- Do not restart Dashboard; restart only the existing Radar process if every deployment gate passes.
- Keep Ondo limited to BTC, ETH, and NVDA and keep Telegram/monitor participation disabled for those markets.
- Use public research fees of maker 1 bp and taker 2.5 bps only as configuration metadata; do not present them as account fees or realized funding.
- Stop before deployment if the Production checkout is dirty, the running command cannot be identified safely, the config cannot be backed up/validated, or any test/health gate fails.

## Work Units

1. Verify local branch ancestry and clean state; inspect the merged Ondo collector and monitor wiring.
2. Read-only inspect Mac mini Production HEAD, config hash, process/tmux ownership, feed count, SQLite/Parquet health, and worktree.
3. Add the smallest monitor-exclusion model/wiring with regression tests if the merged code lacks it.
4. Run focused tests, all non-live pytest, compileall, and `git diff --check`; run isolated public Ondo smoke with separate data paths.
5. Commit only the tested change, fast-forward/publish the exact tested commit as required by the existing release procedure, back up and validate the Production config, then restart only the existing Radar process.
6. Verify Ondo feeds, existing feeds, persistence, heartbeat, Dashboard, and monitor/Telegram exclusion without creating synthetic opportunities.
7. Start a separate read-only observation recorder with an explicit start timestamp and durable output path; report that the 24-hour window is pending rather than complete.

## Review Focus

- Enabled Ondo rows must appear in collector and Dashboard feed inventories.
- Ondo rows must not be passed to Spread, Anomaly V2, or Manual Opportunity route construction.
- Existing non-Ondo monitor routes and alert settings must remain unchanged.
- Production config and historical data must be backed up/preserved; no `sync`, destructive reset, or duplicate process is allowed.
- Final claims must be based on fresh command output and clearly separate historical isolated-pilot evidence from current Production evidence.
