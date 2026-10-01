# Opportunity Radar Anomaly Episode v2 Implementation Plan

## Goal

Add a disabled-by-default anomaly-episode v2 path that reuses the existing
`AnomalyTracker`, keeps legacy spread monitoring unchanged, persists lifecycle
state separately, and adds asynchronous Telegram and read-only Dashboard
presentation.

## Constraints

- Do not touch production data, processes, configuration, or deployment.
- Keep collectors, sampling cadence, normalized schemas, storage schemas, and
  trading boundaries unchanged.
- Keep 12h/24h/48h history work out of `SpreadMonitor.evaluate()`.
- Dashboard reads persisted state and history only; it never evaluates a
  monitor or writes runtime state.

## Tasks

1. **Configuration and pure tracker restore**
   - Add the nested disabled-by-default `anomaly_v2` configuration and
     validation.
   - Add a supported `AnomalyTracker` restore path without changing detector
     semantics.
   - Add red/green tests for config defaults/validation and tracker round trips.

2. **Historical confirmation context**
   - Add an exact directional-pair, prior-only 12h/24h/48h query in
     `history/spread.py` with latest-observed dedup, bounded skew, exact sample
     pairing, and replay-compatible eligibility.
   - Add synthetic Parquet tests and a replay-equivalence regression.

3. **Live v2 monitor lifecycle**
   - Add a small persisted v2 runtime wrapper in `SpreadMonitor`.
   - Branch cleanly between legacy and v2 modes; use `AnomalyTracker` for all
     v2 transitions.
   - Emit deterministic initial/expansion/return lifecycle requests and persist
     `anomaly_episodes_v2` independently of legacy `episodes`.
   - Add restart, gap, expansion, resolution, direction-isolation, and legacy
     compatibility tests.

4. **Telegram v2 notification path and chart**
   - Add typed v2 payload/detail models, compact initial/expansion/return
     formatting, notification state under `anomaly_notifications_v2`, and
     fail-closed context/alignment gating.
   - Keep slow history and chart work off the asyncio loop and preserve the
     existing caption fallback.
   - Add a v2 chart with raw spread, prior-only 24h mean, frozen reference,
     and lifecycle markers; leave the legacy chart path intact.
   - Add deterministic gate, dedup, transport-failure, formatting, and chart
     tests.

5. **Application wiring**
   - Pass the v2 configuration/runtime dependencies through the existing app
     construction path without changing telegram-smoke or legacy behavior.
   - Add focused construction/integration tests.

6. **Read-only Dashboard v2**
   - Add `/api/anomalies`, the Anomalies page, v2 lifecycle fields on exact
     Pair Detail, and concise v2 Status counters.
   - Read SQLite in read-only mode and keep existing All Pairs/Status APIs and
     links compatible.
   - Add API, sorting/filter, lifecycle, malformed-state, chart-marker, and
     read-only/no-monitor-evaluation tests.

7. **Validation and review**
   - Run focused tests after each task, then the full non-live suite,
     compileall, Ruff, changed-file Mypy, and diff check.
   - Run the QQQ replay regression and an isolated synthetic v2 integration
     check.
  - Review the diff for scope, legacy compatibility, and absence of
    production/deployment changes; commit logical groups and push only the
    feature branch.
