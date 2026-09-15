# Start Here — Codex

The repository is currently on the `feature/radar-v1` development branch.

Before changing code, read in this order:

1. `AGENTS.md`
2. `docs/superpowers/specs/2026-09-15-opportunity-radar-requirements.md`
3. `docs/superpowers/specs/2026-09-15-opportunity-radar-architecture.md`
4. `docs/superpowers/plans/2026-09-15-opportunity-radar-macbook-v1.md`

## First Codex assignment

Implement **Task 2 — Market Data Pipeline** only.

Requirements:
- Do not begin Task 3 or later tasks.
- Preserve all three architecture boundaries in `AGENTS.md`.
- Use fixtures for venue-specific parser/normalization behavior.
- Add only limited live smoke tests under a `live` pytest marker.
- Use aligned 10-second sample times and concurrent venue collection.
- Missing/failed venue data must fail closed and must not reuse old executable prices as fresh data.
- Keep Hyperliquid and Lighter venue logic isolated from monitor logic.
- At completion, run the full non-live test suite and report:
  - starting SHA
  - ending SHA
  - files changed
  - tests run/results
  - live checks performed
  - design deviations
  - known limitations
  - `git status`

Do not introduce plugin frameworks, event buses, dependency-injection frameworks, Redis, Kafka, or trading execution.
