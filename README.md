# Opportunity Radar

A small, read-only personal market opportunity radar for 24/7 market monitoring.

## v1 scope

The first monitor is **Perp ↔ Perp Cross-Exchange Spread**. Development starts with Lighter + Hyperliquid and BTC / ETH / SOL.

The system collects normalized market data, stores a rolling 90-day history, evaluates monitors, reconstructs historical context on demand, and sends Telegram alerts. It does **not** place trades.

## Start developing

```bash
uv sync
uv run pytest
```

Read these before implementation:

- `AGENTS.md`
- `docs/superpowers/specs/2026-09-15-opportunity-radar-requirements.md`
- `docs/superpowers/specs/2026-09-15-opportunity-radar-architecture.md`
- `docs/superpowers/plans/2026-09-15-opportunity-radar-macbook-v1.md`

Current checkpoint: **Task 1 Foundation**.

## Dependency lock note

This starter archive may not contain `uv.lock`. On your MacBook, run `uv sync`; uv will resolve the declared dependencies and create/update the lockfile. Commit that lockfile before continuing Task 2.
