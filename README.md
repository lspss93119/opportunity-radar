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

## Read-only dashboard

Run the dashboard as a separate process from Radar using the existing config:

```sh
uv run python -m radar.dashboard --config /absolute/path/to/existing-config.yaml
```

Open `http://127.0.0.1:8787/` for opportunities, pair history, and status. Defaults
read `data/` and `runtime/radar.sqlite3` relative to the working directory; use
`--host`, `--port`, `--data-root`, and `--runtime-db` for explicit overrides.
The dashboard reads existing sources without trading, monitor evaluation, or
changes to Radar's launch process. Missing sources display down/degraded state.

See the [dashboard deployment guide](docs/operations/opportunity-radar-dashboard.md)
and its LaunchDaemon example for install-time path/user discovery, Tailscale-only
binding, separate logs, and the production benchmark required before deployment.

## Dependency lock note

This starter archive may not contain `uv.lock`. On your MacBook, run `uv sync`; uv will resolve the declared dependencies and create/update the lockfile. Commit that lockfile before continuing Task 2.
