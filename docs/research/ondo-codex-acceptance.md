# Ondo PR #4 — Codex acceptance runbook

Scope: `feature/ondo-readonly-feed` ONLY. **Do not** merge PR #4, change
production configuration, restart Mac mini services, place orders, send
Telegram, or modify unrelated collectors and monitors.

1. Fetch and check out the branch. Inspect `AGENTS.md`, `docs/research/ondo-readonly-feed.md` and diff against `feature/radar-v1`.
2. Run `uv sync`; `uv run pytest tests/test_ondo.py tests/test_config.py tests/test_pipeline.py`;
   `uv run pytest -m "not live"`; `uv run python -m compileall -q src tests`;
   `git diff --check`.
3. Repair only reproducible failures within the Ondo PR. Verify isolated mock
   tests cover incorrect envelopes, missing/delisted markets, stale books, bad
   depths, funding timestamps and conditional factory registration.
4. Perform read-only API smoke from the MacBook:
   `https://api.ondoperps.xyz/v1/perps/contracts?sparkline=false`
   `https://api.ondoperps.xyz/v1/perps/depth?market=BTC-USD.P&depth=100`
   `https://api.ondoperps.xyz/v1/perps/funding_rates?market=BTC-USD.P`
   Record HTTP status, JSON envelope and sanitized field names only; no keys.
   Verify symbols, book units (base quantity vs contracts), contract
   multipliers, funding unit/interval/next time and cross-market comparability.
   **Never** fabricate fixtures from assumptions. If a public endpoint requires
   authorization or differs from the published gateway example, report the
   incompatibility rather than adding private API credentials.
5. If confirmed, implement minimal corrections, add captured **sanitized**
   public-data fixtures and regression tests; rerun full non-live suite.
   Inspect `MarketDataPipeline` 10-second cadence with Ondo unavailable.
6. Report start/end SHAs, tests, verified live payload metadata, actual
   read-only sample count and errors. Keep PR draft until live schema and
   semantics are fully verified.

The Ondo collector is public REST only. A separate high-frequency
maker order-book/event recorder is **out of scope**.
