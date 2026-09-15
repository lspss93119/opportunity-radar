# Opportunity Radar — Requirements

## Purpose
A 24/7, read-only personal market opportunity radar. It identifies opportunities that persist long enough for human review and manual execution.

## v1 monitor
Perp ↔ Perp cross-exchange executable-spread monitoring only.

## Explicit non-goals
No order placement, wallet signing, private-key handling, automatic execution, position management, HFT, sub-second strategies, MEV, ML scoring, dashboard, or microservices.

## Universe
Manually configured `venue + venue_symbol -> canonical_symbol`. v1 pilot: Lighter + Hyperliquid, BTC / ETH / SOL.

## Sampling
- Market snapshot: aligned every 10 seconds, all venues concurrent.
- Keep both `sample_time` and real `observed_at` in UTC.
- Funding: settlement-aware / at least hourly observation, preserving `effective_time`.
- OI and 24h volume: hourly background context.

## Executable depth
Collectors may read L2 in memory, but full L2 is never persisted. Persist executable VWAP at fixed notionals: $1k, $5k, $10k. Insufficient depth => NULL; never extrapolate. Primary scanner size: $10k.

## Spread
Long A / Short B raw spread in bps:
`(B_sell_10k / A_buy_10k - 1) * 10,000`

Current net spread subtracts manually configured taker fees for both venues. Funding is context only and is not mixed into v1 spread.

## Scanner
For each canonical symbol, rank all valid venue $10k buy VWAPs ascending and sell VWAPs descending. Evaluate Top 3 buys × Top 3 sells only; remove same-venue, stale, invalid, or insufficient-depth combinations.

Default candidate: net >= 10 bps for >= 30 seconds.
Default alert: net >= 20 bps for >= 120 seconds.
Thresholds are config values and may have symbol-level overrides later.

## Historical view
Reconstruct pair spread on demand from source venue history, not persisted pairwise matrices. Support 7d / 30d / 90d raw executable spread. Market-to-market alignment uses exact `sample_time`; slow context may use backward ASOF only (`effective_time <= target time`). No look-ahead.

## Storage
- Parquet: market, funding, hourly context; rolling 90 days only.
- DuckDB: read/query Parquet for historical views.
- SQLite: monitor runtime state, alert state, compact opportunity log.
- YAML config: universe, fees, thresholds, monitor enablement.
- Secrets: environment/secret store only.

## Reliability principle
Fail closed. Missing/stale/invalid data may cause missed alerts but must never create false opportunities.
