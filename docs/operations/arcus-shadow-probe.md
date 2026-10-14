# Arcus REST / WebSocket shadow probe (read-only)

This isolated probe compares **full order-book snapshots** from Arcus REST
`GET /v1/l2OrderBook/{market}` and WebSocket `l2Orderbook`. It **never**
publishes into Radar's shared cache, alters Production configuration, sends
orders, or writes raw L2 levels. It records BBO/VWAP summaries only.

Source protocol: [Arcus open-source Go websocket adapter](https://pkg.go.dev/github.com/k4k3ru-hub/onchain/go/venues/arcus) and
[order-book response model](https://docs.rs/perp_dex/latest/perp_dex/exchange/arcus/schemas/rest/orderbook/struct.RestOrderBookArcus.html).
This implementation documents `wss://api.arcus.xyz/v1/ws`, `l2Orderbook`,
`id` as market display name, and full snapshot metadata. A read-only live
check on `trading-mini` confirmed the expected `connected` → `subscribed`
sequence for `SNDK-USD`, with 20 bid/ask levels and all five metadata fields
(`timestamp`, `lastSequenceId`, `globalSequenceId`, `bids`, `asks`) present.

## Run separately from Production

```bash
uv run python -m radar.history.arcus_shadow_probe \
  --market SNDK-USD \
  --duration-seconds 900 \
  --rest-interval-seconds 10 \
  --output runtime/research/arcus-shadow-SNDK-15m.csv
```

Run outside the Production process in an isolated checkout. The 15-minute
check below is a protocol, schema, freshness, and REST/WS comparability smoke;
it is not a full-session or 24-hour market-regime validation. Compare only
contemporaneous REST/WS records; do not assume arrival timestamps equal
exchange timestamps.

CSV fields include:
- `received_at_utc`: local completion/receipt timestamp.
- `request_started_at_utc` and `request_duration_ms`: REST call latency.
- `venue_timestamp_raw`: raw server timestamp; the current public Arcus feed
  returned Unix microseconds in the live check. Keep it as source metadata and
  do not substitute it for local receipt time when measuring latency.
- `last_sequence_id` and `global_sequence_id`: audit-only (not a reconstructed book).
- `best_bid`, `best_ask`, 1k/5k/10k VWAP, status and error type.

This uses **periodic snapshot** channel `l2Orderbook`, not delta
`l2OrderbookUpdates`, so no delta reconstruction or sequence continuity is
claimed. Absence of WS messages is not treated as a zero-price quote.

## Verified read-only observation (2026-10-14)

The following run used the isolated checkout on `trading-mini` at commit
`6de11b1f4d9444af3bab63ba1aecffd0ef3acbd5`, not the running Production
checkout. The configured, online market was `SNDK-USD`.

- Window: `2026-10-14T05:00:48Z`–`05:15:48Z` (15 minutes).
- Output: `/Users/lspss93119/Research/opportunity-radar-data-quality-v1-20261014/runtime/research/arcus-shadow-15m.csv`.
- REST: 89/89 valid; receive interval P50/P95/P99/max `10.153/10.328/10.396/10.408 s`; request duration P50/P95/P99/max `0.151/0.326/0.393/0.407 s`.
- WS: 3,437/3,437 valid; receive interval P50/P95/P99/max `0.201/1.185/1.485/1.836 s`.
- VWAP availability: REST 1k/5k/10k all 100%; WS 1k/5k 100%, 10k buy `99.884%`, sell `99.855%`.
- Exact shared exchange-timestamp pairs: 62. BBO and every available 1k/5k/10k VWAP absolute difference was zero across those pairs.
- A direct public WS check received `connected` then `subscribed` for `l2Orderbook`, with 20 bid and 20 ask levels and `timestamp`, `lastSequenceId`, `globalSequenceId`, `bids`, and `asks` present. The REST snapshot also returned 20 bid and 20 ask levels with the same metadata fields.

The observation confirms protocol compatibility and short-window freshness, but
does not establish regular-session or 24-hour behavior. Keep the Production
ArcusCollector on REST; do not switch it to WS based on this smoke alone.
