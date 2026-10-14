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
  --market BTC-USD \
  --duration-seconds 3600 \
  --rest-interval-seconds 10 \
  --output runtime/research/arcus-shadow-BTC-1h.csv
```

After verifying one-hour connectivity and the returned snapshot shape, run
a 24h trial for a representative active symbol. Run outside the production
process in an isolated checkout. Compare only contemporaneous REST/WS records;
do not assume arrival timestamps equal exchange timestamps.

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

Analysis checklist: connection/subscribe acceptance, WS message cadence,
REST request latency, temporal comparison at a shared as-of time, VWAP
agreement for each available tier, stale/unknown intervals, and stability
across market sessions. Keep WS shadow out of Production until these tests
confirm accuracy and coverage; no fallback to an unverified delta feed.
