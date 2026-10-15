# Arcus WebSocket Collector

This feature adds an opt-in Arcus `l2Orderbook` source. Production remains
REST by default; changing `arcus.source` is an explicit deployment decision and
is not part of the collector implementation.

## Configuration

The existing REST behavior is preserved when the `arcus` block is omitted:

```yaml
arcus:
  source: rest
  ws_url: wss://api.arcus.xyz/v1/ws
  ws_levels: 20
  ws_silence_seconds: 5.0
  ws_reconnect_delay_seconds: 1.0
  rest_fallback: true
```

For an isolated canary only, set `source: ws`. The collector keeps mark price,
index price, funding, open interest, and volume on their existing REST paths.
The WebSocket is used only for the high-frequency L2 book.

## Freshness and fallback

- Arcus full-book snapshots are normalized atomically per market.
- `observed_at` is the local UTC receive time. The raw venue timestamp is kept
  in the in-memory WebSocket snapshot for diagnostics and is not used as a
  freshness substitute.
- A valid WS book is eligible for Live monitoring until the configured silence
  timeout. A disconnect, `degraded` notice, malformed book, or silence clears
  readiness; a new valid snapshot is required after recovery.
- When `rest_fallback: true`, the existing REST book may still be sampled and
  archived, but it is marked `live_signal_eligible: false`. It cannot enter
  `RadarState`, Spread Monitor, or Manual Opportunity. WS eligibility returns
  only after a new WS snapshot.
- The 10-second sampler and Parquet schema are unchanged. Individual WS
  messages are never written directly to Parquet.

## No-write canary

Run the canary from an isolated checkout and an output directory outside the
Production data and runtime paths. Use only selected public Arcus markets, do
not load Telegram credentials, and do not construct a `ParquetStorage` or
`RadarState` writer. Record the checkout SHA, host, UTC start/end, configuration
source, duration, and output file checksums.

The required comparison includes raw WS messages, accepted complete snapshots,
sequence/silence/reconnect counters, BBO-change and snapshot intervals, local
REST request intervals, depth/VWAP availability, and RSS/CPU. REST response
time is not an exchange event timestamp; if the REST payload has no comparable
venue timestamp, report near-time local comparisons separately and mark exact
exchange-time matching as unavailable.

Before any future source switch, repeat the canary during a normal US session
and compare the five representative Arcus markets (SNDK, TSLA, NVDA, AMD, and
MU) against the same-time non-Arcus feed. This document does not authorize a
Production source switch.
