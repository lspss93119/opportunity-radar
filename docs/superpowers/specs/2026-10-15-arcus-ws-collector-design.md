# Arcus WebSocket Collector Design

## Goal

Add a formally selectable Arcus `l2Orderbook` WebSocket source while keeping REST as the default and preserving the existing 10-second sampler, storage schema, low-frequency market context, and production behavior.

## Decisions

- `RadarConfig.arcus.source` accepts `rest` (default) or `ws`; no existing YAML needs to change.
- Arcus WS uses `wss://api.arcus.xyz/v1/ws` and one `l2Orderbook` subscription per configured market with `nLevels=20`.
- The channel is treated as complete snapshots. A valid snapshot replaces the per-market book atomically. `lastSequenceId` and `globalSequenceId` are optional monotonic guards only; missing sequence fields do not manufacture a gap, while a backwards value is ignored.
- `observed_at` is the local UTC receive time. The venue timestamp is retained in collector diagnostics/statistics only and is never used as a freshness substitute.
- A reconnect, `degraded` notice, invalid snapshot, or per-market silence invalidates readiness. The next valid snapshot is required before the market can be a live input again.
- WS mode may refresh the existing public REST book as a fallback. Fallback books are archived through the normal 10-second sampler, but are marked in the in-memory latest-data layer as `live_signal_eligible=False`; they cannot populate `RadarState`, `SpreadMonitor`, or Manual Opportunity. A WS snapshot restores eligibility.
- REST mode keeps the existing code path and semantics byte-for-byte where practical.
- Mark/index/funding/open-interest/volume remain on the existing REST paths; only market L2 ingestion is selectable.
- No new persistence columns, raw L2 storage, trading API, order placement, or production source switch are introduced.

## Components

1. `LatestMarketData` gains an in-memory live-signal eligibility bit and diagnostics metadata without changing `MarketSnapshot` or Parquet.
2. `ArcusWebSocketFeed` owns one reconnecting socket, subscriptions, per-market monotonic guards, silence invalidation, and clean cancellation.
3. `ArcusCollector` selects REST or WS, owns the feed and existing low-frequency REST work, and uses non-live REST fallback only when the WS book is not fresh.
4. `RadarConfig` and `MarketDataPipeline.from_config` expose the source choice with REST defaults.

## Error and lifecycle behavior

- Unknown notices are ignored; protocol errors, malformed configured-market books, and connection failures are reported through the existing collector error handler.
- A malformed or old snapshot never overwrites the last valid book. A socket disconnect clears all configured books before reconnecting.
- `stop()` cancels and awaits the feed and REST refresh task; no orphan task or open socket remains.

## Verification

- Unit tests cover parser/book validation, sequence ordering, subscription, silence, disconnect/reconnect, fallback eligibility, depth/VWAP, configuration, pipeline wiring, and REST compatibility.
- Full non-live pytest, `compileall`, and `git diff --check` are required.
- A separate no-write canary uses the new WS feed against selected Arcus markets and compares REST/WS metrics. It never uses Production storage, Telegram, or the running Radar.
