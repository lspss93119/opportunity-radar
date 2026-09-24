# Opportunity Radar Latency-Independent Market Ingestion

Status: approved design; implementation pending.

## Goal

Make the 10-second market sampler independent of venue network latency while
preserving the existing normalized models, fail-closed behavior, RadarState,
Parquet storage, SpreadMonitor, alerting, fees, and sampling cadence.

The application will continuously maintain the newest usable market data in
process. The aligned 10-second sampler will read that data synchronously and
will never wait for a REST request, a WebSocket message, a metadata refresh, or
hourly context collection.

## Scope and non-goals

In scope:

- a small in-process latest-market-data cache;
- lifecycle-managed background ingestion for Lighter, Lighter Robinhood,
  Backpack, Hyperliquid, `trade_xyz`, and `entropy`;
- background REST market ingestion for Arcus;
- independent funding/hourly-context collection;
- a fixed-clock market sampler that creates the existing `MarketSnapshot`
  objects;
- deterministic readiness, freshness, reconnect, and failure behavior;
- fixture/unit tests and a real-application cadence validation.

Out of scope:

- changes to normalized model schemas or Parquet schemas;
- changes to SpreadMonitor thresholds, episode semantics, fees, or alerts;
- storage of full L2 books;
- external queues, brokers, Redis, Kafka, databases, worker frameworks, or a
  custom executor framework;
- trading, execution, credentials, or private APIs;
- undocumented Arcus WebSocket work;
- collector-specific retry frameworks or broad rate-limit redesign.

## Required invariants

### The market sample clock is unique

The aligned 10-second sampler is the only production market sample clock. The
application scheduler owns the `:00`, `:10`, `:20`, ... UTC slots. Background
I/O may update the latest-data cache at any time, but it must not delay, move,
duplicate, backfill, or otherwise influence a market sample slot.

At a slot, the sampler:

1. derives the aligned `sample_time`;
2. reads the current cache without network I/O;
3. emits snapshots only for feeds that are ready and fresh;
4. applies the resulting market batch to `RadarState` and the historical
   storage buffer;
5. returns control to the existing monitor cycle.

The sampler must not call `orderBookOrders`, Hyperliquid `l2Book` REST, a
Backpack depth request, Arcus REST, or any other network operation.

The existing no-overlap application behavior remains: if a market collection
cycle or monitor evaluation runs long, the next scheduler boundary is not
backfilled or overlapped.

### Readiness and freshness are independent gates

Every cached venue/symbol book has two distinct properties:

- `ready`: a valid local book has been built from the current connection or
  from a valid REST snapshot plus the required incremental synchronization;
- `observed_at`: the source update/REST observation time used to evaluate age.

Both gates are fail-closed:

- `ready == False` produces no `MarketSnapshot`;
- a ready book whose age is negative (future data) or exceeds the configured
  `stale_after_seconds` produces no `MarketSnapshot`.

Disconnects, reconnects, sequence gaps, invalid snapshots, and failed rebuilds
clear readiness. They must not make the last pre-disconnect book appear fresh.
`observed_at` is the actual WebSocket receive time or REST response observation
time, never the sampler time.

Readiness does not imply freshness, and freshness does not imply readiness.
Tests must exercise both gates independently.

### Market and context state application are separate

`RadarState` must expose or support separate market and context application.
Applying an hourly batch must not replace, clear, or otherwise mutate the
current market snapshot mapping. A market sampling batch remains authoritative
for market data: feeds omitted because they are not ready/fresh are absent from
that sample and are therefore not carried forward as current market data.

Funding and hourly-context batches update only their own state. The existing
hourly `sample_time` alignment and funding effective-time semantics are
unchanged.

### Metadata failure does not invalidate an independent book

A metadata refresh failure must retain an already ready and fresh local order
book. It must not remove the entire venue's market snapshots merely because a
mark/index/OI/volume refresh failed.

The exception is a metadata field that is genuinely required to establish a
valid market identity or to construct a valid snapshot for that venue. In that
case only the affected feed is omitted until the required metadata is
available; unrelated ready/fresh books remain usable. Optional normalized
fields such as mark price, index price, open interest, and volume remain `None`
when the existing model permits `None`.

## Architecture

The production data flow is:

```text
venue WS / background REST / hourly REST
              |
              v
     latest in-process market data
              |
              v
       aligned 10-second sampler
              |
              v
       CollectorBatch of MarketSnapshot
          |                    |
          v                    v
      RadarState          Parquet buffer
              |
              v
        SpreadMonitor
              |
              v
           Telegram
```

The cache is a small in-process data structure, not a message bus. It stores
only the latest complete normalized order-book view and the latest metadata
needed to build a snapshot. It never persists full L2 history.

A small shared market-data module will provide the cache and snapshot-building
operation. Venue collectors remain responsible for parsing their own protocols
and for maintaining protocol-specific local books. They publish complete,
sorted bid/ask levels to the cache; the cache calculates the existing `$1k`,
`$5k`, and `$10k` executable VWAPs using the existing VWAP logic.

The concrete collectors will have lifecycle and background responsibilities:

- `start()` creates the venue ingestion/metadata tasks;
- `stop()` cancels and awaits all tasks and closes open WebSockets;
- a synchronous cache read is used by the sampler;
- an independent hourly method collects funding and hourly context.

The existing `CollectorBatch` remains the normalized handoff type. No monitor
or alert code is imported by the collector or cache layer. Existing one-shot
collector behavior used by deterministic tests may remain as a compatibility
adapter, but the production pipeline must not use that network path for its
10-second sampler.

## Pipeline and lifecycle

`MarketDataPipeline.start()` starts all configured ingestion tasks and one
independent hourly-context task. It does not start a second market sampling
clock. `RadarApplication` continues to own the aligned 10-second scheduling
loop and calls `pipeline.collect_once()` at each boundary.

`MarketDataPipeline.collect_once()` performs no network awaits. It asks the
latest-data cache for all configured enabled markets using the current time and
the configured freshness limit, creates one aligned market batch, applies only
market state, and appends only that batch to the existing storage buffer.

The hourly task uses the existing `+60` second grace rule:

- at `HH:00:00` and before `HH:01:00`, it does not collect the hour;
- at or after `HH:01:00`, it collects the aligned `HH:00:00` context once;
- a process starting mid-hour collects the current hour immediately;
- funding `effective_time` continues to come from the venue response;
- slow hourly work cannot block or move the 10-second market clock.

Hourly collection errors are isolated per collector and reported through the
existing collector error path. A slow hourly call may take longer than one
market interval; it must not create overlapping market work or backfill market
slots.

Shutdown cancels the hourly task and all collector tasks, awaits them, closes
WebSockets, and leaves no orphan task. Startup creates no synthetic snapshot.

If the application flushes Parquet from a worker thread while a background task
appends to the in-memory storage buffer, the existing storage owner will use a
small synchronization guard around append/flush as needed. This does not alter
the Parquet layout, schema, retention, or batching behavior.

## Venue ingestion designs

### Lighter and Lighter Robinhood

The existing reusable Lighter WebSocket implementation remains the source of
order-book state:

- normal endpoint:
  `wss://mainnet.zklighter.elliot.ai/stream`;
- Robinhood endpoint:
  `wss://api.rh.lighter.xyz/stream`;
- channel:
  `order_book/<market_id>`.

The current initial snapshot plus nonce-checked delta semantics remain in use.
Zero-size updates delete levels, bids remain descending, and asks remain
ascending. A reconnect clears all books, resubscribes all configured market
IDs, and stays not-ready until new valid snapshots and updates are received.

`orderBookDetails` is used at startup to discover market IDs and in a low-
frequency metadata refresh task for mark/index/OI/volume. It is removed from
the 10-second path. A refresh failure retains the last valid details and does
not clear ready/fresh WebSocket books. A market cannot subscribe until its
market ID is known; that startup/discovery dependency is allowed to fail closed
for that market.

Funding requests and Lighter funding normalization are unchanged and run only
on the independent hourly path.

### Backpack

Backpack uses one persistent connection for all configured markets:

- endpoint: `wss://ws.backpack.exchange`;
- subscription channel: `depth.<venue_symbol>`;
- REST snapshot: the existing public `/api/v1/depth` endpoint, only for initial
  synchronization and sequence-gap recovery.

The local book follows the documented snapshot-plus-incremental protocol:

1. obtain a REST snapshot and its `lastUpdateId`;
2. ignore updates whose final ID is at or below the snapshot ID;
3. accept the first update that bridges `lastUpdateId + 1`, allowing
   `U <= lastUpdateId + 1 <= u`;
4. after synchronization, require each update to continue from the previous
   final ID (`U == previous_u + 1`);
5. apply `b` and `a` entries as absolute price-level sizes, deleting zero-size
   levels;
6. on a gap or malformed update, clear readiness and rebuild from a fresh REST
   snapshot.

Disconnect/reconnect clears every affected local book, resubscribes all
configured symbols, and rebuilds before sampling resumes. Different symbols
have independent state and sequence validation. The sampler never calls the
depth endpoint.

Backpack market metadata, mark/index, OI, and volume refresh in a low-frequency
background task. Metadata failure retains usable book state; only a missing
required active-market identity suppresses that specific feed.

### Hyperliquid, `trade_xyz`, and `entropy`

The three logical domains use one reusable Hyperliquid WebSocket feed
implementation per configured logical domain, with independent state:

- endpoint: `wss://api.hyperliquid.xyz/ws`;
- subscription method: `subscribe`;
- subscription type: `l2Book`;
- coin identity: `BTC`, `ETH`, `xyz:<symbol>`, `io:<symbol>`, or the exact
  configured venue symbol.

The official `l2Book` messages are complete book snapshots. Each valid message
atomically replaces the corresponding local book; no delta logic is introduced.
The received coin must exactly match the subscribed configured coin. A
reconnect clears readiness, resubscribes all coins for that logical domain, and
keeps each book fail-closed until a new valid complete snapshot arrives.

`metaAndAssetCtxs` remains a low-frequency background metadata refresh. It may
provide mark/oracle, OI, volume, and funding context, but a metadata failure
does not clear an independently ready/fresh L2 book. No REST `l2Book` request is
made by the 10-second sampler.

The `dex`/coin namespace is part of identity. Mainnet coins, `xyz:*`, and
`io:*` are stored under their separate logical venue/symbol mappings and are
never cross-populated.

### Arcus

Arcus remains REST-only because no documented public WebSocket contract has
been approved. A background refresh task performs the existing markets and L2
requests, without overlap within that task, and records the latest complete
book plus its actual observation time.

The 10-second sampler reads Arcus cache state only. A slow or failed Arcus
refresh can make individual symbols stale or absent, but it cannot delay the
global market clock or other venues. Existing Arcus parsing, funding, and
hourly semantics are preserved.

## State, storage, and failure behavior

The market batch is authoritative for the current `RadarState.markets` mapping.
Missing feeds are omitted rather than carried forward. Context application is a
separate operation and does not clear the market mapping.

Parquet receives the same normalized models and schemas as today. The new
ingestion layer does not persist raw L2 books or alter retention. Hourly batches
are appended when their background collection completes. A storage error is
reported using existing application behavior; it does not turn a failed venue
into a synthetic successful snapshot.

Venue failures are isolated. A failed network request, malformed message,
metadata timeout, stale book, sequence gap, or disconnected feed produces no
fresh snapshot for the affected feed. Other ready/fresh feeds continue to be
sampled. No previous snapshot is relabeled with the current sample time.

## Testing requirements

All deterministic behavior is developed test-first with fixtures and injected
clocks.

Core sampler tests must prove:

- a network operation lasting longer than 10 seconds does not move the next
  aligned sample slot;
- a sampler call performs no network request;
- aligned sample times remain unchanged;
- fresh ready data is emitted with the current aligned `sample_time` and the
  source `observed_at`;
- not-ready, stale, and future-observed data is omitted;
- hourly context application does not clear market state;
- slow hourly collection does not block a market sample.

Lighter tests must prove metadata refresh failure preserves a ready/fresh WS
book, reconnect clears readiness, all configured markets are resubscribed, and
existing snapshot/delta/VWAP behavior remains correct.

Backpack fixtures must cover initial snapshot bridging, absolute updates,
deletes, continuity, sequence gaps, REST rebuild, reconnect/resubscribe,
per-symbol isolation, and executable VWAP.

Hyperliquid fixtures must cover multiple mainnet/HIP-3 markets, complete
snapshot replacement, reconnect, invalid coin identity, and no cross-dex state
mixing.

Arcus tests must prove slow REST work is outside the sampler and stale cached
books fail closed. Hourly tests must prove the +60 second boundary and
mid-hour-start behavior.

Existing Lighter funding tests and all other collector/storage/monitor tests
must remain passing.

## Live validation

After the non-live suite passes, run the real application scheduler against the
expanded 127-feed configuration for at least 30 minutes, allowing all WS feeds
to warm up before steady-state accounting.

Report separately for warm-up and steady state:

- scheduled sample slots and produced `sample_time` values;
- missing or duplicated slots;
- overall and per-venue feed completeness;
- Lighter and Lighter Robinhood completeness;
- sampler execution durations and median/p95/max;
- observed book-age distributions;
- WS reconnect counts;
- REST timeout/429 counts by venue;
- hourly duration and whether hourly work caused a market-slot gap.

The target is zero missed steady-state market slots, at least 99% overall and
per-WS-venue completeness, at least 99% sampler executions within 10 seconds,
and no hourly-caused market gaps. If Arcus alone prevents 99% feed
completeness while the global clock remains exact, report the distinction
explicitly rather than hiding it.

## Quality gates and commit boundary

Run the full non-live pytest suite, relevant live tests, Ruff and Mypy only when
already available/configured, `python -m compileall src`, and `git diff --check`.

Do not commit unless the implementation and live result materially improve the
current latency/failure baseline. Do not push, merge, or open a pull request.

## Authoritative protocol references

- [Hyperliquid WebSocket API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
- [Backpack WebSocket API](https://docs.backpack.exchange/)
