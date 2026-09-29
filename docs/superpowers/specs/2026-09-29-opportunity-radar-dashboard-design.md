# Opportunity Radar — Read-Only Dashboard Design

**Status:** Approved design for implementation planning

## Purpose and scope

Opportunity Radar will gain a persistent, read-only web dashboard for
inspecting current executable-spread opportunities, pair history, and data
health. The dashboard is a presentation/query layer for the existing Radar
runtime. It is not a trading interface, execution system, second strategy
engine, or alternative alert lifecycle.

The production Radar process, collectors, `RadarState`, monitor evaluation,
Parquet schemas, SQLite schemas, Telegram behavior, sampling cadence, and
trigger semantics remain unchanged.

The dashboard runs as a separate process:

```text
python -m radar.dashboard
```

It reads existing files and exposes a small HTTP API plus a vanilla HTML/CSS/
JavaScript UI. No new database, frontend framework, broker, service, or
runtime dependency is introduced.

## Architecture and boundaries

```text
Parquet + SQLite + YAML
          |
          v
  DashboardQueryService
  (read-only bounded queries)
          |
          v
  HTTP API + dashboard UI
```

The dashboard is deliberately separate from `radar.app`:

- It cannot block the asyncio market sampler or monitor runner.
- It does not share mutable `RadarState` with the Radar process.
- It does not call `SpreadMonitor.evaluate()`.
- It does not create a shadow monitor, reconstruct lifecycle events, or write
  monitor state.
- It may reuse pure production calculation helpers and persisted production
  state where that preserves exact semantics.

The existing `ThreadingHTTPServer`, `DashboardStatusService`,
`create_dashboard_server`, and dashboard tests should be reused where clean.

## Source of truth and ownership

| Source | Dashboard use | Ownership |
| --- | --- | --- |
| YAML loaded through `RadarConfig` | Enabled universe, venue symbols, fees, monitor trigger configuration, stale window | Configuration source of truth |
| `data/market/date=*/part-*.parquet` | Current market rows and historical executable spread | Historical market-data source of truth; dashboard never writes |
| `runtime/radar.sqlite3` `monitor_state` | Persisted active spread episodes, alert flags, signal-duration timestamps, monitor heartbeat | Production lifecycle source of truth; dashboard opens read-only |
| `runtime/radar.sqlite3` `opportunity_log` | Recent alert/resolution lifecycle display | Production event log; dashboard opens read-only |
| `data/funding` and `data/hourly_context` | Optional pair context only when cheap and directly available | Historical context; not required for v1 |

The dashboard must not create a missing production SQLite database. SQLite
connections use a read-only URI. A missing or locked runtime database produces
an explicit degraded/unknown state rather than an empty healthy state.

## Market and pair semantics

The dashboard uses the same executable semantics as the production spread
monitor:

- Primary executable prices are `buy_10k_vwap` for the long venue and
  `sell_10k_vwap` for the short venue.
- Raw spread is `(short_sell_vwap / long_buy_vwap - 1) * 10,000`.
- A pair requires an exact common `sample_time`.
- `observed_at` is retained separately and is used for freshness and skew.
- Missing/stale/future rows, missing primary VWAP, and missing explicit fee
  data fail closed and are excluded from opportunity results.
- No interpolation, stale carry-forward, synthetic price, or look-ahead is
  permitted.

Pair identity is always the complete key:

```text
canonical_symbol
long_venue
long_venue_symbol
short_venue
short_venue_symbol
```

If a venue has multiple configured symbols for one canonical symbol, the
dashboard must not guess. The API requires exact venue symbols or returns an
ambiguous-identity error.

### Current scanner state

The scanner reads the latest valid market rows from Parquet and joins the
matching persisted production episode from SQLite when one exists.

- `data_as_of` is the latest dataset `sample_time`, not the HTTP request time
  and not the newest file modification time.
- Signal duration, candidate confirmation, and alert state come from the
  persisted production episode. The dashboard never derives a replacement
  lifecycle.
- A valid current pair without an active persisted episode has signal duration
  zero and is not an active alert.
- A malformed or unavailable episode state is displayed as unknown/degraded;
  it is never silently rebuilt by running the monitor.

The 24-hour prior-only mean/std calculation uses exact matched pair samples
strictly before the current pair sample. It preserves the production basis
requirements: full 24-hour history, at least 80% expected coverage, and the
existing prior-only population statistics. Production basis constants and
pure helpers may be reused, but `SpreadMonitor.evaluate()` is never invoked.

The display-only round-trip fee follows the existing alert payload semantics:

```text
round_trip_fee_bps = 2 * (long_fee_bps + short_fee_bps)
theoretical_edge_bps = deviation_bps - round_trip_fee_bps
```

These values must be labeled display-only and must not alter opportunity
qualification.

## HTTP API contract

All endpoints are `GET` and read-only. JSON responses use UTC ISO-8601
timestamps. Numeric values are finite numbers or `null`; no `NaN` or infinity
is emitted.

### `GET /api/status`

Preserve the existing endpoint and extend it with:

- `generated_at`
- `overall.status`: `healthy`, `degraded`, or `down`
- `radar_heartbeat` / latest persisted monitor update age
- latest market `sample_time` and age
- configured/latest/healthy feed totals
- per-venue expected, available, missing, and maximum observation age
- latest Parquet file/write observation available to the dashboard
- SQLite read status
- active episode and active alert counts
- recent persisted alert/resolution events
- `errors`

Exact collector error counters, Telegram transport health, and deployed SHA
are not available from the current persisted sources. These fields must be
`unknown`/omitted rather than inferred. Missing/stale feed counts remain
visible, including transient partial cycles such as 126 of 127 feeds.

### `GET /api/opportunities`

Supported query parameters:

```text
symbol=<canonical symbol>
long_venue=<venue>
short_venue=<venue>
max_std=<non-negative number>
min_deviation=<finite number>
min_duration=<non-negative seconds>
active_only=true|false
limit=<bounded positive integer>
```

The response contains:

```json
{
  "data_as_of": "...",
  "generated_at": "...",
  "status": "healthy|degraded|down",
  "rows": [
    {
      "canonical_symbol": "QQQ",
      "long_venue": "arcus",
      "long_venue_symbol": "QQQ-USD",
      "short_venue": "lighter_robinhood",
      "short_venue_symbol": "QQQ",
      "current_raw_spread_bps": 21.4,
      "rolling_mean_bps": 2.1,
      "rolling_std_bps": 1.4,
      "deviation_bps": 19.3,
      "signal_duration_seconds": 80,
      "round_trip_fee_bps": 4.5,
      "theoretical_edge_bps": 14.8,
      "observed_at_skew_seconds": 0.4,
      "sample_time": "...",
      "freshness_seconds": 3.2,
      "active": true,
      "alerted": false
    }
  ],
  "errors": []
}
```

Default ordering is `deviation_bps` descending. The API does not introduce a
composite opportunity score. Filtering and sorting are display-only.

### `GET /api/pair`

Required parameters:

```text
symbol=<canonical symbol>
long_venue=<venue>
short_venue=<venue>
range=1h|6h|24h|3d|7d|all
```

`long_venue_symbol` and `short_venue_symbol` are required when the configured
mapping is not unique. The response contains:

- exact pair identity
- `data_as_of`
- current raw spread
- prior-only 24h mean/std/deviation
- production signal duration
- long `$10k` buy VWAP
- short `$10k` sell VWAP
- round-trip fee
- theoretical return-to-mean edge
- observed-at skew
- freshness and sample time
- ordered historical points containing `sample_time` and raw executable spread
- optional readable prior-only mean series
- `errors` / degraded status

The selected range never changes historical statistics; any display downsampling
is presentation-only. `all` is bounded by the existing rolling 90-day Parquet
retention.

### Browser routes

The first version exposes:

- `/` and `/opportunities`: default scanner
- `/pair?...`: pair detail
- `/status`: health view

Filter and pair identity state should be query-string addressable so Telegram
links and bookmarks can later target a pair without changing the data model.
Telegram link insertion is not part of this design.

Invalid filters return a client error. Unknown or ambiguous exact pair identity
returns a client error. Storage read failures return a degraded response with
empty/partial data and an explicit error, following the existing status
endpoint's fail-safe presentation behavior.

## UI structure

### Opportunities (default page)

Top filters:

- symbol
- long venue
- short venue
- maximum 24h std
- minimum deviation
- minimum signal duration
- active only / all

Primary table columns:

- Symbol
- Long venue
- Short venue
- Current spread
- 24h mean
- 24h std
- Deviation
- Signal duration
- Round-trip fee
- Theoretical edge
- Observed skew
- Freshness

Rows link to the exact `/pair` identity. The default sort is deviation
descending.

### Pair Detail

The first version focuses on executable spread inspection:

- exact identity
- current raw spread
- prior-only 24h mean/std/deviation
- production signal duration
- long `$10k` buy VWAP
- short `$10k` sell VWAP
- round-trip fee
- theoretical edge
- observed-at skew
- freshness/sample time
- historical raw executable spread chart

The chart contains the raw spread, rolling 24-hour mean, and current point.
Standard-deviation bands are optional and may be omitted when they reduce
readability. Ranges are `1h`, `6h`, `24h`, `3d`, `7d`, and `all available`.

Funding, OI, and volume may be shown only when an existing cheap direct query
already provides them. They are not required for v1.

### Data Health / Status

Show Radar heartbeat, latest sample age, feed completeness, per-venue health,
Parquet read/write evidence, SQLite read status, active episodes, and recent
persisted events. Missing persisted error counters are clearly marked as
unavailable rather than represented as zero.

## Performance and production validation

The dashboard must not materially affect Radar's 10-second collection cycle.
The initial implementation uses only a simple bounded 5–10 second process-local
TTL cache keyed by endpoint and normalized query parameters. No cache service,
background worker framework, materialized database, or precomputed research
pipeline is designed in advance.

Queries must remain bounded:

- opportunities: recent 24-hour market partitions only
- pair detail: one exact pair and the requested range
- status: latest feed rows, SQLite state, and bounded recent events

Before accepting implementation, run a production-data benchmark on
`trading-mini` that records:

- cold `/api/opportunities` latency: at least 30 uncached requests
- cached `/api/opportunities` latency: at least 30 cache-hit requests
- pair-detail latency for representative `1h`, `24h`, and `all` ranges
- dashboard CPU and RSS memory at idle and during representative refreshes
- Radar scheduler critical-path timing during dashboard activity
- missed/duplicate 10-second sample slots and application cycle failures

Acceptance requires zero additional missed or duplicate sampler slots and no
material increase in Radar scheduler critical-path latency over a comparable
baseline. The benchmark must report p50, p95, and maximum request latency plus
CPU/RSS observations. A cache redesign or other optimization is allowed only
after this measurement demonstrates a real production bottleneck.

## Failure and degraded states

- No current market row: show unavailable/down for that feed; never carry an
  old executable value forward.
- Missing primary VWAP or fee: exclude the pair from opportunity results.
- Stale or future observation: exclude it and expose freshness/missing state.
- Parquet read error: return degraded/down status with the error; do not write
  or repair files.
- SQLite unavailable/locked: show runtime state unknown/degraded; do not create
  or mutate the database.
- Malformed persisted episode/event: ignore only the malformed item, report a
  degraded error, and do not reconstruct it by evaluating the monitor.
- Transient partial venue cycles remain visible as missing feeds, not as an
  automatic Radar outage.

## Deployment assumptions

The target is macOS `trading-mini`, accessed from the MacBook through Tailscale.

- Run a separate dashboard process with `python -m radar.dashboard`.
- Default port is `8787`.
- Add a configurable host while preserving the current loopback default for
  local development; production binds to the trading-mini Tailscale address,
  not a public interface.
- Use a macOS `LaunchDaemon` with `KeepAlive` and `RunAtLoad` so the dashboard
  survives logout and SSH disconnect.
- Use explicit absolute paths for config, data root, runtime database, Python,
  stdout, and stderr.
- Tailscale ACLs provide network access control; no dashboard authentication or
  public Internet exposure is introduced in v1.
- Radar's launch process, data paths, runtime database, and lifecycle are not
  changed by this dashboard.

The actual Tailscale IP, user identity, Python executable path, and log paths
are deployment values and must not be guessed in source control.

## Explicitly deferred

The following are outside v1:

- delayed-entry PnL UI
- MAE/MFE analytics
- full historical episode research
- historical backfill
- Google Drive sync
- automated trading
- order entry or wallet connectivity
- new health-persistence schema
- collector-error counters not already persisted
- Telegram dashboard-link changes
- new venue collectors or monitor types

## Acceptance criteria

The dashboard implementation is acceptable only when:

1. It runs as a separate read-only `python -m radar.dashboard` process.
2. Existing dashboard health behavior and tests remain passing.
3. Opportunities is the default page with the specified columns, filters, and
   deviation-descending order.
4. Pair Detail preserves complete exact pair identity and offers the specified
   ranges without interpolation, look-ahead, or stale carry.
5. SQLite remains authoritative for persisted active episode, alert, and
   signal-duration state; no monitor evaluation occurs in the dashboard.
6. `data_as_of` is explicitly the latest dataset `sample_time`.
7. Parquet and SQLite are opened/read without dashboard writes or schema
   changes.
8. Missing/stale/ambiguous data fails closed and is visible as degraded state.
9. The trading-mini production benchmark records cold/cached API latency,
   pair-detail latency, CPU, RSS, and Radar sampler impact before acceptance.
    10. Tailscale-only LaunchDaemon deployment is documented with explicit paths,
    logs, KeepAlive, and Radar isolation.
