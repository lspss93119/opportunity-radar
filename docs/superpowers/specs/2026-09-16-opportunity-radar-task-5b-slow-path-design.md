# Opportunity Radar Task 5B — Spread Alert Slow Path Design

## Scope

Task 5B handles work after a `SpreadMonitor` alert crosses the existing
`asyncio.Queue[AlertRequest]` boundary. It reconstructs historical raw
executable spread, calculates compact historical context, optionally renders
a chart, formats a Traditional Chinese alert, and sends it through Telegram.

Task 5A remains responsible for current-data validation, thresholds, episode
lifecycle, persistence, and creation of the immutable `AlertRequest`. Task 6
application wiring, secret loading, live Telegram checks, scheduling, and
production operations are out of scope.

## Design goals and boundaries

The slow path is split into small components with explicit data flow:

```text
AlertRequest
    |
    v
SpreadAlertProcessor
    |
    +--> SpreadHistory (DuckDB / committed Parquet)
    +--> chart renderer (in-memory PNG)
    +--> message formatter
    +--> TelegramTransport

asyncio.Queue[AlertRequest] --> AlertWorker --> SpreadAlertProcessor
```

`SpreadMonitor.evaluate()` and `MonitorRunner` are not changed. No slow
historical query, chart rendering, Telegram HTTP request, or filesystem-heavy
work is added to the fast path. The slow path does not access `RadarState`,
SQLite episode state, current fees for historical calculations, or any
unflushed Parquet buffer.

## Historical spread reconstruction

`src/radar/history/spread.py` provides a focused history reader. It accepts a
`data_root`, canonical symbol, both venue/symbol identities, the supported
primary executable size, and an explicit UTC `as_of` timestamp. It does not
accept a monitor, state object, fee map, or Telegram dependency.

The reader queries only:

```text
<data_root>/market/date=*/part-*.parquet
```

The query filters both sides by `canonical_symbol`, venue, venue symbol, and
`sample_time <= as_of`, with a lower bound of `as_of - 90 days`. Each side is
deduplicated with `row_number()` over its full identity and `sample_time`,
ordered by `observed_at DESC`. The deduplicated sides are joined only on exact
`sample_time`; there is no ASOF join, tolerance, interpolation, or fill.

The executable columns are selected only from the fixed size mapping:

```text
1000  -> buy_1k_vwap / sell_1k_vwap
5000  -> buy_5k_vwap / sell_5k_vwap
10000 -> buy_10k_vwap / sell_10k_vwap
```

For each valid joined point, the raw spread is:

```text
(short_sell_vwap / long_buy_vwap - 1.0) * 10_000
```

NULL, non-positive, and non-finite executable values are dropped. Current
taker fees never enter the historical query or calculation.

The reader returns immutable structures:

```text
HistoricalSpreadPoint(sample_time, raw_spread_bps)
WindowStats(sample_count, median_raw_spread_bps)
HistoricalSpreadContext(points_7d, stats_7d, stats_30d, stats_90d)
```

The 7-day point series is the inclusive window
`[as_of - 7 days, as_of]` and is the only series retained for charting. The
7-day, 30-day, and 90-day statistics use the corresponding inclusive windows
and contain only exact joined valid points. Missing files or matching rows
produce an empty context. A DuckDB/query failure is allowed to propagate to
the processor, which degrades the alert to current-data text.

## Alert parsing, formatting, and charting

`src/radar/alerts/spread.py` parses the JSON-compatible payload of a
`SpreadMonitor` `AlertRequest` into a small typed spread-alert value. It
validates the monitor name, supported executable size, timestamps, current
prices, spread/fee numbers, identities, durations, and optional funding
payload. It also formats the current alert and all three historical summary
windows as concise Traditional Chinese plain text. Missing funding or history
is rendered as unavailable, and no historical arrays are included.

`src/radar/alerts/chart.py` uses Matplotlib's `Agg` backend. It accepts the
parsed current alert and `HistoricalSpreadContext`, plots the 7-day raw spread
points, and adds current raw spread plus available 7-day and 30-day medians.
The 90-day median is a text annotation. It returns PNG bytes from `BytesIO`,
returns `None` when fewer than two useful points exist, and always closes the
figure. It writes no required persistent chart file.

## Telegram transport

`src/radar/alerts/telegram.py` contains `TelegramTransport`, which receives
`bot_token` and `chat_id` explicitly and has a simple injectable async HTTP
client seam for tests. It calls:

```text
POST https://api.telegram.org/bot<token>/sendMessage
POST https://api.telegram.org/bot<token>/sendPhoto
```

Text alerts use `sendMessage`; chart alerts use `sendPhoto` with an in-memory
PNG multipart payload and the same formatted caption. HTTP status failures and
transport failures raise an observable transport exception whose formatted
message does not contain the bot token. The transport performs no history,
spread, state, or episode work and never logs credentials.

## Processor and worker behavior

`SpreadAlertProcessor` accepts a `SpreadHistory`, `TelegramTransport`, and an
optional chart-rendering function for deterministic tests. It processes only
`AlertRequest.monitor == "spread"`:

1. parse the current alert payload;
2. query history using the payload's `sample_time` as `as_of`;
3. if history fails, log the failure and use an empty context;
4. try to render a chart;
5. if rendering fails or returns no chart, send the current alert as text;
6. otherwise send the chart with the formatted caption.

Telegram failures are not swallowed by the processor. The worker observes
them and continues with later queue items.

`src/radar/alerts/worker.py` provides `AlertWorker` around the existing
`asyncio.Queue[AlertRequest]`. `run_once()` gets one item, calls the processor,
reports any exception through a simple error hook/log, and calls
`queue.task_done()` in a `finally` block. `run_forever()` repeatedly calls
`run_once()`. There is no retry queue, durable broker, scheduler subsystem, or
background framework; an in-memory queue item may be lost on process crash.

## Dependencies and tests

Add only the required bounded dependencies: Matplotlib and `httpx`, keeping
the existing DuckDB/PyArrow stack. No pandas or database/server dependency is
added.

Tests use temporary Parquet fixtures written with the existing stable market
schema and fake HTTP clients. They cover exact joins, identity filtering,
duplicate selection, no-lookahead, fixed VWAP-size mapping, invalid depth,
window counts/medians, empty history, fee independence, PNG generation and
cleanup, deterministic message content, Telegram request paths and sanitized
failures, processor degradation, worker continuation, and queue accounting.
All tests are non-live; no Telegram live test or Task 6 application test is
added.

