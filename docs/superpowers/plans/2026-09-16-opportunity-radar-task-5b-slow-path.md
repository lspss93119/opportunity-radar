# Task 5B Spread Alert Slow Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Process queued spread alerts on a slow path by reconstructing exact raw executable history, rendering optional context, formatting Traditional Chinese text, and delivering through Telegram without blocking the fast asyncio loop.

**Architecture:** A synchronous `SpreadHistory.query()` reads only the two required venue identities from committed market Parquet through DuckDB. `SpreadAlertProcessor` keeps parsing and formatting inline, but calls the synchronous history query and Matplotlib renderer through `asyncio.to_thread()` before using async Telegram HTTP; `AlertWorker` consumes the existing queue and always accounts for each item.

**Tech Stack:** Python 3.13, uv, DuckDB, PyArrow/Parquet, Matplotlib `Agg`, httpx async HTTP, asyncio, pytest, pytest-asyncio.

**Spec:** `docs/superpowers/specs/2026-09-16-opportunity-radar-task-5b-slow-path-design.md`

**Implementation baseline:** `9d1c4046ff8b9f8c7359f9d23a088fb414e20d98`

## Global Constraints

- Implement Task 5B only; do not start Task 6 application wiring or live pilot work.
- Preserve `SpreadMonitor.evaluate()`, `MonitorRunner`, `AlertRequest`, Task 5A thresholds, lifecycle, fee handling, and persistence rollback semantics.
- Keep the existing `asyncio.Queue[AlertRequest]` as the fast/slow boundary.
- DuckDB and Matplotlib operations are synchronous and must run through `asyncio.to_thread()` from the async processor.
- Lightweight parsing and formatting may remain inline; Telegram uses async `httpx.AsyncClient` directly.
- Historical spread is raw executable spread only and never applies current taker fees.
- Historical long/short rows require exact `sample_time` equality, complete identity filtering, latest-`observed_at` deduplication, and `sample_time <= as_of`.
- Use only fixed executable sizes `$1,000`, `$5,000`, and `$10,000`; never interpolate or substitute another price field.
- Missing history/chart errors degrade the current alert; Telegram errors remain observable and must not kill later worker items.
- Telegram photo captions are limited to 1,024 characters: never truncate an alert; an oversized formatted message must use text-only delivery.
- Do not add pandas, a custom executor, process pool, worker pool, retry framework, broker, database server, or durable queue.
- Do not read `ParquetStorage._pending`, persist full L2, or add Task 6 scheduling/secret/application wiring.

---

### Task 1: Historical spread reconstruction

**Files:**
- Create: `src/radar/history/__init__.py`
- Create: `src/radar/history/spread.py`
- Test: `tests/test_history_spread.py`

**Interfaces:**
- Consumes: committed files at `<data_root>/market/date=*/part-*.parquet`, existing `ParquetStorage` fixtures, and explicit pair identity/size/UTC cutoff values.
- Produces: `HistoricalSpreadPoint`, `WindowStats`, `HistoricalSpreadContext`, and `SpreadHistory.query` for the processor.

- [ ] **Step 1: Write failing history tests and fixture helpers**

Create deterministic `MarketSnapshot` helpers that can set venue, venue symbol,
canonical symbol, sample time, observed time, and the three fixed VWAP pairs.
Write fixtures through the existing `ParquetStorage` and call:

```python
context = SpreadHistory(tmp_path / "data").query(
    canonical_symbol="BTC",
    long_venue="lighter",
    long_venue_symbol="BTC",
    short_venue="hyperliquid",
    short_venue_symbol="BTC",
    primary_size_usd=10_000,
    as_of=as_of,
)
```

Cover these independent behaviors in `tests/test_history_spread.py`:

```python
def test_exact_sample_time_join_uses_short_sell_over_long_buy():
    assert len(context.points_7d) == 1
    assert context.points_7d[0].sample_time == as_of
    assert context.points_7d[0].raw_spread_bps == pytest.approx(100.0)

def test_offset_sample_times_do_not_form_asof_point():
    assert context.points_7d == ()

def test_latest_observed_duplicate_is_selected_once():
    assert context.points_7d[0].raw_spread_bps == pytest.approx(expected)

def test_rows_after_as_of_are_excluded():
    assert context.stats_90d.sample_count == 1
```

Also add parametrized `$1,000/$5,000/$10,000` column-selection tests, complete
identity-filter tests, NULL/non-positive/non-finite depth tests, exact
7d/30d/90d counts and medians, empty-data tests, and a repeated query with no
fee argument to demonstrate that history has no current-fee input.

- [ ] **Step 2: Run the history tests to verify the intended failure**

Run:

```bash
uv run pytest -q tests/test_history_spread.py
```

Expected: collection fails because `radar.history.spread` and its result types
do not exist yet. Fix only test fixture typos if the failure is unrelated to
the missing implementation.

- [ ] **Step 3: Implement the minimal immutable history API**

Define:

```python
@dataclass(frozen=True)
class HistoricalSpreadPoint:
    sample_time: datetime
    raw_spread_bps: float

@dataclass(frozen=True)
class WindowStats:
    sample_count: int
    median_raw_spread_bps: float | None

@dataclass(frozen=True)
class HistoricalSpreadContext:
    points_7d: tuple[HistoricalSpreadPoint, ...]
    stats_7d: WindowStats
    stats_30d: WindowStats
    stats_90d: WindowStats

    @classmethod
    def empty(cls) -> "HistoricalSpreadContext":
        empty_stats = WindowStats(0, None)
        return cls((), empty_stats, empty_stats, empty_stats)

class SpreadHistory:
    def __init__(self, data_root: Path) -> None:
        self.data_root = Path(data_root)

    def query(
        self,
        *,
        canonical_symbol: str,
        long_venue: str,
        long_venue_symbol: str,
        short_venue: str,
        short_venue_symbol: str,
        primary_size_usd: int,
        as_of: datetime,
    ) -> HistoricalSpreadContext:
        raise NotImplementedError
```

Normalize `as_of` to UTC and reject unsupported sizes. If the exact
`market/date=*/part-*.parquet` glob has no files, return
`HistoricalSpreadContext.empty()` without opening DuckDB.

Format the selected executable column names only from this fixed mapping:

```python
VWAP_COLUMNS = {
    1_000: ("buy_1k_vwap", "sell_1k_vwap"),
    5_000: ("buy_5k_vwap", "sell_5k_vwap"),
    10_000: ("buy_10k_vwap", "sell_10k_vwap"),
}
```

Use one DuckDB query with parameterized values and a fixed-column f-string:
filter the 90-day inclusive range, `sample_time <= as_of`, both complete
identities, and either side's venue identity; use `row_number()` partitioned
by `venue, venue_symbol, canonical_symbol, sample_time` ordered by
`observed_at DESC`; then exact-join the deduplicated long and short CTEs on
`sample_time`. Convert rows to Python, drop invalid executable prices, compute
`(short_sell_vwap / long_buy_vwap - 1.0) * 10_000`, and discard non-finite
results. Do not import `SpreadMonitor`, `RadarState`, or current fees.

Build sorted valid points, calculate inclusive `[as_of - N days, as_of]`
windows with `statistics.median`, retain only 7-day points in the result, and
return `None` medians for empty windows. Keep query exceptions observable so
the processor can apply its text-only degradation policy.

- [ ] **Step 4: Run the history tests and verify all deterministic cases pass**

Run:

```bash
uv run pytest -q tests/test_history_spread.py
```

Expected: all exact-join, identity, deduplication, cutoff, size, validity,
window, median, empty-data, and fee-independence tests pass.

- [ ] **Step 5: Commit the history component**

```bash
git add src/radar/history tests/test_history_spread.py
git commit -m "Add exact historical spread reconstruction"
```

### Task 2: Alert value parsing, message formatting, and chart rendering

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `src/radar/alerts/__init__.py`
- Create: `src/radar/alerts/models.py`
- Create: `src/radar/alerts/spread.py`
- Create: `src/radar/alerts/chart.py`
- Test: `tests/test_alert_formatting.py`
- Test: `tests/test_alert_chart.py`

**Interfaces:**
- Consumes: Task 5A `AlertRequest` payloads and Task 1 `HistoricalSpreadContext`.
- Produces: `SpreadAlertDetails`, `parse_spread_alert`, `format_spread_alert`, and synchronous `render_spread_chart`.

- [ ] **Step 1: Add only the chart dependency and write failing parser/formatter/chart tests**

Add `matplotlib>=3.9,<4` to the project dependencies and regenerate
`uv.lock`. Create a deterministic alert helper using the same payload keys
already produced by `SpreadMonitor._build_alert_request()`.

Write tests such as:

```python
details = parse_spread_alert(alert)
assert details.primary_size_usd == 10_000
assert details.sample_time == sample_time
message = format_spread_alert(details, context)
assert "BTC" in message
assert "7日" in message and "30日" in message and "90日" in message
assert "Lighter" in message and "Hyperliquid" in message
```

Test missing history/funding formatting, invalid monitor/size/timestamp/number
payloads, current executable prices, raw/fee/net values, durations, and
available funding fields. Chart tests must assert valid context returns bytes
whose first eight bytes are `b"\\x89PNG\\r\\n\\x1a\\n"`, empty/one-point
context returns `None`, no chart file is created, and repeated calls leave
`matplotlib.pyplot.get_fignums()` unchanged.

- [ ] **Step 2: Run the new alert tests to verify they fail for missing code**

Run:

```bash
uv run pytest -q tests/test_alert_formatting.py tests/test_alert_chart.py
```

Expected: collection fails because the new alert modules and functions are
not implemented.

- [ ] **Step 3: Implement typed payload extraction and deterministic formatting**

Define these small frozen values in `src/radar/alerts/models.py`:

```python
@dataclass(frozen=True)
class FundingContext:
    effective_time: datetime
    observed_at: datetime
    funding_rate: float
    next_funding_time: datetime | None

@dataclass(frozen=True)
class SpreadAlertDetails:
    canonical_symbol: str
    long_venue: str
    long_venue_symbol: str
    short_venue: str
    short_venue_symbol: str
    primary_size_usd: int
    long_buy_vwap: float
    short_sell_vwap: float
    raw_spread_bps: float
    long_fee_bps: float
    short_fee_bps: float
    net_spread_bps: float
    sample_time: datetime
    candidate_duration_seconds: int
    alert_duration_seconds: int
    long_funding: FundingContext | None
    short_funding: FundingContext | None
```

Implement `parse_spread_alert(alert: AlertRequest) -> SpreadAlertDetails`
with explicit finite/positive/UTC checks and the fixed supported-size mapping.
Implement `format_spread_alert(details, context) -> str` as concise
Traditional Chinese plain text containing symbol, size, long/short venues and
prices, raw spread, both fees, net spread, durations, both funding values when
present, and all three median/count summaries. Use a deterministic
`不可用` representation for missing context; never include point arrays.

- [ ] **Step 4: Implement the headless in-memory chart renderer**

In `src/radar/alerts/chart.py`, select Matplotlib's `Agg` backend before
importing `pyplot`. Implement:

```python
def render_spread_chart(
    details: SpreadAlertDetails,
    context: HistoricalSpreadContext,
) -> bytes | None:
    raise NotImplementedError
```

Return `None` for fewer than two 7-day points. Otherwise create one figure,
plot 7-day raw spread against UTC timestamps, add current raw spread and
available 7-day/30-day median lines, annotate the 90-day median, write a PNG to
`BytesIO`, and close the figure in `finally`. Use the title
`<symbol> | Long <venue> / Short <venue> | $<size>`, UTC x-axis, and raw bps
y-axis. Do not write a persistent file or plot historical net spread.

- [ ] **Step 5: Run the parser, formatter, and chart tests**

Run:

```bash
uv run pytest -q tests/test_alert_formatting.py tests/test_alert_chart.py
```

Expected: all parser validation, message-content, missing-data, PNG, empty
history, and figure-cleanup tests pass.

- [ ] **Step 6: Commit alert formatting and charting**

```bash
git add pyproject.toml uv.lock src/radar/alerts tests/test_alert_formatting.py tests/test_alert_chart.py
git commit -m "Add spread alert formatting and chart rendering"
```

### Task 3: Async Telegram transport

**Files:**
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Create: `src/radar/alerts/telegram.py`
- Test: `tests/test_telegram.py`

**Interfaces:**
- Consumes: explicit `bot_token`, `chat_id`, text, and in-memory PNG bytes.
- Produces: `TelegramTransport.send_text()` and `TelegramTransport.send_chart()` using async HTTP only.

- [ ] **Step 1: Add httpx and write failing transport tests**

Add `httpx>=0.27,<1` and regenerate `uv.lock`. Build a fake async client and
response with `post()` and `raise_for_status()` methods. Assert the fake client
receives:

```python
await transport.send_text("alert")
assert calls[0].url.endswith("/sendMessage")
assert calls[0].data == {"chat_id": "chat", "text": "alert"}

await transport.send_chart(b"png", "caption")
assert calls[1].url.endswith("/sendPhoto")
assert calls[1].data == {"chat_id": "chat", "caption": "caption"}
assert calls[1].files["photo"][1] == b"png"
```

Also test `raise_for_status()` failure and request failure are observable and
that the bot token is absent from the resulting exception text.

- [ ] **Step 2: Run transport tests to verify the intended failure**

Run:

```bash
uv run pytest -q tests/test_telegram.py
```

Expected: collection fails because `TelegramTransport` is not implemented.

- [ ] **Step 3: Implement the transport-only client**

Define:

```python
class TelegramTransportError(RuntimeError):
    pass

class TelegramTransport:
    def __init__(
        self,
        bot_token: str,
        chat_id: str | int,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        raise NotImplementedError

    async def send_text(self, text: str) -> None:
        raise NotImplementedError

    async def send_chart(self, png: bytes, caption: str) -> None:
        raise NotImplementedError
```

Use `https://api.telegram.org/bot{bot_token}/sendMessage` and
`https://api.telegram.org/bot{bot_token}/sendPhoto`, async `client.post`, `data` for chat/text/caption, and
`files={"photo": ("spread.png", png, "image/png")}` for charts. If no client
was supplied, create and close one `httpx.AsyncClient` for the request. Call
`raise_for_status()` and convert status/transport exceptions into
`TelegramTransportError` messages containing only a generic failure and HTTP
status where available; do not chain or log the token.

- [ ] **Step 4: Run transport tests and commit**

Run:

```bash
uv run pytest -q tests/test_telegram.py
```

Expected: all fake-HTTP path, payload, status-failure, and token-redaction
tests pass.

```bash
git add pyproject.toml uv.lock src/radar/alerts/telegram.py tests/test_telegram.py
git commit -m "Add async Telegram alert transport"
```

### Task 4: Spread alert processor with blocking-work offload

**Files:**
- Modify: `src/radar/alerts/spread.py`
- Modify: `src/radar/alerts/__init__.py`
- Test: `tests/test_alert_processor.py`

**Interfaces:**
- Consumes: `AlertRequest`, `SpreadHistory`, `TelegramTransport`, and the synchronous `render_spread_chart` callable.
- Produces: `SpreadAlertProcessor.process(alert: AlertRequest) -> None`.

- [ ] **Step 1: Write failing processor tests**

Use a fake history object, fake Telegram transport, and injected synchronous
chart function. Record the thread ID inside both sync callables and compare it
with the async test's event-loop thread ID:

```python
loop_thread = threading.get_ident()

def query(**kwargs):
    history_thread_ids.append(threading.get_ident())
    return context

def render(details, context):
    chart_thread_ids.append(threading.get_ident())
    return b"png"

await processor.process(alert)
assert history_thread_ids[0] != loop_thread
assert chart_thread_ids[0] != loop_thread
```

Cover successful chart delivery, empty history text delivery, history
exception text fallback, chart exception text fallback, current payload values
remaining in the message, and non-spread request rejection. Assert the
Telegram fake receives exactly one text or chart call. Also cover Telegram
caption safety with these delivery cases:

```python
if chart_png is None:
    await telegram.send_text(message)
elif len(message) <= 1_024:
    await telegram.send_chart(chart_png, message)
else:
    await telegram.send_text(message)
```

Use one message at exactly or below 1,024 characters and one message above
1,024 characters. Assert that the first uses chart delivery, the second uses
text-only delivery, the oversized text is passed through unchanged, and
`send_chart()` is not called for the oversized message.

- [ ] **Step 2: Run processor tests to verify they fail before implementation**

Run:

```bash
uv run pytest -q tests/test_alert_processor.py
```

Expected: collection fails because `SpreadAlertProcessor` is not implemented.

- [ ] **Step 3: Implement the processor with explicit `asyncio.to_thread()` calls**

Implement:

```python
class SpreadAlertProcessor:
    def __init__(
        self,
        history: SpreadHistory,
        telegram: TelegramTransport,
        *,
        chart_renderer: Callable[
            [SpreadAlertDetails, HistoricalSpreadContext], bytes | None
        ] = render_spread_chart,
    ) -> None:
        raise NotImplementedError

    async def process(self, alert: AlertRequest) -> None:
        raise NotImplementedError
```

Parse and format inline. Call history exactly as:

```python
context = await asyncio.to_thread(
    self._history.query,
    canonical_symbol=details.canonical_symbol,
    long_venue=details.long_venue,
    long_venue_symbol=details.long_venue_symbol,
    short_venue=details.short_venue,
    short_venue_symbol=details.short_venue_symbol,
    primary_size_usd=details.primary_size_usd,
    as_of=details.sample_time,
)
```

Catch and log history exceptions using only the alert event ID, then use
`HistoricalSpreadContext.empty()`. Call the injected chart renderer through
`await asyncio.to_thread(self._chart_renderer, details, context)`, catch and
log rendering exceptions, and select delivery exactly as follows:

```python
if chart_png is None:
    await self._telegram.send_text(message)
elif len(message) <= 1_024:
    await self._telegram.send_chart(chart_png, message)
else:
    await self._telegram.send_text(message)
```

Do not truncate the formatted message to fit the caption limit. Do not catch
Telegram exceptions; let the worker observe them.

- [ ] **Step 4: Run processor tests and commit**

Run:

```bash
uv run pytest -q tests/test_alert_processor.py
```

Expected: all offload-thread, history/chart degradation, text/chart selection,
and payload-preservation tests pass.

```bash
git add src/radar/alerts/spread.py src/radar/alerts/__init__.py tests/test_alert_processor.py
git commit -m "Add offloaded spread alert processor"
```

### Task 5: Queue alert worker

**Files:**
- Create: `src/radar/alerts/worker.py`
- Modify: `src/radar/alerts/__init__.py`
- Test: `tests/test_alert_worker.py`

**Interfaces:**
- Consumes: `asyncio.Queue[AlertRequest]`, `SpreadAlertProcessor`, and an optional `Callable[[AlertRequest, Exception], None]` error hook.
- Produces: `AlertWorker.run_once()` and `AlertWorker.run_forever()` with failure continuation and queue accounting.

- [ ] **Step 1: Write failing worker tests**

Create a fake processor that records alerts and optionally raises. Enqueue a
successful request, run `await worker.run_once()`, and assert `await
queue.join()` completes and the request was processed. Enqueue a failing request
followed by a successful request, run `run_once()` twice, and assert:

```python
assert failures == [(failed_alert, processor.error)]
assert processor.processed == [failed_alert, succeeding_alert]
assert queue.unfinished_tasks == 0
```

Also test that a processor exception does not escape `run_once()`, the error
hook exception is isolated, and a non-spread parse failure still calls
`task_done()`.

- [ ] **Step 2: Run worker tests to verify the intended failure**

Run:

```bash
uv run pytest -q tests/test_alert_worker.py
```

Expected: collection fails because `AlertWorker` is not implemented.

- [ ] **Step 3: Implement the minimal worker loop**

Define:

```python
class AlertWorker:
    def __init__(
        self,
        queue: asyncio.Queue[AlertRequest],
        processor: SpreadAlertProcessor,
        *,
        error_handler: Callable[[AlertRequest, Exception], None] | None = None,
    ) -> None:
        raise NotImplementedError

    async def run_once(self) -> None:
        raise NotImplementedError

    async def run_forever(self) -> None:
        raise NotImplementedError
```

`run_once()` must await `queue.get()`, call the processor, report exceptions
through the hook or logger, and call `queue.task_done()` in `finally`. If an
error hook raises, log that secondary error and continue. `run_forever()` is a
simple `while True: await self.run_once()` loop with no retry or shutdown
framework.

- [ ] **Step 4: Run worker tests and commit**

Run:

```bash
uv run pytest -q tests/test_alert_worker.py
```

Expected: success/failure continuation, error observation, and
`queue.task_done()` tests pass.

```bash
git add src/radar/alerts/worker.py src/radar/alerts/__init__.py tests/test_alert_worker.py
git commit -m "Add resilient spread alert queue worker"
```

### Final Task 5B verification: full suite and scope audit

**Files:**
- No implementation files; inspect the complete Task 5B diff and repository state.

**Interfaces:**
- Consumes: all Task 5B modules and tests.
- Produces: verified Task 5B checkpoint with no Task 6 changes.

- [ ] **Step 1: Run the complete non-live test suite**

Run:

```bash
uv run pytest -m "not live"
```

Expected: all existing Task 5A tests and all new Task 5B tests pass; live
collector tests remain deselected.

- [ ] **Step 2: Run static and compile checks**

Run each command completely:

```bash
uv run ruff check .
uv run mypy src
uv run python -m compileall src
git diff --check
```

Expected: Ruff reports no violations, Mypy reports no source errors,
Compileall exits zero, and diff check produces no output.

- [ ] **Step 3: Audit the fast/slow boundary and Task 6 exclusion**

Run:

```bash
rg -n "DuckDB|read_parquet|matplotlib|pyplot|httpx|Telegram|to_thread" src/radar/monitors src/radar/monitors/runner.py
git diff --name-only 9d1c4046ff8b9f8c7359f9d23a088fb414e20d98..HEAD
git status --short --branch
```

Confirm no Task 5B slow-path import or call was added to
`SpreadMonitor.evaluate()`/`MonitorRunner`, every DuckDB/Matplotlib processor
call is behind `asyncio.to_thread()`, Telegram remains async HTTP, no secret or
live Telegram test exists, and no Task 6 application file was changed.

- [ ] **Step 4: Commit any remaining Task 5B checkpoint changes**

After the full verification is green and the scope audit is clean, inspect
`git status`. Only if Task 5B changes remain uncommitted, stage those
Task-5B-only files and create the checkpoint commit:

```bash
git add src tests pyproject.toml uv.lock
git commit -m "Implement Task 5B spread alert slow path"
```

If the working tree is already clean, do not create an empty checkpoint
commit.

Report the starting SHA, all Task 5B commit SHAs, final SHA, files changed,
historical query semantics, offload behavior, Telegram endpoints, tests and
results, design deviations, known limitations, and final `git status`.
