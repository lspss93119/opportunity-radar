from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3  # noqa: F401 - preserved module attribute for compatibility tests
from collections.abc import Callable
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

from radar.config import RadarConfig, load_config
from radar.dashboard_data import (
    DashboardQueryService,
    MAX_OPPORTUNITY_LIMIT,
    OpportunitiesFilters,
    classify_heartbeat as _classify_heartbeat,
    utc_now,
)

DEFAULT_DATA_ROOT = Path("data")
DEFAULT_RUNTIME_DB = Path("runtime/radar.sqlite3")


def classify_heartbeat(age_seconds: float | None) -> str:
    """Preserve the original dashboard health helper import path."""
    return _classify_heartbeat(age_seconds)


class DashboardStatusService:
    """Compatibility facade over the read-only dashboard query service."""

    def __init__(
        self,
        config: RadarConfig,
        *,
        data_root: Path = DEFAULT_DATA_ROOT,
        runtime_db: Path = DEFAULT_RUNTIME_DB,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config
        self.data_root = Path(data_root)
        self.runtime_db = Path(runtime_db)
        self._clock = clock
        self._query_service = DashboardQueryService(
            config,
            data_root=self.data_root,
            runtime_db=self.runtime_db,
            clock=clock,
            cache_ttl_seconds=0.0,
        )

    def get_status(self, *, now: datetime | None = None) -> dict[str, object]:
        return self._query_service.get_status(now=now)

    def get_opportunities(
        self, filters: OpportunitiesFilters, *, now: datetime | None = None
    ) -> dict[str, object]:
        return self._query_service.get_opportunities(filters, now=now)


def parse_opportunities_filters(query: str) -> OpportunitiesFilters:
    """Reject malformed/duplicate input before accessing persisted sources."""
    allowed = {"symbol", "long_venue", "short_venue", "max_std", "min_deviation",
               "min_duration", "active_only", "limit"}
    if re.search(r"%(?![0-9a-fA-F]{2})", query):
        raise ValueError("invalid percent encoding")
    values: dict[str, str] = {}
    for name, value in parse_qsl(
        query, keep_blank_values=True, strict_parsing=True,
        errors="strict", max_num_fields=16,
    ):
        if name not in allowed:
            raise ValueError(f"unknown filter: {name}")
        if name in values:
            raise ValueError(f"duplicate filter: {name}")
        value = value.strip()
        if not value or any(ord(character) < 32 for character in value):
            raise ValueError(f"{name} must be non-empty text")
        values[name] = value

    numbers: dict[str, float] = {}
    for name in ("max_std", "min_deviation"):
        if name in values:
            if not re.fullmatch(r"[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", values[name], re.ASCII):
                raise ValueError(f"{name} must be a finite non-negative number")
            number = float(values[name])
            if not math.isfinite(number):
                raise ValueError(f"{name} must be finite")
            numbers[name] = number
    integers: dict[str, int] = {}
    for name in ("min_duration", "limit"):
        if name in values:
            if not re.fullmatch(r"[0-9]+", values[name]):
                raise ValueError(f"{name} must be a non-negative integer")
            integers[name] = int(values[name])
    limit = integers.get("limit", MAX_OPPORTUNITY_LIMIT)
    if not 1 <= limit <= MAX_OPPORTUNITY_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_OPPORTUNITY_LIMIT}")
    active_only = values.get("active_only", "false")
    if active_only not in ("true", "false"):
        raise ValueError("active_only must be true or false")
    return OpportunitiesFilters(
        symbol=values["symbol"].upper() if "symbol" in values else None,
        long_venue=values["long_venue"].lower() if "long_venue" in values else None,
        short_venue=values["short_venue"].lower() if "short_venue" in values else None,
        max_std_bps=numbers.get("max_std"),
        min_deviation_bps=numbers.get("min_deviation"),
        min_duration_seconds=integers.get("min_duration"),
        active_only=active_only == "true", limit=limit,
    )


HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Opportunity Radar</title>
<style>
:root { color-scheme: dark; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #111827; color: #e5e7eb; }
* { box-sizing: border-box; }
body { margin: 0; }
main { max-width: 1440px; margin: auto; padding: 24px; }
h1 { margin: 0; font-size: 1.2rem; letter-spacing: .06em; }
h2 { margin: 0 0 12px; font-size: .95rem; }
header, nav, .summary { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }
header { justify-content: space-between; }
a { color: #93c5fd; text-underline-offset: 3px; }
nav a { padding: 8px 12px; border-radius: 6px; }
nav a[aria-current="page"] { background: #374151; color: #fff; }
.muted, .metric-label { color: #94a3b8; font-size: .8rem; }
.metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 18px; }
.card { background: #1f2937; border: 1px solid #374151; border-radius: 8px; padding: 14px; overflow-wrap: anywhere; }
.metric-value { margin-top: 6px; font-size: 1rem; }
#filters { display: flex; gap: 10px; align-items: end; flex-wrap: wrap; margin-bottom: 16px; }
label { display: grid; gap: 5px; font-size: .78rem; color: #cbd5e1; }
input, button { font: inherit; color: #e5e7eb; background: #1f2937; border: 1px solid #4b5563; border-radius: 5px; padding: 8px; }
input { width: 130px; }
input[type="checkbox"] { width: auto; }
.check { display: flex; align-items: center; padding: 8px 0; }
button { cursor: pointer; }
a:focus-visible, input:focus-visible, button:focus-visible { outline: 2px solid #93c5fd; outline-offset: 3px; }
.section { min-width: 0; margin-bottom: 20px; }
.table-wrap { overflow-x: auto; border: 1px solid #374151; border-radius: 8px; }
table { width: 100%; border-collapse: collapse; background: #1f2937; font-size: .8rem; }
th, td { padding: 10px; text-align: left; border-bottom: 1px solid #374151; white-space: nowrap; font-variant-numeric: tabular-nums; }
th { color: #cbd5e1; font-size: .74rem; }
tr:last-child td { border-bottom: 0; }
td small { display: block; color: #94a3b8; margin-top: 3px; }
.healthy { color: #6ee7b7; }
.degraded { color: #fcd34d; }
.down, .error { color: #fca5a5; }
.error { margin: 8px 0; overflow-wrap: anywhere; font-size: .82rem; }
[data-view="opportunities"] #status-view, [data-view="status"] #opportunities-view { display: none; }
@media(max-width: 760px) { main { padding: 14px; } #filters label:not(.check) { flex: 1 1 130px; } input { width: 100%; } }
</style>
</head>
<body data-view="__VIEW__">
<main>
<header><h1>OPPORTUNITY RADAR</h1><span class="muted">Read-only · $10k executable VWAP</span></header>
<nav aria-label="Dashboard"><a id="nav-opportunities" href="/opportunities">Opportunities</a><a id="nav-status" href="/status">Status</a></nav>
<div class="summary" aria-live="polite"><span id="overall">Loading…</span><span id="data-as-of" class="muted">Data as of: Unavailable</span><span id="refreshed" class="muted"></span></div>
<div id="errors" role="alert"></div>
<section id="opportunities-view">
<form id="filters">
<label>Symbol<input id="symbol" name="symbol" placeholder="All symbols"></label>
<label>Long venue<input id="long_venue" name="long_venue" placeholder="All venues"></label>
<label>Short venue<input id="short_venue" name="short_venue" placeholder="All venues"></label>
<label>Max 24h std (bps)<input id="max_std" name="max_std" type="number" min="0" step="any"></label>
<label>Min deviation (bps)<input id="min_deviation" name="min_deviation" type="number" min="0" step="any"></label>
<label>Min duration (s)<input id="min_duration" name="min_duration" type="number" min="0" step="1"></label>
<label>Limit<input id="limit" name="limit" type="number" min="1" max="200" step="1" placeholder="200"></label>
<label class="check"><input id="active_only" name="active_only" type="checkbox">Active only</label>
<button type="submit">Apply filters</button><button id="reset" type="button">Reset</button>
</form>
<p class="muted">Deviation descending · RT Fee and Theo Edge are display-only · Duration comes from persisted SQLite episodes. Pair detail is available in Phase 3.</p>
<div class="table-wrap"><table aria-label="Opportunities"><thead><tr><th>Symbol</th><th>Long</th><th>Short</th><th>Spread</th><th>24h Mean</th><th>24h Std</th><th>Deviation</th><th>Duration</th><th>RT Fee</th><th>Theo Edge</th><th>Skew</th><th>Freshness</th></tr></thead><tbody id="opportunity-rows"><tr><td colspan="12" class="muted">Loading…</td></tr></tbody></table></div>
</section>
<section id="status-view">
<div class="metrics">
<div class="card"><div class="metric-label">Radar heartbeat</div><div id="heartbeat" class="metric-value">Unavailable</div></div>
<div class="card"><div class="metric-label">Latest sample / age</div><div id="sample" class="metric-value">Unavailable</div></div>
<div class="card"><div class="metric-label">Latest / configured feeds</div><div id="feeds" class="metric-value">Unavailable</div></div>
<div class="card"><div class="metric-label">Healthy / primary VWAP ready</div><div id="primary" class="metric-value">Unavailable</div></div>
<div class="card"><div class="metric-label">Parquet read / file evidence</div><div id="parquet" class="metric-value">Unavailable</div></div>
<div class="card"><div class="metric-label">SQLite read status</div><div id="sqlite" class="metric-value">Unavailable</div></div>
<div class="card"><div class="metric-label">Active / confirmed / alerted episodes</div><div id="episodes-count" class="metric-value">Unavailable</div></div>
</div>
<section class="section"><h2>Venues — missing counts refer to the latest dataset sample</h2><div class="table-wrap"><table><thead><tr><th>Venue</th><th>Expected</th><th>Available</th><th>Latest</th><th>Missing</th><th>Max age</th></tr></thead><tbody id="venues"></tbody></table></div></section>
<section class="section"><h2>Market feeds</h2><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Venue</th><th>Venue symbol</th><th>Status</th><th>Age</th><th>VWAP</th></tr></thead><tbody id="feed-rows"></tbody></table></div></section>
<section class="section"><h2>Active episodes — persisted production state</h2><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Long</th><th>Short</th><th>Net bps</th><th>Confirmed</th><th>Alerted</th></tr></thead><tbody id="episode-rows"></tbody></table></div></section>
<section class="section"><h2>Recent events</h2><div class="table-wrap"><table><thead><tr><th>Time</th><th>Symbol</th><th>Type</th><th>Net bps</th></tr></thead><tbody id="event-rows"></tbody></table></div></section>
<p id="unavailable" class="muted">Unavailable from persisted sources: deployed SHA, Telegram transport health, collector error counters.</p>
</section>
</main>
<script>
const byId = id => document.getElementById(id);
const esc = value => String(value ?? 'Unavailable').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const number = value => typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : 'Unavailable';
const age = value => typeof value !== 'number' || !Number.isFinite(value) ? 'Unavailable' : value < 60 ? Math.round(value) + 's' : Math.floor(value / 60) + 'm ' + Math.round(value % 60) + 's';
const statusClass = value => ['healthy','degraded','down'].includes(value) ? value : 'degraded';
const text = (id, value) => { byId(id).textContent = value ?? 'Unavailable'; };
const emptyRow = (columns, message) => '<tr><td colspan="' + columns + '" class="muted">' + esc(message) + '</td></tr>';
const cell = value => '<td>' + esc(value) + '</td>';
const filterNames = ['symbol','long_venue','short_venue','max_std','min_deviation','min_duration','limit'];
const isStatus = location.pathname === '/status';
byId(isStatus ? 'nav-status' : 'nav-opportunities').setAttribute?.('aria-current', 'page');
function restoreFilters() {
  const query = new URLSearchParams(location.search);
  for (const name of filterNames) byId(name).value = query.get(name) ?? '';
  byId('active_only').checked = query.get('active_only') === 'true';
}
function filterQuery() {
  const query = new URLSearchParams();
  for (const name of filterNames) {
    let value = byId(name).value.trim();
    if (name === 'symbol') value = value.toUpperCase();
    if (name.endsWith('_venue')) value = value.toLowerCase();
    if (value) query.set(name, value);
  }
  if (byId('active_only').checked) query.set('active_only', 'true');
  return query;
}
function pairLink(row) {
  const fields = ['canonical_symbol','long_venue','long_venue_symbol','short_venue','short_venue_symbol'];
  if (!fields.every(name => typeof row[name] === 'string' && row[name].trim())) return null;
  const query = new URLSearchParams();
  for (const name of fields) query.set(name === 'canonical_symbol' ? 'symbol' : name, row[name]);
  return '/pair?' + query.toString();
}
function pairCell(row, symbolField) {
  const link = pairLink(row);
  const symbol = esc(row[symbolField]);
  return link ? '<td><a href="' + esc(link) + '">' + symbol + '</a></td>' : '<td>' + symbol + '<small>Exact identity unavailable</small></td>';
}
function renderOpportunities(data) {
  byId('opportunity-rows').innerHTML = (data.rows || []).map(row => '<tr>' +
    pairCell(row, 'canonical_symbol') +
    '<td>' + esc(row.long_venue) + '<small>' + esc(row.long_venue_symbol) + '</small></td>' +
    '<td>' + esc(row.short_venue) + '<small>' + esc(row.short_venue_symbol) + '</small></td>' +
    [row.current_raw_spread_bps,row.rolling_mean_bps,row.rolling_std_bps,row.deviation_bps].map(value => cell(number(value))).join('') +
    cell(age(row.signal_duration_seconds)) + cell(number(row.round_trip_fee_bps)) + cell(number(row.theoretical_edge_bps)) +
    cell(age(row.observed_at_skew_seconds)) + '<td title="' + esc(row.sample_time) + '">' + esc(age(row.freshness_seconds)) + '</td></tr>'
  ).join('') || emptyRow(12, data.status === 'healthy' ? 'No matching opportunities' : 'No matching rows — degraded or unavailable data');
}
function renderStatus(data) {
  const overall = data.overall || {}, heartbeat = data.heartbeat || {};
  text('heartbeat', (heartbeat.status ?? 'Unavailable') + ' · ' + age(heartbeat.age_seconds) + ' · ' + (heartbeat.updated_at ?? 'Unavailable'));
  text('sample', (data.data_as_of ?? 'Unavailable') + ' · ' + age(data.sample_age_seconds));
  text('feeds', (overall.latest_feeds ?? 'Unavailable') + ' / ' + (overall.configured_feeds ?? 'Unavailable'));
  text('primary', (overall.healthy_feeds ?? 'Unavailable') + ' / ' + (overall.primary_vwap_ready ?? 'Unavailable'));
  const parquet = data.parquet || {};
  text('parquet', (parquet.status ?? 'Unavailable') + ' · files: ' + (parquet.files_considered ?? 'Unavailable') + ' · latest file mtime: ' + (parquet.latest_file_mtime ?? 'Unavailable'));
  text('sqlite', data.sqlite?.status);
  text('episodes-count', [overall.active_episodes,overall.confirmed_candidates,overall.active_alerted_episodes].map(value => value ?? 'Unavailable').join(' / '));
  byId('venues').innerHTML = Object.entries(data.venues || {}).map(([venue, value]) => '<tr>' +
    [venue,value.expected,value.available,value.latest,value.missing,age(value.max_observation_age_seconds)].map(cell).join('') + '</tr>').join('') || emptyRow(6, 'Venue evidence unavailable');
  byId('feed-rows').innerHTML = (data.feeds || []).map(feed => '<tr>' +
    [feed.canonical_symbol,feed.venue,feed.venue_symbol].map(cell).join('') + '<td class="' + statusClass(feed.status) + '">' + esc(feed.status) + '</td>' +
    cell(age(feed.age_seconds)) + cell((feed.vwap_available ?? 'Unavailable') + '/' + (feed.vwap_total ?? 'Unavailable')) + '</tr>').join('') || emptyRow(6, 'No feed evidence');
  byId('episode-rows').innerHTML = (data.episodes || []).map(row => '<tr>' + pairCell({...row,canonical_symbol:row.symbol}, 'symbol') +
    cell((row.long_venue ?? 'Unavailable') + ' · ' + (row.long_venue_symbol ?? 'Unavailable')) +
    cell((row.short_venue ?? 'Unavailable') + ' · ' + (row.short_venue_symbol ?? 'Unavailable')) +
    cell(number(row.net_spread_bps)) + cell(row.candidate_confirmed ? 'yes' : 'no') + cell(row.alerted ? 'yes' : 'no') + '</tr>').join('') ||
    emptyRow(6, data.sqlite?.status === 'healthy' ? 'No active episodes' : 'Episodes unavailable');
  byId('event-rows').innerHTML = (data.recent_events || []).map(row => '<tr>' +
    [row.occurred_at,row.symbol,row.event_type,number(row.net_spread_bps)].map(cell).join('') + '</tr>').join('') ||
    emptyRow(4, data.sqlite?.status === 'healthy' ? 'No recent events' : 'Events unavailable');
}
let requestSequence = 0;
async function refresh(query = new URLSearchParams(location.search)) {
  const sequence = ++requestSequence;
  try {
    const response = await fetch(isStatus ? '/api/status' : '/api/opportunities?' + query.toString(), {cache:'no-store'});
    const data = await response.json();
    if (sequence !== requestSequence) return;
    const status = isStatus ? data.overall?.status : data.status;
    text('overall', (status ?? 'degraded').toUpperCase());
    byId('overall').className = statusClass(status);
    text('data-as-of', 'Data as of: ' + (data.data_as_of ?? 'Unavailable'));
    text('refreshed', 'Fetched ' + new Date().toLocaleTimeString() + ' · Generated ' + (data.generated_at ?? 'Unavailable'));
    byId('errors').innerHTML = (data.errors || []).map(error => '<div class="error">' + esc(error) + '</div>').join('');
    if (isStatus) renderStatus(data); else renderOpportunities(data);
  } catch (error) {
    if (sequence !== requestSequence) return;
    text('overall', 'DEGRADED — dashboard request failed');
    byId('overall').className = 'degraded';
    text('data-as-of', 'Data as of: Unavailable');
    text('refreshed', 'Request failed at ' + new Date().toLocaleTimeString());
    byId('errors').innerHTML = '<div class="error">' + esc(error) + '</div>';
    if (isStatus) renderStatus({}); else byId('opportunity-rows').innerHTML = emptyRow(12, 'Data unavailable');
  }
}
restoreFilters();
byId('filters').addEventListener('submit', event => {
  event.preventDefault();
  const query = filterQuery();
  history.replaceState(null, '', location.pathname + (query.size ? '?' + query.toString() : ''));
  refresh(query);
});
byId('reset').addEventListener('click', () => {
  for (const name of filterNames) byId(name).value = '';
  byId('active_only').checked = false;
  history.replaceState(null, '', location.pathname);
  refresh(new URLSearchParams());
});
window.addEventListener('popstate', () => { restoreFilters(); refresh(); });
refresh();
setInterval(() => refresh(), 10000);
</script>
</body>
</html>"""


def _handler_for(
    service: DashboardStatusService | DashboardQueryService,
) -> type[BaseHTTPRequestHandler]:
    class DashboardRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            request = urlparse(self.path)
            path = request.path
            if path in ("/", "/opportunities", "/status"):
                view = "status" if path == "/status" else "opportunities"
                self._send(200, "text/html; charset=utf-8", HTML.replace("__VIEW__", view).encode("utf-8"))
                return
            if path in ("/api/status", "/api/opportunities"):
                filters = None
                if path == "/api/opportunities":
                    try:
                        filters = parse_opportunities_filters(request.query)
                    except ValueError as error:
                        self._json(400, {"status": "degraded", "data_as_of": None, "rows": [], "errors": [str(error)]})
                        return
                try:
                    payload = service.get_status() if filters is None else service.get_opportunities(filters)
                    # A failed storage read is a dashboard degradation, never
                    # evidence that Radar itself is down. Status retains the
                    # heartbeat-authoritative compatibility contract.
                    if filters is not None and payload.get("errors") and payload.get("status") == "down":
                        payload = {**payload, "status": "degraded"}
                    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
                except Exception as error:  # noqa: BLE001
                    self._json(200, {
                        "generated_at": utc_now().isoformat(),
                        "data_as_of": None,
                        "status": "degraded",
                        "overall": {"status": "degraded"},
                        "feeds": [], "rows": [], "episodes": [], "recent_events": [],
                        "errors": [f"dashboard query failed: {type(error).__name__}: {error}"],
                    })
                    return
                self._send(200, "application/json; charset=utf-8", body)
                return
            if path.startswith("/api/"):
                self._json(404, {"errors": ["unknown API path"]})
                return
            self.send_error(404)

        def _json(self, status: int, payload: dict[str, object]) -> None:
            self._send(status, "application/json; charset=utf-8",
                       json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8"))

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return DashboardRequestHandler


def create_dashboard_server(
    service: DashboardStatusService | DashboardQueryService,
    *,
    port: int,
) -> ThreadingHTTPServer:
    return ThreadingHTTPServer(("127.0.0.1", port), _handler_for(service))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Opportunity Radar dashboard")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8787)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    server = create_dashboard_server(DashboardStatusService(config), port=args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
