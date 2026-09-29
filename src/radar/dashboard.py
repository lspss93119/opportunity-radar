from __future__ import annotations

import argparse
import json
import sqlite3  # noqa: F401 - preserved module attribute for compatibility tests
from collections.abc import Callable
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from radar.config import RadarConfig, load_config
from radar.dashboard_data import (
    DashboardQueryService,
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


HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Opportunity Radar</title>
<style>
:root { color-scheme: dark; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #111827; color: #e5e7eb; }
* { box-sizing: border-box; }
body { margin: 0; background: #111827; }
main { max-width: 1280px; margin: 0 auto; padding: 24px; }
h1 { margin: 0; font-size: 1.25rem; letter-spacing: .08em; }
h2 { margin: 0 0 12px; font-size: .95rem; color: #cbd5e1; }
.topline, .metrics, .grid { display: grid; gap: 12px; }
.topline { grid-template-columns: 1fr auto; align-items: center; margin-bottom: 18px; }
.metrics { grid-template-columns: repeat(auto-fit, minmax(135px, 1fr)); margin-bottom: 18px; }
.card, table { background: #1f2937; border: 1px solid #374151; border-radius: 8px; }
.card { padding: 14px; }
.metric-label { color: #94a3b8; font-size: .72rem; text-transform: uppercase; letter-spacing: .06em; }
.metric-value { margin-top: 5px; font-size: 1.15rem; font-weight: 650; }
.grid { grid-template-columns: minmax(0, 1.4fr) minmax(300px, 1fr); }
.section { min-width: 0; margin-bottom: 18px; }
table { width: 100%; border-collapse: separate; border-spacing: 0; overflow: hidden; font-size: .82rem; }
th, td { padding: 9px 10px; border-bottom: 1px solid #374151; text-align: left; white-space: nowrap; }
th { color: #94a3b8; font-size: .7rem; text-transform: uppercase; letter-spacing: .04em; }
tr:last-child td { border-bottom: 0; }
.dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 6px; background: #64748b; }
.healthy .dot, .dot.healthy { background: #34d399; }
.degraded .dot, .dot.degraded { background: #fbbf24; }
.down .dot, .dot.down { background: #f87171; }
.muted { color: #94a3b8; }
.error { color: #fca5a5; margin: 8px 0 0; font-size: .8rem; }
@media (max-width: 760px) { main { padding: 14px; } .topline, .grid { grid-template-columns: 1fr; } .section { overflow-x: auto; } }
</style>
</head>
<body>
<main>
  <div class="topline"><h1>OPPORTUNITY RADAR</h1><div id="refreshed" class="muted">Loading…</div></div>
  <div class="metrics">
    <div class="card"><div class="metric-label">Overall</div><div id="overall" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">Monitor heartbeat</div><div id="heartbeat" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">Configured feeds</div><div id="feeds" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">$10k VWAP</div><div id="primary" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">Spread monitor</div><div id="monitor" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">Active episodes</div><div id="episodes-count" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">Confirmed candidates</div><div id="confirmed" class="metric-value">—</div></div>
    <div class="card"><div class="metric-label">Recent alerts</div><div id="alerts" class="metric-value">—</div></div>
  </div>
  <div id="errors"></div>
  <section class="section"><h2>MARKET FEEDS</h2><table><thead><tr><th>Symbol</th><th>Venue</th><th>Status</th><th>Age</th><th>VWAP</th></tr></thead><tbody id="feed-rows"></tbody></table></section>
  <div class="grid">
    <section class="section"><h2>ACTIVE EPISODES</h2><table><thead><tr><th>Symbol</th><th>Long</th><th>Short</th><th>Net bps</th><th>Confirmed</th><th>Alerted</th></tr></thead><tbody id="episode-rows"></tbody></table></section>
    <section class="section"><h2>RECENT EVENTS</h2><table><thead><tr><th>Time</th><th>Symbol</th><th>Type</th><th>Net bps</th></tr></thead><tbody id="event-rows"></tbody></table></section>
  </div>
</main>
<script>
const esc = value => String(value ?? '—').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const age = value => value == null ? '—' : (value < 60 ? `${Math.round(value)}s` : `${Math.floor(value / 60)}m ${Math.round(value % 60)}s`);
const statusClass = value => value === 'healthy' ? 'healthy' : value === 'degraded' ? 'degraded' : 'down';
async function refresh() {
  try {
    const response = await fetch('/api/status', {cache: 'no-store'});
    const data = await response.json();
    const overall = data.overall || {};
    const heartbeat = data.heartbeat || {};
    document.getElementById('overall').innerHTML = `<span class="dot ${statusClass(overall.status)}"></span>${esc((overall.status || 'down').toUpperCase())}`;
    document.getElementById('heartbeat').textContent = age(heartbeat.age_seconds);
    document.getElementById('feeds').textContent = `${overall.healthy_feeds ?? 0} / ${overall.configured_feeds ?? 0}`;
    document.getElementById('primary').textContent = `${overall.primary_vwap_ready ?? 0} / ${overall.configured_feeds ?? 0}`;
    document.getElementById('monitor').textContent = heartbeat.status === 'down' ? 'DOWN' : heartbeat.status === 'degraded' ? 'DEGRADED' : 'RUNNING';
    document.getElementById('episodes-count').textContent = overall.active_episodes ?? 0;
    document.getElementById('confirmed').textContent = overall.confirmed_candidates ?? 0;
    document.getElementById('alerts').textContent = (data.recent_events || []).filter(event => event.event_type === 'alert').length;
    document.getElementById('refreshed').textContent = `Last refreshed ${new Date().toLocaleTimeString()}`;
    document.getElementById('errors').innerHTML = (data.errors || []).map(error => `<div class="error">${esc(error)}</div>`).join('');
    document.getElementById('feed-rows').innerHTML = (data.feeds || []).map(feed => `<tr><td>${esc(feed.canonical_symbol)}</td><td>${esc(feed.venue)}</td><td class="${statusClass(feed.status)}"><span class="dot"></span>${esc(feed.status)}</td><td>${age(feed.age_seconds)}</td><td>${feed.vwap_available ?? 0}/3</td></tr>`).join('') || '<tr><td colspan="5" class="muted">No configured feeds</td></tr>';
    document.getElementById('episode-rows').innerHTML = (data.episodes || []).map(episode => `<tr><td>${esc(episode.symbol)}</td><td>${esc(episode.long_venue)}</td><td>${esc(episode.short_venue)}</td><td>${episode.net_spread_bps == null ? '—' : Number(episode.net_spread_bps).toFixed(2)}</td><td>${episode.candidate_confirmed ? 'yes' : 'no'}</td><td>${episode.alerted ? 'yes' : 'no'}</td></tr>`).join('') || '<tr><td colspan="6" class="muted">No active episodes</td></tr>';
    document.getElementById('event-rows').innerHTML = (data.recent_events || []).map(event => `<tr><td>${esc(event.occurred_at)}</td><td>${esc(event.symbol)}</td><td>${esc(event.event_type)}</td><td>${event.net_spread_bps == null ? '—' : Number(event.net_spread_bps).toFixed(2)}</td></tr>`).join('') || '<tr><td colspan="4" class="muted">No recent events</td></tr>';
  } catch (error) {
    document.getElementById('overall').textContent = 'DOWN';
    document.getElementById('errors').innerHTML = `<div class="error">Dashboard request failed: ${esc(error)}</div>`;
  }
}
refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>"""


def _handler_for(service: DashboardStatusService) -> type[BaseHTTPRequestHandler]:
    class DashboardRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/":
                self._send(200, "text/html; charset=utf-8", HTML.encode("utf-8"))
                return
            if path == "/api/status":
                try:
                    payload = service.get_status()
                    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode(
                        "utf-8"
                    )
                    self._send(200, "application/json; charset=utf-8", body)
                except Exception as error:  # noqa: BLE001
                    body = json.dumps(
                        {
                            "generated_at": utc_now().isoformat(),
                            "overall": {"status": "down"},
                            "feeds": [],
                            "episodes": [],
                            "recent_events": [],
                            "errors": [
                                f"dashboard status failed: {type(error).__name__}: {error}"
                            ],
                        },
                        allow_nan=False,
                    ).encode("utf-8")
                    self._send(200, "application/json; charset=utf-8", body)
                return
            self.send_error(404)

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
    service: DashboardStatusService,
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
