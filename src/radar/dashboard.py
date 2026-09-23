from __future__ import annotations

import argparse
import json
import math
import sqlite3
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import duckdb  # type: ignore[import-untyped]

from radar.config import RadarConfig, load_config

DEFAULT_DATA_ROOT = Path("data")
DEFAULT_RUNTIME_DB = Path("runtime/radar.sqlite3")
HEARTBEAT_HEALTHY_SECONDS = 30.0
HEARTBEAT_DEGRADED_SECONDS = 60.0
FEED_HEALTHY_SECONDS = 90.0
FEED_DEGRADED_SECONDS = 180.0
VWAP_FIELDS = (
    ("buy_1k_vwap", "sell_1k_vwap"),
    ("buy_5k_vwap", "sell_5k_vwap"),
    ("buy_10k_vwap", "sell_10k_vwap"),
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def classify_heartbeat(age_seconds: float | None) -> str:
    if age_seconds is None or age_seconds > HEARTBEAT_DEGRADED_SECONDS:
        return "down"
    if age_seconds > HEARTBEAT_HEALTHY_SECONDS:
        return "degraded"
    return "healthy"


def _classify_feed(age_seconds: float | None, primary_vwap_ready: bool) -> str:
    if age_seconds is None or age_seconds > FEED_DEGRADED_SECONDS:
        return "down"
    if age_seconds > FEED_HEALTHY_SECONDS or not primary_vwap_ready:
        return "degraded"
    return "healthy"


def _iso(value: datetime | None) -> str | None:
    return None if value is None else _as_utc(value, "timestamp").isoformat()


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


class DashboardStatusService:
    """Read-only view of persisted Radar health and monitor state."""

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

    def get_status(self, *, now: datetime | None = None) -> dict[str, object]:
        current_time = _as_utc(self._clock() if now is None else now, "now")
        errors: list[str] = []
        feeds, market_errors = self._read_feeds(current_time)
        errors.extend(market_errors)
        heartbeat, episodes, runtime_errors = self._read_runtime(current_time)
        errors.extend(runtime_errors)
        recent_events, event_errors = self._read_recent_events()
        errors.extend(event_errors)

        heartbeat_age = heartbeat.get("age_seconds")
        heartbeat_status = classify_heartbeat(
            heartbeat_age if isinstance(heartbeat_age, (int, float)) else None
        )
        healthy_feeds = sum(1 for feed in feeds if feed.get("status") == "healthy")
        primary_vwap_ready = sum(
            1 for feed in feeds if feed.get("primary_vwap_ready") is True
        )
        if heartbeat_status == "down":
            overall_status = "down"
        elif heartbeat_status == "degraded":
            overall_status = "degraded"
        elif errors or any(feed.get("status") != "healthy" for feed in feeds):
            overall_status = "degraded"
        else:
            overall_status = "healthy"

        return {
            "generated_at": current_time.isoformat(),
            "heartbeat": heartbeat,
            "overall": {
                "status": overall_status,
                "heartbeat_age_seconds": heartbeat["age_seconds"],
                "configured_feeds": len(feeds),
                "healthy_feeds": healthy_feeds,
                "primary_vwap_ready": primary_vwap_ready,
                "active_episodes": len(episodes),
                "confirmed_candidates": sum(
                    1 for episode in episodes if episode.get("candidate_confirmed") is True
                ),
                "active_alerted_episodes": sum(
                    1 for episode in episodes if episode.get("alerted") is True
                ),
            },
            "feeds": feeds,
            "episodes": episodes,
            "recent_events": recent_events,
            "errors": errors,
        }

    def _expected_feeds(self) -> list[tuple[str, str, str]]:
        return [
            (market.venue, market.venue_symbol, market.canonical_symbol)
            for market in self.config.markets
            if market.enabled
        ]

    def _read_feeds(
        self, now: datetime
    ) -> tuple[list[dict[str, object]], list[str]]:
        expected = self._expected_feeds()
        rows, errors = self._read_latest_market_rows(expected, now)
        feeds: list[dict[str, object]] = []
        for venue, venue_symbol, canonical_symbol in expected:
            row = rows.get((venue, venue_symbol, canonical_symbol))
            observed_at = row.get("observed_at") if row is not None else None
            age_seconds = None
            observed_timestamp = (
                observed_at if isinstance(observed_at, datetime) else None
            )
            if observed_timestamp is not None:
                age_seconds = max(
                    0.0,
                    (now - _as_utc(observed_timestamp, "observed_at")).total_seconds(),
                )
            vwap_available = 0
            if row is not None:
                vwap_available = sum(
                    row.get(buy_field) is not None and row.get(sell_field) is not None
                    for buy_field, sell_field in VWAP_FIELDS
                )
            primary_vwap_ready = row is not None and (
                row.get("buy_10k_vwap") is not None
                and row.get("sell_10k_vwap") is not None
            )
            feeds.append(
                {
                    "venue": venue,
                    "venue_symbol": venue_symbol,
                    "canonical_symbol": canonical_symbol,
                    "status": _classify_feed(age_seconds, primary_vwap_ready),
                    "age_seconds": age_seconds,
                    "observed_at": _iso(observed_timestamp),
                    "vwap_available": vwap_available,
                    "vwap_total": len(VWAP_FIELDS),
                    "primary_vwap_ready": primary_vwap_ready,
                    "buy_1k_vwap": row.get("buy_1k_vwap") if row else None,
                    "sell_1k_vwap": row.get("sell_1k_vwap") if row else None,
                    "buy_5k_vwap": row.get("buy_5k_vwap") if row else None,
                    "sell_5k_vwap": row.get("sell_5k_vwap") if row else None,
                    "buy_10k_vwap": row.get("buy_10k_vwap") if row else None,
                    "sell_10k_vwap": row.get("sell_10k_vwap") if row else None,
                }
            )
        return feeds, errors

    def _read_latest_market_rows(
        self,
        expected: Iterable[tuple[str, str, str]],
        now: datetime,
    ) -> tuple[dict[tuple[str, str, str], dict[str, object]], list[str]]:
        expected = tuple(expected)
        if not expected:
            return {}, []
        dates = (now.date(), now.date() - timedelta(days=1))
        files = tuple(
            path
            for partition_date in dates
            for path in sorted(
                (self.data_root / "market" / f"date={partition_date.isoformat()}").glob(
                    "part-*.parquet"
                )
            )
            if path.is_file()
        )
        if not files:
            return {}, []

        path_list = ", ".join(
            "'" + str(path).replace("'", "''") + "'" for path in files
        )
        predicates = " OR ".join(
            "(venue = ? AND venue_symbol = ? AND canonical_symbol = ?)"
            for _ in expected
        )
        query = f"""
            WITH ranked AS (
                SELECT
                    venue,
                    venue_symbol,
                    canonical_symbol,
                    observed_at,
                    buy_1k_vwap,
                    sell_1k_vwap,
                    buy_5k_vwap,
                    sell_5k_vwap,
                    buy_10k_vwap,
                    sell_10k_vwap,
                    row_number() OVER (
                        PARTITION BY venue, venue_symbol, canonical_symbol
                        ORDER BY observed_at DESC
                    ) AS row_number
                FROM read_parquet([{path_list}])
                WHERE {predicates}
            )
            SELECT
                venue,
                venue_symbol,
                canonical_symbol,
                observed_at,
                buy_1k_vwap,
                sell_1k_vwap,
                buy_5k_vwap,
                sell_5k_vwap,
                buy_10k_vwap,
                sell_10k_vwap
            FROM ranked
            WHERE row_number = 1
        """
        parameters = [value for identity in expected for value in identity]
        try:
            with duckdb.connect() as connection:
                result = connection.execute(query, parameters).fetchall()
        except Exception as error:  # noqa: BLE001
            return {}, [f"market data read failed: {type(error).__name__}: {error}"]

        rows = {
            (row[0], row[1], row[2]): {
                "observed_at": row[3],
                "buy_1k_vwap": row[4],
                "sell_1k_vwap": row[5],
                "buy_5k_vwap": row[6],
                "sell_5k_vwap": row[7],
                "buy_10k_vwap": row[8],
                "sell_10k_vwap": row[9],
            }
            for row in result
        }
        return rows, []

    def _read_runtime(
        self, now: datetime
    ) -> tuple[dict[str, object], list[dict[str, object]], list[str]]:
        heartbeat: dict[str, object] = {
            "status": "down",
            "updated_at": None,
            "age_seconds": None,
        }
        errors: list[str] = []
        try:
            connection = sqlite3.connect(
                self.runtime_db.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=0.2,
            )
        except sqlite3.Error as error:
            return heartbeat, [], [
                f"runtime state read failed: {type(error).__name__}: {error}"
            ]

        try:
            row = connection.execute(
                """
                SELECT state_json, updated_at
                FROM monitor_state
                WHERE monitor_name = 'spread' AND state_key = 'episodes'
                """
            ).fetchone()
            raw_episodes: object = {}
            if row is not None:
                try:
                    updated_at = datetime.fromisoformat(row[1]).astimezone(UTC)
                    age_seconds = max(0.0, (now - updated_at).total_seconds())
                    heartbeat = {
                        "status": classify_heartbeat(age_seconds),
                        "updated_at": updated_at.isoformat(),
                        "age_seconds": age_seconds,
                    }
                    raw_episodes = json.loads(row[0])
                except (TypeError, ValueError, json.JSONDecodeError) as error:
                    errors.append(f"monitor state read failed: {type(error).__name__}: {error}")
            episodes = self._summarize_episodes(raw_episodes, errors)
            return heartbeat, episodes, errors
        except sqlite3.Error as error:
            return heartbeat, [], [
                f"runtime state read failed: {type(error).__name__}: {error}"
            ]
        finally:
            connection.close()

    @staticmethod
    def _summarize_episodes(
        raw_episodes: object,
        errors: list[str],
    ) -> list[dict[str, object]]:
        if not isinstance(raw_episodes, dict):
            return []
        episodes: list[dict[str, object]] = []
        for raw_episode in raw_episodes.values():
            if not isinstance(raw_episode, dict):
                errors.append("invalid episode state ignored")
                continue
            key = raw_episode.get("key")
            candidate = raw_episode.get("candidate")
            if not isinstance(key, dict) or not isinstance(candidate, dict):
                errors.append("invalid episode state ignored")
                continue
            net_spread_bps = _finite_float(candidate.get("net_spread_bps"))
            episodes.append(
                {
                    "symbol": _text(key.get("canonical_symbol")),
                    "long_venue": _text(key.get("long_venue")),
                    "long_venue_symbol": _text(key.get("long_venue_symbol")),
                    "short_venue": _text(key.get("short_venue")),
                    "short_venue_symbol": _text(key.get("short_venue_symbol")),
                    "net_spread_bps": net_spread_bps,
                    "first_seen_at": raw_episode.get("first_seen_at"),
                    "last_seen_at": raw_episode.get("last_seen_at"),
                    "candidate_confirmed": raw_episode.get("candidate_confirmed") is True,
                    "alerted": raw_episode.get("alerted") is True,
                }
            )
        def sort_key(episode: dict[str, object]) -> tuple[float, str, str, str]:
            net_spread_bps = episode.get("net_spread_bps")
            symbol = episode.get("symbol")
            long_venue = episode.get("long_venue")
            short_venue = episode.get("short_venue")
            return (
                -float(net_spread_bps)
                if isinstance(net_spread_bps, (int, float))
                else math.inf,
                symbol if isinstance(symbol, str) else "",
                long_venue if isinstance(long_venue, str) else "",
                short_venue if isinstance(short_venue, str) else "",
            )

        episodes.sort(key=sort_key)
        return episodes

    def _read_recent_events(
        self,
    ) -> tuple[list[dict[str, object]], list[str]]:
        try:
            connection = sqlite3.connect(
                self.runtime_db.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=0.2,
            )
        except sqlite3.Error:
            return [], []

        errors: list[str] = []
        try:
            rows = connection.execute(
                """
                SELECT event_id, event_type, event_json, occurred_at
                FROM opportunity_log
                WHERE monitor_name = 'spread'
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT 20
                """
            ).fetchall()
        except sqlite3.Error as error:
            return [], [f"opportunity log read failed: {type(error).__name__}: {error}"]
        finally:
            connection.close()

        events: list[dict[str, object]] = []
        for event_id, event_type, event_json, occurred_at in rows:
            try:
                event = json.loads(event_json)
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                event = {}
                errors.append(f"event JSON read failed: {type(error).__name__}: {error}")
            if not isinstance(event, dict):
                errors.append("event JSON object expected; using empty event")
                event = {}
            events.append(
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "occurred_at": occurred_at,
                    "symbol": _text(event.get("canonical_symbol")),
                    "long_venue": _text(event.get("long_venue")),
                    "short_venue": _text(event.get("short_venue")),
                    "net_spread_bps": _finite_float(event.get("net_spread_bps")),
                }
            )
        return events, errors


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
