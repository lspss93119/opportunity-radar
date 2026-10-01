from __future__ import annotations

import json
import re
import shutil
import sqlite3
import subprocess
import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen
from urllib.parse import parse_qs, urlencode, urlparse

import pytest

from radar.config import MarketConfig, RadarConfig
from radar.dashboard import DashboardStatusService, classify_heartbeat, create_dashboard_server
from radar.models import MarketSnapshot
from radar.storage.parquet import ParquetStorage
from radar.storage.sqlite import SQLiteRuntimeStore

NOW = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)


def make_config(*markets: MarketConfig) -> RadarConfig:
    return RadarConfig(markets=list(markets))


def make_market(
    *,
    venue: str = "lighter",
    venue_symbol: str = "BTC",
    canonical_symbol: str = "BTC",
    sample_time: datetime | None = None,
    observed_at: datetime = NOW,
    buy_1k_vwap: float | None = 100.0,
    sell_1k_vwap: float | None = 101.0,
    buy_5k_vwap: float | None = 100.0,
    sell_5k_vwap: float | None = 101.0,
    buy_10k_vwap: float | None = 100.0,
    sell_10k_vwap: float | None = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=observed_at if sample_time is None else sample_time,
        observed_at=observed_at,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=99.0,
        best_bid_size=2.0,
        best_ask=100.0,
        best_ask_size=3.0,
        buy_1k_vwap=buy_1k_vwap,
        sell_1k_vwap=sell_1k_vwap,
        buy_5k_vwap=buy_5k_vwap,
        sell_5k_vwap=sell_5k_vwap,
        buy_10k_vwap=buy_10k_vwap,
        sell_10k_vwap=sell_10k_vwap,
    )


def write_markets(data_root: Path, snapshots: list[MarketSnapshot], *, now: datetime = NOW) -> None:
    storage = ParquetStorage(data_root)
    for snapshot in snapshots:
        storage.append(snapshot)
    storage.flush(now=now)


def write_heartbeat(runtime_db: Path, *, updated_at: datetime = NOW, episodes=None) -> None:
    with SQLiteRuntimeStore(runtime_db) as store:
        store.set_monitor_state(
            "spread",
            "episodes",
            {} if episodes is None else episodes,
            updated_at=updated_at,
        )


def make_episode(
    *,
    symbol: str,
    net_spread_bps: float,
    confirmed: bool = False,
    alerted: bool = False,
) -> dict[str, object]:
    return {
        "episode_id": f"{symbol}:episode",
        "key": {
            "canonical_symbol": symbol,
            "long_venue": "lighter",
            "long_venue_symbol": symbol,
            "short_venue": "hyperliquid",
            "short_venue_symbol": symbol,
        },
        "first_seen_at": "2026-09-23T11:58:00+00:00",
        "last_seen_at": "2026-09-23T11:59:50+00:00",
        "candidate": {
            "sample_time": "2026-09-23T11:59:50+00:00",
            "long_buy_vwap": 100.0,
            "short_sell_vwap": 101.0,
            "long_fee_bps": 4.5,
            "short_fee_bps": 3.5,
            "raw_spread_bps": net_spread_bps + 8.0,
            "net_spread_bps": net_spread_bps,
        },
        "candidate_confirmed": confirmed,
        "candidate_confirmed_at": (
            "2026-09-23T11:58:30+00:00" if confirmed else None
        ),
        "alert_condition_since": None,
        "alerted": alerted,
    }


def test_heartbeat_health_boundaries_are_deterministic():
    assert classify_heartbeat(None) == "down"
    assert classify_heartbeat(30.0) == "healthy"
    assert classify_heartbeat(30.1) == "degraded"
    assert classify_heartbeat(60.0) == "degraded"
    assert classify_heartbeat(60.1) == "down"


def test_expected_feeds_are_dynamic_and_disabled_markets_are_ignored(tmp_path):
    config = make_config(
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"),
        MarketConfig(
            venue="hyperliquid",
            venue_symbol="ETH",
            canonical_symbol="ETH",
            enabled=False,
        ),
        MarketConfig(venue="trade_xyz", venue_symbol="xyz:TSLA", canonical_symbol="TSLA", enabled=False),
    )
    write_markets(tmp_path / "data", [make_market()])
    write_heartbeat(tmp_path / "runtime.sqlite3")

    status = DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status()

    assert status["overall"]["configured_feeds"] == 1
    assert [(feed["venue"], feed["venue_symbol"]) for feed in status["feeds"]] == [
        ("lighter", "BTC")
    ]


def test_latest_market_row_is_selected_by_observed_at(tmp_path):
    config = make_config(
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    )
    older = make_market(observed_at=NOW - timedelta(seconds=20), buy_10k_vwap=90.0)
    newer = make_market(observed_at=NOW - timedelta(seconds=5), buy_10k_vwap=100.0)
    write_markets(tmp_path / "data", [older, newer])
    write_heartbeat(tmp_path / "runtime.sqlite3")

    status = DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status()

    feed = status["feeds"][0]
    assert feed["observed_at"] == newer.observed_at.isoformat()
    assert feed["buy_10k_vwap"] == 100.0


def test_previous_utc_date_partition_is_considered(tmp_path):
    dashboard_now = NOW.replace(hour=0)
    previous = dashboard_now - timedelta(seconds=30)
    config = make_config(
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    )
    write_markets(tmp_path / "data", [make_market(observed_at=previous)], now=dashboard_now)
    write_heartbeat(tmp_path / "runtime.sqlite3", updated_at=dashboard_now)

    status = DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: dashboard_now,
    ).get_status()

    assert status["feeds"][0]["status"] == "healthy"
    assert status["feeds"][0]["observed_at"] == previous.isoformat()


def test_missing_primary_vwap_degrades_overall_but_missing_lower_sizes_does_not(
    tmp_path,
):
    config = make_config(
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    )
    lower_sizes_missing = make_market(
        observed_at=NOW - timedelta(seconds=2),
        buy_1k_vwap=None,
        sell_1k_vwap=None,
        buy_5k_vwap=None,
        sell_5k_vwap=None,
    )
    write_markets(tmp_path / "data", [lower_sizes_missing])
    write_heartbeat(tmp_path / "runtime.sqlite3")
    service = DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    )

    status = service.get_status()
    assert status["overall"]["status"] == "healthy"
    assert status["feeds"][0]["vwap_available"] == 1
    assert status["feeds"][0]["primary_vwap_ready"] is True

    write_markets(
        tmp_path / "data",
        [
            make_market(
                observed_at=NOW - timedelta(seconds=1),
                sample_time=NOW,
                buy_10k_vwap=None,
                sell_10k_vwap=None,
            )
        ],
    )
    degraded = service.get_status()
    assert degraded["overall"]["status"] == "degraded"
    assert degraded["overall"]["primary_vwap_ready"] == 0


@pytest.mark.parametrize(
    ("age", "expected"),
    [(90, "healthy"), (90.1, "degraded"), (180, "degraded"), (180.1, "down")],
)
def test_feed_freshness_boundaries(age, expected, tmp_path):
    config = make_config(
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    )
    observed_at = NOW - timedelta(seconds=age)
    write_markets(tmp_path / "data", [make_market(observed_at=observed_at)])
    write_heartbeat(tmp_path / "runtime.sqlite3")

    status = DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status()

    assert status["feeds"][0]["status"] == expected


def test_missing_data_and_sqlite_return_valid_down_status_without_creating_db(tmp_path):
    runtime_db = tmp_path / "missing" / "runtime.sqlite3"
    config = make_config(
        MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC")
    )

    status = DashboardStatusService(
        config,
        data_root=tmp_path / "missing-data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    ).get_status()

    assert status["overall"]["status"] == "down"
    assert status["errors"]
    assert not runtime_db.exists()


def test_sqlite_is_opened_with_read_only_uri(tmp_path, monkeypatch):
    import radar.dashboard as dashboard

    calls: list[tuple[object, bool]] = []
    original_connect = dashboard.sqlite3.connect

    def recording_connect(database, *args, **kwargs):
        calls.append((database, kwargs.get("uri", False)))
        return original_connect(database, *args, **kwargs)

    monkeypatch.setattr(dashboard.sqlite3, "connect", recording_connect)
    config = make_config()
    DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status()

    assert calls
    database_uri, uri = calls[0]
    assert uri is True
    assert str(database_uri).endswith("?mode=ro")


def test_episode_summary_counts_and_sorts_by_net_spread(tmp_path):
    config = make_config()
    episodes = {
        "btc": make_episode(symbol="BTC", net_spread_bps=20.0, confirmed=True),
        "eth": make_episode(
            symbol="ETH", net_spread_bps=50.0, confirmed=True, alerted=True
        ),
        "sol": make_episode(symbol="SOL", net_spread_bps=10.0),
    }
    write_heartbeat(tmp_path / "runtime.sqlite3", episodes=episodes)

    status = DashboardStatusService(
        config,
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    ).get_status()

    assert status["overall"]["active_episodes"] == 3
    assert status["overall"]["confirmed_candidates"] == 2
    assert status["overall"]["active_alerted_episodes"] == 1
    assert [episode["symbol"] for episode in status["episodes"]] == [
        "ETH",
        "BTC",
        "SOL",
    ]


def test_recent_events_are_newest_first_limited_and_tolerate_malformed_json(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(runtime_db) as store:
        for index in range(25):
            occurred_at = NOW - timedelta(minutes=25 - index)
            store.append_opportunity(
                "spread",
                f"event-{index}",
                "alert",
                {
                    "canonical_symbol": "BTC",
                    "long_venue": "lighter",
                    "short_venue": "hyperliquid",
                    "net_spread_bps": float(index),
                },
                occurred_at=occurred_at,
            )
    connection = sqlite3.connect(runtime_db)
    connection.execute(
        "INSERT INTO opportunity_log VALUES (?, ?, ?, ?, ?)",
        ("spread", "broken", "resolved", "{not-json", NOW.isoformat()),
    )
    connection.commit()
    connection.close()

    status = DashboardStatusService(
        make_config(),
        data_root=tmp_path / "data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    ).get_status()

    assert len(status["recent_events"]) == 20
    assert status["recent_events"][0]["event_id"] == "broken"
    assert status["recent_events"][0]["symbol"] is None
    assert any("event JSON" in error for error in status["errors"])


def test_status_json_is_finite_and_http_routes_work(tmp_path):
    service = DashboardStatusService(
        make_config(),
        data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    )
    server = create_dashboard_server(service, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        assert host == "127.0.0.1"
        with urlopen(f"http://{host}:{port}/api/status") as response:
            payload = json.loads(response.read())
            assert response.status == 200
        assert payload["overall"]["status"] == "down"
        json.dumps(payload, allow_nan=False)

        with urlopen(f"http://{host}:{port}/") as response:
            html = response.read().decode("utf-8")
            assert response.status == 200
            assert "OPPORTUNITY RADAR" in html

        with pytest.raises(HTTPError) as error:
            urlopen(f"http://{host}:{port}/unknown")
        assert error.value.code == 404
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@contextmanager
def dashboard_http(service):
    server = create_dashboard_server(service, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def http_json(base, path, expected_status=200):
    try:
        response = urlopen(base + path, timeout=15)
    except HTTPError as error:
        response = error
    with response:
        assert response.code == expected_status
        assert response.headers["Content-Type"] == "application/json; charset=utf-8"
        assert response.headers["Cache-Control"] == "no-store"
        # Reject non-standard JSON tokens even when Python's decoder accepts them.
        def reject_constant(value):
            raise AssertionError(f"non-finite JSON: {value}")
        return json.loads(response.read(), parse_constant=reject_constant)


@pytest.fixture(scope="module")
def scanner_service(tmp_path_factory):
    root = tmp_path_factory.mktemp("dashboard-http")
    markets, snapshots, episodes = [], [], {}
    for symbol, current_spread, duration in [("BTC", 500, 75), ("ETH", 300, 20)]:
        long_symbol, short_symbol = f"{symbol}-USD", f"xyz:{symbol}"
        for venue, venue_symbol in [("lighter", long_symbol), ("hyperliquid", short_symbol)]:
            markets.append(MarketConfig(venue=venue, venue_symbol=venue_symbol, canonical_symbol=symbol))
        prior_times = [NOW - timedelta(days=1) + timedelta(seconds=10 * i)
                       for i in range(8640) if i % 5 != 1]
        for index, sample_time in enumerate([*prior_times, NOW]):
            spread = current_spread if sample_time == NOW else (100 if index % 2 == 0 else 300)
            snapshots.extend([
                make_market(venue="lighter", venue_symbol=long_symbol, canonical_symbol=symbol,
                            sample_time=sample_time, observed_at=sample_time),
                make_market(venue="hyperliquid", venue_symbol=short_symbol, canonical_symbol=symbol,
                            sample_time=sample_time, observed_at=sample_time, sell_10k_vwap=100 * (1 + spread / 10000)),
            ])
        episode = make_episode(symbol=symbol, net_spread_bps=current_spread - 3, confirmed=True)
        episode["key"]["long_venue_symbol"] = long_symbol
        episode["key"]["short_venue_symbol"] = short_symbol
        episode["alert_condition_since"] = (NOW - timedelta(seconds=duration)).isoformat()
        episodes[symbol] = episode
    write_markets(root / "data", snapshots)
    write_heartbeat(root / "runtime.sqlite3", episodes=episodes)
    return DashboardStatusService(
        RadarConfig(markets=markets, fees_bps={"lighter": 1, "hyperliquid": 2}),
        data_root=root / "data", runtime_db=root / "runtime.sqlite3", clock=lambda: NOW,
    )


@pytest.fixture(scope="module")
def scanner_http(scanner_service):
    with dashboard_http(scanner_service) as base:
        yield base


def test_opportunities_http_orders_by_deviation_and_has_finite_required_fields(scanner_http):
    payload = http_json(scanner_http, "/api/opportunities")
    assert payload["data_as_of"] == NOW.isoformat()
    assert payload["generated_at"] == NOW.isoformat()
    rows = payload["rows"]
    assert len(rows) == 4
    assert rows[0]["canonical_symbol"] == "BTC"
    assert rows[0]["deviation_bps"] == pytest.approx(300)
    assert rows[1]["deviation_bps"] == pytest.approx(100)
    assert [row["deviation_bps"] for row in rows] == sorted(
        [row["deviation_bps"] for row in rows], reverse=True)
    assert {"canonical_symbol", "long_venue", "long_venue_symbol", "short_venue",
            "short_venue_symbol", "current_raw_spread_bps", "rolling_mean_bps",
            "rolling_std_bps", "deviation_bps", "signal_duration_seconds",
            "round_trip_fee_bps", "theoretical_edge_bps", "observed_at_skew_seconds",
            "sample_time", "freshness_seconds", "active", "alerted"} <= rows[0].keys()


@pytest.mark.parametrize(("query", "count", "first_symbol"), [
    ("symbol=%20eth%20", 2, "ETH"), ("long_venue=%20LIGHTER%20", 2, "BTC"),
    ("short_venue=HYPERLIQUID", 2, "BTC"), ("max_std=99", 2, "BTC"),
    ("max_std=100.01", 4, "BTC"), ("min_deviation=200", 1, "BTC"),
    ("min_duration=60", 1, "BTC"), ("active_only=true", 2, "BTC"),
    ("active_only=false", 4, "BTC"), ("limit=1", 1, "BTC"),
    ("symbol=BTC&long_venue=lighter&short_venue=hyperliquid&max_std=101&min_deviation=250&min_duration=70&active_only=true", 1, "BTC"),
])
def test_opportunities_http_filters(scanner_http, query, count, first_symbol):
    rows = http_json(scanner_http, "/api/opportunities?" + query)["rows"]
    assert len(rows) == count
    if count:
        assert rows[0]["canonical_symbol"] == first_symbol
    if query == "max_std=99":
        # Reverse pairs use constant buy/sell prices in this fixture (std=0).
        assert all(row["long_venue"] == "hyperliquid" for row in rows)
        assert all(row["rolling_std_bps"] == pytest.approx(0) for row in rows)


@pytest.mark.parametrize("query", [
    "max_std=nope", "max_std=-1", "max_std=NaN", "max_std=inf", "max_std=",
    "min_deviation=nope", "min_deviation=-1", "min_deviation=-inf", "min_deviation=1e999",
    "min_duration=-1", "min_duration=1.5", "min_duration=nan",
    "active_only=1", "active_only=yes", "active_only=", "active_only=TRUE",
    "limit=0", "limit=-1", "limit=201", "limit=1.5", "limit=NaN", "limit=",
    "score=1", "symbol=", "long_venue=", "short_venue=", "symbol=BTC&symbol=ETH",
    "limit=1&limit=1", "long_venue_symbol=BTC", "canonical_symbol=BTC", "symbol=%FF",
])
def test_opportunities_http_rejects_bad_unknown_and_duplicate_filters(scanner_http, query):
    payload = http_json(scanner_http, "/api/opportunities?" + query, 400)
    assert payload["errors"]
    assert payload["rows"] == []


@pytest.mark.parametrize(("heartbeat_age", "feed_age", "primary", "heartbeat", "feed", "overall"), [
    (30, 90, True, "healthy", "healthy", "healthy"),
    (30.1, 90, True, "degraded", "healthy", "degraded"),
    (60, 90, True, "degraded", "healthy", "degraded"),
    (60.1, 90, True, "down", "healthy", "down"),
    (None, 90, True, "down", "healthy", "down"),
    (0, 90.1, True, "healthy", "degraded", "degraded"),
    (0, 180, True, "healthy", "degraded", "degraded"),
    (0, 180.1, True, "healthy", "down", "degraded"),
    (0, None, True, "healthy", "down", "degraded"),
    (0, 0, False, "healthy", "degraded", "degraded"),
])
def test_status_http_preserves_evidence_boundaries(tmp_path, heartbeat_age, feed_age, primary, heartbeat, feed, overall):
    config = make_config(MarketConfig(venue="lighter", venue_symbol="BTC", canonical_symbol="BTC"))
    if heartbeat_age is not None:
        write_heartbeat(tmp_path / "runtime.sqlite3", updated_at=NOW - timedelta(seconds=heartbeat_age))
    if feed_age is not None:
        write_markets(tmp_path / "data", [make_market(observed_at=NOW - timedelta(seconds=feed_age),
            buy_10k_vwap=100 if primary else None)])
    service = DashboardStatusService(config, data_root=tmp_path / "data",
        runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW)
    with dashboard_http(service) as base:
        payload = http_json(base, "/api/status")
    assert payload["heartbeat"]["status"] == heartbeat
    assert payload["feeds"][0]["status"] == feed
    assert payload["overall"]["status"] == overall
    assert {"radar_heartbeat", "data_as_of", "sample_age_seconds", "venues", "parquet", "sqlite", "episodes", "recent_events"} <= payload.keys()


def test_status_http_partial_cycle_and_storage_errors_are_degraded(tmp_path):
    config = make_config(*[MarketConfig(venue="lighter", venue_symbol=s, canonical_symbol=s) for s in ("BTC", "ETH")])
    write_markets(tmp_path / "data", [make_market()])
    write_heartbeat(tmp_path / "runtime.sqlite3")
    service = DashboardStatusService(config, data_root=tmp_path / "data", runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW)
    with dashboard_http(service) as base:
        payload = http_json(base, "/api/status")
        assert payload["overall"]["status"] == "degraded"
        assert payload["overall"]["latest_feeds"] == 1
        assert payload["venues"]["lighter"]["missing"] == 1
        # A corrupt Parquet file cannot be represented as a healthy empty dataset.
        (tmp_path / "data" / "market" / "date=2026-09-23" / "part-broken.parquet").write_bytes(b"broken")
        broken = http_json(base, "/api/status")
        assert broken["overall"]["status"] == "degraded"
        assert broken["errors"]


def test_storage_failure_json_and_http_reads_never_create_sources(tmp_path, monkeypatch):
    service = DashboardStatusService(make_config(), data_root=tmp_path / "missing-data", runtime_db=tmp_path / "missing-runtime" / "radar.sqlite3", clock=lambda: NOW)
    with dashboard_http(service) as base:
        for path in ("/api/opportunities", "/api/status"):
            payload = http_json(base, path)
            assert payload["errors"] and payload["data_as_of"] is None
        assert not service.data_root.exists()
        assert not service.runtime_db.parent.exists()

        def fail(**kwargs):
            raise OSError("fixture storage unavailable")
        monkeypatch.setattr(service, "get_status", fail)
        payload = http_json(base, "/api/status")
        assert payload["overall"]["status"] == "degraded"
        assert payload["data_as_of"] is None


@pytest.mark.parametrize("path", ["/", "/anomalies", "/opportunities", "/status"])
def test_dashboard_html_navigation_and_view_anchors(scanner_http, path):
    with urlopen(scanner_http + path) as response:
        assert response.headers["Content-Type"] == "text/html; charset=utf-8"
        html = response.read().decode()
    assert 'href="/opportunities"' in html and 'href="/status"' in html
    if path == "/status":
        for anchor in ("heartbeat", "venues", "parquet", "sqlite", "episode-rows", "event-rows", "unavailable"):
            assert f'id="{anchor}"' in html
    elif path == "/opportunities":
        for anchor in ("filters", "opportunity-rows", "data-as-of"):
            assert f'id="{anchor}"' in html
        for column in ("Symbol", "Long", "Short", "Spread", "24h Mean", "24h Std", "Deviation", "Duration", "RT Fee", "Theo Edge", "Skew", "Freshness"):
            assert f">{column}<" in html
    else:
        for anchor in ("anomaly-filters", "anomaly-symbol", "anomaly-long-venue", "anomaly-short-venue", "anomaly-eligible-only", "anomaly-rows", "data-as-of"):
            assert f'id="{anchor}"' in html


def run_ui_script(html, expression, setup=""):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is optional and only needed to execute vanilla UI behavior tests")
    script = re.search(r"<script>(.*?)</script>", html, re.S).group(1)
    harness = """
const elements = new Map();
const document = {getElementById(id) { if (!elements.has(id)) elements.set(id, {textContent:'', innerHTML:'', value:'', checked:false, hidden:false, listeners:{}, addEventListener(type, callback){this.listeners[type]=callback;}}); return elements.get(id); }};
const location = {pathname:'/', search:'?symbol=ETH&active_only=true'};
const history = {replaceState(state, title, url){this.url=url; location.search = new URL(url, 'http://localhost').search;}};
const window = {listeners:{}, addEventListener(type, callback){this.listeners[type]=callback;}};
let setInterval = () => {};
let fetch = () => new Promise(() => {});
"""
    result = subprocess.run([node, "-e", harness + setup + script + "\n" + expression], text=True, capture_output=True, check=True, timeout=10)
    return json.loads(result.stdout)


POLLING_SETUP = """
const requests = [];
let tick, interval;
setInterval = (callback, milliseconds) => { tick = callback; interval = milliseconds; };
fetch = url => new Promise((resolve, reject) => requests.push({url, resolve, reject}));
document.getElementById('overall').textContent = 'Loading';
const settle = () => new Promise(resolve => setImmediate(resolve));
function succeed(request, marker) {
  request.resolve({json: async () => ({
    status:'healthy', overall:{status:'healthy'}, data_as_of:marker,
    heartbeat:{status:'healthy', updated_at:marker, age_seconds:0},
    rows:[{canonical_symbol:marker}], errors:[]
  })});
}
"""


@pytest.mark.parametrize("path", ["/opportunities", "/status"])
def test_dashboard_polling_renders_delayed_successes_without_overlap(scanner_http, path):
    with urlopen(scanner_http + path) as response:
        html = response.read().decode()
    result = run_ui_script(html, """
(async () => {
  const rendered = [];
  for (let cycle = 0; cycle < 3; cycle++) {
    const request = requests.at(-1);
    tick(); tick(); // Two 10-second ticks before this successful response arrives.
    succeed(request, 'sample-' + cycle);
    await settle();
    rendered.push({overall:byId('overall').textContent, sample:byId('data-as-of').textContent,
      view:isStatus ? byId('heartbeat').textContent : byId('opportunity-rows').innerHTML});
    if (cycle < 2) tick();
  }
  console.log(JSON.stringify({rendered, requests:requests.length, interval}));
})().catch(error => { console.error(error); process.exitCode = 1; });
""", setup=POLLING_SETUP + f"location.pathname = {json.dumps(path)};\n")
    assert [view["overall"] for view in result["rendered"]] == ["HEALTHY"] * 3
    for cycle, view in enumerate(result["rendered"]):
        assert view["sample"] == f"Data as of: sample-{cycle}"
        assert f"sample-{cycle}" in view["view"]
    assert result["requests"] == 3
    assert result["interval"] == 10_000


@pytest.mark.parametrize("action", ["submit", "reset", "popstate"])
@pytest.mark.parametrize("old_first", [True, False])
def test_dashboard_filter_refresh_invalidates_pending_response(scanner_http, action, old_first):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
(async () => {
  const old = requests[0];
  if (action === 'submit') {
    byId('symbol').value = 'BTC';
    byId('filters').listeners.submit({preventDefault(){}});
  } else if (action === 'reset') {
    byId('reset').listeners.click();
  } else {
    location.search = '?symbol=SOL';
    window.listeners.popstate();
  }
  const current = requests.at(-1);
  if (oldFirst) { succeed(old, 'obsolete'); await settle(); }
  tick(); // An obsolete completion must not unlock polling of the current request.
  succeed(current, 'current');
  await settle();
  if (!oldFirst) { succeed(old, 'obsolete'); await settle(); }
  const rendered = {overall:byId('overall').textContent, sample:byId('data-as-of').textContent,
    rows:byId('opportunity-rows').innerHTML};
  tick(); // Polling resumes after the current request has completed.
  console.log(JSON.stringify({rendered, urls:requests.map(request => request.url)}));
})().catch(error => { console.error(error); process.exitCode = 1; });
""", setup=POLLING_SETUP + f"location.pathname = '/opportunities'; const action = {json.dumps(action)}, oldFirst = {json.dumps(old_first)};\n")
    assert result["rendered"]["overall"] == "HEALTHY"
    assert result["rendered"]["sample"] == "Data as of: current"
    assert "current" in result["rendered"]["rows"]
    assert "obsolete" not in result["rendered"]["rows"]
    expected_query = {"submit": {"symbol": ["BTC"], "active_only": ["true"]},
                      "reset": {}, "popstate": {"symbol": ["SOL"]}}[action]
    assert len(result["urls"]) == 3
    for url in result["urls"][1:]:
        assert parse_qs(urlparse(url).query) == expected_query


@pytest.mark.parametrize("path", ["/opportunities", "/status"])
def test_dashboard_polling_resumes_after_request_failure(scanner_http, path):
    with urlopen(scanner_http + path) as response:
        html = response.read().decode()
    result = run_ui_script(html, """
(async () => {
  requests[0].reject(new Error('temporary failure'));
  await settle();
  const failed = byId('overall').textContent;
  tick();
  succeed(requests.at(-1), 'recovered');
  await settle();
  console.log(JSON.stringify({failed, overall:byId('overall').textContent,
    sample:byId('data-as-of').textContent, requests:requests.length}));
})().catch(error => { console.error(error); process.exitCode = 1; });
""", setup=POLLING_SETUP + f"location.pathname = {json.dumps(path)};\n")
    assert result == {"failed": "DEGRADED — dashboard request failed", "overall": "HEALTHY",
                      "sample": "Data as of: recovered", "requests": 2}


def test_scanner_exact_pair_links_and_missing_identity_never_guessed(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    links = run_ui_script(html, """
const exact = {canonical_symbol:'BRK B', long_venue:'arcus', long_venue_symbol:'BRK/B-USD', short_venue:'trade_xyz', short_venue_symbol:'xyz:BRK B'};
console.log(JSON.stringify([pairLink(exact), pairLink({...exact, long_venue_symbol:null}), pairLink({...exact, short_venue_symbol:''})]));
""", setup="location.pathname = '/opportunities';\n")
    assert parse_qs(urlparse(links[0]).query) == {
        "symbol": ["BRK B"], "long_venue": ["arcus"], "long_venue_symbol": ["BRK/B-USD"],
        "short_venue": ["trade_xyz"], "short_venue_symbol": ["xyz:BRK B"],
    }
    assert urlparse(links[0]).path == "/pair"
    assert links[1:] == [None, None]


def test_scanner_restores_and_preserves_filters_without_navigation(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
document.getElementById('min_deviation').value = '200';
const query = filterQuery();
console.log(JSON.stringify({symbol:document.getElementById('symbol').value, active:document.getElementById('active_only').checked, query:query.toString()}));
""", setup="location.pathname = '/opportunities';\n")
    assert result["symbol"] == "ETH" and result["active"] is True
    assert parse_qs(result["query"]) == {"symbol": ["ETH"], "active_only": ["true"], "min_deviation": ["200"]}


def test_anomaly_filters_round_trip_reset_and_browser_navigation(scanner_http):
    with urlopen(scanner_http + "/anomalies") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
const initial = {
  symbol: byId('anomaly-symbol').value,
  longVenue: byId('anomaly-long-venue').value,
  eligible: byId('anomaly-eligible-only').checked
};
byId('anomaly-symbol').value = 'BTC';
byId('anomaly-long-venue').value = 'arcus';
byId('anomaly-eligible-only').checked = true;
byId('anomaly-filters').listeners.submit({preventDefault(){}});
const applied = history.url;
byId('anomaly-reset').listeners.click();
const reset = history.url;
location.search = '?symbol=SOL&short_venue=lighter_robinhood&eligible_only=true';
window.listeners.popstate();
console.log(JSON.stringify({
  initial,
  applied: parse_qs_for_test(applied),
  reset: parse_qs_for_test(reset),
  restored: {
    symbol: byId('anomaly-symbol').value,
    shortVenue: byId('anomaly-short-venue').value,
    eligible: byId('anomaly-eligible-only').checked
  }
}));
function parse_qs_for_test(url) {
  return new URL(url, 'http://localhost').search;
}
""", setup="location.pathname = '/anomalies'; location.search = '?symbol=ETH&long_venue=arcus&eligible_only=true';\n")
    assert result["initial"] == {"symbol": "ETH", "longVenue": "arcus", "eligible": True}
    assert "symbol=BTC" in result["applied"]
    assert "long_venue=arcus" in result["applied"]
    assert "eligible_only=true" in result["applied"]
    assert result["reset"] == ""
    assert result["restored"] == {
        "symbol": "SOL",
        "shortVenue": "lighter_robinhood",
        "eligible": True,
    }


def test_unknown_api_is_json(scanner_http):
    assert http_json(scanner_http, "/api/unknown", 404)["errors"]


PAIR_QUERY = dict(symbol="BTC", long_venue="lighter", long_venue_symbol="BTC-USD",
                  short_venue="hyperliquid", short_venue_symbol="xyz:BTC")


@pytest.mark.parametrize("range_name", ["1h", "6h", "24h", "3d", "7d", "all"])
def test_pair_http_ranges_summary_and_complete_statistics(scanner_http, range_name):
    payload = http_json(scanner_http, "/api/pair?" + urlencode({**PAIR_QUERY, "range": range_name}))
    assert payload["identity"] == {"canonical_symbol": "BTC", **{k: v for k, v in PAIR_QUERY.items() if k != "symbol"}}
    assert payload["range"] == range_name
    assert payload["data_as_of"] == payload["generated_at"] == NOW.isoformat()
    current = payload["current"]
    for field, expected in dict(raw_spread_bps=500, rolling_mean_bps=200,
        rolling_std_bps=100, deviation_bps=300, signal_duration_seconds=75,
        long_buy_vwap=100, short_sell_vwap=105, round_trip_fee_bps=6,
        theoretical_edge_bps=294, observed_at_skew_seconds=0, freshness_seconds=0).items():
        assert current[field] == pytest.approx(expected)
    assert current["sample_time"] == NOW.isoformat()
    assert payload["lifecycle"]["active"] and payload["lifecycle"]["candidate_confirmed"]
    assert payload["lifecycle"]["episode_id"] == "BTC:episode"
    assert payload["basis"]["sample_count"] == 6912  # All prior points, not <=500 chart points.
    assert 0 < len(payload["history"]) <= 500
    assert len(payload["rolling_mean_series"]) == len(payload["history"])
    assert payload["history"][-1]["sample_time"] == NOW.isoformat()
    assert payload["history"][-1]["raw_spread_bps"] == pytest.approx(500)
    assert payload["rolling_mean_series"][-1]["rolling_mean_bps"] == pytest.approx(200)


@pytest.mark.parametrize("missing", list(PAIR_QUERY))
def test_pair_http_requires_every_exact_identity_even_unique_mapping(scanner_http, missing):
    query = {k: v for k, v in PAIR_QUERY.items() if k != missing}
    payload = http_json(scanner_http, "/api/pair?" + urlencode(query), 400)
    assert payload["errors"] and payload["current"] is None


@pytest.mark.parametrize("suffix", ["&range=2h", "&range=", "&range=ALL", "&range=1h&range=6h",
    "&symbol=ETH", "&long_venue_symbol=", "&extra=1", "&range=%FF", "&range=%ZZ"])
def test_pair_http_rejects_invalid_duplicate_unknown_query(scanner_http, suffix):
    assert http_json(scanner_http, "/api/pair?" + urlencode(PAIR_QUERY) + suffix, 400)["errors"]


@pytest.mark.parametrize("replacement", [dict(long_venue_symbol="BTC"), dict(symbol="btc"),
    dict(long_venue="LIGHTER"), dict(short_venue="lighter", short_venue_symbol="BTC-USD")])
def test_pair_http_unknown_exact_identity_is_client_error(scanner_http, replacement):
    assert http_json(scanner_http, "/api/pair?" + urlencode({**PAIR_QUERY, **replacement}), 404)["errors"]


def test_pair_http_exact_matching_formula_unavailable_basis_and_dataset_time(tmp_path, monkeypatch):
    from radar.monitors.spread import SpreadMonitor

    def forbidden(*args, **kwargs):
        raise AssertionError("dashboard must not evaluate monitor")
    monkeypatch.setattr(SpreadMonitor, "evaluate", forbidden)
    markets = [MarketConfig(venue=v, venue_symbol=s, canonical_symbol="BTC") for v, s in
               [("lighter", "BTC-USD"), ("lighter", "BTC-USDC"), ("hyperliquid", "xyz:BTC")]]
    t = NOW - timedelta(seconds=30)
    snapshots = [make_market(venue="lighter", venue_symbol="BTC-USD", sample_time=t, observed_at=t, buy_10k_vwap=200),
        make_market(venue="hyperliquid", venue_symbol="xyz:BTC", sample_time=t, observed_at=t, sell_10k_vwap=202),
        make_market(venue="lighter", venue_symbol="BTC-USD", sample_time=NOW - timedelta(seconds=10)),
        make_market(venue="hyperliquid", venue_symbol="xyz:BTC", sample_time=NOW - timedelta(seconds=11)),
        make_market(venue="lighter", venue_symbol="BTC-USD", sample_time=NOW - timedelta(seconds=2), observed_at=NOW - timedelta(seconds=1), buy_10k_vwap=200),
        make_market(venue="hyperliquid", venue_symbol="xyz:BTC", sample_time=NOW - timedelta(seconds=2), observed_at=NOW, sell_10k_vwap=206)]
    write_markets(tmp_path / "data", snapshots)
    write_heartbeat(tmp_path / "runtime.sqlite3")
    service = DashboardStatusService(RadarConfig(markets=markets, fees_bps={"lighter": 1, "hyperliquid": 2}),
        data_root=tmp_path / "data", runtime_db=tmp_path / "runtime.sqlite3", clock=lambda: NOW)
    with dashboard_http(service) as base:
        payload = http_json(base, "/api/pair?" + urlencode(PAIR_QUERY))
        assert payload["range"] == "24h"  # Scanner links omit range.
        assert payload["status"] == "degraded"
        assert payload["data_as_of"] == (NOW - timedelta(seconds=2)).isoformat()
        assert [p["raw_spread_bps"] for p in payload["history"]] == pytest.approx([100, 300])
        assert [p["sample_time"] for p in payload["history"]] == [t.isoformat(), (NOW - timedelta(seconds=2)).isoformat()]
        assert payload["current"]["observed_at_skew_seconds"] == 1
        assert payload["current"]["freshness_seconds"] == 1
        assert payload["basis"]["eligible"] is False
        for field in ("rolling_mean_bps", "rolling_std_bps", "deviation_bps", "theoretical_edge_bps", "signal_duration_seconds"):
            assert payload["current"][field] is None
        assert all(p["rolling_mean_bps"] is None for p in payload["rolling_mean_series"])
        assert payload["lifecycle"]["available"] is False
        ambiguous = {k: v for k, v in PAIR_QUERY.items() if k != "long_venue_symbol"}
        assert http_json(base, "/api/pair?" + urlencode(ambiguous), 400)["errors"]


def test_pair_http_storage_failure_never_creates_sources(tmp_path, monkeypatch):
    service = DashboardStatusService(RadarConfig(markets=[
        MarketConfig(venue="lighter", venue_symbol="BTC-USD", canonical_symbol="BTC"),
        MarketConfig(venue="hyperliquid", venue_symbol="xyz:BTC", canonical_symbol="BTC")], fees_bps={"lighter": 1, "hyperliquid": 2}),
        data_root=tmp_path / "missing", runtime_db=tmp_path / "runtime" / "missing.sqlite3", clock=lambda: NOW)
    with dashboard_http(service) as base:
        payload = http_json(base, "/api/pair?" + urlencode(PAIR_QUERY))
        assert payload["errors"] and payload["current"] is None
        assert payload["data_as_of"] is None and payload["status"] == "degraded"
        assert not service.data_root.exists() and not service.runtime_db.parent.exists()


def test_pair_ui_summary_chart_and_range_state(scanner_http):
    query = urlencode(PAIR_QUERY)
    payload = http_json(scanner_http, "/api/pair?" + query)
    with urlopen(scanner_http + "/pair?" + query) as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderPair(payload);
const summary = byId('pair-summary').innerHTML;
const chart = byId('pair-chart').innerHTML;
byId('pair-range').value = '7d';
byId('pair-range').listeners.change();
console.log(JSON.stringify({summary, chart, search:location.search, requests:requests.map(r=>r.url)}));
""", setup=POLLING_SETUP + "location.pathname='/pair'; location.search=" + json.dumps("?" + query) + "; const payload=" + json.dumps(payload) + ";\n")
    for label in ("Raw spread", "24h Mean", "24h Std", "Deviation", "Signal duration", "Long $10k buy VWAP", "Short $10k sell VWAP", "RT Fee", "Theo Edge", "Observed skew", "Freshness", "Sample time", "Lifecycle"):
        assert label in result["summary"]
    assert "500.00" in result["summary"] and "294.00" in result["summary"]
    assert "<svg" in result["chart"] and 'data-series="raw"' in result["chart"]
    assert 'data-series="mean"' in result["chart"] and 'data-series="current"' in result["chart"]
    assert parse_qs(result["search"][1:]) == {**{k: [v] for k, v in PAIR_QUERY.items()}, "range": ["7d"]}
    assert all(url.startswith("/api/pair?") for url in result["requests"])
    assert parse_qs(urlparse(result["requests"][-1]).query)["range"] == ["7d"]


def test_pair_ui_gaps_unavailable_mean_and_current_are_not_synthesized(scanner_http):
    with urlopen(scanner_http + "/pair?" + urlencode(PAIR_QUERY)) as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderPair({history:[{sample_time:'2026-09-23T11:59:00Z',raw_spread_bps:100,segment:0},
  {sample_time:'2026-09-23T12:00:00Z',raw_spread_bps:300,segment:1}],
  rolling_mean_series:[{sample_time:'2026-09-23T11:59:00Z',rolling_mean_bps:null,segment:0},
  {sample_time:'2026-09-23T12:00:00Z',rolling_mean_bps:null,segment:1}],
  basis:{eligible:false}, lifecycle:{available:false}, current:null});
console.log(JSON.stringify({chart:byId('pair-chart').innerHTML,summary:byId('pair-summary').innerHTML}));
""", setup="location.pathname='/pair';\n")
    assert 'data-series="current"' not in result["chart"]
    assert 'data-series="mean"' not in result["chart"]
    raw_paths = re.findall(r'<path[^>]*data-series="raw"[^>]*d="([^"]*)"', result["chart"])
    assert raw_paths and all("L" not in path for path in raw_paths)
    assert "Unavailable" in result["summary"]


@pytest.mark.parametrize("check", ["payload", "svg"])
def test_pair_downsampling_preserves_unavailable_mean_break(tmp_path, monkeypatch, check):
    start = NOW - timedelta(hours=6)
    gap = NOW - timedelta(hours=3)
    snapshots = []
    # A missing 24h boundary makes only the mean at `gap` unavailable.
    # All 2161 raw samples in the displayed six hours remain continuous.
    for index in range(10801):
        sample = start - timedelta(days=1) + timedelta(seconds=10 * index)
        if sample == gap - timedelta(days=1):
            continue
        snapshots.extend([
            make_market(venue="lighter", sample_time=sample, observed_at=sample),
            make_market(venue="hyperliquid", sample_time=sample, observed_at=sample,
                        sell_10k_vwap=102.0),
        ])
    write_markets(tmp_path / "data", snapshots)
    service = DashboardStatusService(
        RadarConfig(markets=[MarketConfig(venue=v, venue_symbol="BTC", canonical_symbol="BTC")
                             for v in ("lighter", "hyperliquid")],
                    fees_bps={"lighter": 1, "hyperliquid": 2}),
        data_root=tmp_path / "data", runtime_db=tmp_path / "runtime.sqlite3",
        clock=lambda: NOW,
    )
    query = dict(canonical_symbol="BTC", long_venue="lighter", long_venue_symbol="BTC",
                 short_venue="hyperliquid", short_venue_symbol="BTC", range_name="6h")
    with monkeypatch.context() as uncapped:
        uncapped.setattr("radar.dashboard_data.MAX_DISPLAY_POINTS", 3000)
        full = service.get_pair(**query)
    assert len(full["history"]) == 2161
    assert {p["segment"] for p in full["history"]} == {0}
    assert [p["sample_time"] for p in full["rolling_mean_series"]
            if p["rolling_mean_bps"] is None] == [gap.isoformat()]
    for point in full["rolling_mean_series"]:
        if point["sample_time"] != gap.isoformat():
            assert point["rolling_mean_bps"] == pytest.approx(200)

    payload = service.get_pair(**query)
    assert len(payload["history"]) <= 500
    assert {p["segment"] for p in payload["history"]} == {0}
    means = payload["rolling_mean_series"]
    assert gap.isoformat() not in {p["sample_time"] for p in means}
    assert all(p["rolling_mean_bps"] == pytest.approx(200) for p in means)
    if check == "payload":
        before = [p for p in means if p["sample_time"] < gap.isoformat()][-1]
        after = [p for p in means if p["sample_time"] > gap.isoformat()][0]
        assert before["segment"] != after["segment"]
    else:
        with dashboard_http(service) as base:
            with urlopen(base + "/pair") as response:
                html = response.read().decode()
        chart = run_ui_script(html, """
renderPair(payload);
console.log(JSON.stringify(byId('pair-chart').innerHTML));
""", setup="location.pathname='/pair'; const payload=" + json.dumps(payload) + ";\n")
        raw = re.search(r'<path data-series="raw" d="([^"]*)"', chart).group(1)
        mean = re.search(r'<path data-series="mean" d="([^"]*)"', chart).group(1)
        assert raw.count("M") == 1
        assert mean.count("M") == 2
        assert "L" in mean
