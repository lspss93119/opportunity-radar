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
from urllib.parse import parse_qs, urlparse

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


@pytest.mark.parametrize("path", ["/", "/opportunities", "/status"])
def test_dashboard_html_navigation_and_view_anchors(scanner_http, path):
    with urlopen(scanner_http + path) as response:
        assert response.headers["Content-Type"] == "text/html; charset=utf-8"
        html = response.read().decode()
    assert 'href="/opportunities"' in html and 'href="/status"' in html
    if path == "/status":
        for anchor in ("heartbeat", "venues", "parquet", "sqlite", "episode-rows", "event-rows", "unavailable"):
            assert f'id="{anchor}"' in html
    else:
        for anchor in ("filters", "opportunity-rows", "data-as-of"):
            assert f'id="{anchor}"' in html
        for column in ("Symbol", "Long", "Short", "Spread", "24h Mean", "24h Std", "Deviation", "Duration", "RT Fee", "Theo Edge", "Skew", "Freshness"):
            assert f">{column}<" in html


def run_ui_script(html, expression):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is optional and only needed to execute vanilla UI behavior tests")
    script = re.search(r"<script>(.*?)</script>", html, re.S).group(1)
    harness = """
const elements = new Map();
const document = {getElementById(id) { if (!elements.has(id)) elements.set(id, {textContent:'', innerHTML:'', value:'', checked:false, hidden:false, addEventListener(){}}); return elements.get(id); }};
const location = {pathname:'/', search:'?symbol=ETH&active_only=true'};
const history = {replaceState(state, title, url){this.url=url;}};
const window = {addEventListener(){}};
const setInterval = () => {};
const fetch = () => new Promise(() => {});
"""
    result = subprocess.run([node, "-e", harness + script + "\n" + expression], text=True, capture_output=True, check=True)
    return json.loads(result.stdout)


def test_scanner_exact_pair_links_and_missing_identity_never_guessed(scanner_http):
    with urlopen(scanner_http + "/") as response:
        html = response.read().decode()
    links = run_ui_script(html, """
const exact = {canonical_symbol:'BRK B', long_venue:'arcus', long_venue_symbol:'BRK/B-USD', short_venue:'trade_xyz', short_venue_symbol:'xyz:BRK B'};
console.log(JSON.stringify([pairLink(exact), pairLink({...exact, long_venue_symbol:null}), pairLink({...exact, short_venue_symbol:''})]));
""")
    assert parse_qs(urlparse(links[0]).query) == {
        "symbol": ["BRK B"], "long_venue": ["arcus"], "long_venue_symbol": ["BRK/B-USD"],
        "short_venue": ["trade_xyz"], "short_venue_symbol": ["xyz:BRK B"],
    }
    assert urlparse(links[0]).path == "/pair"
    assert links[1:] == [None, None]


def test_scanner_restores_and_preserves_filters_without_navigation(scanner_http):
    with urlopen(scanner_http + "/") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
document.getElementById('min_deviation').value = '200';
const query = filterQuery();
console.log(JSON.stringify({symbol:document.getElementById('symbol').value, active:document.getElementById('active_only').checked, query:query.toString()}));
""")
    assert result["symbol"] == "ETH" and result["active"] is True
    assert parse_qs(result["query"]) == {"symbol": ["ETH"], "active_only": ["true"], "min_deviation": ["200"]}


def test_pair_routes_remain_deferred_and_unknown_api_is_json(scanner_http):
    for path in ("/api/pair?symbol=BTC", "/api/unknown"):
        assert http_json(scanner_http, path, 404)["errors"]
    with pytest.raises(HTTPError) as error:
        urlopen(scanner_http + "/pair?symbol=BTC")
    assert error.value.code == 404
