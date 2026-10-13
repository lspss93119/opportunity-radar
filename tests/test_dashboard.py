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


def test_opportunities_http_exposes_v3_scanner_fields_and_root_is_opportunities(scanner_http):
    payload = http_json(scanner_http, "/api/opportunities")
    row = payload["rows"][0]
    assert {
        "basis_state", "basis_sample_count", "basis_coverage",
        "current_net_spread_bps", "convergence_edge_bps",
        "primary_depth_status", "data_as_of", "scanner_data_as_of",
    } <= row.keys()
    assert row["current_net_spread_bps"] == pytest.approx(
        row["current_raw_spread_bps"] - row["long_fee_bps"] - row["short_fee_bps"]
    )
    with urlopen(scanner_http + "/") as response:
        html = response.read().decode()
    assert 'href="/opportunities"' in html
    assert 'id="nav-opportunities"' in html
    assert ">Route<" in html
    assert ">Current<" in html
    assert ">Baseline<" in html
    assert ">Edge<" in html
    assert ">Execution<" in html


def test_opportunities_ui_orders_scan_columns_and_exposes_terminal_hooks(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()

    headers = re.search(r'<table[^>]*aria-label="套利機會"[^>]*>.*?<thead><tr>(.*?)</tr>', html, re.S)
    assert headers is not None
    assert re.findall(r"<th>(.*?)</th>", headers.group(1)) == [
        "交易標的", "交易方向", "收斂優勢", "目前價差", "歷史基準", "可成交性",
    ]
    for hook in (
        "opportunity-terminal-table", "route-cell", "edge-cell", "current-cell",
        "baseline-cell", "execution-cell", "more-filters",
    ):
        assert hook in html


def test_opportunities_ui_uses_explicit_missing_and_freshness_labels(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderOpportunities({status:'healthy', rows:[{
  canonical_symbol:'BTC', long_venue:'lighter', long_venue_symbol:'BTC',
  short_venue:'hyperliquid', short_venue_symbol:'BTC', state:'stale',
  current_raw_spread_bps:0, current_net_spread_bps:null, rolling_mean_bps:null,
  basis_coverage:null, convergence_edge_bps:null, rolling_std_bps:0,
  round_trip_fee_bps:0, freshness_seconds:69, observed_at_skew_seconds:0,
  primary_depth_status:'unavailable'
}]});
console.log(JSON.stringify(byId('opportunity-rows').innerHTML));
""", setup="location.pathname='/opportunities';\n")
    rendered = result
    assert 'class="state-note state-stale"' in rendered
    assert '延遲 1m 9s · 時間差 0s' in rendered
    assert 'raw 0.00 bps' in rendered
    assert '淨價差不可用' in rendered
    assert 'Unavailable' not in rendered


def test_opportunities_ui_describes_the_actual_backend_sort(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    assert "依可操作性排序，再依扣除來回手續費後的收斂優勢排序" in html
    assert "Sorted by deviation" not in html


def test_opportunity_symbol_does_not_show_generic_eligibility(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    rendered = run_ui_script(html, """
renderOpportunities({status:'healthy', rows:[{
  canonical_symbol:'QQQ', long_venue:'arcus', long_venue_symbol:'QQQ-USD',
  short_venue:'lighter_robinhood', short_venue_symbol:'QQQ', state:'eligible', basis_state:'eligible',
  current_raw_spread_bps:26.24, current_net_spread_bps:18.24,
  rolling_mean_bps:8, rolling_std_bps:1.25, basis_coverage:.997,
  convergence_edge_bps:10.24, round_trip_fee_bps:8,
  freshness_seconds:4, observed_at_skew_seconds:1.5, primary_depth_status:'available'
}]});
console.log(JSON.stringify(byId('opportunity-rows').innerHTML));
""", setup="location.pathname='/opportunities';\n")
    symbol_cell = re.search(r'<td class="symbol-cell">.*?</td>', rendered)
    assert symbol_cell is not None
    assert "Eligible" not in symbol_cell.group(0)
    assert rendered.count("Eligible") == 0
    assert "8.00 bps 均值" in rendered


def test_terminal_tables_use_a_shared_sticky_header_only(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        opportunities = response.read().decode()
    with urlopen(scanner_http + "/anomalies") as response:
        anomalies = response.read().decode()
    for html in (opportunities, anomalies):
        assert 'class="table-wrap terminal-table-wrap"' in html
        assert ".terminal-table-wrap thead th" in html
        assert "position: sticky" in html
    assert not re.search(r"\.opportunity-terminal-table[^{}]*td[^{}]*position:\s*sticky", opportunities)
    assert not re.search(r"\.anomaly-terminal-table[^{}]*td[^{}]*position:\s*sticky", anomalies)


def test_dashboard_top_status_uses_compact_age_without_locale_timestamp(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
const request = requests[0];
succeed(request, '2026-10-12T15:21:50+00:00');
await settle();
console.log(JSON.stringify({status:byId('overall').textContent, data:byId('data-as-of').textContent,
  refreshed:byId('refreshed').textContent}));
""", setup=POLLING_SETUP + "location.pathname='/opportunities';\n")
    assert result["status"] == "正常"
    assert result["data"] == "資料延遲 4s"
    assert "2026-10-12T15:21:50" not in result["data"]
    assert "上午" not in result["refreshed"]
    assert "toLocaleTimeString" not in html


def test_pair_top_status_omits_unavailable_data_age(scanner_http):
    with urlopen(scanner_http + "/pair") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
compactDataStatus({data_as_of:'2026-10-12T15:21:50+00:00', generated_at:'2026-10-12T15:21:51+00:00'}, 'HEALTHY');
console.log(JSON.stringify({data:byId('data-as-of').textContent, refreshed:byId('refreshed').textContent}));
""", setup="location.pathname='/pair';\n")
    assert result == {"data": "", "refreshed": "唯讀資料源"}
    assert "data age" not in result["data"]


def test_pair_marker_labels_use_deterministic_non_overlapping_lanes(scanner_http):
    with urlopen(scanner_http + "/pair") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderPair({
  identity:{canonical_symbol:'QQQ',long_venue:'arcus',long_venue_symbol:'QQQ-USD',short_venue:'lighter_robinhood',short_venue_symbol:'QQQ'},
  current:{raw_spread_bps:25,current_net_spread_bps:17,rolling_mean_bps:8,rolling_std_bps:1,deviation_bps:17,signal_duration_seconds:120,long_buy_vwap:100,short_sell_vwap:100.25,round_trip_fee_bps:8,observed_at_skew_seconds:1,freshness_seconds:4,sample_time:'2026-09-23T12:00:00Z'},
  basis:{eligible:true,sample_count:8500,coverage:.997},
  history:[
    {sample_time:'2026-09-23T11:58:00Z',raw_spread_bps:8,segment:0},
    {sample_time:'2026-09-23T11:59:00Z',raw_spread_bps:20,segment:0},
    {sample_time:'2026-09-23T11:59:30Z',raw_spread_bps:22,segment:0},
    {sample_time:'2026-09-23T12:00:00Z',raw_spread_bps:25,segment:0}
  ],
  rolling_mean_series:[{sample_time:'2026-09-23T11:58:00Z',rolling_mean_bps:8,segment:0},{sample_time:'2026-09-23T12:00:00Z',rolling_mean_bps:8,segment:0}],
  lifecycle:{available:true,reference_mean_bps:8,candidate_started_at:'2026-09-23T11:59:00Z',confirmed_at:'2026-09-23T11:59:30Z',candidate_confirmed:true,alerted:false}
});
console.log(JSON.stringify(byId('pair-chart').innerHTML));
""", setup="location.pathname='/pair';\n")
    labels = {label: float(y) for label, y in re.findall(
        r'<text data-marker-label="([^"]+)"[^>]* y="([0-9.]+)"', result
    )}
    assert {"candidate", "confirmation", "reference"} <= labels.keys()
    assert abs(labels["candidate"] - labels["confirmation"]) >= 12
    assert 'data-label-lane="candidate"' in result
    assert 'data-label-lane="confirmation"' in result


def test_anomaly_deviation_uses_neutral_visual_treatment(scanner_http):
    with urlopen(scanner_http + "/anomalies") as response:
        html = response.read().decode()
    rendered = run_ui_script(html, """
renderAnomalies({rows:[{symbol:'QQQ', long_venue:'arcus', long_venue_symbol:'QQQ-USD',
  short_venue:'lighter_robinhood', short_venue_symbol:'QQQ', current_deviation_bps:18,
  current_spread_bps:26, reference_mean_bps:8, mean_alignment_bps:0,
  episode_age_seconds:120, post_confirmation_peak_deviation_bps:22,
  mean_12h_bps:8, mean_24h_bps:8, mean_48h_bps:8, tg_status:'Pending'}]});
console.log(JSON.stringify(byId('anomaly-rows').innerHTML));
""", setup="location.pathname='/anomalies';\n")
    assert 'class="edge-value anomaly-deviation"' in rendered
    assert 'edge-positive' not in rendered


def test_terminal_opportunities_use_six_grouped_decision_columns(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()

    headers = re.search(
        r'<table[^>]*aria-label="套利機會"[^>]*>.*?<thead><tr>(.*?)</tr>',
        html,
        re.S,
    )
    assert headers is not None
    assert re.findall(r"<th>(.*?)</th>", headers.group(1)) == [
        "交易標的", "交易方向", "收斂優勢", "目前價差", "歷史基準", "可成交性",
    ]
    for hook in (
        "opportunity-terminal-table", "route-cell", "edge-cell", "current-cell",
        "baseline-cell", "execution-cell", "filter-primary", "filter-secondary",
        "more-filters", "data-scanner-status",
    ):
        assert hook in html
    for forbidden in (">State<", ">Depth<", ">Std<", ">RT Fee<"):
        assert forbidden not in html


def test_terminal_opportunity_cells_keep_state_and_exact_pair_semantics(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderOpportunities({status:'healthy', rows:[
  {canonical_symbol:'QQQ', long_venue:'arcus', long_venue_symbol:'QQQ-USD',
   short_venue:'lighter_robinhood', short_venue_symbol:'QQQ', state:'eligible', basis_state:'eligible',
   current_raw_spread_bps:26.24, current_net_spread_bps:18.24,
   rolling_mean_bps:8.00, rolling_std_bps:1.25, basis_coverage:0.997,
   convergence_edge_bps:10.24, round_trip_fee_bps:8.00,
   freshness_seconds:4, observed_at_skew_seconds:1.5, primary_depth_status:'available'},
  {canonical_symbol:'SNDK', long_venue:'arcus', long_venue_symbol:'SNDK-USD',
   short_venue:'lighter', short_venue_symbol:'SNDK', state:'pending', basis_state:'pending',
   current_raw_spread_bps:-2, current_net_spread_bps:-10,
   rolling_mean_bps:null, rolling_std_bps:null, basis_coverage:null,
   convergence_edge_bps:-10, round_trip_fee_bps:8,
   freshness_seconds:75, observed_at_skew_seconds:null, primary_depth_status:'unavailable'},
  {canonical_symbol:'NVDA', long_venue:'arcus', long_venue_symbol:'NVDA-USD',
   short_venue:'lighter', short_venue_symbol:'NVDA', state:'stale',
   current_raw_spread_bps:12, current_net_spread_bps:null,
   rolling_mean_bps:7, rolling_std_bps:2, basis_coverage:1,
   convergence_edge_bps:null, round_trip_fee_bps:8,
   freshness_seconds:181, observed_at_skew_seconds:0, primary_depth_status:'unavailable'}
]});
console.log(JSON.stringify(byId('opportunity-rows').innerHTML));
""", setup="location.pathname='/opportunities';\n")
    rendered = result
    assert 'class="route-cell"' in rendered
    assert "做多" in rendered and "做空" in rendered
    assert "+18.24" in rendered and "+26.24" in rendered
    assert "8.00" in rendered and "覆蓋率" in rendered
    assert "延遲 4s" in rendered and "時間差 2s" in rendered
    assert "等待基準建立" in rendered and "資料過期" in rendered
    assert "深度不可用" in rendered
    assert "/pair?symbol=QQQ" in rendered


def test_current_net_uses_neutral_semantics_separate_from_edge(scanner_http):
    with urlopen(scanner_http + "/opportunities") as response:
        html = response.read().decode()
    rendered = run_ui_script(html, """
renderOpportunities({status:'healthy', rows:[{
  canonical_symbol:'BABA', long_venue:'arcus', long_venue_symbol:'BABA-USD',
  short_venue:'lighter', short_venue_symbol:'BABA', state:'eligible',
  current_raw_spread_bps:1.16, current_net_spread_bps:-6.84,
  rolling_mean_bps:-16.73, rolling_std_bps:1, basis_coverage:1,
  convergence_edge_bps:9.89, round_trip_fee_bps:8,
  freshness_seconds:4, observed_at_skew_seconds:1, primary_depth_status:'available'
}]});
console.log(JSON.stringify(byId('opportunity-rows').innerHTML));
""", setup="location.pathname='/opportunities';\n")
    current_cell = re.search(r'<td class="current-cell">(.*?)</td>', rendered, re.S)
    edge_cell = re.search(r'<td class="edge-cell">(.*?)</td>', rendered, re.S)
    assert current_cell is not None and edge_cell is not None
    assert 'class="current-net"' in current_cell.group(1)
    assert "edge-positive" not in current_cell.group(1)
    assert "edge-positive" in edge_cell.group(1)


def test_terminal_anomalies_use_six_grouped_columns(scanner_http):
    with urlopen(scanner_http + "/anomalies") as response:
        html = response.read().decode()
    headers = re.search(
        r'<table[^>]*aria-label="有效異常訊號"[^>]*>.*?<thead><tr>(.*?)</tr>',
        html,
        re.S,
    )
    assert headers is not None
    assert re.findall(r"<th>(.*?)</th>", headers.group(1)) == [
        "交易標的", "交易方向", "偏離幅度", "參考基準", "訊號週期", "歷史比較",
    ]
    for hook in ("anomaly-terminal-table", "anomaly-route", "anomaly-context"):
        assert hook in html


def test_terminal_pair_places_chart_before_secondary_details(scanner_http):
    with urlopen(scanner_http + "/pair") as response:
        html = response.read().decode()
    assert html.index('id="pair-summary"') < html.index('id="pair-chart"')
    assert html.index('id="pair-chart"') < html.index('id="pair-details"')
    for hook in ("pair-kpis", "pair-layout", "pair-main", "pair-right-rail",
                 "pair-long-leg", "pair-short-leg", "pair-trajectory-rail",
                 "pair-lifecycle-rail", "pair-secondary"):
        assert hook in html


def test_terminal_pair_summary_prioritizes_edge_current_baseline_and_execution(scanner_http):
    with urlopen(scanner_http + "/pair") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderPair({
  identity:{canonical_symbol:'QQQ',long_venue:'arcus',long_venue_symbol:'QQQ-USD',short_venue:'lighter_robinhood',short_venue_symbol:'QQQ'},
  current:{raw_spread_bps:26.24,current_net_spread_bps:18.24,rolling_mean_bps:8,rolling_std_bps:1.25,deviation_bps:18.24,theoretical_edge_bps:10.24,signal_duration_seconds:120,long_buy_vwap:100,short_sell_vwap:100.2624,round_trip_fee_bps:8,observed_at_skew_seconds:1.5,freshness_seconds:4,sample_time:'2026-09-23T12:00:00Z'},
  basis:{eligible:true,sample_count:8500,coverage:.997},
  history:[{sample_time:'2026-09-23T11:59:00Z',raw_spread_bps:8,segment:0},{sample_time:'2026-09-23T12:00:00Z',raw_spread_bps:26.24,segment:0}],
  rolling_mean_series:[{sample_time:'2026-09-23T11:59:00Z',rolling_mean_bps:8,segment:0},{sample_time:'2026-09-23T12:00:00Z',rolling_mean_bps:8,segment:0}],
  lifecycle:{available:true,reference_mean_bps:8,candidate_started_at:'2026-09-23T11:58:00Z',confirmed_at:'2026-09-23T11:59:00Z',candidate_confirmed:true,alerted:false}
});
console.log(JSON.stringify({summary:byId('pair-summary').innerHTML, secondary:byId('pair-secondary').innerHTML}));
""", setup="location.pathname='/pair';\n")
    labels = re.findall(r'class="metric-label">([^<]+)', result["summary"])
    assert labels[:4] == ["扣費後預估收斂優勢", "目前價差", "歷史基準", "可成交性"]
    assert "26.24" in result["secondary"] or "26.24" in result["summary"]
    assert "做多端 1 萬美元買入 VWAP" in result["secondary"]
    assert "做空端 1 萬美元賣出 VWAP" in result["secondary"]


def test_status_ui_distinguishes_latest_rows_from_configured_feeds(scanner_http):
    with urlopen(scanner_http + "/status") as response:
        html = response.read().decode()
    assert "最新採樣筆數／已設定資料源" in html
    assert "正常資料源／VWAP 就緒" in html


def test_anomalies_ui_uses_grouped_terminal_presentation(scanner_http):
    with urlopen(scanner_http + "/anomalies") as response:
        html = response.read().decode()
    for hook in ("anomaly-terminal-table", "anomaly-route", "anomaly-context"):
        assert hook in html


def test_opportunities_http_filters_by_basis_state_and_exposes_the_filter(scanner_http):
    all_rows = http_json(scanner_http, "/api/opportunities")["rows"]
    expected = [row for row in all_rows if row["basis_state"] == "eligible"]

    filtered = http_json(scanner_http, "/api/opportunities?basis_state=eligible")

    assert [row["canonical_symbol"] for row in filtered["rows"]] == [
        row["canonical_symbol"] for row in expected
    ]
    assert all(row["basis_state"] == "eligible" for row in filtered["rows"])
    with urlopen(scanner_http + "/") as response:
        html = response.read().decode()
    assert 'id="basis_state"' in html
    assert "基準狀態" in html


@pytest.mark.parametrize(("query", "count", "first_symbol"), [
    ("symbol=%20eth%20", 2, "ETH"), ("long_venue=%20LIGHTER%20", 2, "BTC"),
    ("short_venue=HYPERLIQUID", 2, "BTC"), ("max_std=99", 2, "BTC"),
    ("max_std=100.01", 4, "BTC"), ("min_deviation=200", 1, "BTC"),
    ("min_duration=60", 1, "BTC"), ("min_edge=200", 1, "BTC"),
    ("active_only=true", 2, "BTC"),
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
    "basis_state=other", "basis_state=",
    "limit=0", "limit=-1", "limit=513", "limit=1.5", "limit=NaN", "limit=",
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
    elif path in ("/", "/opportunities"):
        for anchor in ("filters", "opportunity-rows", "data-as-of"):
            assert f'id="{anchor}"' in html
        for column in ("交易標的", "交易方向", "目前價差", "歷史基準", "收斂優勢", "可成交性"):
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
    sample_age_seconds:4, scanner_sample_age_seconds:4,
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
    assert [view["overall"] for view in result["rendered"]] == ["正常"] * 3
    for cycle, view in enumerate(result["rendered"]):
        assert view["sample"] == "資料延遲 4s"
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
    assert result["rendered"]["overall"] == "正常"
    assert result["rendered"]["sample"] == "資料延遲 4s"
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
    assert result == {"failed": "儀表板讀取失敗", "overall": "正常",
                      "sample": "資料延遲 4s", "requests": 2}


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
    document.getElementById('min_edge').value = '200';
const query = filterQuery();
console.log(JSON.stringify({symbol:document.getElementById('symbol').value, active:document.getElementById('active_only').checked, query:query.toString()}));
""", setup="location.pathname = '/opportunities';\n")
    assert result["symbol"] == "ETH" and result["active"] is True
    assert parse_qs(result["query"]) == {"symbol": ["ETH"], "active_only": ["true"], "min_edge": ["200"]}


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
const secondary = byId('pair-secondary').innerHTML;
const chart = byId('pair-chart').innerHTML;
byId('pair-range').value = '7d';
byId('pair-range').listeners.change();
console.log(JSON.stringify({summary, secondary, chart, search:location.search, requests:requests.map(r=>r.url)}));
""", setup=POLLING_SETUP + "location.pathname='/pair'; location.search=" + json.dumps("?" + query) + "; const payload=" + json.dumps(payload) + ";\n")
    combined = result["summary"] + result["secondary"]
    for label in ("原始價差", "24 小時均值", "24 小時標準差", "偏離幅度", "訊號持續時間", "做多端 1 萬美元買入 VWAP", "做空端 1 萬美元賣出 VWAP", "來回手續費", "觀測時間差", "資料延遲", "採樣時間", "訊號生命週期"):
        assert label in combined
    assert "500.00" in combined and "294.00" in combined
    assert "<svg" in result["chart"] and 'data-series="raw"' in result["chart"]
    assert 'data-series="mean"' in result["chart"] and 'data-series="current"' in result["chart"]
    assert parse_qs(result["search"][1:]) == {**{k: [v] for k, v in PAIR_QUERY.items()}, "range": ["7d"]}
    assert all(url.startswith("/api/pair?") for url in result["requests"])
    assert parse_qs(urlparse(result["requests"][-1]).query)["range"] == ["7d"]


def test_pair_ui_prioritizes_decision_metrics_and_keeps_lifecycle_markers(scanner_http):
    with urlopen(scanner_http + "/pair?" + urlencode(PAIR_QUERY)) as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderPair({
  identity:{canonical_symbol:'BTC',long_venue:'lighter',long_venue_symbol:'BTC',short_venue:'hyperliquid',short_venue_symbol:'BTC'},
  current:{raw_spread_bps:5,current_net_spread_bps:1,rolling_mean_bps:2,rolling_std_bps:1,deviation_bps:3,theoretical_edge_bps:4,
    signal_duration_seconds:60,long_buy_vwap:100,short_sell_vwap:101,round_trip_fee_bps:4,observed_at_skew_seconds:0,
    freshness_seconds:1,sample_time:'2026-09-23T12:00:00Z'},
  basis:{eligible:true, sample_count:10, coverage:1},
  history:[
    {sample_time:'2026-09-23T11:58:00Z',raw_spread_bps:2,segment:0},
    {sample_time:'2026-09-23T11:59:00Z',raw_spread_bps:3,segment:0},
    {sample_time:'2026-09-23T12:00:00Z',raw_spread_bps:5,segment:0}],
  rolling_mean_series:[
    {sample_time:'2026-09-23T11:58:00Z',rolling_mean_bps:2,segment:0},
    {sample_time:'2026-09-23T11:59:00Z',rolling_mean_bps:2,segment:0},
    {sample_time:'2026-09-23T12:00:00Z',rolling_mean_bps:2,segment:0}],
  lifecycle:{available:true,reference_mean_bps:2,candidate_started_at:'2026-09-23T11:58:00Z',confirmed_at:'2026-09-23T11:59:00Z'}
});
console.log(JSON.stringify({summary:byId('pair-summary').innerHTML, chart:byId('pair-chart').innerHTML}));
""", setup="location.pathname='/pair';\n")
    labels = re.findall(r'class="metric-label">([^<]+)', result["summary"])
    assert labels[:3] == [
        "扣費後預估收斂優勢",
        "目前價差",
        "歷史基準",
    ]
    assert 'data-marker="current"' in result["chart"]
    assert 'data-marker="candidate"' in result["chart"]
    assert 'data-marker="confirmation"' in result["chart"]
    assert not re.search(r'<circle(?![^>]*data-marker=)', result["chart"])
    assert result["chart"].count('data-axis-tick=') >= 2


def test_pair_ui_explains_disabled_persisted_route_without_guessing_identity(scanner_http):
    with urlopen(scanner_http + "/pair") as response:
        html = response.read().decode()
    result = run_ui_script(html, """
renderPair({identity:{canonical_symbol:'USO',long_venue:'lighter_robinhood',long_venue_symbol:'USO',short_venue:'arcus',short_venue_symbol:'USO-USD'},
  errors:['pair identity is not an enabled configured mapping'], current:null, history:[], rolling_mean_series:[]});
console.log(JSON.stringify(byId('pair-identity').textContent + '|' + byId('pair-chart').textContent));
""", setup="location.pathname='/pair';\n")
    assert "已不在目前的監控清單中" in result


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
    assert "—" in result["summary"]


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



def test_pair_api_exposes_display_only_trajectory_context(scanner_http):
    query = urlencode(
        {
            "symbol": "BTC",
            "long_venue": "lighter",
            "long_venue_symbol": "BTC-USD",
            "short_venue": "hyperliquid",
            "short_venue_symbol": "xyz:BTC",
            "range": "24h",
        }
    )
    payload = http_json(scanner_http, "/api/pair?" + query)
    trajectory = payload["trajectory"]

    assert trajectory["available"] is True
    assert trajectory["reference_kind"] == "rolling_24h"
    assert trajectory["reference_bps"] == pytest.approx(payload["basis"]["mean_bps"])
    assert set(trajectory["windows"]) == {"1m", "5m", "15m"}
    assert all(
        {
            "available",
            "seconds",
            "prior_sample_time",
            "spread_change_bps",
            "edge_change_bps",
            "state",
        }
        <= entry.keys()
        for entry in trajectory["windows"].values()
    )
    assert trajectory["edge_from_recent_peak_bps"] >= 0


def test_pair_ui_renders_compact_trajectory_without_creating_entry_rule(scanner_http):
    with urlopen(scanner_http + "/pair") as response:
        html = response.read().decode()

    rendered = run_ui_script(
        html,
        """
renderPair({
  identity:{canonical_symbol:'QQQ',long_venue:'arcus',long_venue_symbol:'QQQ-USD',short_venue:'lighter_robinhood',short_venue_symbol:'QQQ'},
  current:{raw_spread_bps:26,current_net_spread_bps:18,rolling_mean_bps:8,rolling_std_bps:1,deviation_bps:18,signal_duration_seconds:120,long_buy_vwap:100,short_sell_vwap:100.26,primary_depth_status:'available',round_trip_fee_bps:8,observed_at_skew_seconds:1,freshness_seconds:4,sample_time:'2026-09-23T12:00:00Z'},
  basis:{eligible:true,sample_count:8500,coverage:.997},
  history:[
    {sample_time:'2026-09-23T11:45:00Z',raw_spread_bps:20,segment:0},
    {sample_time:'2026-09-23T12:00:00Z',raw_spread_bps:26,segment:0}
  ],
  rolling_mean_series:[
    {sample_time:'2026-09-23T11:45:00Z',rolling_mean_bps:8,segment:0},
    {sample_time:'2026-09-23T12:00:00Z',rolling_mean_bps:8,segment:0}
  ],
  lifecycle:{available:false},
  trajectory:{
    available:true,
    reference_bps:8,
    reference_kind:'rolling_24h',
    current_edge_bps:10,
    current_state:'active',
    recent_peak_edge_bps:13,
    edge_from_recent_peak_bps:3,
    recent_peak_at:'2026-09-23T11:58:00Z',
    windows:{
      '1m':{available:true,edge_change_bps:2,state:'widening'},
      '5m':{available:true,edge_change_bps:-1.5,state:'reverting'},
      '15m':{available:true,edge_change_bps:0,state:'flat'}
    }
  },
  anomaly_markers:[]
});
console.log(JSON.stringify(byId('pair-trajectory-rail').innerHTML));
""",
        setup="location.pathname='/pair';\n",
    )

    assert "價差優勢走勢" in rendered
    assert "+2.00 bps · 持續擴大" in rendered
    assert "-1.50 bps · 正在收斂" in rendered
    assert "0.00 bps · 大致持平" in rendered
    assert "距高點回落" in rendered
    assert "3.00 bps" in rendered
    assert "滾動 24 小時 基準" in rendered
    assert "目前收斂優勢" in rendered
    assert "+10.00 bps" in rendered
    assert "15 分鐘高點" in rendered
    assert "+13.00 bps" in rendered
    assert "僅供觀察" in rendered
    assert "entry" not in rendered.lower()
