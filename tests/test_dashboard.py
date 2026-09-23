from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

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
    observed_at: datetime = NOW,
    buy_1k_vwap: float | None = 100.0,
    sell_1k_vwap: float | None = 101.0,
    buy_5k_vwap: float | None = 100.0,
    sell_5k_vwap: float | None = 101.0,
    buy_10k_vwap: float | None = 100.0,
    sell_10k_vwap: float | None = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=observed_at,
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
                observed_at=NOW + timedelta(seconds=1),
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
