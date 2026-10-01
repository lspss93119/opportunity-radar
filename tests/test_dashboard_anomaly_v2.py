from __future__ import annotations

import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

from radar.config import (
    AnomalyV2Config,
    MarketConfig,
    MonitorConfig,
    RadarConfig,
    SpreadMonitorConfig,
)
from radar.dashboard import DashboardStatusService, create_dashboard_server
from radar.dashboard_data import DashboardQueryService
from radar.models import MarketSnapshot
from radar.storage.parquet import ParquetStorage
from radar.storage.sqlite import SQLiteRuntimeStore


NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
IDENTITY = {
    "canonical_symbol": "QQQ",
    "long_venue": "arcus",
    "long_venue_symbol": "QQQ-USD",
    "short_venue": "lighter_robinhood",
    "short_venue_symbol": "QQQ",
}


def make_config() -> RadarConfig:
    return RadarConfig(
        markets=[
            MarketConfig(
                venue=IDENTITY["long_venue"],
                venue_symbol=IDENTITY["long_venue_symbol"],
                canonical_symbol=IDENTITY["canonical_symbol"],
            ),
            MarketConfig(
                venue=IDENTITY["short_venue"],
                venue_symbol=IDENTITY["short_venue_symbol"],
                canonical_symbol=IDENTITY["canonical_symbol"],
            ),
        ],
        fees_bps={"arcus": 2.25, "lighter_robinhood": 0.0},
        monitors=MonitorConfig(
            spread=SpreadMonitorConfig(
                anomaly_v2=AnomalyV2Config(enabled=True),
            )
        ),
    )


def write_market_data(data_root: Path) -> None:
    storage = ParquetStorage(data_root)
    for venue, venue_symbol, buy, sell in (
        ("arcus", "QQQ-USD", 100.0, 100.0),
        ("lighter_robinhood", "QQQ", 100.0, 101.25),
    ):
        storage.append(
            MarketSnapshot(
                sample_time=NOW,
                observed_at=NOW - timedelta(seconds=1),
                venue=venue,
                venue_symbol=venue_symbol,
                canonical_symbol="QQQ",
                best_bid=99.0,
                best_bid_size=100.0,
                best_ask=100.0,
                best_ask_size=100.0,
                buy_10k_vwap=buy,
                sell_10k_vwap=sell,
            )
        )
    storage.flush(now=NOW)


def v2_episode_state() -> dict[str, object]:
    confirmed_at = NOW - timedelta(seconds=60)
    return {
        "episode": {
            "pair_key": IDENTITY,
            "episode_id": "qqq-anomaly-1",
            "candidate_started_at": (NOW - timedelta(seconds=120)).isoformat(),
            "reference_mean_bps": 100.0,
            "reference_std_bps": 2.0,
            "current_spread_bps": 125.0,
            "current_deviation_from_reference_bps": 25.0,
            "lifetime_peak_spread_bps": 125.0,
            "lifetime_peak_deviation_bps": 25.0,
            "lifetime_peak_at": NOW.isoformat(),
            "last_seen_at": NOW.isoformat(),
            "confirmed_at": confirmed_at.isoformat(),
            "confirmation_spread_bps": 120.0,
            "confirmation_deviation_bps": 20.0,
            "ended_at": None,
        },
        "candidate": {
            **IDENTITY,
            "long_buy_vwap": 100.0,
            "short_sell_vwap": 101.25,
            "raw_spread_bps": 125.0,
            "net_spread_bps": 122.75,
        },
        "initial_event_emitted": True,
    }


def write_v2_runtime(runtime_db: Path, *, malformed: bool = False) -> None:
    with SQLiteRuntimeStore(runtime_db) as store:
        store.set_monitor_state(
            "spread",
            "anomaly_episodes_v2",
            {"qqq-anomaly-1": {"broken": True}} if malformed else {
                "qqq-anomaly-1": v2_episode_state()
            },
            updated_at=NOW,
        )
        if not malformed:
            store.set_monitor_state(
                "spread",
                "anomaly_notifications_v2",
                {
                    "qqq-anomaly-1": {
                        "eligibility": True,
                        "eligibility_reason": "eligible",
                        "mean_alignment_bps": 2.0,
                        "initial_sent_at": NOW.isoformat(),
                        "context": {
                            "12h_mean_bps": 99.0,
                            "24h_mean_bps": 100.0,
                            "48h_mean_bps": 101.0,
                        },
                    }
                },
                updated_at=NOW,
            )
            store.append_opportunity(
                "spread",
                "qqq-anomaly-1:v2:confirmed",
                "anomaly_confirmed",
                {
                    **IDENTITY,
                    "episode_id": "qqq-anomaly-1",
                    "net_spread_bps": 122.75,
                },
                occurred_at=confirmed_at(),
            )


def confirmed_at() -> datetime:
    return NOW - timedelta(seconds=60)


def test_anomalies_page_reads_persisted_v2_state_and_exact_identity(tmp_path, monkeypatch):
    write_market_data(tmp_path / "data")
    runtime_db = tmp_path / "runtime.sqlite3"
    write_v2_runtime(runtime_db)
    config = make_config()

    from radar.monitors.spread.monitor import SpreadMonitor

    async def fail_if_evaluated(*args, **kwargs):
        raise AssertionError("dashboard must not evaluate SpreadMonitor")

    monkeypatch.setattr(SpreadMonitor, "evaluate", fail_if_evaluated)
    service = DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    )

    payload = service.get_anomalies(now=NOW)
    assert payload["status"] == "healthy"
    assert len(payload["rows"]) == 1
    row = payload["rows"][0]
    assert row["key"] == tuple(IDENTITY.values())
    assert row["tg_eligible"] is True
    assert row["current_deviation_bps"] == 25.0
    assert service.get_anomalies(eligible_only=True, now=NOW)["rows"] == [row]

    pair = service.get_pair(
        canonical_symbol="QQQ",
        long_venue="arcus",
        long_venue_symbol="QQQ-USD",
        short_venue="lighter_robinhood",
        short_venue_symbol="QQQ",
        range_name="1h",
        now=NOW,
    )
    assert pair["lifecycle"]["active"] is True
    assert pair["lifecycle"]["tg_status"] == "Eligible / sent"
    assert pair["anomaly_markers"][0]["event_type"] == "anomaly_confirmed"


def test_malformed_v2_state_is_degraded_and_fail_closed(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_v2_runtime(runtime_db, malformed=True)
    service = DashboardQueryService(
        make_config(),
        data_root=tmp_path / "data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    )

    payload = service.get_anomalies(now=NOW)
    assert payload["rows"] == []
    assert payload["status"] == "degraded"
    assert payload["errors"]


def test_anomalies_http_route_and_page_are_available(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_v2_runtime(runtime_db)
    service = DashboardStatusService(
        make_config(),
        data_root=tmp_path / "data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    )
    server = create_dashboard_server(service, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        with urlopen(f"http://{host}:{port}/") as response:
            root_html = response.read().decode()
            assert response.status == 200
            assert 'id="anomalies-view"' in root_html
        with urlopen(f"http://{host}:{port}/opportunities") as response:
            all_pairs_html = response.read().decode()
            assert response.status == 200
            assert 'id="opportunities-view"' in all_pairs_html
        with urlopen(f"http://{host}:{port}/anomalies") as response:
            html = response.read().decode()
            assert response.status == 200
            assert 'id="anomalies-view"' in html
            assert 'href="/anomalies"' in html
            assert 'data-marker="' in html
            assert "'confirmation'" in html
            assert "'peak'" in html
        with urlopen(f"http://{host}:{port}/api/anomalies?eligible_only=true") as response:
            payload = json.loads(response.read())
            assert response.status == 200
            assert len(payload["rows"]) == 1
            assert payload["rows"][0]["canonical_symbol"] == "QQQ"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_anomaly_filters_are_exposed_in_primary_view():
    from radar.dashboard import HTML

    assert 'id="anomaly-filters"' in HTML
    assert 'id="anomaly-symbol"' in HTML
    assert 'id="anomaly-long-venue"' in HTML
    assert 'id="anomaly-short-venue"' in HTML
    assert 'id="anomaly-eligible-only"' in HTML
