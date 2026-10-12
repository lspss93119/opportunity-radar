from __future__ import annotations

import json
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

import pytest

from radar.config import (
    AnomalyV2Config,
    MarketConfig,
    MonitorConfig,
    RadarConfig,
    SpreadMonitorConfig,
)
from radar.dashboard import DashboardStatusService, create_dashboard_server
from radar.dashboard_data import DashboardQueryService, OpportunitiesFilters
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


def make_config(*, anomaly_enabled: bool = True) -> RadarConfig:
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
                anomaly_v2=AnomalyV2Config(enabled=anomaly_enabled),
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


def write_monitor_state(
    runtime_db: Path,
    state_key: str,
    state: object,
    *,
    updated_at: datetime = NOW,
) -> None:
    with SQLiteRuntimeStore(runtime_db) as store:
        store.set_monitor_state("spread", state_key, state, updated_at=updated_at)


def write_legacy_state(
    runtime_db: Path,
    *,
    confirmed: bool,
    alerted: bool,
    updated_at: datetime,
) -> None:
    first_seen = NOW - timedelta(seconds=120)
    last_seen = NOW - timedelta(seconds=30)
    write_monitor_state(
        runtime_db,
        "episodes",
        {
            "legacy-episode": {
                "episode_id": "legacy-episode",
                "key": IDENTITY,
                "first_seen_at": first_seen.isoformat(),
                "last_seen_at": last_seen.isoformat(),
                "candidate": {
                    **IDENTITY,
                    "net_spread_bps": 100.0,
                },
                "candidate_confirmed": confirmed,
                "candidate_confirmed_at": (
                    (NOW - timedelta(seconds=60)).isoformat()
                    if confirmed
                    else None
                ),
                "alert_condition_since": (
                    (NOW - timedelta(seconds=90)).isoformat()
                    if confirmed
                    else None
                ),
                "alerted": alerted,
            }
        },
        updated_at=updated_at,
    )


def replace_state_json(runtime_db: Path, state_key: str, state_json: str) -> None:
    with sqlite3.connect(runtime_db) as connection:
        connection.execute(
            """
            UPDATE monitor_state
            SET state_json = ?
            WHERE monitor_name = 'spread' AND state_key = ?
            """,
            (state_json, state_key),
        )
        connection.commit()


def confirmed_at() -> datetime:
    return NOW - timedelta(seconds=60)


def status_for(
    tmp_path: Path,
    *,
    config: RadarConfig,
    runtime_db: Path,
) -> dict[str, object]:
    write_market_data(tmp_path / "data")
    return DashboardQueryService(
        config,
        data_root=tmp_path / "data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    ).get_status(now=NOW)


def test_status_uses_fresh_v2_heartbeat_and_drops_stale_legacy_counts(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_legacy_state(
        runtime_db,
        confirmed=True,
        alerted=True,
        updated_at=NOW - timedelta(hours=1),
    )
    write_monitor_state(runtime_db, "anomaly_episodes_v2", {}, updated_at=NOW)

    status = status_for(tmp_path, config=make_config(), runtime_db=runtime_db)

    assert status["heartbeat"] == {
        "status": "healthy",
        "updated_at": NOW.isoformat(),
        "age_seconds": 0.0,
    }
    assert status["radar_heartbeat"] == status["heartbeat"]
    assert status["overall"]["status"] == "healthy"
    assert status["overall"]["active_episodes"] == 0
    assert status["overall"]["confirmed_candidates"] == 0
    assert status["overall"]["active_alerted_episodes"] == 0
    assert status["episodes"] == []


@pytest.mark.parametrize(
    ("age_seconds", "expected"),
    [(31, "degraded"), (61, "down")],
)
def test_status_uses_v2_heartbeat_age_boundaries(tmp_path, age_seconds, expected):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_monitor_state(
        runtime_db,
        "anomaly_episodes_v2",
        {},
        updated_at=NOW - timedelta(seconds=age_seconds),
    )

    status = status_for(tmp_path, config=make_config(), runtime_db=runtime_db)

    assert status["heartbeat"]["status"] == expected


def test_v2_heartbeat_missing_does_not_fallback_to_legacy(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_legacy_state(
        runtime_db,
        confirmed=True,
        alerted=True,
        updated_at=NOW,
    )

    status = status_for(tmp_path, config=make_config(), runtime_db=runtime_db)

    assert status["heartbeat"]["status"] == "down"
    assert status["heartbeat"]["updated_at"] is None


@pytest.mark.parametrize(
    "updated_at",
    ["not-a-timestamp", (NOW + timedelta(seconds=1)).isoformat()],
)
def test_v2_heartbeat_invalid_or_future_timestamp_fails_closed(tmp_path, updated_at):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_monitor_state(runtime_db, "anomaly_episodes_v2", {}, updated_at=NOW)
    with sqlite3.connect(runtime_db) as connection:
        connection.execute(
            """
            UPDATE monitor_state
            SET updated_at = ?
            WHERE monitor_name = 'spread' AND state_key = 'anomaly_episodes_v2'
            """,
            (updated_at,),
        )
        connection.commit()

    status = status_for(tmp_path, config=make_config(), runtime_db=runtime_db)

    assert status["heartbeat"]["status"] == "down"


def test_legacy_mode_ignores_leftover_v2_state(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_legacy_state(
        runtime_db,
        confirmed=False,
        alerted=False,
        updated_at=NOW,
    )
    write_monitor_state(
        runtime_db,
        "anomaly_episodes_v2",
        {"stale": {"not": "used"}},
        updated_at=NOW - timedelta(hours=1),
    )

    status = status_for(
        tmp_path,
        config=make_config(anomaly_enabled=False),
        runtime_db=runtime_db,
    )

    assert status["heartbeat"]["status"] == "healthy"
    assert status["heartbeat"]["updated_at"] == NOW.isoformat()
    assert status["overall"]["active_episodes"] == 1


def test_legacy_mode_stale_heartbeat_remains_down(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_legacy_state(
        runtime_db,
        confirmed=False,
        alerted=False,
        updated_at=NOW - timedelta(seconds=61),
    )

    status = status_for(
        tmp_path,
        config=make_config(anomaly_enabled=False),
        runtime_db=runtime_db,
    )

    assert status["heartbeat"]["status"] == "down"


def test_malformed_selected_v2_state_is_not_exposed_as_lifecycle(tmp_path):
    runtime_db = tmp_path / "runtime.sqlite3"
    write_legacy_state(
        runtime_db,
        confirmed=True,
        alerted=True,
        updated_at=NOW,
    )
    write_monitor_state(runtime_db, "anomaly_episodes_v2", {}, updated_at=NOW)
    replace_state_json(runtime_db, "anomaly_episodes_v2", "{malformed")

    status = status_for(tmp_path, config=make_config(), runtime_db=runtime_db)

    assert status["episodes"] == []
    assert status["overall"]["active_episodes"] == 0
    assert status["overall"]["status"] == "degraded"
    assert status["errors"]


def test_opportunities_use_v2_lifecycle_instead_of_stale_legacy_state(tmp_path):
    write_market_data(tmp_path / "data")
    runtime_db = tmp_path / "runtime.sqlite3"
    write_legacy_state(
        runtime_db,
        confirmed=False,
        alerted=False,
        updated_at=NOW - timedelta(hours=1),
    )
    write_v2_runtime(runtime_db)
    service = DashboardQueryService(
        make_config(),
        data_root=tmp_path / "data",
        runtime_db=runtime_db,
        clock=lambda: NOW,
    )

    payload = service.get_opportunities(OpportunitiesFilters(), now=NOW)

    row = next(
        row
        for row in payload["rows"]
        if (
            row["canonical_symbol"] == "QQQ"
            and row["long_venue"] == "arcus"
            and row["short_venue"] == "lighter_robinhood"
        )
    )
    assert row["active"] is True
    assert row["candidate_confirmed"] is True
    assert row["alerted"] is True
    assert row["signal_duration_seconds"] == 60


def test_status_http_api_uses_selected_v2_heartbeat(tmp_path):
    write_market_data(tmp_path / "data")
    runtime_db = tmp_path / "runtime.sqlite3"
    write_monitor_state(runtime_db, "anomaly_episodes_v2", {}, updated_at=NOW)
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
        with urlopen(f"http://{host}:{port}/api/status") as response:
            payload = json.loads(response.read())
        assert payload["heartbeat"]["status"] == "healthy"
        assert payload["radar_heartbeat"] == payload["heartbeat"]
        assert payload["overall"]["status"] == "healthy"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


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
