from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import ValidationError

from radar.alerts.spread import format_anomaly_alert, parse_anomaly_alert
from radar.config import AnomalyV2Config, SpreadMonitorConfig
from radar.history.spread import AnomalyConfirmationContext
from radar.monitors.spread.models import SpreadPairKey
from radar.monitors.spread.monitor import SpreadMonitor
from radar.storage.sqlite import SQLiteRuntimeStore
from test_anomaly_monitor_v2 import START, state_at

KEY = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")


def make_monitor(anchors, *, store=None) -> SpreadMonitor:
    config = SpreadMonitorConfig(
        primary_size_usd=10_000,
        stale_after_seconds=120,
        anomaly_v2=AnomalyV2Config(
            enabled=True,
            deviation_bps=15.0,
            confirmation_seconds=0,
            return_band_bps=5.0,
            max_gap_seconds=20,
            anchors=anchors,
            median_window_seconds=7_200,
        ),
    )
    return SpreadMonitor(
        config,
        {"arcus": 2.25, "lighter_robinhood": 0.0},
        runtime_store=store,
        basis_window_seconds=12_000,
        basis_min_observations=4,
        basis_expected_interval_seconds=10,
    )


def prime(monitor: SpreadMonitor, *, older: float, recent: float) -> None:
    """12000s of history: the last 2h at ``recent``, everything before at ``older``."""
    points = []
    for index in range(1_200):
        seconds_ago = 12_000 - index * 10
        points.append(
            (START - timedelta(seconds=seconds_ago), recent if seconds_ago <= 7_200 else older)
        )
    monitor.hydrate_history({KEY: tuple(points)})


async def run(monitor: SpreadMonitor, spread_bps: float = 116.0):
    return await monitor.evaluate(START, state_at(START, spread_bps=spread_bps))


def test_default_anchor_is_mean_only():
    assert AnomalyV2Config().anchors == ["mean_24h"]
    monitor = make_monitor(["mean_24h"])
    assert list(monitor._anomaly_lifecycles) == ["mean_24h"]
    assert monitor._median_by_key == {}


@pytest.mark.parametrize("anchors", [[], ["mean_24h", "mean_24h"], ["mode_1h"]])
def test_anchor_config_validation(anchors):
    with pytest.raises(ValidationError):
        AnomalyV2Config(anchors=anchors)


@pytest.mark.asyncio
async def test_mean_only_keeps_previous_ids_state_key_and_labels_anchor(tmp_path):
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        monitor = make_monitor(["mean_24h"], store=store)
        prime(monitor, older=100.0, recent=100.0)
        alerts = await run(monitor)

        assert [a.payload["event_kind"] for a in alerts] == ["anomaly_initial"]
        payload = alerts[0].payload
        assert payload["anchor"] == "mean_24h"
        assert ":mean_24h" not in payload["episode_id"]
        assert alerts[0].event_id.endswith(":v2:confirmed")
        assert store.get_monitor_state("spread", "anomaly_episodes_v2")
        assert store.get_monitor_state("spread", "anomaly_episodes_v2_median_2h") is None


@pytest.mark.asyncio
async def test_median_anchor_triggers_where_the_mean_does_not():
    monitor = make_monitor(["mean_24h", "median_2h"])
    # Mean over 12000s is 110 (deviation 6); the 2h median is 90 (deviation 26).
    prime(monitor, older=140.0, recent=90.0)

    alerts = await run(monitor)

    assert [(a.payload["anchor"], a.payload["event_kind"]) for a in alerts] == [
        ("median_2h", "anomaly_initial")
    ]
    payload = alerts[0].payload
    assert payload["reference_mean_bps"] == pytest.approx(90.0)
    assert payload["episode_id"].endswith(":median_2h")
    assert ":median_2h:v2:" in alerts[0].event_id


@pytest.mark.asyncio
async def test_both_anchors_alert_separately_and_restore_after_restart(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        monitor = make_monitor(["mean_24h", "median_2h"], store=store)
        prime(monitor, older=100.0, recent=100.0)
        alerts = await run(monitor)

        assert sorted(a.payload["anchor"] for a in alerts) == ["mean_24h", "median_2h"]
        assert len({a.event_id for a in alerts}) == 2
        assert store.get_monitor_state("spread", "anomaly_episodes_v2")
        assert store.get_monitor_state("spread", "anomaly_episodes_v2_median_2h")

    with SQLiteRuntimeStore(database) as store:
        restarted = make_monitor(["mean_24h", "median_2h"], store=store)
        assert len(restarted.active_anomaly_episodes) == 2
        prime(restarted, older=100.0, recent=100.0)
        # Episodes are already confirmed: no duplicate initial alerts.
        assert await run(restarted) == []


@pytest.mark.asyncio
async def test_alert_text_names_the_anchor():
    monitor = make_monitor(["median_2h"])
    prime(monitor, older=140.0, recent=90.0)
    alert = (await run(monitor))[0]

    message = format_anomaly_alert(parse_anomaly_alert(alert), AnomalyConfirmationContext.empty())
    assert "錨點：2h 中位數" in message

    legacy = dict(alert.payload)
    del legacy["anchor"]
    from radar.monitors.base import AlertRequest

    legacy_alert = AlertRequest(
        monitor=alert.monitor, event_id=alert.event_id, created_at=alert.created_at, payload=legacy
    )
    assert "錨點：24h 平均" in format_anomaly_alert(
        parse_anomaly_alert(legacy_alert), AnomalyConfirmationContext.empty()
    )
