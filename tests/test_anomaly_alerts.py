from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.alerts.chart import render_anomaly_chart
from radar.alerts.spread import format_anomaly_alert
from radar.alerts.spread import SpreadAlertProcessor
from radar.history.spread import (
    AnomalyConfirmationContext,
    AnomalyWindowStats,
    HistoricalSpreadContext,
    WindowStats,
)
from radar.monitors.base import AlertRequest
from radar.storage.sqlite import SQLiteRuntimeStore


NOW = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def context(*means: float, eligible: bool = True) -> AnomalyConfirmationContext:
    stats = tuple(
        AnomalyWindowStats(100, mean, 2.0, mean - 1, mean + 1, eligible)
        for mean in means
    )
    return AnomalyConfirmationContext(*stats)


def alert(event_kind: str = "anomaly_initial") -> AlertRequest:
    candidate = NOW - timedelta(seconds=60)
    return AlertRequest(
        monitor="spread",
        event_id=f"episode:v2:{event_kind}",
        created_at=NOW,
        payload={
            "event_kind": event_kind,
            "episode_id": "episode",
            "canonical_symbol": "QQQ",
            "long_venue": "arcus",
            "long_venue_symbol": "QQQ-USD",
            "short_venue": "lighter_robinhood",
            "short_venue_symbol": "QQQ",
            "primary_size_usd": 10_000,
            "sample_time": NOW.isoformat(),
            "candidate_started_at": candidate.isoformat(),
            "confirmed_at": NOW.isoformat(),
            "reference_mean_bps": 9.0,
            "reference_std_bps": 100.0,
            "confirmation_spread_bps": 30.0,
            "confirmation_deviation_bps": 21.0,
            "lifetime_peak_spread_bps": 35.0,
            "lifetime_peak_deviation_bps": 26.0,
            "lifetime_peak_at": NOW.isoformat(),
            "post_confirmation_peak_spread_bps": 35.0,
            "post_confirmation_peak_deviation_bps": 26.0,
            "post_confirmation_peak_at": NOW.isoformat(),
            "current_spread_bps": 30.0,
            "current_deviation_bps": 21.0,
            "current_live_mean_bps": 9.0,
            "current_live_std_bps": 2.0,
            "ended_at": None,
            "end_spread_bps": None,
            "end_deviation_bps": None,
            "resolution_reason": None,
            "long_buy_vwap": 100.0,
            "short_sell_vwap": 100.3,
            "raw_spread_bps": 30.0,
            "long_fee_bps": 2.25,
            "short_fee_bps": 0.0,
            "net_spread_bps": 27.75,
            "observed_at_skew_seconds": 0.5,
            "funding_context": {"long": None, "short": None},
        },
    )


class FakeHistory:
    def __init__(self, supplied: AnomalyConfirmationContext) -> None:
        self.supplied = supplied

    def query_anomaly_context(self, **kwargs):
        return self.supplied

    def query(self, **kwargs):
        return HistoricalSpreadContext(
            points_7d=(),
            stats_7d=WindowStats(0, None),
            stats_30d=WindowStats(0, None),
            stats_90d=WindowStats(0, None),
        )


class FakeTelegram:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.texts: list[str] = []
        self.charts: list[str] = []

    async def send_text(self, message: str) -> None:
        if self.fail:
            raise RuntimeError("transport failed")
        self.texts.append(message)

    async def send_chart(self, chart: bytes, caption: str) -> None:
        if self.fail:
            raise RuntimeError("transport failed")
        self.charts.append(caption)


def test_anomaly_format_is_compact_and_directional():
    from radar.alerts.spread import parse_anomaly_alert

    details = parse_anomaly_alert(alert())
    message = format_anomaly_alert(details, context(10.0, 9.0, 8.8))

    assert "🔴 QQQ Basis Anomaly" in message
    assert "Long Arcus (QQQ-USD)" in message
    assert "Short Lighter Robinhood (QQQ)" in message
    assert "+21.00" in message
    assert "12h / 24h / 48h" in message


def test_anomaly_chart_renders_lifecycle_without_legacy_median_lines():
    from radar.alerts.spread import parse_anomaly_alert
    from radar.history.spread import HistoricalSpreadPoint

    details = parse_anomaly_alert(alert())
    history = HistoricalSpreadContext(
        points_7d=(
            HistoricalSpreadPoint(NOW - timedelta(seconds=10), 10.0),
            HistoricalSpreadPoint(NOW, 30.0),
        ),
        stats_7d=WindowStats(2, 20.0),
        stats_30d=WindowStats(2, 20.0),
        stats_90d=WindowStats(2, 20.0),
    )

    png = render_anomaly_chart(details, history)

    assert png is not None
    assert png.startswith(b"\x89PNG")


@pytest.mark.asyncio
async def test_initial_context_alignment_at_five_bps_is_eligible_and_persisted(tmp_path):
    telegram = FakeTelegram()
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 15.0, 10.0)),
            telegram,
            runtime_store=store,
        )
        await processor.process(alert())
        state = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert telegram.texts
        assert state["episode"]["initial_sent_at"] is not None
        assert state["episode"]["eligibility"] is True


@pytest.mark.asyncio
async def test_alignment_above_five_suppresses_without_transport(tmp_path):
    telegram = FakeTelegram()
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 16.0, 10.0)),
            telegram,
            runtime_store=store,
        )
        await processor.process(alert())
        assert telegram.texts == []
        state = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert state["episode"]["eligibility"] is False
        assert state["episode"]["eligibility_reason"] == "mean_alignment"


@pytest.mark.asyncio
async def test_transport_failure_does_not_mark_initial_sent(tmp_path):
    telegram = FakeTelegram(fail=True)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
        )
        with pytest.raises(RuntimeError, match="transport failed"):
            await processor.process(alert())
        state = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert "initial_sent_at" not in state["episode"]
