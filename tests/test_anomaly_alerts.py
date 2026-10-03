from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from radar.alerts.chart import render_anomaly_chart
from radar.alerts.spread import format_anomaly_alert
from radar.alerts.spread import SpreadAlertProcessor
from radar.alerts.telegram import TelegramTransportError
from radar.config import AnomalyV2Config
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


def alert(
    event_kind: str = "anomaly_initial",
    *,
    episode_id: str = "episode",
    symbol: str = "QQQ",
    created_at: datetime = NOW,
    event_id: str | None = None,
) -> AlertRequest:
    candidate = created_at - timedelta(seconds=60)
    return AlertRequest(
        monitor="spread",
        event_id=event_id or f"{episode_id}:v2:{event_kind}",
        created_at=created_at,
        payload={
            "event_kind": event_kind,
            "episode_id": episode_id,
            "canonical_symbol": symbol,
            "long_venue": "arcus",
            "long_venue_symbol": "QQQ-USD",
            "short_venue": "lighter_robinhood",
            "short_venue_symbol": "QQQ",
            "primary_size_usd": 10_000,
            "sample_time": created_at.isoformat(),
            "candidate_started_at": candidate.isoformat(),
            "confirmed_at": created_at.isoformat(),
            "reference_mean_bps": 9.0,
            "reference_std_bps": 100.0,
            "confirmation_spread_bps": 30.0,
            "confirmation_deviation_bps": 21.0,
            "lifetime_peak_spread_bps": 35.0,
            "lifetime_peak_deviation_bps": 26.0,
            "lifetime_peak_at": created_at.isoformat(),
            "post_confirmation_peak_spread_bps": 35.0,
            "post_confirmation_peak_deviation_bps": 26.0,
            "post_confirmation_peak_at": created_at.isoformat(),
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


class FlakyContextHistory(FakeHistory):
    def __init__(self, supplied: AnomalyConfirmationContext, failures: int) -> None:
        super().__init__(supplied)
        self.failures = failures
        self.context_calls = 0

    def query_anomaly_context(self, **kwargs):
        self.context_calls += 1
        if self.context_calls <= self.failures:
            raise OSError("temporary history failure")
        return self.supplied


class FakeTelegram:
    def __init__(
        self,
        *,
        fail: bool = False,
        error: Exception | None = None,
    ) -> None:
        self.fail = fail
        self.error = error
        self.texts: list[str] = []
        self.charts: list[str] = []

    async def send_text(self, message: str) -> None:
        if self.error is not None:
            raise self.error
        if self.fail:
            raise RuntimeError("transport failed")
        self.texts.append(message)

    async def send_chart(self, chart: bytes, caption: str) -> None:
        if self.error is not None:
            raise self.error
        if self.fail:
            raise RuntimeError("transport failed")
        self.charts.append(caption)


class MutableClock:
    def __init__(self, value: datetime) -> None:
        self.value = value

    def __call__(self) -> datetime:
        return self.value


def route_alert(
    *,
    event_kind: str = "anomaly_initial",
    episode_id: str,
    symbol: str = "QQQ",
    created_at: datetime = NOW,
    long_venue: str = "arcus",
    short_venue: str = "lighter_robinhood",
) -> AlertRequest:
    request = alert(
        event_kind,
        episode_id=episode_id,
        symbol=symbol,
        created_at=created_at,
    )
    request.payload.update(
        {
            "long_venue": long_venue,
            "long_venue_symbol": f"{symbol}-LONG",
            "short_venue": short_venue,
            "short_venue_symbol": f"{symbol}-SHORT",
        }
    )
    return request


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
async def test_incomplete_context_persists_suppression_without_transport(tmp_path):
    telegram = FakeTelegram()
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0, eligible=False)),
            telegram,
            runtime_store=store,
        )
        await processor.process(alert())
        assert telegram.texts == []
        state = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert state["episode"]["eligibility"] is False
        assert state["episode"]["eligibility_reason"] == "context_unavailable"


@pytest.mark.asyncio
async def test_context_query_retries_then_delivers(tmp_path, monkeypatch):
    monkeypatch.setattr("radar.alerts.spread.ANOMALY_CONTEXT_QUERY_DELAY_SECONDS", 0)
    telegram = FakeTelegram()
    history = FlakyContextHistory(context(10.0, 10.0, 10.0), failures=1)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(history, telegram, runtime_store=store)
        await processor.process(alert())
        assert history.context_calls == 2
        assert telegram.texts
        state = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert state["episode"]["eligibility"] is True


@pytest.mark.asyncio
async def test_repeated_context_query_failure_leaves_eligibility_undecided(
    tmp_path, monkeypatch
):
    monkeypatch.setattr("radar.alerts.spread.ANOMALY_CONTEXT_QUERY_DELAY_SECONDS", 0)
    telegram = FakeTelegram()
    history = FlakyContextHistory(context(10.0, 10.0, 10.0), failures=3)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(history, telegram, runtime_store=store)
        with pytest.raises(OSError, match="temporary history failure"):
            await processor.process(alert())
        assert history.context_calls == 3
        assert telegram.texts == []
        assert store.get_monitor_state("spread", "anomaly_notifications_v2") is None


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


@pytest.mark.asyncio
async def test_same_symbol_initial_is_suppressed_and_audit_is_persisted(tmp_path):
    telegram = FakeTelegram()
    clock = MutableClock(NOW)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=clock,
        )
        await processor.process(route_alert(episode_id="episode-1"))
        clock.value = NOW + timedelta(seconds=100)
        await processor.process(
            route_alert(
                episode_id="episode-2",
                long_venue="backpack",
                short_venue="lighter",
                created_at=clock.value,
            )
        )

        assert len(telegram.texts) == 1
        second_state = store.get_monitor_state("spread", "anomaly_notifications_v2")[
            "episode-2"
        ]
        assert "initial_sent_at" not in second_state
        assert second_state["suppression_reason"] == "symbol_cooldown"
        assert second_state["suppressed_until"] == (
            NOW + timedelta(seconds=300)
        ).isoformat()
        events = store.list_opportunities(monitor_name="spread")
        assert [event["event_type"] for event in events] == [
            "anomaly_delivery_attempt",
            "anomaly_delivery_result",
            "anomaly_notification_suppressed",
        ]


@pytest.mark.asyncio
async def test_cooldown_is_per_symbol_and_expires(tmp_path):
    telegram = FakeTelegram()
    clock = MutableClock(NOW)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=clock,
        )
        await processor.process(route_alert(episode_id="qqq-1"))
        await processor.process(
            route_alert(episode_id="btc-1", symbol="BTC", created_at=NOW)
        )
        clock.value = NOW + timedelta(seconds=301)
        await processor.process(
            route_alert(episode_id="qqq-2", created_at=clock.value)
        )

        assert len(telegram.texts) == 3
        clusters = store.get_monitor_state(
            "spread", "anomaly_notification_clusters_v2"
        )
        assert clusters["QQQ"]["last_nonreturn_sent_at"] == (
            NOW + timedelta(seconds=301)
        ).isoformat()
        assert "BTC" not in clusters


@pytest.mark.asyncio
async def test_zero_cooldown_preserves_multiple_deliveries(tmp_path):
    telegram = FakeTelegram()
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=0
            ),
        )
        await processor.process(route_alert(episode_id="episode-1"))
        await processor.process(route_alert(episode_id="episode-2"))

    assert len(telegram.texts) == 2


@pytest.mark.asyncio
async def test_transport_error_is_audited_without_consuming_cooldown(tmp_path):
    telegram = FakeTelegram(fail=True)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
        )
        with pytest.raises(RuntimeError, match="transport failed"):
            await processor.process(route_alert(episode_id="episode-1"))

        telegram.fail = False
        await processor.process(route_alert(episode_id="episode-2"))

        assert len(telegram.texts) == 1
        clusters = store.get_monitor_state(
            "spread", "anomaly_notification_clusters_v2"
        )
        assert clusters["QQQ"]["episode_id"] == "episode-2"
        events = store.list_opportunities(monitor_name="spread")
        assert [event["event_type"] for event in events] == [
            "anomaly_delivery_attempt",
            "anomaly_delivery_result",
            "anomaly_delivery_attempt",
            "anomaly_delivery_result",
        ]
        assert events[1]["event"]["outcome"] == "transport_error"
        assert events[1]["event"]["exception_class"] == "RuntimeError"
        assert "message" not in events[1]["event"]
        event_ids = [event["event_id"] for event in events]
        assert len(event_ids) == len(set(event_ids))


@pytest.mark.asyncio
async def test_telegram_transport_error_audit_is_structured_and_sanitized(tmp_path):
    telegram = FakeTelegram(
        error=TelegramTransportError(
            error_kind="read_timeout",
            status_code=None,
        )
    )
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
        )
        with pytest.raises(TelegramTransportError):
            await processor.process(route_alert(episode_id="episode-1"))

        state = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert "initial_sent_at" not in state["episode-1"]
        events = store.list_opportunities(monitor_name="spread")
        result = events[-1]["event"]
        assert result["outcome"] == "transport_error"
        assert result["exception_class"] == "TelegramTransportError"
        assert result["error_kind"] == "read_timeout"
        assert result["status_code"] is None
        assert "message" not in result


@pytest.mark.asyncio
async def test_return_bypasses_symbol_cooldown_but_requires_initial(tmp_path):
    telegram = FakeTelegram()
    clock = MutableClock(NOW)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=clock,
        )
        await processor.process(route_alert(episode_id="episode-1"))
        clock.value = NOW + timedelta(seconds=1)
        await processor.process(
            route_alert(
                event_kind="anomaly_return",
                episode_id="episode-1",
                created_at=clock.value,
            )
        )
        await processor.process(
            route_alert(
                event_kind="anomaly_return",
                episode_id="episode-never-sent",
                created_at=clock.value,
            )
        )

        assert len(telegram.texts) == 2
        assert store.get_monitor_state("spread", "anomaly_notifications_v2")[
            "episode-1"
        ]["return_sent_at"] == clock.value.isoformat()


@pytest.mark.asyncio
async def test_unknown_attempt_has_no_result_and_does_not_auto_retry(tmp_path):
    class BlockingTelegram(FakeTelegram):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def send_text(self, message: str) -> None:
            self.started.set()
            await self.release.wait()
            await super().send_text(message)

    telegram = BlockingTelegram()
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            clock=lambda: NOW,
        )
        task = asyncio.create_task(processor.process(route_alert(episode_id="episode-1")))
        await telegram.started.wait()
        assert [
            event["event_type"] for event in store.list_opportunities(monitor_name="spread")
        ] == ["anomaly_delivery_attempt"]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        notification = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert "initial_sent_at" not in notification["episode-1"]
        assert store.get_monitor_state("spread", "anomaly_notification_clusters_v2") is None


@pytest.mark.asyncio
async def test_expansion_cooldown_suppresses_then_allows_initial_recovery(tmp_path):
    telegram = FakeTelegram()
    clock = MutableClock(NOW)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=clock,
        )
        await processor.process(route_alert(episode_id="episode-1"))
        clock.value = NOW + timedelta(seconds=100)
        await processor.process(
            route_alert(
                event_kind="anomaly_expansion",
                episode_id="episode-2",
                created_at=clock.value,
            )
        )
        assert "initial_sent_at" not in store.get_monitor_state(
            "spread", "anomaly_notifications_v2"
        )["episode-2"]

        clock.value = NOW + timedelta(seconds=301)
        await processor.process(
            route_alert(
                event_kind="anomaly_expansion",
                episode_id="episode-2",
                created_at=clock.value,
            )
        )

        assert len(telegram.texts) == 2
        recovered = store.get_monitor_state(
            "spread", "anomaly_notifications_v2"
        )["episode-2"]
        assert recovered["initial_sent_at"] == clock.value.isoformat()


@pytest.mark.asyncio
async def test_sent_expansion_updates_peak_only_after_cooldown_expires(tmp_path):
    telegram = FakeTelegram()
    clock = MutableClock(NOW)
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=clock,
        )
        await processor.process(route_alert(episode_id="episode-1"))
        clock.value = NOW + timedelta(seconds=100)
        await processor.process(
            route_alert(
                event_kind="anomaly_expansion",
                episode_id="episode-1",
                created_at=clock.value,
            )
        )
        assert "last_expansion_peak_bps" not in store.get_monitor_state(
            "spread", "anomaly_notifications_v2"
        )["episode-1"]

        clock.value = NOW + timedelta(seconds=301)
        await processor.process(
            route_alert(
                event_kind="anomaly_expansion",
                episode_id="episode-1",
                created_at=clock.value,
            )
        )
        assert store.get_monitor_state("spread", "anomaly_notifications_v2")[
            "episode-1"
        ]["last_expansion_peak_bps"] == 26.0
        assert len(telegram.texts) == 2


@pytest.mark.asyncio
async def test_symbol_cooldown_survives_processor_restart(tmp_path):
    database = tmp_path / "runtime.sqlite3"
    with SQLiteRuntimeStore(database) as store:
        first = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            FakeTelegram(),
            runtime_store=store,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=lambda: NOW,
        )
        await first.process(route_alert(episode_id="episode-1"))

    second_telegram = FakeTelegram()
    with SQLiteRuntimeStore(database) as reopened:
        second = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            second_telegram,
            runtime_store=reopened,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=lambda: NOW + timedelta(seconds=100),
        )
        await second.process(
            route_alert(
                episode_id="episode-2",
                created_at=NOW + timedelta(seconds=100),
            )
        )
        assert second_telegram.texts == []

    third_telegram = FakeTelegram()
    with SQLiteRuntimeStore(database) as reopened:
        third = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            third_telegram,
            runtime_store=reopened,
            anomaly_v2_config=AnomalyV2Config(
                enabled=True, notification_symbol_cooldown_seconds=300
            ),
            clock=lambda: NOW + timedelta(seconds=301),
        )
        await third.process(
            route_alert(
                episode_id="episode-3",
                created_at=NOW + timedelta(seconds=301),
            )
        )
        assert len(third_telegram.texts) == 1


@pytest.mark.asyncio
async def test_delivery_state_and_success_audit_are_atomic(tmp_path, monkeypatch):
    telegram = FakeTelegram()
    with SQLiteRuntimeStore(tmp_path / "runtime.sqlite3") as store:
        def fail_commit(*args, **kwargs):
            raise RuntimeError("persistence failed")

        monkeypatch.setattr(store, "set_monitor_states_and_append_opportunities", fail_commit)
        processor = SpreadAlertProcessor(
            FakeHistory(context(10.0, 10.0, 10.0)),
            telegram,
            runtime_store=store,
            clock=lambda: NOW,
        )
        with pytest.raises(RuntimeError, match="persistence failed"):
            await processor.process(route_alert(episode_id="episode-1"))

        notification = store.get_monitor_state("spread", "anomaly_notifications_v2")
        assert "initial_sent_at" not in notification["episode-1"]
        assert store.get_monitor_state("spread", "anomaly_notification_clusters_v2") is None
        assert [
            event["event_type"] for event in store.list_opportunities(monitor_name="spread")
        ] == ["anomaly_delivery_attempt"]
        assert len(telegram.texts) == 1
