from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest

from radar.history.spread import (
    HistoricalSpreadContext,
    HistoricalSpreadPoint,
    WindowStats,
)
from radar.monitors.base import AlertRequest

SAMPLE_TIME = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)


def make_alert(**payload_overrides: object) -> AlertRequest:
    payload: dict[str, object] = {
        "canonical_symbol": "BTC",
        "long_venue": "lighter",
        "long_venue_symbol": "BTC",
        "short_venue": "hyperliquid",
        "short_venue_symbol": "BTC",
        "primary_size_usd": 10_000,
        "long_buy_vwap": 100.0,
        "short_sell_vwap": 101.0,
        "raw_spread_bps": 100.0,
        "long_fee_bps": 4.5,
        "short_fee_bps": 3.5,
        "net_spread_bps": 92.0,
        "sample_time": SAMPLE_TIME.isoformat(),
        "episode_started_at": (
            SAMPLE_TIME - timedelta(seconds=30)
        ).isoformat(),
        "candidate_confirmed_at": SAMPLE_TIME.isoformat(),
        "alert_condition_started_at": (
            SAMPLE_TIME - timedelta(seconds=120)
        ).isoformat(),
        "candidate_duration_seconds": 30,
        "alert_duration_seconds": 120,
        "funding_context": {"long": None, "short": None},
    }
    payload.update(payload_overrides)
    return AlertRequest(
        monitor="spread",
        event_id="episode:alert",
        created_at=SAMPLE_TIME,
        payload=payload,  # type: ignore[arg-type]
    )


def make_context() -> HistoricalSpreadContext:
    return HistoricalSpreadContext(
        points_7d=(
            HistoricalSpreadPoint(
                SAMPLE_TIME - timedelta(days=1),
                80.0,
            ),
            HistoricalSpreadPoint(SAMPLE_TIME, 100.0),
        ),
        stats_7d=WindowStats(2, 90.0),
        stats_30d=WindowStats(5, 85.0),
        stats_90d=WindowStats(10, 75.0),
    )


class FakeHistory:
    def __init__(
        self,
        context: HistoricalSpreadContext,
        error: Exception | None = None,
    ) -> None:
        self.context = context
        self.error = error
        self.calls: list[dict[str, object]] = []
        self.thread_ids: list[int] = []

    def query(self, **kwargs: object) -> HistoricalSpreadContext:
        self.calls.append(kwargs)
        self.thread_ids.append(threading.get_ident())
        if self.error is not None:
            raise self.error
        return self.context


class FakeTelegram:
    def __init__(self) -> None:
        self.text_calls: list[str] = []
        self.chart_calls: list[tuple[bytes, str]] = []

    async def send_text(self, text: str) -> None:
        self.text_calls.append(text)

    async def send_chart(self, png: bytes, caption: str) -> None:
        self.chart_calls.append((png, caption))


@pytest.mark.asyncio
async def test_processor_offloads_history_and_chart_and_delivers_chart():
    from radar.alerts.spread import SpreadAlertProcessor

    history = FakeHistory(make_context())
    telegram = FakeTelegram()
    chart_thread_ids: list[int] = []

    def render(details, context):
        chart_thread_ids.append(threading.get_ident())
        return b"png"

    processor = SpreadAlertProcessor(history, telegram, chart_renderer=render)  # type: ignore[arg-type]
    loop_thread = threading.get_ident()

    await processor.process(make_alert())

    assert history.thread_ids[0] != loop_thread
    assert chart_thread_ids[0] != loop_thread
    assert len(telegram.chart_calls) == 1
    assert telegram.chart_calls[0][0] == b"png"
    assert telegram.text_calls == []
    assert history.calls[0]["as_of"] == SAMPLE_TIME


@pytest.mark.asyncio
async def test_processor_sends_text_when_history_is_empty():
    from radar.alerts.spread import SpreadAlertProcessor

    history = FakeHistory(HistoricalSpreadContext.empty())
    telegram = FakeTelegram()
    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=lambda details, context: None,
    )  # type: ignore[arg-type]

    await processor.process(make_alert())

    assert len(telegram.text_calls) == 1
    assert telegram.chart_calls == []


@pytest.mark.asyncio
async def test_processor_degrades_history_exception_to_text():
    from radar.alerts.spread import SpreadAlertProcessor

    history = FakeHistory(make_context(), error=RuntimeError("history failed"))
    telegram = FakeTelegram()
    contexts: list[HistoricalSpreadContext] = []

    def render(details, context):
        contexts.append(context)

    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=render,
    )  # type: ignore[arg-type]

    await processor.process(make_alert())

    assert contexts == [HistoricalSpreadContext.empty()]
    assert len(telegram.text_calls) == 1
    assert telegram.chart_calls == []


@pytest.mark.asyncio
async def test_processor_degrades_chart_exception_to_text():
    from radar.alerts.spread import SpreadAlertProcessor

    history = FakeHistory(make_context())
    telegram = FakeTelegram()

    def render(details, context):
        raise RuntimeError("chart failed")

    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=render,
    )  # type: ignore[arg-type]

    await processor.process(make_alert())

    assert len(telegram.text_calls) == 1
    assert telegram.chart_calls == []


@pytest.mark.asyncio
async def test_processor_keeps_current_payload_values_in_message():
    from radar.alerts.spread import SpreadAlertProcessor

    history = FakeHistory(HistoricalSpreadContext.empty())
    telegram = FakeTelegram()
    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=lambda details, context: None,
    )  # type: ignore[arg-type]

    await processor.process(make_alert())

    assert "100.00" in telegram.text_calls[0]
    assert "101.00" in telegram.text_calls[0]
    assert "92.00" in telegram.text_calls[0]


@pytest.mark.asyncio
async def test_processor_rejects_non_spread_alert_before_slow_work():
    from radar.alerts.spread import SpreadAlertProcessor

    alert = AlertRequest(
        monitor="other",
        event_id="other:event",
        created_at=SAMPLE_TIME,
        payload=make_alert().payload,
    )
    history = FakeHistory(HistoricalSpreadContext.empty())
    telegram = FakeTelegram()
    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=lambda details, context: b"png",
    )  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="spread"):
        await processor.process(alert)

    assert history.calls == []
    assert telegram.text_calls == []
    assert telegram.chart_calls == []


@pytest.mark.asyncio
async def test_processor_sends_chart_for_message_at_caption_limit():
    from radar.alerts.spread import SpreadAlertProcessor

    history = FakeHistory(make_context())
    telegram = FakeTelegram()
    message = "x" * 1_024
    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=lambda details, context: b"png",
    )  # type: ignore[arg-type]

    # The injected formatter keeps this test at the exact Telegram boundary.
    import radar.alerts.spread as spread_module

    original_formatter = spread_module.format_spread_alert
    spread_module.format_spread_alert = lambda details, context: message  # type: ignore[assignment]
    try:
        await processor.process(make_alert())
    finally:
        spread_module.format_spread_alert = original_formatter

    assert telegram.chart_calls == [(b"png", message)]
    assert telegram.text_calls == []


@pytest.mark.asyncio
async def test_processor_sends_oversized_message_as_text_without_truncating():
    from radar.alerts.spread import (
        SpreadAlertProcessor,
        format_spread_alert,
        parse_spread_alert,
    )

    long_symbol = "BTC" + "-LONG" * 220
    alert = make_alert(
        long_venue_symbol=long_symbol,
        funding_context={"long": None, "short": None},
    )
    history = FakeHistory(make_context())
    telegram = FakeTelegram()
    processor = SpreadAlertProcessor(
        history,
        telegram,
        chart_renderer=lambda details, context: b"png",
    )  # type: ignore[arg-type]

    expected = format_spread_alert(
        parse_spread_alert(alert),
        make_context(),
    )
    assert len(expected) > 1_024

    await processor.process(alert)

    assert telegram.text_calls == [expected]
    assert telegram.chart_calls == []
    assert long_symbol in telegram.text_calls[0]
