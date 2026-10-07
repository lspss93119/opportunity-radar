from __future__ import annotations

from datetime import UTC, datetime

import pytest

from radar.alerts.manual_opportunity import (
    ManualOpportunityAlertProcessor,
    format_manual_opportunity_alert,
    parse_manual_opportunity_alert,
)
from radar.history.manual_opportunity import (
    ManualOpportunityHistoryContext,
    ManualOpportunityHistoryPoint,
)
from radar.monitors.base import AlertRequest


def payload(event_kind: str = "manual_initial") -> dict[str, object]:
    return {
        "event_kind": event_kind,
        "episode_id": "episode-1",
        "canonical_symbol": "QQQ",
        "long_venue": "arcus",
        "long_venue_symbol": "QQQ-USD",
        "short_venue": "lighter_robinhood",
        "short_venue_symbol": "QQQ",
        "signal_duration_seconds": 60,
        "current_spread_bps": 30.2,
        "reference_a_bps": 9.08,
        "deviation_bps": 21.12,
        "round_trip_fee_bps": 4.5,
        "expected_net_at_a_bps": 16.62,
        "mean_2h_bps": 9.72,
        "mean_24h_bps": 9.08,
        "mean_3d_bps": 8.9,
        "baseline_range_bps": 0.82,
        "long_volume_24h": 2_000_000.0,
        "short_volume_24h": 3_000_000.0,
        "route_volume_24h": 2_000_000.0,
        "long_best_ask": 100.0,
        "short_best_bid": 100.302,
        "candidate_started_at": datetime(2026, 9, 28, 10, 30, tzinfo=UTC).isoformat(),
        "confirmed_at": datetime(2026, 9, 28, 10, 31, tzinfo=UTC).isoformat(),
        "sample_time": datetime(2026, 9, 28, 10, 31, tzinfo=UTC).isoformat(),
        "expansion_level_bps": None,
    }


def test_manual_alert_parser_and_formatter_keep_bbo_fields_only():
    details = parse_manual_opportunity_alert(payload())
    message = format_manual_opportunity_alert(details)

    assert "QQQ Manual Opportunity" in message
    assert "Long  arcus" in message
    assert "Short lighter_robinhood" in message
    assert "Current spread" in message
    assert "4-leg taker fee" in message
    assert "Expected net at a" in message
    assert "Route min" in message
    assert "VWAP" not in message
    assert "ENME" not in message
    assert "Funding" not in message
    assert "Normal basis a" in message
    assert "24h Volume" in message
    assert "Long best ask" in message
    assert "Short best bid" in message


def test_manual_expansion_message_identifies_new_expected_net_level():
    data = payload("manual_expansion")
    data["expansion_level_bps"] = 20.0
    message = format_manual_opportunity_alert(parse_manual_opportunity_alert(data))

    assert "Expansion" in message
    assert "20.00 bps" in message


@pytest.mark.asyncio
async def test_manual_initial_processor_sends_chart_with_matching_caption():
    class FakeTelegram:
        def __init__(self):
            self.text_calls: list[str] = []
            self.chart_calls: list[tuple[bytes, str]] = []

        async def send_text(self, text: str) -> None:
            self.text_calls.append(text)

        async def send_chart(self, png: bytes, caption: str) -> None:
            self.chart_calls.append((png, caption))

    class FakeHistory:
        def query(self, **kwargs: object) -> ManualOpportunityHistoryContext:
            del kwargs
            return ManualOpportunityHistoryContext(
                points=(
                    ManualOpportunityHistoryPoint(
                        datetime(2026, 9, 28, 10, 30, tzinfo=UTC), 9.0
                    ),
                    ManualOpportunityHistoryPoint(
                        datetime(2026, 9, 28, 10, 31, tzinfo=UTC), 30.2
                    ),
                )
            )

    telegram = FakeTelegram()
    alert = AlertRequest(
        monitor="manual_opportunity",
        event_id="manual:initial",
        created_at=datetime(2026, 9, 28, 10, 31, tzinfo=UTC),
        payload=payload(),
    )

    processor = ManualOpportunityAlertProcessor(
        telegram,
        history=FakeHistory(),  # type: ignore[arg-type]
        chart_renderer=lambda details, context: b"png",
    )
    await processor.process(alert)

    assert telegram.text_calls == []
    assert telegram.chart_calls == [(b"png", format_manual_opportunity_alert(alert))]
    assert "Current spread       +30.20 bps" in telegram.chart_calls[0][1]
    assert "Normal basis a       +9.08 bps" in telegram.chart_calls[0][1]
    assert "Expected net at a    +16.62 bps" in telegram.chart_calls[0][1]


@pytest.mark.asyncio
async def test_manual_expansion_remains_text_only_even_when_history_is_available():
    class FakeTelegram:
        def __init__(self):
            self.text_calls: list[str] = []
            self.chart_calls: list[tuple[bytes, str]] = []

        async def send_text(self, text: str) -> None:
            self.text_calls.append(text)

        async def send_chart(self, png: bytes, caption: str) -> None:
            self.chart_calls.append((png, caption))

    class FakeHistory:
        def query(self, **kwargs: object) -> ManualOpportunityHistoryContext:
            del kwargs
            return ManualOpportunityHistoryContext.empty()

    data = payload("manual_expansion")
    data["expansion_level_bps"] = 20.0
    alert = AlertRequest(
        monitor="manual_opportunity",
        event_id="manual:expansion",
        created_at=datetime(2026, 9, 28, 10, 31, tzinfo=UTC),
        payload=data,
    )
    telegram = FakeTelegram()
    processor = ManualOpportunityAlertProcessor(
        telegram,
        history=FakeHistory(),  # type: ignore[arg-type]
        chart_renderer=lambda details, context: b"png",
    )

    await processor.process(alert)

    assert len(telegram.text_calls) == 1
    assert telegram.chart_calls == []


@pytest.mark.asyncio
async def test_manual_initial_falls_back_to_text_when_history_is_insufficient():
    class FakeTelegram:
        def __init__(self):
            self.text_calls: list[str] = []
            self.chart_calls: list[tuple[bytes, str]] = []

        async def send_text(self, text: str) -> None:
            self.text_calls.append(text)

        async def send_chart(self, png: bytes, caption: str) -> None:
            self.chart_calls.append((png, caption))

    class FakeHistory:
        def query(self, **kwargs: object) -> ManualOpportunityHistoryContext:
            del kwargs
            return ManualOpportunityHistoryContext.empty()

    telegram = FakeTelegram()
    alert = AlertRequest(
        monitor="manual_opportunity",
        event_id="manual:initial",
        created_at=datetime(2026, 9, 28, 10, 31, tzinfo=UTC),
        payload=payload(),
    )
    processor = ManualOpportunityAlertProcessor(
        telegram,
        history=FakeHistory(),  # type: ignore[arg-type]
        chart_renderer=lambda details, context: None,
    )

    await processor.process(alert)

    assert len(telegram.text_calls) == 1
    assert telegram.chart_calls == []
