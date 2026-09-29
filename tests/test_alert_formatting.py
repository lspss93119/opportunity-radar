from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

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
        "rolling_mean_bps": 80.0,
        "rolling_std_bps": 2.5,
        "deviation_bps": 20.0,
        "signal_duration_seconds": 120,
        "observed_at_skew_seconds": 0.5,
        "round_trip_fee_bps": 16.0,
        "theoretical_edge_bps": 4.0,
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
        "funding_context": {
            "long": {
                "effective_time": (
                    SAMPLE_TIME - timedelta(hours=1)
                ).isoformat(),
                "observed_at": SAMPLE_TIME.isoformat(),
                "venue": "lighter",
                "venue_symbol": "BTC",
                "canonical_symbol": "BTC",
                "funding_rate": 0.0001,
                "next_funding_time": (
                    SAMPLE_TIME + timedelta(hours=7)
                ).isoformat(),
            },
            "short": None,
        },
    }
    payload.update(payload_overrides)
    return AlertRequest(
        monitor="spread",
        event_id="episode:alert",
        created_at=SAMPLE_TIME,
        payload=payload,  # type: ignore[arg-type]
    )


def make_context() -> HistoricalSpreadContext:
    points = (
        HistoricalSpreadPoint(
            sample_time=SAMPLE_TIME - timedelta(days=1),
            raw_spread_bps=80.0,
        ),
        HistoricalSpreadPoint(sample_time=SAMPLE_TIME, raw_spread_bps=100.0),
    )
    return HistoricalSpreadContext(
        points_7d=points,
        stats_7d=WindowStats(2, 90.0),
        stats_30d=WindowStats(5, 85.0),
        stats_90d=WindowStats(10, 75.0),
    )


def test_parse_and_format_spread_alert_preserves_current_and_history_values():
    from radar.alerts.spread import format_spread_alert, parse_spread_alert

    details = parse_spread_alert(make_alert())

    assert details.primary_size_usd == 10_000
    assert details.sample_time == SAMPLE_TIME
    assert details.long_buy_vwap == 100.0
    assert details.short_sell_vwap == 101.0
    assert details.raw_spread_bps == 100.0
    assert details.net_spread_bps == 92.0
    assert details.long_funding is not None
    assert details.long_funding.funding_rate == 0.0001
    assert details.long_funding.next_funding_time == (
        SAMPLE_TIME + timedelta(hours=7)
    )
    assert details.short_funding is None

    message = format_spread_alert(
        details,
        make_context(),
        candidate_net_bps=12.5,
        alert_net_bps=25.0,
    )

    assert "BTC" in message
    assert "$10,000" in message
    assert "7日" in message and "30日" in message and "90日" in message
    assert "Lighter" in message and "Hyperliquid" in message
    assert "100.00" in message and "101.00" in message
    assert "100.00 bps" in message and "92.00 bps" in message
    assert "不可用" in message
    assert "7日 90.00 bps" in message
    assert "30日 85.00 bps" in message
    assert "90日 75.00 bps" in message
    assert "24h 均值: 80.00 bps" in message
    assert "Std: 2.50 bps" in message
    assert "偏離: +20.00 bps" in message
    assert "訊號持續: 120s" in message


def test_format_spread_alert_is_grouped_into_compact_sections():
    from radar.alerts.spread import format_spread_alert, parse_spread_alert

    details = parse_spread_alert(make_alert())
    message = format_spread_alert(details, make_context())

    sections = message.split("\n\n")

    assert len(sections) == 4
    assert sections[0].splitlines()[0] == "價差警報 | BTC | $10,000"
    assert "做多: Lighter (BTC)" in sections[0]
    assert "做空: Hyperliquid (BTC)" in sections[0]
    assert "淨價差: 92.00 bps" in sections[0]
    assert "原始價差: 100.00 bps" in sections[0]
    assert "往返手續費: 16.00 bps" in sections[0]
    assert "理論均值回歸邊際: +4.00 bps" in sections[0]
    assert "買入 VWAP: 100.00 | 賣出 VWAP: 101.00" in sections[1]
    assert "樣本時間: 2026-09-16 12:00:00 UTC" in sections[1]
    assert "觀測偏差: 0.50s" in sections[2]
    assert sections[3].startswith("歷史原始價差中位數:")


def test_format_spread_alert_uses_traditional_chinese_wording():
    from radar.alerts.spread import format_spread_alert, parse_spread_alert

    details = parse_spread_alert(make_alert())
    message = format_spread_alert(details, make_context())

    for wording in (
        "價差警報",
        "買入",
        "賣出",
        "樣本時間",
        "原始價差",
        "手續費",
        "淨價差",
        "24h 均值",
        "Std",
        "偏離",
        "訊號持續",
        "資金費率",
        "歷史原始價差",
        "樣本數",
        "中位數",
    ):
        assert wording in message

    for wording in (
        "价差警报",
        "买入",
        "卖出",
        "样本时间",
        "原始价差",
        "手续费",
        "净价差",
        "候选持续",
        "警报持续",
        "资金费率",
        "历史原始价差",
        "样本数",
        "中位数",
    ):
        assert wording not in message


def test_format_missing_history_and_funding_is_deterministically_unavailable():
    from radar.alerts.spread import format_spread_alert, parse_spread_alert

    details = parse_spread_alert(make_alert(funding_context={"long": None, "short": None}))
    message = format_spread_alert(details, HistoricalSpreadContext.empty())

    assert message.count("不可用") >= 5
    assert "7日 不可用" in message
    assert "30日 不可用" in message
    assert "90日 不可用" in message


def test_format_alert_with_unknown_fees_keeps_raw_trigger_context():
    from radar.alerts.spread import format_spread_alert, parse_spread_alert

    details = parse_spread_alert(
        make_alert(
            long_fee_bps=None,
            short_fee_bps=None,
            net_spread_bps=None,
            round_trip_fee_bps=None,
            theoretical_edge_bps=None,
        )
    )
    message = format_spread_alert(details, HistoricalSpreadContext.empty())

    assert "原始價差: 100.00 bps" in message
    assert "淨價差: 不可用 bps" in message
    assert "往返手續費: 不可用 bps" in message
    assert "理論均值回歸邊際: 不可用 bps" in message


@pytest.mark.parametrize(
    "payload_update",
    [
        {"canonical_symbol": ""},
        {"primary_size_usd": 2_000},
        {"sample_time": "2026-09-16T12:00:00"},
        {"long_buy_vwap": 0.0},
        {"short_sell_vwap": "nan"},
        {"raw_spread_bps": float("inf")},
        {"long_fee_bps": -1.0},
        {"candidate_duration_seconds": -1},
        {"funding_context": {"long": {"funding_rate": 0.1}, "short": None}},
    ],
)
def test_parse_rejects_invalid_payload_values(payload_update):
    from radar.alerts.spread import parse_spread_alert

    with pytest.raises((TypeError, ValueError)):
        parse_spread_alert(make_alert(**payload_update))


def test_parse_rejects_non_spread_alert_requests():
    from radar.alerts.spread import parse_spread_alert

    alert = AlertRequest(
        monitor="other",
        event_id="other:event",
        created_at=SAMPLE_TIME,
        payload=make_alert().payload,
    )

    with pytest.raises(ValueError, match="spread"):
        parse_spread_alert(alert)


def test_parse_accepts_utc_offset_and_normalizes_timestamps():
    from radar.alerts.spread import parse_spread_alert

    offset = timezone(timedelta(hours=8))
    details = parse_spread_alert(
        make_alert(sample_time=SAMPLE_TIME.astimezone(offset).isoformat())
    )

    assert details.sample_time == SAMPLE_TIME
