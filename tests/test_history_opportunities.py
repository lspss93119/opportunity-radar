from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.config import MonitorConfig, RadarConfig, SpreadMonitorConfig
from radar.models import MarketSnapshot
from radar.storage.parquet import ParquetStorage

AS_OF = datetime(2026, 9, 23, 12, 0, tzinfo=UTC)


def make_market(
    venue: str,
    sample_time: datetime,
    *,
    observed_at: datetime | None = None,
    canonical_symbol: str = "BTC",
    venue_symbol: str | None = None,
    buy_1k_vwap: float | None = 100.0,
    sell_1k_vwap: float | None = 101.0,
    buy_5k_vwap: float | None = 200.0,
    sell_5k_vwap: float | None = 202.0,
    buy_10k_vwap: float | None = 100.0,
    sell_10k_vwap: float | None = 101.0,
) -> MarketSnapshot:
    return MarketSnapshot(
        sample_time=sample_time,
        observed_at=sample_time if observed_at is None else observed_at,
        venue=venue,
        venue_symbol=venue if venue_symbol is None else venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=99.0,
        best_bid_size=10.0,
        best_ask=100.0,
        best_ask_size=10.0,
        buy_1k_vwap=buy_1k_vwap,
        sell_1k_vwap=sell_1k_vwap,
        buy_5k_vwap=buy_5k_vwap,
        sell_5k_vwap=sell_5k_vwap,
        buy_10k_vwap=buy_10k_vwap,
        sell_10k_vwap=sell_10k_vwap,
    )


def make_config(
    *,
    fees: dict[str, float] | None = None,
    primary_size_usd: int = 10_000,
    candidate_net_bps: float = 10.0,
    candidate_duration_seconds: int = 30,
    alert_net_bps: float = 20.0,
    alert_duration_seconds: int = 120,
    interval_seconds: int = 10,
) -> RadarConfig:
    return RadarConfig(
        fees_bps={} if fees is None else fees,
        monitors=MonitorConfig(
            spread=SpreadMonitorConfig(
                primary_size_usd=primary_size_usd,
                candidate_net_bps=candidate_net_bps,
                candidate_duration_seconds=candidate_duration_seconds,
                alert_net_bps=alert_net_bps,
                alert_duration_seconds=alert_duration_seconds,
                interval_seconds=interval_seconds,
            )
        ),
    )


def write_markets(tmp_path, snapshots: list[MarketSnapshot]) -> None:
    store = ParquetStorage(tmp_path / "data")
    for snapshot in snapshots:
        store.append(snapshot)
    store.flush(now=AS_OF + timedelta(days=1))


def build_report(tmp_path, config: RadarConfig, *, hours: float = 6, **kwargs):
    from radar.history.opportunities import build_opportunity_report

    return build_opportunity_report(
        tmp_path / "data",
        config,
        hours=hours,
        **kwargs,
    )


def test_report_deduplicates_and_evaluates_all_directional_pairs_with_fees(tmp_path):
    old_long = make_market(
        "long",
        AS_OF,
        observed_at=AS_OF + timedelta(milliseconds=100),
        buy_10k_vwap=90.0,
    )
    latest_long = make_market(
        "long",
        AS_OF,
        observed_at=AS_OF + timedelta(milliseconds=200),
        buy_10k_vwap=101.0,
    )
    snapshots = [
        old_long,
        latest_long,
        make_market("short_a", AS_OF, sell_10k_vwap=102.0),
        make_market("short_b", AS_OF, sell_10k_vwap=103.0),
    ]
    write_markets(tmp_path, snapshots)

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 1.0, "short_a": 2.0, "short_b": 3.0},
            candidate_net_bps=0.0,
            candidate_duration_seconds=0,
            alert_net_bps=1_000.0,
        ),
    )

    assert len(report.pair_samples) == 6
    assert all(
        sample.key.long_venue != sample.key.short_venue
        for sample in report.pair_samples
    )
    long_to_a = next(
        sample
        for sample in report.pair_samples
        if sample.key.long_venue == "long" and sample.key.short_venue == "short_a"
    )
    assert long_to_a.long_buy_vwap == 101.0
    assert long_to_a.net_spread_bps == pytest.approx(
        (102.0 / 101.0 - 1.0) * 10_000 - 1.0 - 2.0
    )


def test_report_missing_vwap_or_fee_fails_closed(tmp_path):
    write_markets(
        tmp_path,
        [
            make_market("good", AS_OF, buy_10k_vwap=100.0, sell_10k_vwap=101.0),
            make_market("missing_buy", AS_OF, buy_10k_vwap=None),
            make_market("missing_fee", AS_OF, sell_10k_vwap=105.0),
        ],
    )

    report = build_report(
        tmp_path,
        make_config(
            fees={"good": 0.0, "missing_buy": 0.0},
            candidate_net_bps=0.0,
            candidate_duration_seconds=0,
        ),
    )

    assert report.pair_samples
    assert all(
        sample.key.long_venue != "missing_buy"
        and sample.key.short_venue != "missing_fee"
        for sample in report.pair_samples
    )


def test_episode_reconstruction_matches_candidate_continuity_and_confirmation(
    tmp_path,
):
    snapshots: list[MarketSnapshot] = []
    for offset, short_sell in (
        (0, 101.0),
        (10, 101.0),
        (20, 101.0),
        (30, 101.0),
        (40, 100.05),
        (50, 101.0),
    ):
        sample_time = AS_OF + timedelta(seconds=offset)
        snapshots.extend(
            [
                make_market(
                    "long",
                    sample_time,
                    sell_10k_vwap=99.0,
                    buy_10k_vwap=100.0,
                ),
                make_market(
                    "short",
                    sample_time,
                    buy_10k_vwap=102.0,
                    sell_10k_vwap=short_sell,
                ),
            ]
        )
    write_markets(tmp_path, snapshots)

    report = build_report(tmp_path, make_config(fees={"long": 0.0, "short": 0.0}))

    assert len(report.episodes) == 2
    confirmed, unconfirmed = report.episodes
    assert confirmed.duration_seconds == 30.0
    assert confirmed.sample_count == 4
    assert confirmed.candidate_confirmed is True
    assert unconfirmed.duration_seconds == 0.0
    assert unconfirmed.sample_count == 1
    assert unconfirmed.candidate_confirmed is False
    assert report.unconfirmed_spikes == (unconfirmed,)


def test_episode_gap_over_two_intervals_starts_a_new_episode(tmp_path):
    snapshots = []
    for offset in (0, 10, 31):
        sample_time = AS_OF + timedelta(seconds=offset)
        snapshots.extend(
            [
                make_market(
                    "long",
                    sample_time,
                    sell_10k_vwap=99.0,
                    buy_10k_vwap=100.0,
                ),
                make_market(
                    "short",
                    sample_time,
                    buy_10k_vwap=102.0,
                    sell_10k_vwap=101.0,
                ),
            ]
        )
    write_markets(tmp_path, snapshots)

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 0.0, "short": 0.0},
            candidate_duration_seconds=0,
        ),
    )

    target_episodes = [
        episode
        for episode in report.episodes
        if episode.key.long_venue == "long"
        and episode.key.short_venue == "short"
    ]
    assert [(episode.start_time, episode.end_time) for episode in target_episodes] == [
        (AS_OF, AS_OF + timedelta(seconds=10)),
        (AS_OF + timedelta(seconds=31), AS_OF + timedelta(seconds=31)),
    ]


def test_alert_qualification_requires_continuous_alert_threshold_segment(tmp_path):
    snapshots: list[MarketSnapshot] = []
    for offset, short_sell in (
        (0, 101.3),
        (10, 101.15),
        (20, 101.3),
        (30, 101.3),
        (40, 101.3),
    ):
        sample_time = AS_OF + timedelta(seconds=offset)
        snapshots.extend(
            [
                make_market(
                    "long",
                    sample_time,
                    sell_10k_vwap=99.0,
                    buy_10k_vwap=100.0,
                ),
                make_market(
                    "short",
                    sample_time,
                    buy_10k_vwap=102.0,
                    sell_10k_vwap=short_sell,
                ),
            ]
        )
    write_markets(tmp_path, snapshots)

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 0.0, "short": 0.0},
            candidate_duration_seconds=0,
            alert_net_bps=20.0,
            alert_duration_seconds=20,
        ),
    )

    assert len(report.episodes) == 1
    episode = report.episodes[0]
    assert episode.candidate_confirmed is True
    assert episode.alert_qualified is True


def test_one_alert_threshold_spike_is_not_alert_qualified(tmp_path):
    snapshots = [
        make_market("long", AS_OF, buy_10k_vwap=100.0, sell_10k_vwap=99.0),
        make_market("short", AS_OF, buy_10k_vwap=102.0, sell_10k_vwap=101.3),
        make_market(
            "long",
            AS_OF + timedelta(seconds=10),
            buy_10k_vwap=100.0,
            sell_10k_vwap=99.0,
        ),
        make_market(
            "short",
            AS_OF + timedelta(seconds=10),
            buy_10k_vwap=102.0,
            sell_10k_vwap=101.15,
        ),
    ]
    write_markets(tmp_path, snapshots)

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 0.0, "short": 0.0},
            candidate_duration_seconds=0,
            alert_net_bps=20.0,
            alert_duration_seconds=20,
        ),
    )

    assert report.episodes[0].alert_qualified is False


@pytest.mark.parametrize(
    ("size", "long_buy", "short_sell"),
    [(1_000, 10.0, 10.1), (5_000, 20.0, 20.2), (10_000, 30.0, 30.3)],
)
def test_report_selects_requested_vwap_size(
    tmp_path,
    size: int,
    long_buy: float,
    short_sell: float,
):
    write_markets(
        tmp_path,
        [
            make_market(
                "long",
                AS_OF,
                buy_1k_vwap=10.0,
                sell_1k_vwap=9.0,
                buy_5k_vwap=20.0,
                sell_5k_vwap=19.0,
                buy_10k_vwap=30.0,
                sell_10k_vwap=29.0,
            ),
            make_market(
                "short",
                AS_OF,
                buy_1k_vwap=9.0,
                sell_1k_vwap=10.1,
                buy_5k_vwap=19.0,
                sell_5k_vwap=20.2,
                buy_10k_vwap=29.0,
                sell_10k_vwap=30.3,
            ),
        ],
    )

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 0.0, "short": 0.0},
            primary_size_usd=10_000,
            candidate_net_bps=0.0,
            candidate_duration_seconds=0,
        ),
        size_usd=size,
    )

    sample = next(
        sample
        for sample in report.pair_samples
        if sample.key.long_venue == "long" and sample.key.short_venue == "short"
    )
    assert sample.long_buy_vwap == long_buy
    assert sample.short_sell_vwap == short_sell


def test_report_calculates_observation_skew_and_data_as_of(tmp_path):
    write_markets(
        tmp_path,
        [
            make_market(
                "long",
                AS_OF,
                observed_at=AS_OF + timedelta(milliseconds=100),
            ),
            make_market(
                "short",
                AS_OF,
                observed_at=AS_OF + timedelta(milliseconds=550),
            ),
        ],
    )

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 0.0, "short": 0.0},
            candidate_net_bps=0.0,
            candidate_duration_seconds=0,
        ),
    )

    assert report.data_as_of == AS_OF
    assert report.pair_samples[0].observed_skew_ms == pytest.approx(450.0)


def test_pair_summary_reports_candidate_rate_and_percentile(tmp_path):
    snapshots = []
    for offset, short_sell in ((0, 101.0), (10, 100.5)):
        sample_time = AS_OF + timedelta(seconds=offset)
        snapshots.extend(
            [
                make_market(
                    "long",
                    sample_time,
                    sell_10k_vwap=99.0,
                    buy_10k_vwap=100.0,
                ),
                make_market(
                    "short",
                    sample_time,
                    buy_10k_vwap=102.0,
                    sell_10k_vwap=short_sell,
                ),
            ]
        )
    write_markets(tmp_path, snapshots)

    report = build_report(
        tmp_path,
        make_config(
            fees={"long": 0.0, "short": 0.0},
            candidate_net_bps=80.0,
            candidate_duration_seconds=0,
        ),
        min_net_bps=80.0,
    )

    summary = next(
        summary
        for summary in report.pair_summaries
        if summary.key.long_venue == "long" and summary.key.short_venue == "short"
    )
    assert summary.sample_count == 2
    assert summary.candidate_sample_count == 1
    assert summary.candidate_percent == pytest.approx(50.0)
    assert summary.max_net_spread_bps == pytest.approx(100.0)
    assert summary.median_net_spread_bps == pytest.approx(75.0)
    assert summary.p95_net_spread_bps == pytest.approx(97.5)
    assert summary.confirmed_candidate_episode_count == 1


def test_empty_dataset_formats_cleanly(tmp_path):
    from radar.history.opportunities import format_opportunity_report

    report = build_report(tmp_path, make_config(), hours=6)

    output = format_opportunity_report(report)

    assert report.data_as_of is None
    assert "No market data found" in output


def test_opportunities_cli_does_not_require_telegram_credentials(tmp_path, monkeypatch, capsys):
    from radar.app import main

    write_markets(
        tmp_path,
        [
            make_market("long", AS_OF, buy_10k_vwap=100.0),
            make_market("short", AS_OF, sell_10k_vwap=101.0),
        ],
    )
    config_path = tmp_path / "radar.yaml"
    config_path.write_text(
        """
fees_bps:
  long: 0
  short: 0
monitors:
  spread:
    candidate_net_bps: 0
    candidate_duration_seconds: 0
""",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("RADAR_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("RADAR_TELEGRAM_CHAT_ID", raising=False)

    assert main(["opportunities", "--config", str(config_path), "--hours", "6"]) == 0

    output = capsys.readouterr().out
    assert "OPPORTUNITY RADAR REPORT" in output
    assert "Data as of: 2026-09-23 12:00:00 UTC" in output
    assert "Candidate:  >= 0.00 bps for 0s" in output
    assert "Alert:      >= 20.00 bps for 120s" in output
    assert "long -> short" in output
