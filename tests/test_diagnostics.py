from __future__ import annotations

import gc
import logging
from datetime import UTC, datetime, timedelta

import pytest

from radar.diagnostics import (
    EventLoopLagWatchdog,
    GCTelemetry,
    ResourceSnapshot,
    advance_scheduled_sample_time,
    install_diagnostic_logging,
)


def test_event_loop_watchdog_warns_once_per_episode_and_aggregates_lag(caplog):
    logger = logging.getLogger("test.event-loop")
    caplog.set_level(logging.WARNING, logger=logger.name)
    watchdog = EventLoopLagWatchdog(
        logger=logger,
        resource_snapshot=lambda: ResourceSnapshot(
            rss_bytes=123,
            process_cpu_ms=4.5,
            load_average_1m=0.25,
        ),
    )

    watchdog.observe(
        expected_monotonic=0.0,
        actual_monotonic=0.150,
        wall_time=datetime(2026, 10, 8, 1, 2, 3, tzinfo=UTC),
        cycle_number=7,
    )
    watchdog.observe(
        expected_monotonic=0.1,
        actual_monotonic=0.250,
        wall_time=datetime(2026, 10, 8, 1, 2, 3, 100000, tzinfo=UTC),
        cycle_number=7,
    )
    assert [record for record in caplog.records if record.levelno >= logging.WARNING]
    assert len(caplog.records) == 1

    watchdog.observe(
        expected_monotonic=0.2,
        actual_monotonic=0.205,
        wall_time=datetime(2026, 10, 8, 1, 2, 3, 200000, tzinfo=UTC),
        cycle_number=7,
    )
    watchdog.observe(
        expected_monotonic=0.3,
        actual_monotonic=1.501,
        wall_time=datetime(2026, 10, 8, 1, 2, 4, tzinfo=UTC),
        cycle_number=8,
    )
    assert len(caplog.records) == 2

    summary = watchdog.summary()
    assert summary["samples"] == 4
    assert summary["max_lag_ms"] == pytest.approx(1201.0)
    assert summary["over_100ms"] == 3
    assert summary["over_500ms"] == 1
    assert summary["over_1s"] == 1
    assert summary["over_5s"] == 0


@pytest.mark.parametrize(
    ("after_cycle_offset", "expected_skipped", "expected_next"),
    [
        (0.020, (), 10),
        (3.000, (), 10),
        (10.001, (10,), 20),
        (31.000, (10, 20, 30), 40),
    ],
)
def test_scheduler_advance_reports_exact_skipped_slots(
    after_cycle_offset: float,
    expected_skipped: tuple[int, ...],
    expected_next: int,
):
    previous = datetime(2026, 10, 8, 1, 0, 0, tzinfo=UTC)
    result = advance_scheduled_sample_time(
        previous,
        previous + timedelta(seconds=after_cycle_offset),
        sampling_seconds=10,
    )

    assert result.skipped_slots == tuple(
        previous + timedelta(seconds=seconds) for seconds in expected_skipped
    )
    assert result.next_sample_time == previous + timedelta(seconds=expected_next)


def test_gc_telemetry_tracks_pause_without_logging_in_callback():
    logger = logging.getLogger("test.gc")
    current = [0.0]
    telemetry = GCTelemetry(
        clock=lambda: current[0],
        logger=logger,
        warning_threshold_ms=100.0,
    )
    original_callbacks = list(gc.callbacks)
    telemetry.start()
    assert telemetry.callback in gc.callbacks
    try:
        telemetry.callback("start", {"generation": 2})
        current[0] = 0.250
        telemetry.callback(
            "stop",
            {"generation": 2, "collected": 3, "uncollectable": 1},
        )
        assert telemetry.drain_long_pauses()[0].duration_ms == pytest.approx(250.0)
        summary = telemetry.summary()
        assert summary["generation_2"]["count"] == 1
        assert summary["generation_2"]["collected"] == 3
        assert summary["generation_2"]["uncollectable"] == 1
    finally:
        telemetry.stop()
    assert telemetry.callback not in gc.callbacks
    assert gc.callbacks == original_callbacks


def test_diagnostic_log_is_bounded_and_marks_session_boundaries(tmp_path):
    logger = logging.getLogger("test.diagnostic-file")
    logger.setLevel(logging.INFO)
    session = install_diagnostic_logging(
        tmp_path / "radar-diagnostics.log",
        logger=logger,
        session_id="test-run-1",
    )
    try:
        logger.info("diagnostic payload")
        session.handler.flush()
        assert session.handler.maxBytes == 20 * 1024 * 1024
        assert session.handler.backupCount == 3
        text = session.path.read_text()
        assert "diagnostic session start session_id=test-run-1" in text
        assert "diagnostic payload" in text
    finally:
        session.close()

    text = session.path.read_text()
    assert "diagnostic session end session_id=test-run-1" in text
