from datetime import UTC, datetime, timedelta

import pytest

from radar.config import AnomalyV2Config, SpreadMonitorConfig
from radar.monitors.spread.anomaly import (
    AnomalyObservation,
    AnomalyParameters,
    AnomalyTracker,
)
from radar.monitors.spread.models import SpreadPairKey


KEY = SpreadPairKey("QQQ", "arcus", "QQQ-USD", "lighter_robinhood", "QQQ")


def test_anomaly_v2_defaults_are_disabled_and_use_approved_parameters():
    config = SpreadMonitorConfig()

    assert config.anomaly_v2 == AnomalyV2Config()
    assert config.anomaly_v2.enabled is False
    assert config.anomaly_v2.deviation_bps == 15.0
    assert config.anomaly_v2.confirmation_seconds == 60
    assert config.anomaly_v2.return_band_bps == 5.0
    assert config.anomaly_v2.max_gap_seconds == 20
    assert config.anomaly_v2.notification_symbol_cooldown_seconds == 300
    assert config.anomaly_v2.telegram_enabled is True


def test_anomaly_v2_telegram_gate_accepts_false():
    config = AnomalyV2Config.model_validate({"telegram_enabled": False})

    assert config.telegram_enabled is False


@pytest.mark.parametrize(
    "field,value",
    [
        ("deviation_bps", -1),
        ("return_band_bps", -1),
        ("mean_alignment_max_bps", -1),
        ("confirmation_seconds", -1),
        ("max_gap_seconds", 0),
        ("expansion_notify_step_bps", 0),
        ("notification_symbol_cooldown_seconds", -1),
    ],
)
def test_anomaly_v2_rejects_invalid_parameters(field, value):
    with pytest.raises(ValueError):
        AnomalyV2Config(**{field: value})


def _observation(seconds: int) -> AnomalyObservation:
    return AnomalyObservation(
        sample_time=datetime(2026, 10, 1, tzinfo=UTC) + timedelta(seconds=seconds),
        raw_spread_bps=112.0,
        rolling_mean_bps=100.0,
        rolling_std_bps=2.0,
        basis_eligible=True,
    )


def test_tracker_restore_round_trip_uses_supported_api():
    parameters = AnomalyParameters(
        anomaly_deviation_bps=10.0,
        confirmation_seconds=60,
        return_band_bps=5.0,
        max_gap_seconds=20,
    )
    original = AnomalyTracker(KEY, parameters)
    original.observe(_observation(0))
    state = original.state_dict()

    restored = AnomalyTracker(KEY, parameters)
    restored.restore(
        active_episode=original.active_episode,
        last_sample_time=datetime.fromisoformat(state["last_sample_time"]),
    )

    assert restored.active_episode is not None
    assert restored.active_episode.to_dict() == original.active_episode.to_dict()
    assert restored.state_dict() == state
