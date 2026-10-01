from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from radar.monitors.spread.anomaly import (
    AnomalyObservation,
    AnomalyParameters,
    AnomalyTracker,
)
from radar.monitors.spread.models import SpreadPairKey


START = datetime(2026, 9, 28, 0, 0, tzinfo=UTC)
KEY = SpreadPairKey(
    canonical_symbol="QQQ",
    long_venue="arcus",
    long_venue_symbol="QQQ-USD",
    short_venue="lighter_robinhood",
    short_venue_symbol="QQQ",
)


def observation(
    seconds: int,
    *,
    raw: float = 112.0,
    mean: float | None = 100.0,
    std: float | None = 1.0,
    eligible: bool = True,
) -> AnomalyObservation:
    return AnomalyObservation(
        sample_time=START + timedelta(seconds=seconds),
        raw_spread_bps=raw,
        rolling_mean_bps=mean,
        rolling_std_bps=std,
        basis_eligible=eligible,
    )


def tracker(
    *,
    deviation: float = 10.0,
    confirmation: int = 60,
    return_band: float = 5.0,
    max_gap: int = 20,
) -> AnomalyTracker:
    return AnomalyTracker(
        KEY,
        AnomalyParameters(
            anomaly_deviation_bps=deviation,
            confirmation_seconds=confirmation,
            return_band_bps=return_band,
            max_gap_seconds=max_gap,
        ),
    )


def test_candidate_starts_on_threshold_crossing_and_freezes_prior_reference():
    state = tracker().observe(observation(0, raw=110.0, mean=100.0, std=2.5))

    assert [transition.kind for transition in state] == ["candidate_started"]
    episode = state[0].episode
    assert episode.candidate_started_at == START
    assert episode.reference_mean_bps == 100.0
    assert episode.reference_std_bps == 2.5

    tracker_instance = tracker(max_gap=120)
    tracker_instance.observe(observation(0, raw=110.0, mean=100.0, std=2.5))
    tracker_instance.observe(observation(50, raw=121.0, mean=110.0, std=3.5))
    assert tracker_instance.active_episode is not None
    assert tracker_instance.active_episode.reference_mean_bps == 100.0
    assert tracker_instance.active_episode.reference_std_bps == 2.5


def test_candidate_below_threshold_before_confirmation_is_discarded():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=110.0))

    transitions = instance.observe(observation(50, raw=109.9))

    assert instance.active_episode is None
    assert all(transition.kind != "confirmed" for transition in transitions)
    assert instance.confirmed_episodes == ()


def test_confirmation_occurs_at_exact_duration_and_only_once():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))

    before = instance.observe(observation(59, raw=112.0))
    at_duration = instance.observe(observation(60, raw=112.0))
    after = instance.observe(observation(70, raw=112.0))

    assert all(transition.kind != "confirmed" for transition in before)
    assert [transition.kind for transition in at_duration] == ["confirmed"]
    assert [transition.kind for transition in after] == []
    assert instance.active_episode is not None
    assert instance.active_episode.confirmed_at == START + timedelta(seconds=60)
    assert instance.active_episode.confirmation_spread_bps == 112.0
    assert instance.active_episode.confirmation_deviation_bps == 12.0


def test_confirmed_episode_survives_dropping_below_entry_threshold():
    instance = tracker(deviation=15.0, max_gap=120)
    instance.observe(observation(0, raw=116.0))
    instance.observe(observation(60, raw=116.0))

    transitions = instance.observe(observation(70, raw=112.0))

    assert transitions == ()
    assert instance.active_episode is not None
    assert instance.active_episode.resolution_reason is None


def test_confirmed_episode_uses_frozen_mean_when_live_basis_becomes_unavailable():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))
    instance.observe(observation(60, raw=112.0))

    transitions = instance.observe(
        observation(70, raw=106.0, mean=101.0, std=None, eligible=False)
    )

    assert transitions == ()
    assert instance.active_episode is not None
    assert instance.active_episode.current_live_mean_bps == 101.0
    assert instance.active_episode.current_live_std_bps is None
    assert instance.active_episode.current_deviation_from_reference_bps == pytest.approx(6.0)


def test_peak_updates_only_for_larger_reference_deviation_and_tracks_expansion():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))
    instance.observe(observation(60, raw=112.0))
    instance.observe(observation(70, raw=120.0))

    episode = instance.active_episode
    assert episode is not None
    assert episode.peak_spread_bps == 120.0
    assert episode.peak_deviation_bps == 20.0
    assert episode.peak_at == START + timedelta(seconds=70)
    assert episode.post_confirmation_expansion_bps == pytest.approx(8.0)
    assert episode.confirmation_to_peak_seconds == pytest.approx(10.0)

    instance.observe(observation(80, raw=115.0))
    assert instance.active_episode is not None
    assert instance.active_episode.peak_spread_bps == 120.0
    assert instance.active_episode.peak_at == START + timedelta(seconds=70)


def test_return_band_and_crossing_reference_mean_resolve_episode():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))
    instance.observe(observation(60, raw=112.0))

    returned = instance.observe(observation(70, raw=105.0))
    assert [transition.kind for transition in returned] == ["resolved"]
    episode = returned[0].episode
    assert episode.resolution_reason == "returned_to_mean_band"
    assert episode.ended_at == START + timedelta(seconds=70)
    assert episode.end_spread_bps == 105.0
    assert episode.end_deviation_bps == 5.0
    assert episode.total_duration_seconds == pytest.approx(70.0)
    assert episode.post_confirmation_alive_seconds == pytest.approx(10.0)
    assert instance.active_episode is None

    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))
    instance.observe(observation(60, raw=112.0))
    crossed_mean = instance.observe(observation(70, raw=99.0))
    assert crossed_mean[0].episode.resolution_reason == "returned_to_mean_band"
    assert crossed_mean[0].episode.end_deviation_bps == pytest.approx(-1.0)


def test_data_gap_abandons_candidate_and_resolves_confirmed_episode():
    candidate = tracker(max_gap=20)
    candidate.observe(observation(0, raw=112.0))
    candidate.observe(observation(30, raw=112.0))
    assert candidate.active_episode is not None
    assert candidate.active_episode.candidate_started_at == START + timedelta(seconds=30)
    assert candidate.confirmed_episodes == ()

    confirmed = tracker(max_gap=20)
    for seconds in range(0, 61, 10):
        confirmed.observe(observation(seconds, raw=112.0))
    transitions = confirmed.observe(observation(90, raw=112.0))

    assert [transition.kind for transition in transitions] == ["resolved", "candidate_started"]
    assert transitions[0].episode.resolution_reason == "data_gap"
    assert transitions[0].episode.ended_at == START + timedelta(seconds=60)


def test_active_episode_at_end_is_marked_open_at_end():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))
    instance.observe(observation(60, raw=112.0))

    episode = instance.finalize()

    assert episode is not None
    assert episode.resolution_reason == "open_at_end"
    assert episode.ended_at == START + timedelta(seconds=60)
    assert instance.active_episode is None
    assert instance.confirmed_episodes == (episode,)


def test_episode_serialization_round_trip_is_json_compatible():
    instance = tracker(max_gap=120)
    instance.observe(observation(0, raw=112.0))
    instance.observe(observation(60, raw=112.0))
    instance.observe(observation(70, raw=120.0))
    episode = instance.finalize()
    assert episode is not None

    payload = episode.to_dict()
    restored = type(episode).from_dict(payload)

    assert restored == episode
    assert payload["candidate_started_at"].endswith("+00:00")
    assert payload["pair_key"] == {
        "canonical_symbol": "QQQ",
        "long_venue": "arcus",
        "long_venue_symbol": "QQQ-USD",
        "short_venue": "lighter_robinhood",
        "short_venue_symbol": "QQQ",
    }


def test_timestamps_cannot_move_backward():
    instance = tracker()
    instance.observe(observation(10, raw=112.0))

    with pytest.raises(ValueError, match="move backwards"):
        instance.observe(observation(0, raw=112.0))


def test_directional_pair_keys_remain_independent():
    reverse_key = SpreadPairKey(
        canonical_symbol="QQQ",
        long_venue="lighter_robinhood",
        long_venue_symbol="QQQ",
        short_venue="arcus",
        short_venue_symbol="QQQ-USD",
    )
    forward = AnomalyTracker(KEY, AnomalyParameters())
    reverse = AnomalyTracker(reverse_key, AnomalyParameters())

    forward.observe(observation(0, raw=112.0))

    assert forward.active_episode is not None
    assert reverse.active_episode is None
    assert forward.active_episode.pair_key != reverse_key
