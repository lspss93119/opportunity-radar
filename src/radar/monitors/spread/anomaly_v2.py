from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
import math

from radar.config import AnomalyV2Config
from radar.monitors.base import AlertRequest, JSONValue
from radar.monitors.spread.anomaly import (
    AnomalyEpisode,
    AnomalyObservation,
    AnomalyParameters,
    AnomalyTracker,
)
from radar.monitors.spread.basis import RollingBasisStats
from radar.monitors.spread.models import SpreadCandidate, SpreadPairKey
from radar.state import RadarState

OpportunityEvent = tuple[str, str, object, datetime]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


@dataclass
class AnomalyV2EpisodeState:
    episode: AnomalyEpisode
    candidate: SpreadCandidate
    initial_event_emitted: bool = False
    last_expansion_event_peak_deviation_bps: float | None = None


@dataclass(frozen=True)
class AnomalyV2Evaluation:
    alerts: tuple[AlertRequest, ...]
    events: tuple[OpportunityEvent, ...]


class AnomalyV2Lifecycle:
    """Small live adapter around the shared pure AnomalyTracker."""

    def __init__(self, config: AnomalyV2Config, *, primary_size_usd: int = 10_000) -> None:
        self.config = config
        self.primary_size_usd = primary_size_usd
        self.parameters = AnomalyParameters(
            anomaly_deviation_bps=config.deviation_bps,
            confirmation_seconds=config.confirmation_seconds,
            return_band_bps=config.return_band_bps,
            max_gap_seconds=config.max_gap_seconds,
        )
        self._trackers: dict[SpreadPairKey, AnomalyTracker] = {}
        self._states: dict[SpreadPairKey, AnomalyV2EpisodeState] = {}

    @property
    def active_states(self) -> tuple[AnomalyV2EpisodeState, ...]:
        return tuple(
            self._states[key]
            for key in sorted(self._states, key=_pair_sort_key)
        )

    def snapshot(self) -> dict[str, object]:
        self._validate_active_state_invariants()
        return {
            state.episode.episode_id: {
                "episode": state.episode.to_dict(),
                "candidate": _candidate_to_dict(state.candidate),
                "initial_event_emitted": state.initial_event_emitted,
                "last_expansion_event_peak_deviation_bps": (
                    state.last_expansion_event_peak_deviation_bps
                ),
                "tracker": self._trackers[state.episode.pair_key].state_dict(),
            }
            for state in self._states.values()
        }

    def _validate_active_state_invariants(self) -> None:
        for key, state in self._states.items():
            tracker = self._trackers.get(key)
            if tracker is None:
                raise RuntimeError(
                    "anomaly v2 state/tracker invariant violated: "
                    f"missing tracker for {key!r}"
                )
            active_episode = tracker.active_episode
            if active_episode is None:
                raise RuntimeError(
                    "anomaly v2 state/tracker invariant violated: "
                    f"state has no active tracker episode for {key!r}"
                )
            if tracker.pair_key != key or active_episode.pair_key != key:
                raise RuntimeError(
                    "anomaly v2 state/tracker invariant violated: "
                    f"tracker pair mismatch for {key!r}"
                )
            if state.episode.pair_key != key or state.candidate.key != key:
                raise RuntimeError(
                    "anomaly v2 state/tracker invariant violated: "
                    f"state pair mismatch for {key!r}"
                )
            if state.episode.episode_id != active_episode.episode_id:
                raise RuntimeError(
                    "anomaly v2 state/tracker invariant violated: "
                    f"episode identity mismatch for {key!r}"
                )
        for key, tracker in self._trackers.items():
            if tracker.active_episode is not None and key not in self._states:
                raise RuntimeError(
                    "anomaly v2 state/tracker invariant violated: "
                    f"active tracker has no state for {key!r}"
                )

    def restore(self, raw: object) -> None:
        if not isinstance(raw, dict):
            return
        for value in raw.values():
            try:
                if not isinstance(value, dict):
                    raise TypeError("state must be an object")
                episode = AnomalyEpisode.from_dict(value["episode"])
                candidate = _candidate_from_dict(value["candidate"])
                if candidate.key != episode.pair_key:
                    raise ValueError("candidate pair does not match episode")
                emitted = value.get("initial_event_emitted", False)
                if not isinstance(emitted, bool):
                    raise TypeError("initial_event_emitted must be boolean")
                peak = value.get("last_expansion_event_peak_deviation_bps")
                if peak is not None:
                    peak = _finite(peak, "last_expansion_event_peak_deviation_bps")
                tracker = AnomalyTracker(episode.pair_key, self.parameters)
                tracker_raw = value.get("tracker")
                last_sample_time = episode.last_seen_at
                if isinstance(tracker_raw, dict):
                    raw_last = tracker_raw.get("last_sample_time")
                    if isinstance(raw_last, str):
                        last_sample_time = _parse_time(raw_last)
                tracker.restore(
                    active_episode=episode,
                    last_sample_time=last_sample_time,
                )
                self._trackers[episode.pair_key] = tracker
                self._states[episode.pair_key] = AnomalyV2EpisodeState(
                    episode=episode,
                    candidate=candidate,
                    initial_event_emitted=emitted,
                    last_expansion_event_peak_deviation_bps=peak,
                )
            except (KeyError, TypeError, ValueError):
                continue
        self._validate_active_state_invariants()

    def evaluate(
        self,
        *,
        candidates: Mapping[SpreadPairKey, SpreadCandidate],
        basis_stats: Mapping[SpreadPairKey, RollingBasisStats],
        state: RadarState,
        now: datetime,
    ) -> AnomalyV2Evaluation:
        current_time = _as_utc(now)
        alerts: list[AlertRequest] = []
        events: list[OpportunityEvent] = []
        for key in sorted(candidates, key=_pair_sort_key):
            candidate = candidates[key]
            stats = basis_stats.get(key)
            observation = AnomalyObservation(
                sample_time=candidate.sample_time,
                raw_spread_bps=candidate.raw_spread_bps,
                rolling_mean_bps=stats.mean_bps if stats is not None else None,
                rolling_std_bps=stats.std_bps if stats is not None else None,
                basis_eligible=stats.eligible if stats is not None else False,
            )
            tracker = self._trackers.setdefault(
                key,
                AnomalyTracker(key, self.parameters),
            )
            transitions = tracker.observe(observation)
            state_record = self._states.get(key)
            for transition in transitions:
                if transition.kind == "candidate_started":
                    state_record = AnomalyV2EpisodeState(
                        episode=transition.episode,
                        candidate=candidate,
                    )
                    self._states[key] = state_record
                elif transition.kind == "confirmed":
                    state_record = self._ensure_state(state_record, transition.episode, candidate)
                    state_record.episode = transition.episode
                    if not state_record.initial_event_emitted:
                        alert = self._alert_for(
                            "anomaly_initial",
                            state_record.episode,
                            candidate,
                            current_time,
                            state,
                        )
                        alerts.append(alert)
                        events.append(
                            self._event(
                                "anomaly_confirmed",
                                alert,
                                transition.episode.confirmed_at or candidate.sample_time,
                            )
                        )
                        state_record.initial_event_emitted = True
                elif transition.kind == "resolved":
                    # A data-gap transition can be followed by a new
                    # candidate_started transition in the same observation.
                    # Consume the old state before replacing it so the
                    # resolved event retains the old candidate's exact VWAPs.
                    resolved_state = state_record
                    if resolved_state is None:
                        resolved_state = AnomalyV2EpisodeState(
                            episode=transition.episode,
                            candidate=candidate,
                        )
                    resolved_state.episode = transition.episode
                    alert = self._alert_for(
                        "anomaly_return"
                        if transition.episode.resolution_reason
                        == "returned_to_mean_band"
                        else "anomaly_resolved",
                        transition.episode,
                        resolved_state.candidate,
                        current_time,
                        state,
                    )
                    events.append(
                        self._event("anomaly_resolved", alert, transition.at)
                    )
                    if (
                        transition.episode.resolution_reason
                        == "returned_to_mean_band"
                    ):
                        alerts.append(alert)
                    self._states.pop(key, None)
                    state_record = None
            if tracker.active_episode is not None:
                state_record = self._ensure_state(
                    state_record,
                    tracker.active_episode,
                    candidate,
                )
                state_record.episode = AnomalyEpisode.from_dict(
                    tracker.active_episode.to_dict()
                )
                state_record.candidate = candidate
                self._maybe_expansion(
                    state_record,
                    candidate,
                    current_time,
                    state,
                    alerts,
                    events,
                )
            else:
                # An unconfirmed candidate can be abandoned by the tracker
                # on a present observation without emitting a transition.
                # Remove the wrapper state before inactive trackers are swept.
                self._states.pop(key, None)
        for key, tracker in tuple(self._trackers.items()):
            if key in candidates:
                continue
            transitions = tracker.advance_time(current_time)
            state_record = self._states.get(key)
            for transition in transitions:
                if transition.kind != "resolved" or state_record is None:
                    continue
                state_record.episode = transition.episode
                alert = self._alert_for(
                    "anomaly_resolved",
                    transition.episode,
                    state_record.candidate,
                    current_time,
                    state,
                )
                events.append(self._event("anomaly_resolved", alert, transition.at))
                self._states.pop(key, None)
            if tracker.active_episode is None:
                # Unconfirmed gaps are abandoned without a lifecycle event;
                # remove their persisted wrapper state as well.
                self._states.pop(key, None)
        # A tracker with no active episode has no restartable live state.
        for key, tracker in tuple(self._trackers.items()):
            if tracker.active_episode is None:
                self._trackers.pop(key, None)
        self._validate_active_state_invariants()
        return AnomalyV2Evaluation(tuple(alerts), tuple(events))

    def _ensure_state(
        self,
        current: AnomalyV2EpisodeState | None,
        episode: AnomalyEpisode,
        candidate: SpreadCandidate,
    ) -> AnomalyV2EpisodeState:
        if current is not None:
            return current
        state = AnomalyV2EpisodeState(episode=episode, candidate=candidate)
        self._states[episode.pair_key] = state
        return state

    def _maybe_expansion(
        self,
        state_record: AnomalyV2EpisodeState,
        candidate: SpreadCandidate,
        now: datetime,
        state: RadarState,
        alerts: list[AlertRequest],
        events: list[OpportunityEvent],
    ) -> None:
        episode = state_record.episode
        peak = episode.post_confirmation_peak_deviation_bps
        confirmation = episode.confirmation_deviation_bps
        if peak is None or confirmation is None:
            return
        baseline = (
            confirmation
            if state_record.last_expansion_event_peak_deviation_bps is None
            else state_record.last_expansion_event_peak_deviation_bps
        )
        if peak - baseline < self.config.expansion_notify_step_bps:
            return
        alert = self._alert_for("anomaly_expansion", episode, candidate, now, state)
        alerts.append(alert)
        events.append(self._event("anomaly_expansion", alert, candidate.sample_time))
        state_record.last_expansion_event_peak_deviation_bps = peak

    def _alert_for(
        self,
        event_kind: str,
        episode: AnomalyEpisode,
        candidate: SpreadCandidate,
        now: datetime,
        state: RadarState,
    ) -> AlertRequest:
        payload: dict[str, JSONValue] = {
            "event_kind": event_kind,
            "episode_id": episode.episode_id,
            "canonical_symbol": candidate.key.canonical_symbol,
            "long_venue": candidate.key.long_venue,
            "long_venue_symbol": candidate.key.long_venue_symbol,
            "short_venue": candidate.key.short_venue,
            "short_venue_symbol": candidate.key.short_venue_symbol,
            "primary_size_usd": self.primary_size_usd,
            "sample_time": candidate.sample_time.isoformat(),
            "candidate_started_at": episode.candidate_started_at.isoformat(),
            "confirmed_at": _optional_iso(episode.confirmed_at),
            "reference_mean_bps": episode.reference_mean_bps,
            "reference_std_bps": episode.reference_std_bps,
            "confirmation_spread_bps": episode.confirmation_spread_bps,
            "confirmation_deviation_bps": episode.confirmation_deviation_bps,
            "lifetime_peak_spread_bps": episode.lifetime_peak_spread_bps,
            "lifetime_peak_deviation_bps": episode.lifetime_peak_deviation_bps,
            "lifetime_peak_at": episode.lifetime_peak_at.isoformat(),
            "post_confirmation_peak_spread_bps": episode.post_confirmation_peak_spread_bps,
            "post_confirmation_peak_deviation_bps": episode.post_confirmation_peak_deviation_bps,
            "post_confirmation_peak_at": _optional_iso(episode.post_confirmation_peak_at),
            "current_spread_bps": episode.current_spread_bps,
            "current_deviation_bps": episode.current_deviation_from_reference_bps,
            "current_live_mean_bps": episode.current_live_mean_bps,
            "current_live_std_bps": episode.current_live_std_bps,
            "ended_at": _optional_iso(episode.ended_at),
            "end_spread_bps": episode.end_spread_bps,
            "end_deviation_bps": episode.end_deviation_bps,
            "resolution_reason": episode.resolution_reason,
            "long_buy_vwap": candidate.long_buy_vwap,
            "short_sell_vwap": candidate.short_sell_vwap,
            "raw_spread_bps": candidate.raw_spread_bps,
            "long_fee_bps": candidate.long_fee_bps,
            "short_fee_bps": candidate.short_fee_bps,
            "net_spread_bps": candidate.net_spread_bps,
            "observed_at_skew_seconds": candidate.observed_at_skew_seconds,
            "funding_context": {
                "long": _funding_payload(state, candidate.key, "long"),
                "short": _funding_payload(state, candidate.key, "short"),
            },
        }
        return AlertRequest(
            monitor="spread",
            event_id=self._event_id(event_kind, episode, candidate),
            created_at=now,
            payload=payload,
        )

    @staticmethod
    def _event(
        event_type: str,
        alert: AlertRequest,
        occurred_at: datetime,
    ) -> OpportunityEvent:
        return (alert.event_id, event_type, alert.payload, occurred_at)

    @staticmethod
    def _event_id(
        event_kind: str,
        episode: AnomalyEpisode,
        candidate: SpreadCandidate,
    ) -> str:
        if event_kind == "anomaly_initial":
            suffix = "confirmed"
        elif event_kind == "anomaly_expansion":
            suffix = "expansion:" + (
                episode.post_confirmation_peak_at or candidate.sample_time
            ).isoformat()
        elif event_kind == "anomaly_return":
            suffix = "resolved"
        else:
            suffix = "resolved:" + str(episode.resolution_reason)
        return f"{episode.episode_id}:v2:{suffix}"


def _pair_sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
    return (
        key.canonical_symbol,
        key.long_venue,
        key.long_venue_symbol,
        key.short_venue,
        key.short_venue_symbol,
    )


def _finite(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise TypeError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _optional_iso(value: datetime | None) -> str | None:
    return None if value is None else _as_utc(value).isoformat()


def _parse_time(value: str) -> datetime:
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _candidate_to_dict(candidate: SpreadCandidate) -> dict[str, JSONValue]:
    return {
        "key": {
            "canonical_symbol": candidate.key.canonical_symbol,
            "long_venue": candidate.key.long_venue,
            "long_venue_symbol": candidate.key.long_venue_symbol,
            "short_venue": candidate.key.short_venue,
            "short_venue_symbol": candidate.key.short_venue_symbol,
        },
        "sample_time": candidate.sample_time.isoformat(),
        "long_buy_vwap": candidate.long_buy_vwap,
        "short_sell_vwap": candidate.short_sell_vwap,
        "long_fee_bps": candidate.long_fee_bps,
        "short_fee_bps": candidate.short_fee_bps,
        "raw_spread_bps": candidate.raw_spread_bps,
        "net_spread_bps": candidate.net_spread_bps,
        "observed_at_skew_seconds": candidate.observed_at_skew_seconds,
    }


def _candidate_from_dict(raw: object) -> SpreadCandidate:
    if not isinstance(raw, dict) or not isinstance(raw.get("key"), dict):
        raise TypeError("candidate must be an object")
    key_raw = raw["key"]
    key = SpreadPairKey(
        canonical_symbol=_text(key_raw["canonical_symbol"]),
        long_venue=_text(key_raw["long_venue"]),
        long_venue_symbol=_text(key_raw["long_venue_symbol"]),
        short_venue=_text(key_raw["short_venue"]),
        short_venue_symbol=_text(key_raw["short_venue_symbol"]),
    )
    return SpreadCandidate(
        key=key,
        sample_time=_parse_time(_text(raw["sample_time"])),
        long_buy_vwap=_positive(raw["long_buy_vwap"]),
        short_sell_vwap=_positive(raw["short_sell_vwap"]),
        long_fee_bps=_optional_nonnegative(raw.get("long_fee_bps")),
        short_fee_bps=_optional_nonnegative(raw.get("short_fee_bps")),
        raw_spread_bps=_finite(raw["raw_spread_bps"], "raw_spread_bps"),
        net_spread_bps=(
            None
            if raw.get("net_spread_bps") is None
            else _finite(raw["net_spread_bps"], "net_spread_bps")
        ),
        observed_at_skew_seconds=_finite(
            raw.get("observed_at_skew_seconds", 0.0),
            "observed_at_skew_seconds",
        ),
    )


def _text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("text must be non-empty")
    return value


def _positive(value: object) -> float:
    result = _finite(value, "price")
    if result <= 0:
        raise ValueError("price must be positive")
    return result


def _optional_nonnegative(value: object) -> float | None:
    if value is None:
        return None
    result = _finite(value, "fee")
    if result < 0:
        raise ValueError("fee must be non-negative")
    return result


def _funding_payload(
    state: RadarState,
    key: SpreadPairKey,
    side: str,
) -> dict[str, JSONValue] | None:
    venue = key.long_venue if side == "long" else key.short_venue
    venue_symbol = key.long_venue_symbol if side == "long" else key.short_venue_symbol
    for funding in state.funding:
        if (
            funding.canonical_symbol == key.canonical_symbol
            and funding.venue == venue
            and funding.venue_symbol == venue_symbol
        ):
            return {
                "effective_time": funding.effective_time.isoformat(),
                "observed_at": funding.observed_at.isoformat(),
                "venue": funding.venue,
                "venue_symbol": funding.venue_symbol,
                "canonical_symbol": funding.canonical_symbol,
                "funding_rate": funding.funding_rate,
                "next_funding_time": (
                    funding.next_funding_time.isoformat()
                    if funding.next_funding_time is not None
                    else None
                ),
            }
    return None
