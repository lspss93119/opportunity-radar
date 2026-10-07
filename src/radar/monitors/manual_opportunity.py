from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import logging
import math
import time
from typing import Any

from radar.config import ManualOpportunityConfig
from radar.models import MarketSnapshot
from radar.monitors.base import AlertRequest, JSONValue
from radar.monitors.spread.models import SpreadPairKey
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore

DEFAULT_MANUAL_STALE_AFTER_SECONDS = 30
LOGGER = logging.getLogger(__name__)


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000.0


def _as_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must use UTC")
    return value.astimezone(UTC)


def _finite(value: float, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


def _positive(value: float, field_name: str) -> float:
    result = _finite(value, field_name)
    if result <= 0:
        raise ValueError(f"{field_name} must be positive")
    return result


def _optional_finite(value: float | None, field_name: str) -> float | None:
    if value is None:
        return None
    return _finite(value, field_name)


def _optional_nonnegative(value: float | None, field_name: str) -> float | None:
    result = _optional_finite(value, field_name)
    if result is not None and result < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return result


def _fee_for(venue: str, fees_bps: Mapping[str, float]) -> float | None:
    for configured_venue, raw_fee in fees_bps.items():
        if not isinstance(configured_venue, str):
            continue
        if configured_venue.lower() != venue.lower():
            continue
        if isinstance(raw_fee, bool):
            return None
        try:
            fee = float(raw_fee)
        except (TypeError, ValueError):
            return None
        return fee if math.isfinite(fee) and fee >= 0 else None
    return None


def _log_transition(
    event: str,
    *,
    sample_time: datetime,
    available_at: datetime,
    key: SpreadPairKey,
    expected_net: float | None,
    reason: str | None = None,
) -> None:
    LOGGER.info(
        "manual opportunity %s sample_time=%s available_at=%s "
        "symbol=%s long_venue=%s short_venue=%s expected_net=%s%s",
        event,
        sample_time,
        available_at,
        key.canonical_symbol,
        key.long_venue,
        key.short_venue,
        expected_net,
        "" if reason is None else f" reason={reason}",
    )


@dataclass(frozen=True)
class ManualOpportunityObservation:
    key: SpreadPairKey
    sample_time: datetime
    long_observed_at: datetime
    short_observed_at: datetime
    long_best_ask: float
    short_best_bid: float
    mean_2h_bps: float | None
    mean_24h_bps: float | None
    mean_3d_bps: float | None
    long_volume_24h: float | None
    short_volume_24h: float | None
    long_fee_bps: float | None
    short_fee_bps: float | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "sample_time", _as_utc(self.sample_time, "sample_time"))
        object.__setattr__(
            self,
            "long_observed_at",
            _as_utc(self.long_observed_at, "long_observed_at"),
        )
        object.__setattr__(
            self,
            "short_observed_at",
            _as_utc(self.short_observed_at, "short_observed_at"),
        )
        object.__setattr__(self, "long_best_ask", _positive(self.long_best_ask, "long_best_ask"))
        object.__setattr__(self, "short_best_bid", _positive(self.short_best_bid, "short_best_bid"))
        for name in ("mean_2h_bps", "mean_24h_bps", "mean_3d_bps"):
            object.__setattr__(self, name, _optional_finite(getattr(self, name), name))
        for name in ("long_volume_24h", "short_volume_24h"):
            object.__setattr__(
                self, name, _optional_nonnegative(getattr(self, name), name)
            )
        for name in ("long_fee_bps", "short_fee_bps"):
            object.__setattr__(
                self, name, _optional_nonnegative(getattr(self, name), name)
            )

    @property
    def current_spread_bps(self) -> float:
        value = (self.short_best_bid / self.long_best_ask - 1.0) * 10_000.0
        if not math.isfinite(value):
            raise ValueError("current BBO spread must be finite")
        return value

    @property
    def available_at(self) -> datetime:
        """Earliest time when both BBO legs and the sample slot were available."""
        return max(self.sample_time, self.long_observed_at, self.short_observed_at)

    @property
    def reference_a_bps(self) -> float | None:
        return self.mean_24h_bps

    @property
    def baseline_range_bps(self) -> float | None:
        means = (self.mean_2h_bps, self.mean_24h_bps, self.mean_3d_bps)
        if any(value is None for value in means):
            return None
        numeric_means = tuple(value for value in means if value is not None)
        return max(numeric_means) - min(numeric_means)

    @property
    def round_trip_fee_bps(self) -> float | None:
        if self.long_fee_bps is None or self.short_fee_bps is None:
            return None
        return 2.0 * (self.long_fee_bps + self.short_fee_bps)

    @property
    def route_volume_24h(self) -> float | None:
        if self.long_volume_24h is None or self.short_volume_24h is None:
            return None
        return min(self.long_volume_24h, self.short_volume_24h)

    @property
    def deviation_bps(self) -> float | None:
        if self.reference_a_bps is None:
            return None
        return self.current_spread_bps - self.reference_a_bps

    @property
    def expected_net_at_a_bps(self) -> float | None:
        if self.deviation_bps is None or self.round_trip_fee_bps is None:
            return None
        return self.deviation_bps - self.round_trip_fee_bps


def build_manual_observation(
    long_snapshot: MarketSnapshot,
    short_snapshot: MarketSnapshot,
    *,
    mean_2h_bps: float | None,
    mean_24h_bps: float | None,
    mean_3d_bps: float | None,
    long_volume_24h: float | None,
    short_volume_24h: float | None,
    fees_bps: Mapping[str, float],
) -> ManualOpportunityObservation:
    if long_snapshot.canonical_symbol != short_snapshot.canonical_symbol:
        raise ValueError("snapshots must share canonical_symbol")
    if long_snapshot.venue.lower() == short_snapshot.venue.lower():
        raise ValueError("long and short venues must differ")
    if long_snapshot.sample_time != short_snapshot.sample_time:
        raise ValueError("snapshots must share sample_time")
    key = SpreadPairKey(
        canonical_symbol=long_snapshot.canonical_symbol,
        long_venue=long_snapshot.venue,
        long_venue_symbol=long_snapshot.venue_symbol,
        short_venue=short_snapshot.venue,
        short_venue_symbol=short_snapshot.venue_symbol,
    )
    return ManualOpportunityObservation(
        key=key,
        sample_time=long_snapshot.sample_time,
        long_observed_at=long_snapshot.observed_at,
        short_observed_at=short_snapshot.observed_at,
        long_best_ask=long_snapshot.best_ask,
        short_best_bid=short_snapshot.best_bid,
        mean_2h_bps=mean_2h_bps,
        mean_24h_bps=mean_24h_bps,
        mean_3d_bps=mean_3d_bps,
        long_volume_24h=long_volume_24h,
        short_volume_24h=short_volume_24h,
        long_fee_bps=_fee_for(long_snapshot.venue, fees_bps),
        short_fee_bps=_fee_for(short_snapshot.venue, fees_bps),
    )


def manual_opportunity_rejection_reason(
    observation: ManualOpportunityObservation,
    config: ManualOpportunityConfig,
) -> str | None:
    if (
        observation.mean_2h_bps is None
        or observation.mean_24h_bps is None
        or observation.mean_3d_bps is None
        or observation.baseline_range_bps is None
    ):
        return "insufficient_baseline_history"
    if observation.baseline_range_bps > config.baseline_range_max_bps:
        return "unstable_baseline"
    if observation.round_trip_fee_bps is None:
        return "fees_unavailable"
    expected_net = observation.expected_net_at_a_bps
    if expected_net is None or expected_net + 1e-9 < config.expected_net_min_bps:
        return "expected_net_below_min"
    route_volume = observation.route_volume_24h
    if route_volume is None or not math.isfinite(route_volume) or route_volume <= 0:
        return "volume_unavailable"
    if route_volume < config.volume_24h_min_usd:
        return "volume_below_min"
    return None


def manual_opportunity_temporal_rejection_reason(
    observation: ManualOpportunityObservation,
    now: datetime,
    *,
    stale_after_seconds: int = DEFAULT_MANUAL_STALE_AFTER_SECONDS,
) -> str | None:
    """Reject BBO data that was not available and fresh at evaluation time."""
    current_time = _as_utc(now, "now")
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    if observation.sample_time > current_time:
        return "future_sample"
    for observed_at in (observation.long_observed_at, observation.short_observed_at):
        age_seconds = (current_time - observed_at).total_seconds()
        if age_seconds < 0:
            return "future_bbo"
        if age_seconds > stale_after_seconds:
            return "stale_bbo"
    return None


@dataclass
class ManualOpportunityEpisode:
    key: SpreadPairKey
    episode_id: str
    candidate_started_at: datetime
    last_seen_at: datetime
    last_observation: ManualOpportunityObservation
    confirmed_at: datetime | None = None
    reference_a_bps: float | None = None
    frozen_round_trip_fee_bps: float | None = None
    confirmation_current_spread_bps: float | None = None
    confirmation_deviation_bps: float | None = None
    confirmation_expected_net_at_a_bps: float | None = None
    highest_notified_level_bps: float | None = None


@dataclass
class _ManualNotificationRouteState:
    key: SpreadPairKey
    armed: bool = True
    last_sent_episode_id: str | None = None
    quiet_started_at: datetime | None = None
    last_observation_at: datetime | None = None
    pending_episode_id: str | None = None


class ManualOpportunityNotificationGate:
    """Restart-safe Telegram gate for one exact directional route."""

    name = "manual_opportunity"
    state_key = "telegram_notification_gate"

    def __init__(
        self,
        config: ManualOpportunityConfig,
        *,
        runtime_store: SQLiteRuntimeStore | None = None,
    ) -> None:
        self._config = config
        self._runtime_store = runtime_store
        self._states = self._load_states(
            None
            if runtime_store is None
            else runtime_store.get_monitor_state(self.name, self.state_key)
        )
        self._cycle_originals: dict[
            SpreadPairKey, _ManualNotificationRouteState | None
        ] | None = None

    def is_armed(self, key: SpreadPairKey) -> bool:
        state = self._states.get(key)
        return state is None or state.armed

    def notified_episode_id(self, key: SpreadPairKey) -> str | None:
        state = self._states.get(key)
        return None if state is None else state.last_sent_episode_id

    def reserve_initial(self, key: SpreadPairKey, episode_id: str) -> bool:
        state = self._states.setdefault(
            key, _ManualNotificationRouteState(key=key)
        )
        if not state.armed or state.pending_episode_id is not None:
            return False
        state.pending_episode_id = episode_id
        return True

    def mark_sent(
        self,
        key: SpreadPairKey,
        episode_id: str,
        *,
        sent_at: datetime | None = None,
    ) -> None:
        timestamp = datetime.now(UTC) if sent_at is None else _as_utc(sent_at, "sent_at")
        state = self._states.get(key)
        if state is None:
            state = _ManualNotificationRouteState(key=key)
            self._states[key] = state
        if (
            state.pending_episode_id is not None
            and state.pending_episode_id != episode_id
        ):
            raise ValueError("notification reservation does not match episode")
        state.armed = False
        state.last_sent_episode_id = episode_id
        state.pending_episode_id = None
        state.quiet_started_at = None
        LOGGER.info(
            "manual_telegram_disarmed symbol=%s long_venue=%s short_venue=%s "
            "episode_id=%s",
            key.canonical_symbol,
            key.long_venue,
            key.short_venue,
            episode_id,
        )
        self._persist(timestamp)

    def mark_failed(self, key: SpreadPairKey, episode_id: str) -> None:
        state = self._states.get(key)
        if state is not None and state.pending_episode_id == episode_id:
            state.pending_episode_id = None

    def allow_expansion(self, key: SpreadPairKey, episode_id: str) -> bool:
        state = self._states.get(key)
        return state is not None and state.last_sent_episode_id == episode_id

    def observe(
        self,
        key: SpreadPairKey,
        *,
        qualifies: bool,
        observed_at: datetime,
    ) -> None:
        timestamp = _as_utc(observed_at, "observed_at")
        state = self._states.get(key)
        if state is None:
            return
        if state.last_observation_at is not None and timestamp < state.last_observation_at:
            return
        if state.armed:
            state.last_observation_at = timestamp
            return
        previous = state.last_observation_at
        state.last_observation_at = timestamp
        if previous is None or (
            timestamp - previous
        ).total_seconds() > self._config.max_gap_seconds:
            state.quiet_started_at = timestamp
            LOGGER.info(
                "manual_telegram_quiet_start symbol=%s long_venue=%s "
                "short_venue=%s",
                key.canonical_symbol,
                key.long_venue,
                key.short_venue,
            )
            return
        if qualifies:
            if state.quiet_started_at is not None:
                LOGGER.info(
                    "manual_telegram_quiet_reset symbol=%s long_venue=%s "
                    "short_venue=%s",
                    key.canonical_symbol,
                    key.long_venue,
                    key.short_venue,
                )
            state.quiet_started_at = None
            return
        if state.quiet_started_at is None:
            state.quiet_started_at = timestamp
            LOGGER.info(
                "manual_telegram_quiet_start symbol=%s long_venue=%s "
                "short_venue=%s",
                key.canonical_symbol,
                key.long_venue,
                key.short_venue,
            )
            return
        quiet_seconds = (timestamp - state.quiet_started_at).total_seconds()
        if quiet_seconds >= self._config.telegram_rearm_quiet_seconds:
            state.armed = True
            state.quiet_started_at = None
            LOGGER.info(
                "manual_telegram_rearmed symbol=%s long_venue=%s short_venue=%s",
                key.canonical_symbol,
                key.long_venue,
                key.short_venue,
            )

    def begin_cycle(self) -> None:
        if self._cycle_originals is not None:
            raise RuntimeError("manual notification gate cycle is already active")
        self._cycle_originals = {
            key: replace(state) for key, state in self._states.items()
        }

    def rollback_cycle(self) -> None:
        if self._cycle_originals is None:
            return
        self._states = {
            key: replace(state)
            for key, state in self._cycle_originals.items()
            if state is not None
        }
        self._cycle_originals = None

    def commit_cycle(self, updated_at: datetime, *, persist: bool = True) -> None:
        if self._cycle_originals is None:
            raise RuntimeError("manual notification gate cycle is not active")
        if persist:
            self._persist(_as_utc(updated_at, "updated_at"))
        self._cycle_originals = None

    def serialized_state(self) -> dict[str, JSONValue]:
        routes: list[JSONValue] = []
        for state in sorted(self._states.values(), key=lambda item: self._sort_key(item.key)):
            routes.append(
                {
                    "key": self._serialize_key(state.key),
                    "armed": state.armed,
                    "last_sent_episode_id": state.last_sent_episode_id,
                    "quiet_started_at": (
                        None
                        if state.quiet_started_at is None
                        else state.quiet_started_at.isoformat()
                    ),
                    "last_observation_at": (
                        None
                        if state.last_observation_at is None
                        else state.last_observation_at.isoformat()
                    ),
                }
            )
        return {"routes": routes}

    def _persist(self, updated_at: datetime) -> None:
        if self._runtime_store is not None:
            self._runtime_store.set_monitor_state(
                self.name,
                self.state_key,
                self.serialized_state(),
                updated_at=updated_at,
            )

    @classmethod
    def _load_states(
        cls, state: object | None
    ) -> dict[SpreadPairKey, _ManualNotificationRouteState]:
        if state is None:
            return {}
        if not isinstance(state, dict) or not isinstance(state.get("routes"), list):
            raise ValueError("invalid manual notification gate state")
        result: dict[SpreadPairKey, _ManualNotificationRouteState] = {}
        for raw in state["routes"]:
            if not isinstance(raw, dict):
                raise ValueError("invalid manual notification route state")
            key = cls._deserialize_key(raw.get("key"))
            armed = raw.get("armed")
            if not isinstance(armed, bool):
                raise ValueError("invalid manual notification armed state")
            last_sent = raw.get("last_sent_episode_id")
            if last_sent is not None and not isinstance(last_sent, str):
                raise ValueError("invalid manual notification episode state")
            result[key] = _ManualNotificationRouteState(
                key=key,
                armed=armed,
                last_sent_episode_id=last_sent,
                quiet_started_at=_optional_time(
                    raw.get("quiet_started_at"), "quiet_started_at"
                ),
                last_observation_at=_optional_time(
                    raw.get("last_observation_at"), "last_observation_at"
                ),
            )
        return result

    @staticmethod
    def _serialize_key(key: SpreadPairKey) -> dict[str, JSONValue]:
        return {
            "canonical_symbol": key.canonical_symbol,
            "long_venue": key.long_venue,
            "long_venue_symbol": key.long_venue_symbol,
            "short_venue": key.short_venue,
            "short_venue_symbol": key.short_venue_symbol,
        }

    @staticmethod
    def _deserialize_key(raw: object) -> SpreadPairKey:
        if not isinstance(raw, dict):
            raise ValueError("invalid manual notification route key")
        return SpreadPairKey(
            canonical_symbol=_required_text(raw.get("canonical_symbol"), "canonical_symbol"),
            long_venue=_required_text(raw.get("long_venue"), "long_venue"),
            long_venue_symbol=_required_text(raw.get("long_venue_symbol"), "long_venue_symbol"),
            short_venue=_required_text(raw.get("short_venue"), "short_venue"),
            short_venue_symbol=_required_text(raw.get("short_venue_symbol"), "short_venue_symbol"),
        )

    @staticmethod
    def _sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
        return (
            key.canonical_symbol,
            key.long_venue,
            key.long_venue_symbol,
            key.short_venue,
            key.short_venue_symbol,
        )


class ManualOpportunityLifecycle:
    """Standalone BBO-only lifecycle for manual Telegram opportunities."""

    name = "manual_opportunity"
    state_key = "episodes"

    def __init__(
        self,
        config: ManualOpportunityConfig,
        fees_bps: Mapping[str, float],
        *,
        runtime_store: SQLiteRuntimeStore | None = None,
        stale_after_seconds: int = DEFAULT_MANUAL_STALE_AFTER_SECONDS,
    ) -> None:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.config = config
        self._fees_bps = dict(fees_bps)
        self._runtime_store = runtime_store
        self._stale_after_seconds = stale_after_seconds
        self._episodes: dict[SpreadPairKey, ManualOpportunityEpisode] = {}
        self._cycle_originals: dict[
            SpreadPairKey, ManualOpportunityEpisode | None
        ] | None = None
        self._cycle_events: list[tuple[str, str, object, datetime | None]] | None = None
        if runtime_store is not None:
            self._episodes = self._load_episodes(
                runtime_store.get_monitor_state(self.name, self.state_key)
            )

    @property
    def active_episodes(self) -> tuple[ManualOpportunityEpisode, ...]:
        return tuple(
            self._episodes[key]
            for key in sorted(self._episodes, key=self._sort_key)
        )

    @property
    def active_keys(self) -> tuple[SpreadPairKey, ...]:
        """Return active route keys without copying or sorting episode objects."""
        return tuple(self._episodes)

    def is_active(self, key: SpreadPairKey) -> bool:
        return key in self._episodes

    def begin_cycle(self) -> None:
        if self._cycle_originals is not None:
            raise RuntimeError("manual opportunity cycle is already active")
        self._cycle_originals = {}
        self._cycle_events = []

    def rollback_cycle(self) -> None:
        if self._cycle_originals is None:
            return
        for key, previous in self._cycle_originals.items():
            if previous is None:
                self._episodes.pop(key, None)
            else:
                self._episodes[key] = previous
        self._cycle_originals = None
        self._cycle_events = None

    def flush_cycle(
        self,
        updated_at: datetime,
        *,
        additional_states: Sequence[tuple[str, str, object, datetime | None]] = (),
    ) -> None:
        if self._cycle_originals is None or self._cycle_events is None:
            raise RuntimeError("manual opportunity cycle is not active")
        try:
            self._persist(updated_at, self._cycle_events, additional_states)
        except Exception:
            self.rollback_cycle()
            raise
        self._cycle_originals = None
        self._cycle_events = None

    def _remember_episode(self, key: SpreadPairKey) -> None:
        if self._cycle_originals is None:
            raise RuntimeError("manual opportunity cycle is not active")
        if key in self._cycle_originals:
            return
        episode = self._episodes.get(key)
        self._cycle_originals[key] = None if episode is None else replace(episode)

    def evaluate(
        self,
        observation: ManualOpportunityObservation,
        *,
        now: datetime | None = None,
        persist: bool = True,
    ) -> list[AlertRequest]:
        evaluation_time = (
            datetime.now(UTC) if now is None else _as_utc(now, "now")
        )
        owns_cycle = persist
        if owns_cycle:
            self.begin_cycle()
        elif self._cycle_originals is None:
            raise RuntimeError("persist=False requires an active cycle")
        try:
            alerts, events = self._evaluate_mutation(observation, evaluation_time)
            if self._cycle_events is None:
                raise RuntimeError("manual opportunity cycle events are unavailable")
            self._cycle_events.extend(events)
            if owns_cycle:
                self.flush_cycle(evaluation_time)
            return alerts
        except Exception:
            if owns_cycle:
                self.rollback_cycle()
            raise

    def _evaluate_mutation(
        self,
        observation: ManualOpportunityObservation,
        evaluation_time: datetime,
    ) -> tuple[list[AlertRequest], list[tuple[str, str, object, datetime | None]]]:
        observation = self._with_configured_fees(observation)
        self._remember_episode(observation.key)
        episode = self._episodes.get(observation.key)
        temporal_reason = manual_opportunity_temporal_rejection_reason(
            observation,
            evaluation_time,
            stale_after_seconds=self._stale_after_seconds,
        )
        reason = temporal_reason or manual_opportunity_rejection_reason(
            observation, self.config
        )

        if episode is not None and self._gap_exceeded(episode, observation):
            _log_transition(
                "candidate_reset",
                sample_time=observation.sample_time,
                available_at=observation.available_at,
                key=observation.key,
                expected_net=observation.expected_net_at_a_bps,
                reason="gap_exceeded",
            )
            del self._episodes[observation.key]
            episode = None

        if episode is not None and episode.confirmed_at is not None:
            reason = temporal_reason or self._post_confirmation_reason(
                episode, observation
            )

        alerts: list[AlertRequest] = []
        events: list[tuple[str, str, object, datetime | None]] = []
        if reason is not None:
            if episode is not None:
                _log_transition(
                    "candidate_reset",
                    sample_time=observation.sample_time,
                    available_at=observation.available_at,
                    key=observation.key,
                    expected_net=observation.expected_net_at_a_bps,
                    reason=reason,
                )
                del self._episodes[observation.key]
            return alerts, events

        if episode is None:
            episode = self._new_episode(observation)
            self._episodes[observation.key] = episode
            _log_transition(
                "candidate_start",
                sample_time=observation.sample_time,
                available_at=observation.available_at,
                key=observation.key,
                expected_net=observation.expected_net_at_a_bps,
            )
        else:
            episode.last_seen_at = observation.available_at
            episode.last_observation = observation

        if episode.confirmed_at is None:
            elapsed = (
                observation.available_at - episode.candidate_started_at
            ).total_seconds()
            if elapsed >= self.config.confirmation_seconds:
                self._confirm(episode, observation)
                _log_transition(
                    "manual_confirm",
                    sample_time=observation.sample_time,
                    available_at=observation.available_at,
                    key=observation.key,
                    expected_net=episode.confirmation_expected_net_at_a_bps,
                )
                alert = self._build_alert(episode, observation, "manual_initial", None)
                alerts.append(alert)
                events.append(
                    (
                        alert.event_id,
                        "manual_initial",
                        alert.payload,
                        alert.created_at,
                    )
                )
        else:
            expansion_alert = self._maybe_expansion(episode, observation)
            if expansion_alert is not None:
                _log_transition(
                    "manual_expansion",
                    sample_time=observation.sample_time,
                    available_at=observation.available_at,
                    key=observation.key,
                    expected_net=observation.expected_net_at_a_bps,
                )
                alerts.append(expansion_alert)
                events.append(
                    (
                        expansion_alert.event_id,
                        "manual_expansion",
                        expansion_alert.payload,
                        expansion_alert.created_at,
                    )
                )
        return alerts, events

    def observe_gap(
        self,
        key: SpreadPairKey,
        sample_time: datetime,
        *,
        persist: bool = True,
    ) -> None:
        """End a route episode when a sample slot has no valid observation."""
        timestamp = _as_utc(sample_time, "sample_time")
        if key not in self._episodes:
            return
        owns_cycle = persist
        if owns_cycle:
            self.begin_cycle()
        elif self._cycle_originals is None:
            raise RuntimeError("persist=False requires an active cycle")
        try:
            self._remember_episode(key)
            del self._episodes[key]
            if self._cycle_events is None:
                raise RuntimeError("manual opportunity cycle events are unavailable")
            if owns_cycle:
                self.flush_cycle(timestamp)
        except Exception:
            if owns_cycle:
                self.rollback_cycle()
            raise

    def _new_episode(
        self, observation: ManualOpportunityObservation
    ) -> ManualOpportunityEpisode:
        key = observation.key
        candidate_started_at = observation.available_at
        episode_id = (
            f"manual:{key.canonical_symbol}:{key.long_venue}:{key.long_venue_symbol}:"
            f"{key.short_venue}:{key.short_venue_symbol}:"
            f"{candidate_started_at.isoformat()}"
        )
        return ManualOpportunityEpisode(
            key=key,
            episode_id=episode_id,
            candidate_started_at=candidate_started_at,
            last_seen_at=candidate_started_at,
            last_observation=observation,
        )

    def _with_configured_fees(
        self, observation: ManualOpportunityObservation
    ) -> ManualOpportunityObservation:
        long_fee = _fee_for(observation.key.long_venue, self._fees_bps)
        short_fee = _fee_for(observation.key.short_venue, self._fees_bps)
        if (
            observation.long_fee_bps == long_fee
            and observation.short_fee_bps == short_fee
        ):
            return observation
        return replace(
            observation,
            long_fee_bps=long_fee,
            short_fee_bps=short_fee,
        )

    def _confirm(
        self,
        episode: ManualOpportunityEpisode,
        observation: ManualOpportunityObservation,
    ) -> None:
        episode.confirmed_at = observation.sample_time
        episode.reference_a_bps = observation.reference_a_bps
        episode.frozen_round_trip_fee_bps = observation.round_trip_fee_bps
        episode.confirmation_current_spread_bps = observation.current_spread_bps
        episode.confirmation_deviation_bps = (
            None
            if episode.reference_a_bps is None
            else observation.current_spread_bps - episode.reference_a_bps
        )
        episode.confirmation_expected_net_at_a_bps = (
            None
            if episode.confirmation_deviation_bps is None
            or episode.frozen_round_trip_fee_bps is None
            else episode.confirmation_deviation_bps
            - episode.frozen_round_trip_fee_bps
        )
        expected = episode.confirmation_expected_net_at_a_bps
        if expected is None:
            raise ValueError("eligible observation must have an expected net")
        step = self.config.expansion_notify_step_bps
        episode.highest_notified_level_bps = math.floor(expected / step) * step

    def _maybe_expansion(
        self,
        episode: ManualOpportunityEpisode,
        observation: ManualOpportunityObservation,
    ) -> AlertRequest | None:
        if episode.reference_a_bps is None or episode.frozen_round_trip_fee_bps is None:
            raise ValueError("confirmed episode is missing frozen basis")
        expected_net = (
            observation.current_spread_bps
            - episode.reference_a_bps
            - episode.frozen_round_trip_fee_bps
        )
        if not math.isfinite(expected_net):
            raise ValueError("expected net must be finite")
        step = self.config.expansion_notify_step_bps
        level = math.floor(expected_net / step) * step
        watermark = episode.highest_notified_level_bps
        if watermark is None or level <= watermark:
            return None
        episode.highest_notified_level_bps = level
        return self._build_alert(episode, observation, "manual_expansion", level)

    def _post_confirmation_reason(
        self,
        episode: ManualOpportunityEpisode,
        observation: ManualOpportunityObservation,
    ) -> str | None:
        if (
            observation.mean_2h_bps is None
            or observation.mean_24h_bps is None
            or observation.mean_3d_bps is None
            or observation.baseline_range_bps is None
        ):
            return "insufficient_baseline_history"
        if observation.baseline_range_bps > self.config.baseline_range_max_bps:
            return "unstable_baseline"
        if observation.round_trip_fee_bps is None:
            return "fees_unavailable"
        if observation.route_volume_24h is None or observation.route_volume_24h <= 0:
            return "volume_unavailable"
        if observation.route_volume_24h < self.config.volume_24h_min_usd:
            return "volume_below_min"
        if episode.reference_a_bps is None or episode.frozen_round_trip_fee_bps is None:
            return "missing_frozen_basis"
        expected_net = (
            observation.current_spread_bps
            - episode.reference_a_bps
            - episode.frozen_round_trip_fee_bps
        )
        return (
            None
            if expected_net + 1e-9 >= self.config.expected_net_min_bps
            else "expected_net_below_min"
        )

    def _build_alert(
        self,
        episode: ManualOpportunityEpisode,
        observation: ManualOpportunityObservation,
        event_kind: str,
        expansion_level_bps: float | None,
    ) -> AlertRequest:
        reference = (
            episode.reference_a_bps
            if episode.reference_a_bps is not None
            else observation.reference_a_bps
        )
        round_trip_fee = (
            episode.frozen_round_trip_fee_bps
            if episode.frozen_round_trip_fee_bps is not None
            else observation.round_trip_fee_bps
        )
        if reference is None or round_trip_fee is None:
            raise ValueError("manual alert requires frozen basis and fees")
        deviation = observation.current_spread_bps - reference
        expected_net = deviation - round_trip_fee
        signal_duration = int(
            (observation.available_at - episode.candidate_started_at).total_seconds()
        )
        payload: dict[str, JSONValue] = {
            "event_kind": event_kind,
            "episode_id": episode.episode_id,
            "canonical_symbol": episode.key.canonical_symbol,
            "long_venue": episode.key.long_venue,
            "long_venue_symbol": episode.key.long_venue_symbol,
            "short_venue": episode.key.short_venue,
            "short_venue_symbol": episode.key.short_venue_symbol,
            "signal_duration_seconds": signal_duration,
            "current_spread_bps": observation.current_spread_bps,
            "reference_a_bps": reference,
            "deviation_bps": deviation,
            "round_trip_fee_bps": round_trip_fee,
            "expected_net_at_a_bps": expected_net,
            "mean_2h_bps": observation.mean_2h_bps,
            "mean_24h_bps": observation.mean_24h_bps,
            "mean_3d_bps": observation.mean_3d_bps,
            "baseline_range_bps": observation.baseline_range_bps,
            "long_volume_24h": observation.long_volume_24h,
            "short_volume_24h": observation.short_volume_24h,
            "route_volume_24h": observation.route_volume_24h,
            "long_best_ask": observation.long_best_ask,
            "short_best_bid": observation.short_best_bid,
            "sample_time": observation.sample_time.isoformat(),
            "expansion_level_bps": expansion_level_bps,
            "candidate_started_at": episode.candidate_started_at.isoformat(),
            "confirmed_at": (
                None if episode.confirmed_at is None else episode.confirmed_at.isoformat()
            ),
        }
        suffix = "initial" if expansion_level_bps is None else f"expansion:{level_text(expansion_level_bps)}"
        event_id = f"{episode.episode_id}:{suffix}"
        return AlertRequest(
            monitor=self.name,
            event_id=event_id,
            created_at=observation.available_at,
            payload=payload,
        )

    def _persist(
        self,
        updated_at: datetime,
        events: list[tuple[str, str, object, datetime | None]],
        additional_states: Sequence[tuple[str, str, object, datetime | None]] = (),
    ) -> None:
        if self._runtime_store is None:
            return
        self._runtime_store.set_monitor_state_and_append_opportunities(
            self.name,
            self.state_key,
            self._serialize_state(),
            updated_at=updated_at,
            opportunities=events,
            additional_states=additional_states,
        )

    def _serialize_state(self) -> dict[str, JSONValue]:
        return {"episodes": [self._serialize_episode(episode) for episode in self._episodes.values()]}

    @staticmethod
    def _serialize_episode(episode: ManualOpportunityEpisode) -> dict[str, JSONValue]:
        observation = episode.last_observation
        return {
            "key": {
                "canonical_symbol": episode.key.canonical_symbol,
                "long_venue": episode.key.long_venue,
                "long_venue_symbol": episode.key.long_venue_symbol,
                "short_venue": episode.key.short_venue,
                "short_venue_symbol": episode.key.short_venue_symbol,
            },
            "episode_id": episode.episode_id,
            "candidate_started_at": episode.candidate_started_at.isoformat(),
            "last_seen_at": episode.last_seen_at.isoformat(),
            "confirmed_at": None
            if episode.confirmed_at is None
            else episode.confirmed_at.isoformat(),
            "reference_a_bps": episode.reference_a_bps,
            "frozen_round_trip_fee_bps": episode.frozen_round_trip_fee_bps,
            "confirmation_current_spread_bps": episode.confirmation_current_spread_bps,
            "confirmation_deviation_bps": episode.confirmation_deviation_bps,
            "confirmation_expected_net_at_a_bps": episode.confirmation_expected_net_at_a_bps,
            "highest_notified_level_bps": episode.highest_notified_level_bps,
            "last_observation": ManualOpportunityLifecycle._serialize_observation(
                observation
            ),
        }

    @staticmethod
    def _serialize_observation(
        observation: ManualOpportunityObservation,
    ) -> dict[str, JSONValue]:
        return {
            "key": {
                "canonical_symbol": observation.key.canonical_symbol,
                "long_venue": observation.key.long_venue,
                "long_venue_symbol": observation.key.long_venue_symbol,
                "short_venue": observation.key.short_venue,
                "short_venue_symbol": observation.key.short_venue_symbol,
            },
            "sample_time": observation.sample_time.isoformat(),
            "long_observed_at": observation.long_observed_at.isoformat(),
            "short_observed_at": observation.short_observed_at.isoformat(),
            "long_best_ask": observation.long_best_ask,
            "short_best_bid": observation.short_best_bid,
            "mean_2h_bps": observation.mean_2h_bps,
            "mean_24h_bps": observation.mean_24h_bps,
            "mean_3d_bps": observation.mean_3d_bps,
            "long_volume_24h": observation.long_volume_24h,
            "short_volume_24h": observation.short_volume_24h,
            "long_fee_bps": observation.long_fee_bps,
            "short_fee_bps": observation.short_fee_bps,
        }

    @classmethod
    def _load_episodes(cls, state: object) -> dict[SpreadPairKey, ManualOpportunityEpisode]:
        if state is None:
            return {}
        if not isinstance(state, dict) or not isinstance(state.get("episodes"), list):
            raise ValueError("invalid manual opportunity state")
        episodes: dict[SpreadPairKey, ManualOpportunityEpisode] = {}
        for raw in state["episodes"]:
            if not isinstance(raw, dict):
                raise ValueError("invalid manual opportunity episode")
            episode = cls._deserialize_episode(raw)
            episodes[episode.key] = episode
        return episodes

    @classmethod
    def _deserialize_episode(cls, raw: dict[str, object]) -> ManualOpportunityEpisode:
        key = cls._deserialize_key(raw.get("key"))
        raw_observation = raw.get("last_observation")
        if not isinstance(raw_observation, dict):
            raise ValueError("invalid manual opportunity observation")
        observation = cls._deserialize_observation(raw_observation)
        return ManualOpportunityEpisode(
            key=key,
            episode_id=_required_text(raw.get("episode_id"), "episode_id"),
            candidate_started_at=_parse_time(raw.get("candidate_started_at"), "candidate_started_at"),
            last_seen_at=_parse_time(raw.get("last_seen_at"), "last_seen_at"),
            last_observation=observation,
            confirmed_at=_optional_time(raw.get("confirmed_at"), "confirmed_at"),
            reference_a_bps=_optional_number(raw.get("reference_a_bps"), "reference_a_bps"),
            frozen_round_trip_fee_bps=_optional_number(
                raw.get("frozen_round_trip_fee_bps"), "frozen_round_trip_fee_bps"
            ),
            confirmation_current_spread_bps=_optional_number(
                raw.get("confirmation_current_spread_bps"),
                "confirmation_current_spread_bps",
            ),
            confirmation_deviation_bps=_optional_number(
                raw.get("confirmation_deviation_bps"), "confirmation_deviation_bps"
            ),
            confirmation_expected_net_at_a_bps=_optional_number(
                raw.get("confirmation_expected_net_at_a_bps"),
                "confirmation_expected_net_at_a_bps",
            ),
            highest_notified_level_bps=_optional_number(
                raw.get("highest_notified_level_bps"), "highest_notified_level_bps"
            ),
        )

    @staticmethod
    def _deserialize_key(raw: object) -> SpreadPairKey:
        if not isinstance(raw, dict):
            raise ValueError("invalid manual opportunity key")
        return SpreadPairKey(
            canonical_symbol=_required_text(raw.get("canonical_symbol"), "canonical_symbol"),
            long_venue=_required_text(raw.get("long_venue"), "long_venue"),
            long_venue_symbol=_required_text(raw.get("long_venue_symbol"), "long_venue_symbol"),
            short_venue=_required_text(raw.get("short_venue"), "short_venue"),
            short_venue_symbol=_required_text(raw.get("short_venue_symbol"), "short_venue_symbol"),
        )

    @classmethod
    def _deserialize_observation(
        cls, raw: dict[str, object]
    ) -> ManualOpportunityObservation:
        return ManualOpportunityObservation(
            key=cls._deserialize_key(raw.get("key")),
            sample_time=_parse_time(raw.get("sample_time"), "sample_time"),
            long_observed_at=_parse_time(raw.get("long_observed_at"), "long_observed_at"),
            short_observed_at=_parse_time(raw.get("short_observed_at"), "short_observed_at"),
            long_best_ask=_required_number(raw.get("long_best_ask"), "long_best_ask"),
            short_best_bid=_required_number(raw.get("short_best_bid"), "short_best_bid"),
            mean_2h_bps=_optional_number(raw.get("mean_2h_bps"), "mean_2h_bps"),
            mean_24h_bps=_optional_number(raw.get("mean_24h_bps"), "mean_24h_bps"),
            mean_3d_bps=_optional_number(raw.get("mean_3d_bps"), "mean_3d_bps"),
            long_volume_24h=_optional_number(raw.get("long_volume_24h"), "long_volume_24h"),
            short_volume_24h=_optional_number(raw.get("short_volume_24h"), "short_volume_24h"),
            long_fee_bps=_optional_number(raw.get("long_fee_bps"), "long_fee_bps"),
            short_fee_bps=_optional_number(raw.get("short_fee_bps"), "short_fee_bps"),
        )

    def _gap_exceeded(
        self,
        episode: ManualOpportunityEpisode,
        observation: ManualOpportunityObservation,
    ) -> bool:
        delta = (observation.available_at - episode.last_seen_at).total_seconds()
        return delta < 0 or delta > self.config.max_gap_seconds

    @staticmethod
    def _sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
        return (
            key.canonical_symbol,
            key.long_venue,
            key.long_venue_symbol,
            key.short_venue,
            key.short_venue_symbol,
        )


class ManualOpportunityMonitor:
    """Live BBO-only monitor backed by the manual opportunity lifecycle."""

    name = "manual_opportunity"

    def __init__(
        self,
        config: ManualOpportunityConfig,
        fees_bps: Mapping[str, float],
        *,
        runtime_store: SQLiteRuntimeStore | None = None,
        interval_seconds: int = 10,
        stale_after_seconds: int = DEFAULT_MANUAL_STALE_AFTER_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        self.interval_seconds = interval_seconds
        self.config = config
        self._fees_bps = dict(fees_bps)
        self._runtime_store = runtime_store
        self._stale_after_seconds = stale_after_seconds
        self._histories: dict[SpreadPairKey, Any] = {}
        self._notification_gate = ManualOpportunityNotificationGate(
            config,
            runtime_store=runtime_store,
        )
        self._lifecycle = ManualOpportunityLifecycle(
            config,
            fees_bps,
            runtime_store=runtime_store,
            stale_after_seconds=stale_after_seconds,
        )

    @property
    def active_episodes(self) -> tuple[ManualOpportunityEpisode, ...]:
        return self._lifecycle.active_episodes

    @property
    def notification_gate(self) -> ManualOpportunityNotificationGate:
        return self._notification_gate

    def hydrate_history(
        self,
        points_by_key: Mapping[SpreadPairKey, Sequence[tuple[datetime, float]]],
    ) -> None:
        """Replace live route histories with bounded strictly-prior points."""
        from radar.history.manual_opportunity import BboRollingHistory

        histories: dict[SpreadPairKey, Any] = {}
        for key, points in points_by_key.items():
            if isinstance(points, (str, bytes)):
                raise TypeError("history points must be a sequence of pairs")
            history = BboRollingHistory()
            history.hydrate(list(points))
            histories[key] = history
        self._histories = histories

    async def evaluate(
        self,
        now: datetime,
        state: RadarState,
    ) -> list[AlertRequest]:
        current_time = _as_utc(now, "now")
        cycle_started = time.perf_counter()
        route_preparation_ms = 0.0
        history_ms = 0.0
        lifecycle_ms = 0.0
        sqlite_persistence_ms = 0.0
        routes_evaluated = 0
        route_observations: list[ManualOpportunityObservation] = []
        current_keys: set[SpreadPairKey] = set()
        alerts: list[AlertRequest] = []
        history_mutations: list[tuple[Any, Any]] = []
        new_history_keys: set[SpreadPairKey] = set()
        markets_by_symbol: dict[
            str, dict[tuple[str, str], MarketSnapshot]
        ] = {}
        self._notification_gate.begin_cycle()
        self._lifecycle.begin_cycle()
        try:
            preparation_started = time.perf_counter()
            for snapshot in state.markets:
                markets_by_symbol.setdefault(snapshot.canonical_symbol, {})[
                    (snapshot.venue, snapshot.venue_symbol)
                ] = snapshot
            context_by_key = {
                (context.venue, context.venue_symbol, context.canonical_symbol): context
                for context in state.hourly_context
            }
            for canonical_symbol, symbol_markets in sorted(markets_by_symbol.items()):
                ordered_markets = sorted(symbol_markets.items())
                for (long_venue, long_symbol), long_snapshot in ordered_markets:
                    for (short_venue, short_symbol), short_snapshot in ordered_markets:
                        if long_venue.lower() == short_venue.lower():
                            continue
                        routes_evaluated += 1
                        if not self._usable_snapshot(long_snapshot) or not self._usable_snapshot(
                            short_snapshot
                        ):
                            continue
                        if long_snapshot.sample_time != short_snapshot.sample_time:
                            continue
                        long_context = context_by_key.get(
                            (long_venue, long_symbol, canonical_symbol)
                        )
                        short_context = context_by_key.get(
                            (short_venue, short_symbol, canonical_symbol)
                        )
                        if long_context is None or short_context is None:
                            continue
                        try:
                            observation = build_manual_observation(
                                long_snapshot,
                                short_snapshot,
                                mean_2h_bps=None,
                                mean_24h_bps=None,
                                mean_3d_bps=None,
                                long_volume_24h=long_context.volume_24h,
                                short_volume_24h=short_context.volume_24h,
                                fees_bps=self._fees_bps,
                            )
                        except (TypeError, ValueError):
                            continue
                        if (
                            manual_opportunity_temporal_rejection_reason(
                                observation,
                                current_time,
                                stale_after_seconds=self._stale_after_seconds,
                            )
                            is not None
                        ):
                            continue
                        current_keys.add(observation.key)
                        route_observations.append(observation)
            route_preparation_ms = _elapsed_ms(preparation_started)

            history_started = time.perf_counter()
            lifecycle_observations: list[ManualOpportunityObservation] = []
            for observation in route_observations:
                history = self._histories.get(observation.key)
                if history is None:
                    history = self._new_history()
                    self._histories[observation.key] = history
                    new_history_keys.add(observation.key)
                stats, mutation = history.observe_with_rollback(
                    observation.sample_time,
                    observation.current_spread_bps,
                )
                history_mutations.append((history, mutation))
                lifecycle_observations.append(
                    replace(
                        observation,
                        mean_2h_bps=stats["2h"].mean_bps,
                        mean_24h_bps=stats["24h"].mean_bps,
                        mean_3d_bps=stats["3d"].mean_bps,
                    )
                )
            history_ms = _elapsed_ms(history_started)

            lifecycle_started = time.perf_counter()
            for observation in lifecycle_observations:
                qualifies = (
                    manual_opportunity_rejection_reason(observation, self.config)
                    is None
                )
                self._notification_gate.observe(
                    observation.key,
                    qualifies=qualifies,
                    observed_at=observation.available_at,
                )
                lifecycle_alerts = self._lifecycle.evaluate(
                    observation,
                    now=current_time,
                    persist=False,
                )
                alerts.extend(
                    self._filter_notification_alerts(observation.key, lifecycle_alerts)
                )

            active_episodes = {
                episode.key: episode for episode in self._lifecycle.active_episodes
            }
            for key in self._lifecycle.active_keys:
                if key not in current_keys:
                    episode = active_episodes.get(key)
                    _log_transition(
                        "candidate_reset",
                        sample_time=max(
                            (snapshot.sample_time for snapshot in state.markets),
                            default=current_time,
                        ),
                        available_at=current_time,
                        key=key,
                        expected_net=(
                            None
                            if episode is None
                            else episode.last_observation.expected_net_at_a_bps
                        ),
                        reason="missing_observation",
                    )
                    self._lifecycle.observe_gap(key, current_time, persist=False)
            lifecycle_ms = _elapsed_ms(lifecycle_started)

            persistence_started = time.perf_counter()
            try:
                self._lifecycle.flush_cycle(
                    current_time,
                    additional_states=(
                        (
                            self._notification_gate.name,
                            self._notification_gate.state_key,
                            self._notification_gate.serialized_state(),
                            current_time,
                        ),
                    ),
                )
                self._notification_gate.commit_cycle(current_time, persist=False)
            finally:
                sqlite_persistence_ms = _elapsed_ms(persistence_started)
            return alerts
        except Exception:
            self._notification_gate.rollback_cycle()
            self._lifecycle.rollback_cycle()
            for history, mutation in reversed(history_mutations):
                mutation.rollback()
            for key in new_history_keys:
                self._histories.pop(key, None)
            raise
        finally:
            LOGGER.info(
                "manual opportunity cycle route_preparation_ms=%.3f "
                "history_ms=%.3f lifecycle_ms=%.3f "
                "sqlite_persistence_ms=%.3f total_ms=%.3f "
                "routes_evaluated=%d observations=%d active_episodes=%d "
                "alerts_emitted=%d",
                route_preparation_ms,
                history_ms,
                lifecycle_ms,
                sqlite_persistence_ms,
                (time.perf_counter() - cycle_started) * 1000.0,
                routes_evaluated,
                len(route_observations),
                len(self._lifecycle.active_keys),
                len(alerts),
            )

    def _filter_notification_alerts(
        self,
        key: SpreadPairKey,
        alerts: Sequence[AlertRequest],
    ) -> list[AlertRequest]:
        filtered: list[AlertRequest] = []
        for alert in alerts:
            event_kind = alert.payload.get("event_kind")
            episode_id = alert.payload.get("episode_id")
            if not isinstance(event_kind, str) or not isinstance(episode_id, str):
                filtered.append(alert)
                continue
            if event_kind == "manual_initial":
                if self._notification_gate.reserve_initial(key, episode_id):
                    filtered.append(alert)
                else:
                    LOGGER.info(
                        "manual_telegram_initial_suppressed symbol=%s "
                        "long_venue=%s short_venue=%s episode_id=%s "
                        "expected_net=%s reason=quiet_rearm_not_met",
                        key.canonical_symbol,
                        key.long_venue,
                        key.short_venue,
                        episode_id,
                        alert.payload.get("expected_net_at_a_bps"),
                    )
            elif event_kind == "manual_expansion":
                if self._notification_gate.allow_expansion(key, episode_id):
                    filtered.append(alert)
                else:
                    LOGGER.info(
                        "manual_telegram_expansion_suppressed symbol=%s "
                        "long_venue=%s short_venue=%s episode_id=%s",
                        key.canonical_symbol,
                        key.long_venue,
                        key.short_venue,
                        episode_id,
                    )
            else:
                filtered.append(alert)
        return filtered

    @staticmethod
    def _usable_snapshot(snapshot: MarketSnapshot) -> bool:
        return (
            math.isfinite(snapshot.best_bid)
            and math.isfinite(snapshot.best_ask)
            and math.isfinite(snapshot.best_bid_size)
            and math.isfinite(snapshot.best_ask_size)
            and snapshot.best_bid > 0
            and snapshot.best_ask > 0
            and snapshot.best_bid < snapshot.best_ask
            and snapshot.best_bid_size > 0
            and snapshot.best_ask_size > 0
        )

    @staticmethod
    def _new_history() -> Any:
        from radar.history.manual_opportunity import BboRollingHistory

        return BboRollingHistory()


def level_text(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be text")
    return value


def _parse_time(value: object, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO timestamp")
    return _as_utc(datetime.fromisoformat(value), field_name)


def _optional_time(value: object, field_name: str) -> datetime | None:
    return None if value is None else _parse_time(value, field_name)


def _required_number(value: object, field_name: str) -> float:
    if value is None:
        raise ValueError(f"{field_name} is required")
    return _finite(value, field_name)  # type: ignore[arg-type]


def _optional_number(value: object, field_name: str) -> float | None:
    return None if value is None else _finite(value, field_name)  # type: ignore[arg-type]
