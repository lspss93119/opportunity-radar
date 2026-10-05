from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import math

from radar.config import ManualOpportunityConfig
from radar.models import MarketSnapshot
from radar.monitors.base import AlertRequest, JSONValue
from radar.monitors.spread.models import SpreadPairKey
from radar.storage.sqlite import SQLiteRuntimeStore


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
    ) -> None:
        self.config = config
        self._fees_bps = dict(fees_bps)
        self._runtime_store = runtime_store
        self._episodes: dict[SpreadPairKey, ManualOpportunityEpisode] = {}
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

    def evaluate(self, observation: ManualOpportunityObservation) -> list[AlertRequest]:
        observation = self._with_configured_fees(observation)
        snapshot = deepcopy(self._episodes) if self._runtime_store is not None else None
        alerts: list[AlertRequest] = []
        events: list[tuple[str, str, object, datetime | None]] = []
        try:
            episode = self._episodes.get(observation.key)
            reason = manual_opportunity_rejection_reason(observation, self.config)

            if episode is not None and self._gap_exceeded(episode, observation):
                del self._episodes[observation.key]
                episode = None

            if reason is not None:
                if episode is not None:
                    del self._episodes[observation.key]
                self._persist(observation.sample_time, events)
                return alerts

            if episode is None:
                episode = self._new_episode(observation)
                self._episodes[observation.key] = episode
            else:
                episode.last_seen_at = observation.sample_time
                episode.last_observation = observation

            if episode.confirmed_at is None:
                elapsed = (
                    observation.sample_time - episode.candidate_started_at
                ).total_seconds()
                if elapsed >= self.config.confirmation_seconds:
                    self._confirm(episode, observation)
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
                post_confirmation_reason = self._post_confirmation_reason(
                    episode, observation
                )
                if post_confirmation_reason is not None:
                    del self._episodes[observation.key]
                    alerts.clear()
                else:
                    expansion_alert = self._maybe_expansion(episode, observation)
                    if expansion_alert is not None:
                        alerts.append(expansion_alert)
                        events.append(
                            (
                                expansion_alert.event_id,
                                "manual_expansion",
                                expansion_alert.payload,
                                expansion_alert.created_at,
                            )
                        )

            self._persist(observation.sample_time, events)
            return alerts
        except Exception:
            if snapshot is not None:
                self._episodes = snapshot
            raise

    def observe_gap(self, key: SpreadPairKey, sample_time: datetime) -> None:
        """End a route episode when a sample slot has no valid observation."""
        timestamp = _as_utc(sample_time, "sample_time")
        if key not in self._episodes:
            return
        snapshot = deepcopy(self._episodes) if self._runtime_store is not None else None
        try:
            del self._episodes[key]
            self._persist(timestamp, [])
        except Exception:
            if snapshot is not None:
                self._episodes = snapshot
            raise

    def _new_episode(
        self, observation: ManualOpportunityObservation
    ) -> ManualOpportunityEpisode:
        key = observation.key
        episode_id = (
            f"manual:{key.canonical_symbol}:{key.long_venue}:{key.long_venue_symbol}:"
            f"{key.short_venue}:{key.short_venue_symbol}:"
            f"{observation.sample_time.isoformat()}"
        )
        return ManualOpportunityEpisode(
            key=key,
            episode_id=episode_id,
            candidate_started_at=observation.sample_time,
            last_seen_at=observation.sample_time,
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
        if observation.round_trip_fee_bps is None:
            return "fees_unavailable"
        if observation.route_volume_24h is None or observation.route_volume_24h <= 0:
            return "volume_unavailable"
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
            (observation.sample_time - episode.candidate_started_at).total_seconds()
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
            created_at=observation.sample_time,
            payload=payload,
        )

    def _persist(
        self,
        updated_at: datetime,
        events: list[tuple[str, str, object, datetime | None]],
    ) -> None:
        if self._runtime_store is None:
            return
        self._runtime_store.set_monitor_state_and_append_opportunities(
            self.name,
            self.state_key,
            self._serialize_state(),
            updated_at=updated_at,
            opportunities=events,
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
        delta = (observation.sample_time - episode.last_seen_at).total_seconds()
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
