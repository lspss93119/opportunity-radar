from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import logging
import math
import time

from radar.config import SpreadMonitorConfig
from radar.monitors.base import AlertRequest, JSONValue
from radar.models import FundingSnapshot
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore

from radar.monitors.spread.basis import (
    EXPECTED_INTERVAL_SECONDS,
    MIN_HISTORY_OBSERVATIONS,
    ROLLING_WINDOW_SECONDS,
    RollingBasis,
    RollingBasisMutation,
    RollingBasisStats,
)
from radar.monitors.spread.anomaly_v2 import AnomalyV2Lifecycle
from radar.monitors.spread.models import (
    SpreadCandidate,
    SpreadPairKey,
    build_spread_candidates,
)

UTC = timezone.utc
OpportunityEvent = tuple[str, str, object, datetime]

BASIS_STD_MAX_BPS = 3.0
BASIS_DEVIATION_MIN_BPS = 15.0
BASIS_PERSISTENCE_SECONDS = 60
LOGGER = logging.getLogger(__name__)


def _elapsed_ms(started: float) -> float:
    return (time.perf_counter() - started) * 1000.0


def _pair_sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
    return (
        key.canonical_symbol,
        key.long_venue,
        key.long_venue_symbol,
        key.short_venue,
        key.short_venue_symbol,
    )


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass
class SpreadEpisode:
    key: SpreadPairKey
    episode_id: str
    first_seen_at: datetime
    last_seen_at: datetime
    candidate: SpreadCandidate
    candidate_confirmed: bool = False
    candidate_confirmed_at: datetime | None = None
    alert_condition_since: datetime | None = None
    alerted: bool = False
    rolling_mean_bps: float | None = None
    rolling_std_bps: float | None = None

    @property
    def last_raw_spread_bps(self) -> float:
        return self.candidate.raw_spread_bps

    @property
    def last_net_spread_bps(self) -> float | None:
        return self.candidate.net_spread_bps


class SpreadMonitor:
    name = "spread"

    def __init__(
        self,
        config: SpreadMonitorConfig,
        fees_bps: Mapping[str, float],
        *,
        runtime_store: SQLiteRuntimeStore | None = None,
        basis_window_seconds: int = ROLLING_WINDOW_SECONDS,
        basis_min_observations: int = MIN_HISTORY_OBSERVATIONS,
        basis_expected_interval_seconds: int = EXPECTED_INTERVAL_SECONDS,
    ) -> None:
        self.interval_seconds = config.interval_seconds
        self.config = config
        self._fees_bps = dict(fees_bps)
        self._runtime_store = runtime_store
        self._basis_window_seconds = basis_window_seconds
        self._basis_min_observations = basis_min_observations
        self._basis_expected_interval_seconds = basis_expected_interval_seconds
        self._episodes: dict[SpreadPairKey, SpreadEpisode] = {}
        self._basis_by_key: dict[SpreadPairKey, RollingBasis] = {}
        self._anomaly_v2 = (
            AnomalyV2Lifecycle(config.anomaly_v2, primary_size_usd=config.primary_size_usd)
            if config.anomaly_v2.enabled
            else None
        )
        if runtime_store is not None:
            self._episodes = self._load_episodes(
                runtime_store.get_monitor_state(self.name, "episodes")
            )
            if self._anomaly_v2 is not None:
                self._anomaly_v2.restore(
                    runtime_store.get_monitor_state(self.name, "anomaly_episodes_v2")
                )

    @property
    def active_episodes(self) -> tuple[SpreadEpisode, ...]:
        return tuple(
            self._episodes[key]
            for key in sorted(self._episodes, key=_pair_sort_key)
        )

    @property
    def active_anomaly_episodes(self) -> tuple[object, ...]:
        """Read-only view of active v2 wrappers for diagnostics/tests."""
        if self._anomaly_v2 is None:
            return ()
        return self._anomaly_v2.active_states

    def hydrate_history(
        self,
        points_by_key: Mapping[SpreadPairKey, object],
    ) -> None:
        """Load bounded prior observations once before live sampling starts."""
        for key, raw_points in points_by_key.items():
            if not isinstance(raw_points, (tuple, list)):
                raise TypeError("history points must be a sequence")
            points: list[tuple[datetime, float]] = []
            for point in raw_points:
                if not isinstance(point, (tuple, list)) or len(point) != 2:
                    raise TypeError("history points must contain (timestamp, value)")
                timestamp, value = point
                if not isinstance(timestamp, datetime):
                    raise TypeError("history sample_time must be a datetime")
                points.append((timestamp, float(value)))
            basis = self._new_basis()
            basis.hydrate(points)
            self._basis_by_key[key] = basis

    def _new_basis(self) -> RollingBasis:
        return RollingBasis(
            window_seconds=self._basis_window_seconds,
            min_observations=self._basis_min_observations,
            expected_interval_seconds=self._basis_expected_interval_seconds,
        )

    def _rollback_basis_mutations(
        self,
        mutations: list[RollingBasisMutation],
        created_keys: set[SpreadPairKey],
    ) -> None:
        for mutation in reversed(mutations):
            mutation.rollback()
        for key in created_keys:
            self._basis_by_key.pop(key, None)

    @staticmethod
    def _log_cycle_timing(
        *,
        candidate_build_ms: float,
        basis_ms: float,
        lifecycle_ms: float,
        sqlite_persistence_ms: float,
        total_ms: float,
        route_count: int,
        alert_count: int,
    ) -> None:
        LOGGER.info(
            "spread cycle candidate_build_ms=%.3f basis_ms=%.3f "
            "lifecycle_ms=%.3f sqlite_persistence_ms=%.3f total_ms=%.3f "
            "routes=%d alerts=%d",
            candidate_build_ms,
            basis_ms,
            lifecycle_ms,
            sqlite_persistence_ms,
            total_ms,
            route_count,
            alert_count,
        )

    async def evaluate(
        self,
        now: datetime,
        state: RadarState,
    ) -> list[AlertRequest]:
        current_time = _as_utc(now, "now")
        if self._anomaly_v2 is not None:
            return await self._evaluate_anomaly_v2(current_time, state)
        cycle_started = time.perf_counter()
        candidate_build_started = time.perf_counter()
        candidates = build_spread_candidates(
            state.markets,
            current_time,
            primary_size_usd=self.config.primary_size_usd,
            stale_after_seconds=self.config.stale_after_seconds,
            fees_bps=self._fees_bps,
            require_fees=False,
        )
        candidate_build_ms = _elapsed_ms(candidate_build_started)
        previous_episodes = (
            deepcopy(self._episodes) if self._runtime_store is not None else None
        )

        events: list[OpportunityEvent] = []
        alerts: list[AlertRequest] = []
        current_candidates = {candidate.key: candidate for candidate in candidates}
        basis_stats: dict[SpreadPairKey, RollingBasisStats] = {}
        basis_mutations: list[RollingBasisMutation] = []
        created_basis_keys: set[SpreadPairKey] = set()
        basis_started = time.perf_counter()
        try:
            for candidate in candidates:
                basis = self._basis_by_key.get(candidate.key)
                if basis is None:
                    basis = self._new_basis()
                    self._basis_by_key[candidate.key] = basis
                    created_basis_keys.add(candidate.key)
                stats, mutation = basis.observe_with_rollback(
                    candidate.sample_time,
                    candidate.raw_spread_bps,
                )
                basis_mutations.append(mutation)
                basis_stats[candidate.key] = stats
            basis_ms = _elapsed_ms(basis_started)

            lifecycle_started = time.perf_counter()
            for key in tuple(self._episodes):
                current_candidate = current_candidates.get(key)
                episode_stats = basis_stats.get(key)
                if current_candidate is None:
                    if not self._episodes[key].alerted:
                        self._resolve_episode(
                            key,
                            current_time,
                            reason="missing_observation",
                            events=events,
                        )
                    continue
                if episode_stats is None or not episode_stats.eligible:
                    if not self._episodes[key].alerted:
                        self._resolve_episode(
                            key,
                            current_time,
                            reason="insufficient_history",
                            events=events,
                        )
                    else:
                        self._update_episode(
                            self._episodes[key], current_candidate, current_time, episode_stats
                        )
                    continue
                if episode_stats.mean_bps is None or episode_stats.std_bps is None:
                    continue
                deviation = current_candidate.raw_spread_bps - episode_stats.mean_bps
                if deviation < BASIS_DEVIATION_MIN_BPS:
                    self._resolve_episode(
                        key,
                        current_time,
                        reason="rearmed_below_deviation",
                        events=events,
                    )

            for key in sorted(basis_stats, key=_pair_sort_key):
                candidate = current_candidates[key]
                stats = basis_stats[key]
                if not stats.eligible or stats.mean_bps is None or stats.std_bps is None:
                    continue
                deviation = candidate.raw_spread_bps - stats.mean_bps
                stable_condition = (
                    stats.std_bps <= BASIS_STD_MAX_BPS
                    and deviation >= BASIS_DEVIATION_MIN_BPS
                )
                episode = self._episodes.get(key)
                if episode is None:
                    if not stable_condition:
                        continue
                    episode = self._start_episode(
                        candidate,
                        current_time,
                        stats=stats,
                    )
                    self._episodes[key] = episode
                elif not self._is_continuous(episode, current_time):
                    if episode.alerted:
                        self._update_episode(episode, candidate, current_time, stats)
                        continue
                    self._resolve_episode(key, current_time, reason="continuity_gap", events=events)
                    if not stable_condition:
                        continue
                    episode = self._start_episode(
                        candidate,
                        current_time,
                        stats=stats,
                    )
                    self._episodes[key] = episode
                else:
                    self._update_episode(episode, candidate, current_time, stats)

                if (
                    not episode.candidate_confirmed
                    and current_time - episode.first_seen_at
                    >= timedelta(seconds=self.config.candidate_duration_seconds)
                ):
                    episode.candidate_confirmed = True
                    episode.candidate_confirmed_at = current_time
                    self._log_event(
                        episode,
                        "candidate_confirmed",
                        current_time,
                        events=events,
                    )

                if stable_condition:
                    if episode.alert_condition_since is None:
                        episode.alert_condition_since = current_time
                elif not episode.alerted:
                    episode.alert_condition_since = None

                if self._is_alert_eligible(episode, current_time):
                    episode.alerted = True
                    alert = self._build_alert_request(episode, current_time, state)
                    self._log_event(
                        episode,
                        "alert",
                        current_time,
                        event=alert.payload,
                        events=events,
                    )
                    alerts.append(alert)

            lifecycle_ms = _elapsed_ms(lifecycle_started)
            persistence_started = time.perf_counter()
            self._persist_episodes(current_time, events)
            sqlite_persistence_ms = _elapsed_ms(persistence_started)
            self._log_cycle_timing(
                candidate_build_ms=candidate_build_ms,
                basis_ms=basis_ms,
                lifecycle_ms=lifecycle_ms,
                sqlite_persistence_ms=sqlite_persistence_ms,
                total_ms=_elapsed_ms(cycle_started),
                route_count=len(candidates),
                alert_count=len(alerts),
            )
        except Exception:
            if self._runtime_store is not None:
                self._rollback_basis_mutations(basis_mutations, created_basis_keys)
                if previous_episodes is not None:
                    self._episodes = previous_episodes
            raise
        return alerts

    async def _evaluate_anomaly_v2(
        self,
        current_time: datetime,
        state: RadarState,
    ) -> list[AlertRequest]:
        cycle_started = time.perf_counter()
        candidate_build_started = time.perf_counter()
        candidates = build_spread_candidates(
            state.markets,
            current_time,
            primary_size_usd=self.config.primary_size_usd,
            stale_after_seconds=self.config.stale_after_seconds,
            fees_bps=self._fees_bps,
            require_fees=False,
        )
        candidate_build_ms = _elapsed_ms(candidate_build_started)
        candidate_by_key = {candidate.key: candidate for candidate in candidates}
        previous_v2 = (
            deepcopy(self._anomaly_v2) if self._runtime_store is not None else None
        )
        basis_stats: dict[SpreadPairKey, RollingBasisStats] = {}
        basis_mutations: list[RollingBasisMutation] = []
        created_basis_keys: set[SpreadPairKey] = set()
        basis_started = time.perf_counter()
        try:
            for candidate in candidates:
                basis = self._basis_by_key.get(candidate.key)
                if basis is None:
                    basis = self._new_basis()
                    self._basis_by_key[candidate.key] = basis
                    created_basis_keys.add(candidate.key)
                stats, mutation = basis.observe_with_rollback(
                    candidate.sample_time,
                    candidate.raw_spread_bps,
                )
                basis_mutations.append(mutation)
                basis_stats[candidate.key] = stats
            basis_ms = _elapsed_ms(basis_started)

            assert self._anomaly_v2 is not None
            lifecycle_started = time.perf_counter()
            result = self._anomaly_v2.evaluate(
                candidates=candidate_by_key,
                basis_stats=basis_stats,
                state=state,
                now=current_time,
            )
            lifecycle_ms = _elapsed_ms(lifecycle_started)
            persistence_started = time.perf_counter()
            self._persist_anomaly_v2(current_time, result.events)
            sqlite_persistence_ms = _elapsed_ms(persistence_started)
            alert_count = len(result.alerts) if self.config.anomaly_v2.telegram_enabled else 0
            self._log_cycle_timing(
                candidate_build_ms=candidate_build_ms,
                basis_ms=basis_ms,
                lifecycle_ms=lifecycle_ms,
                sqlite_persistence_ms=sqlite_persistence_ms,
                total_ms=_elapsed_ms(cycle_started),
                route_count=len(candidates),
                alert_count=alert_count,
            )
        except Exception:
            if self._runtime_store is not None:
                self._rollback_basis_mutations(basis_mutations, created_basis_keys)
                if previous_v2 is not None:
                    self._anomaly_v2 = previous_v2
            raise
        if not self.config.anomaly_v2.telegram_enabled:
            return []
        return list(result.alerts)

    def _start_episode(
        self,
        candidate: SpreadCandidate,
        now: datetime,
        *,
        stats: RollingBasisStats,
    ) -> SpreadEpisode:
        episode = SpreadEpisode(
            key=candidate.key,
            episode_id=self._episode_id(candidate.key, now),
            first_seen_at=now,
            last_seen_at=now,
            candidate=candidate,
            rolling_mean_bps=stats.mean_bps,
            rolling_std_bps=stats.std_bps,
        )
        episode.alert_condition_since = now
        return episode

    @staticmethod
    def _update_episode(
        episode: SpreadEpisode,
        candidate: SpreadCandidate,
        now: datetime,
        stats: RollingBasisStats | None,
    ) -> None:
        episode.last_seen_at = now
        episode.candidate = candidate
        if stats is not None:
            episode.rolling_mean_bps = stats.mean_bps
            episode.rolling_std_bps = stats.std_bps

    def _is_continuous(self, episode: SpreadEpisode, now: datetime) -> bool:
        gap_seconds = (now - episode.last_seen_at).total_seconds()
        return 0 <= gap_seconds <= self.interval_seconds * 2

    def _is_alert_eligible(self, episode: SpreadEpisode, now: datetime) -> bool:
        if episode.alerted:
            return False
        if episode.alert_condition_since is None:
            return False
        return (
            now - episode.alert_condition_since
            >= timedelta(seconds=BASIS_PERSISTENCE_SECONDS)
        )

    @staticmethod
    def _episode_id(key: SpreadPairKey, first_seen_at: datetime) -> str:
        return ":".join(
            (
                key.canonical_symbol,
                key.long_venue,
                key.long_venue_symbol,
                key.short_venue,
                key.short_venue_symbol,
                first_seen_at.isoformat(),
            )
        )

    def _build_alert_request(
        self,
        episode: SpreadEpisode,
        now: datetime,
        state: RadarState,
    ) -> AlertRequest:
        candidate = episode.candidate
        funding_context: dict[str, JSONValue] = {
            "long": self._funding_payload(
                self._find_funding(
                    state,
                    candidate.key.canonical_symbol,
                    candidate.key.long_venue,
                    candidate.key.long_venue_symbol,
                )
            ),
            "short": self._funding_payload(
                self._find_funding(
                    state,
                    candidate.key.canonical_symbol,
                    candidate.key.short_venue,
                    candidate.key.short_venue_symbol,
                )
            ),
        }
        payload: dict[str, JSONValue] = {
            "canonical_symbol": candidate.key.canonical_symbol,
            "long_venue": candidate.key.long_venue,
            "long_venue_symbol": candidate.key.long_venue_symbol,
            "short_venue": candidate.key.short_venue,
            "short_venue_symbol": candidate.key.short_venue_symbol,
            "primary_size_usd": self.config.primary_size_usd,
            "long_buy_vwap": candidate.long_buy_vwap,
            "short_sell_vwap": candidate.short_sell_vwap,
            "raw_spread_bps": candidate.raw_spread_bps,
            "long_fee_bps": candidate.long_fee_bps,
            "short_fee_bps": candidate.short_fee_bps,
            "net_spread_bps": candidate.net_spread_bps,
            "rolling_mean_bps": episode.rolling_mean_bps,
            "rolling_std_bps": episode.rolling_std_bps,
            "deviation_bps": (
                candidate.raw_spread_bps - episode.rolling_mean_bps
                if episode.rolling_mean_bps is not None
                else None
            ),
            "signal_duration_seconds": int(
                (now - episode.alert_condition_since).total_seconds()
            )
            if episode.alert_condition_since is not None
            else 0,
            "observed_at_skew_seconds": candidate.observed_at_skew_seconds,
            "round_trip_fee_bps": (
                2.0 * (candidate.long_fee_bps + candidate.short_fee_bps)
                if candidate.long_fee_bps is not None
                and candidate.short_fee_bps is not None
                else None
            ),
            "theoretical_edge_bps": (
                candidate.raw_spread_bps - episode.rolling_mean_bps
                - 2.0 * (candidate.long_fee_bps + candidate.short_fee_bps)
                if episode.rolling_mean_bps is not None
                and candidate.long_fee_bps is not None
                and candidate.short_fee_bps is not None
                else None
            ),
            "sample_time": candidate.sample_time.isoformat(),
            "episode_started_at": episode.first_seen_at.isoformat(),
            "candidate_confirmed_at": episode.candidate_confirmed_at.isoformat()
            if episode.candidate_confirmed_at is not None
            else None,
            "alert_condition_started_at": episode.alert_condition_since.isoformat()
            if episode.alert_condition_since is not None
            else None,
            "candidate_duration_seconds": self.config.candidate_duration_seconds,
            "alert_duration_seconds": self.config.alert_duration_seconds,
            "funding_context": funding_context,
        }
        return AlertRequest(
            monitor=self.name,
            event_id=f"{episode.episode_id}:alert",
            created_at=now,
            payload=payload,
        )

    def _resolve_episode(
        self,
        key: SpreadPairKey,
        now: datetime,
        *,
        reason: str,
        events: list[OpportunityEvent],
    ) -> None:
        episode = self._episodes.pop(key)
        if episode.candidate_confirmed or episode.alerted:
            self._log_event(
                episode,
                "resolved",
                now,
                event={
                    "episode_id": episode.episode_id,
                    "reason": reason,
                    "net_spread_bps": episode.last_net_spread_bps,
                    "rolling_mean_bps": episode.rolling_mean_bps,
                    "rolling_std_bps": episode.rolling_std_bps,
                },
                events=events,
            )

    def _log_event(
        self,
        episode: SpreadEpisode,
        event_type: str,
        occurred_at: datetime,
        *,
        event: Mapping[str, JSONValue] | None = None,
        events: list[OpportunityEvent],
    ) -> None:
        if self._runtime_store is None:
            return
        if event is None:
            candidate = episode.candidate
            event = {
                "episode_id": episode.episode_id,
                "canonical_symbol": candidate.key.canonical_symbol,
                "long_venue": candidate.key.long_venue,
                "long_venue_symbol": candidate.key.long_venue_symbol,
                "short_venue": candidate.key.short_venue,
                "short_venue_symbol": candidate.key.short_venue_symbol,
                "sample_time": candidate.sample_time.isoformat(),
                "raw_spread_bps": candidate.raw_spread_bps,
                "net_spread_bps": candidate.net_spread_bps,
            }
        events.append(
            (
                f"{episode.episode_id}:{event_type}",
                event_type,
                dict(event),
                occurred_at,
            )
        )

    def _persist_episodes(
        self,
        now: datetime,
        events: list[OpportunityEvent],
    ) -> None:
        if self._runtime_store is None:
            return
        self._runtime_store.set_monitor_state_and_append_opportunities(
            self.name,
            "episodes",
            {
                episode.episode_id: self._serialize_episode(episode)
                for episode in self._episodes.values()
            },
            updated_at=now,
            opportunities=events,
        )

    def _persist_anomaly_v2(
        self,
        now: datetime,
        events: tuple[OpportunityEvent, ...],
    ) -> None:
        if self._runtime_store is None or self._anomaly_v2 is None:
            return
        self._runtime_store.set_monitor_state_and_append_opportunities(
            self.name,
            "anomaly_episodes_v2",
            self._anomaly_v2.snapshot(),
            updated_at=now,
            opportunities=events,
        )

    @staticmethod
    def _serialize_episode(episode: SpreadEpisode) -> dict[str, JSONValue]:
        candidate = episode.candidate
        return {
            "episode_id": episode.episode_id,
            "key": {
                "canonical_symbol": candidate.key.canonical_symbol,
                "long_venue": candidate.key.long_venue,
                "long_venue_symbol": candidate.key.long_venue_symbol,
                "short_venue": candidate.key.short_venue,
                "short_venue_symbol": candidate.key.short_venue_symbol,
            },
            "first_seen_at": episode.first_seen_at.isoformat(),
            "last_seen_at": episode.last_seen_at.isoformat(),
            "candidate": {
                "sample_time": candidate.sample_time.isoformat(),
                "long_buy_vwap": candidate.long_buy_vwap,
                "short_sell_vwap": candidate.short_sell_vwap,
                "long_fee_bps": candidate.long_fee_bps,
                "short_fee_bps": candidate.short_fee_bps,
                "raw_spread_bps": candidate.raw_spread_bps,
                "net_spread_bps": candidate.net_spread_bps,
                "observed_at_skew_seconds": candidate.observed_at_skew_seconds,
            },
            "candidate_confirmed": episode.candidate_confirmed,
            "candidate_confirmed_at": (
                episode.candidate_confirmed_at.isoformat()
                if episode.candidate_confirmed_at is not None
                else None
            ),
            "alert_condition_since": (
                episode.alert_condition_since.isoformat()
                if episode.alert_condition_since is not None
                else None
            ),
            "alerted": episode.alerted,
            "rolling_mean_bps": episode.rolling_mean_bps,
            "rolling_std_bps": episode.rolling_std_bps,
        }

    @classmethod
    def _load_episodes(cls, raw: object) -> dict[SpreadPairKey, SpreadEpisode]:
        if not isinstance(raw, dict):
            return {}
        episodes: dict[SpreadPairKey, SpreadEpisode] = {}
        for raw_episode in raw.values():
            try:
                episode = cls._deserialize_episode(raw_episode)
            except (KeyError, TypeError, ValueError):
                continue
            episodes[episode.key] = episode
        return episodes

    @classmethod
    def _deserialize_episode(cls, raw: object) -> SpreadEpisode:
        if not isinstance(raw, dict):
            raise TypeError("episode must be an object")
        key_raw = raw["key"]
        candidate_raw = raw["candidate"]
        if not isinstance(key_raw, dict) or not isinstance(candidate_raw, dict):
            raise TypeError("episode key and candidate must be objects")
        key = SpreadPairKey(
            canonical_symbol=cls._text(key_raw["canonical_symbol"]),
            long_venue=cls._text(key_raw["long_venue"]),
            long_venue_symbol=cls._text(key_raw["long_venue_symbol"]),
            short_venue=cls._text(key_raw["short_venue"]),
            short_venue_symbol=cls._text(key_raw["short_venue_symbol"]),
        )
        candidate = SpreadCandidate(
            key=key,
            sample_time=cls._timestamp(candidate_raw["sample_time"]),
            long_buy_vwap=cls._positive(candidate_raw["long_buy_vwap"]),
            short_sell_vwap=cls._positive(candidate_raw["short_sell_vwap"]),
            long_fee_bps=cls._optional_non_negative(candidate_raw.get("long_fee_bps")),
            short_fee_bps=cls._optional_non_negative(candidate_raw.get("short_fee_bps")),
            raw_spread_bps=cls._finite(candidate_raw["raw_spread_bps"]),
            net_spread_bps=cls._optional_finite(candidate_raw.get("net_spread_bps")),
            observed_at_skew_seconds=cls._non_negative(
                candidate_raw.get("observed_at_skew_seconds", 0.0)
            ),
        )
        episode_id = cls._text(raw["episode_id"])
        first_seen_at = cls._timestamp(raw["first_seen_at"])
        last_seen_at = cls._timestamp(raw["last_seen_at"])
        if last_seen_at < first_seen_at:
            raise ValueError("episode timestamps are not ordered")
        candidate_confirmed = raw["candidate_confirmed"]
        alerted = raw["alerted"]
        if not isinstance(candidate_confirmed, bool) or not isinstance(alerted, bool):
            raise TypeError("episode flags must be boolean")
        candidate_confirmed_at = cls._optional_timestamp(raw["candidate_confirmed_at"])
        alert_condition_since = cls._optional_timestamp(raw["alert_condition_since"])
        if candidate_confirmed and candidate_confirmed_at is None:
            raise ValueError("confirmed episode must have a confirmation time")
        if not alerted:
            # Old runtime state used the legacy absolute alert timer.  It is
            # not valid evidence for a new stable-basis episode.
            alert_condition_since = None
        return SpreadEpisode(
            key=key,
            episode_id=episode_id,
            first_seen_at=first_seen_at,
            last_seen_at=last_seen_at,
            candidate=candidate,
            candidate_confirmed=candidate_confirmed,
            candidate_confirmed_at=candidate_confirmed_at,
            alert_condition_since=alert_condition_since,
            alerted=alerted,
            rolling_mean_bps=cls._optional_finite(raw.get("rolling_mean_bps")),
            rolling_std_bps=cls._optional_finite(raw.get("rolling_std_bps")),
        )

    @staticmethod
    def _text(value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("episode text must be non-empty")
        return value

    @staticmethod
    def _timestamp(value: object) -> datetime:
        if not isinstance(value, str):
            raise TypeError("episode timestamp must be a string")
        return _as_utc(datetime.fromisoformat(value), "episode timestamp")

    @classmethod
    def _optional_timestamp(cls, value: object) -> datetime | None:
        return None if value is None else cls._timestamp(value)

    @staticmethod
    def _finite(value: object) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise TypeError("episode number must be numeric")
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("episode number must be finite")
        return result

    @classmethod
    def _positive(cls, value: object) -> float:
        result = cls._finite(value)
        if result <= 0:
            raise ValueError("episode number must be positive")
        return result

    @classmethod
    def _non_negative(cls, value: object) -> float:
        result = cls._finite(value)
        if result < 0:
            raise ValueError("episode number must be non-negative")
        return result

    @classmethod
    def _optional_finite(cls, value: object) -> float | None:
        return None if value is None else cls._finite(value)

    @classmethod
    def _optional_non_negative(cls, value: object) -> float | None:
        return None if value is None else cls._non_negative(value)

    @staticmethod
    def _find_funding(
        state: RadarState,
        canonical_symbol: str,
        venue: str,
        venue_symbol: str,
    ) -> FundingSnapshot | None:
        for snapshot in state.funding:
            if (
                snapshot.canonical_symbol == canonical_symbol
                and snapshot.venue == venue
                and snapshot.venue_symbol == venue_symbol
            ):
                return snapshot
        return None

    @staticmethod
    def _funding_payload(snapshot: FundingSnapshot | None) -> dict[str, JSONValue] | None:
        if snapshot is None:
            return None
        return {
            "effective_time": snapshot.effective_time.isoformat(),
            "observed_at": snapshot.observed_at.isoformat(),
            "venue": snapshot.venue,
            "venue_symbol": snapshot.venue_symbol,
            "canonical_symbol": snapshot.canonical_symbol,
            "funding_rate": snapshot.funding_rate,
            "next_funding_time": snapshot.next_funding_time.isoformat()
            if snapshot.next_funding_time is not None
            else None,
        }
