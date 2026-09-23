from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math

from radar.config import SpreadMonitorConfig
from radar.monitors.base import AlertRequest, JSONValue
from radar.models import FundingSnapshot
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore

from radar.monitors.spread.models import (
    SpreadCandidate,
    SpreadPairKey,
    build_spread_candidates,
)

UTC = timezone.utc
OpportunityEvent = tuple[str, str, object, datetime]


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

    @property
    def last_raw_spread_bps(self) -> float:
        return self.candidate.raw_spread_bps

    @property
    def last_net_spread_bps(self) -> float:
        return self.candidate.net_spread_bps


class SpreadMonitor:
    name = "spread"

    def __init__(
        self,
        config: SpreadMonitorConfig,
        fees_bps: Mapping[str, float],
        *,
        runtime_store: SQLiteRuntimeStore | None = None,
    ) -> None:
        self.interval_seconds = config.interval_seconds
        self.config = config
        self._fees_bps = dict(fees_bps)
        self._runtime_store = runtime_store
        self._episodes: dict[SpreadPairKey, SpreadEpisode] = {}
        if runtime_store is not None:
            self._episodes = self._load_episodes(
                runtime_store.get_monitor_state(self.name, "episodes")
            )

    @property
    def active_episodes(self) -> tuple[SpreadEpisode, ...]:
        return tuple(
            self._episodes[key]
            for key in sorted(self._episodes, key=_pair_sort_key)
        )

    async def evaluate(
        self,
        now: datetime,
        state: RadarState,
    ) -> list[AlertRequest]:
        current_time = _as_utc(now, "now")
        candidates = build_spread_candidates(
            state.markets,
            current_time,
            primary_size_usd=self.config.primary_size_usd,
            stale_after_seconds=self.config.stale_after_seconds,
            fees_bps=self._fees_bps,
        )
        qualifying = {
            candidate.key: candidate
            for candidate in candidates
            if candidate.net_spread_bps >= self.config.candidate_net_bps
        }
        previous_episodes = (
            deepcopy(self._episodes) if self._runtime_store is not None else None
        )

        events: list[OpportunityEvent] = []
        for key in tuple(self._episodes):
            if key not in qualifying:
                self._resolve_episode(
                    key,
                    current_time,
                    reason="not_qualifying",
                    events=events,
                )

        alerts: list[AlertRequest] = []
        for key in sorted(qualifying, key=_pair_sort_key):
            candidate = qualifying[key]
            episode = self._episodes.get(key)
            if episode is None:
                episode = self._start_episode(candidate, current_time)
                self._episodes[key] = episode
            elif not self._is_continuous(episode, current_time):
                self._resolve_episode(
                    key,
                    current_time,
                    reason="continuity_gap",
                    events=events,
                )
                episode = self._start_episode(candidate, current_time)
                self._episodes[key] = episode
            else:
                episode.last_seen_at = current_time
                episode.candidate = candidate

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

            if candidate.net_spread_bps >= self.config.alert_net_bps:
                if episode.alert_condition_since is None:
                    episode.alert_condition_since = current_time
            else:
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

        try:
            self._persist_episodes(current_time, events)
        except Exception:
            if previous_episodes is not None:
                self._episodes = previous_episodes
            raise
        return alerts

    def _start_episode(
        self,
        candidate: SpreadCandidate,
        now: datetime,
    ) -> SpreadEpisode:
        episode = SpreadEpisode(
            key=candidate.key,
            episode_id=self._episode_id(candidate.key, now),
            first_seen_at=now,
            last_seen_at=now,
            candidate=candidate,
        )
        if candidate.net_spread_bps >= self.config.alert_net_bps:
            episode.alert_condition_since = now
        return episode

    def _is_continuous(self, episode: SpreadEpisode, now: datetime) -> bool:
        gap_seconds = (now - episode.last_seen_at).total_seconds()
        return 0 <= gap_seconds <= self.interval_seconds * 2

    def _is_alert_eligible(self, episode: SpreadEpisode, now: datetime) -> bool:
        if episode.alerted or not episode.candidate_confirmed:
            return False
        if episode.alert_condition_since is None:
            return False
        return (
            now - episode.alert_condition_since
            >= timedelta(seconds=self.config.alert_duration_seconds)
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
        if episode.candidate_confirmed:
            self._log_event(
                episode,
                "resolved",
                now,
                event={
                    "episode_id": episode.episode_id,
                    "reason": reason,
                    "net_spread_bps": episode.last_net_spread_bps,
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
            long_fee_bps=cls._non_negative(candidate_raw["long_fee_bps"]),
            short_fee_bps=cls._non_negative(candidate_raw["short_fee_bps"]),
            raw_spread_bps=cls._finite(candidate_raw["raw_spread_bps"]),
            net_spread_bps=cls._finite(candidate_raw["net_spread_bps"]),
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
        if alerted and not candidate_confirmed:
            raise ValueError("alerted episode must be confirmed")
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
