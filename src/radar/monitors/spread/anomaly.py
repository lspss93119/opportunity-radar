from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import math
from typing import Any, Literal

from radar.monitors.spread.models import SpreadPairKey


ResolutionReason = Literal["returned_to_mean_band", "data_gap", "open_at_end"]


def _as_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _finite(value: float, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


@dataclass(frozen=True)
class AnomalyParameters:
    anomaly_deviation_bps: float = 10.0
    confirmation_seconds: int = 60
    return_band_bps: float = 5.0
    max_gap_seconds: int = 20

    def __post_init__(self) -> None:
        deviation = _finite(self.anomaly_deviation_bps, "anomaly_deviation_bps")
        return_band = _finite(self.return_band_bps, "return_band_bps")
        if deviation < 0:
            raise ValueError("anomaly_deviation_bps must be non-negative")
        if return_band < 0:
            raise ValueError("return_band_bps must be non-negative")
        if self.confirmation_seconds < 0:
            raise ValueError("confirmation_seconds must be non-negative")
        if self.max_gap_seconds <= 0:
            raise ValueError("max_gap_seconds must be positive")


@dataclass(frozen=True)
class AnomalyObservation:
    sample_time: datetime
    raw_spread_bps: float
    rolling_mean_bps: float | None
    rolling_std_bps: float | None
    basis_eligible: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "sample_time", _as_utc(self.sample_time, "sample_time"))
        object.__setattr__(
            self,
            "raw_spread_bps",
            _finite(self.raw_spread_bps, "raw_spread_bps"),
        )
        if self.rolling_mean_bps is not None:
            object.__setattr__(
                self,
                "rolling_mean_bps",
                _finite(self.rolling_mean_bps, "rolling_mean_bps"),
            )
        if self.rolling_std_bps is not None:
            object.__setattr__(
                self,
                "rolling_std_bps",
                _finite(self.rolling_std_bps, "rolling_std_bps"),
            )


@dataclass
class AnomalyEpisode:
    pair_key: SpreadPairKey
    episode_id: str
    candidate_started_at: datetime
    reference_mean_bps: float
    reference_std_bps: float
    current_spread_bps: float
    current_deviation_from_reference_bps: float
    lifetime_peak_spread_bps: float
    lifetime_peak_deviation_bps: float
    lifetime_peak_at: datetime
    last_seen_at: datetime
    post_confirmation_peak_spread_bps: float | None = None
    post_confirmation_peak_deviation_bps: float | None = None
    post_confirmation_peak_at: datetime | None = None
    confirmed_at: datetime | None = None
    confirmation_spread_bps: float | None = None
    confirmation_deviation_bps: float | None = None
    confirmation_live_mean_bps: float | None = None
    confirmation_live_std_bps: float | None = None
    current_live_mean_bps: float | None = None
    current_live_std_bps: float | None = None
    ended_at: datetime | None = None
    end_spread_bps: float | None = None
    end_deviation_bps: float | None = None
    resolution_reason: ResolutionReason | None = None

    @property
    def is_confirmed(self) -> bool:
        return self.confirmed_at is not None

    @property
    def peak_spread_bps(self) -> float:
        """Backward-compatible alias for the lifetime peak."""
        return self.lifetime_peak_spread_bps

    @property
    def peak_deviation_bps(self) -> float:
        """Backward-compatible alias for the lifetime peak."""
        return self.lifetime_peak_deviation_bps

    @property
    def peak_at(self) -> datetime:
        """Backward-compatible alias for the lifetime peak timestamp."""
        return self.lifetime_peak_at

    @property
    def post_confirmation_expansion_bps(self) -> float | None:
        if (
            self.confirmation_deviation_bps is None
            or self.post_confirmation_peak_deviation_bps is None
        ):
            return None
        return (
            self.post_confirmation_peak_deviation_bps
            - self.confirmation_deviation_bps
        )

    @property
    def confirmation_to_peak_seconds(self) -> float | None:
        if self.confirmed_at is None or self.post_confirmation_peak_at is None:
            return None
        return (self.post_confirmation_peak_at - self.confirmed_at).total_seconds()

    @property
    def total_duration_seconds(self) -> float | None:
        if self.ended_at is None:
            return None
        return (self.ended_at - self.candidate_started_at).total_seconds()

    @property
    def post_confirmation_alive_seconds(self) -> float | None:
        if self.ended_at is None or self.confirmed_at is None:
            return None
        return (self.ended_at - self.confirmed_at).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_key": {
                "canonical_symbol": self.pair_key.canonical_symbol,
                "long_venue": self.pair_key.long_venue,
                "long_venue_symbol": self.pair_key.long_venue_symbol,
                "short_venue": self.pair_key.short_venue,
                "short_venue_symbol": self.pair_key.short_venue_symbol,
            },
            "episode_id": self.episode_id,
            "candidate_started_at": self.candidate_started_at.isoformat(),
            "reference_mean_bps": self.reference_mean_bps,
            "reference_std_bps": self.reference_std_bps,
            "current_spread_bps": self.current_spread_bps,
            "current_deviation_from_reference_bps": self.current_deviation_from_reference_bps,
            "lifetime_peak_spread_bps": self.lifetime_peak_spread_bps,
            "lifetime_peak_deviation_bps": self.lifetime_peak_deviation_bps,
            "lifetime_peak_at": self.lifetime_peak_at.isoformat(),
            "last_seen_at": self.last_seen_at.isoformat(),
            "post_confirmation_peak_spread_bps": self.post_confirmation_peak_spread_bps,
            "post_confirmation_peak_deviation_bps": self.post_confirmation_peak_deviation_bps,
            "post_confirmation_peak_at": _optional_time(self.post_confirmation_peak_at),
            "confirmed_at": _optional_time(self.confirmed_at),
            "confirmation_spread_bps": self.confirmation_spread_bps,
            "confirmation_deviation_bps": self.confirmation_deviation_bps,
            "confirmation_live_mean_bps": self.confirmation_live_mean_bps,
            "confirmation_live_std_bps": self.confirmation_live_std_bps,
            "current_live_mean_bps": self.current_live_mean_bps,
            "current_live_std_bps": self.current_live_std_bps,
            "ended_at": _optional_time(self.ended_at),
            "end_spread_bps": self.end_spread_bps,
            "end_deviation_bps": self.end_deviation_bps,
            "resolution_reason": self.resolution_reason,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> AnomalyEpisode:
        pair_payload = payload["pair_key"]
        pair_key = SpreadPairKey(
            canonical_symbol=pair_payload["canonical_symbol"],
            long_venue=pair_payload["long_venue"],
            long_venue_symbol=pair_payload["long_venue_symbol"],
            short_venue=pair_payload["short_venue"],
            short_venue_symbol=pair_payload["short_venue_symbol"],
        )
        return cls(
            pair_key=pair_key,
            episode_id=payload["episode_id"],
            candidate_started_at=_parse_time(payload["candidate_started_at"]),
            reference_mean_bps=float(payload["reference_mean_bps"]),
            reference_std_bps=float(payload["reference_std_bps"]),
            current_spread_bps=float(payload["current_spread_bps"]),
            current_deviation_from_reference_bps=float(
                payload["current_deviation_from_reference_bps"]
            ),
            lifetime_peak_spread_bps=_payload_float(
                payload, "lifetime_peak_spread_bps", "peak_spread_bps"
            ),
            lifetime_peak_deviation_bps=_payload_float(
                payload, "lifetime_peak_deviation_bps", "peak_deviation_bps"
            ),
            lifetime_peak_at=_parse_time(
                payload["lifetime_peak_at"]
                if "lifetime_peak_at" in payload
                else payload["peak_at"]
            ),
            last_seen_at=_parse_time(payload["last_seen_at"]),
            post_confirmation_peak_spread_bps=_legacy_or_optional_float(
                payload,
                "post_confirmation_peak_spread_bps",
                "peak_spread_bps",
            ),
            post_confirmation_peak_deviation_bps=_legacy_or_optional_float(
                payload,
                "post_confirmation_peak_deviation_bps",
                "peak_deviation_bps",
            ),
            post_confirmation_peak_at=_legacy_or_optional_time(
                payload,
                "post_confirmation_peak_at",
                "peak_at",
            ),
            confirmed_at=_parse_optional_time(payload.get("confirmed_at")),
            confirmation_spread_bps=_optional_float(payload.get("confirmation_spread_bps")),
            confirmation_deviation_bps=_optional_float(
                payload.get("confirmation_deviation_bps")
            ),
            confirmation_live_mean_bps=_optional_float(
                payload.get("confirmation_live_mean_bps")
            ),
            confirmation_live_std_bps=_optional_float(
                payload.get("confirmation_live_std_bps")
            ),
            current_live_mean_bps=_optional_float(payload.get("current_live_mean_bps")),
            current_live_std_bps=_optional_float(payload.get("current_live_std_bps")),
            ended_at=_parse_optional_time(payload.get("ended_at")),
            end_spread_bps=_optional_float(payload.get("end_spread_bps")),
            end_deviation_bps=_optional_float(payload.get("end_deviation_bps")),
            resolution_reason=payload.get("resolution_reason"),
        )


@dataclass(frozen=True)
class AnomalyTransition:
    kind: Literal["candidate_started", "confirmed", "resolved"]
    at: datetime
    episode: AnomalyEpisode


class AnomalyTracker:
    """Pure positive-deviation lifecycle for one exact directional pair."""

    def __init__(self, pair_key: SpreadPairKey, parameters: AnomalyParameters) -> None:
        self.pair_key = pair_key
        self.parameters = parameters
        self._active_episode: AnomalyEpisode | None = None
        self._confirmed_episodes: list[AnomalyEpisode] = []
        self._last_sample_time: datetime | None = None

    @property
    def active_episode(self) -> AnomalyEpisode | None:
        return self._active_episode

    @property
    def confirmed_episodes(self) -> tuple[AnomalyEpisode, ...]:
        return tuple(self._confirmed_episodes)

    def observe(self, observation: AnomalyObservation) -> tuple[AnomalyTransition, ...]:
        sample_time = observation.sample_time
        if self._last_sample_time is not None and sample_time < self._last_sample_time:
            raise ValueError("sample_time must not move backwards")

        transitions: list[AnomalyTransition] = []
        episode = self._active_episode
        if episode is not None:
            gap_seconds = (sample_time - episode.last_seen_at).total_seconds()
            if gap_seconds > self.parameters.max_gap_seconds:
                if episode.is_confirmed:
                    transitions.append(
                        self._resolve(
                            episode,
                            ended_at=episode.last_seen_at,
                            end_spread=episode.current_spread_bps,
                            reason="data_gap",
                        )
                    )
                else:
                    self._active_episode = None
                episode = self._active_episode

        if episode is None:
            if self._qualifies(observation):
                episode = self._start(observation)
                self._active_episode = episode
                transitions.append(
                    AnomalyTransition(
                        kind="candidate_started",
                        at=sample_time,
                        episode=self._snapshot(episode),
                    )
                )
                if self.parameters.confirmation_seconds == 0:
                    self._confirm(episode, observation)
                    transitions.append(
                        AnomalyTransition(
                            kind="confirmed",
                            at=sample_time,
                            episode=self._snapshot(episode),
                        )
                    )
        elif not episode.is_confirmed:
            if not self._qualifies(observation):
                self._active_episode = None
            else:
                self._update(episode, observation)
                if (
                    sample_time - episode.candidate_started_at
                ).total_seconds() >= self.parameters.confirmation_seconds:
                    self._confirm(episode, observation)
                    transitions.append(
                        AnomalyTransition(
                            kind="confirmed",
                            at=sample_time,
                            episode=self._snapshot(episode),
                        )
                    )
        else:
            self._update(episode, observation)
            if (
                observation.raw_spread_bps
                <= episode.reference_mean_bps + self.parameters.return_band_bps
            ):
                transitions.append(
                    self._resolve(
                        episode,
                        ended_at=sample_time,
                        end_spread=observation.raw_spread_bps,
                        reason="returned_to_mean_band",
                    )
                )

        self._last_sample_time = sample_time
        return tuple(transitions)

    def finalize(self) -> AnomalyEpisode | None:
        episode = self._active_episode
        if episode is None:
            return None
        self._active_episode = None
        if not episode.is_confirmed:
            return None
        episode.ended_at = episode.last_seen_at
        episode.end_spread_bps = episode.current_spread_bps
        episode.end_deviation_bps = episode.current_deviation_from_reference_bps
        episode.resolution_reason = "open_at_end"
        self._confirmed_episodes.append(episode)
        return episode

    def _qualifies(self, observation: AnomalyObservation) -> bool:
        if not observation.basis_eligible:
            return False
        if observation.rolling_mean_bps is None or observation.rolling_std_bps is None:
            return False
        return (
            observation.raw_spread_bps - observation.rolling_mean_bps
            >= self.parameters.anomaly_deviation_bps
        )

    def _start(self, observation: AnomalyObservation) -> AnomalyEpisode:
        assert observation.rolling_mean_bps is not None
        assert observation.rolling_std_bps is not None
        reference_mean = observation.rolling_mean_bps
        current_deviation = observation.raw_spread_bps - reference_mean
        return AnomalyEpisode(
            pair_key=self.pair_key,
            episode_id=self._episode_id(observation.sample_time),
            candidate_started_at=observation.sample_time,
            reference_mean_bps=reference_mean,
            reference_std_bps=observation.rolling_std_bps,
            current_spread_bps=observation.raw_spread_bps,
            current_deviation_from_reference_bps=current_deviation,
            lifetime_peak_spread_bps=observation.raw_spread_bps,
            lifetime_peak_deviation_bps=current_deviation,
            lifetime_peak_at=observation.sample_time,
            last_seen_at=observation.sample_time,
            current_live_mean_bps=observation.rolling_mean_bps,
            current_live_std_bps=observation.rolling_std_bps,
        )

    @staticmethod
    def _update(episode: AnomalyEpisode, observation: AnomalyObservation) -> None:
        episode.last_seen_at = observation.sample_time
        episode.current_spread_bps = observation.raw_spread_bps
        episode.current_deviation_from_reference_bps = (
            observation.raw_spread_bps - episode.reference_mean_bps
        )
        episode.current_live_mean_bps = observation.rolling_mean_bps
        episode.current_live_std_bps = observation.rolling_std_bps
        if (
            episode.current_deviation_from_reference_bps
            > episode.lifetime_peak_deviation_bps
        ):
            episode.lifetime_peak_deviation_bps = (
                episode.current_deviation_from_reference_bps
            )
            episode.lifetime_peak_spread_bps = observation.raw_spread_bps
            episode.lifetime_peak_at = observation.sample_time
        if (
            episode.confirmed_at is not None
            and observation.sample_time > episode.confirmed_at
            and (
                episode.post_confirmation_peak_deviation_bps is None
                or episode.current_deviation_from_reference_bps
                > episode.post_confirmation_peak_deviation_bps
            )
        ):
            episode.post_confirmation_peak_deviation_bps = (
                episode.current_deviation_from_reference_bps
            )
            episode.post_confirmation_peak_spread_bps = observation.raw_spread_bps
            episode.post_confirmation_peak_at = observation.sample_time

    @staticmethod
    def _confirm(episode: AnomalyEpisode, observation: AnomalyObservation) -> None:
        episode.confirmed_at = observation.sample_time
        episode.confirmation_spread_bps = observation.raw_spread_bps
        episode.confirmation_deviation_bps = (
            observation.raw_spread_bps - episode.reference_mean_bps
        )
        episode.post_confirmation_peak_spread_bps = observation.raw_spread_bps
        episode.post_confirmation_peak_deviation_bps = (
            episode.confirmation_deviation_bps
        )
        episode.post_confirmation_peak_at = observation.sample_time
        episode.confirmation_live_mean_bps = observation.rolling_mean_bps
        episode.confirmation_live_std_bps = observation.rolling_std_bps

    def _resolve(
        self,
        episode: AnomalyEpisode,
        *,
        ended_at: datetime,
        end_spread: float,
        reason: ResolutionReason,
    ) -> AnomalyTransition:
        episode.ended_at = ended_at
        episode.end_spread_bps = end_spread
        episode.end_deviation_bps = end_spread - episode.reference_mean_bps
        episode.resolution_reason = reason
        self._active_episode = None
        self._confirmed_episodes.append(episode)
        return AnomalyTransition(
            kind="resolved",
            at=ended_at,
            episode=self._snapshot(episode),
        )

    def _episode_id(self, candidate_started_at: datetime) -> str:
        return ":".join(
            (
                self.pair_key.canonical_symbol,
                self.pair_key.long_venue,
                self.pair_key.long_venue_symbol,
                self.pair_key.short_venue,
                self.pair_key.short_venue_symbol,
                candidate_started_at.isoformat(),
            )
        )

    @staticmethod
    def _snapshot(episode: AnomalyEpisode) -> AnomalyEpisode:
        return AnomalyEpisode.from_dict(episode.to_dict())


def _optional_time(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _parse_time(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO-8601 string")
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")), "timestamp")


def _parse_optional_time(value: object) -> datetime | None:
    return None if value is None else _parse_time(value)


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("numeric field must be JSON-compatible")
    return float(value)


def _payload_float(
    payload: dict[str, Any],
    field_name: str,
    legacy_field_name: str,
) -> float:
    value = payload[field_name] if field_name in payload else payload[legacy_field_name]
    return float(value)


def _legacy_or_optional_float(
    payload: dict[str, Any],
    field_name: str,
    legacy_field_name: str,
) -> float | None:
    if field_name in payload:
        return _optional_float(payload.get(field_name))
    if payload.get("confirmed_at") is None:
        return None
    return _optional_float(payload.get(legacy_field_name))


def _legacy_or_optional_time(
    payload: dict[str, Any],
    field_name: str,
    legacy_field_name: str,
) -> datetime | None:
    if field_name in payload:
        return _parse_optional_time(payload.get(field_name))
    if payload.get("confirmed_at") is None:
        return None
    return _parse_optional_time(payload.get(legacy_field_name))
