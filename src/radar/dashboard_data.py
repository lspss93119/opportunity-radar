from __future__ import annotations

import copy
import json
import math
import sqlite3
import threading
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Literal, cast

import duckdb  # type: ignore[import-untyped]

from radar.config import RadarConfig
from radar.monitors.spread.basis import (
    EXPECTED_INTERVAL_SECONDS,
    MIN_HISTORY_OBSERVATIONS,
    ROLLING_WINDOW_SECONDS,
    RollingBasis,
    RollingBasisStats,
)
from radar.monitors.spread.models import SpreadPairKey, calculate_raw_spread_bps

DEFAULT_DATA_ROOT = Path("data")
DEFAULT_RUNTIME_DB = Path("runtime/radar.sqlite3")
HEARTBEAT_HEALTHY_SECONDS = 30.0
HEARTBEAT_DEGRADED_SECONDS = 60.0
FEED_HEALTHY_SECONDS = 90.0
FEED_DEGRADED_SECONDS = 180.0
MAX_OPPORTUNITY_LIMIT = 200
MAX_DISPLAY_POINTS = 500
SCANNER_REFRESH_SECONDS = 10.0
SCANNER_OVERLAP_SECONDS = EXPECTED_INTERVAL_SECONDS * 2
# A newly visible Parquet file can contain a partial sample while the Radar
# writer is handing rows to storage.  One missing configured feed is a
# legitimate production sample; a larger gap is held back as unsettled.
SCANNER_MAX_MISSING_FEEDS = 1
SCANNER_BATCH_ROWS = 8_192
PAIR_RANGES = ("1h", "6h", "24h", "3d", "7d", "all")
PairRange = Literal["1h", "6h", "24h", "3d", "7d", "all"]
VWAP_FIELDS = (
    ("buy_1k_vwap", "sell_1k_vwap"),
    ("buy_5k_vwap", "sell_5k_vwap"),
    ("buy_10k_vwap", "sell_10k_vwap"),
)


def utc_now() -> datetime:
    return datetime.now(UTC)


def _as_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else _as_utc(value, "timestamp").isoformat()


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _positive_float(value: object) -> float | None:
    result = _finite_float(value)
    return result if result is not None and result > 0 else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


ExactIdentity = tuple[str, str, str, str, str]


def _parse_identity(value: object) -> ExactIdentity | None:
    if isinstance(value, dict):
        values = (
            value.get("canonical_symbol"),
            value.get("long_venue"),
            value.get("long_venue_symbol"),
            value.get("short_venue"),
            value.get("short_venue_symbol"),
        )
    elif isinstance(value, (tuple, list)):
        values = tuple(value)
    else:
        return None
    if len(values) != 5 or any(not isinstance(item, str) or not item for item in values):
        return None
    return cast(ExactIdentity, tuple(values))


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        try:
            return _as_utc(value, "timestamp")
        except ValueError:
            return None
    if not isinstance(value, str) or not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(value), "timestamp")
    except (TypeError, ValueError):
        return None


def classify_heartbeat(age_seconds: float | None) -> str:
    if age_seconds is None or age_seconds > HEARTBEAT_DEGRADED_SECONDS:
        return "down"
    if age_seconds > HEARTBEAT_HEALTHY_SECONDS:
        return "degraded"
    return "healthy"


def _classify_feed(age_seconds: float | None, primary_vwap_ready: bool) -> str:
    if age_seconds is None or age_seconds > FEED_DEGRADED_SECONDS:
        return "down"
    if age_seconds > FEED_HEALTHY_SECONDS or not primary_vwap_ready:
        return "degraded"
    return "healthy"


@dataclass(frozen=True)
class OpportunitiesFilters:
    symbol: str | None = None
    long_venue: str | None = None
    short_venue: str | None = None
    max_std_bps: float | None = None
    min_deviation_bps: float | None = None
    min_duration_seconds: int | None = None
    active_only: bool = False
    limit: int = MAX_OPPORTUNITY_LIMIT


@dataclass(frozen=True)
class _MarketRow:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    canonical_symbol: str
    buy_1k_vwap: float | None
    sell_1k_vwap: float | None
    buy_5k_vwap: float | None
    sell_5k_vwap: float | None
    buy_10k_vwap: float | None
    sell_10k_vwap: float | None


@dataclass(frozen=True)
class _PairObservation:
    key: SpreadPairKey
    sample_time: datetime
    observed_at: datetime
    long_buy_vwap: float
    short_sell_vwap: float
    long_fee_bps: float
    short_fee_bps: float
    raw_spread_bps: float
    observed_at_skew_seconds: float
    freshness_seconds: float | None
    valid_for_history: bool


@dataclass
class _RuntimeRead:
    heartbeat: dict[str, object]
    episodes: list[dict[str, object]]
    episodes_by_key: dict[tuple[str, str, str, str, str], dict[str, object]]
    recent_events: list[dict[str, object]]
    sqlite: dict[str, object]
    errors: list[str]


@dataclass(frozen=True)
class _CacheEntry:
    expires_at: datetime
    value: dict[str, object]


@dataclass(frozen=True, slots=True)
class _RollingPoint:
    sample_epoch: int
    raw_spread_bps: float


@dataclass(slots=True)
class _PairScannerState:
    points: deque[_RollingPoint]
    total: float = 0.0
    total_squares: float = 0.0
    latest: _PairObservation | None = None

    def upsert_point(
        self,
        sample_epoch: int,
        raw_spread_bps: float,
        *,
        valid_for_history: bool,
    ) -> None:
        for index in range(len(self.points) - 1, -1, -1):
            point = self.points[index]
            if point.sample_epoch == sample_epoch:
                self.total -= point.raw_spread_bps
                self.total_squares -= point.raw_spread_bps * point.raw_spread_bps
                del self.points[index]
                break
            if point.sample_epoch < sample_epoch:
                break
        if valid_for_history:
            point = _RollingPoint(sample_epoch, raw_spread_bps)
            if not self.points or sample_epoch > self.points[-1].sample_epoch:
                self.points.append(point)
            else:
                insert_at = next(
                    index
                    for index, existing in enumerate(self.points)
                    if existing.sample_epoch > sample_epoch
                )
                self.points.insert(insert_at, point)
            self.total += raw_spread_bps
            self.total_squares += raw_spread_bps * raw_spread_bps

    def prune(self, cutoff_epoch: int) -> None:
        while self.points and self.points[0].sample_epoch < cutoff_epoch:
            point = self.points.popleft()
            self.total -= point.raw_spread_bps
            self.total_squares -= point.raw_spread_bps * point.raw_spread_bps

    def stats_before(self, sample_epoch: int, *, prune: bool = True) -> RollingBasisStats:
        cutoff_epoch = sample_epoch - ROLLING_WINDOW_SECONDS
        if prune:
            self.prune(cutoff_epoch)
            points = tuple(self.points)
            total = self.total
            total_squares = self.total_squares
        else:
            # Snapshot construction must not mutate a state still referenced
            # by a concurrently readable immutable snapshot.
            points = tuple(
                point for point in self.points if point.sample_epoch >= cutoff_epoch
            )
            total = math.fsum(point.raw_spread_bps for point in points)
            total_squares = math.fsum(
                point.raw_spread_bps * point.raw_spread_bps for point in points
            )
        excluded = (
            points[-1]
            if points and points[-1].sample_epoch == sample_epoch
            else None
        )
        sample_count = len(points) - (1 if excluded is not None else 0)
        total -= excluded.raw_spread_bps if excluded is not None else 0.0
        total_squares -= (
            excluded.raw_spread_bps * excluded.raw_spread_bps
            if excluded is not None
            else 0.0
        )
        mean: float | None = None
        std: float | None = None
        if sample_count > 0:
            mean = total / sample_count
            variance = max(0.0, total_squares / sample_count - mean * mean)
            std = math.sqrt(variance)
        oldest = next(
            (point.sample_epoch for point in points if point.sample_epoch < sample_epoch),
            None,
        )
        coverage = sample_count / (
            ROLLING_WINDOW_SECONDS / EXPECTED_INTERVAL_SECONDS
        )
        if oldest is None or oldest > cutoff_epoch:
            coverage = 0.0
        return RollingBasisStats(
            sample_count=sample_count,
            coverage=coverage,
            mean_bps=mean,
            std_bps=std,
            min_observations=MIN_HISTORY_OBSERVATIONS,
        )


@dataclass(slots=True)
class _ScannerModel:
    known_files: set[Path]
    latest_rows: dict[tuple[str, str, str], _MarketRow]
    pair_states: dict[SpreadPairKey, _PairScannerState]
    data_as_of: datetime | None = None
    scanner_data_as_of: datetime | None = None
    errors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _ScannerSnapshot:
    files: tuple[Path, ...]
    latest_file_mtime: datetime | None
    latest_rows: Mapping[tuple[str, str, str], _MarketRow]
    pair_states: Mapping[SpreadPairKey, _PairScannerState]
    current_observations: Mapping[SpreadPairKey, _PairObservation]
    basis_by_key: Mapping[SpreadPairKey, dict[str, object]]
    data_as_of: datetime | None
    scanner_data_as_of: datetime | None
    errors: tuple[str, ...]


class DashboardQueryService:
    """Bounded, read-only queries over Radar's existing persisted sources."""

    def __init__(
        self,
        config: RadarConfig,
        *,
        data_root: Path = DEFAULT_DATA_ROOT,
        runtime_db: Path = DEFAULT_RUNTIME_DB,
        clock: Callable[[], datetime] = utc_now,
        cache_ttl_seconds: float = 5.0,
        max_cache_entries: int = 64,
    ) -> None:
        if not math.isfinite(cache_ttl_seconds) or cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds must be finite and non-negative")
        if max_cache_entries <= 0:
            raise ValueError("max_cache_entries must be positive")
        self.config = config
        self.data_root = Path(data_root)
        self.runtime_db = Path(runtime_db)
        self._clock = clock
        self._cache_ttl_seconds = cache_ttl_seconds
        self._max_cache_entries = max_cache_entries
        self._cache: dict[tuple[object, ...], _CacheEntry] = {}
        self._cache_lock = threading.Lock()
        self._scanner_lock = threading.RLock()
        self._scanner_refresh_lock = threading.Lock()
        self._scanner_model: _ScannerModel | None = None
        self._scanner_snapshot: _ScannerSnapshot | None = None
        self._scanner_stop = threading.Event()
        self._scanner_thread: threading.Thread | None = None

    def start(self, *, background: bool = False) -> None:
        """Hydrate the read view and optionally refresh it in the background."""
        self._ensure_scanner(_as_utc(self._clock(), "now"))
        if not background:
            return
        with self._scanner_lock:
            if self._scanner_thread is not None:
                return
            self._scanner_stop.clear()
            self._scanner_thread = threading.Thread(
                target=self._scanner_loop,
                name="radar-dashboard-scanner",
                daemon=True,
            )
            self._scanner_thread.start()

    def close(self) -> None:
        """Stop the optional refresh thread without touching persisted sources."""
        self._scanner_stop.set()
        thread = self._scanner_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)
        with self._scanner_lock:
            self._scanner_thread = None

    def _scanner_loop(self) -> None:
        while not self._scanner_stop.wait(SCANNER_REFRESH_SECONDS):
            try:
                self._refresh_scanner(_as_utc(self._clock(), "now"))
            except Exception:  # noqa: BLE001
                # The refresh method records source errors; a background read
                # failure must not terminate the dashboard process.
                continue

    def _ensure_scanner(self, now: datetime) -> None:
        with self._scanner_lock:
            initial = self._scanner_snapshot is None
        self._refresh_scanner(now, force=initial)

    def _scanner_snapshot_or_raise(self) -> _ScannerSnapshot:
        with self._scanner_lock:
            snapshot = self._scanner_snapshot
        if snapshot is None:
            raise RuntimeError("dashboard scanner is not hydrated")
        return snapshot

    def _refresh_scanner(self, now: datetime, *, force: bool = False) -> None:
        current_time = _as_utc(now, "now")
        with self._scanner_refresh_lock:
            files = self._partition_files(
                current_time - timedelta(seconds=ROLLING_WINDOW_SECONDS), current_time
            )
            with self._scanner_lock:
                model = self._scanner_model
                known_files = set(model.known_files) if model is not None else set()
                last_data_as_of = model.data_as_of if model is not None else None
            if model is not None and not force and not (set(files) - known_files):
                return

            if model is None:
                model = _ScannerModel(
                    known_files=set(),
                    latest_rows={},
                    pair_states={},
                )
                query_start = current_time - timedelta(
                    seconds=ROLLING_WINDOW_SECONDS
                    + self.config.monitors.spread.stale_after_seconds
                )
            else:
                query_start = current_time - timedelta(
                    seconds=ROLLING_WINDOW_SECONDS
                    + self.config.monitors.spread.stale_after_seconds
                )
                if last_data_as_of is not None:
                    query_start = max(
                        query_start,
                        last_data_as_of - timedelta(seconds=SCANNER_OVERLAP_SECONDS),
                    )

            sample_rows: list[_MarketRow] = []
            sample_time: datetime | None = None
            mutable_state_keys: set[SpreadPairKey] = set()

            def consume(row: _MarketRow) -> None:
                nonlocal sample_rows, sample_time
                if sample_time is not None and row.sample_time != sample_time:
                    self._consider_scanner_sample(
                        model,
                        sample_rows,
                        current_time,
                        mutable_state_keys,
                    )
                    sample_rows = []
                sample_time = row.sample_time
                sample_rows.append(row)
                identity = (row.venue, row.venue_symbol, row.canonical_symbol)
                previous = model.latest_rows.get(identity)
                if previous is None or (row.sample_time, row.observed_at) > (
                    previous.sample_time,
                    previous.observed_at,
                ):
                    model.latest_rows[identity] = row

            errors, data_as_of = self._read_market_rows_stream(
                query_start,
                current_time,
                files=files,
                on_row=consume,
            )
            if sample_rows:
                self._consider_scanner_sample(
                    model,
                    sample_rows,
                    current_time,
                    mutable_state_keys,
                )
            if data_as_of is not None:
                model.data_as_of = max(model.data_as_of or data_as_of, data_as_of)
            if errors:
                model.errors = tuple(errors)
            else:
                model.errors = ()
                model.known_files = set(files)
            with self._scanner_lock:
                self._scanner_model = model
                self._scanner_snapshot = self._build_scanner_snapshot(model, files)

    def _consider_scanner_sample(
        self,
        model: _ScannerModel,
        rows: list[_MarketRow],
        now: datetime,
        mutable_state_keys: set[SpreadPairKey],
    ) -> None:
        if not rows:
            return
        sample_time = rows[0].sample_time
        if (
            model.scanner_data_as_of is not None
            and sample_time < model.scanner_data_as_of
        ):
            return
        if not self._has_publishable_feed_coverage(rows):
            return
        self._apply_scanner_sample(model, rows, now, mutable_state_keys)
        if (
            model.scanner_data_as_of is None
            or sample_time > model.scanner_data_as_of
        ):
            model.scanner_data_as_of = sample_time

    def _has_publishable_feed_coverage(self, rows: Iterable[_MarketRow]) -> bool:
        identities = {
            (row.venue, row.venue_symbol, row.canonical_symbol) for row in rows
        }
        symbols = {row.canonical_symbol for row in rows}
        expected_count = sum(
            canonical_symbol in symbols
            for _, _, canonical_symbol in self._expected_feeds()
        )
        minimum_count = max(1, expected_count - SCANNER_MAX_MISSING_FEEDS)
        return len(identities) >= minimum_count

    def _apply_scanner_sample(
        self,
        model: _ScannerModel,
        rows: list[_MarketRow],
        now: datetime,
        mutable_state_keys: set[SpreadPairKey],
    ) -> None:
        for observation in self._build_pair_observations(rows, now):
            if observation.key not in mutable_state_keys:
                previous_state = model.pair_states.get(observation.key)
                state = (
                    copy.deepcopy(previous_state)
                    if previous_state is not None
                    else _PairScannerState(points=deque())
                )
                model.pair_states[observation.key] = state
                mutable_state_keys.add(observation.key)
            else:
                state = model.pair_states[observation.key]
            state.upsert_point(
                int(observation.sample_time.timestamp()),
                observation.raw_spread_bps,
                valid_for_history=observation.valid_for_history,
            )
            previous_observation = state.latest
            if previous_observation is None or (
                observation.sample_time,
                observation.observed_at,
            ) >= (
                previous_observation.sample_time,
                previous_observation.observed_at,
            ):
                state.latest = observation
            state.prune(
                int(observation.sample_time.timestamp()) - ROLLING_WINDOW_SECONDS
            )

    def _build_scanner_snapshot(
        self,
        model: _ScannerModel,
        files: tuple[Path, ...],
    ) -> _ScannerSnapshot:
        current: dict[SpreadPairKey, _PairObservation] = {}
        basis_by_key: dict[SpreadPairKey, dict[str, object]] = {}
        pair_states = dict(model.pair_states)
        if not model.errors:
            for key, state in pair_states.items():
                if (
                    state.latest is None
                    or model.scanner_data_as_of is None
                    or state.latest.sample_time != model.scanner_data_as_of
                ):
                    continue
                current[key] = state.latest
                stats = state.stats_before(
                    int(state.latest.sample_time.timestamp()),
                    prune=False,
                )
                basis_by_key[key] = self._basis_stats_payload(stats)
        return _ScannerSnapshot(
            files=files,
            latest_file_mtime=self._latest_file_mtime(files),
            latest_rows=MappingProxyType(dict(model.latest_rows if not model.errors else {})),
            pair_states=MappingProxyType(pair_states),
            current_observations=MappingProxyType(current),
            basis_by_key=MappingProxyType(basis_by_key),
            data_as_of=model.data_as_of,
            scanner_data_as_of=model.scanner_data_as_of,
            errors=model.errors,
        )

    def get_status(self, *, now: datetime | None = None) -> dict[str, object]:
        current_time = _as_utc(self._clock() if now is None else now, "now")
        self._ensure_scanner(current_time)
        return self._cached(
            ("status",),
            current_time,
            lambda: self._compute_status(current_time),
        )

    def get_opportunities(
        self,
        filters: OpportunitiesFilters,
        *,
        now: datetime | None = None,
    ) -> dict[str, object]:
        self._validate_filters(filters)
        current_time = _as_utc(self._clock() if now is None else now, "now")
        self._ensure_scanner(current_time)
        key = (
            "opportunities",
            filters.symbol,
            filters.long_venue,
            filters.short_venue,
            filters.max_std_bps,
            filters.min_deviation_bps,
            filters.min_duration_seconds,
            filters.active_only,
            filters.limit,
        )
        return self._cached(
            key,
            current_time,
            lambda: self._compute_opportunities(filters, current_time),
        )

    def get_anomalies(
        self,
        *,
        symbol: str | None = None,
        long_venue: str | None = None,
        short_venue: str | None = None,
        eligible_only: bool = False,
        now: datetime | None = None,
    ) -> dict[str, object]:
        current_time = _as_utc(self._clock() if now is None else now, "now")
        key = ("anomalies", symbol, long_venue, short_venue, eligible_only)
        return self._cached(
            key,
            current_time,
            lambda: self._compute_anomalies(
                current_time,
                symbol=symbol,
                long_venue=long_venue,
                short_venue=short_venue,
                eligible_only=eligible_only,
            ),
        )

    def _compute_anomalies(
        self,
        now: datetime,
        *,
        symbol: str | None,
        long_venue: str | None,
        short_venue: str | None,
        eligible_only: bool,
    ) -> dict[str, object]:
        active, _by_key, _notifications, _events, errors = self._read_v2_runtime(now)
        rows = [
            row
            for row in active
            if (symbol is None or row.get("symbol") == symbol.upper())
            and (long_venue is None or row.get("long_venue") == long_venue.lower())
            and (short_venue is None or row.get("short_venue") == short_venue.lower())
            and (not eligible_only or row.get("tg_eligible") is True)
        ]
        def anomaly_sort_key(row: dict[str, object]) -> tuple[int, float, str, str, str, str, str]:
            deviation = _finite_float(row.get("current_deviation_bps"))
            return (
                0 if row.get("tg_eligible") is True else 1,
                -(deviation if deviation is not None else 0.0),
                str(row.get("symbol") or ""),
                str(row.get("long_venue") or ""),
                str(row.get("long_venue_symbol") or ""),
                str(row.get("short_venue") or ""),
                str(row.get("short_venue_symbol") or ""),
            )

        rows.sort(key=anomaly_sort_key)
        return {
            "generated_at": now.isoformat(),
            "data_as_of": _iso(now),
            "status": "degraded" if errors else "healthy",
            "rows": rows,
            "errors": errors,
        }

    def get_pair(
        self,
        *,
        canonical_symbol: str,
        long_venue: str,
        long_venue_symbol: str,
        short_venue: str,
        short_venue_symbol: str,
        range_name: PairRange,
        now: datetime | None = None,
    ) -> dict[str, object]:
        if range_name not in PAIR_RANGES:
            raise ValueError("range_name must be one of 1h, 6h, 24h, 3d, 7d, all")
        identity = (
            canonical_symbol,
            long_venue,
            long_venue_symbol,
            short_venue,
            short_venue_symbol,
        )
        if any(not isinstance(value, str) or not value for value in identity):
            raise ValueError("pair identity fields must be non-empty strings")
        current_time = _as_utc(self._clock() if now is None else now, "now")
        key = ("pair", *identity, range_name)
        return self._cached(
            key,
            current_time,
            lambda: self._compute_pair(identity, range_name, current_time),
        )

    def _cached(
        self,
        key: tuple[object, ...],
        now: datetime,
        compute: Callable[[], dict[str, object]],
    ) -> dict[str, object]:
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry is not None and now < entry.expires_at:
                return copy.deepcopy(entry.value)
            if entry is not None:
                self._cache.pop(key, None)

        value = compute()
        expires_at = now + timedelta(seconds=self._cache_ttl_seconds)
        with self._cache_lock:
            self._cache[key] = _CacheEntry(expires_at=expires_at, value=copy.deepcopy(value))
            while len(self._cache) > self._max_cache_entries:
                self._cache.pop(next(iter(self._cache)))
        return value

    @staticmethod
    def _validate_filters(filters: OpportunitiesFilters) -> None:
        if not isinstance(filters, OpportunitiesFilters):
            raise TypeError("filters must be OpportunitiesFilters")
        for value, name in (
            (filters.max_std_bps, "max_std_bps"),
            (filters.min_deviation_bps, "min_deviation_bps"),
        ):
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(f"{name} must be finite")
        if filters.max_std_bps is not None and filters.max_std_bps < 0:
            raise ValueError("max_std_bps must be non-negative")
        if filters.min_duration_seconds is not None and (
            isinstance(filters.min_duration_seconds, bool)
            or not isinstance(filters.min_duration_seconds, int)
            or filters.min_duration_seconds < 0
        ):
            raise ValueError("min_duration_seconds must be a non-negative integer")
        if not isinstance(filters.active_only, bool):
            raise ValueError("active_only must be boolean")
        if (
            isinstance(filters.limit, bool)
            or not isinstance(filters.limit, int)
            or not 0 < filters.limit <= MAX_OPPORTUNITY_LIMIT
        ):
            raise ValueError(f"limit must be between 1 and {MAX_OPPORTUNITY_LIMIT}")

    def _compute_status(self, now: datetime) -> dict[str, object]:
        snapshot = self._scanner_snapshot_or_raise()
        market_errors = list(snapshot.errors)
        data_as_of = snapshot.data_as_of
        scanner_data_as_of = snapshot.scanner_data_as_of
        runtime = self._read_runtime(now)
        v2_active, _v2_by_key, _v2_notifications, _v2_events, v2_errors = self._read_v2_runtime(now)
        errors = [*market_errors, *runtime.errors, *v2_errors]
        expected = self._expected_feeds()

        feeds: list[dict[str, object]] = []
        for venue, venue_symbol, canonical_symbol in expected:
            row = snapshot.latest_rows.get((venue, venue_symbol, canonical_symbol))
            feed = self._feed_payload(row, now)
            feed.update(
                {
                    "venue": venue,
                    "venue_symbol": venue_symbol,
                    "canonical_symbol": canonical_symbol,
                }
            )
            feeds.append(feed)

        healthy_feeds = sum(feed["status"] == "healthy" for feed in feeds)
        primary_vwap_ready = sum(feed["primary_vwap_ready"] is True for feed in feeds)
        heartbeat_age = runtime.heartbeat.get("age_seconds")
        heartbeat_status = runtime.heartbeat.get("status")
        if heartbeat_status == "down":
            overall_status = "down"
        elif heartbeat_status == "degraded":
            overall_status = "degraded"
        elif errors or any(feed["status"] != "healthy" for feed in feeds):
            overall_status = "degraded"
        else:
            overall_status = "healthy"

        files = snapshot.files
        parquet_status = "healthy" if files and not market_errors else "unavailable"
        if files and market_errors:
            parquet_status = "degraded"
        latest_file_mtime = self._latest_file_mtime(files)
        per_venue = self._venue_health(feeds, data_as_of)
        return {
            "generated_at": now.isoformat(),
            "data_as_of": _iso(data_as_of),
            "latest_dataset_sample_time": _iso(data_as_of),
            "scanner_data_as_of": _iso(scanner_data_as_of),
            "sample_age_seconds": (
                (now - data_as_of).total_seconds() if data_as_of is not None else None
            ),
            "scanner_sample_age_seconds": (
                (now - scanner_data_as_of).total_seconds()
                if scanner_data_as_of is not None
                else None
            ),
            "heartbeat": runtime.heartbeat,
            "radar_heartbeat": runtime.heartbeat,
            "overall": {
                "status": overall_status,
                "heartbeat_age_seconds": heartbeat_age,
                "configured_feeds": len(feeds),
                "latest_feeds": sum(
                    1 for entry in per_venue.values() if entry.get("latest") == 1
                ),
                "healthy_feeds": healthy_feeds,
                "primary_vwap_ready": primary_vwap_ready,
                "active_episodes": len(runtime.episodes),
                "confirmed_candidates": sum(
                    episode.get("candidate_confirmed") is True
                    for episode in runtime.episodes
                ),
                "active_alerted_episodes": sum(
                    episode.get("alerted") is True for episode in runtime.episodes
                ),
                "active_anomaly_candidates": 0,
                "active_confirmed_anomalies": len(v2_active),
                "anomaly_tg_eligible": sum(
                    row.get("tg_eligible") is True for row in v2_active
                ),
                "anomaly_tg_suppressed": sum(
                    row.get("tg_eligible") is False for row in v2_active
                ),
            },
            "feeds": feeds,
            "venues": per_venue,
            "parquet": {
                "status": parquet_status,
                "latest_file_mtime": _iso(latest_file_mtime),
                "files_considered": len(files),
            },
            "sqlite": runtime.sqlite,
            "episodes": runtime.episodes,
            "recent_events": runtime.recent_events,
            "errors": errors,
        }

    def _compute_opportunities(
        self,
        filters: OpportunitiesFilters,
        now: datetime,
    ) -> dict[str, object]:
        snapshot = self._scanner_snapshot_or_raise()
        market_errors = list(snapshot.errors)
        data_as_of = snapshot.data_as_of
        scanner_data_as_of = snapshot.scanner_data_as_of
        runtime = self._read_runtime(now)
        current_by_key = {}
        for key, observation in snapshot.current_observations.items():
            if not self._is_current_observation(observation, now):
                continue
            current_by_key[key] = replace(
                observation,
                freshness_seconds=(now - observation.observed_at).total_seconds(),
            )
        output_rows: list[dict[str, object]] = []
        basis_unavailable = False
        for key, observation in current_by_key.items():
            row = self._opportunity_payload(
                observation,
                (),
                runtime.episodes_by_key,
                now,
                basis=snapshot.basis_by_key.get(key, self._unavailable_basis()),
            )
            if not self._matches_filters(row, filters):
                continue
            output_rows.append(row)
            basis_unavailable = basis_unavailable or row["basis_eligible"] is not True

        output_rows.sort(key=self._opportunity_sort_key)
        output_rows = output_rows[: filters.limit]
        errors = [*market_errors, *runtime.errors]
        status = "healthy"
        if errors or basis_unavailable or not current_by_key:
            status = "degraded"
        if not output_rows and (market_errors or runtime.heartbeat["status"] == "down"):
            status = "down"
        return {
            "data_as_of": _iso(data_as_of),
            "latest_dataset_sample_time": _iso(data_as_of),
            "scanner_data_as_of": _iso(scanner_data_as_of),
            "generated_at": now.isoformat(),
            "status": status,
            "rows": output_rows,
            "errors": errors,
        }

    def _compute_pair(
        self,
        identity: tuple[str, str, str, str, str],
        range_name: PairRange,
        now: datetime,
    ) -> dict[str, object]:
        key = SpreadPairKey(*identity)
        errors: list[str] = []
        if not self._is_configured_pair(identity):
            return {
                "identity": self._identity_payload(key),
                "range": range_name,
                "generated_at": now.isoformat(),
                "data_as_of": None,
                "status": "down",
                "current": None,
                "basis": self._unavailable_basis(),
                "lifecycle": self._unavailable_lifecycle(),
                "history": [],
                "rolling_mean_series": [],
                "errors": ["pair identity is not an enabled configured mapping"],
            }

        display_start = self._range_start(range_name, now)
        query_start = display_start - timedelta(seconds=ROLLING_WINDOW_SECONDS)
        pair_feeds = (
            (key.long_venue, key.long_venue_symbol, key.canonical_symbol),
            (key.short_venue, key.short_venue_symbol, key.canonical_symbol),
        )
        rows, market_errors, data_as_of = self._read_market_rows(
            query_start, now, identities=pair_feeds
        )
        errors.extend(market_errors)
        observations = [
            observation
            for observation in self._build_pair_observations(rows, now)
            if observation.key == key
        ]
        current_observation = self._current_observations(
            observations,
            rows,
            now,
        ).get(key)
        runtime = self._read_runtime(now)
        errors.extend(runtime.errors)
        active_v2, v2_by_key, _notifications, v2_events, v2_errors = self._read_v2_runtime(now)
        errors.extend(v2_errors)
        basis = self._basis_payload_for(observations, current_observation)
        if current_observation is None:
            errors.append("current exact pair observation unavailable")

        display_points = [
            observation
            for observation in observations
            if display_start <= observation.sample_time <= now
            and (
                observation.valid_for_history
                or (
                    current_observation is not None
                    and observation.sample_time == current_observation.sample_time
                )
            )
        ]
        # Segment on complete observations before display downsampling, so an
        # omitted slot never becomes a line through a gap in the browser.
        segments: dict[datetime, int] = {}
        segment = 0
        for index, point in enumerate(display_points):
            if index and (point.sample_time - display_points[index - 1].sample_time).total_seconds() > EXPECTED_INTERVAL_SECONDS:
                segment += 1
            segments[point.sample_time] = segment
        history = [
            {
                "sample_time": observation.sample_time.isoformat(),
                "raw_spread_bps": observation.raw_spread_bps,
                "segment": segments[observation.sample_time],
            }
            for observation in self._downsample_points(display_points)
        ]
        rolling_mean_series = self._rolling_mean_series(observations, display_points)
        lifecycle = self._lifecycle_for(key, runtime.episodes_by_key, now)
        v2_lifecycle = self._anomaly_lifecycle_for(key, v2_by_key, v2_events, now)
        if v2_lifecycle["available"]:
            lifecycle = v2_lifecycle
        current_payload = (
            self._current_payload(current_observation, basis, lifecycle)
            if current_observation is not None
            else None
        )
        status = "healthy"
        if errors or not basis["eligible"]:
            status = "degraded"
        if current_observation is None and not observations:
            status = "down"
        return {
            "identity": self._identity_payload(key),
            "range": range_name,
            "generated_at": now.isoformat(),
            "data_as_of": _iso(data_as_of),
            "status": status,
            "current": current_payload,
            "basis": basis,
            "lifecycle": lifecycle,
            "anomaly_markers": self._anomaly_markers_for(key, v2_events),
            "history": history,
            "rolling_mean_series": rolling_mean_series,
            "errors": errors,
        }

    def _market_query(
        self,
        files: tuple[Path, ...],
        expected: tuple[tuple[str, str, str], ...],
    ) -> tuple[str, list[object]]:
        predicates = " OR ".join(
            "(venue = ? AND venue_symbol = ? AND canonical_symbol = ?)"
            for _ in expected
        ) or "FALSE"
        path_list = ", ".join(
            "'" + str(path).replace("'", "''") + "'" for path in files
        )
        query = f"""
            WITH ranked AS (
                SELECT
                    sample_time,
                    observed_at,
                    venue,
                    venue_symbol,
                    canonical_symbol,
                    buy_1k_vwap,
                    sell_1k_vwap,
                    buy_5k_vwap,
                    sell_5k_vwap,
                    buy_10k_vwap,
                    sell_10k_vwap,
                    row_number() OVER (
                        PARTITION BY venue, venue_symbol, canonical_symbol, sample_time
                        ORDER BY observed_at DESC
                    ) AS row_number
                FROM read_parquet([{path_list}])
                WHERE sample_time >= ? AND sample_time <= ?
                  AND ({predicates})
            )
            SELECT
                sample_time,
                observed_at,
                venue,
                venue_symbol,
                canonical_symbol,
                buy_1k_vwap,
                sell_1k_vwap,
                buy_5k_vwap,
                sell_5k_vwap,
                buy_10k_vwap,
                sell_10k_vwap
            FROM ranked
            WHERE row_number = 1
            ORDER BY sample_time, venue, venue_symbol
        """
        return query, [value for item in expected for value in item]

    @staticmethod
    def _market_row_from_values(values: tuple[object, ...]) -> tuple[_MarketRow | None, str | None]:
        sample_time = _parse_timestamp(values[0])
        observed_at = _parse_timestamp(values[1])
        venue = _text(values[2])
        venue_symbol = _text(values[3])
        canonical_symbol = _text(values[4])
        if (
            sample_time is None
            or observed_at is None
            or venue is None
            or venue_symbol is None
            or canonical_symbol is None
        ):
            return None, "invalid market row ignored"
        return (
            _MarketRow(
                sample_time=sample_time,
                observed_at=observed_at,
                venue=venue,
                venue_symbol=venue_symbol,
                canonical_symbol=canonical_symbol,
                buy_1k_vwap=_finite_float(values[5]),
                sell_1k_vwap=_finite_float(values[6]),
                buy_5k_vwap=_finite_float(values[7]),
                sell_5k_vwap=_finite_float(values[8]),
                buy_10k_vwap=_finite_float(values[9]),
                sell_10k_vwap=_finite_float(values[10]),
            ),
            None,
        )

    def _read_market_rows_stream(
        self,
        start: datetime,
        end: datetime,
        *,
        files: tuple[Path, ...],
        on_row: Callable[[_MarketRow], None],
        identities: Iterable[tuple[str, str, str]] | None = None,
    ) -> tuple[list[str], datetime | None]:
        start = _as_utc(start, "start")
        end = _as_utc(end, "end")
        if start > end:
            raise ValueError("start must not be after end")
        if not files:
            return [], None
        expected = tuple(self._expected_feeds() if identities is None else identities)
        query, identity_params = self._market_query(files, expected)
        errors: list[str] = []
        data_as_of: datetime | None = None
        try:
            with duckdb.connect() as connection:
                result = connection.execute(query, [start, end, *identity_params])
                while batch := result.fetchmany(SCANNER_BATCH_ROWS):
                    for values in batch:
                        row, error = self._market_row_from_values(values)
                        if error is not None:
                            errors.append(error)
                        if row is None:
                            continue
                        data_as_of = max(data_as_of or row.sample_time, row.sample_time)
                        on_row(row)
        except Exception as error:  # noqa: BLE001
            errors.append(f"market data read failed: {type(error).__name__}: {error}")
        return errors, data_as_of

    def _read_market_rows(
        self,
        start: datetime,
        end: datetime,
        *,
        identities: Iterable[tuple[str, str, str]] | None = None,
    ) -> tuple[list[_MarketRow], list[str], datetime | None]:
        start = _as_utc(start, "start")
        end = _as_utc(end, "end")
        if start > end:
            raise ValueError("start must not be after end")
        files = self._partition_files(start, end)
        if not files:
            return [], [], None
        expected = tuple(self._expected_feeds() if identities is None else identities)
        path_list = ", ".join(
            "'" + str(path).replace("'", "''") + "'" for path in files
        )
        query, identity_params = self._market_query(files, expected)
        try:
            with duckdb.connect() as connection:
                data_as_of_row = connection.execute(
                    f"SELECT max(sample_time) FROM read_parquet([{path_list}]) "
                    "WHERE sample_time >= ? AND sample_time <= ?",
                    [start, end],
                ).fetchone()
                data_as_of_value = (
                    data_as_of_row[0] if data_as_of_row is not None else None
                )
                data_as_of = (
                    data_as_of_value
                    if isinstance(data_as_of_value, datetime)
                    else None
                )
                result = connection.execute(
                    query, [start, end, *identity_params]
                ).fetchall()
        except Exception as error:  # noqa: BLE001
            return [], [f"market data read failed: {type(error).__name__}: {error}"], None

        rows: list[_MarketRow] = []
        errors: list[str] = []
        for values in result:
            row, row_error = self._market_row_from_values(values)
            if row_error is not None:
                errors.append(row_error)
            if row is not None:
                rows.append(row)
        return rows, errors, _parse_timestamp(data_as_of)

    def _partition_files(self, start: datetime, end: datetime) -> tuple[Path, ...]:
        start_date = start.date()
        end_date = end.date()
        files: list[Path] = []
        partition_date = start_date
        while partition_date <= end_date:
            partition = self.data_root / "market" / f"date={partition_date.isoformat()}"
            files.extend(
                path
                for path in sorted(partition.glob("part-*.parquet"))
                if path.is_file()
            )
            partition_date += timedelta(days=1)
        return tuple(files)

    @staticmethod
    def _latest_file_mtime(files: Iterable[Path]) -> datetime | None:
        mtimes: list[datetime] = []
        for path in files:
            try:
                mtimes.append(datetime.fromtimestamp(path.stat().st_mtime, UTC))
            except OSError:
                continue
        return max(mtimes) if mtimes else None

    def _expected_feeds(self) -> list[tuple[str, str, str]]:
        return [
            (market.venue, market.venue_symbol, market.canonical_symbol)
            for market in self.config.markets
            if market.enabled
        ]

    def _enabled_identities(self) -> set[tuple[str, str, str]]:
        return set(self._expected_feeds())

    def _is_configured_pair(self, identity: tuple[str, str, str, str, str]) -> bool:
        canonical_symbol, long_venue, long_symbol, short_venue, short_symbol = identity
        if long_venue.lower() == short_venue.lower():
            return False
        return (
            (long_venue, long_symbol, canonical_symbol) in self._enabled_identities()
            and (short_venue, short_symbol, canonical_symbol) in self._enabled_identities()
        )

    def _feed_payload(self, row: _MarketRow | None, now: datetime) -> dict[str, object]:
        if row is None:
            return {
                "status": "down",
                "sample_time": None,
                "age_seconds": None,
                "observed_at": None,
                "vwap_available": 0,
                "vwap_total": len(VWAP_FIELDS),
                "primary_vwap_ready": False,
                "buy_1k_vwap": None,
                "sell_1k_vwap": None,
                "buy_5k_vwap": None,
                "sell_5k_vwap": None,
                "buy_10k_vwap": None,
                "sell_10k_vwap": None,
            }
        age_seconds = (now - row.observed_at).total_seconds()
        age = age_seconds if age_seconds >= 0 else None
        vwap_available = sum(
            _positive_float(getattr(row, buy)) is not None
            and _positive_float(getattr(row, sell)) is not None
            for buy, sell in VWAP_FIELDS
        )
        primary_ready = (
            _positive_float(row.buy_10k_vwap) is not None
            and _positive_float(row.sell_10k_vwap) is not None
            and age is not None
            and row.sample_time <= now
        )
        return {
            "status": _classify_feed(age, primary_ready),
            "sample_time": row.sample_time.isoformat(),
            "age_seconds": age,
            "observed_at": row.observed_at.isoformat(),
            "vwap_available": vwap_available,
            "vwap_total": len(VWAP_FIELDS),
            "primary_vwap_ready": primary_ready,
            "buy_1k_vwap": row.buy_1k_vwap,
            "sell_1k_vwap": row.sell_1k_vwap,
            "buy_5k_vwap": row.buy_5k_vwap,
            "sell_5k_vwap": row.sell_5k_vwap,
            "buy_10k_vwap": row.buy_10k_vwap,
            "sell_10k_vwap": row.sell_10k_vwap,
        }

    @staticmethod
    def _venue_health(
        feeds: list[dict[str, object]], data_as_of: datetime | None
    ) -> dict[str, dict[str, object]]:
        result: dict[str, dict[str, object]] = {}
        for feed in feeds:
            venue = str(feed["venue"])
            entry = result.setdefault(
                venue,
                {
                    "expected": 0,
                    "available": 0,
                    "latest": 0,
                    "missing": 0,
                    "max_observation_age_seconds": None,
                },
            )
            expected = entry.get("expected")
            entry["expected"] = (expected if isinstance(expected, int) else 0) + 1
            if feed["sample_time"] is not None:
                available = entry.get("available")
                entry["available"] = (available if isinstance(available, int) else 0) + 1
            if data_as_of is not None and feed["sample_time"] == _iso(data_as_of):
                latest = entry.get("latest")
                entry["latest"] = (latest if isinstance(latest, int) else 0) + 1
            else:
                missing = entry.get("missing")
                entry["missing"] = (missing if isinstance(missing, int) else 0) + 1
            age = feed["age_seconds"]
            if isinstance(age, (int, float)):
                previous_age = entry["max_observation_age_seconds"]
                entry["max_observation_age_seconds"] = max(
                    float(age),
                    float(previous_age) if isinstance(previous_age, (int, float)) else 0.0,
                )
        return result

    def _build_pair_observations(
        self,
        rows: Iterable[_MarketRow],
        now: datetime,
    ) -> list[_PairObservation]:
        grouped: dict[tuple[str, datetime], list[_MarketRow]] = {}
        enabled = self._enabled_identities()
        for row in rows:
            identity = (row.venue, row.venue_symbol, row.canonical_symbol)
            if identity in enabled:
                grouped.setdefault((row.canonical_symbol, row.sample_time), []).append(row)

        observations: list[_PairObservation] = []
        for (canonical_symbol, sample_time), sample_rows in grouped.items():
            for long_row in sample_rows:
                long_buy = _positive_float(long_row.buy_10k_vwap)
                long_fee = self._fee_for(long_row.venue)
                if long_buy is None or long_fee is None:
                    continue
                for short_row in sample_rows:
                    if long_row is short_row or long_row.venue.lower() == short_row.venue.lower():
                        continue
                    short_sell = _positive_float(short_row.sell_10k_vwap)
                    short_fee = self._fee_for(short_row.venue)
                    if short_sell is None or short_fee is None:
                        continue
                    if not math.isfinite(2.0 * (long_fee + short_fee)):
                        continue
                    try:
                        raw_spread = calculate_raw_spread_bps(long_buy, short_sell)
                    except (OverflowError, ValueError):
                        continue
                    observed_skew = abs(
                        (long_row.observed_at - short_row.observed_at).total_seconds()
                    )
                    observations.append(
                        _PairObservation(
                            key=SpreadPairKey(
                                canonical_symbol=canonical_symbol,
                                long_venue=long_row.venue,
                                long_venue_symbol=long_row.venue_symbol,
                                short_venue=short_row.venue,
                                short_venue_symbol=short_row.venue_symbol,
                            ),
                            sample_time=sample_time,
                            observed_at=max(long_row.observed_at, short_row.observed_at),
                            long_buy_vwap=long_buy,
                            short_sell_vwap=short_sell,
                            long_fee_bps=long_fee,
                            short_fee_bps=short_fee,
                            raw_spread_bps=raw_spread,
                            observed_at_skew_seconds=observed_skew,
                            freshness_seconds=None,
                            valid_for_history=(
                                # The slot identifies the sample, not when it
                                # became available. Bound both age and delay.
                                all(
                                    row.observed_at <= now
                                    and abs((sample_time - row.observed_at).total_seconds())
                                    <= self.config.monitors.spread.stale_after_seconds
                                    for row in (long_row, short_row)
                                )
                            ),
                        )
                    )
        observations.sort(key=lambda observation: (self._pair_sort_key(observation.key), observation.sample_time))
        return observations

    @staticmethod
    def _pair_sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
        return (
            key.canonical_symbol,
            key.long_venue,
            key.long_venue_symbol,
            key.short_venue,
            key.short_venue_symbol,
        )

    def _current_observations(
        self,
        observations: Iterable[_PairObservation],
        rows: Iterable[_MarketRow],
        now: datetime,
    ) -> dict[SpreadPairKey, _PairObservation]:
        observations = tuple(observations)
        rows = tuple(rows)
        row_by_identity_sample = {
            (row.venue, row.venue_symbol, row.canonical_symbol, row.sample_time): row
            for row in rows
        }
        sample_times_by_identity: dict[tuple[str, str, str], set[datetime]] = {}
        for row in rows:
            identity = (row.venue, row.venue_symbol, row.canonical_symbol)
            sample_times_by_identity.setdefault(identity, set()).add(row.sample_time)

        observation_keys = {observation.key for observation in observations}
        latest_exact_sample: dict[SpreadPairKey, datetime] = {}
        for key in observation_keys:
            long_times = sample_times_by_identity.get(
                (key.long_venue, key.long_venue_symbol, key.canonical_symbol),
                set(),
            )
            short_times = sample_times_by_identity.get(
                (key.short_venue, key.short_venue_symbol, key.canonical_symbol),
                set(),
            )
            common_times = long_times & short_times
            if common_times:
                latest_exact_sample[key] = max(common_times)

        current: dict[SpreadPairKey, _PairObservation] = {}
        for observation in observations:
            if observation.sample_time != latest_exact_sample.get(observation.key):
                continue
            long_row = row_by_identity_sample.get(
                (
                    observation.key.long_venue,
                    observation.key.long_venue_symbol,
                    observation.key.canonical_symbol,
                    observation.sample_time,
                )
            )
            short_row = row_by_identity_sample.get(
                (
                    observation.key.short_venue,
                    observation.key.short_venue_symbol,
                    observation.key.canonical_symbol,
                    observation.sample_time,
                )
            )
            if long_row is None or short_row is None:
                continue
            if not self._is_current_row(long_row, now) or not self._is_current_row(short_row, now):
                continue
            freshness = max(
                (now - long_row.observed_at).total_seconds(),
                (now - short_row.observed_at).total_seconds(),
            )
            current_observation = _PairObservation(
                **{
                    **observation.__dict__,
                    "freshness_seconds": freshness,
                }
            )
            previous = current.get(observation.key)
            if previous is None or current_observation.sample_time > previous.sample_time:
                current[observation.key] = current_observation
        return current

    def _is_current_row(self, row: _MarketRow, now: datetime) -> bool:
        age_seconds = (now - row.observed_at).total_seconds()
        return (
            row.sample_time <= now
            and 0 <= age_seconds <= self.config.monitors.spread.stale_after_seconds
        )

    def _is_current_observation(
        self,
        observation: _PairObservation,
        now: datetime,
    ) -> bool:
        age_seconds = (now - observation.observed_at).total_seconds()
        return (
            observation.sample_time <= now
            and 0 <= age_seconds <= self.config.monitors.spread.stale_after_seconds
            and age_seconds + observation.observed_at_skew_seconds
            <= self.config.monitors.spread.stale_after_seconds
        )

    def _fee_for(self, venue: str) -> float | None:
        for configured_venue, value in self.config.fees_bps.items():
            if not isinstance(configured_venue, str) or configured_venue.lower() != venue.lower():
                continue
            if isinstance(value, bool):
                return None
            fee = _finite_float(value)
            return fee if fee is not None and fee >= 0 else None
        return None

    def _opportunity_payload(
        self,
        observation: _PairObservation,
        all_observations: Iterable[_PairObservation],
        episodes_by_key: Mapping[tuple[str, str, str, str, str], dict[str, object]],
        now: datetime,
        *,
        basis: dict[str, object] | None = None,
    ) -> dict[str, object]:
        basis = (
            self._basis_payload_for(all_observations, observation)
            if basis is None
            else basis
        )
        lifecycle = self._lifecycle_for(observation.key, episodes_by_key, now)
        deviation = (
            observation.raw_spread_bps - float(basis["mean_bps"])
            if isinstance(basis["mean_bps"], (int, float))
            else None
        )
        round_trip_fee_bps = 2.0 * (observation.long_fee_bps + observation.short_fee_bps)
        return {
            **self._identity_payload(observation.key),
            "current_raw_spread_bps": observation.raw_spread_bps,
            "rolling_mean_bps": basis["mean_bps"],
            "rolling_std_bps": basis["std_bps"],
            "deviation_bps": deviation,
            "basis_eligible": basis["eligible"],
            "signal_duration_seconds": lifecycle["signal_duration_seconds"],
            "round_trip_fee_bps": round_trip_fee_bps,
            "theoretical_edge_bps": (
                deviation - round_trip_fee_bps
                if isinstance(deviation, (int, float))
                else None
            ),
            "observed_at_skew_seconds": observation.observed_at_skew_seconds,
            "sample_time": observation.sample_time.isoformat(),
            "freshness_seconds": observation.freshness_seconds,
            "long_buy_vwap": observation.long_buy_vwap,
            "short_sell_vwap": observation.short_sell_vwap,
            "long_fee_bps": observation.long_fee_bps,
            "short_fee_bps": observation.short_fee_bps,
            "active": lifecycle["active"],
            "alerted": lifecycle["alerted"],
            "candidate_confirmed": lifecycle["candidate_confirmed"],
        }

    def _current_payload(
        self,
        observation: _PairObservation,
        basis: dict[str, object],
        lifecycle: dict[str, object],
    ) -> dict[str, object]:
        mean = basis["mean_bps"]
        deviation = (
            observation.raw_spread_bps - float(mean)
            if isinstance(mean, (int, float))
            else None
        )
        round_trip_fee = 2.0 * (observation.long_fee_bps + observation.short_fee_bps)
        theoretical_edge = (
            deviation - round_trip_fee if isinstance(deviation, (int, float)) else None
        )
        return {
            "sample_time": observation.sample_time.isoformat(),
            "raw_spread_bps": observation.raw_spread_bps,
            "rolling_mean_bps": mean,
            "rolling_std_bps": basis["std_bps"],
            "deviation_bps": deviation,
            "long_buy_vwap": observation.long_buy_vwap,
            "short_sell_vwap": observation.short_sell_vwap,
            "long_fee_bps": observation.long_fee_bps,
            "short_fee_bps": observation.short_fee_bps,
            "round_trip_fee_bps": round_trip_fee,
            "theoretical_edge_bps": theoretical_edge,
            "observed_at_skew_seconds": observation.observed_at_skew_seconds,
            "freshness_seconds": observation.freshness_seconds,
            "signal_duration_seconds": lifecycle["signal_duration_seconds"],
        }

    def _basis_payload_for(
        self,
        observations: Iterable[_PairObservation],
        current: _PairObservation | None,
    ) -> dict[str, object]:
        if current is None:
            return self._unavailable_basis()
        matching = [
            observation
            for observation in observations
            if observation.key == current.key
            and observation.valid_for_history
            and observation.observed_at < current.observed_at
            and observation.sample_time < current.sample_time
            and observation.sample_time >= current.sample_time - timedelta(seconds=ROLLING_WINDOW_SECONDS)
        ]
        try:
            squares = math.fsum(point.raw_spread_bps ** 2 for point in matching)
        except OverflowError:
            return self._unavailable_basis()
        if not math.isfinite(squares):
            return self._unavailable_basis()
        basis = RollingBasis(
            window_seconds=ROLLING_WINDOW_SECONDS,
            min_observations=MIN_HISTORY_OBSERVATIONS,
            expected_interval_seconds=EXPECTED_INTERVAL_SECONDS,
        )
        basis.hydrate(
            (observation.sample_time, observation.raw_spread_bps)
            for observation in matching
        )
        try:
            stats = basis.stats_before(current.sample_time)
        except ValueError:
            return self._unavailable_basis()
        return self._basis_stats_payload(stats)

    @staticmethod
    def _basis_stats_payload(stats: RollingBasisStats) -> dict[str, object]:
        eligible = (
            stats.eligible
            and _finite_float(stats.mean_bps) is not None
            and _finite_float(stats.std_bps) is not None
        )
        return {
            "sample_count": stats.sample_count,
            "coverage": stats.coverage,
            "mean_bps": stats.mean_bps if eligible else None,
            "std_bps": stats.std_bps if eligible else None,
            "eligible": eligible,
            "min_observations": stats.min_observations,
        }

    @staticmethod
    def _unavailable_basis() -> dict[str, object]:
        return {
            "sample_count": 0,
            "coverage": 0.0,
            "mean_bps": None,
            "std_bps": None,
            "eligible": False,
            "min_observations": MIN_HISTORY_OBSERVATIONS,
        }

    def _read_runtime(self, now: datetime) -> _RuntimeRead:
        heartbeat: dict[str, object] = {
            "status": "down",
            "updated_at": None,
            "age_seconds": None,
        }
        unavailable: dict[str, object] = {"status": "unavailable"}
        try:
            connection = sqlite3.connect(
                self.runtime_db.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=0.2,
            )
        except sqlite3.Error as error:
            return _RuntimeRead(
                heartbeat=heartbeat,
                episodes=[],
                episodes_by_key={},
                recent_events=[],
                sqlite=unavailable,
                errors=[
                    f"runtime state read failed: {type(error).__name__}: {error}"
                ],
            )

        errors: list[str] = []
        try:
            # Set before any database access: WAL must not map/write a shared
            # wal-index. If read-only exclusive access is impossible, fail
            # explicitly instead of ignoring WAL via immutable=1.
            connection.execute("PRAGMA locking_mode=EXCLUSIVE")
            state_row = connection.execute(
                """
                SELECT state_json, updated_at
                FROM monitor_state
                WHERE monitor_name = 'spread' AND state_key = 'episodes'
                """
            ).fetchone()
            episodes_raw: object = {}
            if state_row is not None:
                updated_at = _parse_timestamp(state_row[1])
                if updated_at is None:
                    errors.append("monitor state read failed: invalid updated_at")
                else:
                    age_seconds = (now - updated_at).total_seconds()
                    heartbeat = {
                        "status": classify_heartbeat(age_seconds if age_seconds >= 0 else None),
                        "updated_at": updated_at.isoformat(),
                        "age_seconds": age_seconds if age_seconds >= 0 else None,
                    }
                try:
                    episodes_raw = json.loads(state_row[0])
                except (TypeError, ValueError, json.JSONDecodeError) as error:
                    errors.append(
                        f"monitor state read failed: {type(error).__name__}: {error}"
                    )
            episodes, episodes_by_key = self._parse_episodes(episodes_raw, errors, now)
            events = self._read_events(connection, errors)
        except sqlite3.Error as error:
            return _RuntimeRead(
                heartbeat=heartbeat,
                episodes=[],
                episodes_by_key={},
                recent_events=[],
                sqlite=unavailable,
                errors=[
                    f"runtime state read failed: {type(error).__name__}: {error}"
                ],
            )
        finally:
            connection.close()
        return _RuntimeRead(
            heartbeat=heartbeat,
            episodes=episodes,
            episodes_by_key=episodes_by_key,
            recent_events=events,
            sqlite={"status": "healthy"},
            errors=errors,
        )

    @staticmethod
    def _parse_episodes(
        raw_episodes: object,
        errors: list[str],
        now: datetime,
    ) -> tuple[
        list[dict[str, object]],
        dict[tuple[str, str, str, str, str], dict[str, object]],
    ]:
        if not isinstance(raw_episodes, dict):
            if raw_episodes not in ({}, None):
                errors.append("invalid episode state ignored")
            return [], {}
        episodes: list[dict[str, object]] = []
        by_key: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
        ambiguous: set[tuple[str, str, str, str, str]] = set()
        for raw_episode in raw_episodes.values():
            if not isinstance(raw_episode, dict):
                errors.append("invalid episode state ignored")
                continue
            raw_key = raw_episode.get("key")
            candidate = raw_episode.get("candidate")
            if not isinstance(raw_key, dict) or not isinstance(candidate, dict):
                errors.append("invalid episode state ignored")
                continue
            identity = _parse_identity(raw_key)
            if identity is None:
                errors.append("invalid episode identity ignored")
                continue
            condition_since = _parse_timestamp(raw_episode.get("alert_condition_since"))
            last_seen_at = _parse_timestamp(raw_episode.get("last_seen_at"))
            first_seen_at = _parse_timestamp(raw_episode.get("first_seen_at"))
            if raw_episode.get("alert_condition_since") is not None and condition_since is None:
                errors.append("invalid episode alert_condition_since ignored")
                continue
            if (
                first_seen_at is None or last_seen_at is None
                or not first_seen_at <= last_seen_at <= now
                or (condition_since is not None and not first_seen_at <= condition_since <= last_seen_at)
                or not isinstance(raw_episode.get("candidate_confirmed"), bool)
                or not isinstance(raw_episode.get("alerted"), bool)
                or identity[1].lower() == identity[3].lower()
            ):
                errors.append("invalid episode lifecycle ignored")
                continue
            entry: dict[str, object] = {
                "episode_id": _text(raw_episode.get("episode_id")),
                "key": identity,
                "symbol": identity[0],
                "long_venue": identity[1],
                "long_venue_symbol": identity[2],
                "short_venue": identity[3],
                "short_venue_symbol": identity[4],
                "net_spread_bps": _finite_float(candidate.get("net_spread_bps")),
                "first_seen_at": _iso(first_seen_at),
                "last_seen_at": _iso(last_seen_at),
                "alert_condition_since": _iso(condition_since),
                "candidate_confirmed": raw_episode.get("candidate_confirmed") is True,
                "alerted": raw_episode.get("alerted") is True,
            }
            episodes.append(entry)
            if identity in by_key:
                ambiguous.add(identity)
            else:
                by_key[identity] = entry
        for identity in ambiguous:
            by_key.pop(identity, None)
            errors.append("ambiguous persisted episode identity ignored")
        episodes.sort(key=DashboardQueryService._episode_sort_key)
        return episodes, by_key

    @staticmethod
    def _episode_sort_key(episode: dict[str, object]) -> tuple[float, str, str, str]:
        spread = episode.get("net_spread_bps")
        return (
            -float(spread) if isinstance(spread, (int, float)) else math.inf,
            str(episode.get("symbol") or ""),
            str(episode.get("long_venue") or ""),
            str(episode.get("short_venue") or ""),
        )

    @staticmethod
    def _read_events(
        connection: sqlite3.Connection,
        errors: list[str],
    ) -> list[dict[str, object]]:
        try:
            rows = connection.execute(
                """
                SELECT event_id, event_type, event_json, occurred_at
                FROM opportunity_log
                WHERE monitor_name = 'spread'
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT 20
                """
            ).fetchall()
        except sqlite3.Error as error:
            errors.append(
                f"opportunity log read failed: {type(error).__name__}: {error}"
            )
            return []
        events: list[dict[str, object]] = []
        for event_id, event_type, event_json, occurred_at in rows:
            try:
                event = json.loads(event_json)
            except (TypeError, ValueError, json.JSONDecodeError) as error:
                event = {}
                errors.append(f"event JSON read failed: {type(error).__name__}: {error}")
            if not isinstance(event, dict):
                errors.append("event JSON object expected; using empty event")
                event = {}
            occurred = _parse_timestamp(occurred_at)
            events.append(
                {
                    "event_id": event_id,
                    "event_type": event_type,
                    "occurred_at": _iso(occurred) or occurred_at,
                    "symbol": _text(event.get("canonical_symbol")),
                    "long_venue": _text(event.get("long_venue")),
                    "long_venue_symbol": _text(event.get("long_venue_symbol")),
                    "short_venue": _text(event.get("short_venue")),
                    "short_venue_symbol": _text(event.get("short_venue_symbol")),
                    "net_spread_bps": _finite_float(event.get("net_spread_bps")),
                }
            )
        return events

    def _read_v2_runtime(
        self,
        now: datetime,
    ) -> tuple[
        list[dict[str, object]],
        dict[tuple[str, str, str, str, str], dict[str, object]],
        dict[str, dict[str, object]],
        list[dict[str, object]],
        list[str],
    ]:
        if not self.config.monitors.spread.anomaly_v2.enabled:
            return [], {}, {}, [], []
        try:
            connection = sqlite3.connect(
                self.runtime_db.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=0.2,
            )
        except sqlite3.Error as error:
            return [], {}, {}, [], [
                f"anomaly state read failed: {type(error).__name__}: {error}"
            ]
        errors: list[str] = []
        active: list[dict[str, object]] = []
        by_key: dict[tuple[str, str, str, str, str], dict[str, object]] = {}
        notifications: dict[str, dict[str, object]] = {}
        events: list[dict[str, object]] = []
        try:
            state_rows = connection.execute(
                """
                SELECT state_key, state_json
                FROM monitor_state
                WHERE monitor_name = 'spread'
                  AND state_key IN ('anomaly_episodes_v2', 'anomaly_notifications_v2')
                """
            ).fetchall()
            raw_episode_state: object = {}
            raw_notification_state: object = {}
            for state_key, state_json in state_rows:
                try:
                    parsed = json.loads(state_json)
                except (TypeError, ValueError, json.JSONDecodeError):
                    errors.append(f"invalid {state_key} JSON ignored")
                    continue
                if state_key == "anomaly_episodes_v2":
                    raw_episode_state = parsed
                else:
                    raw_notification_state = parsed
            if isinstance(raw_notification_state, dict):
                notifications = {
                    str(key): dict(value)
                    for key, value in raw_notification_state.items()
                    if isinstance(value, dict)
                }
            if isinstance(raw_episode_state, dict):
                for raw_wrapper in raw_episode_state.values():
                    try:
                        if not isinstance(raw_wrapper, dict):
                            raise TypeError("wrapper")
                        episode = raw_wrapper["episode"]
                        candidate = raw_wrapper["candidate"]
                        if not isinstance(episode, dict) or not isinstance(candidate, dict):
                            raise TypeError("episode")
                        pair = episode["pair_key"]
                        if not isinstance(pair, dict):
                            raise TypeError("pair")
                        identity_values = _parse_identity(pair)
                        if identity_values is None:
                            raise ValueError("identity")
                        confirmed_at = _parse_timestamp(episode.get("confirmed_at"))
                        ended_at = _parse_timestamp(episode.get("ended_at"))
                        if confirmed_at is None or ended_at is not None:
                            continue
                        episode_id = episode["episode_id"]
                        notification = notifications.get(str(episode_id), {})
                        context = notification.get("context")
                        context_values = context if isinstance(context, dict) else {}
                        entry: dict[str, object] = {
                            "episode_id": episode_id,
                            "key": identity_values,
                            "symbol": identity_values[0],
                            "canonical_symbol": identity_values[0],
                            "long_venue": identity_values[1],
                            "long_venue_symbol": identity_values[2],
                            "short_venue": identity_values[3],
                            "short_venue_symbol": identity_values[4],
                            "candidate_started_at": episode.get("candidate_started_at"),
                            "confirmed_at": episode.get("confirmed_at"),
                            "last_seen_at": episode.get("last_seen_at"),
                            "reference_mean_bps": _finite_float(episode.get("reference_mean_bps")),
                            "reference_std_bps": _finite_float(episode.get("reference_std_bps")),
                            "current_spread_bps": _finite_float(episode.get("current_spread_bps")),
                            "current_deviation_bps": _finite_float(episode.get("current_deviation_from_reference_bps")),
                            "confirmation_deviation_bps": _finite_float(episode.get("confirmation_deviation_bps")),
                            "post_confirmation_peak_deviation_bps": _finite_float(episode.get("post_confirmation_peak_deviation_bps")),
                            "post_confirmation_peak_at": episode.get("post_confirmation_peak_at"),
                            "long_buy_vwap": _finite_float(candidate.get("long_buy_vwap")),
                            "short_sell_vwap": _finite_float(candidate.get("short_sell_vwap")),
                            "raw_spread_bps": _finite_float(candidate.get("raw_spread_bps")),
                            "long_fee_bps": _finite_float(candidate.get("long_fee_bps")),
                            "short_fee_bps": _finite_float(candidate.get("short_fee_bps")),
                            "tg_eligible": notification.get("eligibility"),
                            "eligibility_reason": notification.get("eligibility_reason"),
                            "mean_alignment_bps": notification.get("mean_alignment_bps"),
                            "initial_sent_at": notification.get("initial_sent_at"),
                            "mean_12h_bps": context_values.get("12h_mean_bps"),
                            "mean_24h_bps": context_values.get("24h_mean_bps"),
                            "mean_48h_bps": context_values.get("48h_mean_bps"),
                        }
                        identity = identity_values
                        active.append(entry)
                        if identity in by_key:
                            by_key.pop(identity, None)
                            errors.append("ambiguous anomaly pair identity ignored")
                        else:
                            by_key[identity] = entry
                    except (KeyError, TypeError, ValueError):
                        errors.append("invalid anomaly episode state ignored")
            event_rows = connection.execute(
                """
                SELECT event_id, event_type, event_json, occurred_at
                FROM opportunity_log
                WHERE monitor_name = 'spread' AND event_type LIKE 'anomaly_%'
                ORDER BY occurred_at DESC, event_id DESC
                LIMIT 200
                """
            ).fetchall()
            for event_id, event_type, event_json, occurred_at in event_rows:
                try:
                    payload = json.loads(event_json)
                    if not isinstance(payload, dict):
                        raise ValueError("event")
                    event_identity = _parse_identity(payload)
                    if event_identity is None:
                        continue
                    events.append(
                        {
                            "event_id": event_id,
                            "event_type": event_type,
                            "payload": payload,
                            "occurred_at": occurred_at,
                            "identity": event_identity,
                        }
                    )
                except (TypeError, ValueError, json.JSONDecodeError):
                    errors.append("invalid anomaly event ignored")
        except sqlite3.Error as error:
            errors.append(f"anomaly state read failed: {type(error).__name__}: {error}")
            active, by_key, notifications, events = [], {}, {}, []
        finally:
            connection.close()
        active.sort(
            key=lambda row: (
                str(row.get("symbol") or ""),
                str(row.get("long_venue") or ""),
                str(row.get("long_venue_symbol") or ""),
                str(row.get("short_venue") or ""),
                str(row.get("short_venue_symbol") or ""),
            )
        )
        return active, by_key, notifications, events, errors

    @staticmethod
    def _identity_payload(key: SpreadPairKey) -> dict[str, str]:
        return {
            "canonical_symbol": key.canonical_symbol,
            "long_venue": key.long_venue,
            "long_venue_symbol": key.long_venue_symbol,
            "short_venue": key.short_venue,
            "short_venue_symbol": key.short_venue_symbol,
        }

    @staticmethod
    def _unavailable_lifecycle() -> dict[str, object]:
        return {
            "available": False,
            "active": False,
            "candidate_confirmed": False,
            "alerted": False,
            "signal_duration_seconds": None,
            "alert_condition_since": None,
            "last_seen_at": None,
            "episode_id": None,
        }

    @staticmethod
    def _lifecycle_for(
        key: SpreadPairKey,
        episodes_by_key: Mapping[tuple[str, str, str, str, str], dict[str, object]],
        now: datetime,
    ) -> dict[str, object]:
        episode = episodes_by_key.get(
            (
                key.canonical_symbol,
                key.long_venue,
                key.long_venue_symbol,
                key.short_venue,
                key.short_venue_symbol,
            )
        )
        if episode is None:
            return DashboardQueryService._unavailable_lifecycle()
        since = _parse_timestamp(episode.get("alert_condition_since"))
        duration = None
        if isinstance(since, datetime):
            duration = max(0, int((now - since).total_seconds()))
        return {
            "available": True,
            "active": True,
            "candidate_confirmed": episode["candidate_confirmed"],
            "alerted": episode["alerted"],
            "signal_duration_seconds": duration,
            "alert_condition_since": _iso(since) if isinstance(since, datetime) else None,
            "last_seen_at": episode.get("last_seen_at"),
            "episode_id": episode.get("episode_id"),
        }

    def _anomaly_lifecycle_for(
        self,
        key: SpreadPairKey,
        episodes_by_key: Mapping[tuple[str, str, str, str, str], dict[str, object]],
        events: list[dict[str, object]],
        now: datetime,
    ) -> dict[str, object]:
        identity = (
            key.canonical_symbol,
            key.long_venue,
            key.long_venue_symbol,
            key.short_venue,
            key.short_venue_symbol,
        )
        episode = episodes_by_key.get(identity)
        if episode is not None:
            confirmed = _parse_timestamp(episode.get("confirmed_at"))
            started = _parse_timestamp(episode.get("candidate_started_at"))
            duration = None
            if confirmed is not None:
                duration = max(0, int((now - confirmed).total_seconds()))
            eligible = episode.get("tg_eligible")
            if eligible is True:
                tg_status = "Eligible / sent" if episode.get("initial_sent_at") else "Eligible / pending"
            elif episode.get("eligibility_reason") == "mean_alignment":
                tg_status = "Suppressed: alignment"
            elif episode.get("eligibility_reason") == "context_unavailable":
                tg_status = "Suppressed: context unavailable"
            else:
                tg_status = "Pending"
            return {
                **episode,
                "available": True,
                "active": True,
                "candidate_confirmed": True,
                "alerted": episode.get("initial_sent_at") is not None,
                "signal_duration_seconds": duration,
                "alert_condition_since": _iso(confirmed),
                "last_seen_at": episode.get("last_seen_at"),
                "duration_seconds": duration,
                "episode_age_seconds": (
                    max(0, int((now - started).total_seconds()))
                    if started is not None
                    else None
                ),
                "tg_status": tg_status,
            }
        for event in events:
            if event.get("event_type") != "anomaly_resolved":
                continue
            if _parse_identity(event.get("identity")) != identity:
                continue
            payload = event.get("payload")
            if not isinstance(payload, dict):
                continue
            return {
                "available": True,
                "active": False,
                "candidate_confirmed": True,
                "alerted": False,
                "signal_duration_seconds": None,
                "alert_condition_since": None,
                "last_seen_at": payload.get("ended_at"),
                "episode_id": payload.get("episode_id"),
                "resolution_reason": payload.get("resolution_reason"),
                "candidate_started_at": payload.get("candidate_started_at"),
                "confirmed_at": payload.get("confirmed_at"),
                "ended_at": payload.get("ended_at"),
                "reference_mean_bps": payload.get("reference_mean_bps"),
                "current_spread_bps": payload.get("current_spread_bps"),
                "current_deviation_bps": payload.get("current_deviation_bps"),
                "confirmation_deviation_bps": payload.get("confirmation_deviation_bps"),
                "post_confirmation_peak_deviation_bps": payload.get(
                    "post_confirmation_peak_deviation_bps"
                ),
                "post_confirmation_peak_at": payload.get("post_confirmation_peak_at"),
                "tg_status": "Resolved",
            }
        return {"available": False, "active": False, "episode_id": None}

    @staticmethod
    def _anomaly_markers_for(
        key: SpreadPairKey,
        events: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        identity = (
            key.canonical_symbol,
            key.long_venue,
            key.long_venue_symbol,
            key.short_venue,
            key.short_venue_symbol,
        )
        return [
            {
                "event_type": event.get("event_type"),
                "occurred_at": event.get("occurred_at"),
                "payload": event.get("payload"),
            }
            for event in events
            if _parse_identity(event.get("identity")) == identity
        ]

    @staticmethod
    def _matches_filters(row: dict[str, object], filters: OpportunitiesFilters) -> bool:
        if filters.symbol is not None and row["canonical_symbol"] != filters.symbol:
            return False
        if filters.long_venue is not None and row["long_venue"] != filters.long_venue:
            return False
        if filters.short_venue is not None and row["short_venue"] != filters.short_venue:
            return False
        if filters.max_std_bps is not None:
            value = row["rolling_std_bps"]
            if not isinstance(value, (int, float)) or value > filters.max_std_bps:
                return False
        if filters.min_deviation_bps is not None:
            value = row["deviation_bps"]
            if not isinstance(value, (int, float)) or value < filters.min_deviation_bps:
                return False
        if filters.min_duration_seconds is not None:
            value = row["signal_duration_seconds"]
            if not isinstance(value, int) or value < filters.min_duration_seconds:
                return False
        return not filters.active_only or row["active"] is True

    @staticmethod
    def _opportunity_sort_key(row: dict[str, object]) -> tuple[bool, float, tuple[str, ...]]:
        deviation = row.get("deviation_bps")
        numeric = float(deviation) if isinstance(deviation, (int, float)) else 0.0
        identity = tuple(
            str(row.get(field) or "")
            for field in (
                "canonical_symbol",
                "long_venue",
                "long_venue_symbol",
                "short_venue",
                "short_venue_symbol",
            )
        )
        return (deviation is None, -numeric, identity)

    @staticmethod
    def _range_start(range_name: PairRange, now: datetime) -> datetime:
        seconds = {
            "1h": 60 * 60,
            "6h": 6 * 60 * 60,
            "24h": 24 * 60 * 60,
            "3d": 3 * 24 * 60 * 60,
            "7d": 7 * 24 * 60 * 60,
            "all": 90 * 24 * 60 * 60,
        }[range_name]
        return now - timedelta(seconds=seconds)

    @staticmethod
    def _downsample_points(
        points: list[_PairObservation],
    ) -> list[_PairObservation]:
        if len(points) <= MAX_DISPLAY_POINTS:
            return points
        last_index = len(points) - 1
        selected: set[int] = {0, last_index}
        selected.update(
            (
                min(range(len(points)), key=lambda index: points[index].raw_spread_bps),
                max(range(len(points)), key=lambda index: points[index].raw_spread_bps),
            )
        )

        extrema: list[tuple[float, int]] = []
        for index in range(1, last_index):
            value = points[index].raw_spread_bps
            previous = points[index - 1].raw_spread_bps
            following = points[index + 1].raw_spread_bps
            if (value > previous and value > following) or (
                value < previous and value < following
            ):
                prominence = min(abs(value - previous), abs(value - following))
                extrema.append((prominence, index))
        extrema.sort(key=lambda item: (-item[0], item[1]))
        for _, index in extrema:
            if len(selected) >= MAX_DISPLAY_POINTS:
                break
            selected.add(index)

        for position in range(MAX_DISPLAY_POINTS):
            if len(selected) >= MAX_DISPLAY_POINTS:
                break
            index = round(position * last_index / (MAX_DISPLAY_POINTS - 1))
            selected.add(index)
        return [points[index] for index in sorted(selected)]

    def _rolling_mean_series(
        self,
        all_points: list[_PairObservation],
        display_points: list[_PairObservation],
    ) -> list[dict[str, object]]:
        if not all_points or not display_points:
            return []
        means: dict[datetime, float | None] = {}
        basis = RollingBasis(
            window_seconds=ROLLING_WINDOW_SECONDS,
            min_observations=MIN_HISTORY_OBSERVATIONS,
            expected_interval_seconds=EXPECTED_INTERVAL_SECONDS,
        )
        display_times = {point.sample_time for point in display_points}
        prior_points: deque[_PairObservation] = deque()
        latest_prior_observed_at: datetime | None = None
        for point in all_points:
            cutoff = point.sample_time - timedelta(seconds=ROLLING_WINDOW_SECONDS)
            while prior_points and prior_points[0].sample_time < cutoff:
                prior_points.popleft()
            valid = point.valid_for_history and math.isfinite(point.raw_spread_bps * point.raw_spread_bps)
            stats = (
                basis.observe(point.sample_time, point.raw_spread_bps)
                if valid
                else basis.stats_before(point.sample_time)
            )
            if point.sample_time in display_times:
                # A delayed prior slot can arrive after this observation.
                # Recheck only that bounded window when availability overlaps.
                payload = (
                    self._basis_payload_for(prior_points, point)
                    if latest_prior_observed_at is not None
                    and latest_prior_observed_at >= point.observed_at
                    else self._basis_stats_payload(stats)
                )
                mean_value = payload.get("mean_bps")
                means[point.sample_time] = (
                    float(mean_value)
                    if isinstance(mean_value, (int, float)) and not isinstance(mean_value, bool)
                    else None
                )
            if valid:
                prior_points.append(point)
                latest_prior_observed_at = max(
                    point.observed_at, latest_prior_observed_at or point.observed_at
                )
        # Mean availability can break even when raw observations are continuous.
        # Mark those breaks before a null point can be omitted by downsampling.
        segments: dict[datetime, int] = {}
        segment = 0
        for index, point in enumerate(display_points):
            if index:
                previous = display_points[index - 1]
                if (
                    (point.sample_time - previous.sample_time).total_seconds() > EXPECTED_INTERVAL_SECONDS
                    or (means.get(point.sample_time) is None) != (means.get(previous.sample_time) is None)
                ):
                    segment += 1
            segments[point.sample_time] = segment
        return [
            {
                "sample_time": point.sample_time.isoformat(),
                "rolling_mean_bps": means.get(point.sample_time),
                "segment": segments[point.sample_time],
            }
            for point in self._downsample_points(display_points)
        ]
