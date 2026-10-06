from __future__ import annotations

from bisect import bisect_right
from collections import deque
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import math
from pathlib import Path
from typing import Any

import duckdb

from radar.config import RadarConfig
from radar.monitors.manual_opportunity import (
    ManualOpportunityLifecycle,
    ManualOpportunityObservation,
    manual_opportunity_rejection_reason,
    manual_opportunity_temporal_rejection_reason,
)
from radar.monitors.spread.models import SpreadPairKey


@dataclass(frozen=True)
class BboWindowStats:
    """Prior-only statistics for one BBO spread window."""

    sample_count: int
    coverage: float
    mean_bps: float | None
    available: bool


@dataclass
class BboHistoryMutation:
    """Minimal inverse operation for one rolling-history observation."""

    history: BboRollingHistory
    previous_last_sample_time: datetime | None
    appended_timestamp: datetime
    appended_value: float
    removed_current: dict[str, tuple[datetime, float] | None]
    pruned_points: dict[str, tuple[tuple[datetime, float], ...]]
    _rolled_back: bool = False

    def rollback(self) -> None:
        if self._rolled_back:
            return
        for name in self.history.windows_seconds:
            points = self.history._points[name]
            if not points or points[-1] != (
                self.appended_timestamp,
                self.appended_value,
            ):
                raise RuntimeError("rolling history changed before rollback")
            points.pop()
            self.history._sums[name] -= self.appended_value
            pruned = self.pruned_points[name]
            points.extendleft(reversed(pruned))
            self.history._sums[name] += sum(value for _, value in pruned)
            removed = self.removed_current[name]
            if removed is not None:
                points.append(removed)
                self.history._sums[name] += removed[1]
        self.history._last_sample_time = self.previous_last_sample_time
        self._rolled_back = True


def _as_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _finite(value: float, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field_name} must be finite")
    return result


class BboRollingHistory:
    """Bounded, strictly-prior BBO history for one directional pair.

    ``observe`` calculates each window before adding the current sample.  A
    timestamp is a single observation key, so a late replacement cannot make
    the current sample leak into its own baseline.
    """

    def __init__(
        self,
        *,
        windows_seconds: Mapping[str, int] | None = None,
        expected_interval_seconds: int = 10,
        minimum_coverage: float = 0.8,
    ) -> None:
        self.windows_seconds = dict(
            windows_seconds
            or {"2h": 2 * 60 * 60, "24h": 24 * 60 * 60, "3d": 3 * 24 * 60 * 60}
        )
        if not self.windows_seconds or any(
            not isinstance(name, str) or not name or seconds <= 0
            for name, seconds in self.windows_seconds.items()
        ):
            raise ValueError("windows_seconds must contain positive named windows")
        if expected_interval_seconds <= 0:
            raise ValueError("expected_interval_seconds must be positive")
        if not 0 <= minimum_coverage <= 1:
            raise ValueError("minimum_coverage must be between 0 and 1")
        self.expected_interval_seconds = expected_interval_seconds
        self.minimum_coverage = minimum_coverage
        self._points: dict[str, deque[tuple[datetime, float]]] = {
            name: deque() for name in self.windows_seconds
        }
        self._sums: dict[str, float] = {name: 0.0 for name in self.windows_seconds}
        self._last_sample_time: datetime | None = None

    def observe(
        self, sample_time: datetime, raw_spread_bps: float
    ) -> dict[str, BboWindowStats]:
        results, _mutation = self.observe_with_rollback(sample_time, raw_spread_bps)
        return results

    def observe_with_rollback(
        self, sample_time: datetime, raw_spread_bps: float
    ) -> tuple[dict[str, BboWindowStats], BboHistoryMutation]:
        timestamp = _as_utc(sample_time, "sample_time")
        value = _finite(raw_spread_bps, "raw_spread_bps")
        if (
            self._last_sample_time is not None
            and timestamp < self._last_sample_time
        ):
            raise ValueError("sample_time must not move backwards")

        previous_last_sample_time = self._last_sample_time
        removed_current: dict[str, tuple[datetime, float] | None] = {}
        pruned_points: dict[str, tuple[tuple[datetime, float], ...]] = {}
        results: dict[str, BboWindowStats] = {}
        for name, window_seconds in self.windows_seconds.items():
            points = self._points[name]
            if points and points[-1][0] == timestamp:
                removed_current[name] = points.pop()
            else:
                removed_current[name] = None
            cutoff = timestamp - timedelta(seconds=window_seconds)
            pruned_points[name] = tuple(self._prune(name, cutoff))
            count = len(points)
            expected_slots = window_seconds / self.expected_interval_seconds
            coverage = min(1.0, count / expected_slots) if expected_slots else 0.0
            mean = self._sums[name] / count if count else None
            oldest = points[0][0] if points else None
            required_count = math.ceil(expected_slots * self.minimum_coverage)
            covers_window_start = (
                self.minimum_coverage == 0
                or oldest is not None
                and oldest <= cutoff
            )
            available = (
                mean is not None
                and count >= required_count
                and covers_window_start
            )
            results[name] = BboWindowStats(
                sample_count=count,
                coverage=coverage,
                mean_bps=mean if available else None,
                available=available,
            )

        self._append(timestamp, value)
        return results, BboHistoryMutation(
            history=self,
            previous_last_sample_time=previous_last_sample_time,
            appended_timestamp=timestamp,
            appended_value=value,
            removed_current=removed_current,
            pruned_points=pruned_points,
        )

    def hydrate(self, points: list[tuple[datetime, float]]) -> None:
        """Replace the bounded history with sorted, de-duplicated points."""
        for name in self.windows_seconds:
            self._points[name].clear()
            self._sums[name] = 0.0
        self._last_sample_time = None
        values: dict[datetime, float] = {}
        for timestamp, value in points:
            values[_as_utc(timestamp, "sample_time")] = _finite(
                value, "raw_spread_bps"
            )
        for timestamp in sorted(values):
            self._append(timestamp, values[timestamp])

    def _append(self, timestamp: datetime, value: float) -> None:
        for name in self.windows_seconds:
            self._points[name].append((timestamp, value))
            self._sums[name] += value
        self._last_sample_time = timestamp

    def _prune(self, name: str, cutoff: datetime) -> list[tuple[datetime, float]]:
        points = self._points[name]
        removed: list[tuple[datetime, float]] = []
        while points and points[0][0] < cutoff:
            removed.append(points.popleft())
            _timestamp, value = removed[-1]
            self._sums[name] -= value
        return removed


@dataclass(frozen=True)
class ManualReplayResult:
    report: dict[str, object]


@dataclass(frozen=True)
class _ReplayMarketRow:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    canonical_symbol: str
    best_bid: float
    best_ask: float


@dataclass(frozen=True)
class _ReplayVolume:
    observed_at: datetime
    sample_time: datetime
    volume_24h: float | None


@dataclass(frozen=True)
class _ReplayVolumeSeries:
    observed_times: tuple[datetime, ...]
    values: tuple[_ReplayVolume, ...]


def replay_manual_opportunity(
    *,
    data_root: Path,
    config: RadarConfig,
    start: datetime,
    end: datetime,
    output_dir: Path | None = None,
) -> ManualReplayResult:
    """Replay Manual Opportunity v1 from persisted BBO and hourly data.

    The market query is ordered and consumed as bounded Arrow batches.  Only
    one sample slot, route histories, and the small hourly-volume index remain
    in Python memory.  No runtime store or production application object is
    used by this read-only replay.
    """
    start_utc = _as_utc(start, "start")
    end_utc = _as_utc(end, "end")
    if end_utc <= start_utc:
        raise ValueError("end must be after start")
    warmup_start = start_utc - timedelta(days=3)
    market_root = _dataset_root(data_root, "market")
    if market_root is None:
        raise FileNotFoundError("market Parquet dataset is missing")
    context_glob = _dataset_glob(data_root, "hourly_context")
    allowed_feeds = {
        (market.venue, market.venue_symbol, market.canonical_symbol)
        for market in config.markets
        if market.enabled
    }
    if not allowed_feeds:
        raise ValueError("config contains no enabled market feeds")

    volumes = _load_volume_index(context_glob, warmup_start, end_utc)
    histories: dict[SpreadPairKey, BboRollingHistory] = {}
    lifecycle = ManualOpportunityLifecycle(config.manual_opportunity, config.fees_bps)
    counters: dict[str, int] = {
        "baseline_history_available": 0,
        "stable_baseline": 0,
        "expected_net_at_least_10": 0,
        "volume_at_least_1m": 0,
        "persistence_broken": 0,
        "data_gap": 0,
        "insufficient_baseline_history": 0,
        "unstable_baseline": 0,
        "expected_net_below_min": 0,
        "volume_below_min": 0,
        "volume_unavailable": 0,
        "bbo_freshness_rejected": 0,
        "future_bbo": 0,
        "stale_bbo": 0,
        "future_sample": 0,
    }
    alert_payloads: list[dict[str, object]] = []
    unique_routes: set[SpreadPairKey] = set()
    route_sample_count = 0
    sample_slots = 0
    oversized_spreads = 0
    largest_abs_spread = 0.0
    invalid_bbo_rows = 0
    qqq_inspection: list[dict[str, object]] = []
    requested_start = start_utc
    observed_after_sample_rows = 0
    oversized_fresh_observations = 0
    oversized_candidate_episode_ids: set[str] = set()
    oversized_confirmed_episode_ids: set[str] = set()

    def process_slot(
        rows: list[_ReplayMarketRow],
        empty_sample_time: datetime | None = None,
    ) -> None:
        nonlocal route_sample_count, sample_slots, oversized_spreads
        nonlocal largest_abs_spread, invalid_bbo_rows
        nonlocal oversized_fresh_observations, observed_after_sample_rows
        if not rows:
            if empty_sample_time is not None and empty_sample_time >= requested_start:
                for key in lifecycle.active_keys:
                    lifecycle.observe_gap(key, empty_sample_time)
                    counters["data_gap"] += 1
                    counters["persistence_broken"] += 1
            return
        sample_time = rows[0].sample_time
        sample_slots += int(requested_start <= sample_time < end_utc)
        by_symbol: dict[str, dict[tuple[str, str], _ReplayMarketRow]] = {}
        for row in rows:
            feed_key = (row.venue, row.venue_symbol, row.canonical_symbol)
            if feed_key not in allowed_feeds:
                continue
            if row.observed_at > row.sample_time:
                observed_after_sample_rows += 1
            if not _valid_bbo(row.best_bid, row.best_ask):
                invalid_bbo_rows += 1
                continue
            by_symbol.setdefault(row.canonical_symbol, {})[
                (row.venue, row.venue_symbol)
            ] = row

        current_keys: set[SpreadPairKey] = set()

        for canonical_symbol, feeds in sorted(by_symbol.items()):
            for (long_venue, long_symbol), long_row in sorted(feeds.items()):
                for (short_venue, short_symbol), short_row in sorted(feeds.items()):
                    if long_venue.lower() == short_venue.lower():
                        continue
                    key = SpreadPairKey(
                        canonical_symbol=canonical_symbol,
                        long_venue=long_venue,
                        long_venue_symbol=long_symbol,
                        short_venue=short_venue,
                        short_venue_symbol=short_symbol,
                    )
                    current_keys.add(key)
                    unique_routes.add(key)
                    long_volume = _volume_as_of(
                        volumes,
                        (long_venue, long_symbol, canonical_symbol),
                        sample_time,
                    )
                    short_volume = _volume_as_of(
                        volumes,
                        (short_venue, short_symbol, canonical_symbol),
                        sample_time,
                    )
                    history = histories.setdefault(key, BboRollingHistory())
                    stats = history.observe(
                        sample_time,
                        (short_row.best_bid / long_row.best_ask - 1.0) * 10_000.0,
                    )
                    spread = (short_row.best_bid / long_row.best_ask - 1.0) * 10_000.0
                    largest_abs_spread = max(largest_abs_spread, abs(spread))
                    if abs(spread) > 1_000:
                        oversized_spreads += 1
                    if sample_time < requested_start:
                        continue
                    route_sample_count += 1
                    observation = ManualOpportunityObservation(
                        key=key,
                        sample_time=sample_time,
                        long_observed_at=long_row.observed_at,
                        short_observed_at=short_row.observed_at,
                        long_best_ask=long_row.best_ask,
                        short_best_bid=short_row.best_bid,
                        mean_2h_bps=stats["2h"].mean_bps,
                        mean_24h_bps=stats["24h"].mean_bps,
                        mean_3d_bps=stats["3d"].mean_bps,
                        long_volume_24h=long_volume,
                        short_volume_24h=short_volume,
                        long_fee_bps=_fee(config.fees_bps, long_venue),
                        short_fee_bps=_fee(config.fees_bps, short_venue),
                    )
                    availability_time = observation.available_at
                    temporal_reason = manual_opportunity_temporal_rejection_reason(
                        observation,
                        availability_time,
                    )
                    if temporal_reason is not None:
                        counters["bbo_freshness_rejected"] += 1
                        counters[temporal_reason] += 1
                    else:
                        _count_funnel(observation, config, counters)
                    reason = temporal_reason or manual_opportunity_rejection_reason(
                        observation, config.manual_opportunity
                    )
                    if abs(spread) > 1_000 and temporal_reason is None:
                        oversized_fresh_observations += 1
                    if reason is not None:
                        counters[reason] = counters.get(reason, 0) + 1
                    was_active = lifecycle.is_active(key)
                    alerts = lifecycle.evaluate(observation, now=availability_time)
                    active_episode = next(
                        (
                            episode
                            for episode in lifecycle.active_episodes
                            if episode.key == key
                        ),
                        None,
                    )
                    if abs(spread) > 1_000 and temporal_reason is None and active_episode is not None:
                        oversized_candidate_episode_ids.add(active_episode.episode_id)
                    if was_active and reason is not None and not lifecycle.is_active(key):
                        counters["persistence_broken"] += 1
                    for alert in alerts:
                        alert_payloads.append(dict(alert.payload))
                        if (
                            abs(spread) > 1_000
                            and alert.payload.get("event_kind") == "manual_initial"
                        ):
                            oversized_confirmed_episode_ids.add(alert.event_id.split(":initial")[0])
                    if (
                        canonical_symbol == "QQQ"
                        and long_venue.lower() == "arcus"
                        and short_venue.lower() == "lighter_robinhood"
                        and datetime(2026, 9, 28, 10, 30, tzinfo=UTC)
                        <= sample_time
                        <= datetime(2026, 9, 28, 10, 32, tzinfo=UTC)
                    ):
                        qqq_inspection.append(
                            {
                                "sample_time": sample_time.isoformat(),
                                "long_observed_at": long_row.observed_at.isoformat(),
                                "short_observed_at": short_row.observed_at.isoformat(),
                                "available_at": availability_time.isoformat(),
                                "current_spread_bps": spread,
                                "mean_2h_bps": stats["2h"].mean_bps,
                                "mean_24h_bps": stats["24h"].mean_bps,
                                "mean_3d_bps": stats["3d"].mean_bps,
                                "baseline_range_bps": observation.baseline_range_bps,
                                "expected_net_at_a_bps": observation.expected_net_at_a_bps,
                                "long_volume_24h": long_volume,
                                "short_volume_24h": short_volume,
                                "route_volume_24h": observation.route_volume_24h,
                                "rejection_reason": reason,
                                "temporal_rejection_reason": temporal_reason,
                            }
                        )

        if sample_time >= requested_start:
            for key in lifecycle.active_keys:
                if key not in current_keys:
                    lifecycle.observe_gap(key, sample_time)
                    counters["data_gap"] += 1
                    counters["persistence_broken"] += 1

    _stream_market_rows(
        market_root,
        warmup_start,
        end_utc,
        process_slot,
    )

    initial = [
        payload
        for payload in alert_payloads
        if payload.get("event_kind") == "manual_initial"
    ]
    expansions = [
        payload
        for payload in alert_payloads
        if payload.get("event_kind") == "manual_expansion"
    ]
    unique_episode_ids = sorted(
        {str(payload["episode_id"]) for payload in initial if "episode_id" in payload}
    )
    days = (end_utc - start_utc).total_seconds() / 86_400.0
    report: dict[str, object] = {
        "period": {
            "start": start_utc.isoformat(),
            "end": end_utc.isoformat(),
            "warmup_start": warmup_start.isoformat(),
        },
        "routes_evaluated": route_sample_count,
        "unique_directional_routes": len(unique_routes),
        "sample_slots_seen": sample_slots,
        "funnel": {
            "baseline_history_available": counters["baseline_history_available"],
            "stable_baseline_le_5_bps": counters["stable_baseline"],
            "expected_net_ge_10_bps": counters["expected_net_at_least_10"],
            "volume_ge_1m": counters["volume_at_least_1m"],
            "strict_60s_confirmed": len(initial),
        },
        "episodes": {
            "final_unique_manual_opportunity_episodes": len(unique_episode_ids),
            "initial_notifications": len(initial),
            "expansion_notifications": len(expansions),
            "events_per_day": len(alert_payloads) / days,
            "episode_ids": unique_episode_ids,
        },
        "rejection_counts": {
            key: value
            for key, value in counters.items()
            if key in {
                "insufficient_baseline_history",
                "unstable_baseline",
                "expected_net_below_min",
                "volume_below_min",
                "volume_unavailable",
                "persistence_broken",
                "data_gap",
                "bbo_freshness_rejected",
                "future_bbo",
                "stale_bbo",
                "future_sample",
            }
        },
        "symbols_routes": [
            {
                "canonical_symbol": key.canonical_symbol,
                "long": f"{key.long_venue}:{key.long_venue_symbol}",
                "short": f"{key.short_venue}:{key.short_venue_symbol}",
            }
            for key in sorted(unique_routes, key=_route_sort_key)
        ],
        "examples": initial[:10],
        "qqq_arcus_to_lighter_robinhood": {
            "observations": qqq_inspection,
            "qualifies_naturally": any(
                item["rejection_reason"] is None for item in qqq_inspection
            ),
        },
        "sanity": {
            "invalid_bbo_rows": invalid_bbo_rows,
            "abs_spread_over_1000_bps": oversized_spreads,
            "largest_abs_spread_bps": largest_abs_spread,
            "oversized_fresh_observations": oversized_fresh_observations,
            "oversized_candidate_episodes": len(oversized_candidate_episode_ids),
            "oversized_confirmed_episodes": len(oversized_confirmed_episode_ids),
            "observed_at_after_sample_rows": observed_after_sample_rows,
            "duplicate_initial_event_ids": len(initial)
            - len({str(item.get("episode_id")) for item in initial}),
            "source_mutation": False,
            "runtime_store_writes": False,
        },
    }
    result = ManualReplayResult(report)
    if output_dir is not None:
        _write_replay_output(Path(output_dir), report)
    return result


def _dataset_root(data_root: Path, dataset: str) -> Path | None:
    root = Path(data_root) / dataset
    files = tuple(root.glob("date=*/part-*.parquet"))
    if not files:
        return None
    return root


def _dataset_glob(data_root: Path, dataset: str) -> str | None:
    root = _dataset_root(data_root, dataset)
    if root is None:
        return None
    return str(root / "date=*" / "part-*.parquet").replace("'", "''")


def load_recent_bbo_history(
    data_root: Path,
    *,
    allowed_feeds: Collection[tuple[str, str, str]],
    as_of: datetime,
    window_seconds: int = 3 * 24 * 60 * 60,
) -> dict[SpreadPairKey, tuple[tuple[datetime, float], ...]]:
    """Load strictly-prior directional BBO history for enabled exact feeds.

    The Parquet scan selects only the fields needed to build a directional BBO
    spread and consumes Arrow batches ordered by sample slot.  A SQL window
    keeps the latest observed row for each feed and slot before route pairing,
    so late duplicate writes cannot create duplicate route points.
    """
    as_of_utc = _as_utc(as_of, "as_of")
    if window_seconds <= 0:
        raise ValueError("window_seconds must be positive")
    feeds = set(allowed_feeds)
    if not feeds:
        return {}
    market_root = _dataset_root(data_root, "market")
    if market_root is None:
        raise FileNotFoundError("market Parquet dataset is missing")
    market_glob = _dataset_glob(data_root, "market")
    if market_glob is None:
        raise FileNotFoundError("market Parquet dataset is missing")

    start = as_of_utc - timedelta(seconds=window_seconds)
    query = f"""
        WITH ranked AS (
            SELECT
                sample_time,
                observed_at,
                venue,
                venue_symbol,
                canonical_symbol,
                best_bid,
                best_ask,
                row_number() OVER (
                    PARTITION BY venue, venue_symbol, canonical_symbol, sample_time
                    ORDER BY observed_at DESC
                ) AS row_number
            FROM read_parquet('{market_glob}')
            WHERE sample_time >= ?
              AND sample_time < ?
              AND observed_at <= ?
        )
        SELECT sample_time, observed_at, venue, venue_symbol, canonical_symbol,
               best_bid, best_ask
        FROM ranked
        WHERE row_number = 1
        ORDER BY sample_time, canonical_symbol, venue, venue_symbol
    """

    points: dict[SpreadPairKey, list[tuple[datetime, float]]] = {}

    def process_slot(rows: list[_ReplayMarketRow]) -> None:
        if not rows:
            return
        by_symbol: dict[str, dict[tuple[str, str], _ReplayMarketRow]] = {}
        for row in rows:
            feed = (row.venue, row.venue_symbol, row.canonical_symbol)
            if feed not in feeds or not _valid_bbo(row.best_bid, row.best_ask):
                continue
            by_symbol.setdefault(row.canonical_symbol, {})[
                (row.venue, row.venue_symbol)
            ] = row

        sample_time = rows[0].sample_time
        for canonical_symbol, symbol_feeds in sorted(by_symbol.items()):
            ordered_feeds = sorted(symbol_feeds.items())
            for (long_venue, long_symbol), long_row in ordered_feeds:
                for (short_venue, short_symbol), short_row in ordered_feeds:
                    if long_venue.lower() == short_venue.lower():
                        continue
                    key = SpreadPairKey(
                        canonical_symbol=canonical_symbol,
                        long_venue=long_venue,
                        long_venue_symbol=long_symbol,
                        short_venue=short_venue,
                        short_venue_symbol=short_symbol,
                    )
                    spread_bps = (
                        short_row.best_bid / long_row.best_ask - 1.0
                    ) * 10_000.0
                    points.setdefault(key, []).append((sample_time, spread_bps))

    current_sample: datetime | None = None
    slot_rows: list[_ReplayMarketRow] = []
    with duckdb.connect() as connection:
        reader = connection.execute(query, [start, as_of_utc, as_of_utc]).to_arrow_reader(
            batch_size=50_000
        )
        for batch in reader:
            for raw_row in batch.to_pylist():
                parsed = _parse_market_row(raw_row)
                if parsed is None:
                    continue
                if current_sample is None:
                    current_sample = parsed.sample_time
                elif parsed.sample_time != current_sample:
                    process_slot(slot_rows)
                    slot_rows = []
                    current_sample = parsed.sample_time
                slot_rows.append(parsed)
    process_slot(slot_rows)

    return {key: tuple(route_points) for key, route_points in points.items()}


def _stream_market_rows(
    market_root: Path,
    start: datetime,
    end: datetime,
    process_slot: Any,
) -> None:
    """Stream one UTC date partition at a time to bound sort memory."""
    slot_time: datetime | None = None
    slot_rows: list[_ReplayMarketRow] = []
    current_date = start.date()
    last_date = (end - timedelta(microseconds=1)).date()
    while current_date <= last_date:
        partition = market_root / f"date={current_date.isoformat()}"
        files = tuple(partition.glob("part-*.parquet"))
        if files:
            day_start = datetime(
                current_date.year,
                current_date.month,
                current_date.day,
                tzinfo=UTC,
            )
            day_end = day_start + timedelta(days=1)
            path_glob = str(partition / "part-*.parquet").replace("'", "''")
            query = f"""
                WITH ranked AS (
                    SELECT
                        sample_time,
                        observed_at,
                        venue,
                        venue_symbol,
                        canonical_symbol,
                        best_bid,
                        best_ask,
                        row_number() OVER (
                            PARTITION BY venue, venue_symbol, canonical_symbol, sample_time
                            ORDER BY observed_at DESC
                        ) AS row_number
                    FROM read_parquet('{path_glob}')
                    WHERE sample_time >= ? AND sample_time < ?
                )
                SELECT sample_time, observed_at, venue, venue_symbol, canonical_symbol,
                       best_bid, best_ask
                FROM ranked
                WHERE row_number = 1
                ORDER BY sample_time, canonical_symbol, venue, venue_symbol
            """
            with duckdb.connect() as connection:
                connection.execute("PRAGMA memory_limit='1GB'")
                current_hour = day_start
                while current_hour < day_end:
                    hour_end = current_hour + timedelta(hours=1)
                    query_start = max(start, current_hour)
                    query_end = min(end, hour_end)
                    if query_start < query_end:
                        reader = connection.execute(
                            query, [query_start, query_end]
                        ).to_arrow_reader(batch_size=50_000)
                        for batch in reader:
                            for row in batch.to_pylist():
                                parsed = _parse_market_row(row)
                                if parsed is None:
                                    continue
                                if slot_time is None:
                                    slot_time = parsed.sample_time
                                if parsed.sample_time != slot_time:
                                    completed_slot = slot_time
                                    process_slot(slot_rows)
                                    missing_slot = completed_slot + timedelta(seconds=10)
                                    while missing_slot < parsed.sample_time:
                                        process_slot([], missing_slot)
                                        missing_slot += timedelta(seconds=10)
                                    slot_rows = []
                                    slot_time = parsed.sample_time
                                slot_rows.append(parsed)
                    current_hour = hour_end
        current_date += timedelta(days=1)
    if slot_rows:
        process_slot(slot_rows)


def _parse_market_row(row: Mapping[str, object]) -> _ReplayMarketRow | None:
    try:
        raw_sample_time = row["sample_time"]
        raw_observed_at = row["observed_at"]
        if not isinstance(raw_sample_time, datetime) or not isinstance(
            raw_observed_at, datetime
        ):
            return None
        sample_time = _as_utc(raw_sample_time, "sample_time")
        observed_at = _as_utc(raw_observed_at, "observed_at")
        venue = str(row["venue"])
        venue_symbol = str(row["venue_symbol"])
        canonical_symbol = str(row["canonical_symbol"])
        raw_best_bid = row["best_bid"]
        raw_best_ask = row["best_ask"]
        if not isinstance(raw_best_bid, (int, float)) or isinstance(
            raw_best_bid, bool
        ):
            return None
        if not isinstance(raw_best_ask, (int, float)) or isinstance(
            raw_best_ask, bool
        ):
            return None
        best_bid = _finite(raw_best_bid, "best_bid")
        best_ask = _finite(raw_best_ask, "best_ask")
    except (KeyError, TypeError, ValueError):
        return None
    return _ReplayMarketRow(
        sample_time=sample_time,
        observed_at=observed_at,
        venue=venue,
        venue_symbol=venue_symbol,
        canonical_symbol=canonical_symbol,
        best_bid=best_bid,
        best_ask=best_ask,
    )


def _load_volume_index(
    context_glob: str | None,
    start: datetime,
    end: datetime,
) -> dict[tuple[str, str, str], _ReplayVolumeSeries]:
    if context_glob is None:
        return {}
    query = f"""
        SELECT sample_time, observed_at, venue, venue_symbol, canonical_symbol,
               volume_24h
        FROM read_parquet('{context_glob}')
        WHERE sample_time >= ? AND sample_time < ?
        ORDER BY observed_at, sample_time
    """
    latest: dict[tuple[str, str, str, datetime], _ReplayVolume] = {}
    with duckdb.connect() as connection:
        reader = connection.execute(query, [start, end]).to_arrow_reader(
            batch_size=50_000
        )
        for batch in reader:
            for row in batch.to_pylist():
                try:
                    sample_time = _as_utc(row["sample_time"], "sample_time")
                    observed_at = _as_utc(row["observed_at"], "observed_at")
                    key = (
                        str(row["venue"]),
                        str(row["venue_symbol"]),
                        str(row["canonical_symbol"]),
                        sample_time,
                    )
                    raw_volume = row.get("volume_24h")
                    volume = (
                        None
                        if raw_volume is None
                        else _finite(raw_volume, "volume_24h")  # type: ignore[arg-type]
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                previous = latest.get(key)
                if previous is None or observed_at >= previous.observed_at:
                    latest[key] = _ReplayVolume(observed_at, sample_time, volume)
    result: dict[tuple[str, str, str], list[_ReplayVolume]] = {}
    for (venue, venue_symbol, canonical_symbol, _sample_time), value in latest.items():
        result.setdefault((venue, venue_symbol, canonical_symbol), []).append(value)
    return {
        key: _ReplayVolumeSeries(
            observed_times=tuple(
                item.observed_at
                for item in sorted(values, key=lambda item: item.observed_at)
            ),
            values=tuple(sorted(values, key=lambda item: item.observed_at)),
        )
        for key, values in result.items()
    }


def _volume_as_of(
    volumes: Mapping[tuple[str, str, str], _ReplayVolumeSeries],
    key: tuple[str, str, str],
    as_of: datetime,
) -> float | None:
    series = volumes.get(key)
    if series is None:
        return None
    end = bisect_right(series.observed_times, as_of)
    for value in reversed(series.values[:end]):
        if value.sample_time <= as_of:
            return value.volume_24h
    return None


def _valid_bbo(best_bid: float, best_ask: float) -> bool:
    return (
        math.isfinite(best_bid)
        and math.isfinite(best_ask)
        and best_bid > 0
        and best_ask > 0
        and best_bid < best_ask
    )


def _fee(fees_bps: Mapping[str, float], venue: str) -> float | None:
    for configured, value in fees_bps.items():
        if configured.lower() == venue.lower():
            try:
                result = float(value)
            except (TypeError, ValueError):
                return None
            return result if math.isfinite(result) and result >= 0 else None
    return None


def _count_funnel(
    observation: ManualOpportunityObservation,
    config: RadarConfig,
    counters: dict[str, int],
) -> None:
    if all(
        value is not None
        for value in (
            observation.mean_2h_bps,
            observation.mean_24h_bps,
            observation.mean_3d_bps,
        )
    ):
        counters["baseline_history_available"] += 1
        if (
            observation.baseline_range_bps is not None
            and observation.baseline_range_bps <= config.manual_opportunity.baseline_range_max_bps
        ):
            counters["stable_baseline"] += 1
            expected = observation.expected_net_at_a_bps
            if (
                expected is not None
                and expected + 1e-9 >= config.manual_opportunity.expected_net_min_bps
            ):
                counters["expected_net_at_least_10"] += 1
                route_volume = observation.route_volume_24h
                if (
                    route_volume is not None
                    and math.isfinite(route_volume)
                    and route_volume >= config.manual_opportunity.volume_24h_min_usd
                ):
                    counters["volume_at_least_1m"] += 1


def _route_sort_key(key: SpreadPairKey) -> tuple[str, str, str, str, str]:
    return (
        key.canonical_symbol,
        key.long_venue,
        key.long_venue_symbol,
        key.short_venue,
        key.short_venue_symbol,
    )


def _write_replay_output(output_dir: Path, report: dict[str, object]) -> None:
    import json

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    lines = ["Manual Opportunity v1 BBO replay", ""]
    lines.append(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    (output_dir / "report.txt").write_text("\n".join(lines), encoding="utf-8")
