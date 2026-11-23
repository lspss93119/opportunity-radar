"""Cross-venue funding differential monitor (handoff types B and C).

For every canonical symbol and ordered venue pair (long A, short B), the net
funding APR is ``APR_B - APR_A``: positive means the hedged pair is paid.

* Spike: the current net APR stays at or above ``spike_net_apr`` for at least
  ``spike_min_samples`` consecutive funding samples.
* Carry: the time-weighted net APR over ``carry_window_hours`` is at least
  ``carry_avg_net_apr`` and the expected funding over ``carry_horizon_hours``
  exceeds the current $-sized round-trip cost.

Evaluation only reads in-memory state and bounded per-feed history; history is
loaded once at startup by the application, never queried here.
"""

from __future__ import annotations

import bisect
import logging
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from radar.config import FundingMonitorConfig
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot
from radar.monitors.base import AlertRequest, JSONValue
from radar.monitors.spread.models import (
    calculate_raw_spread_bps,
    is_valid_market_snapshot,
    selected_vwap,
)
from radar.state import RadarState
from radar.storage.sqlite import SQLiteRuntimeStore

UTC = timezone.utc
LOGGER = logging.getLogger(__name__)
HOURS_PER_YEAR = 365 * 24
FeedKey = tuple[str, str, str]  # (venue, venue_symbol, canonical_symbol)
SPIKE = "funding_spike"
CARRY = "funding_carry"


@dataclass(frozen=True)
class FeedWindow:
    mean_apr: float | None
    coverage: float


@dataclass
class FeedHistory:
    """Funding APR observations for one feed, held as a step function."""

    times: list[datetime] = field(default_factory=list)
    aprs: list[float] = field(default_factory=list)

    def record(self, observed_at: datetime, apr: float) -> bool:
        if self.times and observed_at <= self.times[-1]:
            return False
        self.times.append(observed_at)
        self.aprs.append(apr)
        return True

    def prune(self, cutoff: datetime) -> None:
        # Keep the last point before the cutoff: it defines the value at cutoff.
        index = bisect.bisect_left(self.times, cutoff) - 1
        if index > 0:
            del self.times[:index]
            del self.aprs[:index]

    def value_at(self, moment: datetime, max_gap: timedelta) -> float | None:
        index = bisect.bisect_right(self.times, moment) - 1
        if index < 0 or moment - self.times[index] > max_gap:
            return None
        return self.aprs[index]

    def window(self, start: datetime, end: datetime, max_gap: timedelta) -> FeedWindow:
        """Time-weighted mean over [start, end); stretches past max_gap count as gaps."""
        breakpoints = [start, *(t for t in self.times if start < t < end), end]
        covered = 0.0
        weighted = 0.0
        for left, right in zip(breakpoints, breakpoints[1:]):
            value = self.value_at(left, max_gap)
            if value is None:
                continue
            index = bisect.bisect_right(self.times, left) - 1
            # A value goes stale max_gap after it was observed.
            usable_right = min(right, self.times[index] + max_gap)
            seconds = (usable_right - left).total_seconds()
            if seconds > 0:
                covered += seconds
                weighted += value * seconds
        total = (end - start).total_seconds()
        return FeedWindow(
            mean_apr=weighted / covered if covered > 0 else None,
            coverage=covered / total if total > 0 else 0.0,
        )


def trailing_seconds_at_or_above(
    long_history: FeedHistory,
    short_history: FeedHistory,
    *,
    threshold: float,
    end: datetime,
    lookback: timedelta,
    max_gap: timedelta,
) -> float:
    """How long the net APR has continuously been at or above ``threshold``."""
    start = end - lookback
    breakpoints = sorted(
        {start, end, *(t for t in (*long_history.times, *short_history.times) if start < t < end)}
    )
    held = 0.0
    for left, right in reversed(list(zip(breakpoints, breakpoints[1:]))):
        long_apr = long_history.value_at(left, max_gap)
        short_apr = short_history.value_at(left, max_gap)
        if long_apr is None or short_apr is None or short_apr - long_apr < threshold:
            break
        held += (right - left).total_seconds()
    return held


@dataclass
class PairState:
    streak: int = 0
    streak_started_at: datetime | None = None
    last_marker: datetime | None = None
    alerted: dict[str, bool] = field(default_factory=lambda: {SPIKE: False, CARRY: False})
    cleared_since: dict[str, datetime | None] = field(
        default_factory=lambda: {SPIKE: None, CARRY: None}
    )

    def to_json(self) -> dict[str, JSONValue]:
        return {
            "streak": self.streak,
            "streak_started_at": _iso(self.streak_started_at),
            "last_marker": _iso(self.last_marker),
            "alerted": dict(self.alerted),
            "cleared_since": {kind: _iso(value) for kind, value in self.cleared_since.items()},
        }

    @classmethod
    def from_json(cls, raw: object) -> "PairState":
        if not isinstance(raw, dict):
            raise TypeError("pair state must be an object")
        alerted = raw.get("alerted") or {}
        cleared = raw.get("cleared_since") or {}
        streak = raw.get("streak", 0)
        if isinstance(streak, bool) or not isinstance(streak, int) or streak < 0:
            raise ValueError("invalid streak")
        return cls(
            streak=streak,
            streak_started_at=_parse_iso(raw.get("streak_started_at")),
            last_marker=_parse_iso(raw.get("last_marker")),
            alerted={kind: alerted.get(kind) is True for kind in (SPIKE, CARRY)},
            cleared_since={kind: _parse_iso(cleared.get(kind)) for kind in (SPIKE, CARRY)},
        )


def _iso(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _parse_iso(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("timestamp must be an ISO string")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)


def _pair_id(long_key: FeedKey, short_key: FeedKey) -> str:
    return "|".join((long_key[2], *long_key[:2], *short_key[:2]))


class FundingMonitor:
    name = "funding"
    state_key = "pairs"

    def __init__(
        self,
        config: FundingMonitorConfig,
        fees_bps: Mapping[str, float],
        *,
        spread_stale_after_seconds: int,
        rfq_stale_after_seconds: int,
        market_identities: Collection[FeedKey] | None = None,
        runtime_store: SQLiteRuntimeStore | None = None,
    ) -> None:
        self.config = config
        self.interval_seconds = config.interval_seconds
        self._fees_bps = {venue.lower(): fee for venue, fee in fees_bps.items()}
        self._spread_stale_after_seconds = spread_stale_after_seconds
        self._rfq_stale_after_seconds = rfq_stale_after_seconds
        self._market_identities = (
            None if market_identities is None else frozenset(market_identities)
        )
        self._runtime_store = runtime_store
        self._histories: dict[FeedKey, FeedHistory] = defaultdict(FeedHistory)
        self._windows: dict[FeedKey, FeedWindow] = {}
        self._window_marker: dict[FeedKey, datetime] = {}
        self._pairs: dict[str, PairState] = {}
        if runtime_store is not None:
            self._pairs = self._load_pairs(
                runtime_store.get_monitor_state(self.name, self.state_key)
            )

    @property
    def _window(self) -> timedelta:
        return timedelta(hours=self.config.carry_window_hours)

    @property
    def _max_gap(self) -> timedelta:
        return timedelta(seconds=self.config.max_history_gap_seconds)

    def hydrate_history(self, points: Mapping[FeedKey, Iterable[tuple[datetime, float]]]) -> None:
        """Load prior (observed_at, apr) points once before live evaluation."""
        for key, feed_points in points.items():
            history = self._histories[key]
            for observed_at, apr in sorted(feed_points):
                history.record(observed_at.astimezone(UTC), float(apr))

    def set_market_identities(
        self,
        market_identities: Collection[FeedKey],
        market_sample_seconds: Mapping[FeedKey, int] | None = None,
    ) -> None:
        self._market_identities = frozenset(market_identities)
        for key in tuple(self._histories):
            if key not in self._market_identities:
                del self._histories[key]
                self._windows.pop(key, None)
                self._window_marker.pop(key, None)

    def _in_scope(self, key: FeedKey) -> bool:
        return self._market_identities is None or key in self._market_identities

    async def evaluate(self, now: datetime, state: RadarState) -> list[AlertRequest]:
        now = now.astimezone(UTC)
        stale = timedelta(seconds=self.config.stale_after_seconds)
        latest: dict[FeedKey, FundingSnapshot] = {}
        for snapshot in state.funding:
            key = (snapshot.venue, snapshot.venue_symbol, snapshot.canonical_symbol)
            if not self._in_scope(key):
                continue
            history = self._histories[key]
            history.record(snapshot.observed_at, snapshot.apr)
            history.prune(now - self._window - self._max_gap)
            age = now - snapshot.observed_at
            if timedelta(0) <= age <= stale:
                latest[key] = snapshot

        by_symbol: dict[str, list[FeedKey]] = defaultdict(list)
        for key in sorted(latest):
            by_symbol[key[2]].append(key)
        markets = {
            (snapshot.venue, snapshot.venue_symbol, snapshot.canonical_symbol): snapshot
            for snapshot in state.markets
        }
        contexts = {
            (context.venue, context.venue_symbol, context.canonical_symbol): context
            for context in state.hourly_context
        }

        alerts: list[AlertRequest] = []
        seen: set[str] = set()
        changed = False
        for keys in by_symbol.values():
            for long_key in keys:
                for short_key in keys:
                    if long_key[0].lower() == short_key[0].lower():
                        continue
                    pair_id = _pair_id(long_key, short_key)
                    seen.add(pair_id)
                    pair_alerts, pair_changed = self._evaluate_pair(
                        pair_id,
                        latest[long_key],
                        latest[short_key],
                        long_key,
                        short_key,
                        now,
                        markets,
                        contexts,
                    )
                    alerts.extend(pair_alerts)
                    changed = changed or pair_changed

        # Pairs without fresh funding on both sides fail closed: conditions false.
        for pair_id in tuple(self._pairs):
            if pair_id in seen:
                continue
            pair = self._pairs[pair_id]
            if pair.streak:
                pair.streak = 0
                pair.streak_started_at = None
                changed = True
            for kind in (SPIKE, CARRY):
                changed = self._clear(pair, kind, now) or changed
            if not any(pair.alerted.values()) and pair.streak == 0:
                del self._pairs[pair_id]
                changed = True

        if changed and self._runtime_store is not None:
            self._runtime_store.set_monitor_state(
                self.name,
                self.state_key,
                {pair_id: pair.to_json() for pair_id, pair in sorted(self._pairs.items())},
                updated_at=now,
            )
        return alerts

    def _feed_window(self, key: FeedKey, now: datetime) -> FeedWindow:
        history = self._histories[key]
        marker = history.times[-1] if history.times else None
        cached = self._windows.get(key)
        # Funding changes only when a new sample lands, so reuse until then;
        # still refresh hourly so coverage keeps tracking the sliding window.
        if (
            cached is not None
            and marker is not None
            and self._window_marker.get(key) == marker
            and now - marker < timedelta(hours=1)
        ):
            return cached
        window = history.window(now - self._window, now, self._max_gap)
        self._windows[key] = window
        if marker is not None:
            self._window_marker[key] = marker
        return window

    def _evaluate_pair(
        self,
        pair_id: str,
        long_funding: FundingSnapshot,
        short_funding: FundingSnapshot,
        long_key: FeedKey,
        short_key: FeedKey,
        now: datetime,
        markets: Mapping[FeedKey, MarketSnapshot],
        contexts: Mapping[FeedKey, HourlyContext],
    ) -> tuple[list[AlertRequest], bool]:
        config = self.config
        net_apr = short_funding.apr - long_funding.apr
        marker = max(long_funding.observed_at, short_funding.observed_at)
        pair = self._pairs.get(pair_id)
        is_new = pair is None
        if pair is None:
            pair = PairState()
        changed = False
        if pair.last_marker != marker:
            pair.last_marker = marker
            if net_apr >= config.spike_net_apr:
                if pair.streak == 0:
                    pair.streak_started_at = marker
                pair.streak += 1
            else:
                pair.streak = 0
                pair.streak_started_at = None
            changed = True

        long_window = self._feed_window(long_key, now)
        short_window = self._feed_window(short_key, now)
        coverage = min(long_window.coverage, short_window.coverage)
        avg_net_apr = (
            short_window.mean_apr - long_window.mean_apr
            if long_window.mean_apr is not None
            and short_window.mean_apr is not None
            and coverage >= config.carry_min_coverage
            else None
        )
        cost = self._round_trip(long_key, short_key, markets, now)
        expected_apr = avg_net_apr if avg_net_apr is not None else net_apr
        expected_funding_bps = expected_apr * config.carry_horizon_hours / HOURS_PER_YEAR * 10_000

        conditions = {
            SPIKE: pair.streak >= config.spike_min_samples,
            CARRY: avg_net_apr is not None
            and avg_net_apr >= config.carry_avg_net_apr
            and cost["round_trip_cost_bps"] is not None
            and expected_funding_bps > cost["round_trip_cost_bps"],
        }
        alerts: list[AlertRequest] = []
        for kind, active in conditions.items():
            if not active:
                changed = self._clear(pair, kind, now) or changed
                continue
            if pair.cleared_since[kind] is not None:
                pair.cleared_since[kind] = None
                changed = True
            if pair.alerted[kind]:
                continue
            pair.alerted[kind] = True
            changed = True
            threshold = config.spike_net_apr if kind == SPIKE else config.carry_avg_net_apr
            alerts.append(
                self._alert(
                    kind,
                    pair,
                    long_funding,
                    short_funding,
                    long_key,
                    short_key,
                    now,
                    net_apr=net_apr,
                    avg_net_apr=avg_net_apr,
                    coverage=coverage,
                    expected_funding_bps=expected_funding_bps,
                    condition_seconds=trailing_seconds_at_or_above(
                        self._histories[long_key],
                        self._histories[short_key],
                        threshold=threshold,
                        end=now,
                        lookback=self._window,
                        max_gap=self._max_gap,
                    ),
                    cost=cost,
                    contexts=contexts,
                    markets=markets,
                )
            )

        # Only pairs with a running streak or an alerted condition are kept.
        keep = bool(pair.streak) or any(pair.alerted.values())
        if is_new:
            if keep:
                self._pairs[pair_id] = pair
            return alerts, keep
        if not keep:
            del self._pairs[pair_id]
            return alerts, True
        return alerts, changed

    def _clear(self, pair: PairState, kind: str, now: datetime) -> bool:
        """Re-arm an alerted condition only after it stays false for rearm_seconds."""
        if not pair.alerted[kind]:
            return False
        if pair.cleared_since[kind] is None:
            pair.cleared_since[kind] = now
            return True
        if now - pair.cleared_since[kind] >= timedelta(seconds=self.config.rearm_seconds):
            pair.alerted[kind] = False
            pair.cleared_since[kind] = None
            return True
        return False

    def _round_trip(
        self,
        long_key: FeedKey,
        short_key: FeedKey,
        markets: Mapping[FeedKey, MarketSnapshot],
        now: datetime,
    ) -> dict[str, float | None]:
        """Enter long A / short B now and exit at today's reverse quotes.

        With equal quantity on both legs the basis cancels exactly between
        entry and exit, so the cost is both venues' bid/ask widths at the
        evaluated size (as bps of the long notional) plus four taker fills.
        """
        empty: dict[str, float | None] = {
            "entry_spread_bps": None,
            "reverse_spread_bps": None,
            "round_trip_cost_bps": None,
        }
        long_snapshot = markets.get(long_key)
        short_snapshot = markets.get(short_key)
        if long_snapshot is None or short_snapshot is None:
            return empty
        for snapshot in (long_snapshot, short_snapshot):
            if not is_valid_market_snapshot(
                snapshot,
                now,
                stale_after_seconds=self._spread_stale_after_seconds,
                rfq_stale_after_seconds=self._rfq_stale_after_seconds,
            ):
                return empty
        size = self.config.primary_size_usd
        prices = (
            selected_vwap(long_snapshot, "buy", size),
            selected_vwap(short_snapshot, "sell", size),
            selected_vwap(short_snapshot, "buy", size),
            selected_vwap(long_snapshot, "sell", size),
        )
        if any(price is None for price in prices):
            return empty
        long_buy, short_sell, short_buy, long_sell = prices
        entry = calculate_raw_spread_bps(long_buy, short_sell)
        reverse = calculate_raw_spread_bps(short_buy, long_sell)
        long_fee = self._fees_bps.get(long_key[0].lower())
        short_fee = self._fees_bps.get(short_key[0].lower())
        cost = None
        if long_fee is not None and short_fee is not None:
            widths_bps = ((long_buy - long_sell) + (short_buy - short_sell)) / long_buy * 10_000
            cost = widths_bps + 2.0 * (long_fee + short_fee)
        return {
            "entry_spread_bps": entry,
            "reverse_spread_bps": reverse,
            "round_trip_cost_bps": cost,
        }

    def _alert(
        self,
        kind: str,
        pair: PairState,
        long_funding: FundingSnapshot,
        short_funding: FundingSnapshot,
        long_key: FeedKey,
        short_key: FeedKey,
        now: datetime,
        *,
        net_apr: float,
        avg_net_apr: float | None,
        coverage: float,
        expected_funding_bps: float,
        condition_seconds: float,
        cost: Mapping[str, float | None],
        contexts: Mapping[FeedKey, HourlyContext],
        markets: Mapping[FeedKey, MarketSnapshot],
    ) -> AlertRequest:
        payload: dict[str, JSONValue] = {
            "kind": kind,
            "canonical_symbol": long_key[2],
            "long_venue": long_key[0],
            "long_venue_symbol": long_key[1],
            "short_venue": short_key[0],
            "short_venue_symbol": short_key[1],
            "net_apr": net_apr,
            "avg_net_apr": avg_net_apr,
            "window_hours": self.config.carry_window_hours,
            "coverage": coverage,
            "spike_samples": pair.streak,
            "condition_seconds": condition_seconds,
            "horizon_hours": self.config.carry_horizon_hours,
            "expected_funding_bps": expected_funding_bps,
            "primary_size_usd": self.config.primary_size_usd,
            **cost,
            "long_funding": _funding_payload(long_funding),
            "short_funding": _funding_payload(short_funding),
            "long_market": _market_payload(markets.get(long_key), contexts.get(long_key)),
            "short_market": _market_payload(markets.get(short_key), contexts.get(short_key)),
            "evaluated_at": now.isoformat(),
        }
        marker = pair.last_marker or now
        return AlertRequest(
            monitor=self.name,
            event_id=f"{_pair_id(long_key, short_key)}:{kind}:{marker.isoformat()}",
            created_at=now,
            payload=payload,
        )

    @staticmethod
    def _load_pairs(raw: object) -> dict[str, PairState]:
        if not isinstance(raw, dict):
            return {}
        pairs: dict[str, PairState] = {}
        for pair_id, raw_pair in raw.items():
            try:
                pairs[str(pair_id)] = PairState.from_json(raw_pair)
            except (TypeError, ValueError):
                continue
        return pairs


def _funding_payload(snapshot: FundingSnapshot) -> dict[str, JSONValue]:
    return {
        "funding_rate": snapshot.funding_rate,
        "interval_seconds": snapshot.interval_seconds,
        "apr": snapshot.apr,
        "observed_at": snapshot.observed_at.isoformat(),
        "next_funding_time": _iso(snapshot.next_funding_time),
    }


def _market_payload(
    snapshot: MarketSnapshot | None, context: HourlyContext | None
) -> dict[str, JSONValue]:
    mark = None if snapshot is None else snapshot.mark_price
    open_interest_usd = None
    if context is not None and context.open_interest is not None and mark is not None:
        # Order-book venues report open interest in base units.
        open_interest_usd = context.open_interest * mark
    return {
        "price_source": "order_book" if snapshot is None else snapshot.price_source,
        "open_interest_usd": open_interest_usd,
        "volume_24h_usd": None if context is None else context.volume_24h,
        "long_open_interest_usd": None if context is None else context.long_open_interest,
        "short_open_interest_usd": None if context is None else context.short_open_interest,
    }
