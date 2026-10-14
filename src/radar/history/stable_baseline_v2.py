"""V2 offline data-quality and convergence research primitives.

This module is intentionally separate from the V1 replay scanner.  It reads
historical observations, attributes why a pair slot was unusable, and provides
pure helpers for causal research.  It never places orders or touches live
Radar state.
"""
from __future__ import annotations

import csv
import json
import math
from collections import Counter
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from itertools import combinations, groupby
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

from radar.history.stable_baseline import (
    Pair,
    Params as V1Params,
    _as_datetime,
    _quantile,
    _sample_pairs,
    csv_or_parquet_quotes,
)


BASIS_TYPES = ("mid", "exec_buy_a_sell_b", "exec_buy_b_sell_a")


@dataclass(frozen=True)
class AnalysisParams:
    sample_seconds: int = 60
    raw_sample_seconds: int = 10
    max_quote_age_seconds: int = 12
    max_skew_seconds: int = 3
    max_gap_seconds: int = 120
    min_daily_coverage: float = 0.60
    min_baseline_days: int = 5
    stable_range_bps: float = 12.0
    return_band_bps: float = 5.0

    def __post_init__(self) -> None:
        if self.sample_seconds <= 0 or self.raw_sample_seconds <= 0:
            raise ValueError("sampling intervals must be positive")
        if self.max_quote_age_seconds < 0 or self.max_skew_seconds < 0:
            raise ValueError("quote age and skew limits must be non-negative")
        if self.max_gap_seconds < self.sample_seconds:
            raise ValueError("max_gap_seconds must cover one sampled slot")
        if not 0 < self.min_daily_coverage <= 1:
            raise ValueError("min_daily_coverage must be in (0, 1]")
        if self.min_baseline_days < 1:
            raise ValueError("min_baseline_days must be positive")
        for name in ("stable_range_bps", "return_band_bps"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and nonnegative")


@dataclass(frozen=True)
class MarketObservation:
    sample_time: datetime
    observed_at: datetime
    venue: str
    venue_symbol: str
    symbol: str
    buy: float | None
    sell: float | None


@dataclass(frozen=True)
class ObservationAssessment:
    valid: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class PairPoint:
    sample_time: datetime
    pair: Pair
    a: MarketObservation | None
    b: MarketObservation | None


@dataclass(frozen=True)
class PairSlotAssessment:
    valid_sync: bool
    reasons: tuple[str, ...]
    skew_seconds: float | None


@dataclass(frozen=True)
class DailySeries:
    day: date
    values: dict[str, tuple[float, ...]]
    sample_count: int
    coverage: float


@dataclass(frozen=True)
class BaselineReference:
    baseline_bps: float
    q90_q10_bps: float
    days_used: int
    stable: bool
    drift_bps_per_day: float
    days: tuple[date, ...]


@dataclass(frozen=True)
class AnomalyEpisode:
    pair: Pair
    basis_type: str
    threshold_bps: float
    direction: int
    start: datetime
    end: datetime
    baseline_bps: float
    peak_deviation_bps: float
    sample_count: int
    confirmed: bool
    status: str
    convergence_seconds: float | None
    converged_15m: bool | None
    converged_1h: bool | None
    converged_4h: bool | None
    gap_seconds: float
    max_adverse_deviation_bps: float | None = None
    regime: str = "all"


def _classify_observation(
    observation: MarketObservation,
    params: AnalysisParams = AnalysisParams(),
) -> ObservationAssessment:
    reasons: list[str] = []
    age = (observation.sample_time - observation.observed_at).total_seconds()
    if age < 0:
        reasons.append("observed_after_sample")
    elif age > params.max_quote_age_seconds:
        reasons.append("stale")
    prices_valid = (
        observation.buy is not None
        and observation.sell is not None
        and math.isfinite(observation.buy)
        and math.isfinite(observation.sell)
        and observation.buy > 0
        and observation.sell > 0
        and observation.buy >= observation.sell
    )
    if not prices_valid:
        reasons.append("invalid_vwap")
    return ObservationAssessment(not reasons, tuple(reasons))


def _attribute_pair_slot(
    point: PairPoint,
    params: AnalysisParams = AnalysisParams(),
) -> PairSlotAssessment:
    if point.a is None and point.b is None:
        return PairSlotAssessment(False, ("missing_a", "missing_b"), None)
    if point.a is None:
        return PairSlotAssessment(False, ("missing_a",), None)
    if point.b is None:
        return PairSlotAssessment(False, ("missing_b",), None)
    if point.a.sample_time != point.b.sample_time:
        return PairSlotAssessment(False, ("sample_time_mismatch",), None)
    a_status = _classify_observation(point.a, params)
    b_status = _classify_observation(point.b, params)
    reasons = [f"a_{reason}" for reason in a_status.reasons]
    reasons.extend(f"b_{reason}" for reason in b_status.reasons)
    skew = abs((point.a.observed_at - point.b.observed_at).total_seconds())
    if not reasons and skew > params.max_skew_seconds:
        reasons.append("observed_at_skew")
    return PairSlotAssessment(not reasons, tuple(reasons), skew)


def _sampling_loss_count(
    valid_raw_slots: set[datetime],
    valid_sampled_slots: set[datetime],
) -> int:
    """Count valid raw slots not represented by the coarser sample grid."""

    return len(valid_raw_slots - valid_sampled_slots)


def us_session_regime(timestamp: datetime) -> str:
    """Classify a timestamp using New York regular/extended/closed sessions."""

    local = timestamp.astimezone(ZoneInfo("America/New_York"))
    if local.weekday() >= 5:
        return "us_closed"
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    if regular_start <= local.time() < regular_end:
        return "us_regular"
    return "us_extended"


def _basis_values(point: PairPoint) -> dict[str, float]:
    if point.a is None or point.b is None:
        raise ValueError("basis requires both observations")
    a = point.a
    b = point.b
    if any(value is None for value in (a.buy, a.sell, b.buy, b.sell)):
        raise ValueError("basis requires complete VWAP values")
    a_mid = (a.buy + a.sell) / 2
    b_mid = (b.buy + b.sell) / 2
    return {
        "mid": 10000 * (b_mid / a_mid - 1),
        "exec_buy_a_sell_b": 10000 * (b.sell / a.buy - 1),
        "exec_buy_b_sell_a": 10000 * (a.sell / b.buy - 1),
    }


def build_daily_series(
    points: Iterator[PairPoint],
    *,
    params: AnalysisParams = AnalysisParams(),
    regime: str = "all",
) -> dict[date, DailySeries]:
    values_by_day: dict[date, dict[str, list[float]]] = {}
    for point in points:
        if regime != "all" and us_session_regime(point.sample_time) != regime:
            continue
        if not _attribute_pair_slot(point, params).valid_sync:
            continue
        day_values = values_by_day.setdefault(
            point.sample_time.date(), {basis: [] for basis in BASIS_TYPES}
        )
        for basis, value in _basis_values(point).items():
            day_values[basis].append(value)
    expected = 86400 / params.sample_seconds
    return {
        day: DailySeries(
            day=day,
            values={basis: tuple(day_values[basis]) for basis in BASIS_TYPES},
            sample_count=len(day_values["mid"]),
            coverage=len(day_values["mid"]) / expected,
        )
        for day, day_values in values_by_day.items()
    }


def _linear_slope(days: list[date], values: list[float]) -> float:
    if len(days) < 2:
        return 0.0
    x = [(day - days[0]).days for day in days]
    x_mean = sum(x) / len(x)
    y_mean = sum(values) / len(values)
    denominator = sum((value - x_mean) ** 2 for value in x)
    if denominator == 0:
        return 0.0
    return sum((x_value - x_mean) * (value - y_mean)
               for x_value, value in zip(x, values, strict=True)) / denominator


def causal_baseline(
    history: dict[date, DailySeries],
    *,
    today: date,
    basis_type: str,
    lookback_days: int,
    min_days: int,
    use_daily_medians: bool,
    stable_range_bps: float,
    min_daily_coverage: float = 0.60,
) -> BaselineReference | None:
    if basis_type not in BASIS_TYPES:
        raise ValueError(f"unknown basis_type: {basis_type}")
    eligible = [
        series
        for day, series in history.items()
        if 0 < (today - day).days <= lookback_days
        and series.coverage >= min_daily_coverage
        and series.values[basis_type]
    ]
    eligible.sort(key=lambda series: series.day)
    if len(eligible) < min_days:
        return None
    if use_daily_medians:
        values = [median(series.values[basis_type]) for series in eligible]
    else:
        values = [value for series in eligible for value in series.values[basis_type]]
    baseline = median(values)
    spread = _quantile(values, 0.9) - _quantile(values, 0.1)
    daily_days = [series.day for series in eligible]
    daily_medians = [median(series.values[basis_type]) for series in eligible]
    return BaselineReference(
        baseline_bps=baseline,
        q90_q10_bps=spread,
        days_used=len(eligible),
        stable=spread <= stable_range_bps,
        drift_bps_per_day=_linear_slope(daily_days, daily_medians),
        days=tuple(daily_days),
    )


def build_baseline_rows(
    pair: Pair,
    history: dict[date, DailySeries],
    *,
    params: AnalysisParams = AnalysisParams(),
    regime: str = "all",
) -> list[dict[str, object]]:
    windows = (
        ("24h", 1, 1, False),
        ("7d", 7, params.min_baseline_days, True),
        ("30d", 30, params.min_baseline_days, True),
    )
    rows: list[dict[str, object]] = []
    for window, lookback_days, min_days, use_daily_medians in windows:
        for basis_type in BASIS_TYPES:
            references = [
                reference
                for today in sorted(history)
                if (reference := causal_baseline(
                    history,
                    today=today,
                    basis_type=basis_type,
                    lookback_days=lookback_days,
                    min_days=min_days,
                    use_daily_medians=use_daily_medians,
                    stable_range_bps=params.stable_range_bps,
                    min_daily_coverage=params.min_daily_coverage,
                )) is not None
            ]
            spreads = [reference.q90_q10_bps for reference in references]
            drifts = [reference.drift_bps_per_day for reference in references]
            daily_medians = {
                day: median(series.values[basis_type])
                for day, series in history.items()
                if series.coverage >= params.min_daily_coverage
                and series.values[basis_type]
            }
            changes = [
                abs(daily_medians[next_day] - daily_medians[day])
                for day, next_day in zip(
                    sorted(daily_medians), sorted(daily_medians)[1:], strict=False
                )
            ]
            status = (
                "insufficient_data" if not references
                else "stable" if all(reference.stable for reference in references)
                else "unstable"
            )
            rows.append({
                **{
                    "symbol": pair.symbol,
                    "venue_a": pair.venue_a,
                    "symbol_a": pair.symbol_a,
                    "venue_b": pair.venue_b,
                    "symbol_b": pair.symbol_b,
                },
                "regime": regime,
                "window": window,
                "basis_type": basis_type,
                "covered_days": len(daily_medians),
                "eligible_windows": len(references),
                "stable_windows": sum(reference.stable for reference in references),
                "unstable_windows": sum(not reference.stable for reference in references),
                "median_daily_coverage": median([series.coverage for series in history.values()]) if history else 0.0,
                "median_baseline_bps": median([reference.baseline_bps for reference in references]) if references else None,
                "median_q90_q10_bps": median(spreads) if spreads else None,
                "max_q90_q10_bps": max(spreads) if spreads else None,
                "median_abs_day_change_bps": median(changes) if changes else None,
                "p90_abs_day_change_bps": _quantile(changes, 0.9) if changes else None,
                "median_drift_bps_per_day": median(drifts) if drifts else None,
                "status": status,
            })
    return rows


def _convergence_flag(
    *,
    convergence_seconds: float | None,
    duration_seconds: float,
    window_seconds: int,
    censored: bool,
) -> bool | None:
    if convergence_seconds is not None:
        return convergence_seconds <= window_seconds
    if duration_seconds >= window_seconds and not censored:
        return False
    return None


def detect_episodes(
    points: Iterator[PairPoint],
    *,
    baseline_by_day: dict[date, BaselineReference],
    basis_type: str,
    threshold_bps: float,
    direction: int,
    params: AnalysisParams = AnalysisParams(),
    regime: str = "all",
) -> list[AnomalyEpisode]:
    """Detect independent causal deviation episodes for one directed basis."""

    if basis_type not in BASIS_TYPES:
        raise ValueError(f"unknown basis_type: {basis_type}")
    if direction not in (-1, 1) or threshold_bps < 0:
        raise ValueError("direction must be -1 or 1 and threshold nonnegative")

    episodes: list[AnomalyEpisode] = []
    active: dict[str, object] | None = None
    last_valid_time: datetime | None = None

    def finish(status: str, end: datetime, gap_seconds: float = 0.0) -> None:
        nonlocal active
        if active is None:
            return
        start = active["start"]
        convergence_time = active["convergence_time"]
        duration_seconds = (end - start).total_seconds()
        convergence_seconds = (
            (convergence_time - start).total_seconds()
            if convergence_time is not None else None
        )
        censored = status.startswith("censored")
        episodes.append(AnomalyEpisode(
            pair=active["pair"],
            basis_type=basis_type,
            threshold_bps=threshold_bps,
            direction=direction,
            start=start,
            end=end,
            baseline_bps=active["baseline_bps"],
            peak_deviation_bps=active["peak_deviation_bps"],
            sample_count=active["sample_count"],
            confirmed=active["confirmed"],
            status=status,
            convergence_seconds=convergence_seconds,
            converged_15m=_convergence_flag(
                convergence_seconds=convergence_seconds,
                duration_seconds=duration_seconds,
                window_seconds=900,
                censored=censored,
            ),
            converged_1h=_convergence_flag(
                convergence_seconds=convergence_seconds,
                duration_seconds=duration_seconds,
                window_seconds=3600,
                censored=censored,
            ),
            converged_4h=_convergence_flag(
                convergence_seconds=convergence_seconds,
                duration_seconds=duration_seconds,
                window_seconds=14400,
                censored=censored,
            ),
            gap_seconds=gap_seconds,
            max_adverse_deviation_bps=active["peak_deviation_bps"],
            regime=regime,
        ))
        active = None

    for point in points:
        if not _attribute_pair_slot(point, params).valid_sync:
            continue
        timestamp = point.sample_time
        if last_valid_time is not None:
            gap_seconds = (timestamp - last_valid_time).total_seconds()
            if gap_seconds > params.max_gap_seconds and active is not None:
                finish("censored_data_gap", last_valid_time, gap_seconds)
        last_valid_time = timestamp
        reference = baseline_by_day.get(timestamp.date())
        if reference is None or not reference.stable:
            if active is not None:
                finish("censored_baseline", timestamp)
            continue
        value = _basis_values(point)[basis_type]
        directed_deviation = direction * (value - reference.baseline_bps)
        if active is not None:
            active["peak_deviation_bps"] = max(
                active["peak_deviation_bps"], directed_deviation
            )
            active["sample_count"] += 1
            previous_event_time = active["last_event_time"]
            if ((timestamp - previous_event_time).total_seconds()
                    <= params.sample_seconds):
                active["confirmation_count"] += 1
            else:
                active["confirmation_count"] = 1
            active["last_event_time"] = timestamp
            active["confirmed"] = active["confirmation_count"] >= 2
            if abs(value - reference.baseline_bps) <= params.return_band_bps:
                active["convergence_time"] = timestamp
                finish("converged", timestamp)
            elif directed_deviation < threshold_bps:
                finish("no_convergence", timestamp)
            continue
        if directed_deviation >= threshold_bps:
            active = {
                "pair": point.pair,
                "start": timestamp,
                "baseline_bps": reference.baseline_bps,
                "peak_deviation_bps": directed_deviation,
                "sample_count": 1,
                "confirmation_count": 1,
                "confirmed": False,
                "last_event_time": timestamp,
                "convergence_time": None,
            }
    if active is not None and last_valid_time is not None:
        finish("censored_end", last_valid_time)
    return episodes


def summarize_episodes(episodes: list[AnomalyEpisode]) -> dict[str, object]:
    """Aggregate an episode group without treating unknowns as failures."""

    def convergence_metrics(name: str) -> dict[str, object]:
        values = [getattr(episode, name) for episode in episodes]
        known = [value for value in values if value is not None]
        count = sum(value is True for value in known)
        return {
            f"{name}_known": len(known),
            f"{name}_count": count,
            f"{name}_rate": count / len(known) if known else None,
        }

    result: dict[str, object] = {
        "anomaly_episodes": len(episodes),
        "confirmed_anomalies": sum(episode.confirmed for episode in episodes),
        "converged_episodes": sum(episode.status == "converged" for episode in episodes),
        "censored_episodes": sum(episode.status.startswith("censored") for episode in episodes),
        "average_duration_seconds": (
            sum((episode.end - episode.start).total_seconds() for episode in episodes)
            / len(episodes) if episodes else None
        ),
        "average_convergence_seconds": (
            sum(episode.convergence_seconds for episode in episodes
                if episode.convergence_seconds is not None)
            / sum(episode.convergence_seconds is not None for episode in episodes)
            if any(episode.convergence_seconds is not None for episode in episodes) else None
        ),
        "max_adverse_deviation_bps": (
            max(episode.peak_deviation_bps for episode in episodes)
            if episodes else None
        ),
    }
    for name in ("converged_15m", "converged_1h", "converged_4h"):
        result.update(convergence_metrics(name))
    return result


def estimate_cost_scenarios(
    *,
    entry_edge_bps: float,
    exit_edge_bps: float,
    venue_a: str,
    venue_b: str,
    fees: Mapping[str, float],
    notionals: Sequence[float] = (1000.0, 5000.0),
    extra_slippage_bps: Sequence[float] = (0.0, 2.0),
) -> list[dict[str, object]]:
    """Estimate four-fill net edge; this is not a fill or funding guarantee."""

    if venue_a not in fees or venue_b not in fees:
        raise ValueError("missing configured fee for cost estimate")
    gross_bps = entry_edge_bps - exit_edge_bps
    fee_bps = 2 * (fees[venue_a] + fees[venue_b])
    rows: list[dict[str, object]] = []
    for notional in notionals:
        for extra in extra_slippage_bps:
            slippage_bps = 4 * extra
            net_bps = gross_bps - fee_bps - slippage_bps
            rows.append({
                "notional_per_leg": float(notional),
                "extra_slippage_bps": float(extra),
                "gross_bps": gross_bps,
                "fee_bps": fee_bps,
                "slippage_bps": slippage_bps,
                "net_bps": net_bps,
                "gross_usd": gross_bps * notional / 10000,
                "fee_usd": fee_bps * notional / 10000,
                "slippage_usd": slippage_bps * notional / 10000,
                "net_usd": net_bps * notional / 10000,
                "funding_included": False,
                "fee_source": "configured_assumption",
            })
    return rows


def classify_candidate(metrics: Mapping[str, object]) -> dict[str, object]:
    """Classify by evidence gates before any PnL-like metric."""

    if not metrics.get("valid_sync_samples") or not metrics.get("coverage_eligible"):
        return {"classification": "C", "classification_reason": "data_quality_or_coverage"}
    if not metrics.get("stable_baseline"):
        return {"classification": "D", "classification_reason": "baseline_unstable"}
    if (
        int(metrics.get("confirmed_anomalies", 0)) >= 2
        and int(metrics.get("observed_convergence", 0)) >= 2
        and int(metrics.get("executable_convergence", 0)) >= 1
        and int(metrics.get("economically_viable_1000_2bps", 0)) >= 1
    ):
        return {"classification": "A", "classification_reason": "repeated_executable_and_cost_resilient"}
    return {"classification": "B", "classification_reason": "stable_but_event_or_cost_evidence_insufficient"}


FUNNEL_STAGES = (
    ("configured_pairs", "configured"),
    ("valid_synchronized_pairs", "valid_sync"),
    ("coverage_eligible", "coverage_eligible"),
    ("stable_baseline", "stable_baseline"),
    ("anomaly_episodes", "anomaly_episodes"),
    ("confirmed_anomalies", "confirmed_anomalies"),
    ("observed_convergence", "observed_convergence"),
    ("executable_convergence", "executable_convergence"),
    ("economically_viable_candidates", "economically_viable"),
)


def build_funnel_rows(pair_metrics: list[Mapping[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    denominator = len(pair_metrics)
    for stage, key in FUNNEL_STAGES:
        is_count = stage in {
            "anomaly_episodes", "confirmed_anomalies", "observed_convergence",
            "executable_convergence",
        }
        if is_count:
            candidate_count = sum(int(metrics.get(key, 0)) for metrics in pair_metrics)
        else:
            candidate_count = sum(bool(metrics.get(key)) for metrics in pair_metrics)
        reasons = Counter(
            str(metrics.get("exclusion_reason", "not_eligible"))
            for metrics in pair_metrics
            if not (int(metrics.get(key, 0)) if is_count else bool(metrics.get(key)))
        )
        rows.append({
            "scope": "global",
            "stage": stage,
            "candidate_count": candidate_count,
            "denominator": denominator,
            "excluded_count": max(denominator - candidate_count, 0),
            "exclusion_reasons": ";".join(
                f"{reason}:{count}" for reason, count in sorted(reasons.items())
            ),
        })
        for metrics in pair_metrics:
            passed = bool(metrics.get(key)) if not is_count else int(metrics.get(key, 0)) > 0
            rows.append({
                "scope": "pair",
                "pair_id": metrics.get("pair_id"),
                "stage": stage,
                "candidate_count": int(metrics.get(key, 0)) if is_count else int(passed),
                "denominator": 1,
                "excluded_count": int(not passed),
                "exclusion_reasons": "" if passed else str(metrics.get("exclusion_reason", "not_eligible")),
            })
    return rows


_OUTPUT_FIELDS = {
    "data_quality_report.csv": (
        "record_type", "symbol", "venue", "venue_symbol", "venue_a", "symbol_a",
        "venue_b", "symbol_b", "metric", "count", "denominator", "rate",
        "first_utc", "last_utc", "notes",
    ),
    "baseline_stability.csv": (
        "symbol", "venue_a", "symbol_a", "venue_b", "symbol_b", "regime", "window",
        "basis_type", "covered_days", "eligible_windows", "stable_windows",
        "unstable_windows", "median_daily_coverage", "median_baseline_bps",
        "median_q90_q10_bps", "max_q90_q10_bps", "median_abs_day_change_bps",
        "p90_abs_day_change_bps", "median_drift_bps_per_day", "status",
    ),
    "anomaly_convergence.csv": (
        "record_type", "symbol", "venue_a", "symbol_a", "venue_b", "symbol_b",
        "basis_type", "threshold_bps", "direction", "start_utc", "end_utc",
        "baseline_bps", "peak_deviation_bps", "sample_count", "confirmed", "status",
        "convergence_seconds", "converged_15m", "converged_1h", "converged_4h",
        "gap_seconds", "max_adverse_deviation_bps", "regime", "notes",
    ),
    "opportunity_funnel.csv": (
        "scope", "pair_id", "stage", "candidate_count", "denominator",
        "excluded_count", "exclusion_reasons",
    ),
    "candidate_ranking.csv": (
        "rank", "classification", "classification_reason", "symbol", "venue_a", "symbol_a",
        "venue_b", "symbol_b", "valid_sync_samples", "coverage_eligible",
        "stable_baseline", "anomaly_episodes", "confirmed_anomalies",
        "observed_convergence", "executable_convergence", "economically_viable_1000_2bps",
        "net_bps_1000_0bps", "net_bps_1000_2bps", "net_bps_5000_0bps",
        "net_bps_5000_2bps", "notes",
    ),
}


def _write_csv(path: Path, rows: list[Mapping[str, object]], preferred: Sequence[str]) -> None:
    keys = set().union(*(row.keys() for row in rows)) if rows else set()
    fields = list(preferred) + sorted(keys - set(preferred))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_v2_outputs(
    output: Path,
    *,
    quality_rows: list[Mapping[str, object]],
    baseline_rows: list[Mapping[str, object]],
    episode_rows: list[Mapping[str, object]],
    funnel_rows: list[Mapping[str, object]],
    ranking_rows: list[Mapping[str, object]],
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    data = {
        "data_quality_report.csv": quality_rows,
        "baseline_stability.csv": baseline_rows,
        "anomaly_convergence.csv": episode_rows,
        "opportunity_funnel.csv": funnel_rows,
        "candidate_ranking.csv": ranking_rows,
    }
    for filename, rows in data.items():
        _write_csv(output / filename, rows, _OUTPUT_FIELDS[filename])


def configured_pairs(
    configured: set[tuple[str, str, str]],
) -> list[Pair]:
    by_symbol: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for venue, venue_symbol, symbol in configured:
        by_symbol[symbol].append((venue, venue_symbol))
    return [
        Pair(symbol, a[0], a[1], b[0], b[1])
        for symbol, markets in sorted(by_symbol.items())
        for a, b in combinations(sorted(markets), 2)
        if a[0] != b[0]
    ]


def _create_configured_table(connection, configured: set[tuple[str, str, str]]) -> None:
    placeholders = ", ".join("(?, ?, ?)" for _ in configured)
    values = [value for row in sorted(configured) for value in row]
    connection.execute(
        "CREATE TEMP TABLE configured_markets "
        "(venue VARCHAR, venue_symbol VARCHAR, canonical_symbol VARCHAR)"
    )
    if values:
        connection.execute(f"INSERT INTO configured_markets VALUES {placeholders}", values)


def _create_configured_pairs_table(connection, configured: set[tuple[str, str, str]]) -> None:
    pairs = configured_pairs(configured)
    connection.execute(
        "CREATE TEMP TABLE configured_pairs "
        "(symbol VARCHAR, venue_a VARCHAR, symbol_a VARCHAR, "
        "venue_b VARCHAR, symbol_b VARCHAR)"
    )
    values = [value for pair in pairs for value in (
        pair.symbol, pair.venue_a, pair.symbol_a, pair.venue_b, pair.symbol_b,
    )]
    if values:
        placeholders = ", ".join("(?, ?, ?, ?, ?)" for _ in pairs)
        connection.execute(f"INSERT INTO configured_pairs VALUES {placeholders}", values)


def _create_latest_table(
    connection,
    files: list[str],
    *,
    table_name: str,
    configured: set[tuple[str, str, str]],
    sample_seconds: int,
    size_usd: int = 1000,
) -> None:
    buy = f"buy_{size_usd // 1000}k_vwap"
    sell = f"sell_{size_usd // 1000}k_vwap"
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.execute(
        f"""
        CREATE TEMP TABLE {table_name} AS
        SELECT
            CAST(r.sample_time AS TIMESTAMPTZ) AS sample_time,
            CAST(r.observed_at AS TIMESTAMPTZ) AS observed_at,
            r.venue,
            r.venue_symbol,
            r.canonical_symbol,
            r.{buy} AS buy_price,
            r.{sell} AS sell_price
        FROM read_parquet(?, union_by_name=true) AS r
        INNER JOIN configured_markets AS c
            ON c.venue = r.venue
           AND c.venue_symbol = r.venue_symbol
           AND c.canonical_symbol = r.canonical_symbol
        WHERE CAST(EXTRACT(EPOCH FROM CAST(r.sample_time AS TIMESTAMPTZ)) AS BIGINT) % ? = 0
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY r.sample_time, r.venue, r.venue_symbol, r.canonical_symbol
            ORDER BY r.observed_at DESC
        ) = 1
        """,
        [files, sample_seconds],
    )


def _market_source_counts(connection, files: list[str], sample_seconds: int) -> dict[tuple[str, str, str], int]:
    rows = connection.execute(
        """
        SELECT r.venue, r.venue_symbol, r.canonical_symbol, COUNT(*)
        FROM read_parquet(?, union_by_name=true) AS r
        INNER JOIN configured_markets AS c
            ON c.venue = r.venue
           AND c.venue_symbol = r.venue_symbol
           AND c.canonical_symbol = r.canonical_symbol
        WHERE CAST(EXTRACT(EPOCH FROM CAST(r.sample_time AS TIMESTAMPTZ)) AS BIGINT) % ? = 0
        GROUP BY 1, 2, 3
        """,
        [files, sample_seconds],
    ).fetchall()
    return {(str(venue), str(venue_symbol), str(symbol)): int(count)
            for venue, venue_symbol, symbol, count in rows}


def _market_latest_stats(connection, table_name: str) -> dict[tuple[str, str, str], dict[str, object]]:
    valid_vwap = "buy_price IS NOT NULL AND sell_price IS NOT NULL AND isfinite(buy_price) AND isfinite(sell_price) AND buy_price > 0 AND sell_price > 0 AND buy_price >= sell_price"
    rows = connection.execute(
        f"""
        SELECT venue, venue_symbol, canonical_symbol,
               COUNT(*) AS sample_slots,
               SUM(CASE WHEN {valid_vwap} THEN 1 ELSE 0 END) AS valid_vwap_slots,
               SUM(CASE WHEN observed_at > sample_time THEN 1 ELSE 0 END) AS after_sample_slots,
               SUM(CASE WHEN sample_time >= observed_at
                              AND EXTRACT(EPOCH FROM (sample_time - observed_at)) > 12
                        THEN 1 ELSE 0 END) AS stale_slots,
               MIN(sample_time), MAX(sample_time)
        FROM {table_name}
        GROUP BY 1, 2, 3
        """
    ).fetchall()
    return {
        (str(venue), str(venue_symbol), str(symbol)): {
            "sample_slots": int(sample_slots),
            "valid_vwap_slots": int(valid_vwap_slots or 0),
            "after_sample_slots": int(after_sample_slots or 0),
            "stale_slots": int(stale_slots or 0),
            "first_utc": _as_datetime(first_time).isoformat() if first_time else None,
            "last_utc": _as_datetime(last_time).isoformat() if last_time else None,
        }
        for venue, venue_symbol, symbol, sample_slots, valid_vwap_slots,
        after_sample_slots, stale_slots, first_time, last_time in rows
    }


def _pair_latest_stats(connection, table_name: str) -> dict[Pair, dict[str, int]]:
    valid_vwap_a = "a.buy_price IS NOT NULL AND a.sell_price IS NOT NULL AND isfinite(a.buy_price) AND isfinite(a.sell_price) AND a.buy_price > 0 AND a.sell_price > 0 AND a.buy_price >= a.sell_price"
    valid_vwap_b = "b.buy_price IS NOT NULL AND b.sell_price IS NOT NULL AND isfinite(b.buy_price) AND isfinite(b.sell_price) AND b.buy_price > 0 AND b.sell_price > 0 AND b.buy_price >= b.sell_price"
    valid_time_a = "a.observed_at <= a.sample_time AND EXTRACT(EPOCH FROM (a.sample_time - a.observed_at)) <= 12"
    valid_time_b = "b.observed_at <= b.sample_time AND EXTRACT(EPOCH FROM (b.sample_time - b.observed_at)) <= 12"
    both_valid = f"({valid_vwap_a}) AND ({valid_vwap_b}) AND ({valid_time_a}) AND ({valid_time_b})"
    pair_vwap_a = valid_vwap_a.replace("a.buy_price", "p.buy_a").replace("a.sell_price", "p.sell_a")
    pair_vwap_b = valid_vwap_b.replace("b.buy_price", "p.buy_b").replace("b.sell_price", "p.sell_b")
    pair_time_a = valid_time_a.replace("a.observed_at", "p.observed_a").replace("a.sample_time", "p.sample_time")
    pair_time_b = valid_time_b.replace("b.observed_at", "p.observed_b").replace("b.sample_time", "p.sample_time")
    pair_both_valid = f"({pair_vwap_a}) AND ({pair_vwap_b}) AND ({pair_time_a}) AND ({pair_time_b})"
    rows = connection.execute(
        f"""
        WITH pair_slots AS (
            SELECT a.sample_time,
                   c.venue_a, c.symbol_a, c.venue_b, c.symbol_b,
                   c.symbol,
                   a.observed_at AS observed_a, b.observed_at AS observed_b,
                   a.buy_price AS buy_a, a.sell_price AS sell_a,
                   b.buy_price AS buy_b, b.sell_price AS sell_b
            FROM configured_pairs AS c
            INNER JOIN {table_name} AS a
                ON a.venue = c.venue_a
               AND a.venue_symbol = c.symbol_a
               AND a.canonical_symbol = c.symbol
            INNER JOIN {table_name} AS b
                ON b.sample_time = a.sample_time
               AND b.venue = c.venue_b
               AND b.venue_symbol = c.symbol_b
               AND b.canonical_symbol = c.symbol
        )
        SELECT symbol, venue_a, symbol_a, venue_b, symbol_b,
               COUNT(*) AS common_slots,
               SUM(CASE WHEN NOT ({pair_vwap_a})
                              OR NOT ({pair_vwap_b})
                        THEN 1 ELSE 0 END) AS invalid_vwap_slots,
               SUM(CASE WHEN p.observed_a > p.sample_time OR p.observed_b > p.sample_time
                        THEN 1 ELSE 0 END) AS after_sample_slots,
               SUM(CASE WHEN p.observed_a <= p.sample_time
                              AND p.observed_b <= p.sample_time
                              AND (EXTRACT(EPOCH FROM (p.sample_time - p.observed_a)) > 12
                                   OR EXTRACT(EPOCH FROM (p.sample_time - p.observed_b)) > 12)
                        THEN 1 ELSE 0 END) AS stale_slots,
               SUM(CASE WHEN ({pair_both_valid})
                              AND ABS(EXTRACT(EPOCH FROM (p.observed_a - p.observed_b))) > 3
                        THEN 1 ELSE 0 END) AS skew_3_slots,
               SUM(CASE WHEN ({pair_both_valid})
                              AND ABS(EXTRACT(EPOCH FROM (p.observed_a - p.observed_b))) <= 3
                        THEN 1 ELSE 0 END) AS valid_sync_3,
               SUM(CASE WHEN ({pair_both_valid})
                              AND ABS(EXTRACT(EPOCH FROM (p.observed_a - p.observed_b))) <= 5
                        THEN 1 ELSE 0 END) AS valid_sync_5,
               SUM(CASE WHEN ({pair_both_valid})
                              AND ABS(EXTRACT(EPOCH FROM (p.observed_a - p.observed_b))) <= 10
                        THEN 1 ELSE 0 END) AS valid_sync_10
        FROM pair_slots AS p
        GROUP BY 1, 2, 3, 4, 5
        """
    ).fetchall()
    result: dict[Pair, dict[str, int]] = {}
    for row in rows:
        symbol, venue_a, symbol_a, venue_b, symbol_b, *counts = row
        pair = Pair(str(symbol), str(venue_a), str(symbol_a), str(venue_b), str(symbol_b))
        result[pair] = {
            key: int(value or 0)
            for key, value in zip(
                ("common_slots", "invalid_vwap_slots", "after_sample_slots", "stale_slots",
                 "skew_3_slots", "valid_sync_3", "valid_sync_5", "valid_sync_10"),
                counts,
                strict=True,
            )
        }
    return result


def _metric_row(
    *,
    record_type: str,
    metric: str,
    count: int,
    denominator: int | None,
    notes: str = "",
    **identity: object,
) -> dict[str, object]:
    rate = count / denominator if denominator else None
    return {
        "record_type": record_type,
        "metric": metric,
        "count": count,
        "denominator": denominator,
        "rate": rate,
        "notes": notes,
        **identity,
    }


def collect_data_quality(
    files: list[str],
    configured: set[tuple[str, str, str]],
    *,
    params: AnalysisParams = AnalysisParams(),
) -> tuple[list[dict[str, object]], dict[Pair, dict[str, object]]]:
    """Attribute 60s losses and compare valid 10s versus 60s pair slots."""

    import duckdb  # type: ignore[import-untyped]

    connection = duckdb.connect()
    connection.execute("PRAGMA threads=2")
    try:
        _create_configured_table(connection, configured)
        _create_configured_pairs_table(connection, configured)
        raw_60 = _market_source_counts(connection, files, params.sample_seconds)
        _create_latest_table(
            connection, files, table_name="latest_60", configured=configured,
            sample_seconds=params.sample_seconds,
        )
        latest_60 = _market_latest_stats(connection, "latest_60")
        pair_60 = _pair_latest_stats(connection, "latest_60")
        connection.execute("DROP TABLE latest_60")
        raw_10 = _market_source_counts(connection, files, params.raw_sample_seconds)
        _create_latest_table(
            connection, files, table_name="latest_10", configured=configured,
            sample_seconds=params.raw_sample_seconds,
        )
        latest_10 = _market_latest_stats(connection, "latest_10")
        pair_10 = _pair_latest_stats(connection, "latest_10")
    finally:
        connection.close()

    pairs = configured_pairs(configured)
    quality_rows: list[dict[str, object]] = []
    for market in sorted(configured):
        venue, venue_symbol, symbol = market
        latest = latest_60.get(market, {})
        latest10 = latest_10.get(market, {})
        raw_rows = raw_60.get(market, 0)
        for metric, count, denominator, notes in (
            ("raw_sample_rows_60s", raw_rows, raw_rows, "source rows at 60s grid; duplicates retained"),
            ("sampled_market_slots_60s", int(latest.get("sample_slots", 0)), raw_rows, "latest row per market and sample_time"),
            ("valid_vwap_slots_60s", int(latest.get("valid_vwap_slots", 0)), int(latest.get("sample_slots", 0)), "buy/sell VWAP finite, positive, non-crossed"),
            ("observed_after_sample_slots_60s", int(latest.get("after_sample_slots", 0)), int(latest.get("sample_slots", 0)), "observed_at after sample_time"),
            ("stale_slots_60s", int(latest.get("stale_slots", 0)), int(latest.get("sample_slots", 0)), "quote age > 12s"),
            ("raw_sample_rows_10s", raw_10.get(market, 0), raw_10.get(market, 0), "source rows at 10s grid; duplicates retained"),
            ("sampled_market_slots_10s", int(latest10.get("sample_slots", 0)), raw_10.get(market, 0), "latest row per market and 10s sample_time"),
        ):
            quality_rows.append(_metric_row(
                record_type="market", metric=metric, count=count,
                denominator=denominator, first_utc=latest.get("first_utc"),
                last_utc=latest.get("last_utc"), symbol=symbol, venue=venue,
                venue_symbol=venue_symbol, notes=notes,
            ))

    pair_metrics: dict[Pair, dict[str, object]] = {}
    for pair in pairs:
        a_key = (pair.venue_a, pair.symbol_a, pair.symbol)
        b_key = (pair.venue_b, pair.symbol_b, pair.symbol)
        stats = pair_60.get(pair, {})
        stats10 = pair_10.get(pair, {})
        count_a = int(latest_60.get(a_key, {}).get("sample_slots", 0))
        count_b = int(latest_60.get(b_key, {}).get("sample_slots", 0))
        common = int(stats.get("common_slots", 0))
        union_slots = count_a + count_b - common
        valid_60 = int(stats.get("valid_sync_3", 0))
        valid_10 = int(stats10.get("valid_sync_3", 0))
        pair_metrics[pair] = {
            "pair_id": _pair_id(pair),
            "valid_sync_samples": valid_60,
            "valid_sync_samples_10s": valid_10,
            "sampling_loss_slots": max(valid_10 - valid_60, 0),
            "pair_union_slots": union_slots,
            "missing_leg_slots": max(union_slots - common, 0),
            "quality_loss_slots": max(union_slots - valid_60, 0),
        }
        identity = {
            "symbol": pair.symbol, "venue_a": pair.venue_a, "symbol_a": pair.symbol_a,
            "venue_b": pair.venue_b, "symbol_b": pair.symbol_b,
        }
        metrics = (
            ("pair_union_slots_60s", union_slots, union_slots, "union of latest 60s slots across both legs"),
            ("common_sample_time_slots_60s", common, union_slots, "both legs have exact same sample_time"),
            ("missing_leg_slots_60s", max(union_slots - common, 0), union_slots, "one leg has no exact 60s slot"),
            ("invalid_vwap_slots_60s", int(stats.get("invalid_vwap_slots", 0)), common, "both legs present but at least one VWAP invalid"),
            ("observed_after_sample_slots_60s", int(stats.get("after_sample_slots", 0)), common, "at least one observed_at after sample_time"),
            ("stale_slots_60s", int(stats.get("stale_slots", 0)), common, "at least one leg quote age > 12s"),
            ("observed_at_skew_slots_3s", int(stats.get("skew_3_slots", 0)), common, "both legs otherwise valid but skew > 3s"),
            ("valid_sync_slots_3s", valid_60, common, "canonical V1 synchronization rule"),
            ("valid_sync_slots_5s", int(stats.get("valid_sync_5", 0)), common, "sensitivity only; not used for canonical ranking"),
            ("valid_sync_slots_10s_skew", int(stats.get("valid_sync_10", 0)), common, "sensitivity only; not used for canonical ranking"),
            ("valid_sync_slots_10s_sample", valid_10, int(stats10.get("common_slots", 0)), "10s raw sample grid with canonical 3s skew"),
            ("sampling_loss_slots_60s_vs_10s", max(valid_10 - valid_60, 0), valid_10, "valid 10s opportunities absent from 60s grid"),
            ("sample_time_mismatch_slots", 0, common, "60s query is exact sample_time; no mismatch observed in aligned slot set"),
        )
        for metric, count, denominator, notes in metrics:
            quality_rows.append(_metric_row(
                record_type="pair", metric=metric, count=count,
                denominator=denominator, **identity, notes=notes,
            ))

    system_totals = Counter()
    for market in configured:
        latest = latest_60.get(market, {})
        system_totals["sampled_market_slots_60s"] += int(latest.get("sample_slots", 0))
        system_totals["valid_vwap_slots_60s"] += int(latest.get("valid_vwap_slots", 0))
        system_totals["observed_after_sample_slots_60s"] += int(latest.get("after_sample_slots", 0))
        system_totals["stale_slots_60s"] += int(latest.get("stale_slots", 0))
        system_totals["raw_sample_rows_60s"] += raw_60.get(market, 0)
    system_totals["valid_sync_slots_3s"] = sum(int(stats.get("valid_sync_3", 0)) for stats in pair_60.values())
    system_totals["valid_sync_slots_5s"] = sum(int(stats.get("valid_sync_5", 0)) for stats in pair_60.values())
    system_totals["valid_sync_slots_10s_skew"] = sum(int(stats.get("valid_sync_10", 0)) for stats in pair_60.values())
    system_totals["valid_sync_slots_10s_sample"] = sum(int(stats.get("valid_sync_3", 0)) for stats in pair_10.values())
    system_totals["sampling_loss_slots_60s_vs_10s"] = sum(
        max(int(pair_10.get(pair, {}).get("valid_sync_3", 0))
            - int(pair_60.get(pair, {}).get("valid_sync_3", 0)), 0)
        for pair in pairs
    )
    for metric, count in sorted(system_totals.items()):
        quality_rows.append(_metric_row(
            record_type="system", metric=metric, count=count,
            denominator=None, notes="aggregate across configured markets/pairs",
        ))
    return quality_rows, pair_metrics


def _pair_id(pair: Pair) -> str:
    return f"{pair.symbol}:{pair.venue_a}:{pair.symbol_a}__{pair.venue_b}:{pair.symbol_b}"


@dataclass
class SizeAnalysis:
    size_usd: int
    histories: dict[Pair, dict[str, dict[date, DailySeries]]]
    baseline_rows: list[dict[str, object]]
    episodes: list[AnomalyEpisode]
    episode_summary_rows: list[dict[str, object]]


def _is_us_equity_pair(pair: Pair) -> bool:
    return ".US_" in pair.symbol_a or ".US_" in pair.symbol_b


def _iter_pair_groups(
    files: list[str],
    configured: set[tuple[str, str, str]],
    *,
    size_usd: int,
    sample_seconds: int,
) -> Iterator[tuple[Pair, list[PairPoint]]]:
    stream = iter_pair_points(
        files, configured=configured, size_usd=size_usd,
        sample_seconds=sample_seconds,
    )
    for pair, group in groupby(stream, key=lambda point: point.pair):
        yield pair, list(group)


def _episode_row(episode: AnomalyEpisode, *, size_usd: int, record_type: str = "episode") -> dict[str, object]:
    return {
        "record_type": record_type,
        "size_usd": size_usd,
        "symbol": episode.pair.symbol,
        "venue_a": episode.pair.venue_a,
        "symbol_a": episode.pair.symbol_a,
        "venue_b": episode.pair.venue_b,
        "symbol_b": episode.pair.symbol_b,
        "basis_type": episode.basis_type,
        "threshold_bps": episode.threshold_bps,
        "direction": episode.direction,
        "start_utc": episode.start.isoformat(),
        "end_utc": episode.end.isoformat(),
        "baseline_bps": episode.baseline_bps,
        "peak_deviation_bps": episode.peak_deviation_bps,
        "sample_count": episode.sample_count,
        "confirmed": episode.confirmed,
        "status": episode.status,
        "convergence_seconds": episode.convergence_seconds,
        "converged_15m": episode.converged_15m,
        "converged_1h": episode.converged_1h,
        "converged_4h": episode.converged_4h,
        "gap_seconds": episode.gap_seconds,
        "max_adverse_deviation_bps": episode.max_adverse_deviation_bps,
        "regime": episode.regime,
        "event_id": (
            f"{_pair_id(episode.pair)}|{episode.basis_type}|"
            f"{episode.threshold_bps:g}|{episode.direction}|{episode.start.isoformat()}"
        ),
    }


def _summary_row(
    pair: Pair,
    *,
    basis_type: str,
    threshold_bps: float,
    direction: int,
    size_usd: int,
    summary: Mapping[str, object],
    regime: str = "all",
) -> dict[str, object]:
    return {
        "record_type": "summary",
        "size_usd": size_usd,
        "symbol": pair.symbol,
        "venue_a": pair.venue_a,
        "symbol_a": pair.symbol_a,
        "venue_b": pair.venue_b,
        "symbol_b": pair.symbol_b,
        "basis_type": basis_type,
        "threshold_bps": threshold_bps,
        "direction": direction,
        "regime": regime,
        **summary,
    }


def _collect_size_analysis(
    files: list[str],
    configured: set[tuple[str, str, str]],
    *,
    size_usd: int,
    params: AnalysisParams,
    thresholds: Sequence[float] = (10.0, 15.0, 20.0, 25.0),
) -> SizeAnalysis:
    pairs = configured_pairs(configured)
    histories: dict[Pair, dict[str, dict[date, DailySeries]]] = {
        pair: {"all": {}} for pair in pairs
    }
    for pair, points in _iter_pair_groups(
        files, configured, size_usd=size_usd, sample_seconds=params.sample_seconds
    ):
        histories[pair]["all"] = build_daily_series(iter(points), params=params)
        if _is_us_equity_pair(pair):
            for regime in ("us_regular", "us_extended", "us_closed"):
                histories[pair][regime] = build_daily_series(
                    iter(points), params=params, regime=regime
                )
    baseline_rows: list[dict[str, object]] = []
    for pair in pairs:
        for regime, history in histories[pair].items():
            for row in build_baseline_rows(pair, history, params=params, regime=regime):
                row["size_usd"] = size_usd
                baseline_rows.append(row)

    baseline_by_pair: dict[Pair, dict[date, BaselineReference]] = {}
    for pair in pairs:
        history = histories[pair]["all"]
        baseline_by_pair[pair] = {
            today: reference
            for today in sorted(history)
            if (reference := causal_baseline(
                history,
                today=today,
                basis_type="mid",
                lookback_days=7,
                min_days=params.min_baseline_days,
                use_daily_medians=True,
                stable_range_bps=params.stable_range_bps,
                min_daily_coverage=params.min_daily_coverage,
            )) is not None
        }

    episodes: list[AnomalyEpisode] = []
    summary_rows: list[dict[str, object]] = []
    for pair, points in _iter_pair_groups(
        files, configured, size_usd=size_usd, sample_seconds=params.sample_seconds
    ):
        references = baseline_by_pair.get(pair, {})
        if not references:
            continue
        for basis_type in BASIS_TYPES:
            for direction in (1, -1):
                for threshold in thresholds:
                    current = detect_episodes(
                        iter(points), baseline_by_day=references,
                        basis_type=basis_type, threshold_bps=threshold,
                        direction=direction, params=params,
                    )
                    episodes.extend(current)
                    summary_rows.append(_summary_row(
                        pair, basis_type=basis_type, threshold_bps=threshold,
                        direction=direction, size_usd=size_usd,
                        summary=summarize_episodes(current),
                    ))
    return SizeAnalysis(
        size_usd=size_usd,
        histories=histories,
        baseline_rows=baseline_rows,
        episodes=episodes,
        episode_summary_rows=summary_rows,
    )


def _episode_net_bps(
    episode: AnomalyEpisode,
    *,
    fees: Mapping[str, float],
    extra_slippage_bps: float,
) -> float:
    gross_bps = episode.peak_deviation_bps - 5.0
    fee_bps = 2 * (fees[episode.pair.venue_a] + fees[episode.pair.venue_b])
    return gross_bps - fee_bps - 4 * extra_slippage_bps


def build_candidate_rows(
    configured: set[tuple[str, str, str]],
    quality_metrics: Mapping[Pair, Mapping[str, object]],
    primary: SizeAnalysis,
    larger: SizeAnalysis | None,
    *,
    fees: Mapping[str, float],
    params: AnalysisParams,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    pairs = configured_pairs(configured)
    ranking: list[dict[str, object]] = []
    funnel_metrics: list[dict[str, object]] = []
    baseline_by_pair: dict[Pair, dict[str, object]] = {}
    for row in primary.baseline_rows:
        if row["regime"] == "all" and row["window"] == "7d" and row["basis_type"] == "mid":
            baseline_by_pair[Pair(
                str(row["symbol"]), str(row["venue_a"]), str(row["symbol_a"]),
                str(row["venue_b"]), str(row["symbol_b"]),
            )] = row
    for pair in pairs:
        quality = dict(quality_metrics.get(pair, {}))
        baseline = baseline_by_pair.get(pair, {})
        primary_events = [
            episode for episode in primary.episodes
            if episode.pair == pair and episode.basis_type == "mid"
            and episode.threshold_bps == 20.0
        ]
        executable_events = [
            episode for episode in primary.episodes
            if episode.pair == pair and episode.basis_type.startswith("exec_")
            and episode.threshold_bps == 20.0
        ]
        if larger is not None:
            executable_events_5k = [
                episode for episode in larger.episodes
                if episode.pair == pair and episode.basis_type.startswith("exec_")
                and episode.threshold_bps == 20.0
            ]
        else:
            executable_events_5k = []
        stable_baseline = baseline.get("status") == "stable" and int(baseline.get("stable_windows", 0)) >= 5
        history = primary.histories[pair]["all"]
        coverage_eligible = sum(
            series.coverage >= params.min_daily_coverage for series in history.values()
        ) >= params.min_baseline_days
        confirmed = sum(episode.confirmed for episode in primary_events)
        observed = sum(episode.status == "converged" for episode in primary_events)
        executable = sum(episode.status == "converged" for episode in executable_events)
        econ_by_scenario: dict[tuple[float, float], list[float]] = defaultdict(list)
        for episode in executable_events:
            if episode.status != "converged":
                continue
            for extra in (0.0, 2.0):
                econ_by_scenario[(1000.0, extra)].append(
                    _episode_net_bps(episode, fees=fees, extra_slippage_bps=extra)
                )
        for episode in executable_events_5k:
            if episode.status == "converged":
                for extra in (0.0, 2.0):
                    econ_by_scenario[(5000.0, extra)].append(
                        _episode_net_bps(episode, fees=fees, extra_slippage_bps=extra)
                    )
        econ_1000_0 = sum(value > 0 for value in econ_by_scenario[(1000.0, 0.0)])
        econ_1000_2 = sum(value > 0 for value in econ_by_scenario[(1000.0, 2.0)])
        econ_5000_0 = sum(value > 0 for value in econ_by_scenario[(5000.0, 0.0)])
        econ_5000_2 = sum(value > 0 for value in econ_by_scenario[(5000.0, 2.0)])
        metrics: dict[str, object] = {
            "pair_id": _pair_id(pair),
            "configured": True,
            "valid_sync": bool(quality.get("valid_sync_samples")),
            "valid_sync_samples": int(quality.get("valid_sync_samples", 0)),
            "coverage_eligible": coverage_eligible,
            "stable_baseline": stable_baseline,
            "anomaly_episodes": len(primary_events),
            "confirmed_anomalies": confirmed,
            "observed_convergence": observed,
            "executable_convergence": executable,
            "economically_viable": econ_1000_2,
            "economically_viable_1000_2bps": econ_1000_2,
            "exclusion_reason": (
                "no_valid_sync" if not quality.get("valid_sync_samples")
                else "coverage" if not coverage_eligible
                else "baseline_unstable" if not stable_baseline
                else "event_or_cost_evidence"
            ),
        }
        classification = classify_candidate(metrics)
        net_values = {
            (1000.0, 0.0): econ_by_scenario[(1000.0, 0.0)],
            (1000.0, 2.0): econ_by_scenario[(1000.0, 2.0)],
            (5000.0, 0.0): econ_by_scenario[(5000.0, 0.0)],
            (5000.0, 2.0): econ_by_scenario[(5000.0, 2.0)],
        }
        ranking.append({
            **classification,
            **metrics,
            "symbol": pair.symbol, "venue_a": pair.venue_a, "symbol_a": pair.symbol_a,
            "venue_b": pair.venue_b, "symbol_b": pair.symbol_b,
            "stable_windows": baseline.get("stable_windows", 0),
            "unstable_windows": baseline.get("unstable_windows", 0),
            "net_bps_1000_0bps": median(net_values[(1000.0, 0.0)]) if net_values[(1000.0, 0.0)] else None,
            "net_bps_1000_2bps": median(net_values[(1000.0, 2.0)]) if net_values[(1000.0, 2.0)] else None,
            "net_bps_5000_0bps": median(net_values[(5000.0, 0.0)]) if net_values[(5000.0, 0.0)] else None,
            "net_bps_5000_2bps": median(net_values[(5000.0, 2.0)]) if net_values[(5000.0, 2.0)] else None,
            "notes": "Potential edge estimate from executable basis episodes; not a fill or funding-adjusted PnL.",
        })
        funnel_metrics.append(metrics)
    class_order = {"A": 0, "B": 1, "C": 2, "D": 3}
    ranking.sort(key=lambda row: (
        class_order[str(row["classification"])],
        -int(row.get("executable_convergence", 0)),
        -int(row.get("observed_convergence", 0)),
        -int(row.get("confirmed_anomalies", 0)),
        -int(row.get("valid_sync_samples", 0)),
        str(row["symbol"]),
    ))
    for rank, row in enumerate(ranking, start=1):
        row["rank"] = rank
    return ranking, funnel_metrics


def _read_v1_gap_trades(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [row for row in csv.DictReader(handle) if row.get("exit_reason") == "data_gap"]


def audit_v1_gap_events(
    files: list[str],
    gap_trades: list[Mapping[str, str]],
    *,
    params: AnalysisParams = AnalysisParams(),
    size_usd: int = 1000,
) -> list[dict[str, object]]:
    """Reconstruct each V1 gap with 10s observations around the outage."""

    import duckdb  # type: ignore[import-untyped]

    rows: list[dict[str, object]] = []
    for index, trade in enumerate(gap_trades, start=1):
        pair = Pair(
            trade["symbol"], trade["venue_a"], trade["symbol_a"],
            trade["venue_b"], trade["symbol_b"],
        )
        entry = _as_datetime(trade["entry_utc"])
        exit_time = _as_datetime(trade["exit_utc"])
        query_start = entry - timedelta(minutes=5)
        query_end = exit_time + timedelta(minutes=1)
        connection = duckdb.connect()
        try:
            connection.execute(
                "CREATE TEMP TABLE configured_markets "
                "(venue VARCHAR, venue_symbol VARCHAR, canonical_symbol VARCHAR)"
            )
            connection.execute(
                "INSERT INTO configured_markets VALUES (?, ?, ?), (?, ?, ?)",
                [pair.venue_a, pair.symbol_a, pair.symbol,
                 pair.venue_b, pair.symbol_b, pair.symbol],
            )
            _create_latest_table(
                connection, files, table_name="gap_latest", configured={
                    (pair.venue_a, pair.symbol_a, pair.symbol),
                    (pair.venue_b, pair.symbol_b, pair.symbol),
                }, sample_seconds=params.raw_sample_seconds, size_usd=size_usd,
            )
            raw_rows = connection.execute(
                """
                SELECT sample_time, observed_at, venue, venue_symbol,
                       canonical_symbol, buy_price, sell_price
                FROM gap_latest
                WHERE sample_time BETWEEN ? AND ?
                ORDER BY sample_time, venue
                """,
                [query_start, query_end],
            ).fetchall()
        finally:
            connection.close()
        by_time: dict[datetime, dict[str, MarketObservation]] = defaultdict(dict)
        for sample_time, observed_at, venue, venue_symbol, symbol, buy, sell in raw_rows:
            timestamp = _as_datetime(sample_time)
            by_time[timestamp][str(venue)] = MarketObservation(
                sample_time=timestamp,
                observed_at=_as_datetime(observed_at),
                venue=str(venue),
                venue_symbol=str(venue_symbol),
                symbol=str(symbol),
                buy=float(buy) if buy is not None else None,
                sell=float(sell) if sell is not None else None,
            )
        points: list[tuple[datetime, PairSlotAssessment]] = []
        for timestamp, observations in sorted(by_time.items()):
            assessment = _attribute_pair_slot(
                PairPoint(
                    sample_time=timestamp,
                    pair=pair,
                    a=observations.get(pair.venue_a),
                    b=observations.get(pair.venue_b),
                ),
                params,
            )
            points.append((timestamp, assessment))
        valid_before_exit = [
            timestamp for timestamp, assessment in points
            if timestamp < exit_time and assessment.valid_sync
        ]
        gap_start = max(valid_before_exit) if valid_before_exit else entry
        in_gap = [
            (timestamp, assessment) for timestamp, assessment in points
            if gap_start < timestamp < exit_time
        ]
        valid_10s = sum(assessment.valid_sync for _, assessment in in_gap)
        reason_counts: Counter[str] = Counter()
        for _, assessment in in_gap:
            for reason in assessment.reasons:
                reason_counts[reason] += 1
        if valid_10s:
            classification = "sampling_loss_or_recoverable_10s"
        elif reason_counts.get("missing_a", 0) or reason_counts.get("missing_b", 0):
            classification = "true_data_gap_missing_leg"
        elif reason_counts.get("observed_at_skew", 0):
            classification = "synchronization_filter"
        elif any("invalid_vwap" in reason for reason in reason_counts):
            classification = "invalid_vwap"
        elif any("stale" in reason for reason in reason_counts):
            classification = "stale_quote"
        elif any("sample_time_mismatch" in reason for reason in reason_counts):
            classification = "sample_time_mismatch"
        else:
            classification = "unknown_no_valid_10s_observation"
        rows.append({
            "record_type": "v1_data_gap_event",
            "event_id": f"v1-gap-{index:02d}",
            "symbol": pair.symbol,
            "venue_a": pair.venue_a,
            "symbol_a": pair.symbol_a,
            "venue_b": pair.venue_b,
            "symbol_b": pair.symbol_b,
            "entry_utc": entry.isoformat(),
            "v1_exit_utc": exit_time.isoformat(),
            "gap_start_utc": gap_start.isoformat(),
            "gap_end_utc": exit_time.isoformat(),
            "gap_duration_seconds": (exit_time - gap_start).total_seconds(),
            "raw_10s_slots_in_gap": len(in_gap),
            "valid_10s_sync_slots_in_gap": valid_10s,
            "ten_second_data_available": bool(in_gap),
            "recoverable_without_future_data": bool(valid_10s),
            "classification": classification,
            "dominant_reasons": ";".join(
                f"{reason}:{count}" for reason, count in reason_counts.most_common()
            ),
            "notes": "V1 data_gap exit remains excluded from scored convergence; 10s audit is diagnostic only.",
        })
    return rows


def _append_candidate_quality_rows(
    quality_rows: list[dict[str, object]],
    ranking_rows: list[Mapping[str, object]],
) -> None:
    for row in ranking_rows:
        identity = {
            "symbol": row["symbol"], "venue_a": row["venue_a"],
            "symbol_a": row["symbol_a"], "venue_b": row["venue_b"],
            "symbol_b": row["symbol_b"],
        }
        for metric, value, note in (
            ("coverage_eligible", int(bool(row.get("coverage_eligible"))), "requires >=5 complete days at >=60% coverage"),
            ("stable_baseline", int(bool(row.get("stable_baseline"))), "7d mid baseline with >=5 stable windows"),
            ("anomaly_episodes_20bps_mid", int(row.get("anomaly_episodes", 0)), "causal 7d stable baseline; both directions"),
            ("confirmed_anomalies_20bps_mid", int(row.get("confirmed_anomalies", 0)), "at least two consecutive sampled observations"),
            ("observed_convergence_20bps_mid", int(row.get("observed_convergence", 0)), "natural return within 5bps"),
            ("executable_convergence_20bps", int(row.get("executable_convergence", 0)), "directional VWAP basis episode convergence"),
            ("economically_viable_1000_2bps", int(row.get("economically_viable_1000_2bps", 0)), "potential edge after configured fee and +2bps/fill; not PnL"),
        ):
            quality_rows.append(_metric_row(
                record_type="pair", metric=metric, count=value,
                denominator=None, **identity, notes=note,
            ))


def main(argv: list[str] | None = None) -> int:
    from radar.config import load_config

    parser = __import__("argparse").ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--data-root", type=Path)
    source.add_argument("--csv-glob")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--v1-trades", type=Path)
    parser.add_argument("--sample-seconds", type=int, default=60)
    parser.add_argument("--raw-sample-seconds", type=int, default=10)
    parser.add_argument("--size-usd", type=int, choices=(1000, 5000), default=1000)
    args = parser.parse_args(argv)
    if args.csv_glob:
        raise SystemExit("V2 currently requires Production Parquet via --data-root")
    paths = sorted(str(path) for path in args.data_root.glob("market/date=*/part-*.parquet"))
    if not paths:
        parser.error("no input market Parquet files matched")
    config = load_config(args.config)
    configured = {
        (market.venue, market.venue_symbol, market.canonical_symbol)
        for market in config.markets if market.enabled
    }
    params = AnalysisParams(
        sample_seconds=args.sample_seconds,
        raw_sample_seconds=args.raw_sample_seconds,
    )
    quality_rows, quality_metrics = collect_data_quality(
        paths, configured, params=params,
    )
    primary = _collect_size_analysis(
        paths, configured, size_usd=args.size_usd, params=params,
    )
    larger = None
    if args.size_usd == 1000:
        larger = _collect_size_analysis(paths, configured, size_usd=5000, params=params)
    ranking_rows, funnel_metrics = build_candidate_rows(
        configured, quality_metrics, primary, larger,
        fees=config.fees_bps, params=params,
    )
    _append_candidate_quality_rows(quality_rows, ranking_rows)
    gap_rows = audit_v1_gap_events(
        paths, _read_v1_gap_trades(args.v1_trades), params=params,
        size_usd=args.size_usd,
    )
    quality_rows.extend(gap_rows)
    episode_rows = [
        _episode_row(episode, size_usd=primary.size_usd)
        for episode in primary.episodes
    ] + primary.episode_summary_rows
    funnel_rows = build_funnel_rows(funnel_metrics)
    write_v2_outputs(
        args.output,
        quality_rows=quality_rows,
        baseline_rows=primary.baseline_rows,
        episode_rows=episode_rows,
        funnel_rows=funnel_rows,
        ranking_rows=ranking_rows,
    )
    summary = {
        "configured_pairs": len(configured_pairs(configured)),
        "valid_sync_pairs": sum(bool(metrics.get("valid_sync_samples")) for metrics in quality_metrics.values()),
        "coverage_eligible_pairs": sum(bool(metrics.get("coverage_eligible")) for metrics in funnel_metrics),
        "classification_counts": dict(Counter(str(row["classification"]) for row in ranking_rows)),
        "v1_gap_events_audited": len(gap_rows),
        "sizes_analyzed": [primary.size_usd] + ([5000] if larger is not None else []),
        "canonical_skew_seconds": params.max_skew_seconds,
        "skew_sensitivity_seconds": [3, 5, 10],
        "funding_included": False,
        "fees_are_configured_assumptions": True,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


def iter_pair_points(
    files: list[str],
    *,
    configured: set[tuple[str, str, str]],
    size_usd: int = 1000,
    sample_seconds: int = 60,
) -> Iterator[PairPoint]:
    """Yield valid synchronized pair slots with the canonical V1 semantics.

    V2 keeps the quality-attribution SQL separate from this timeline.  The
    baseline and episode stream reuses V1's sorted quote iterator and
    ``_sample_pairs`` so duplicate selection, quote-age validation, and
    observed-at skew have exactly the same semantics as the production V1
    replay.  This avoids a second all-pairs SQL self-join and its global sort
    over the full Production history.
    """

    if not files:
        raise ValueError("files required")
    if size_usd not in (1000, 5000, 10000):
        raise ValueError("VWAP size must be 1000, 5000 or 10000")
    if sample_seconds <= 0 or 86400 % sample_seconds:
        raise ValueError("sample_seconds must divide one UTC day")
    if not configured:
        return

    v1_params = V1Params(
        sample_seconds=sample_seconds,
        max_quote_age_seconds=12,
        max_skew_seconds=3,
        max_gap_seconds=120,
    )
    for timestamp, pair, quote_a, quote_b in _sample_pairs(
        csv_or_parquet_quotes(files, size_usd=size_usd, sample_seconds=sample_seconds),
        v1_params,
        configured,
    ):
        yield PairPoint(
            sample_time=_as_datetime(timestamp),
            pair=pair,
            a=MarketObservation(
                sample_time=_as_datetime(quote_a.sample_time),
                observed_at=_as_datetime(quote_a.observed_at),
                venue=quote_a.venue,
                venue_symbol=quote_a.venue_symbol,
                symbol=quote_a.symbol,
                buy=quote_a.buy,
                sell=quote_a.sell,
            ),
            b=MarketObservation(
                sample_time=_as_datetime(quote_b.sample_time),
                observed_at=_as_datetime(quote_b.observed_at),
                venue=quote_b.venue,
                venue_symbol=quote_b.venue_symbol,
                symbol=quote_b.symbol,
                buy=quote_b.buy,
                sell=quote_b.sell,
            ),
        )
