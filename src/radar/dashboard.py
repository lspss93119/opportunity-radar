from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3  # noqa: F401 - preserved module attribute for compatibility tests
from collections.abc import Callable
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import cast
from urllib.parse import parse_qsl, urlparse

from radar.config import RadarConfig, load_config
from radar.dashboard_data import (
    DashboardQueryService,
    MAX_OPPORTUNITY_LIMIT,
    OpportunitiesFilters,
    PAIR_RANGES,
    PairRange,
    classify_heartbeat as _classify_heartbeat,
    utc_now,
)

DEFAULT_DATA_ROOT = Path("data")
DEFAULT_RUNTIME_DB = Path("runtime/radar.sqlite3")


def classify_heartbeat(age_seconds: float | None) -> str:
    """Preserve the original dashboard health helper import path."""
    return _classify_heartbeat(age_seconds)


class DashboardStatusService:
    """Compatibility facade over the read-only dashboard query service."""

    def __init__(
        self,
        config: RadarConfig,
        *,
        data_root: Path = DEFAULT_DATA_ROOT,
        runtime_db: Path = DEFAULT_RUNTIME_DB,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.config = config
        self.data_root = Path(data_root)
        self.runtime_db = Path(runtime_db)
        self._clock = clock
        self._query_service = DashboardQueryService(
            config,
            data_root=self.data_root,
            runtime_db=self.runtime_db,
            clock=clock,
            cache_ttl_seconds=0.0,
        )

    def get_status(self, *, now: datetime | None = None) -> dict[str, object]:
        return self._query_service.get_status(now=now)

    def get_opportunities(
        self, filters: OpportunitiesFilters, *, now: datetime | None = None
    ) -> dict[str, object]:
        return self._query_service.get_opportunities(filters, now=now)

    def get_anomalies(
        self,
        *,
        symbol: str | None = None,
        long_venue: str | None = None,
        short_venue: str | None = None,
        eligible_only: bool = False,
        now: datetime | None = None,
    ) -> dict[str, object]:
        return self._query_service.get_anomalies(
            symbol=symbol,
            long_venue=long_venue,
            short_venue=short_venue,
            eligible_only=eligible_only,
            now=now,
        )

    def get_pair(
        self, *, canonical_symbol: str, long_venue: str, long_venue_symbol: str,
        short_venue: str, short_venue_symbol: str, range_name: PairRange,
        now: datetime | None = None,
    ) -> dict[str, object]:
        return self._query_service.get_pair(
            canonical_symbol=canonical_symbol, long_venue=long_venue,
            long_venue_symbol=long_venue_symbol, short_venue=short_venue,
            short_venue_symbol=short_venue_symbol, range_name=range_name, now=now,
        )

    def start(self, *, background: bool = False) -> None:
        self._query_service.start(background=background)

    def close(self) -> None:
        self._query_service.close()


def parse_pair_query(query: str) -> dict[str, str]:
    """Preserve complete exact identity; scanner links default to 24h."""
    required = {"symbol", "long_venue", "long_venue_symbol", "short_venue", "short_venue_symbol"}
    if re.search(r"%(?![0-9a-fA-F]{2})", query):
        raise ValueError("invalid percent encoding")
    values: dict[str, str] = {}
    for name, value in parse_qsl(query, keep_blank_values=True, strict_parsing=True,
                                 errors="strict", max_num_fields=12):
        if name not in required | {"range"}:
            raise ValueError(f"unknown pair parameter: {name}")
        if name in values:
            raise ValueError(f"duplicate pair parameter: {name}")
        if not value.strip() or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
            raise ValueError(f"{name} must be non-empty text")
        values[name] = value
    if missing := required - values.keys():
        raise ValueError("missing exact pair identity: " + ", ".join(sorted(missing)))
    range_name = values.pop("range", "24h")
    if range_name not in PAIR_RANGES:
        raise ValueError("range must be one of " + ", ".join(PAIR_RANGES))
    values["canonical_symbol"] = values.pop("symbol")
    return {**values, "range_name": range_name}


def parse_opportunities_filters(query: str) -> OpportunitiesFilters:
    """Reject malformed/duplicate input before accessing persisted sources."""
    allowed = {"symbol", "long_venue", "short_venue", "max_std", "min_deviation",
               "min_edge", "min_convergence_edge", "min_duration", "basis_state",
               "active_only", "limit"}
    if re.search(r"%(?![0-9a-fA-F]{2})", query):
        raise ValueError("invalid percent encoding")
    values: dict[str, str] = {}
    for name, value in parse_qsl(
        query, keep_blank_values=True, strict_parsing=True,
        errors="strict", max_num_fields=16,
    ):
        if name not in allowed:
            raise ValueError(f"unknown filter: {name}")
        if name in values:
            raise ValueError(f"duplicate filter: {name}")
        value = value.strip()
        if not value or any(ord(character) < 32 for character in value):
            raise ValueError(f"{name} must be non-empty text")
        values[name] = value

    numbers: dict[str, float] = {}
    for name in ("max_std", "min_deviation", "min_edge", "min_convergence_edge"):
        if name in values:
            if not re.fullmatch(r"[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", values[name], re.ASCII):
                raise ValueError(f"{name} must be a finite non-negative number")
            number = float(values[name])
            if not math.isfinite(number):
                raise ValueError(f"{name} must be finite")
            numbers[name] = number
    integers: dict[str, int] = {}
    for name in ("min_duration", "limit"):
        if name in values:
            if not re.fullmatch(r"[0-9]+", values[name]):
                raise ValueError(f"{name} must be a non-negative integer")
            integers[name] = int(values[name])
    limit = integers.get("limit", MAX_OPPORTUNITY_LIMIT)
    if not 1 <= limit <= MAX_OPPORTUNITY_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_OPPORTUNITY_LIMIT}")
    if "min_edge" in values and "min_convergence_edge" in values:
        raise ValueError("min_edge and min_convergence_edge are duplicates")
    active_only = values.get("active_only", "false")
    if active_only not in ("true", "false"):
        raise ValueError("active_only must be true or false")
    basis_state = values.get("basis_state")
    if basis_state is not None and basis_state not in {
        "eligible", "pending", "unavailable",
    }:
        raise ValueError("basis_state must be eligible, pending, or unavailable")
    return OpportunitiesFilters(
        symbol=values["symbol"].upper() if "symbol" in values else None,
        long_venue=values["long_venue"].lower() if "long_venue" in values else None,
        short_venue=values["short_venue"].lower() if "short_venue" in values else None,
        max_std_bps=numbers.get("max_std"),
        min_deviation_bps=numbers.get("min_deviation"),
        min_convergence_edge_bps=numbers.get(
            "min_edge", numbers.get("min_convergence_edge")
        ),
        min_duration_seconds=integers.get("min_duration"),
        basis_state=basis_state,
        active_only=active_only == "true", limit=limit,
    )


def parse_anomaly_query(query: str) -> dict[str, object]:
    allowed = {"symbol", "long_venue", "short_venue", "eligible_only"}
    values: dict[str, str] = {}
    for name, value in parse_qsl(
        query, keep_blank_values=True, strict_parsing=True, errors="strict", max_num_fields=8
    ):
        if name not in allowed or name in values:
            raise ValueError("invalid anomaly filter")
        if not value.strip() or any(ord(character) < 32 for character in value):
            raise ValueError(f"{name} must be non-empty text")
        values[name] = value.strip()
    eligible = values.get("eligible_only", "false")
    if eligible not in ("true", "false"):
        raise ValueError("eligible_only must be true or false")
    return {
        "symbol": values.get("symbol", "").upper() or None,
        "long_venue": values.get("long_venue", "").lower() or None,
        "short_venue": values.get("short_venue", "").lower() or None,
        "eligible_only": eligible == "true",
    }


HTML = r"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Opportunity Radar｜價差監控儀表板</title>
<style>
:root { color-scheme: dark; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #07111f; color: #dbe7f5; }
* { box-sizing: border-box; }
body { margin: 0; background: #07111f; }
main { width: calc(100% - 36px); max-width: 1760px; margin: auto; padding: 16px 0 28px; }
h1 { margin: 0; font-size: 1rem; letter-spacing: .12em; color: #f8fafc; }
h2 { margin: 0 0 12px; font-size: .92rem; letter-spacing: .04em; }
a { color: #8ec5ff; text-underline-offset: 3px; }
.terminal-header { display: flex; align-items: center; justify-content: space-between; gap: 18px; padding: 10px 0 12px; border-bottom: 1px solid #24364b; }
.brand-line, .terminal-meta, nav { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
.brand-mark { color: #38bdf8; font-weight: 700; }
nav { margin: 12px 0 14px; gap: 4px; }
nav a { padding: 6px 10px; border-radius: 4px; color: #8fa5bb; font-size: .78rem; }
nav a[aria-current="page"] { background: #163149; color: #e0f2fe; }
.read-only, .scanner-status { display: inline-flex; align-items: center; gap: 6px; border: 1px solid #31516d; border-radius: 999px; padding: 4px 8px; color: #93c5fd; font: 700 .66rem/1 "SFMono-Regular", Consolas, monospace; letter-spacing: .08em; }
.scanner-status { border-color: #28634f; color: #6ee7b7; }
.muted, .metric-label { color: #7f95aa; font-size: .76rem; }
.summary { display: flex; align-items: center; gap: 12px; min-height: 24px; margin-bottom: 12px; }
.summary #overall { font: 700 .72rem/1 "SFMono-Regular", Consolas, monospace; letter-spacing: .08em; }
.metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 9px; margin-bottom: 14px; }
.card { background: #0d1b2b; border: 1px solid #24364b; border-radius: 6px; padding: 11px 12px; overflow-wrap: anywhere; }
.metric-value { margin-top: 5px; font: 600 .92rem/1.35 "SFMono-Regular", Consolas, monospace; }
#filters, #anomaly-filters { margin-bottom: 10px; }
#anomaly-filters { display: flex; align-items: end; gap: 8px; flex-wrap: wrap; }
.filter-primary, .filter-secondary { display: flex; align-items: end; gap: 8px; flex-wrap: wrap; }
.filter-primary { padding: 8px; background: #0b1929; border: 1px solid #1f344a; border-radius: 6px; }
.filter-secondary { padding: 8px 0 2px; }
.more-filters { margin-top: 7px; color: #8fa5bb; font-size: .74rem; }
.more-filters summary { cursor: pointer; width: fit-content; }
label { display: grid; gap: 4px; font-size: .7rem; color: #a9bbcc; }
input, button, select { font: inherit; color: #dbe7f5; background: #0e1d2e; border: 1px solid #35516a; border-radius: 4px; padding: 7px 8px; }
input, select { width: 132px; }
input[type="checkbox"] { width: auto; }
.check { display: flex; align-items: center; padding: 7px 0; }
button { cursor: pointer; min-height: 32px; }
.filter-actions { display: flex; gap: 6px; align-items: end; }
a:focus-visible, input:focus-visible, button:focus-visible, summary:focus-visible { outline: 2px solid #7dd3fc; outline-offset: 2px; }
.section { min-width: 0; margin-bottom: 18px; }
.table-wrap { position: relative; overflow-x: auto; border: 1px solid #24364b; border-radius: 6px; scrollbar-color: #46627b #0d1b2b; }
.terminal-table-wrap { max-height: min(68vh, 720px); overflow: auto; }
.terminal-table-wrap thead th { position: sticky; top: 0; z-index: 4; background: #102237; box-shadow: 0 1px 0 #46627b; }
table { width: 100%; border-collapse: collapse; background: #0d1b2b; font-size: .76rem; }
th, td { padding: 8px 9px; text-align: left; border-bottom: 1px solid #203246; font-variant-numeric: tabular-nums; vertical-align: middle; }
th { color: #91a8bd; font-size: .66rem; font-weight: 700; letter-spacing: .07em; text-transform: uppercase; }
tr:last-child td { border-bottom: 0; }
td small { display: block; color: #7890a6; margin-top: 3px; font-size: .68rem; line-height: 1.25; }
.table-scroll-hint { display: block; margin: 5px 2px 0; color: #607991; font-size: .68rem; }
.table-help { margin: 0 0 8px; color: #7890a6; font-size: .7rem; }
.healthy { color: #6ee7b7; }
.degraded { color: #fcd34d; }
.down, .error { color: #fca5a5; }
.error { margin: 8px 0; overflow-wrap: anywhere; font-size: .78rem; }
.state-badge { display: inline-flex; align-items: center; min-height: 21px; padding: 3px 7px; border: 1px solid currentColor; border-radius: 999px; font-size: .66rem; font-weight: 700; line-height: 1.1; }
.state-eligible { color: #6ee7b7; background: #073b32; }
.state-pending, .state-unavailable { color: #fcd34d; background: #3d2d08; }
.state-stale { color: #fca5a5; background: #3b1418; }
.state-depth-unavailable { color: #fdba74; background: #3d1d0f; }
.state-note { font: 700 .66rem/1.2 "SFMono-Regular", Consolas, monospace; letter-spacing: .03em; }
.unavailable { color: #71869b; }
.numeric { text-align: right; }
.opportunity-terminal-table { table-layout: fixed; min-width: 0; }
.opportunity-terminal-table th:nth-child(1), .opportunity-terminal-table td:nth-child(1) { width: 13%; }
.opportunity-terminal-table th:nth-child(2), .opportunity-terminal-table td:nth-child(2) { width: 29%; }
.opportunity-terminal-table th:nth-child(3), .opportunity-terminal-table td:nth-child(3) { width: 14%; }
.opportunity-terminal-table th:nth-child(4), .opportunity-terminal-table td:nth-child(4) { width: 17%; }
.opportunity-terminal-table th:nth-child(5), .opportunity-terminal-table td:nth-child(5) { width: 14%; }
.opportunity-terminal-table th:nth-child(6), .opportunity-terminal-table td:nth-child(6) { width: 13%; }
.opportunity-terminal-table tbody tr:hover, .anomaly-terminal-table tbody tr:hover { background: #10263a; }
.opportunity-terminal-table .symbol-cell { font-weight: 700; white-space: nowrap; }
.route-cell { white-space: normal; }
.route-link { display: block; color: inherit; text-decoration: none; }
.route-side { display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 5px; align-items: baseline; line-height: 1.25; }
.route-side + .route-side { margin-top: 4px; }
.route-label { color: #728aa0; font: 700 .62rem/1 "SFMono-Regular", Consolas, monospace; letter-spacing: .08em; }
.route-venue { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: #dbe7f5; }
.route-symbol { grid-column: 2; color: #7890a6; font: .66rem/1.15 "SFMono-Regular", Consolas, monospace; overflow-wrap: anywhere; }
.route-side.long .route-label { color: #6ee7b7; }
.route-side.short .route-label { color: #fca5a5; }
.edge-cell, .current-cell, .baseline-cell, .execution-cell { white-space: normal; }
.edge-value { font: 700 .93rem/1.15 "SFMono-Regular", Consolas, monospace; }
.edge-positive { color: #6ee7b7; }
.edge-negative { color: #fca5a5; }
.edge-zero { color: #dbe7f5; }
.edge-unavailable { color: #71869b; }
.anomaly-deviation { color: #c4b5fd; }
.current-net { color: #dbe7f5; font: 700 .86rem/1.15 "SFMono-Regular", Consolas, monospace; }
.current-raw { color: #8fa5bb; }
.baseline-primary, .execution-primary { font: 600 .78rem/1.2 "SFMono-Regular", Consolas, monospace; }
.execution-primary { color: #b8d8f0; }
.anomaly-terminal-table { table-layout: fixed; min-width: 0; }
.anomaly-terminal-table th:nth-child(1), .anomaly-terminal-table td:nth-child(1) { width: 12%; }
.anomaly-terminal-table th:nth-child(2), .anomaly-terminal-table td:nth-child(2) { width: 29%; }
.anomaly-terminal-table th:nth-child(3), .anomaly-terminal-table td:nth-child(3) { width: 13%; }
.anomaly-terminal-table th:nth-child(4), .anomaly-terminal-table td:nth-child(4) { width: 13%; }
.anomaly-terminal-table th:nth-child(5), .anomaly-terminal-table td:nth-child(5) { width: 17%; }
.anomaly-terminal-table th:nth-child(6), .anomaly-terminal-table td:nth-child(6) { width: 16%; }
.anomaly-route, .anomaly-context { white-space: normal; }
.pair-identity { margin-bottom: 12px; }
.pair-kpis { grid-template-columns: repeat(4, minmax(0, 1fr)); }
.pair-layout { display: grid; grid-template-columns: minmax(0, 3fr) minmax(230px, 1fr); gap: 12px; align-items: start; }
.pair-main, .pair-right-rail { min-width: 0; }
.pair-right-rail { display: grid; gap: 9px; }
.pair-rail-card { background: #0d1b2b; border: 1px solid #24364b; border-radius: 6px; padding: 10px; }
.pair-rail-card h3 { margin: 0 0 7px; color: #91a8bd; font: 700 .65rem/1.1 "SFMono-Regular", Consolas, monospace; letter-spacing: .08em; text-transform: uppercase; }
.pair-rail-line { display: flex; justify-content: space-between; gap: 10px; padding: 3px 0; font-size: .72rem; }
.pair-rail-line span:last-child { color: #dbe7f5; text-align: right; overflow-wrap: anywhere; }
.trajectory-state { font: 700 .68rem/1.1 "SFMono-Regular", Consolas, monospace; }
.trajectory-widening { color: #fbbf24; }
.trajectory-reverting { color: #7dd3fc; }
.trajectory-flat { color: #a9bbcc; }
.trajectory-gone { color: #fca5a5; }
.trajectory-unavailable { color: #71869b; }
.trajectory-note { display: block; margin-top: 6px; color: #71869b; font-size: .65rem; line-height: 1.3; }
.pair-chart-header { display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; margin-bottom: 8px; }
.pair-chart-header label { display: flex; align-items: center; gap: 7px; }
#pair-chart { min-height: 270px; overflow: hidden; position: relative; padding: 7px; }

.pair-chart-surface { width: 100%; height: min(62vh, 560px); min-height: 360px; }
.pair-chart-tooltip { position: absolute; z-index: 2; top: 13px; left: 15px; max-width: calc(100% - 30px); padding: 6px 10px;
  background: rgba(7,17,31,.85); border: 1px solid #35516a; border-radius: 4px; color: #cbd5e1; font-size: .75rem;
  font-variant-numeric: tabular-nums; pointer-events: none; }
.pair-chart-controls { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.pair-chart-controls button { padding: 5px 9px; font-size: .72rem; min-height: 29px; }
@media(max-width:760px) { .pair-chart-surface { height: 380px; min-height: 300px; } }

#pair-chart svg { display: block; width: 100%; height: auto; }
#pair-chart svg text { font-variant-numeric: tabular-nums; }
#pair-chart .axis-tick { stroke: #334d66; }
.chart-legend { display: flex; gap: 14px; flex-wrap: wrap; margin: 8px 0; font-size: .72rem; }
.raw-color { color: #93c5fd; } .mean-color { color: #fcd34d; } .current-color { color: #6ee7b7; }
.pair-details { margin-top: 12px; border-top: 1px solid #24364b; padding-top: 10px; }
.pair-details summary { cursor: pointer; color: #91a8bd; font-size: .76rem; }
.pair-secondary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; margin-top: 9px; }
.pair-secondary .detail { padding: 8px; background: #0b1929; border: 1px solid #1f344a; border-radius: 4px; }
.pair-secondary .detail-label { color: #71869b; font-size: .66rem; }
.pair-secondary .detail-value { display: block; margin-top: 3px; font: .73rem/1.3 "SFMono-Regular", Consolas, monospace; overflow-wrap: anywhere; }
[data-view="opportunities"] #status-view, [data-view="opportunities"] #pair-view, [data-view="opportunities"] #anomalies-view,
[data-view="status"] #opportunities-view, [data-view="status"] #pair-view, [data-view="status"] #anomalies-view,
[data-view="pair"] #opportunities-view, [data-view="pair"] #status-view, [data-view="pair"] #anomalies-view,
[data-view="anomalies"] #opportunities-view, [data-view="anomalies"] #status-view, [data-view="anomalies"] #pair-view { display: none; }
@media(max-width: 960px) { main { width: calc(100% - 24px); } .pair-layout { grid-template-columns: minmax(0, 1fr); } .pair-right-rail { grid-template-columns: repeat(3, minmax(0, 1fr)); } .pair-secondary { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
@media(max-width: 760px) { main { width: calc(100% - 20px); padding-top: 10px; } .terminal-header { align-items: flex-start; flex-direction: column; gap: 9px; } th, td { padding: 7px; } .pair-kpis { grid-template-columns: repeat(2, minmax(0, 1fr)); } .pair-right-rail { grid-template-columns: 1fr; } .pair-secondary { grid-template-columns: 1fr; } input, select { width: 100%; } .filter-primary, .filter-secondary { align-items: stretch; } .filter-primary label, .filter-secondary label { flex: 1 1 130px; } .filter-actions { width: 100%; } }
</style>
</head>
<body data-view="__VIEW__">
<main>
<header class="terminal-header"><div class="brand-line"><span class="brand-mark">◆</span><h1>OPPORTUNITY RADAR｜價差雷達</h1></div><div class="terminal-meta"><span class="read-only">唯讀模式</span><span class="muted">1 萬美元可成交 VWAP</span><span id="scanner-status" class="scanner-status" data-scanner-status>掃描器 · —</span></div></header>
<nav aria-label="儀表板導覽"><a id="nav-opportunities" href="/opportunities">套利機會</a><a id="nav-anomalies" href="/anomalies">異常訊號</a><a id="nav-status" href="/status">系統狀態</a></nav>
<div class="summary" aria-live="polite"><span id="overall">載入中…</span><span id="data-as-of" class="muted">資料延遲 —</span><span id="refreshed" class="muted"></span></div>
<div id="errors" role="alert"></div>
<section id="anomalies-view">
<h2>有效異常訊號</h2>
<p class="table-help">已確認的 V2 訊號｜事件生命週期與 Telegram 狀態取自唯讀 SQLite。</p>
<form id="anomaly-filters">
<label>交易標的<input id="anomaly-symbol" name="symbol" placeholder="全部標的"></label>
<label>做多交易所<input id="anomaly-long-venue" name="long_venue" placeholder="全部交易所"></label>
<label>做空交易所<input id="anomaly-short-venue" name="short_venue" placeholder="全部交易所"></label>
<label class="check"><input id="anomaly-eligible-only" name="eligible_only" type="checkbox">僅顯示符合 Telegram 條件</label>
<span class="filter-actions"><button type="submit">套用篩選</button><button id="anomaly-reset" type="button">清除篩選</button></span>
</form>
<div class="table-wrap terminal-table-wrap"><table class="anomaly-terminal-table" aria-label="有效異常訊號"><thead><tr><th>交易標的</th><th>交易方向</th><th>偏離幅度</th><th>參考基準</th><th>訊號週期</th><th>歷史比較</th></tr></thead><tbody id="anomaly-rows"><tr><td colspan="6" class="muted">載入中…</td></tr></tbody></table></div>
</section>
<section id="opportunities-view">
<form id="filters" class="filter-toolbar">
<div class="filter-primary">
<label>交易標的<input id="symbol" name="symbol" placeholder="全部標的"></label>
<label>做多交易所<input id="long_venue" name="long_venue" placeholder="全部交易所"></label>
<label>做空交易所<input id="short_venue" name="short_venue" placeholder="全部交易所"></label>
<label>最低預估收斂優勢（bps）<input id="min_edge" name="min_edge" type="number" min="0" step="any"></label>
<label>基準狀態<select id="basis_state" name="basis_state"><option value="">全部狀態</option><option value="eligible">符合條件</option><option value="pending">等待基準建立</option><option value="unavailable">基準不可用</option></select></label>
<span class="filter-actions"><button type="submit">套用篩選</button><button id="reset" type="button">清除篩選</button></span>
</div>
<details id="more-filters" class="more-filters"><summary>更多篩選條件</summary><div class="filter-secondary">
<label>24 小時標準差上限（bps）<input id="max_std" name="max_std" type="number" min="0" step="any"></label>
<label>最短持續時間（秒）<input id="min_duration" name="min_duration" type="number" min="0" step="1"></label>
<label>顯示筆數<input id="limit" name="limit" type="number" min="1" max="512" step="1" placeholder="512"></label>
<label class="check"><input id="active_only" name="active_only" type="checkbox">僅顯示有效訊號</label>
</div></details>
</form>
<p class="table-help">依可操作性排序，再依扣除來回手續費後的收斂優勢排序｜僅供觀察｜持續時間取自 SQLite。</p>
<div class="table-wrap terminal-table-wrap"><table aria-label="套利機會" class="opportunity-terminal-table"><thead><tr><th>交易標的</th><th>交易方向</th><th>收斂優勢</th><th>目前價差</th><th>歷史基準</th><th>可成交性</th></tr></thead><tbody id="opportunity-rows"><tr><td colspan="6" class="muted">載入中…</td></tr></tbody></table></div>
</section>
<section id="status-view">
<div class="metrics">
<div class="card"><div class="metric-label">Radar 心跳</div><div id="heartbeat" class="metric-value">無資料</div></div>
<div class="card"><div class="metric-label">最新採樣／資料延遲</div><div id="sample" class="metric-value">無資料</div></div>
<div class="card"><div class="metric-label">最新採樣筆數／已設定資料源</div><div id="feeds" class="metric-value">無資料</div></div>
<div class="card"><div class="metric-label">正常資料源／VWAP 就緒</div><div id="primary" class="metric-value">無資料</div></div>
<div class="card"><div class="metric-label">Parquet 讀取／檔案狀態</div><div id="parquet" class="metric-value">無資料</div></div>
<div class="card"><div class="metric-label">SQLite 讀取狀態</div><div id="sqlite" class="metric-value">無資料</div></div>
<div class="card"><div class="metric-label">有效／已確認／已通知訊號</div><div id="episodes-count" class="metric-value">無資料</div></div>
</div>
<section class="section"><h2>交易所資料｜缺少筆數以最新採樣為準</h2><div class="table-wrap"><table><thead><tr><th>交易所</th><th>預期筆數</th><th>可用筆數</th><th>最新筆數</th><th>缺少筆數</th><th>最大延遲</th></tr></thead><tbody id="venues"></tbody></table></div></section>
<section class="section"><h2>市場資料源</h2><div class="table-wrap"><table><thead><tr><th>交易標的</th><th>交易所</th><th>交易所標的</th><th>系統狀態</th><th>資料延遲</th><th>VWAP 狀態</th></tr></thead><tbody id="feed-rows"></tbody></table></div></section>
<section class="section"><h2>有效訊號週期｜Production 持久化狀態</h2><div class="table-wrap"><table><thead><tr><th>交易標的</th><th>做多</th><th>做空</th><th>淨價差（bps）</th><th>已確認</th><th>已通知</th></tr></thead><tbody id="episode-rows"></tbody></table></div></section>
<section class="section"><h2>近期事件</h2><div class="table-wrap"><table><thead><tr><th>時間</th><th>交易標的</th><th>事件類型</th><th>淨價差（bps）</th></tr></thead><tbody id="event-rows"></tbody></table></div></section>
<p id="unavailable" class="muted">持久化資料目前不提供：部署 SHA、Telegram 傳輸健康度、資料收集器錯誤計數。</p>
</section>
<section id="pair-view">
<h2 id="pair-identity">配對詳情｜無法辨識完整配對</h2>
<div id="pair-summary" class="pair-kpis metrics"></div>
<div class="pair-layout"><div class="pair-main">
<div class="pair-chart-header"><strong>方向性可成交價差</strong><div class="pair-chart-controls"><button type="button" id="pair-chart-reset">重設視圖</button><button type="button" id="pair-chart-latest">跳至最新</button><label>歷史範圍<select id="pair-range" aria-label="歷史資料範圍"><option value="1h">1 小時</option><option value="6h">6 小時</option><option value="24h" selected>24 小時</option><option value="3d">3 天</option><option value="7d">7 天</option><option value="all">全部資料（最多 90 天）</option></select></label></div></div>
<div class="chart-legend"><span class="raw-color">原始價差</span><span class="mean-color">滾動 24 小時均值</span><span class="current-color">目前價差</span></div>
<div id="pair-chart" class="card">無歷史資料</div>
<p id="pair-coverage" class="muted"></p>
</div><aside class="pair-right-rail"><div id="pair-long-leg" class="pair-rail-card"></div><div id="pair-short-leg" class="pair-rail-card"></div><div id="pair-trajectory-rail" class="pair-rail-card"></div><div id="pair-lifecycle-rail" class="pair-rail-card"></div></aside></div>
<details id="pair-details" class="pair-details"><summary>其他運作細節</summary><div id="pair-secondary" class="pair-secondary"></div></details>
<p class="muted">歷史資料依 UTC 採樣時間精確對齊，缺漏區間保留斷線；資料僅供檢視，均值使用當下之前的資料。統計數值與訊號狀態不在瀏覽器重新計算。</p>
</section>
</main>
<script src="https://cdn.jsdelivr.net/npm/lightweight-charts@5.2.1/dist/lightweight-charts.standalone.production.min.js"></script>
<script>
const byId = id => document.getElementById(id);
const esc = value => String(value ?? '無資料').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
const number = value => typeof value === 'number' && Number.isFinite(value) ? value.toFixed(2) : '—';
const age = value => typeof value !== 'number' || !Number.isFinite(value) ? '—' : value < 60 ? Math.round(value) + 's' : Math.floor(value / 60) + 'm ' + Math.round(value % 60) + 's';
const compactUtc = value => {
  if (typeof value !== 'string' || !value) return '—';
  if (!/^\d{4}-\d{2}-\d{2}T/.test(value) || !Number.isFinite(Date.parse(value))) return value;
  const date = new Date(value), pad = part => String(part).padStart(2, '0');
  return date.getUTCFullYear() + '-' + pad(date.getUTCMonth() + 1) + '-' + pad(date.getUTCDate()) +
    ' ' + pad(date.getUTCHours()) + ':' + pad(date.getUTCMinutes()) + ':' + pad(date.getUTCSeconds()) + 'Z';
};
const statusZh = value => ({healthy:'正常',degraded:'部分異常',down:'離線',eligible:'符合條件',pending:'等待中',unavailable:'不可用',stale:'資料過期',depth_unavailable:'深度不可用'}[value] ?? value ?? '未知');
const statusClass = value => ['healthy','degraded','down'].includes(value) ? value : 'degraded';
const text = (id, value) => { byId(id).textContent = value ?? '無資料'; };
const emptyRow = (columns, message) => '<tr><td colspan="' + columns + '" class="muted">' + esc(message) + '</td></tr>';
const cell = (value, className = '') => '<td class="' + className + (value === null || value === undefined || value === '' ? ' unavailable' : '') + '">' + esc(value === null || value === undefined || value === '' ? '—' : value) + '</td>';
const filterNames = ['symbol','long_venue','short_venue','max_std','min_edge','min_duration','basis_state','limit'];
const anomalyFilterNames = ['symbol','long_venue','short_venue'];
const isStatus = location.pathname === '/status';
const isPair = location.pathname === '/pair';
const isAnomalies = location.pathname === '/anomalies';
const isOpportunities = !isStatus && !isPair && !isAnomalies;
if (!isPair) byId(isAnomalies ? 'nav-anomalies' : isStatus ? 'nav-status' : 'nav-opportunities').setAttribute?.('aria-current', 'page');
function restoreFilters() {
  const query = new URLSearchParams(location.search);
  if (isAnomalies) {
    for (const name of anomalyFilterNames) byId('anomaly-' + name.replace('_', '-')).value = query.get(name) ?? '';
    byId('anomaly-eligible-only').checked = query.get('eligible_only') === 'true';
  } else {
    for (const name of filterNames) byId(name).value = query.get(name) ?? '';
    byId('active_only').checked = query.get('active_only') === 'true';
  }
}
function filterQuery() {
  if (isAnomalies) return anomalyFilterQuery();
  return opportunityFilterQuery();
}
function opportunityFilterQuery() {
  const query = new URLSearchParams();
  for (const name of filterNames) {
    let value = byId(name).value.trim();
    if (name === 'symbol') value = value.toUpperCase();
    if (name.endsWith('_venue')) value = value.toLowerCase();
    if (value) query.set(name, value);
  }
  if (byId('active_only').checked) query.set('active_only', 'true');
  return query;
}
function anomalyFilterQuery() {
  const query = new URLSearchParams();
  for (const name of anomalyFilterNames) {
    let value = byId('anomaly-' + name.replace('_', '-')).value.trim();
    if (name === 'symbol') value = value.toUpperCase();
    if (name.endsWith('_venue')) value = value.toLowerCase();
    if (value) query.set(name, value);
  }
  if (byId('anomaly-eligible-only').checked) query.set('eligible_only', 'true');
  return query;
}
function pairLink(row) {
  const fields = ['canonical_symbol','long_venue','long_venue_symbol','short_venue','short_venue_symbol'];
  if (!fields.every(name => typeof row[name] === 'string' && row[name].trim())) return null;
  const query = new URLSearchParams();
  for (const name of fields) query.set(name === 'canonical_symbol' ? 'symbol' : name, row[name]);
  return '/pair?' + query.toString();
}
function pairCell(row, symbolField, includeState = true) {
  const link = pairLink(row);
  const symbol = esc(row[symbolField]);
  const state = includeState && row.state ? '<small>' + esc(({eligible:'符合條件',pending:'等待基準建立',unavailable:'基準不可用',stale:'資料過期',depth_unavailable:'深度不可用'}[row.state] ?? row.state)) + '</small>' : '';
  return link ? '<td class="symbol-cell"><a href="' + esc(link) + '">' + symbol + '</a>' + state + '</td>' : '<td class="symbol-cell">' + symbol + '<small>完整配對資訊不可用</small>' + state + '</td>';
}
function directionCell(row, venueField, symbolField, direction) {
  return '<td class="route-cell"><span class="direction-label">' + direction + '</span>' + esc(row[venueField]) + '<small>' + esc(row[symbolField]) + '</small></td>';
}
function routeCell(row, extraClass = '') {
  const link = pairLink(row);
  const side = (direction, venueField, symbolField, className) => '<div class="route-side ' + className + '"><span class="route-label">' + direction + '</span><span class="route-venue">' + esc(row[venueField]) + '</span><span class="route-symbol">' + esc(row[symbolField]) + '</span></div>';
  const body = side('做多', 'long_venue', 'long_venue_symbol', 'long') + side('做空', 'short_venue', 'short_venue_symbol', 'short');
  return '<td class="route-cell' + (extraClass ? ' ' + extraClass : '') + '">' + (link ? '<a class="route-link" href="' + esc(link) + '">' + body + '</a>' : body + '<small>完整配對資訊不可用</small>') + '</td>';
}
function signedNumber(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  return (value > 0 ? '+' : '') + value.toFixed(2);
}
function renderOpportunities(data) {
  const stateClass = state => ({eligible:'eligible',pending:'pending',unavailable:'unavailable',stale:'stale',depth_unavailable:'depth-unavailable'}[state] ?? 'unavailable');
  const edgeClass = value => typeof value !== 'number' || !Number.isFinite(value) ? 'edge-unavailable' : value > 0 ? 'edge-positive' : value < 0 ? 'edge-negative' : 'edge-zero';
  const sampleTitle = row => typeof row.sample_time === 'string' ? esc(row.sample_time) : '';
  byId('opportunity-rows').innerHTML = (data.rows || []).map(row => '<tr>' +
    pairCell(row, 'canonical_symbol', false) +
    routeCell(row) +
    '<td class="edge-cell"><strong class="edge-value ' + edgeClass(row.convergence_edge_bps) + '">' + signedNumber(row.convergence_edge_bps) + ' bps</strong><small>扣除 ' + number(row.round_trip_fee_bps) + ' bps 來回手續費</small></td>' +
    '<td class="current-cell">' + (typeof row.current_net_spread_bps === 'number' && Number.isFinite(row.current_net_spread_bps) ? '<strong class="current-net">' + signedNumber(row.current_net_spread_bps) + ' 淨價差</strong>' : '<span class="unavailable">淨價差不可用</span>') + '<small class="current-raw">原始 ' + signedNumber(row.current_raw_spread_bps) + ' bps</small></td>' +
    '<td class="baseline-cell">' + (row.basis_state === 'eligible' && typeof row.rolling_mean_bps === 'number'
      ? '<strong class="baseline-primary">' + number(row.rolling_mean_bps) + ' bps 均值</strong><small>σ ' + number(row.rolling_std_bps) + ' · ' + (typeof row.basis_coverage === 'number' ? number(row.basis_coverage * 100) + '% 覆蓋率' : '—') + '</small>'
      : '<span class="state-note state-' + stateClass(row.basis_state) + '">' + esc(row.basis_state === 'pending' ? '等待基準建立' : row.basis_state === 'unavailable' ? '基準不可用' : '基準不可用') + '</span>') + '</td>' +
    '<td class="execution-cell" title="' + sampleTitle(row) + '">' + (row.state === 'stale' ? '<span class="state-note state-stale">資料過期</span>' : row.primary_depth_status !== 'available' ? '<span class="state-note state-depth-unavailable">深度不可用</span>' : '<span class="execution-primary">深度可用</span>') + '<small>延遲 ' + age(row.freshness_seconds) + ' · 時間差 ' + age(row.observed_at_skew_seconds) + '</small></td></tr>'
  ).join('') || emptyRow(6, data.status === 'healthy' ? '沒有符合條件的套利機會' : '目前沒有符合的資料；部分來源可能異常');
}
function renderAnomalies(data) {
  byId('anomaly-rows').innerHTML = (data.rows || []).map(row => '<tr>' +
    pairCell({...row, canonical_symbol: row.symbol}, 'symbol') +
    routeCell({...row, canonical_symbol: row.symbol}, 'anomaly-route') +
    '<td class="numeric"><strong class="edge-value anomaly-deviation">' + signedNumber(row.current_deviation_bps) + ' bps</strong><small>目前 ' + signedNumber(row.current_spread_bps) + '</small></td>' +
    '<td class="numeric"><strong>' + number(row.reference_mean_bps) + ' bps</strong><small>基準差距 ' + number(row.mean_alignment_bps) + '</small></td>' +
    '<td><strong>' + age(row.episode_age_seconds) + '</strong><small>高峰 ' + signedNumber(row.post_confirmation_peak_deviation_bps) + ' · TG ' + esc(row.tg_status ?? '等待中') + '</small></td>' +
    '<td class="anomaly-context"><strong>' + (row.mean_12h_bps ?? '—') + ' / ' + (row.mean_24h_bps ?? '—') + ' / ' + (row.mean_48h_bps ?? '—') + '</strong><small>12h / 24h / 48h</small></td></tr>').join('') || emptyRow(6, '目前沒有有效且已確認的異常訊號');
}
function renderStatus(data) {
  const overall = data.overall || {}, heartbeat = data.heartbeat || {};
  const metric = (id, value, title) => { const element = byId(id); element.textContent = value ?? '—'; if (title) element.title = title; };
  metric('heartbeat', statusZh(heartbeat.status) + ' · 延遲 ' + age(heartbeat.age_seconds) + ' · ' + compactUtc(heartbeat.updated_at), heartbeat.updated_at);
  metric('sample', compactUtc(data.data_as_of) + ' · 延遲 ' + age(data.sample_age_seconds), data.data_as_of);
  metric('feeds', (overall.latest_feeds ?? '—') + ' / ' + (overall.configured_feeds ?? '—'));
  metric('primary', (overall.healthy_feeds ?? '—') + ' / ' + (overall.primary_vwap_ready ?? '—'));
  const parquet = data.parquet || {};
  metric('parquet', statusZh(parquet.status) + ' · 檔案數：' + (parquet.files_considered ?? '—') + ' · 最新檔案時間：' + compactUtc(parquet.latest_file_mtime), parquet.latest_file_mtime);
  metric('sqlite', statusZh(data.sqlite?.status));
  metric('episodes-count', [overall.active_episodes,overall.confirmed_candidates,overall.active_alerted_episodes].map(value => value ?? '—').join(' / '));
  byId('venues').innerHTML = Object.entries(data.venues || {}).map(([venue, value]) => '<tr>' +
    [venue,value.expected,value.available,value.latest,value.missing,age(value.max_observation_age_seconds)].map(cell).join('') + '</tr>').join('') || emptyRow(6, '交易所資料不可用');
  byId('feed-rows').innerHTML = (data.feeds || []).map(feed => '<tr>' +
    [feed.canonical_symbol,feed.venue,feed.venue_symbol].map(cell).join('') + '<td class="' + statusClass(feed.status) + '">' + esc(statusZh(feed.status)) + '</td>' +
    cell(age(feed.age_seconds)) + cell((feed.vwap_available ?? '無資料') + '/' + (feed.vwap_total ?? '無資料')) + '</tr>').join('') || emptyRow(6, '沒有資料源紀錄');
  byId('episode-rows').innerHTML = (data.episodes || []).map(row => '<tr>' + pairCell({...row,canonical_symbol:row.symbol}, 'symbol') +
    cell((row.long_venue ?? '無資料') + ' · ' + (row.long_venue_symbol ?? '無資料')) +
    cell((row.short_venue ?? '無資料') + ' · ' + (row.short_venue_symbol ?? '無資料')) +
    cell(number(row.net_spread_bps)) + cell(row.candidate_confirmed ? '是' : '否') + cell(row.alerted ? '是' : '否') + '</tr>').join('') ||
    emptyRow(6, data.sqlite?.status === 'healthy' ? '目前沒有有效訊號週期' : '訊號週期資料不可用');
  byId('event-rows').innerHTML = (data.recent_events || []).map(row => '<tr>' +
    [row.occurred_at,row.symbol,row.event_type,number(row.net_spread_bps)].map(cell).join('') + '</tr>').join('') ||
    emptyRow(4, data.sqlite?.status === 'healthy' ? '近期沒有事件' : '事件資料不可用');
}
function restorePairRange() {
  byId('pair-range').value = new URLSearchParams(location.search).get('range') ?? '24h';
}
function renderPair(data) {
  const identity = data.identity || {}, current = data.current || {}, basis = data.basis || {}, lifecycle = data.lifecycle || {}, trajectory = data.trajectory || {};
  const disabledMapping = (data.errors || []).some(error => String(error).includes('not an enabled configured mapping'));
  text('pair-identity', disabledMapping
    ? '這組歷史配對已不在目前的監控清單中。'
    : (identity.canonical_symbol ?? '無資料') + ' · 做多 ' +
      (identity.long_venue ?? '無資料') + ' / ' + (identity.long_venue_symbol ?? '無資料') + ' → 做空 ' +
      (identity.short_venue ?? '無資料') + ' / ' + (identity.short_venue_symbol ?? '無資料'));
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const baselineValue = finite(lifecycle.reference_mean_bps) ? lifecycle.reference_mean_bps : current.rolling_mean_bps;
  const baselineLabel = finite(baselineValue) ? number(baselineValue) + ' bps' : '—';
  const kpis = [
    ['扣費後預估收斂優勢', finite(current.theoretical_edge_bps) ? signedNumber(current.theoretical_edge_bps) + ' bps' : '—', '僅供觀察'],
    ['目前價差', finite(current.current_net_spread_bps) ? signedNumber(current.current_net_spread_bps) + ' 淨價差' : '—', '原始 ' + signedNumber(current.raw_spread_bps) + ' bps'],
    ['歷史基準', baselineLabel, finite(current.rolling_std_bps) ? 'σ ' + number(current.rolling_std_bps) + ' · ' + (typeof basis.coverage === 'number' ? number(basis.coverage * 100) + '% 覆蓋率' : '—') : '僅使用過去資料的 24 小時均值'],
    ['可成交性', current.primary_depth_status === 'available' ? '深度可用' : '深度不可用', '延遲 ' + age(current.freshness_seconds) + ' · 時間差 ' + age(current.observed_at_skew_seconds)],
  ];
  byId('pair-summary').innerHTML = kpis.map(([label,value,detail]) => '<div class="card"><div class="metric-label">' + esc(label) + '</div><div class="metric-value">' + esc(value) + '</div><small>' + esc(detail) + '</small></div>').join('');
  const railLine = (label, value) => '<div class="pair-rail-line"><span>' + esc(label) + '</span><span>' + esc(value) + '</span></div>';
  byId('pair-long-leg').innerHTML = '<h3>做多端</h3>' + railLine('交易所', identity.long_venue ?? '無資料') + railLine('標的', identity.long_venue_symbol ?? '無資料') + railLine('1 萬美元買入 VWAP', number(current.long_buy_vwap));
  byId('pair-short-leg').innerHTML = '<h3>做空端</h3>' + railLine('交易所', identity.short_venue ?? '無資料') + railLine('標的', identity.short_venue_symbol ?? '無資料') + railLine('1 萬美元賣出 VWAP', number(current.short_sell_vwap));
  const trajectoryWindows = trajectory.windows || {};
  const trajectoryClass = state => ['widening','reverting','flat','gone'].includes(state) ? 'trajectory-' + state : 'trajectory-unavailable';
  const trajectoryValue = entry => {
    if (!entry || entry.available !== true || typeof entry.edge_change_bps !== 'number' || !Number.isFinite(entry.edge_change_bps)) return '—';
    return signedNumber(entry.edge_change_bps) + ' bps · ' + statusTrajectoryZh(entry.state);
  };
  const statusTrajectoryZh = value => ({widening:'持續擴大',reverting:'正在收斂',flat:'大致持平',gone:'優勢消失',unavailable:'不可用'}[value] ?? '不可用');
  const trajectoryLine = label => {
    const entry = trajectoryWindows[label];
    const state = entry?.state ?? 'unavailable';
    return '<div class="pair-rail-line"><span>' + esc(({ '1m':'1 分鐘', '5m':'5 分鐘', '15m':'15 分鐘' }[label] ?? label)) + '</span><span class="trajectory-state ' + trajectoryClass(state) + '">' + esc(trajectoryValue(entry)) + '</span></div>';
  };
  const referenceKind = trajectory.reference_kind === 'anomaly_frozen' ? '異常訊號固定基準' : trajectory.reference_kind === 'rolling_24h' ? '滾動 24 小時' : '不可用';
  const currentEdge = typeof trajectory.current_edge_bps === 'number' && Number.isFinite(trajectory.current_edge_bps)
    ? signedNumber(trajectory.current_edge_bps) + ' bps'
    : '—';
  const peakEdge = typeof trajectory.recent_peak_edge_bps === 'number' && Number.isFinite(trajectory.recent_peak_edge_bps)
    ? signedNumber(trajectory.recent_peak_edge_bps) + ' bps'
    : '—';
  const peakOffset = typeof trajectory.edge_from_recent_peak_bps === 'number' && Number.isFinite(trajectory.edge_from_recent_peak_bps)
    ? number(trajectory.edge_from_recent_peak_bps) + ' bps'
    : '—';
  byId('pair-trajectory-rail').innerHTML = trajectory.available === true
    ? '<h3>價差優勢走勢</h3>' +
      railLine('目前收斂優勢', currentEdge + (trajectory.current_state === 'gone' ? ' · 優勢消失' : '')) +
      trajectoryLine('1m') + trajectoryLine('5m') + trajectoryLine('15m') +
      railLine('15 分鐘高點', peakEdge) +
      railLine('距高點回落', peakOffset) +
      '<small class="trajectory-note">Δ = 扣除來回手續費後的收斂優勢變化；對比 ' + esc(referenceKind) + ' 基準｜僅供觀察</small>'
    : '<h3>價差優勢走勢</h3><span class="trajectory-state trajectory-unavailable">不可用</span>' +
      '<small class="trajectory-note">需要有效的最新觀測與歷史基準｜僅供觀察</small>';
  byId('pair-lifecycle-rail').innerHTML = '<h3>訊號生命週期</h3>' + railLine('持續時間', age(current.signal_duration_seconds)) + railLine('候選開始', lifecycle.candidate_started_at ?? '無資料') + railLine('確認時間', lifecycle.confirmed_at ?? '無資料') + railLine('狀態', lifecycle.available ? ((lifecycle.candidate_confirmed ? '已確認' : '候選') + ' · ' + (lifecycle.alerted ? '已通知' : '未通知')) : '無資料');
  const metrics = [
    ['原始價差（bps）', signedNumber(current.raw_spread_bps)], ['目前淨價差（bps）',signedNumber(current.current_net_spread_bps)], ['24 小時均值（bps）',number(current.rolling_mean_bps)], ['24 小時標準差（bps）',number(current.rolling_std_bps)], ['偏離幅度（bps）',signedNumber(current.deviation_bps)],
    ['觀測時間差（秒）',number(current.observed_at_skew_seconds)], ['資料延遲（秒）',number(current.freshness_seconds)], ['訊號持續時間（持久化）',age(current.signal_duration_seconds)],
    ['做多端 1 萬美元買入 VWAP',number(current.long_buy_vwap)], ['做空端 1 萬美元賣出 VWAP',number(current.short_sell_vwap)], ['來回手續費（bps，僅供觀察）',number(current.round_trip_fee_bps)], ['採樣時間（UTC）',current.sample_time ?? '無資料'],
    ['訊號生命週期（持久化）',lifecycle.available ? '有效 · 已確認：' + (lifecycle.candidate_confirmed ? '是' : '否') + ' · 已通知：' + (lifecycle.alerted ? '是' : '否') + ' · ' + (lifecycle.episode_id ?? '無資料') : '非有效狀態／不可用']
  ];
  if (lifecycle.reference_mean_bps !== undefined) {
    metrics.push(
      ['V2 固定參考均值（bps）', number(lifecycle.reference_mean_bps)],
      ['V2 確認偏離幅度（bps）', number(lifecycle.confirmation_deviation_bps)],
      ['V2 確認後最大偏離（bps）', number(lifecycle.post_confirmation_peak_deviation_bps)],
      ['V2 Telegram', lifecycle.tg_status ?? '等待中']
    );
  }
  byId('pair-secondary').innerHTML = metrics.map(([label,value]) => '<div class="detail"><span class="detail-label">' + esc(label) + '</span><span class="detail-value">' + esc(value) + '</span></div>').join('');
  text('pair-coverage', (basis.eligible ? '24 小時基準可用' : '24 小時基準不可用') + ' · 過去樣本數：' +
    (basis.sample_count ?? '無資料') + ' · 覆蓋率：' + (typeof basis.coverage === 'number' ? number(basis.coverage * 100) + '%' : '無資料'));
  renderPairChart(data);
}

let interactivePairChart = null;
let interactivePairRange = null;
let interactivePairChartMarkers = null;
let interactivePairReference = null;

// TradingView Lightweight Charts is presentation-only. Exact timestamps and gaps come from the API.
// The existing SVG below remains an offline fallback when the chart library is unavailable.
function renderInteractivePairChart(data, points, means) {
  const lib = window.LightweightCharts;
  if (!lib || !lib.createChart || !lib.LineSeries || !document.createElement) return false;
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const epoch = value => {
    const ms = Date.parse(value);
    return Number.isFinite(ms) ? ms / 1000 : null;
  };
  // Break on a missing sample or a new server-provided segment; never bridge gaps.
  const toLine = (rows, field) => {
    const sorted = rows.map(row => ({...row, time:epoch(row.sample_time)}))
      .filter(row => row.time !== null).sort((a,b) => a.time - b.time);
    const output = [];
    let previous = null;
    for (const row of sorted) {
      if (previous && row.time <= previous.time) continue;
      if (previous && previous.segment !== row.segment) {
        const gapTime = previous.time + Math.floor((row.time - previous.time) / 2);
        if (gapTime > previous.time && gapTime < row.time) output.push({time:gapTime});
      }
      output.push(finite(row[field]) ? {time:row.time, value:row[field]} : {time:row.time});
      previous = row;
    }
    return output;
  };
  const rawData = toLine(points, 'raw_spread_bps');
  if (!rawData.some(row => finite(row.value))) return false;
  try {
    if (!interactivePairChart) {
      const container = byId('pair-chart');
      container.innerHTML = '<div id="pair-chart-surface" class="pair-chart-surface"></div>' +
        '<div id="pair-chart-tooltip" class="pair-chart-tooltip" aria-live="off">移動游標可查看歷史價差</div>';
      const surface = byId('pair-chart-surface');
      const chart = lib.createChart(surface, {
        autoSize:true,
        layout:{background:{type:'solid',color:'#0d1b2b'},textColor:'#93a8bc',fontFamily:'-apple-system, BlinkMacSystemFont, sans-serif',attributionLogo:true},
        grid:{vertLines:{color:'#183044'},horzLines:{color:'#21364a'}},
        rightPriceScale:{borderColor:'#35516a',autoScale:true},
        timeScale:{
          borderColor:'#35516a',timeVisible:true,secondsVisible:false,
          barSpacing:7,minBarSpacing:0.05,rightOffset:3,
          tickMarkFormatter:t => {
            if(typeof t!=='number') return String(t);
            const date = new Date(t*1000), pad=v=>String(v).padStart(2,'0');
            return pad(date.getUTCMonth()+1)+'/'+pad(date.getUTCDate())+' '+pad(date.getUTCHours())+':'+pad(date.getUTCMinutes());
          }
        },
        localization:{
          locale:'zh-TW',
          timeFormatter:t => typeof t==='number' ? new Date(t*1000).toISOString().replace('T',' ').slice(0,19)+' UTC' : String(t)
        },
        crosshair:{mode:lib.CrosshairMode.Normal,vertLine:{color:'#64748b',labelBackgroundColor:'#334155'},horzLine:{color:'#64748b',labelBackgroundColor:'#334155'}},
        handleScroll:{mouseWheel:true,pressedMouseMove:true,horzTouchDrag:true,vertTouchDrag:true},
        handleScale:{axisPressedMouseMove:true,mouseWheel:true,pinch:true}
      });
      const raw = chart.addSeries(lib.LineSeries, {
        color:'#93c5fd',lineWidth:2,crosshairMarkerVisible:true,
        priceFormat:{type:'custom',minMove:0.01,formatter:v=>v.toFixed(2)+' bps'},
        priceLineVisible:false,lastValueVisible:true
      });
      const mean = chart.addSeries(lib.LineSeries, {
        color:'#fcd34d',lineWidth:2,priceLineVisible:false,lastValueVisible:true,
        crosshairMarkerVisible:false
      });
      const tooltip = byId('pair-chart-tooltip');
      chart.subscribeCrosshairMove(param => {
        if (!param || !param.time || !param.seriesData) {
          tooltip.textContent = '移動游標可查看歷史價差';
          return;
        }
        const at = param.seriesData.get(raw);
        const avg = param.seriesData.get(mean);
        const date = typeof param.time==='number' ? new Date(param.time*1000).toISOString().replace('T',' ').slice(0,19)+' UTC' : String(param.time);
        tooltip.textContent = date+'　價差 '+(at && finite(at.value) ? at.value.toFixed(2)+' bps' : '無資料')+
          '　24 小時均值 '+(avg && finite(avg.value) ? avg.value.toFixed(2)+' bps' : '無資料');
      });
      interactivePairChart = {chart,raw,mean};
    }
    const instance = interactivePairChart;
    const previousView = interactivePairRange === data.range ? instance.chart.timeScale().getVisibleLogicalRange() : null;
    instance.raw.setData(rawData);
    instance.mean.setData(toLine(means, 'rolling_mean_bps'));
    if (interactivePairReference) {
      instance.raw.removePriceLine(interactivePairReference);
      interactivePairReference = null;
    }
    const lifecycle = data.lifecycle || {};
    if (finite(lifecycle.reference_mean_bps)) {
      interactivePairReference = instance.raw.createPriceLine({
        price:lifecycle.reference_mean_bps,color:'#c084fc',lineWidth:1,
        lineStyle:lib.LineStyle.Dashed,axisLabelVisible:true,title:'參考均值'
      });
    }
    const availableTimes = new Set(rawData.filter(row => finite(row.value)).map(row => row.time));
    const markers = [];
    const addMarker = (time, color, label, position='aboveBar') => {
      const timestamp = epoch(time);
      if (timestamp === null || !availableTimes.has(timestamp)) return;
      markers.push({time:timestamp,position,color,shape:'circle',text:label,size:1});
    };
    addMarker(lifecycle.candidate_started_at, '#fbbf24', '候選');
    addMarker(lifecycle.confirmed_at, '#fb923c', '確認');
    addMarker(lifecycle.post_confirmation_peak_at, '#f87171', '高峰');
    if (lifecycle.active === false) addMarker(lifecycle.ended_at, '#4ade80', '回歸', 'belowBar');
    const current = data.current || {};
    if (finite(current.raw_spread_bps)) addMarker(current.sample_time, '#6ee7b7', '目前', 'belowBar');
    markers.sort((a,b) => a.time - b.time);
    if (lib.createSeriesMarkers) {
      if (!interactivePairChartMarkers) interactivePairChartMarkers = lib.createSeriesMarkers(instance.raw, markers);
      else interactivePairChartMarkers.setMarkers(markers);
    }
    if (previousView) instance.chart.timeScale().setVisibleLogicalRange(previousView);
    else instance.chart.timeScale().fitContent();
    interactivePairRange = data.range;
    return true;
  } catch (error) {
    console.warn('互動圖表無法載入，已切換備援圖表', error);
    if (interactivePairChart) interactivePairChart.chart.remove();
    interactivePairChart = null;
    interactivePairChartMarkers = null;
    interactivePairReference = null;
    return false;
  }
}

function renderPairChart(data) {
  const finite = value => typeof value === 'number' && Number.isFinite(value);
  const points = (data.history || []).filter(p => finite(p.raw_spread_bps) && Number.isFinite(Date.parse(p.sample_time)));
  const means = data.rolling_mean_series || [];
  if (renderInteractivePairChart(data, points, means)) return;
  if (interactivePairChart) { interactivePairChart.chart.remove(); interactivePairChart = null; interactivePairChartMarkers = null; interactivePairReference = null; }
  const current = data.current;
  const lifecycle = data.lifecycle || {};
  if (!points.length) { if (interactivePairChart) { interactivePairChart.chart.remove(); interactivePairChart = null; interactivePairChartMarkers = null; interactivePairReference = null; } text('pair-chart', '歷史資料不可用'); return; }
  const values = points.map(p => p.raw_spread_bps).concat(means.filter(p => finite(p.rolling_mean_bps)).map(p => p.rolling_mean_bps));
  if (current && finite(current.raw_spread_bps)) values.push(current.raw_spread_bps);
  const v2Reference = finite(lifecycle.reference_mean_bps) ? lifecycle.reference_mean_bps : null;
  if (v2Reference !== null) values.push(v2Reference);
  const start = Date.parse(points[0].sample_time), end = Date.parse(points.at(-1).sample_time);
  let low = Math.min(...values), high = Math.max(...values);
  const pad = Math.max((high - low) * .08, 1); low -= pad; high += pad;
  const x = time => 70 + (Date.parse(time) - start) / Math.max(end - start, 1) * 840;
  const y = value => 290 - (value - low) / (high - low) * 260;
  function series(rows, field, name, color) {
    let path = '', previous = null;
    for (const point of rows) {
      if (!finite(point[field]) || !Number.isFinite(Date.parse(point.sample_time))) { previous = null; continue; }
      const px = x(point.sample_time).toFixed(2), py = y(point[field]).toFixed(2);
      path += (previous && previous.segment === point.segment ? 'L' : 'M') + px + ',' + py + ' ';
      previous = point;
    }
    return path ? '<g><path data-series="' + name + '" d="' + path + '" fill="none" stroke="' + color + '" stroke-width="1.6"/></g>' : '';
  }
  let svg = '<svg viewBox="0 0 960 350" role="img" aria-label="可成交價差與僅使用歷史資料的 24 小時均值（bps）"><title>精確採樣的可成交價差歷史</title>';
  for (let i = 0; i <= 4; i++) {
    const value = low + (high - low) * i / 4, py = y(value).toFixed(2);
    svg += '<line x1="70" x2="910" y1="' + py + '" y2="' + py + '" stroke="#374151"/><text x="60" y="' + py + '" text-anchor="end" fill="#94a3b8" font-size="11">' + number(value) + '</text>';
  }
  const duration = Math.max(end - start, 1);
  const axisLabel = timestamp => {
    const date = new Date(timestamp), pad = part => String(part).padStart(2, '0');
    const clock = pad(date.getUTCHours()) + ':' + pad(date.getUTCMinutes());
    if (duration <= 6 * 60 * 60 * 1000) return clock + ':' + pad(date.getUTCSeconds()) + 'Z';
    if (duration <= 3 * 24 * 60 * 60 * 1000) return clock + 'Z';
    return pad(date.getUTCMonth() + 1) + '-' + pad(date.getUTCDate()) + ' ' + clock + 'Z';
  };
  svg += '<g data-axis="x">';
  const tickCount = duration <= 6 * 60 * 60 * 1000 ? 5 : 6;
  for (let index = 0; index < tickCount; index++) {
    const timestamp = start + duration * index / (tickCount - 1);
    const px = x(new Date(timestamp).toISOString()).toFixed(2);
    svg += '<line class="axis-tick" x1="' + px + '" x2="' + px + '" y1="290" y2="296"/>';
    svg += '<text data-axis-tick="' + index + '" x="' + px + '" y="320" text-anchor="middle" fill="#94a3b8" font-size="11">' + axisLabel(timestamp) + '</text>';
  }
  svg += '</g>';
  svg += series(points, 'raw_spread_bps', 'raw', '#93c5fd') + series(means, 'rolling_mean_bps', 'mean', '#fcd34d');
  const labelPositions = [];
  const clampLabelY = value => Math.max(18, Math.min(value, 282));
  const placeLabel = (xValue, preferredY) => {
    const candidates = [preferredY, preferredY - 18, preferredY - 36, preferredY + 18, preferredY + 36,
      18, 36, 54, 72, 90, 108, 126, 144, 162, 180, 198, 216, 234, 252, 270];
    for (const candidate of candidates) {
      const yValue = clampLabelY(candidate);
      if (!labelPositions.some(position => Math.abs(position.x - xValue) < 120 && Math.abs(position.y - yValue) < 14)) {
        labelPositions.push({x:xValue, y:yValue});
        return yValue;
      }
    }
    const yValue = clampLabelY(preferredY);
    labelPositions.push({x:xValue, y:yValue});
    return yValue;
  };
  if (v2Reference !== null) {
    const py = y(v2Reference).toFixed(2);
    svg += '<line data-marker="reference" x1="70" x2="910" y1="' + py + '" y2="' + py + '" stroke="#c084fc" stroke-dasharray="5 4"/>';
    const referenceY = placeLabel(78, Number(py) - 4);
    svg += '<text data-marker-label="reference" data-label-lane="reference" x="78" y="' + referenceY.toFixed(2) + '" text-anchor="start" fill="#c084fc" font-size="11" >參考基準</text>';
  }
  if (current && finite(current.raw_spread_bps) && Date.parse(current.sample_time) >= start && Date.parse(current.sample_time) <= end) {
    const currentX = x(current.sample_time), currentY = y(current.raw_spread_bps);
    const currentAnchor = currentX > 840 ? 'end' : 'start';
    const currentLabelX = currentX + (currentAnchor === 'end' ? -8 : 8);
    const currentLabelY = placeLabel(currentLabelX, currentY - 10);
    svg += '<circle data-series="current" data-marker="current" cx="' + currentX.toFixed(2) + '" cy="' + currentY.toFixed(2) + '" r="5" fill="#6ee7b7"><title>目前 · ' + esc(current.sample_time) + ' · ' + number(current.raw_spread_bps) + ' bps</title></circle>';
    svg += '<text data-marker-label="current" data-label-lane="current" x="' + currentLabelX.toFixed(2) + '" y="' + currentLabelY.toFixed(2) + '" text-anchor="' + currentAnchor + '" fill="#6ee7b7" font-size="10">目前</text>';
  }
  if (v2Reference !== null || lifecycle.available) {
    const rawAt = timestamp => {
      const point = points.find(item => item.sample_time === timestamp);
      return point && finite(point.raw_spread_bps) ? point.raw_spread_bps : null;
    };
    const markers = [
      ['candidate', lifecycle.candidate_started_at, rawAt(lifecycle.candidate_started_at), '#fbbf24'],
      ['confirmation', lifecycle.confirmed_at, rawAt(lifecycle.confirmed_at), '#fb923c'],
      ['peak', lifecycle.post_confirmation_peak_at, rawAt(lifecycle.post_confirmation_peak_at), '#f87171'],
      ['return', lifecycle.active === false ? lifecycle.ended_at : null, lifecycle.active === false ? lifecycle.current_spread_bps : null, '#4ade80'],
    ];
    for (const [label, timestamp, value, color] of markers) {
      if (typeof timestamp !== 'string' || !finite(value)) continue;
      const time = Date.parse(timestamp);
      if (!Number.isFinite(time) || time < start || time > end) continue;
      const markerX = x(timestamp), markerY = y(value);
      const anchor = markerX < 130 ? 'start' : markerX > 850 ? 'end' : 'middle';
      const labelX = markerX + (anchor === 'start' ? 7 : anchor === 'end' ? -7 : 0);
      const lane = {candidate:0, confirmation:1, peak:2, return:3}[label] ?? 0;
      const labelY = placeLabel(labelX, markerY - 14 - lane * 18);
      svg += '<circle data-marker="' + label + '" cx="' + markerX.toFixed(2) + '" cy="' + markerY.toFixed(2) + '" r="4" fill="' + color + '"><title>' + esc(label) + ' · ' + esc(timestamp) + ' · ' + number(value) + ' bps</title></circle>';
      svg += '<text data-marker-label="' + label + '" data-label-lane="' + label + '" x="' + labelX.toFixed(2) + '" y="' + labelY.toFixed(2) + '" text-anchor="' + anchor + '" fill="' + color + '" font-size="10">' + label + '</text>';
    }
  }
  svg += '</svg>';
  byId('pair-chart').innerHTML = svg;
}
function compactDataStatus(data) {
  const ageSeconds = typeof data.scanner_sample_age_seconds === 'number'
    ? data.scanner_sample_age_seconds
    : data.sample_age_seconds;
  const exactTimes = [data.data_as_of, data.scanner_data_as_of, data.generated_at]
    .filter(value => typeof value === 'string' && value);
  byId('data-as-of').title = exactTimes.join(' · ');
  byId('refreshed').title = typeof data.generated_at === 'string' ? data.generated_at : '';
  text('data-as-of', typeof ageSeconds === 'number' && Number.isFinite(ageSeconds) ? '資料延遲 ' + age(ageSeconds) : '');
  text('refreshed', '唯讀資料源');
}
let requestSequence = 0;
let requestInFlight = false;
async function refresh(query = new URLSearchParams(location.search)) {
  const sequence = ++requestSequence;
  requestInFlight = true;
  try {
    const response = await fetch(isStatus ? '/api/status' : isAnomalies ? '/api/anomalies?' + query.toString() : (isPair ? '/api/pair?' : '/api/opportunities?') + query.toString(), {cache:'no-store'});
    const data = await response.json();
    if (sequence !== requestSequence) return;
    const status = isStatus ? data.overall?.status : data.status;
    const statusLabel = isOpportunities && status === 'degraded' ? '掃描資料異常' : statusZh(status);
    text('overall', statusLabel);
    byId('overall').className = statusClass(status);
    text('scanner-status', '掃描器 · ' + statusZh(status));
    byId('scanner-status').className = 'scanner-status ' + statusClass(status);
    compactDataStatus(data);
    byId('errors').innerHTML = (data.errors || []).map(error => '<div class="error">' + esc(error) + '</div>').join('');
    if (isStatus) renderStatus(data); else if (isPair) renderPair(data); else if (isAnomalies) renderAnomalies(data); else renderOpportunities(data);
  } catch (error) {
    if (sequence !== requestSequence) return;
    text('overall', '儀表板讀取失敗');
    byId('overall').className = 'degraded';
    text('scanner-status', '掃描器 · 部分異常');
    byId('scanner-status').className = 'scanner-status degraded';
    compactDataStatus({});
    text('refreshed', '請求失敗');
    byId('errors').innerHTML = '<div class="error">' + esc(error) + '</div>';
    if (isStatus) renderStatus({}); else if (isPair) renderPair({}); else if (isAnomalies) byId('anomaly-rows').innerHTML = emptyRow(6, '資料不可用'); else byId('opportunity-rows').innerHTML = emptyRow(6, '資料不可用');
  } finally {
    if (sequence === requestSequence) requestInFlight = false;
  }
}
restoreFilters();
restorePairRange();
byId('pair-chart-reset').addEventListener('click', () => { if (interactivePairChart) interactivePairChart.chart.timeScale().fitContent(); });
byId('pair-chart-latest').addEventListener('click', () => { if (interactivePairChart) interactivePairChart.chart.timeScale().scrollToRealTime(); });
byId('pair-range').addEventListener('change', () => {
  const query = new URLSearchParams(location.search);
  query.set('range', byId('pair-range').value);
  history.replaceState(null, '', location.pathname + '?' + query.toString());
  refresh(query);
});
if (isAnomalies) {
  byId('anomaly-filters').addEventListener('submit', event => {
    event.preventDefault();
    const query = anomalyFilterQuery();
    history.replaceState(null, '', location.pathname + (query.size ? '?' + query.toString() : ''));
    refresh(query);
  });
  byId('anomaly-reset').addEventListener('click', () => {
    for (const name of anomalyFilterNames) byId('anomaly-' + name.replace('_', '-')).value = '';
    byId('anomaly-eligible-only').checked = false;
    history.replaceState(null, '', location.pathname);
    refresh(new URLSearchParams());
  });
} else if (!isStatus && !isPair) {
  byId('filters').addEventListener('submit', event => {
    event.preventDefault();
    const query = opportunityFilterQuery();
    history.replaceState(null, '', location.pathname + (query.size ? '?' + query.toString() : ''));
    refresh(query);
  });
  byId('reset').addEventListener('click', () => {
    for (const name of filterNames) byId(name).value = '';
    byId('active_only').checked = false;
    history.replaceState(null, '', location.pathname);
    refresh(new URLSearchParams());
  });
}
window.addEventListener('popstate', () => { restoreFilters(); restorePairRange(); refresh(); });
refresh();
setInterval(() => { if (!requestInFlight) refresh(); }, 10000);
</script>
</body>
</html>"""


def _handler_for(
    service: DashboardStatusService | DashboardQueryService,
) -> type[BaseHTTPRequestHandler]:
    class DashboardRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            request = urlparse(self.path)
            path = request.path
            if path in ("/", "/anomalies", "/opportunities", "/status", "/pair"):
                view = (
                    "pair" if path == "/pair" else
                    "status" if path == "/status" else
                    "anomalies" if path == "/anomalies" else
                    "opportunities"
                )
                self._send(200, "text/html; charset=utf-8", HTML.replace("__VIEW__", view).encode("utf-8"))
                return
            if path == "/api/pair":
                empty: dict[str, object] = {
                    "status": "degraded", "data_as_of": None, "current": None,
                    "basis": None, "lifecycle": None, "history": [],
                    "rolling_mean_series": [],
                }
                try:
                    pair = parse_pair_query(request.query)
                except ValueError as error:
                    self._json(400, {**empty, "errors": [str(error)]})
                    return
                symbol = pair["canonical_symbol"]
                feeds = {(m.venue, m.venue_symbol, m.canonical_symbol)
                         for m in service.config.markets if m.enabled}
                if (pair["long_venue"].lower() == pair["short_venue"].lower()
                    or (pair["long_venue"], pair["long_venue_symbol"], symbol) not in feeds
                    or (pair["short_venue"], pair["short_venue_symbol"], symbol) not in feeds):
                    self._json(404, {**empty, "errors": ["pair identity is not an enabled configured mapping"]})
                    return
                try:
                    payload = service.get_pair(
                        canonical_symbol=pair["canonical_symbol"],
                        long_venue=pair["long_venue"],
                        long_venue_symbol=pair["long_venue_symbol"],
                        short_venue=pair["short_venue"],
                        short_venue_symbol=pair["short_venue_symbol"],
                        range_name=cast(PairRange, pair["range_name"]),
                    )
                    if payload.get("errors") and payload.get("status") == "down":
                        payload = {**payload, "status": "degraded"}
                    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
                except Exception as error:  # noqa: BLE001
                    self._json(200, {**empty, "generated_at": utc_now().isoformat(),
                        "identity": {k: v for k, v in pair.items() if k != "range_name"},
                        "range": pair["range_name"],
                        "errors": [f"dashboard query failed: {type(error).__name__}: {error}"]})
                    return
                self._send(200, "application/json; charset=utf-8", body)
                return
            if path == "/api/anomalies":
                try:
                    filters = parse_anomaly_query(request.query)
                    payload = service.get_anomalies(
                        symbol=cast(str | None, filters["symbol"]),
                        long_venue=cast(str | None, filters["long_venue"]),
                        short_venue=cast(str | None, filters["short_venue"]),
                        eligible_only=cast(bool, filters["eligible_only"]),
                    )
                    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
                except ValueError as error:
                    self._json(400, {"status": "degraded", "rows": [], "errors": [str(error)]})
                    return
                except Exception as error:  # noqa: BLE001
                    self._json(200, {
                        "status": "degraded", "rows": [],
                        "errors": [f"dashboard query failed: {type(error).__name__}: {error}"],
                    })
                    return
                self._send(200, "application/json; charset=utf-8", body)
                return
            if path in ("/api/status", "/api/opportunities"):
                opportunity_filters: OpportunitiesFilters | None = None
                if path == "/api/opportunities":
                    try:
                        opportunity_filters = parse_opportunities_filters(request.query)
                    except ValueError as error:
                        self._json(400, {"status": "degraded", "data_as_of": None, "rows": [], "errors": [str(error)]})
                        return
                try:
                    payload = (
                        service.get_status()
                        if opportunity_filters is None
                        else service.get_opportunities(opportunity_filters)
                    )
                    # A failed storage read is a dashboard degradation, never
                    # evidence that Radar itself is down. Status retains the
                    # heartbeat-authoritative compatibility contract.
                    if (
                        opportunity_filters is not None
                        and payload.get("errors")
                        and payload.get("status") == "down"
                    ):
                        payload = {**payload, "status": "degraded"}
                    body = json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
                except Exception as error:  # noqa: BLE001
                    self._json(200, {
                        "generated_at": utc_now().isoformat(),
                        "data_as_of": None,
                        "status": "degraded",
                        "overall": {"status": "degraded"},
                        "feeds": [], "rows": [], "episodes": [], "recent_events": [],
                        "errors": [f"dashboard query failed: {type(error).__name__}: {error}"],
                    })
                    return
                self._send(200, "application/json; charset=utf-8", body)
                return
            if path.startswith("/api/"):
                self._json(404, {"errors": ["unknown API path"]})
                return
            self.send_error(404)

        def _json(self, status: int, payload: dict[str, object]) -> None:
            self._send(status, "application/json; charset=utf-8",
                       json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8"))

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    return DashboardRequestHandler


def create_dashboard_server(
    service: DashboardStatusService | DashboardQueryService,
    *,
    port: int,
    host: str = "127.0.0.1",
) -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), _handler_for(service))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Opportunity Radar dashboard")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--runtime-db", type=Path, default=DEFAULT_RUNTIME_DB)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    service = DashboardStatusService(
        config, data_root=args.data_root, runtime_db=args.runtime_db,
    )
    server: ThreadingHTTPServer | None = None
    try:
        service.start(background=True)
        server = create_dashboard_server(service, host=args.host, port=args.port)
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if server is not None:
            server.server_close()
        service.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
