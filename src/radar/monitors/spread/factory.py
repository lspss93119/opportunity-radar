from __future__ import annotations

from radar.config import RadarConfig
from radar.monitors.base import Monitor
from radar.monitors.spread.monitor import SpreadMonitor
from radar.storage.sqlite import SQLiteRuntimeStore


def create_spread_monitor(
    config: RadarConfig,
    runtime_store: SQLiteRuntimeStore | None = None,
) -> Monitor:
    return SpreadMonitor(
        config.monitors.spread,
        config.fees_bps,
        runtime_store=runtime_store,
    )
