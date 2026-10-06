from __future__ import annotations

from collections.abc import Callable
from typing import TypeAlias

from radar.config import RadarConfig
from radar.monitors.base import Monitor
from radar.monitors.manual_opportunity import ManualOpportunityMonitor
from radar.monitors.spread.factory import create_spread_monitor
from radar.storage.sqlite import SQLiteRuntimeStore

MonitorFactory: TypeAlias = Callable[
    [RadarConfig, SQLiteRuntimeStore | None], Monitor
]

MONITOR_FACTORIES: dict[str, MonitorFactory] = {
    "spread": create_spread_monitor,
    "manual_opportunity": lambda config, runtime_store: ManualOpportunityMonitor(
        config.manual_opportunity,
        config.fees_bps,
        runtime_store=runtime_store,
        interval_seconds=config.sampling_seconds,
        stale_after_seconds=config.monitors.spread.stale_after_seconds,
    ),
}


def build_enabled_monitors(
    config: RadarConfig,
    *,
    runtime_store: SQLiteRuntimeStore | None = None,
) -> tuple[Monitor, ...]:
    enabled_names = (
        ("spread", config.monitors.spread.enabled),
        ("manual_opportunity", config.manual_opportunity.enabled),
    )
    monitors: list[Monitor] = []
    for name, enabled in enabled_names:
        if not enabled:
            continue
        factory = MONITOR_FACTORIES.get(name)
        if factory is not None:
            monitors.append(factory(config, runtime_store))
    return tuple(monitors)
