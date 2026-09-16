from __future__ import annotations

from collections.abc import Callable
from typing import TypeAlias

from radar.config import RadarConfig
from radar.monitors.base import Monitor
from radar.monitors.spread.factory import create_spread_monitor
from radar.storage.sqlite import SQLiteRuntimeStore

MonitorFactory: TypeAlias = Callable[
    [RadarConfig, SQLiteRuntimeStore | None], Monitor
]

MONITOR_FACTORIES: dict[str, MonitorFactory] = {
    "spread": create_spread_monitor,
}


def build_enabled_monitors(
    config: RadarConfig,
    *,
    runtime_store: SQLiteRuntimeStore | None = None,
) -> tuple[Monitor, ...]:
    spread_config = config.monitors.spread
    if not spread_config.enabled:
        return ()
    factory = MONITOR_FACTORIES.get("spread")
    if factory is None:
        return ()
    return (factory(config, runtime_store),)
