from __future__ import annotations

from collections.abc import Callable
from typing import TypeAlias

from radar.config import RadarConfig, SpreadMonitorConfig
from radar.monitors.base import Monitor

MonitorFactory: TypeAlias = Callable[[SpreadMonitorConfig], Monitor]

# Task 5 will add the real spread monitor factory. Task 4 keeps the mapping
# explicit without dynamic discovery or a plugin loader.
MONITOR_FACTORIES: dict[str, MonitorFactory] = {}


def build_enabled_monitors(
    config: RadarConfig,
) -> tuple[Monitor, ...]:
    spread_config = config.monitors.spread
    if not spread_config.enabled:
        return ()
    factory = MONITOR_FACTORIES.get("spread")
    if factory is None:
        return ()
    return (factory(spread_config),)
