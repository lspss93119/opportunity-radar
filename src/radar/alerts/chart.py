from __future__ import annotations

from io import BytesIO

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from radar.alerts.models import SpreadAlertDetails
from radar.history.spread import HistoricalSpreadContext


def render_spread_chart(
    details: SpreadAlertDetails,
    context: HistoricalSpreadContext,
) -> bytes | None:
    if len(context.points_7d) < 2:
        return None

    figure, axis = plt.subplots(figsize=(9, 4.5))
    try:
        timestamps = [point.sample_time for point in context.points_7d]
        spreads = [point.raw_spread_bps for point in context.points_7d]
        axis.plot(timestamps, spreads, marker="o", label="7d raw spread")
        axis.axhline(
            details.raw_spread_bps,
            color="black",
            linestyle="--",
            label="current raw spread",
        )
        if context.stats_7d.median_raw_spread_bps is not None:
            axis.axhline(
                context.stats_7d.median_raw_spread_bps,
                color="tab:blue",
                linestyle=":",
                label="7d median",
            )
        if context.stats_30d.median_raw_spread_bps is not None:
            axis.axhline(
                context.stats_30d.median_raw_spread_bps,
                color="tab:orange",
                linestyle=":",
                label="30d median",
            )
        median_90d = context.stats_90d.median_raw_spread_bps
        annotation = (
            f"90d median: {median_90d:.2f} bps"
            if median_90d is not None
            else "90d median: unavailable"
        )
        axis.text(
            0.99,
            0.02,
            annotation,
            transform=axis.transAxes,
            horizontalalignment="right",
            verticalalignment="bottom",
        )
        axis.set_title(
            f"{details.canonical_symbol} | Long {details.long_venue} / "
            f"Short {details.short_venue} | ${details.primary_size_usd:,}"
        )
        axis.set_xlabel("UTC time")
        axis.set_ylabel("Raw spread (bps)")
        axis.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz="UTC"))
        axis.legend()
        figure.autofmt_xdate()
        output = BytesIO()
        figure.savefig(output, format="png", dpi=120, bbox_inches="tight")
        return output.getvalue()
    finally:
        plt.close(figure)
