from __future__ import annotations

from radar.collectors.base import CollectorBatch
from radar.models import FundingSnapshot, HourlyContext, MarketSnapshot


class RadarState:
    """Latest normalized data view for future monitors."""

    def __init__(self) -> None:
        self._markets: dict[tuple[str, str], MarketSnapshot] = {}
        self._funding: dict[tuple[str, str, str], FundingSnapshot] = {}
        self._hourly_context: dict[tuple[str, str, str], HourlyContext] = {}

    def apply_market_batch(self, batch: CollectorBatch) -> None:
        # A collection round is authoritative for executable market data. Missing
        # venues/symbols are deliberately absent instead of being carried forward.
        self._markets = {
            (snapshot.venue, snapshot.venue_symbol): snapshot
            for snapshot in batch.market_snapshots
        }

    def apply_context_batch(self, batch: CollectorBatch) -> None:
        self._funding = {
            (
                snapshot.venue,
                snapshot.venue_symbol,
                snapshot.canonical_symbol,
            ): snapshot
            for snapshot in batch.funding_snapshots
        }
        self._hourly_context = {
            (
                context.venue,
                context.venue_symbol,
                context.canonical_symbol,
            ): context
            for context in batch.hourly_contexts
        }

    def apply(self, batch: CollectorBatch, *, replace_context: bool = False) -> None:
        """Apply a legacy combined batch using the separated state paths."""
        self.apply_market_batch(batch)
        if replace_context:
            self.apply_context_batch(batch)

    @property
    def markets(self) -> tuple[MarketSnapshot, ...]:
        return tuple(self._markets[key] for key in sorted(self._markets))

    @property
    def funding(self) -> tuple[FundingSnapshot, ...]:
        return tuple(self._funding[key] for key in sorted(self._funding))

    @property
    def hourly_context(self) -> tuple[HourlyContext, ...]:
        return tuple(self._hourly_context[key] for key in sorted(self._hourly_context))

    def get_market(self, venue: str, venue_symbol: str) -> MarketSnapshot | None:
        return self._markets.get((venue, venue_symbol))
