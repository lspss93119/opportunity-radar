# Ondo read-only feed — integration contract

Status: **implementation gate**, not deployed. Ondo is not yet registered as a live collector.

## Objective

Add Ondo market data using existing Radar architecture; no order entry,
signing, private account state, reward farming, or production configuration
changes. Preserve 10-second Radar samples and existing market definitions.

## Confirmed external references

- Ondo public market data OpenAPI:
  https://docs.ondo.finance/api-reference/quickstart
- Ondo documented raw market routes include
  `GET /v1/perps/contracts`, `GET /v1/perps/depth`,
  `GET /v1/perps/trades`, `GET /v1/perps/open_interest` and
  `GET /v1/perps/volume`.
- Ondo symbols use `{TICKER}-USD.P` according to independent implementations:
  https://github.com/discountry/ritmex-bot/blob/main/docs/ondoperps/README.md
- LI.FI's independent Ondo provider supports public orderbooks and market
  streams: https://github.com/lifinance/perps-sdk/tree/main/packages/perps-sdk-provider-ondo

**Do not assume** the public REST data envelope, symbol query field, level
units, or funding unit without fixtures captured from the actual interface.
The above documentation is not proof that every public route is accessible
without authentication at runtime.

## Collector implementation checklist

1. Add `radar.collectors.ondo.OndoCollector` implementing the same lifecycle
   and `CollectorBatch` contract as `ArcusCollector` / `BackpackCollector`.
2. Register it in `MarketDataPipeline.from_config` **only when**
   `markets_for_venue(config.markets, "ondo")` is nonempty. No implicit
   market discovery should enable extra feeds.
3. Use `MarketConfig(venue="ondo", venue_symbol="BTC-USD.P", canonical_symbol="BTC")`
   style mappings, and verify contract notional/size normalization before
   enabling any cross-venue pair.
4. Produce existing `MarketSnapshot`, `FundingSnapshot`, and
   `HourlyContext`. Missing / invalid fields remain null or unavailable:
   never use the last valid book as a fresh executable quote.
5. Compute $1k/$5k/$10k VWAP with existing `radar.vwap` helpers,
   using verified BASE asset level sizes. Do not treat contracts as base
   quantity until the contract multiplier is confirmed.
6. Reuse Radar's injected `request_json`, clock, error handler, shared
   `LatestMarketData`, Parquet and lifecycle logic. Avoid a new storage
   system, app process, or monitor.
7. Add deterministic fixtures/tests covering symbols, book ordering,
   quantities, invalid payload, timestamps, missing book, staleness,
   the factory, funding units, and fees. Run non-live suite and a
   separate public API smoke before any production decision.

## Configuration safety

The `ondo` venue requires an explicit `fees_bps.ondo` value if enabled.
Do **not** guess the fee or set it to 0. The exact taker/maker fee must come
from the live account tier and applicable rules; reward/USDC subsidies are
not negative execution fees and belong in research PnL attribution.

Proposed initial allowlist: BTC, ETH, NVDA; SNDK and MU only after
symbol/contract verification. No Ondo markets are enabled by this branch.

## Separate research lane

10-second `MarketSnapshot` is suitable for cross-exchange spread and
funding research, **not** for maker queue fills. After standard feed acceptance,
consider an isolated order-book/trade-event recorder for maker research.
No shadow maker fills or USDC rewards should be inferred from BBO alone.

## Deployment gates

- Existing production unchanged, no restart, no alerts.
- Only add enabled market rows after response schema and unit verification.
- Merge only with non-live tests passing and live public-data smoke inspected.
- No claims of collector readiness or USDC profitability before those gates.
