# Opportunity Radar Manual Opportunity v1 Phase 2 Design

## Status

Approved implementation scope for wiring the existing Manual Opportunity v1
lifecycle into the live Radar path. Signal semantics are fixed by the Phase 1
rules and are not redesigned here.

## Goal

Run the existing BBO-only Manual Opportunity lifecycle on the aligned 10-second
market cadence, restore its three-day BBO history and persisted episode state on
startup, route its text-only alerts separately from spread alerts, and keep the
Anomaly v2 lifecycle available internally while allowing its Telegram emission
to be disabled.

## Non-goals and invariants

- No automated trading, order placement, wallet signing, private APIs, or
  execution path.
- No new collector or normalized model; Manual Opportunity consumes existing
  `MarketSnapshot` and `HourlyContext` data.
- No VWAP, funding, std, trajectory, ENME, stop-loss, return, or resolved
  signal semantics in Manual Opportunity.
- Do not change the approved thresholds, fees, 30-second BBO freshness, or
  60-second persistence rule.
- Spread and Anomaly v2 lifecycle semantics remain available independently of
  Manual Opportunity.
- Fast monitor evaluation remains in-memory and must not perform DuckDB,
  Parquet, chart, or Telegram I/O.

## Fixed signal semantics

For every exact directional pair sharing a canonical symbol and using different
venues:

```text
current_spread_bps = (short.best_bid / long.best_ask - 1) * 10000
a = strictly-prior 24h BBO mean
baseline_range = max(mean_2h, mean_24h, mean_3d)
                 - min(mean_2h, mean_24h, mean_3d)
round_trip_fee = 2 * (long taker fee + short taker fee)
expected_net_at_a = current_spread_bps - a - round_trip_fee
route_volume = min(long volume_24h, short volume_24h)
```

An observation is eligible only when the three means are available, baseline
range is at most 5 bps, expected net is at least 10 bps, route volume is at
least $1,000,000, and both BBO timestamps are fresh within 30 seconds of the
actual evaluation time. Candidate timing uses:

```text
available_at = max(sample_time, long_observed_at, short_observed_at)
```

After 60 seconds of continuous eligibility, emit `manual_initial`; later emit
`manual_expansion` at 15/20/25/30... expected-net levels using the episode's
frozen `a` and frozen fees. No other Manual Opportunity event is a Telegram
alert.

## Runtime data flow

```text
Collectors
    -> RadarState (latest MarketSnapshot / HourlyContext)
    -> MonitorRunner (10-second cadence)
    -> ManualOpportunityMonitor
    -> ManualOpportunityLifecycle + SQLite runtime state
    -> AlertRequest(monitor="manual_opportunity")
    -> AlertWorker router
    -> ManualOpportunityAlertProcessor
    -> Telegram text only
```

Spread remains on its existing path. The application does not calculate signal
conditions; it only assembles the data source, monitor registry, router, and
shutdown lifecycle.

## Components and interfaces

### ManualOpportunityMonitor

Extend the existing manual-opportunity monitor module with a small
`ManualOpportunityMonitor` implementing the existing `Monitor` protocol:

```python
class ManualOpportunityMonitor:
    name = "manual_opportunity"
    interval_seconds = 10

    async def evaluate(
        self, now: datetime, state: RadarState
    ) -> list[AlertRequest]: ...

    def hydrate_history(
        self,
        points_by_key: Mapping[
            SpreadPairKey, Sequence[tuple[datetime, float]]
        ],
    ) -> None: ...
```

The monitor owns one `BboRollingHistory` per exact `SpreadPairKey` and one
`ManualOpportunityLifecycle` configured with the existing SQLite runtime store.
It groups current market snapshots by canonical symbol, creates every ordered
cross-venue pair, reads matching latest hourly volume by exact venue/symbol/
canonical identity, and creates observations using only best ask/bid. Missing,
stale, future, invalid, or insufficient data fails closed. Missing current
routes call the lifecycle gap path so persisted candidates cannot continue
through absent market data.

The monitor never reads Parquet or calls DuckDB during `evaluate()`.

### Startup BBO hydration

Add a read-only history loader in the existing manual history module. It reads
only the market dataset columns required for BBO history, filters to enabled
feeds, uses `sample_time < as_of`, and limits the window to three days. It
deduplicates each exact feed/sample slot by latest `observed_at`, rejects rows
not available by `as_of`, groups one sample slot at a time, and derives the
same directional raw BBO spreads used by the replay.

The loader must stream Arrow/DuckDB batches and avoid one giant full-dataset
intermediate list. It may retain only route-local three-day points needed to
hydrate the monitor's bounded rolling histories. The loader returns route
points plus hydration statistics (routes, observations, elapsed time); it does
not mutate production storage.

Application startup records RSS before and after hydration, elapsed duration,
route count, and observation count at INFO level. RSS reporting uses the
standard-library process measurement available on the target platform; no new
dependency is introduced.

### Monitor registry

The registry constructs enabled factories independently and in stable order:

1. `spread` when `config.monitors.spread.enabled` is true;
2. `manual_opportunity` when `config.manual_opportunity.enabled` is true.

Both may coexist. Duplicate monitor names remain rejected by `MonitorRunner`.
The Manual Opportunity factory uses the configured 10-second sampling cadence
and the existing fixed `fees_bps` mapping; it never changes fee values.

### Alert routing

Keep `AlertWorker`'s queue and generic error handling. Add a small router, not
a plugin/event-bus framework, that dispatches:

```text
alert.monitor == "spread"             -> SpreadAlertProcessor
alert.monitor == "manual_opportunity" -> ManualOpportunityAlertProcessor
```

`ManualOpportunityAlertProcessor` parses the existing manual payload, uses the
existing manual formatter, and calls only `TelegramTransport.send_text`.
It never queries history, renders a chart, or sends a photo. Unknown monitor
names fail through the existing worker error path.

Initial and expansion formatting remains compact and includes the approved
manual context: direction, current spread, frozen/normal basis, deviation,
four-leg fee, expected net, baseline means/range, both volumes, route minimum,
BBO prices, sample time, and expansion level when applicable. It excludes
ENME, funding, std, trajectory, VWAP net, old mean-alignment wording, and the
old anomaly RT-fee field. There are no manual return/resolved messages.

### Anomaly v2 Telegram gate

Add `telegram_enabled: bool = True` to `AnomalyV2Config` so old configs retain
their behavior. The setting is under `monitors.spread.anomaly_v2`.

When false, `SpreadMonitor` still runs the Anomaly v2 lifecycle and persists
its state/events to SQLite, but returns no Anomaly v2 `AlertRequest` to the
monitor queue. This suppresses Telegram emission without deleting Dashboard or
historical research state. Manual Opportunity has its own independent queue
route.

## Restart behavior

`ManualOpportunityLifecycle` remains the source of persisted active episode
state. On startup it restores unconfirmed candidates, confirmed episodes,
frozen basis/fees, and the expansion watermark from SQLite. Hydrated BBO points
are history only and are never evaluated as a new live observation.

The first post-restart evaluation uses a newly collected current snapshot and
actual `now`; it must pass the 30-second freshness checks and continuity gap
before it can update or expand a restored episode. Stale restored observations
cannot emit an expansion by themselves. Persistence failure retains the
existing Phase 1 rollback behavior.

## Configuration

The example config keeps Manual Opportunity disabled by default:

```yaml
manual_opportunity:
  enabled: false
  confirmation_seconds: 60
  baseline_range_max_bps: 5.0
  expected_net_min_bps: 10.0
  volume_24h_min_usd: 1000000
  expansion_notify_step_bps: 5.0
  max_gap_seconds: 20
```

Production changes only `manual_opportunity.enabled: true` and
`monitors.spread.anomaly_v2.telegram_enabled: false`, leaving all existing
fees and signal thresholds unchanged.

## Testing and acceptance

Deterministic tests must cover:

- registry combinations and independent enablement;
- streaming/strictly-prior BBO hydration, exact-pair identity, latest prior
  hourly volume, and current sample exclusion from its own mean;
- stale/future BBO fail-closed behavior;
- restored candidate/confirmed/frozen expansion state and stale restoration
  suppression;
- coexistence of spread and manual monitors and isolated monitor errors;
- manual initial/expansion text formatting and text-only delivery;
- spread routing unchanged;
- anomaly v2 Telegram disabled while lifecycle persistence remains active;
- no manual return/resolved Telegram requests;
- a temporary integration smoke that hydrates history, evaluates both monitors,
  queues a manual initial and expansion, and never sends Telegram or trades.

Before deployment, run the full non-live pytest suite, available Ruff/Mypy,
compileall, and `git diff --check`.

## Production deployment boundary

Deployment is a separate operational step after the tested commit is pushed:

1. Verify `trading-mini`, clean production checkout, expected old HEAD, Radar
   and Dashboard PIDs, config hash, runtime DB, and latest dataset timestamps.
2. Back up the production YAML with a timestamped path without exposing secrets.
3. Fast-forward production to the exact tested commit only.
4. Apply only the two intended config changes and validate with the production
   loader.
5. Restart only the existing Radar tmux-owned process; do not restart Dashboard.
6. Verify hydration, monitor registration, anomaly lifecycle persistence,
   market/hourly collection, Parquet/SQLite health, and at least three aligned
   cycles. Do not manufacture a real Telegram opportunity.

If startup or validation fails, restore the config backup, return to the prior
known-good production revision through a safe Git path, restart the existing
Radar command, and verify service health before leaving the host.

No synthetic Telegram smoke is required when transport health is already known;
if one is necessary it must be one clearly labelled `TEST / SYNTHETIC` manual
message only.
