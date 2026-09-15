# Opportunity Radar — Architecture

## Core flow

```text
Collectors -> normalize -> RadarState + Parquet
                              |
                              v
                        Monitor Runner
                              |
                         FAST evaluate
                              |
                         AlertRequest
                              |
                         asyncio.Queue
                              |
                         SLOW worker
                    DuckDB/chart -> Telegram
```

## Boundaries
1. Data Source != Monitor.
2. Monitor != Application.
3. Fast Scan != Slow Historical Work.

## Shared data
`MarketSnapshot`, `FundingSnapshot`, and `HourlyContext`. v1 fixed executable sizes are $1k / $5k / $10k.

## RadarState
A single in-memory latest-data view for monitors. It stores latest shared datasets and performs no opportunity calculation or historical query.

## Collectors
Each venue adapter only knows that venue and converts venue-specific data into shared models. Failed data is missing/NULL; old executable values are never reused as fresh values.

## Storage
Parquet retains rolling 90-day market history; DuckDB queries it. SQLite stores small runtime state and opportunity logs. Batch writes avoid one Parquet file per sample.

## Monitor model
Each monitor declares a name and cadence and evaluates read-only `RadarState`. Candidate lifecycle is monitor-private. Application only receives `AlertRequest` objects.

## Extensibility
A monitor using existing data should mainly add `monitors/<name>/` plus one registry entry. A monitor needing genuinely new data may add a new collector/model/dataset, without redesigning application lifecycle.
