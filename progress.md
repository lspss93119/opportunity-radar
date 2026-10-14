# SDD ledger — plan: docs/superpowers/plans/2026-10-14-stable-baseline-v2-data-quality.md

Pre-flight: existing isolated clone at `/private/tmp/opportunity-radar-validation.Yugano` is on `feature/radar-v1` at `482ffed`; V1 code and Production checkout are separate.

Ruling: V2's valid timeline now reuses V1 `csv_or_parquet_quotes` and `_sample_pairs`, preserving the canonical latest-duplicate, quote-age, and 3-second observed-at skew semantics while avoiding a second full-history pair self-join and global sort. Quality attribution remains SQL-based, so missing-leg, invalid-VWAP, stale, and skew losses are still measured separately. Cost if incorrect: timeline and quality counts could diverge; mitigation: focused duplicate/latest and quality regression tests pass locally and on the isolated `trading-mini` checkout.

