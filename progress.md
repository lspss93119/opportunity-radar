# SDD ledger — plan: docs/superpowers/plans/2026-10-14-stable-baseline-v2-data-quality.md

Pre-flight: existing isolated clone at `/private/tmp/opportunity-radar-validation.Yugano` is on `feature/radar-v1` at `482ffed`; V1 code and Production checkout are separate.

Ruling: V2's valid timeline now reuses V1 `csv_or_parquet_quotes` and `_sample_pairs`, preserving the canonical latest-duplicate, quote-age, and 3-second observed-at skew semantics while avoiding a second full-history pair self-join and global sort. Quality attribution remains SQL-based, so missing-leg, invalid-VWAP, stale, and skew losses are still measured separately. Cost if incorrect: timeline and quality counts could diverge; mitigation: focused duplicate/latest and quality regression tests pass locally and on the isolated `trading-mini` checkout.

Ruling: Data-quality pair aggregation now joins the exact configured-pair table rather than every same-symbol venue combination. This preserves the full configured-pair funnel while avoiding the 10-second all-combination join that exhausted the isolated run's useful runtime. Cost if incorrect: a configured pair could be omitted from attribution; mitigation: the configured-pair table is generated from the same `configured_pairs()` function used for the funnel and the quality fixture regression remains green.

Ruling: The full-history DuckDB sort was replaced by a per-UTC-partition latest-row map for both the V2 valid timeline and quality grids. This preserves duplicate selection and exact sample-time grouping while bounding research memory to one partition; a one-day Production smoke read returned 177,814 pair points in 1.23 seconds with 370 MB peak RSS.

Ruling: The first successful output exposed that total 10-second valid slots cannot be subtracted from 60-second valid slots because the grids have a 6x observation count. Sampling-loss attribution is corrected to compare only 10-second observations whose timestamps lie on the same 60-second grid; the full scan will be rerun so the delivered CSV does not overstate minute-sampling loss.

