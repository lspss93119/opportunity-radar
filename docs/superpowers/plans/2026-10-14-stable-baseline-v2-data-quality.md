# Stable Baseline Scanner V2 Data Quality and Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline execution is authorized by the user request) and superpowers:test-driven-development.

**Goal:** Add a read-only V2 historical research command that attributes Production data loss, compares causal baseline windows, measures independent anomaly/convergence episodes, builds an opportunity funnel, and emits evidence-based candidate classifications without changing V1 behavior.

**Architecture:** Keep `stable_baseline.py` unchanged as the V1 replay contract. Add one focused offline module, `stable_baseline_v2.py`, whose pure analysis functions consume normalized pair timelines and whose CLI uses DuckDB only to read configured Production Parquet. The CLI writes the six requested CSV/report inputs; it never imports live app services, credentials, order code, or Dashboard code.

**Tech Stack:** Python 3.13+, DuckDB, standard-library dataclasses/csv/json/statistics/zoneinfo, pytest.

**Spec:** User task “OpportunityRadar Stable Baseline Scanner V2 — Data Quality & Convergence Research”; V1 behavior and constraints in `docs/operations/stable-baseline-scanner-v1.md`.

## Global Constraints

- Production Radar/Dashboard/Bots must not be restarted or modified.
- Research reads the Production Parquet directory but runs in an independent checkout/process.
- V1 UTC, exact `sample_time`, quote-age, skew, VWAP, causal baseline, and fail-closed gap semantics remain unchanged.
- All baselines use only completed UTC dates before the evaluated sample.
- A relaxed 5s/10s skew view is sensitivity evidence only; the canonical 3s filter remains unchanged.
- Funding, real fills, latency, queue, rejects, and capital allocation are not treated as zero or as verified execution.
- No execution, wallet, key, position-management, Monitor, or Dashboard code is added.

## Review Focus

- A raw quote can be present while invalid VWAP, stale observation, or observed-time skew makes it unusable; tests pin non-overlapping attribution labels and counts.
- A 60s sampled gap can hide a valid 10s observation; tests distinguish true feed loss from sampling loss.
- A baseline at the current sample must not include the current day or future daily median; tests pin causal 24h/7d/long-window references.
- A sustained anomaly must become one episode, not one event per minute; tests pin re-arm and data-gap censoring.
- A convergence after a data gap or at dataset end is unknown/censored, not success or failure; tests pin status and aggregate denominators.

---

### Task 1: Normalized timeline and data-quality attribution

**Files:**
- Create: `src/radar/history/stable_baseline_v2.py`
- Test: `tests/test_stable_baseline_v2.py`

**Interfaces:**
- Produces `MarketObservation`, `PairPoint`, `AnalysisParams`, `_classify_observation()`, and `_attribute_pair_slot()` for later tasks.
- CLI loader consumes configured market identities and emits canonical 60s/10s slot records from DuckDB.

- [ ] Write failing tests for invalid VWAP, stale/after-sample, skew, missing leg, exact sample-time mismatch, and 10s-valid/60s-missing sampling loss.
- [ ] Run the focused tests and observe the expected failures.
- [ ] Implement the minimal dataclasses, slot classifiers, and DuckDB-backed normalized timeline iterator.
- [ ] Run focused tests and verify they pass.
- [ ] Commit as `feat(research): add V2 timeline quality attribution`.

### Task 2: Causal baseline windows and regime statistics

**Files:**
- Modify: `src/radar/history/stable_baseline_v2.py`
- Test: `tests/test_stable_baseline_v2.py`

**Interfaces:**
- Consumes `PairPoint` streams from Task 1.
- Produces `DailySeries`, `BaselineWindow`, and `build_baseline_rows()` for 24h, 7d, and <=30d windows, including mid and executable directional basis and US-session regime labels.

- [ ] Write failing tests proving current-day exclusion, stable Q90−Q10 calculation, drift calculation, and insufficient regime coverage.
- [ ] Run the focused tests and observe the expected failures.
- [ ] Implement causal daily median/quantile windows and regime aggregation without look-ahead.
- [ ] Run focused tests and verify they pass.
- [ ] Commit as `feat(research): add causal baseline stability analysis`.

### Task 3: Independent anomaly and convergence episodes

**Files:**
- Modify: `src/radar/history/stable_baseline_v2.py`
- Test: `tests/test_stable_baseline_v2.py`

**Interfaces:**
- Consumes causal 7d baseline references and `PairPoint` streams.
- Produces `AnomalyEpisode`, `detect_episodes()`, and `summarize_episodes()` for 10/15/20/25 bps, both directions, mid and executable basis.

- [ ] Write failing tests for one sustained episode, two re-armed episodes, 15m/1h/4h convergence, data-gap censoring, end-of-data censoring, and maximum adverse deviation.
- [ ] Run the focused tests and observe the expected failures.
- [ ] Implement episode state tracking with no per-minute double counting and explicit known/unknown convergence denominators.
- [ ] Run focused tests and verify they pass.
- [ ] Commit as `feat(research): add anomaly convergence episode analysis`.

### Task 4: Funnel, cost sensitivity, CSV outputs, and CLI

**Files:**
- Modify: `src/radar/history/stable_baseline_v2.py`
- Modify: `tests/test_stable_baseline_v2.py`
- Create: `docs/operations/stable-baseline-scanner-v2.md`

**Interfaces:**
- Consumes quality, baseline, episode, and configured fee results.
- Produces `write_v2_outputs()` for `data_quality_report.csv`, `baseline_stability.csv`, `anomaly_convergence.csv`, `opportunity_funnel.csv`, `candidate_ranking.csv`, and a JSON-ready report context.

- [ ] Write failing tests for funnel stage counts, A/B/C/D classification, configured fee plus 0/2 bps and $1k/$5k cost calculations, and deterministic CSV headers.
- [ ] Run the focused tests and observe the expected failures.
- [ ] Implement the CLI, output writers, conservative classification, 3/5/10s skew sensitivity, and V1 data-gap event audit input.
- [ ] Run focused tests and verify they pass.
- [ ] Commit as `feat(research): ship Stable Baseline Scanner V2 outputs`.

### Task 5: Production validation and research report

**Files:**
- Create: `research_report_zh-TW.md` in the dated output directory outside the Production checkout.

- [ ] Run the requested focused test, full non-Live suite, and static checks in the isolated checkout.
- [ ] Run V2 on the full Production Parquet for canonical 3s plus 3/5/10s sensitivity, without changing the live checkout.
- [ ] Audit all seven V1 `data_gap` events with raw 10s observations and record unknown/censored outcomes.
- [ ] Verify all six output CSVs, report metrics, hashes, and production process/status invariants.
- [ ] Push only the research code/docs commits to `feature/radar-v1` and record the final SHA.
