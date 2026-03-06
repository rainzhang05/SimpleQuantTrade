# Execution Plan: NDAX + Binance Unified ML 15m Upgrade

This is the step-by-step delivery plan from current runtime to fully usable production-grade ML runtime.

Primary authority:
- `docs/ROADMAP.md`

## 0) Phase Status Board

- Phase 0 (docs/contract freeze): `implemented`
- Phase 1 (NDAX hardening): `implemented`
- Phase 2 (Binance raw pipeline): `implemented`
- Phase 3 (combined builder): `implemented`
- Phase 4 (calibration research + weights): `implemented`
- Phase 5 (training integration): `pending`
- Phase 6 (walk-forward trainer/evaluator): `pending`
- Phase 7 (promotion + bundle publishing): `pending`
- Phase 8 (live ML inference integration): `pending`
- Phase 9 (staging/cutover ML finalization): `pending`

Notes:
- “implemented” means command + persistence + tests exist in repository.
- “pending” means contract is fixed and implementation remains.

## 0.1) Current Checkpoint and Immediate Next Phase

Current official checkpoint:
- Data stack phases 1-4 are complete and operational.
- Next official implementation phase is **Phase 5 (Training Dataset Integration)**.
- Model-training, promotion, and ML runtime cutover remain pending (phases 5-9).

Phase 5 entry gate (must be true before coding starts):
1. `data-status --dataset combined` shows coverage contract pass for the intended training window.
2. `data-weight-status --timeframe 15m` shows recent monthly weights for train symbols.
3. `data-build-combined` and `data-calibrate-weights` are reproducible over repeated runs.

Phase 5 implementation objective:
- Make `combined` the default supervised training dataset with deterministic per-row weighting.

## 0.2) Step-by-Step Runway: Now -> Final Production ML

1. Complete Phase 5 (weighted training dataset integration).
2. Complete Phase 6 (walk-forward trainer + evaluator + metrics persistence).
3. Complete Phase 7 (promotion gates + bundle artifact writer + atomic active pointer).
4. Complete Phase 8 (live runtime inference on bar close with deterministic blend + observe-only fallback).
5. Complete Phase 9 (staging/cutover finalization, runbook evidence bundle, rollback drill).
6. Enable ML live order path only after all phase gates pass and operator checklist evidence is archived.

Required evidence package for final production readiness:
1. Snapshot reproducibility logs and dataset hashes.
2. Fold-level training/evaluation metrics for minimum fold/trade gates.
3. Promotion decision record with gate pass/fail details.
4. Active bundle integrity verification output.
5. Staging + cutover checklist reports from the same code/config revision.

## 1) Phase 0: Docs and Contract Freeze

### Deliverables
- Canonical roadmap rewritten for dual-source data architecture.
- CLI/config/storage/DB contracts frozen.
- Legacy system moved to explicit archive reference.

### Deterministic constraints
- All public interfaces documented exactly once and kept consistent across docs.

### Acceptance gate
- `ROADMAP.md`, `PLAN.md`, `README.md`, `PRODUCTION_RUNBOOK.md`, `AGENTS.md`, legacy archive are synchronized.

## 2) Phase 1: NDAX Ingestion Hardening

### Deliverables
- 15m NDAX ingestion with boundary-safe chunk overlap.
- Resume by missing-window detection.
- Idempotent reruns and deterministic dedupe.
- Checkpoint persistence in `data_sync_checkpoints`.

### Module boundaries
- `src/qtbot/data.py` NDAX retrieval + parquet merge.
- `src/qtbot/state.py` checkpoint + coverage writes.

### Tests
- chunk boundary correctness
- idempotent rerun
- gap detection

### Acceptance gate
- repeated `data-backfill` over same NDAX window creates no duplicates and no regressions.

## 3) Phase 2: Binance Raw Pipeline

### Deliverables
- Binance client and 15m USDT spot kline ingestion.
- Deterministic pagination with overlap page for restart safety.
- Checkpoint integration and idempotent parquet writes.

### Module boundaries
- `src/qtbot/binance_client.py`
- `src/qtbot/data.py` Binance backfill path

### Tests
- pagination determinism
- restart-safe behavior
- dedupe/idempotency

### Acceptance gate
- `data-backfill --sources ndax,binance` succeeds deterministically on fixture windows.

## 4) Phase 3: Combined Dataset Builder

### Deliverables
- `data-build-combined` command.
- Binance->CAD normalization bridge using NDAX overlap and `USDTCAD` when available.
- Deterministic precedence merge: NDAX first, synthetic fallback.
- Combined build hash and build audit records.

### Module boundaries
- `src/qtbot/data.py` conversion + merge + coverage
- `src/qtbot/state.py` `combined_builds`, `data_coverage_v2`

### Tests
- deterministic build hash
- precedence correctness
- zero-gap target behavior on fixture ranges

### Acceptance gate
- combined parquet output reproducible for same input window.

## 5) Phase 4: Calibration Research and Synthetic Weights

### Deliverables
- `data-calibrate-weights` and `data-weight-status` commands.
- Overlap error metrics (`median_ape_close`, return error/correlation, direction, basis stability).
- Monthly per-symbol weight generation with guardrails and shrinkage.
- Research report output under `runtime/research/bridge_weighting/<RUN_ID>/metrics.json`.

### Module boundaries
- `src/qtbot/data.py` overlap metrics + quality score + weight fusion
- `src/qtbot/state.py` `conversion_quality`, `synthetic_weights`

### Tests
- quality score and weight math determinism
- guardrail fallback path
- report generation

### Acceptance gate
- same fixed input window yields identical monthly `w_final` values.

## 6) Phase 5: Training Dataset Integration (Official Training Data Phase)

This is the official phase where model-training input changes from NDAX-only to weighted combined dataset.

### Deliverables
- dataset builder consumes `combined` as default source.
- row metadata includes source (`ndax` vs synthetic).
- supervised row weights applied:
  - NDAX rows weight `1.0`
  - synthetic rows weight `w_final`
- quality-failed synthetic rows excluded from supervised labels (continuity-only).
- sealed snapshot includes source mix and effective monthly weights used for each row.

### Deterministic constraints
- snapshot hash depends on row order, source tags, and effective monthly weights.

### Tests
- weighting path correctness
- synthetic exclusion path when `quality_pass=false`
- snapshot hash stability with fixed inputs

### Acceptance gate
- training input snapshot is reproducible and includes source-mix audit fields.
- `build-snapshot` writes a manifest with deterministic hash and row-count parity checks.

## 7) Phase 6: Walk-Forward Training + Evaluation (Official Model Training Phase)

This is the official phase where model fitting and evaluation become production-grade.

### Deliverables
- walk-forward folds (default 12m train / 1m validate / 1m step)
- LightGBM global + per-coin training
- cost-aware evaluator with sensitivity runs:
  - synthetic disabled baseline
  - calibrated synthetic weights enabled
- CLI contract implemented for:
  - `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1`
  - `qtbot eval --run <RUN_ID>`

### Module boundaries
- training/eval package (new)
- SQLite tables: `training_runs`, `training_folds`, `fold_metrics`

### Tests
- fold reproducibility
- deterministic model artifacts from fixed seed/snapshot
- evaluator cost correctness
- fold-boundary determinism under repeated runs

### Acceptance gate
- end-to-end run creates deterministic fold metrics and source-mix diagnostics.
- run metadata and fold metrics are persisted to `training_runs`, `training_folds`, and `fold_metrics`.

## 8) Phase 7: Promotion Gates + Bundle Publishing

### Deliverables
- promotion gate engine with hard/soft checks.
- bundle writer with `signature.sha256` and atomic `LATEST` update.
- rollback-safe active-bundle switching.
- CLI contract implemented for:
  - `qtbot promote --run <RUN_ID>`
  - `qtbot model-status`
  - `qtbot set-active-bundle <BUNDLE_ID>`

### Hard gate minimums
- `min_folds=12`
- `min_trades=200`
- net positive after costs
- `max_drawdown<=25%`
- slippage stress remains net positive
- conversion quality pass-rate threshold
- combined coverage contract pass

### Tests
- gate pass/fail matrix
- atomic pointer update
- rollback integrity checks
- signature validation failure path

### Acceptance gate
- promotion deterministically accepts/rejects same run.
- bundle contents match roadmap contract (`manifest.json`, models, feature/threshold/cost files, signature).

## 9) Phase 8: Live ML Inference Integration

### Deliverables
- runtime loads active bundle and performs bar-close-only inference.
- deterministic blend (`0.7*coin + 0.3*global` when eligible).
- existing lifecycle/risk/reconciliation shell preserved.
- observe-only fallback if bundle/data readiness fails.
- CLI contract implemented for:
  - `qtbot predict --symbol <SYM> --at latest`

### Tests
- bar-close trigger behavior
- deterministic decision outputs
- observe-only safety behavior
- preflight bundle/data readiness blocking tests

### Acceptance gate
- runtime decisions are deterministic and safe under failure modes.
- with live disabled, decisions output includes prediction values, blend path, and gate reasons.

## 10) Phase 9: Staging/Cutover Finalization and Rollout

### Deliverables
- staging adds combined-build and calibration smoke checks.
- cutover requires fresh calibration evidence and combined coverage pass.
- runbook finalized with rollback/incident playbooks.
- end-to-end operator procedure documented for:
  - data refresh
  - snapshot/train/eval/promote
  - active bundle verification
  - live cutover and rollback

### Rollback and safe-mode gate (first-class)
- active bundle switch only when paused.
- previous bundles retained.
- failed integrity/readiness => observe-only, no order placement.

### Acceptance gate
- staging + cutover pass in CI and operator workflow.
- operator can execute the full offline-to-live ML path from docs alone without undocumented steps.

## 11) Migration Path: Legacy Runtime -> ML Runtime

1. Keep existing lifecycle and risk shell unchanged.
2. Build and validate dual-source dataset pipeline.
3. Switch training inputs to combined weighted dataset.
4. Promote first ML bundle via deterministic gates.
5. Activate ML inference path in observe-only.
6. After staging/cutover evidence, enable live order path.

## 12) CI and Workflow Expectations

CI must validate:
- unit/integration tests for data, conversion, weighting, and state schema
- offline staging/cutover checks
- docker lifecycle checks

Workflow command checks target:
- `data-backfill --sources ndax,binance` (fixture/mocked environment)
- `data-build-combined`
- `data-calibrate-weights`
- `data-status --dataset combined`

## 13) Final “Fully Usable” Definition

Upgrade is complete when:
1. Dual-source backfill is restart-safe and idempotent.
2. Combined dataset is deterministic and coverage-contract compliant.
3. Monthly calibration is reproducible with persisted quality metrics.
4. Weighted combined dataset is used in training.
5. Walk-forward training/eval/promotion are deterministic.
6. Runtime executes ML decisions safely with rollback and observe-only protections.
