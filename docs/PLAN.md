# Step-by-Step Upgrade Plan: Current Runtime -> Production-Grade ML (15m)

This is the implementation playbook from the current shipped version to a fully usable production-grade ML system.

Primary design authority:
- `docs/ROADMAP.md`

This document answers:
- what exists now
- what to build next, in exact order
- how to verify each step
- what “fully usable” means in production

## 1) Current Baseline (Starting Point)

Current implemented baseline in repository:
- NDAX spot-only CLI bot with lifecycle control plane.
- Existing commands:
- `start`, `pause`, `resume`, `stop`, `status`
- `ndax-pairs`, `ndax-candles`, `ndax-balances`, `ndax-check`
- `staging-validate`, `cutover-checklist`
- Startup reconciliation and go-live preflight.
- Risk controls: daily loss cap, slippage guard, consecutive-error kill-switch.
- Persistent runtime state/logging and Docker workflow.
- Strategy path is still fixed-rule (legacy) in implementation.

Target to reach:
- ML-driven 15m bar-close trading pipeline with deterministic training/eval/promotion/deployment.

## 2) End-State Definition (What We Are Building)

System is fully usable only when all are true:
1. 15m data can be backfilled for Universe V1 and audited for coverage/gaps.
2. Sealed snapshot hashes are reproducible.
3. Walk-forward training/evaluation is deterministic and persisted.
4. Promotion gates deterministically approve/reject model runs.
5. Runtime loads promoted bundle and decides only on closed 15m bars.
6. Observe-only fallback works when model integrity/preflight fails.
7. Staging/cutover and rollback workflows are operational.

## 3) Delivery Rules (Apply to Every Step)

- Do not remove existing safety shell while migrating.
- Keep old strategy path intact until ML path is validated and rollback exists.
- Every step must ship with tests and CI updates.
- No step is complete without deterministic reproducibility checks.
- No live activation without staging + cutover green.

## 4) Step-by-Step Execution Guide

## Step 0: Freeze Baseline and Introduce Compatibility Layer

### Goal
Create a safe migration base so ML interfaces can be added without breaking current runtime.

### Implementation
- Add new config keys from roadmap (with defaults) while preserving existing keys:
- `QTBOT_TIMEFRAME=15m`
- `QTBOT_UNIVERSE=V1`
- `QTBOT_BTC_ETH_LOCK=true`
- `QTBOT_MODEL_BUNDLE_PATH=models/bundles/LATEST`
- `QTBOT_ENTRY_THRESHOLD=0.60`
- `QTBOT_EXIT_THRESHOLD=0.48`
- `QTBOT_SLIPPAGE_MODEL=fixed_bps`
- `QTBOT_FEE_PCT_PER_SIDE=0.002`
- `QTBOT_FEATURE_SPEC_VERSION=v1`
- Keep lifecycle and existing commands unchanged.

### Verification
- Existing CLI smoke tests pass unchanged.
- Existing staging/cutover offline commands still pass.

### Exit criteria
- Runtime can load new config keys without altering current behavior.

## Step 1: Build 15m Data Backfill and Coverage Status

### Goal
Implement deterministic 15m data ingestion as foundation for ML.

### Implementation
- Add `qtbot data-backfill --from --to --timeframe 15m`.
- Add `qtbot data-status`.
- Store canonical candles at:
- `data/raw/ndax/15m/<SYMBOL>.parquet`
- Add dedupe + normalized 15m timestamp rounding.
- Add gap/duplicate reporting and data coverage summary.
- Populate `data_coverage` table in SQLite.

### Tests
- Unit: dedupe, timestamp boundary normalization, gap detection.
- Integration: repeated backfill is idempotent.

### CI updates
- Add data-layer test job.

### Exit criteria
- Full universe backfill produces deterministic files and stable coverage report.

## Step 2: Enforce Data-Quality Gates

### Goal
Prevent bad data from entering training or live inference.

### Implementation
- Add hard gates for:
- monotonic timestamps
- duplicate key rejection
- gap threshold checks
- warmup sufficiency checks
- UTC consistency checks
- Wire gates into training entrypoints and live preflight.

### Tests
- Fail-case fixtures for each gate.
- Preflight blocking tests.

### Exit criteria
- Training and live preflight both block on hard data-gate failures.

## Step 3: Implement Deterministic Feature Pipeline

### Goal
Generate 50 to 80 high-ROI deterministic features.

### Implementation
- Add feature module with fixed windows/transforms.
- Include families:
- returns/momentum
- volatility/range
- trend strength
- mean-reversion
- volume dynamics
- market context
- Emit `feature_spec.json` with feature order and transforms.
- Enforce no leakage and deterministic missing-value policy.

### Tests
- Unit tests per feature family.
- Determinism regression test (same input -> same output).
- Warmup and no-steady-state-NaN checks.

### Exit criteria
- Feature output is reproducible and contract-defined.

## Step 4: Build Sealed Snapshot System

### Goal
Create reproducible dataset artifacts for training.

### Implementation
- Add `qtbot build-snapshot --asof <ISO8601>`.
- Create snapshot folder:
- `data/snapshots/<SNAPSHOT_ID>/`
- Manifest includes checksums and dataset hash.
- Snapshot ID encodes as-of, universe hash, schema version.

### Tests
- Re-run same snapshot build and assert identical hash.
- Manifest integrity verification tests.

### Exit criteria
- Snapshot reproducibility contract passes consistently.

## Step 5: Add Walk-Forward Orchestrator and Training DB Tables

### Goal
Implement deterministic fold schedule and run tracking.

### Implementation
- Add default walk-forward schedule:
- train 12 months, validate 1 month, step 1 month
- Add tables:
- `training_runs`
- `training_folds`
- `fold_metrics`
- Add `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1` scaffold.

### Tests
- Fold boundary determinism.
- Run-state transitions and table writes.

### Exit criteria
- Fold generation and tracking are deterministic and auditable.

## Step 6: Implement LightGBM Trainers (Global + Per-Coin)

### Goal
Train model artifacts deterministically from snapshot/folds.

### Implementation
- Train global pooled LightGBM model.
- Train per-coin LightGBM models when enough symbol history exists.
- Store params/seed/hash metadata in run records.
- Persist fold artifacts and summaries.

### Tests
- Deterministic training with fixed seed.
- Per-coin missing-data fallback tests.

### Exit criteria
- Training runs complete end-to-end with reproducible artifacts.

## Step 7: Build Cost-Aware Evaluator

### Goal
Evaluate models using live-aligned execution constraints.

### Implementation
- Add `qtbot eval --run <RUN_ID>`.
- Simulate constraints:
- market orders only
- cooldown and max entries per cycle
- CAD and exchange-availability caps
- risk halt behavior
- Default fee baseline:
- `QTBOT_FEE_PCT_PER_SIDE=0.002`
- Add slippage model option (`fixed_bps` default).
- Produce fold and aggregate metrics including stress test (+50% slippage).

### Tests
- Fee/slippage correctness tests.
- Stress-test metric consistency tests.

### Exit criteria
- Deterministic metrics produced for every fold and aggregate run.

## Step 8: Implement Promotion Gates and Bundle Publisher

### Goal
Promote only safe/stable runs and publish signed deployment artifact.

### Implementation
- Add `qtbot promote --run <RUN_ID>` with gates:
- `min_folds >= 12`
- `trade_count >= 200`
- `max_drawdown <= 25%`
- net PnL after costs positive
- slippage stress remains net positive
- Emit bundle at `models/bundles/<BUNDLE_ID>/` containing:
- `manifest.json`
- `global_model.txt`
- `per_coin/*.txt`
- `feature_spec.json`
- `thresholds.json`
- `cost_model.json`
- `signature.sha256`
- Update `models/bundles/LATEST` atomically.
- Persist `promotions` table entry.

### Tests
- Gate pass/fail matrix tests.
- Bundle integrity/signature tests.
- Atomic pointer update tests.

### Exit criteria
- Promotions are deterministic and auditable.

## Step 9: Add Runtime Model Loading and Predict Debug Command

### Goal
Load active bundle in runtime and expose deterministic prediction debugging.

### Implementation
- Add bundle loader with integrity checks.
- Add `qtbot model-status`.
- Add `qtbot predict --symbol <SYM> --at latest`.
- Extend preflight with ML checks:
- bundle integrity
- feature-spec compatibility
- warmup coverage
- bar alignment/clock sanity
- Failure behavior default:
- enter observe-only mode, block order placement.

### Tests
- Invalid bundle -> observe-only startup test.
- Model-status/predict command tests.

### Exit criteria
- Runtime can inspect and safely load active bundle.

## Step 10: Integrate 15m Bar-Close Inference and Blend Logic

### Goal
Replace legacy decision engine with ML inference path (while preserving safety shell).

### Implementation
- Decision loop remains 60s.
- Predictions and decisions only on new closed 15m bars.
- Implement deterministic blend:
- if per-coin coverage sufficient: `p = 0.7*p_coin + 0.3*p_global`
- else `p = p_global`
- Default thresholds:
- entry `0.60`
- exit `0.48`
- Respect BTC/ETH lock default and liquidity/cooldown/risk gates.

### Tests
- Bar-close-only decision trigger tests.
- Blend determinism tests.
- Threshold transition tests (`ENTER`, `EXIT`, `HOLD`).

### Exit criteria
- Runtime decisions are deterministic and 15m-aligned.

## Step 11: Add Rollback Command and Finalize Live Activation

### Goal
Provide operator-safe rollback and controlled live enablement.

### Implementation
- Add `qtbot set-active-bundle <BUNDLE_ID>`.
- Enforce paused-state requirement before pointer change.
- Keep previous bundles; never delete on promotion.
- Log rollback and promotion events.
- Maintain market-order only and full risk guard enforcement.

### Tests
- Rollback blocked unless paused.
- Post-rollback model-status correctness.

### Exit criteria
- Operators can switch active bundle safely and deterministically.

## Step 12: Upgrade Staging and Cutover to ML Readiness

### Goal
Guarantee end-to-end validation before production ML launch.

### Implementation
- Extend `qtbot staging-validate` to include:
- sample backfill check
- feature generation check
- bundle load + inference check
- simulated decision cycles
- Extend `qtbot cutover-checklist` ML checks.
- Update runbook and docs with final commands/procedures.

### Tests
- Offline staging/cutover ML-mode tests in CI.
- Full end-to-end dry-run test path.

### Exit criteria
- Staging and cutover report green for ML path.

## 5) Final Production Go-Live Sequence (Operator)

When Steps 0-12 are complete, use this sequence:

1. Backfill and verify data coverage.
2. Build snapshot for latest closed bar.
3. Train and evaluate walk-forward run.
4. Promote only if gates pass.
5. Verify active bundle via `model-status`.
6. Run staging validation (ML path).
7. Run cutover checklist.
8. Start bot with constrained live budget.
9. Monitor first cycles and confirm logs/risk behavior.
10. Keep rollback command ready with prior bundle IDs.

## 6) Required CI End-State

CI must pass all of these on push/PR:
- compile + unit/integration tests + coverage gate
- offline staging validation (ML-aware)
- offline cutover checklist (ML-aware)
- docker build + container CLI checks
- migration/regression tests for deterministic snapshot/training/inference outputs

## 7) “Fully Usable” Acceptance Checklist

All must be true:
- [ ] 15m data backfill/status commands are stable and deterministic.
- [ ] Snapshot hashes reproduce for same as-of/source.
- [ ] Walk-forward runs persist deterministic fold metrics.
- [ ] Promotion gates enforce conservative profile exactly.
- [ ] Runtime uses promoted bundle with 15m bar-close decisions.
- [ ] Observe-only mode triggers correctly on ML readiness failures.
- [ ] Rollback command is safe and tested.
- [ ] Staging and cutover ML checks pass in CI and locally.
- [ ] Runbook procedures match actual command behavior.

Once this checklist is fully green, the system is considered production-grade ML and fully usable.
