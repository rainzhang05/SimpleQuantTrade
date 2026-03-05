# Implementation Plan: SimpleQuantTrade ML 15m Upgrade

This plan operationalizes `docs/ROADMAP.md` into execution phases.

Plan objective:
- Replace fixed-rule 1m strategy path with ML-driven 15m pipeline while preserving operational safety shell.
- Deliver deterministic, laptop-grade production workflow from data ingestion to live ML inference.

## 0) Delivery Principles

- Prioritize correctness and safety over speed.
- Keep deterministic behavior across training and runtime.
- Preserve control-plane, reconciliation, preflight, and risk-halting behavior.
- Do not break Docker and staging/cutover operator workflows.
- Every phase must ship with tests and CI updates for changed behavior.

## 1) Phase 0: Docs and Contract Freeze

### Goals
- Freeze target interfaces before major code migration.
- Archive legacy fixed-rule behavior.

### Deliverables
- Rewrite roadmap, plan, README, runbook, and agent instructions.
- Add legacy archive document.
- Freeze Universe V1, thresholds, gate profile, and defaults.

### Interface/contracts locked
- CLI v2 target command surface.
- Bundle format and active-pointer contract.
- Storage layout and snapshot hash contract.

### Tests and CI
- Documentation lint/check step if available.
- Manual consistency check across all docs for command/config parity.

### Acceptance
- All docs reflect ML 15m as primary architecture.
- Legacy fixed-rule content clearly marked archival.

## 2) Phase 1: Data Layer (Milestone A)

### Goals
- Add deterministic 15m market data ingestion and coverage reporting.

### Deliverables
- `qtbot data-backfill` command.
- `qtbot data-status` command.
- Parquet writer for `data/raw/ndax/15m/<SYMBOL>.parquet`.
- Gap and duplicate detection for candles.

### Module boundaries
- New data service module for chunked NDAX fetch/retry/backoff.
- Data-quality validator module.
- CLI handlers for data commands.

### Schema/interface changes
- Add `data_coverage` table in `runtime/state.sqlite`.
- Define candle schema contract (timestamp/open/high/low/close/volume).

### Tests
- Unit tests for dedupe, timestamp normalization, and chunk merge.
- Integration tests for backfill idempotence and coverage reporting.

### CI updates
- Add offline data-layer tests.
- Keep existing full test suite and coverage gate green.

### Acceptance
- Deterministic data files and per-symbol status.
- Gap detection produces stable results.

## 3) Phase 2: Feature Pipeline (Milestone B)

### Goals
- Build deterministic feature generation for 50 to 80 features.

### Deliverables
- Feature-engineering module with explicit windows/transforms.
- Feature-spec emitter (`feature_spec.json`).
- Warmup handling and null-policy implementation.

### Module boundaries
- `features/` package for transforms and feature registry.
- Shared deterministic math utilities.

### Schema/interface changes
- Feature schema version and hash generation.
- Feature column ordering contract.

### Tests
- Unit tests per feature family.
- Determinism regression tests (same input -> identical output).
- Warmup/NaN behavior tests.

### CI updates
- Add feature determinism test job.

### Acceptance
- Same snapshot yields byte-identical feature output.
- Warmup rows handled per spec without leaking future info.

## 4) Phase 3: Snapshot Builder (Milestone C)

### Goals
- Produce sealed, reproducible datasets for training.

### Deliverables
- `qtbot build-snapshot` command.
- Snapshot manifest writer.
- Dataset hash computation over ordered rows.

### Module boundaries
- Snapshot orchestrator module.
- Manifest and checksum utility module.

### Schema/interface changes
- Snapshot ID format and manifest schema.
- Snapshot directory contract under `data/snapshots/<SNAPSHOT_ID>/`.

### Tests
- Rebuild-equivalence test for identical as-of.
- Manifest checksum validation tests.

### CI updates
- Add snapshot reproducibility test case.

### Acceptance
- Same source data and as-of produce identical `dataset_hash`.

## 5) Phase 4: Walk-Forward Orchestration (Milestone D)

### Goals
- Implement deterministic fold generation and run tracking.

### Deliverables
- Fold generator with default 12m train, 1m validate, 1m step.
- Run lifecycle tracking.
- CLI `train` scaffolding for fold execution.

### Module boundaries
- `training/orchestrator.py`
- `training/folds.py`

### Schema/interface changes
- Add tables:
- `training_runs`
- `training_folds`
- `fold_metrics`

### Tests
- Fold-boundary determinism tests.
- Train/val range integrity tests.
- Run-state transition tests.

### CI updates
- Include orchestrator tests in suite.

### Acceptance
- Fold schedule reproducible from same inputs.
- Fold metadata persisted and auditable.

## 6) Phase 5: LightGBM Trainers (Milestone E)

### Goals
- Train global and per-coin models deterministically.

### Deliverables
- LightGBM trainer module for global model.
- LightGBM trainer module for per-coin models.
- Persist per-fold artifacts and run summary metadata.

### Module boundaries
- `training/lightgbm_trainer.py`
- `training/model_registry.py`

### Schema/interface changes
- Extend run metadata with parameter hash and seed.

### Tests
- Trainer smoke tests on small deterministic fixture.
- Deterministic reproducibility test with fixed seed.
- Missing-per-coin-history fallback tests.

### CI updates
- Add optional LightGBM dependency in test matrix.

### Acceptance
- End-to-end training run completes and artifacts are reproducible.

## 7) Phase 6: Cost-Aware Evaluator (Milestone F)

### Goals
- Simulate live-constrained execution for promotion decisions.

### Deliverables
- `qtbot eval --run <RUN_ID>` command.
- Cost-aware simulator with fees/slippage/liquidity gates.
- Fold and aggregate metrics report.

### Module boundaries
- `evaluation/simulator.py`
- `evaluation/metrics.py`
- `evaluation/cost_model.py`

### Schema/interface changes
- Persist fold metrics JSON and aggregate run metrics.

### Tests
- Cost-model tests for fee/slippage correctness.
- Constraint-alignment tests with live rules.
- Stress test path (+50% slippage) verification.

### CI updates
- Add evaluator regression fixtures.

### Acceptance
- Evaluator outputs stable, deterministic metrics including risk and robustness dimensions.

## 8) Phase 7: Promotion and Bundle Publisher (Milestone G)

### Goals
- Gate model deployment and publish signed bundles.

### Deliverables
- `qtbot promote --run <RUN_ID>` command.
- Hard/soft gate engine using conservative defaults.
- Bundle writer and `LATEST` atomic pointer update.
- `qtbot model-status` command.

### Module boundaries
- `promotion/gates.py`
- `promotion/bundle_writer.py`
- `promotion/model_status.py`

### Schema/interface changes
- Add `promotions` table.
- Persist promotion metadata and bundle hash.

### Tests
- Gate pass/fail scenario coverage.
- Bundle integrity and signature verification tests.
- Atomic pointer swap and rollback safety tests.

### CI updates
- Add promotion gate test set.

### Acceptance
- Promotion fails with explicit reasons when gates are not met.
- Promotion success writes complete bundle and updates active pointer atomically.

## 9) Phase 8: Live Inference Integration (Milestone H)

### Goals
- Integrate ML predictions into 15m live decision pipeline.

### Deliverables
- Runtime bundle loader and integrity validator.
- 15m bar-close scheduler.
- Deterministic blend combiner (`0.7/0.3`, fallback global-only).
- `qtbot predict --symbol <SYM> --at latest` debug command.

### Module boundaries
- `inference/bundle_loader.py`
- `inference/combiner.py`
- `inference/predictor.py`
- runner integration layer

### Schema/interface changes
- Extend decision logs with prediction fields and gating reasons.
- Maintain compatibility with existing runtime tables and lifecycle controls.

### Tests
- Inference determinism tests.
- Observe-only startup when bundle invalid.
- Entry/exit threshold behavior tests.
- Integration tests for bar-close-only execution.

### CI updates
- Add runtime inference tests in dry-run/observe-only mode.

### Acceptance
- Runtime decisions occur only on closed 15m bars.
- Invalid bundle blocks order path and keeps system in observe-only.

## 10) Phase 9: Staging/Cutover Upgrade and Migration Closure (Milestone I)

### Goals
- Upgrade operational validation and complete migration from fixed-rule path.

### Deliverables
- Extend `qtbot staging-validate` to test data->features->model->decision path.
- Extend `qtbot cutover-checklist` for model readiness gates.
- Add `qtbot set-active-bundle <BUNDLE_ID>` rollback command.
- Ensure runbook and docs match final shipped behavior.

### Module boundaries
- staging and cutover modules enhanced with ML checks.
- rollback and active-bundle management module.

### Schema/interface changes
- Promotion and active-bundle events persisted for auditability.

### Tests
- End-to-end staging validation test with mocked model bundle.
- Cutover checklist tests for model integrity/data warmup failures.
- Rollback command tests requiring paused state.

### CI updates
- Add offline ML staging/cutover command checks.
- Preserve existing docker validation job.

### Acceptance
- Staging and cutover report ML readiness reliably.
- Rollback path is deterministic and operator-safe.

## 11) Migration Path: Fixed-Rule to ML Runtime

### Stepwise migration policy
1. Keep existing runtime shell and risk systems intact.
2. Introduce new data/training/eval stack offline first.
3. Enable ML inference in observe-only mode.
4. Promote first bundle only after gates pass.
5. Enable live execution with ML decisions after staging and cutover pass.
6. Deactivate fixed-rule strategy code path once rollback command and observe-only safeguards are validated.

### Compatibility constraints
- `qtbot` command identity remains unchanged.
- Existing lifecycle operations remain stable.
- Existing risk controls remain active across migration.

## 12) Test Strategy (Cross-Phase)

Mandatory test coverage themes:
- deterministic transforms and model outputs
- data-quality and leakage prevention
- reconciliation and safety gate behavior
- promotion and rollback correctness
- control-plane and graceful shutdown under new ML path

Minimum acceptance for merge:
- all tests pass
- coverage gate maintained or improved
- CI green for both standard and docker jobs

## 13) Completion Criteria

Program considered complete when:
1. 15m data and coverage tooling are production-ready.
2. Snapshot and feature pipelines are reproducible.
3. Walk-forward LightGBM training/evaluation runs deterministically.
4. Promotion gates and bundle contracts are enforced.
5. Live runtime uses promoted bundle on bar-close with deterministic decisions.
6. Observe-only fallback, rollback, staging, and cutover checks are all operational.
7. Docs and runbook exactly match shipped interfaces and behavior.
