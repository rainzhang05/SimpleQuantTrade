# Production Runbook: ML 15m Multi-Source Operations

This runbook covers production-safe operation for the NDAX execution runtime with NDAX + Kraken primary + Binance fallback training data pipeline.

Canonical design source:
- `docs/ROADMAP.md`

Current rollout state:
- Data foundation (multi-source + combined + calibration) is active.
- Weighted training snapshot integration (Phase 5) is active.
- Walk-forward training/evaluation (Phase 6) is active.
- Promotion gates + signed bundle publishing (Phase 7) are active.
- Live ML runtime path must still follow `docs/PLAN.md` phases 8-9 in order.

## 1) Pre-Launch Readiness

Required before launch:
1. Fresh staging validation report.
2. Passing cutover checklist.
3. Data coverage confirmed (especially `combined`).
4. Latest calibration report exists and weights are populated.
5. NDAX credentials and runtime paths are valid.

Key artifacts:
- `runtime/state.sqlite`
- `runtime/control.json`
- `runtime/logs/qtbot.log`
- `runtime/logs/decisions.csv`
- `runtime/logs/trades.csv`
- `runtime/logs/data_coverage_*.json`
- `runtime/research/bridge_weighting/<RUN_ID>/metrics.json`
- `runtime/research/training/<RUN_ID>/manifest.json`
- `runtime/research/training/<RUN_ID>/feature_spec.json`
- `runtime/research/training/<RUN_ID>/folds.json`
- `runtime/research/training/<RUN_ID>/metrics.json`
- `runtime/research/training/<RUN_ID>/coin_attribution.json`
- `runtime/research/training/<RUN_ID>/coin_attribution.md`
- `data/snapshots/<SNAPSHOT_ID>/manifest.json`
- `data/snapshots/<SNAPSHOT_ID>/rows.parquet`
- `models/bundles/<BUNDLE_ID>/manifest.json`
- `models/bundles/LATEST`

Local artifact rule:
- `data/` is not version-controlled.
- Every machine must run the data pipeline locally before training/readiness checks.

## 2) ML/Data Readiness Commands

Run in order:
```bash
PYTHONPATH=src python3 -m qtbot data-status --timeframe 15m --dataset all
PYTHONPATH=src python3 -m qtbot data-weight-status --timeframe 15m
PYTHONPATH=src python3 -m qtbot build-snapshot --asof <ISO_TIME> --timeframe 15m
PYTHONPATH=src python3 -m qtbot train --snapshot <SNAPSHOT_ID> --folds 12 --universe V1
PYTHONPATH=src python3 -m qtbot eval --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot attribution --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot promote --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot model-status
PYTHONPATH=src python3 -m qtbot staging-validate --offline-only --budget 1000 --cadence-seconds 1 --min-loops 1 --timeout-seconds 30
PYTHONPATH=src python3 -m qtbot cutover-checklist --offline-only --budget 250 --staging-max-age-hours 168
```

Expected outcomes:
- `combined` coverage meets configured contract.
- at least one recent calibration report exists.
- snapshot manifest reports `parity_check_passed=true`.
- cutover reports `passed=true`.
- official Kraken/Binance outage windows, if any, have been sealed deterministically during backfill.

## 2.1) Required Runway Before ML Live Activation

This sequence is mandatory before enabling ML live order path:
1. Complete Phase 8: runtime inference in observe-only mode with deterministic outputs.
2. Complete Phase 9: staging/cutover reports and rollback drill evidence.

Evidence required to move between steps:
1. reproducible snapshot hash for fixed as-of time.
2. persisted fold metrics and sensitivity outputs.
3. attribution report + promotion decision record + active bundle integrity pass.
4. observe-only runtime logs with prediction + gating reasons.
5. passing staging and cutover checklists from the same code/config revision.

## 3) Data Pipeline Operations

### 3.1 Full historical backfill
```bash
PYTHONPATH=src python3 -m qtbot data-backfill --from earliest --to $(date -u +%F) --timeframe 15m --sources ndax,kraken,binance
```

Local wrapper:
```bash
./scripts/refresh_all_available_data.sh
```

Stop/resume behavior:
- Safe to interrupt.
- Rerun same command to continue missing windows.
- No duplicate rows on rerun.
- This step is mandatory on every fresh clone because historical parquet files are local-only.
- Kraken archive CSVs under `data/kraken/` seed the full historical backfill; post-archive dates are topped up from the Kraken trades API.
- If Kraken top-up is still catching up after the archive end, `data-build-combined` may still remain complete because Binance is allowed to fill timestamps Kraken has not yet reached.

Backfill logs:
- `runtime/logs/data_backfill.log`

### 3.2 Combined dataset build
```bash
PYTHONPATH=src python3 -m qtbot data-build-combined --from 2021-01-01 --to $(date -u +%F) --timeframe 15m
```

Combined build note:
- NDAX remains first priority.
- Kraken remains the preferred calibrated external source.
- Binance may still fill timestamps missing from the preferred external source so the combined dataset remains continuous.

### 3.3 Monthly calibration
```bash
PYTHONPATH=src python3 -m qtbot data-calibrate-weights --from 2021-01-01 --to $(date -u +%F) --timeframe 15m --refresh monthly
PYTHONPATH=src python3 -m qtbot data-weight-status --timeframe 15m
```

### 3.4 Weighted training snapshot
```bash
PYTHONPATH=src python3 -m qtbot build-snapshot --asof "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --timeframe 15m
```

Experiment variants:
```bash
PYTHONPATH=src python3 -m qtbot build-snapshot --asof "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --timeframe 15m --label-horizon-bars 4
PYTHONPATH=src python3 -m qtbot build-snapshot --asof "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --timeframe 15m --exclude-symbols BTC,ETHCAD
```

Expected outputs:
- `data/snapshots/<SNAPSHOT_ID>/manifest.json`
- `data/snapshots/<SNAPSHOT_ID>/rows.parquet`

Snapshot readiness checks:
- `parity_check_passed=true`
- source mix present in manifest
- synthetic rows include monthly effective weights
- `dataset_hash` is stable for repeated runs at the same cutoff
- `label_horizon_bars` / `excluded_symbols` in the manifest match the intended experiment

### 3.5 Walk-forward training + evaluation
```bash
PYTHONPATH=src python3 -m qtbot train --snapshot <SNAPSHOT_ID> --folds 12 --universe V1
PYTHONPATH=src python3 -m qtbot eval --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot backtest --run <RUN_ID>
```

Local wrapper:
```bash
./scripts/train_latest_snapshot.sh
```

Universe note:
- active `V1` training runs use the `27` confident symbols only
- `BCH`, `RUNE`, and `TIA` are excluded from the active universe

Training readiness checks:
- `runtime/research/training/<RUN_ID>/manifest.json` exists
- `training_runs`, `training_folds`, and `fold_metrics` contain the run
- `weighted_combined` metrics are present
- `ndax_only` metrics are present only when the requested folds have sufficient NDAX history; skipped folds/scenarios are recorded in `training_runs.scenario_status`
- repeated `eval --run <RUN_ID>` rewrites identical metrics without duplicate DB rows
- synthetic supervision may appear as `direct`, `carry_backward`, or `carry_forward`; symbols with no anchor remain continuity-only
- `backtests/<BACKTEST_ID>/summary.json` exists when portfolio replay has been run
- portfolio backtest uses the snapshot label horizon as its fixed holding window

### 3.6 Attribution + bundle promotion
```bash
PYTHONPATH=src python3 -m qtbot attribution --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot promote --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot model-status
```

Promotion checks:
- the run must already be `evaluated`
- promotion chooses the best scenario that passes hard gates at the configured promotion threshold
- the global model must pass hard gates
- hard-gate trade counts/returns are recomputed at `QTBOT_PROMOTION_ENTRY_THRESHOLD`
- synthetic conversion quality is checked from current `synthetic_weights.supervised_eligible` coverage for snapshot symbols; `ndax_only` promotion skips that gate
- weak per-coin models may be omitted individually and are recorded in attribution + bundle manifest
- promoted bundle models are full-snapshot refits, not reused fold models

## 4) Launch Procedure

### 4.1 Controlled startup
```bash
PYTHONPATH=src python3 -m qtbot start --budget 250
PYTHONPATH=src python3 -m qtbot status
```

### 4.2 First-cycle validation
Verify in first cycles:
1. decisions are logged with expected gates/reasons.
2. no unexpected order placements.
3. state accounting remains coherent (`bot_cash_cad`, `realized_pnl_cad`, `fees_paid_cad`).
4. risk events trigger expected pause behavior.

## 5) Observe-Only Safety Behavior

If readiness/integrity checks fail:
- runtime must not place orders.
- control plane and logging stay active.
- recover by fixing data/calibration/model issues, then rerun staging/cutover.

## 6) Bundle Operations (When ML Runtime Phase Is Active)

Promotion:
```bash
PYTHONPATH=src python3 -m qtbot attribution --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot promote --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot model-status
```

Manual active bundle switch:
```bash
PYTHONPATH=src python3 -m qtbot pause
PYTHONPATH=src python3 -m qtbot set-active-bundle <BUNDLE_ID>
PYTHONPATH=src python3 -m qtbot model-status
PYTHONPATH=src python3 -m qtbot resume
```

Training/promotion command flow from the current checkpoint:
```bash
PYTHONPATH=src python3 -m qtbot build-snapshot --asof <ISO_TIME>
PYTHONPATH=src python3 -m qtbot train --snapshot <SNAPSHOT_ID> --folds 12 --universe V1
PYTHONPATH=src python3 -m qtbot eval --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot backtest --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot attribution --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot promote --run <RUN_ID>
PYTHONPATH=src python3 -m qtbot model-status
```

## Rollback Procedure

Rollback rules:
- pause/stop before switching active bundle.
- do not delete previous bundles.
- pointer update must be atomic and logged.

Emergency containment:
```bash
PYTHONPATH=src python3 -m qtbot stop
PYTHONPATH=src python3 -m qtbot status
```

Containerized:
```bash
docker compose exec qtbot qtbot stop
docker compose exec qtbot qtbot status
```

Preserve incident artifacts:
- `runtime/state.sqlite`
- `runtime/logs/*`
- latest coverage and calibration reports

## Incident Response

### A) Model/bundle integrity failure
Symptoms:
- startup readiness fails
- observe-only mode engaged

Actions:
1. pause/stop trading.
2. verify `model-status` and bundle manifest/signature.
3. switch to last known-good bundle (if ML runtime is active).
4. rerun staging/cutover.

### B) Data gap / coverage contract failure
Symptoms:
- combined coverage below threshold
- gap or misalignment breaches

Actions:
1. run `data-status --dataset all`.
2. rerun `data-backfill` for affected range.
3. rerun `data-build-combined`.
4. rerun `data-calibrate-weights`.
5. rerun `build-snapshot` for the intended cutoff.
6. verify coverage, weight status, and snapshot manifest again.

Notes:
- Kraken and Binance 15m outage windows are handled by deterministic carry-forward repair rows; do not patch files manually.
- Symbols with little or no symbol-local NDAX overlap rely on shared universe-level CAD conversion fallback during combined build.
- Symbols whose raw NDAX history is empty or internally gapped are expected to remain trainable through the `combined` dataset as long as combined coverage passes.

### C) Risk-trigger halt (loss/slippage/errors)
Actions:
1. keep runtime paused.
2. inspect recent decisions/trades and logs.
3. identify root cause (market regime, execution quality, API instability, config issue).
4. remediate and validate with staging/cutover before resuming.

### D) NDAX connectivity instability
Actions:
1. pause trading.
2. validate credentials/connectivity with `ndax-check`.
3. verify reconciliation and state health.
4. resume only after stable connectivity is restored.

## 9) Guardrails

- Never bypass preflight/risk checks for live execution.
- Never switch active bundle while unpaused.
- Keep operational logs append-only.
- Treat reconciliation with NDAX as mandatory for safe startup.
