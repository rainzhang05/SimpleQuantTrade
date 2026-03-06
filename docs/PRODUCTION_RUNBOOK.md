# Production Runbook: ML 15m Dual-Source Operations

This runbook covers production-safe operation for the NDAX execution runtime with NDAX+Binance training data pipeline.

Canonical design source:
- `docs/ROADMAP.md`

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

## 2) ML/Data Readiness Commands

Run in order:
```bash
PYTHONPATH=src python3 -m qtbot data-status --timeframe 15m --dataset all
PYTHONPATH=src python3 -m qtbot data-weight-status --timeframe 15m
PYTHONPATH=src python3 -m qtbot staging-validate --offline-only --budget 1000 --cadence-seconds 1 --min-loops 1 --timeout-seconds 30
PYTHONPATH=src python3 -m qtbot cutover-checklist --offline-only --budget 250 --staging-max-age-hours 168
```

Expected outcomes:
- `combined` coverage meets configured contract.
- at least one recent calibration report exists.
- cutover reports `passed=true`.

## 3) Data Pipeline Operations

### 3.1 Full historical backfill
```bash
PYTHONPATH=src python3 -m qtbot data-backfill --from 2021-01-01 --to $(date -u +%F) --timeframe 15m --sources ndax,binance
```

Stop/resume behavior:
- Safe to interrupt.
- Rerun same command to continue missing windows.
- No duplicate rows on rerun.

Backfill logs:
- `runtime/logs/data_backfill.log`

### 3.2 Combined dataset build
```bash
PYTHONPATH=src python3 -m qtbot data-build-combined --from 2021-01-01 --to $(date -u +%F) --timeframe 15m
```

### 3.3 Monthly calibration
```bash
PYTHONPATH=src python3 -m qtbot data-calibrate-weights --from 2021-01-01 --to $(date -u +%F) --timeframe 15m --refresh monthly
PYTHONPATH=src python3 -m qtbot data-weight-status --timeframe 15m
```

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
5. verify coverage and weight status again.

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
