# SimpleQuantTrade

SimpleQuantTrade is a production-oriented NDAX trading runtime being upgraded to a laptop-grade ML 15m system.

Authoritative docs:
- Roadmap: `docs/ROADMAP.md`
- Execution plan: `docs/PLAN.md`
- Production runbook: `docs/PRODUCTION_RUNBOOK.md`
- Legacy archive: `docs/LEGACY_FIXED_RULE_ARCHIVE.md`

## Current Active Direction

- Execution venue: NDAX only (spot, CAD budget safety, reconciliation-first startup).
- Data: dual-source 15m pipeline (`NDAX + Binance`) with deterministic combined CAD dataset.
- Safety shell retained: control plane, preflight, risk halts, append-only logs, docker/staging/cutover.
- Training/runtime ML path: phased per roadmap and plan.

## CLI Surface

### Lifecycle and operations
- `qtbot start --budget <CAD>`
- `qtbot pause`
- `qtbot resume`
- `qtbot stop`
- `qtbot status`
- `qtbot ndax-pairs`
- `qtbot ndax-candles --symbol <NDAX_SYMBOL> --from-date YYYY-MM-DD --to-date YYYY-MM-DD`
- `qtbot ndax-balances`
- `qtbot ndax-check`
- `qtbot staging-validate`
- `qtbot cutover-checklist`

### Data pipeline
- `qtbot data-backfill --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m --sources ndax,binance`
- `qtbot data-status --timeframe 15m --dataset ndax|binance|combined|all`
- `qtbot data-build-combined --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m`
- `qtbot data-calibrate-weights --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m --refresh monthly`
- `qtbot data-weight-status --timeframe 15m`
- `qtbot build-snapshot --asof <ISO_TIME> --timeframe 15m`
- `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1`
- `qtbot eval --run <RUN_ID>`

Defaults:
- `data-backfill` defaults to `--sources ndax,binance`
- `data-status` defaults to `--dataset combined`

## Quickstart

### 1) Install
```bash
python3 -m pip install -e .
```

### 2) Configure environment
```bash
cp .env.example .env
```

Required NDAX private credentials for private commands:
- `NDAX_API_KEY`
- `NDAX_API_SECRET`
- `NDAX_USER_ID`

Local data rule:
- `data/` is not version-controlled.
- After cloning on a new machine, you must run the data workflow locally to populate raw, combined, and snapshot artifacts.

### 3) Validate runtime shell
```bash
PYTHONPATH=src python3 -m qtbot status
PYTHONPATH=src python3 -m qtbot staging-validate --offline-only --budget 1000 --cadence-seconds 1 --min-loops 1 --timeout-seconds 30
PYTHONPATH=src python3 -m qtbot cutover-checklist --offline-only --budget 250 --staging-max-age-hours 168
```

## End-to-End Data Workflow (Implemented Through Phase 6)

### Step 1: Backfill raw NDAX + Binance 15m data
```bash
PYTHONPATH=src python3 -m qtbot data-backfill --from 2021-01-01 --to $(date -u +%F) --timeframe 15m --sources ndax,binance
```

Resume behavior:
- Safe to stop anytime.
- Rerun the same command to continue from missing windows.
- No duplicate rows are written (idempotent merge).
- Exchange-wide Binance outage windows are sealed deterministically with flat zero-volume carry rows.

Backfill progress log:
- `runtime/logs/data_backfill.log`

### Step 2: Inspect coverage
```bash
PYTHONPATH=src python3 -m qtbot data-status --timeframe 15m --dataset all
```

Coverage reports are written to:
- `runtime/logs/data_coverage_ndax.json`
- `runtime/logs/data_coverage_binance.json`
- `runtime/logs/data_coverage_combined.json`

### Step 3: Build combined CAD dataset
```bash
PYTHONPATH=src python3 -m qtbot data-build-combined --from 2021-01-01 --to $(date -u +%F) --timeframe 15m
```

Combined build notes:
- NDAX rows still take precedence over synthetic rows.
- Symbols without enough symbol-local NDAX overlap fall back to shared universe-level CAD conversion ratios.
- If NDAX omits a symbol entirely or has internal history gaps, the combined dataset fills those bars from normalized Binance and keeps source tags for audit.

### Step 4: Calibrate monthly synthetic weights
```bash
PYTHONPATH=src python3 -m qtbot data-calibrate-weights --from 2021-01-01 --to $(date -u +%F) --timeframe 15m --refresh monthly
PYTHONPATH=src python3 -m qtbot data-weight-status --timeframe 15m
```

Calibration report output:
- `runtime/research/bridge_weighting/<RUN_ID>/metrics.json`

### Step 5: Build sealed weighted training snapshot
```bash
PYTHONPATH=src python3 -m qtbot build-snapshot --asof "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --timeframe 15m
```

Snapshot output:
- `data/snapshots/<SNAPSHOT_ID>/manifest.json`
- `data/snapshots/<SNAPSHOT_ID>/rows.parquet`

Snapshot supervision notes:
- `synthetic_gap_fill` rows are retained for continuity/audit.
- `synthetic_gap_fill` rows, and rows whose next bar is `synthetic_gap_fill`, are excluded from supervised labels.
- synthetic supervision is enabled only when `synthetic_weights.supervised_eligible=true`.
- direct-quality months use `eligibility_mode=direct`.
- zero-overlap months before the first same-symbol anchor use `eligibility_mode=carry_backward`.
- zero-overlap months after an established anchor use `eligibility_mode=carry_forward`.

### Step 6: Train and evaluate walk-forward models
```bash
PYTHONPATH=src python3 -m qtbot train --snapshot <SNAPSHOT_ID> --folds 12 --universe V1
PYTHONPATH=src python3 -m qtbot eval --run <RUN_ID>
```

Training artifacts:
- `runtime/research/training/<RUN_ID>/manifest.json`
- `runtime/research/training/<RUN_ID>/feature_spec.json`
- `runtime/research/training/<RUN_ID>/folds.json`
- `runtime/research/training/<RUN_ID>/metrics.json`
- `runtime/research/training/<RUN_ID>/predictions/fold_<NN>/<scenario>.parquet`
- `runtime/research/training/<RUN_ID>/models/global/<scenario>/fold_<NN>.txt`
- `runtime/research/training/<RUN_ID>/models/per_coin/<SYMBOL>/<scenario>/fold_<NN>.txt`

## Next Steps to Final Production ML (Current -> Final)

Current program status:
- Implemented now: dual-source ingestion, combined CAD build, monthly calibration weighting, weighted training snapshot integration, and walk-forward training/evaluation.
- Next active build phase: promotion gates and model bundle publishing (see `docs/PLAN.md` phases 7-9).

Execution sequence:
1. Keep data current:
   - rerun `data-backfill`, `data-build-combined`, `data-calibrate-weights`, `build-snapshot`, `train`, and `eval` for the latest cutoff.
2. Implement promotion gates and model bundle publishing (Phase 7).
3. Implement live ML inference path with observe-only fallback (Phase 8).
4. Complete staging/cutover evidence and rollback drill, then enable ML live path (Phase 9).

Implemented Phase 6 commands:
- `qtbot build-snapshot --asof <ISO_TIME>`
- `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1`
- `qtbot eval --run <RUN_ID>`

Planned CLI commands for the remaining phases (not fully implemented yet):
- `qtbot promote --run <RUN_ID>`
- `qtbot model-status`
- `qtbot predict --symbol <SYM> --at latest`
- `qtbot set-active-bundle <BUNDLE_ID>`

Do not enable ML live trading until all phase gates pass:
- deterministic snapshot reproducibility
- deterministic fold metrics and promotion decisions
- bundle integrity verification
- staging and cutover checklist pass

## Storage Contract

All `data/` paths below are local-only and ignored by git:
- `data/raw/ndax/15m/*.parquet`
- `data/raw/binance/15m/*USDT.parquet`
- `data/combined/15m/*.parquet`
- `data/snapshots/*`
- `runtime/state.sqlite`
- `runtime/control.json`
- `runtime/logs/*`

## Key Config (Dual-Source)

- `QTBOT_DATA_SOURCES=ndax,binance`
- `QTBOT_DATASET_MODE=combined`
- `QTBOT_BINANCE_BASE_URL=https://api.binance.com`
- `QTBOT_BINANCE_QUOTE=USDT`
- `QTBOT_BRIDGE_FX_SYMBOL=USDTCAD`
- `QTBOT_SYNTH_WEIGHT_MIN=0.20`
- `QTBOT_SYNTH_WEIGHT_MAX=0.80`
- `QTBOT_SYNTH_WEIGHT_DEFAULT=0.60`
- `QTBOT_SYNTH_WEIGHT_REFRESH=monthly`
- `QTBOT_MIN_OVERLAP_ROWS_FOR_WEIGHT=1000`
- `QTBOT_CONVERSION_MAX_MEDIAN_APE=0.015`
- `QTBOT_COMBINED_MAX_GAP_COUNT=0`
- `QTBOT_COMBINED_MIN_COVERAGE=0.999`
- `QTBOT_TRAIN_SEED=42`
- `QTBOT_TRAIN_WINDOW_MONTHS=12`
- `QTBOT_VALID_WINDOW_MONTHS=1`
- `QTBOT_TRAIN_STEP_MONTHS=1`
- `QTBOT_FEE_PCT_PER_SIDE` defaults to `QTBOT_TAKER_FEE_RATE`

## Docker Usage

```bash
docker build -t simplequanttrade:latest .
docker compose up -d qtbot

docker compose exec qtbot qtbot status
docker compose exec qtbot qtbot data-status --timeframe 15m --dataset combined
docker compose exec qtbot qtbot data-weight-status --timeframe 15m
```

## Testing

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"
```

## Safety Notes

- NDAX is execution truth; trading remains NDAX-only.
- Dual-source data improves training continuity; it does not change execution venue.
- Do not bypass preflight/risk gates.
- If readiness checks fail, run observe-only until resolved.
