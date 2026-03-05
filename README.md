# SimpleQuantTrade

SimpleQuantTrade is a production-oriented NDAX CLI trading bot being upgraded to an ML-driven 15m architecture.

Active design authority:
- `docs/ROADMAP.md` (canonical specification)
- `docs/PLAN.md` (implementation sequencing)
- `docs/PRODUCTION_RUNBOOK.md` (operations)

Legacy fixed-rule 1m EMA/ATR design:
- `docs/LEGACY_FIXED_RULE_ARCHIVE.md` (archive only)

## Architecture Summary

Target v2 architecture:
- NDAX execution venue, spot-only, CAD budget safety model.
- 15m candle data pipeline with Parquet storage.
- Deterministic feature pipeline (50 to 80 features).
- LightGBM global + per-coin models.
- Walk-forward training/evaluation and promotion gates.
- Live inference with deterministic blend combiner.
- Existing operational shell retained:
- control plane (`start/pause/resume/stop/status`)
- reconciliation-first startup
- go-live preflight
- risk halts and append-only logs
- Docker + staging/cutover checks

## Safety Model

Non-negotiable safeguards:
- NDAX only, spot-only, no leverage/margin/borrowing.
- Exchange balances are source of truth.
- Market orders only.
- Daily loss cap, slippage guard, consecutive-error kill-switch.
- Live trading path remains explicitly gated.
- Bundle integrity failures trigger observe-only mode.

## Quickstart

### 1) Environment
- Copy `.env.example` to `.env`.
- Fill required NDAX credentials:
- `NDAX_API_KEY`
- `NDAX_API_SECRET`
- `NDAX_USER_ID`
- Optional: `NDAX_USERNAME`

### 2) Local install
```bash
python3 -m pip install -e .
```

### 3) Core control-plane commands
```bash
qtbot start --budget 1000
qtbot status
qtbot pause
qtbot resume
qtbot stop
```

### 4) Existing NDAX/operator commands
```bash
qtbot ndax-pairs
qtbot ndax-candles --symbol SOLCAD --from-date 2026-03-01 --to-date 2026-03-05
qtbot ndax-balances
qtbot ndax-check
qtbot staging-validate --offline-only
qtbot cutover-checklist --offline-only
```

## ML Workflow (Roadmap v2 Target)

End-to-end v2 workflow:
```bash
qtbot data-backfill --from 2021-01-01 --to 2026-03-01 --timeframe 15m
qtbot data-status
qtbot build-snapshot --asof 2026-03-01T00:00Z
qtbot train --snapshot <SNAPSHOT_ID> --folds 12 --universe V1
qtbot eval --run <RUN_ID>
qtbot promote --run <RUN_ID>
qtbot model-status
qtbot predict --symbol SOLCAD --at latest
qtbot start --budget 1000
```

Rollback workflow:
```bash
qtbot pause
qtbot set-active-bundle <BUNDLE_ID>
qtbot resume
```

Note:
- The roadmap command surface above is the upgrade target and is delivered by phased milestones in `docs/PLAN.md`.

## Universe and Defaults (v2)

Universe V1 (fixed 30 tickers):
- `BTC, ETH, XRP, SOL, ADA, DOGE, AVAX, LINK, DOT, BCH, LTC, XLM, TON, UNI, NEAR, ATOM, HBAR, AAVE, ALGO, APT, ARB, FET, FIL, ICP, INJ, OP, SUI, TIA, RUNE, SEI`

Eligibility rules:
- Trade only tickers that have NDAX CAD pair at runtime.
- Missing CAD pairs are skipped and logged.

Policy defaults:
- `QTBOT_TIMEFRAME=15m`
- `QTBOT_UNIVERSE=V1`
- `QTBOT_BTC_ETH_LOCK=true`
- `QTBOT_FEE_PCT_PER_SIDE=0.002`
- `QTBOT_ENTRY_THRESHOLD=0.60`
- `QTBOT_EXIT_THRESHOLD=0.48`

## Storage Layout (v2)

```text
data/raw/ndax/15m/<SYMBOL>.parquet
data/snapshots/<SNAPSHOT_ID>/
models/bundles/<BUNDLE_ID>/
models/bundles/LATEST
runtime/state.sqlite
runtime/control.json
runtime/logs/
```

## Docker Usage

Build and start:
```bash
docker build -t simplequanttrade:latest .
docker compose up -d qtbot
```

Control from another terminal:
```bash
docker compose exec qtbot qtbot status
docker compose exec qtbot qtbot pause
docker compose exec qtbot qtbot resume
docker compose exec qtbot qtbot stop
```

Run roadmap command surface in container:
```bash
docker compose exec qtbot qtbot model-status
docker compose exec qtbot qtbot data-status
```

## Testing and CI

Run tests locally:
```bash
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"
```

Coverage:
```bash
PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"
coverage report --show-missing
```

CI expectations:
- compile + tests + coverage gate
- offline staging and cutover command checks
- docker image and compose validation

## Operator Notes

Before live launch:
- run staging validation
- run cutover checklist
- verify model bundle integrity and active pointer
- review runbook: `docs/PRODUCTION_RUNBOOK.md`
