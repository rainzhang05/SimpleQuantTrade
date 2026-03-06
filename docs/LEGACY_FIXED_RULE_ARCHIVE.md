# Legacy Archive: Fixed-Rule 1m EMA/ATR Runtime

Status:
- Historical archive only.
- Not the active implementation target.
- Active architecture is defined in `docs/ROADMAP.md`.

Archive purpose:
- preserve migration context from 1m fixed-rule runtime to ML-oriented 15m dual-source pipeline.

## 1) What the Legacy System Was

The retired system used:
- NDAX-only live candle polling
- 1-minute EMA/ATR deterministic rules
- market-order execution
- lifecycle control plane and safety shell
- runtime SQLite state + append-only logs

## 2) Legacy Signal Rules (Historical)

Indicators:
- `EMA_fast=60`
- `EMA_slow=360`
- `ATR=60`

Entry (long):
- trend up (`EMA_fast > EMA_slow`)
- pullback (`close <= EMA_fast`)
- no open position
- cooldown satisfied

Exit:
- trend break
- ATR stop (`entry - 2.5*ATR`)
- max hold timeout

## 3) What Was Kept

The following production shell remains in active architecture:
- lifecycle commands (`start/pause/resume/stop/status`)
- reconciliation-first startup
- preflight safety gate
- daily loss/slippage/error risk halts
- append-only operational logging
- docker/staging/cutover workflow

## 4) What Changed

- Signal source changed: fixed rules -> ML prediction path (phased activation).
- Timeframe changed: 1m -> 15m.
- Data strategy changed: NDAX-only raw -> NDAX+Binance combined training dataset.
- Added deterministic overlap-error calibration and synthetic row weighting.
- Added combined dataset coverage contracts and calibration reports.

## 5) Migration Context

Migration sequence:
1. preserve runtime safety shell
2. implement dual-source data retrieval
3. build combined CAD dataset
4. calibrate monthly synthetic weights
5. integrate weighted training/eval/promotion
6. activate ML runtime path with rollback safeguards

## 6) Legacy Command Context

Legacy command set (still retained):
- `start`, `pause`, `resume`, `stop`, `status`
- `ndax-pairs`, `ndax-candles`, `ndax-balances`, `ndax-check`
- `staging-validate`, `cutover-checklist`

New active data commands are documented in:
- `README.md`
- `docs/ROADMAP.md`

## 7) Usage Policy

Do not use this file for new implementation decisions.
Use:
- `docs/ROADMAP.md` for architecture/contracts
- `docs/PLAN.md` for phase execution
- `docs/PRODUCTION_RUNBOOK.md` for operations
