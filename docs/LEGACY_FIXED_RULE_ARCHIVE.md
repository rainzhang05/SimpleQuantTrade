# Legacy Archive: Fixed-Rule 1m EMA/ATR System

Archive status:
- Retired as active target architecture.
- Preserved for migration history and operational context.
- Archived on March 5, 2026.

Active architecture is defined in `docs/ROADMAP.md`.

## 1) Legacy System Summary

The retired system was a fixed-rule, long-only NDAX spot bot with:
- 60-second loop cadence
- 1-minute signal candles
- EMA/ATR-based deterministic entry and exit rules
- market-order execution
- persistent runtime state in SQLite
- control plane and safety shell

## 2) Legacy Core Rules

### Indicators
- `EMA_fast = 60`
- `EMA_slow = 360`
- `ATR = 60`

### Entry logic
Enter long when all conditions were true:
- symbol in configured universe and CAD pair available
- no open position
- `EMA_fast > EMA_slow`
- `close <= EMA_fast`
- cooldown satisfied

### Exit logic
Exit when any condition was true:
- trend break (`EMA_fast < EMA_slow`)
- ATR stop (`close < entry_price - 2.5 * ATR`)
- time stop (default 48 hours)

### Candidate selection
- scored by `(EMA_fast - EMA_slow) / close`
- capped by `MAX_NEW_ENTRIES_PER_CYCLE=3` per loop for operational burst control

## 3) Legacy Universe and Timeframe

- Universe was hardcoded top-20 list.
- Runtime traded only symbols with NDAX CAD spot pair.
- No BTC/ETH-specific lock policy in active logic.

## 4) Legacy Fee and Risk Defaults

Legacy defaults:
- `QTBOT_TAKER_FEE_RATE=0.002` (0.2% per side)
- daily loss cap enabled
- slippage guard enabled
- consecutive-error kill-switch enabled

## 5) Legacy Operational Shell (Retained in v2)

The following shell components originated in legacy architecture and remain part of v2:
- lifecycle control plane (`start/pause/resume/stop/status`)
- startup reconciliation with NDAX as source of truth
- go-live preflight gate
- append-only runtime logs
- Docker packaging and staging/cutover drills

## 6) Why It Was Retired

Retirement drivers:
- fixed-rule strategy ceiling on adaptability across symbols and regimes
- requirement for reproducible model promotion workflow
- requirement for richer cost-aware evaluation and fold stability controls

The v2 design replaces fixed-rule signal logic with ML prediction while preserving the operational safety shell.

## 7) Migration Mapping (Legacy -> v2)

- Signal engine:
- legacy: EMA/ATR fixed rules
- v2: LightGBM global + per-coin blend

- Timeframe:
- legacy: 1m candles
- v2: 15m candles

- Data pipeline:
- legacy: live-only loop fetch
- v2: persistent 15m Parquet + sealed snapshots

- Evaluation:
- legacy: live-only behavior
- v2: offline walk-forward + promotion gates

- Deployment artifact:
- legacy: strategy code only
- v2: signed model bundle + atomic active pointer

## 8) Command History Context

Legacy command set included:
- `start`, `pause`, `resume`, `stop`, `status`
- `ndax-pairs`, `ndax-candles`, `ndax-balances`, `ndax-check`
- `staging-validate`, `cutover-checklist`

In v2, these remain, and ML/data/training/model commands are added per roadmap.

## 9) Usage Policy

Do not treat this document as active design or implementation guidance.
Use `docs/ROADMAP.md` for all current and future development decisions.
