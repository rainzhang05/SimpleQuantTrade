# SimpleQuantTrade
A simple fixed-rule quantitative crypto trading system that runs as a command-line bot on NDAX, with live evaluation at the smallest practical cadence.

## Current CLI (M1 + M2)

- `qtbot start --budget <CAD>`
- `qtbot pause`
- `qtbot resume`
- `qtbot stop`
- `qtbot status`
- `qtbot ndax-pairs`
- `qtbot ndax-candles --symbol <NDAX_SYMBOL> --from-date YYYY-MM-DD --to-date YYYY-MM-DD`
- `qtbot ndax-balances`
- `qtbot ndax-check`

Copy `.env.example` to `.env` and fill NDAX credentials before private API checks.

## M3/M4 Runtime Behavior

- `qtbot start --budget <CAD>` now evaluates live NDAX data each loop and generates deterministic strategy signals.
- Decisions are appended to `runtime/logs/decisions.csv` with:
  - `timestamp,symbol,close,ema_fast,ema_slow,atr,signal,reason`
- Live execution (M4) is enabled with `QTBOT_ENABLE_LIVE_TRADING=true`.
- In live mode, ENTER/EXIT decisions place NDAX market orders, ledger totals are updated in `runtime/state.sqlite`, and fills are appended to `runtime/logs/trades.csv`.
