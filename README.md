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
