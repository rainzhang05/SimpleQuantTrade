# SimpleQuantTrade
A simple fixed-rule quantitative crypto trading system that runs as a command-line bot on NDAX, with live evaluation at the smallest practical cadence.

## Current CLI (M1-M11)

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

Copy `.env.example` to `.env` and fill NDAX credentials before private API checks.

## M3/M4 Runtime Behavior

- `qtbot start --budget <CAD>` now evaluates live NDAX data each loop and generates deterministic strategy signals.
- Decisions are appended to `runtime/logs/decisions.csv` with:
  - `timestamp,symbol,close,ema_fast,ema_slow,atr,signal,reason`
- Live execution (M4) is enabled with `QTBOT_ENABLE_LIVE_TRADING=true`.
- In live mode, ENTER/EXIT decisions place NDAX market orders, ledger totals are updated in `runtime/state.sqlite`, and fills are appended to `runtime/logs/trades.csv`.

## M5 Startup Reconciliation

- On startup, the runner performs reconciliation against NDAX balances before entering the trading loop.
- NDAX is treated as the source of truth for tracked holdings and reconciliation changes are written to `state_events`.
- In live mode, startup is blocked if reconciliation fails.

## M6 Go-Live Preflight Gate

- In live mode (`QTBOT_ENABLE_LIVE_TRADING=true`), startup now runs a go-live preflight after reconciliation.
- Required checks: credentials/auth, NDAX API reachability, CAD market coverage, candle warm-up sufficiency, state DB health, and control-file integrity.
- Live startup is blocked when any preflight check fails, and failed checks are logged explicitly in `runtime/logs/qtbot.log` and `state_events`.
- Candle warm-up uses coverage gating with `QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE` (default `0.8`) so isolated symbols with sparse candles do not disable all live trading.

## M7 Risk Hardening

- Daily loss cap guard auto-pauses trading when daily realized PnL breaches `QTBOT_DAILY_LOSS_CAP_CAD`.
- Slippage guard monitors realized fill-vs-signal slippage; breach of `QTBOT_MAX_SLIPPAGE_PCT` halts further orders for the cycle and pauses trading.
- Consecutive execution/API errors are tracked in persistent state; if the count reaches `QTBOT_CONSECUTIVE_ERROR_LIMIT`, the bot auto-pauses.

## M8 Logging + Discord Alerting

- Runtime append-only logs remain in:
  - `runtime/logs/qtbot.log`
  - `runtime/logs/decisions.csv`
  - `runtime/logs/trades.csv`
- Optional Discord alerting is enabled by setting `QTBOT_DISCORD_WEBHOOK_URL`.
- Alert transport tuning:
  - `QTBOT_DISCORD_TIMEOUT_SECONDS`
  - `QTBOT_DISCORD_MAX_RETRIES`
- Alerts are emitted for:
  - lifecycle `PAUSE`/`STOP` transitions,
  - repeated API/execution failures,
  - reconciliation anomalies,
  - risk-triggered trading halts.

## M9 Docker Production Packaging

- Build image:
  - `docker build -t simplequanttrade:latest .`
- Start with compose (uses `.env`, persistent `./runtime` volume):
  - `docker compose up -d qtbot`
- Control lifecycle from another terminal:
  - `docker compose exec qtbot qtbot status`
  - `docker compose exec qtbot qtbot pause`
  - `docker compose exec qtbot qtbot resume`
  - `docker compose exec qtbot qtbot stop`
- Default compose startup budget is controlled by:
  - `QTBOT_START_BUDGET_CAD` (defaults to `1000` if unset)

## M10 Staging Validation

- Run full staging validation (live NDAX public checks + dry-run lifecycle drill + simulated reconciliation/risk faults):
  - `PYTHONPATH=src python3 -m qtbot staging-validate --budget 1000 --cadence-seconds 3 --min-loops 2 --timeout-seconds 120`
- Run offline-only staging validation (for CI/local no-network testing):
  - `PYTHONPATH=src python3 -m qtbot staging-validate --offline-only --budget 1000 --cadence-seconds 1 --min-loops 1 --timeout-seconds 30`
- Validation outputs:
  - JSON summary to stdout
  - persisted report at `runtime/staging_validation/logs/staging_validation_report.json`

## M11 Production Cutover Checklist

- Run full cutover readiness checks:
  - `PYTHONPATH=src python3 -m qtbot cutover-checklist --budget 250 --staging-max-age-hours 48`
- Run offline cutover checks (CI/local no-network):
  - `PYTHONPATH=src python3 -m qtbot cutover-checklist --offline-only --budget 250 --staging-max-age-hours 168`
- Optional strict alerting requirement:
  - add `--require-discord` to require `QTBOT_DISCORD_WEBHOOK_URL`
- Outputs:
  - JSON summary to stdout
  - persisted report at `runtime/production_cutover/logs/production_cutover_report.json`
- Operator runbook:
  - `docs/PRODUCTION_RUNBOOK.md`

## Testing and CI

- Local test run:
  - `PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"`
- Coverage run:
  - `PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"`
  - `coverage report --show-missing`
- GitHub Actions CI runs:
  - compile + unit test + coverage gates
  - offline staging validation CLI check
  - offline cutover checklist CLI check
  - Docker image build and containerized CLI/compose validation checks
