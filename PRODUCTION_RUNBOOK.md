# Production Runbook (M11)

This runbook defines the required operational checklist for NDAX production launch.

## Preconditions

- `qtbot staging-validate` passed recently and `runtime/staging_validation/logs/staging_validation_report.json` is green.
- `qtbot cutover-checklist` passed for the target runtime environment.
- `.env` contains valid NDAX credentials (`NDAX_API_KEY`, `NDAX_API_SECRET`, `NDAX_USER_ID`).
- Runtime volume/directory permissions are verified.
- Operator monitoring access is available for:
  - `runtime/logs/qtbot.log`
  - `runtime/logs/decisions.csv`
  - `runtime/logs/trades.csv`

## Launch Procedure

1. Start with constrained budget:
   - `PYTHONPATH=src python3 -m qtbot start --budget 250`
2. Confirm process health:
   - `PYTHONPATH=src python3 -m qtbot status`
3. Monitor first cycles:
   - decisions are appended to `runtime/logs/decisions.csv`
   - no BTC/ETH activity appears in decisions/trades
4. If running in Docker:
   - `docker compose up -d qtbot`
   - `docker compose exec qtbot qtbot status`

## First Trade Verification

After first live fills:

- Confirm at least one row in `runtime/logs/trades.csv`.
- Verify trade fields (`symbol`, `side`, `qty`, `avg_price`, `fee_cad`, `order_id`) are populated.
- Verify `runtime/state.sqlite` totals are internally consistent:
  - `bot_cash_cad`
  - `realized_pnl_cad`
  - `fees_paid_cad`
- Confirm expected alerting behavior for lifecycle/risk/reconciliation paths.

## Rollback Procedure

1. Graceful stop immediately:
   - `PYTHONPATH=src python3 -m qtbot stop`
2. If containerized:
   - `docker compose exec qtbot qtbot stop`
3. Wait until status reports `STOPPED`:
   - `PYTHONPATH=src python3 -m qtbot status`
4. Preserve artifacts:
   - `runtime/state.sqlite`
   - `runtime/logs/qtbot.log`
   - `runtime/logs/trades.csv`
   - `runtime/logs/decisions.csv`

## Incident Response

1. Contain:
   - stop bot with graceful command path above.
2. Capture:
   - snapshot runtime DB/log files and current `.env` runtime knobs (without exposing secrets).
3. Diagnose:
   - identify trigger class: exchange/API, strategy signal, execution/fill, reconciliation, or risk halt.
4. Reconcile:
   - confirm NDAX balances/positions and verify local state convergence before restart.
5. Recover:
   - run `qtbot staging-validate` and `qtbot cutover-checklist` again before resuming live mode.
