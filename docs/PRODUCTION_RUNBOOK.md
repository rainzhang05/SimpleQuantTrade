# Production Runbook: SimpleQuantTrade ML 15m

This runbook defines launch and incident procedures for the ML-driven 15m architecture.

Canonical behavior source:
- `docs/ROADMAP.md`

## 1) Preconditions

Required before production launch:
- Recent successful `qtbot staging-validate` report.
- Successful `qtbot cutover-checklist` in target environment.
- NDAX credentials configured and validated.
- Runtime directories writable:
- `runtime/`
- `runtime/logs/`
- `models/bundles/`
- Active bundle exists and passes integrity validation.
- Operator access to logs and runtime status.

Mandatory monitored artifacts:
- `runtime/logs/qtbot.log`
- `runtime/logs/decisions.csv`
- `runtime/logs/trades.csv`
- `runtime/state.sqlite`

## 2) Pre-Launch ML Readiness Checklist

Run these checks before enabling live order path:
1. Verify data coverage for 15m timeframe is current and gap thresholds pass.
2. Verify active bundle pointer and manifest metadata.
3. Verify bundle signature/hash integrity.
4. Verify feature spec compatibility with runtime code.
5. Verify warmup sufficiency for configured universe.
6. Confirm BTC/ETH lock policy setting.
7. Confirm risk controls and thresholds are configured.

Recommended commands:
```bash
qtbot data-status
qtbot model-status
qtbot cutover-checklist --budget 250 --staging-max-age-hours 48
```

## 3) Launch Procedure

### 3.1 Initial controlled launch
Start with constrained budget:
```bash
qtbot start --budget 250
```

Check health:
```bash
qtbot status
```

### 3.2 Observe-only validation window
If system starts in observe-only mode (for any readiness or integrity reason):
- Do not force live trading.
- Resolve failing preflight conditions.
- Re-run staging and cutover checks.

### 3.3 Containerized launch
```bash
docker compose up -d qtbot
docker compose exec qtbot qtbot status
```

## 4) First-Cycle Validation

Within first cycles after launch, verify:
- Decisions log contains prediction context and gating reasons.
- No orders are placed outside bar-close decision points.
- If live enabled and valid signals exist, fills append to `trades.csv`.
- State totals are coherent:
- `bot_cash_cad`
- `realized_pnl_cad`
- `fees_paid_cad`
- Alerts fire for lifecycle and risk events when triggered.

## 5) Bundle Operations

### 5.1 Promote new model bundle
```bash
qtbot promote --run <RUN_ID>
qtbot model-status
```

### 5.2 Rollback active bundle
Rollback requires paused trading.

```bash
qtbot pause
qtbot set-active-bundle <BUNDLE_ID>
qtbot model-status
qtbot resume
```

Rollback rules:
- Never delete old promoted bundles during rollback.
- Pointer update must be atomic and logged.

## 6) Emergency Stop and Containment

Immediate containment:
```bash
qtbot stop
```

Containerized:
```bash
docker compose exec qtbot qtbot stop
```

Confirm stopped:
```bash
qtbot status
```

Preserve artifacts for investigation:
- `runtime/state.sqlite`
- `runtime/logs/qtbot.log`
- `runtime/logs/decisions.csv`
- `runtime/logs/trades.csv`

## 7) Incident Playbooks

### 7.1 Model bundle integrity failure
Symptoms:
- startup preflight fails bundle integrity checks
- runtime enters observe-only mode

Actions:
1. pause/stop if not already contained
2. inspect `model-status` and bundle manifest/signature
3. switch to last known-good bundle via `set-active-bundle`
4. re-run cutover checklist
5. resume only after checks pass

### 7.2 Data gap or warmup coverage failure
Symptoms:
- preflight blocks live order path due to missing bars/warmup

Actions:
1. run `data-status`
2. backfill missing windows
3. verify gap thresholds clear
4. rerun preflight via cutover checklist

### 7.3 Risk halt triggered
Symptoms:
- trading auto-paused by daily loss cap, slippage guard, or error limit

Actions:
1. keep bot paused
2. inspect recent decisions/trades and risk events in logs
3. classify trigger root cause (market move, execution quality, API instability, config issue)
4. apply remediation
5. run staging/cutover checks before resuming

### 7.4 NDAX API instability
Symptoms:
- repeated connectivity/auth/order status errors

Actions:
1. pause trading
2. verify NDAX reachability and credential status
3. check retry/error trend in `qtbot.log`
4. resume only after stability is restored and reconciliation succeeds

## 8) Recovery and Resume Procedure

After incident remediation:
1. ensure active bundle and data coverage are valid
2. run staging validation
3. run cutover checklist
4. start with constrained budget
5. monitor first cycles closely

## 9) Operational Guardrails

- Never bypass preflight failures for live trading.
- Never switch active bundle while bot is running unpaused.
- Keep all logs append-only and retain evidence for postmortems.
- Treat reconciliation with NDAX as mandatory before order placement.
