# PLAN.md + Docs Migration Plan (NDAX Production Roadmap)

## 1) Scope and Non-Negotiable Constraints

This plan defines implementation phases to build and harden a production-grade CLI quant trader on **NDAX** with deterministic behavior and strict operational safety.

Hard constraints:
- Exchange: **NDAX** only.
- Credentials from `.env`:
  - `NDAX_API_KEY`
  - `NDAX_API_SECRET`
  - `NDAX_USER_ID`
  - `NDAX_USERNAME` (optional)
- Spot-only, no margin/leverage/borrowing/shorting.
- CAD budget virtual sub-account model (`--budget`-based internal cash ledger).
- BTC/ETH are permanently locked (no buy/sell, excluded from logic).
- Tradable universe = hardcoded top 20 list minus BTC/ETH, and only symbols with NDAX CAD spot markets.
- Live 60-second evaluation cadence, 1-minute signal timeframe.
- Graceful control lifecycle from another terminal: `start`, `pause`, `resume`, `stop`, `status`.
- Deterministic strategy and fee-aware accounting (0.4% per side).
- Atomic persistence and safe restart reconciliation with NDAX as source of truth.

## 2) Architecture / Modules (NDAX-Oriented)

Target module boundaries:
- `cli`:
  - command parsing, lifecycle operations, status rendering.
- `control`:
  - atomic `runtime/control.json` updates and reads.
- `runner`:
  - 60-second loop orchestration, command handling, state transitions.
- `config`:
  - defaults, environment loading, runtime validation.
- `ndax_client`:
  - auth/signing, balances, market metadata, OHLC candles, order endpoints, order status/fills.
- `universe`:
  - hardcoded top-20 list, BTC/ETH exclusion, NDAX CAD market validation.
- `strategy`:
  - EMA/ATR calculations, deterministic signal generation.
- `execution`:
  - order sizing, market order submission, fill confirmation, idempotency guards.
- `ledger`:
  - cash/position/PnL/fee accounting and invariants.
- `state`:
  - SQLite persistence and recovery/reconciliation workflow.
- `preflight`:
  - go-live safety validation gate prior to live order placement.
- `risk`:
  - live risk gates (loss cap, slippage, kill-switch thresholds).
- `logging`:
  - `qtbot.log`, `trades.csv`, `decisions.csv` append-only outputs.
- `alerts`:
  - Discord webhook notifications for critical events.

## 3) Implementation Phases (M0-M11)

### M0: Docs and Spec Alignment
- Update all docs to NDAX naming/credentials.
- Create and maintain this `PLAN.md`.
- Confirm docs are consistent (`README`, `ROADMAP`, `AGENTS`, `PLAN`).

Exit criteria:
- No stale legacy exchange/symbol references in core docs.
- Credential contract standardized on `NDAX_API_KEY` / `NDAX_API_SECRET`.

M0 implementation status:
- [x] `README.md` updated to NDAX.
- [x] `ROADMAP.md` updated to NDAX terms, credentials, client naming, and balance variable naming.
- [x] `AGENTS.md` updated to NDAX API behavior wording.
- [x] `PLAN.md` created and aligned with NDAX-first implementation phases.
- [x] Cross-doc consistency checks executed and passed.

M0 validation commands:
- `rg -n "<legacy_exchange_terms>" README.md ROADMAP.md AGENTS.md PLAN.md` -> no matches.
- `rg -n "NDAX_API_KEY|NDAX_API_SECRET|ndax_available_cad|ndax_client" README.md ROADMAP.md AGENTS.md PLAN.md` -> expected NDAX contract matches present.

### M1: CLI, Control Plane, and Persistence Skeleton
- Implement CLI commands:
  - `qtbot start --budget <CAD>`
  - `qtbot pause`
  - `qtbot resume`
  - `qtbot stop`
  - `qtbot status`
- Build control plane using atomic `runtime/control.json`.
- Run loop heartbeat with periodic persistence.

Exit criteria:
- Control commands work cross-terminal without force-kill.
- Pause/stop are graceful and state is persisted.

M1 implementation status:
- [x] Python package + CLI entrypoint created (`qtbot`).
- [x] Control plane implemented using atomic `runtime/control.json` updates.
- [x] Persistent state store implemented in `runtime/state.sqlite`.
- [x] Runner loop implemented with 60-second heartbeat persistence.
- [x] Cross-terminal lifecycle control validated (`start`, `pause`, `resume`, `stop`, `status`).

M1 validation evidence:
- Compile check: `python3 -m compileall src` passed.
- Live run command: `PYTHONPATH=src python3 -m qtbot start --budget 1000`.
- Control commands during run:
  - `PYTHONPATH=src python3 -m qtbot status`
  - `PYTHONPATH=src python3 -m qtbot pause`
  - `PYTHONPATH=src python3 -m qtbot resume`
  - `PYTHONPATH=src python3 -m qtbot stop`
- Persistence checks:
  - `runtime/control.json` updated atomically with latest command.
  - `runtime/state.sqlite` reflects final `STOPPED` status and loop count.
  - `runtime/logs/qtbot.log` contains heartbeat + lifecycle transitions.

### M2: NDAX API Integration
- Implement NDAX authentication and robust client wrapper.
- Pull balances and instrument metadata.
- Pull 1-minute candles for the tradable universe.
- Validate CAD market availability for configured symbols.

Exit criteria:
- Reliable market/balance retrieval with retry/backoff.
- Deterministic validated tradable set from NDAX.

M2 implementation status:
- [x] `.env` loading implemented for runtime and NDAX settings.
- [x] NDAX client wrapper implemented with retry/backoff and signed private request headers.
- [x] Public market metadata implemented via `GetInstruments`.
- [x] 1-minute candle retrieval implemented via `GetTickerHistory` (`Interval=60`).
- [x] Top-20-minus-locked CAD universe validation implemented against live NDAX instruments.
- [x] Authenticated balance flow implemented via `GetUserAccounts` + `GetAccountPositions`.
- [x] CLI integration commands added: `ndax-pairs`, `ndax-candles`, `ndax-balances`, `ndax-check`.

M2 validation evidence:
- Compile check: `python3 -m compileall src` passed.
- Public pair validation: `PYTHONPATH=src python3 -m qtbot ndax-pairs` returned live instrument set and tradable CAD pairs.
- Candle retrieval: `PYTHONPATH=src python3 -m qtbot ndax-candles --symbol SOLCAD --from-date 2026-03-04 --to-date 2026-03-05 --interval 60` returned 1m candles.
- End-to-end public check: `PYTHONPATH=src python3 -m qtbot ndax-check --skip-balances --symbol SOLCAD --from-date 2026-03-04 --to-date 2026-03-05 --interval 60` passed.
- Private credential gate: `PYTHONPATH=src python3 -m qtbot ndax-balances` fails with explicit missing-credential message when auth vars are absent.

### M3: Strategy Signals in Dry-Run
- Implement indicators on 1-minute candles:
  - EMA fast 60, EMA slow 360, ATR 60.
- Implement fixed entry/exit rules and deterministic candidate ranking.
- Emit `decisions.csv` with reasons each cycle.
- Keep order placement disabled via dry-run mode.

Exit criteria:
- Stable reproducible signals for identical inputs.
- No live orders while dry-run mode is enabled.

M3 implementation status:
- [x] Indicator module added with EMA and ATR calculations.
- [x] Signal module added with deterministic ENTER/EXIT/HOLD rule evaluation.
- [x] Runner integrated with strategy engine for per-cycle dry-run evaluation.
- [x] Decision logging implemented in append-only `runtime/logs/decisions.csv`.
- [x] Entry candidate ranking and per-cycle cap (`MAX_NEW_ENTRIES_PER_CYCLE`) enforced.
- [x] No execution path enabled from strategy loop (signals only).

M3 validation evidence:
- Compile check: `python3 -m compileall src` passed.
- Live loop validation: `QTBOT_CADENCE_SECONDS=5 PYTHONPATH=src python3 -m qtbot start --budget 1000` produced strategy cycle logs.
- Decision log validation: `runtime/logs/decisions.csv` created with required columns and signal rows.
- Graceful lifecycle validation retained: start/pause/resume/stop behavior still works after strategy integration.

### M4: Live Execution and Ledger
- Enable market order execution path.
- Confirm fills and compute effective fees per fill.
- Update cash/positions/PnL transactionally.
- Append all fills to `trades.csv`.

Exit criteria:
- Filled orders reconcile with ledger updates.
- Budget and lock constraints remain enforced.

M4 implementation status:
- [x] NDAX client extended with private order endpoints (`SendOrder`, `GetOrderStatus`) and fill polling.
- [x] Live execution engine added to process ENTER/EXIT decisions into market orders with per-order control-plane checks.
- [x] Ledger accounting module added with fee-aware buy/sell updates and realized PnL computation.
- [x] State persistence extended with transactional fill application and totals (`bot_cash_cad`, `realized_pnl_cad`, `fees_paid_cad`).
- [x] Trade logging implemented in append-only `runtime/logs/trades.csv`.
- [x] Runner integrated with execution stage after each strategy cycle.

M4 validation evidence:
- Compile check: `python3 -m compileall src` passed after M4 code changes.
- Lifecycle smoke run (execution disabled): `QTBOT_RUNTIME_DIR=runtime_m4_test QTBOT_CADENCE_SECONDS=2 PYTHONPATH=src python3 -m qtbot start --budget 1000` ran cleanly and stopped gracefully.
- State schema validation: `qtbot status` now reports `realized_pnl_cad` and `fees_paid_cad` fields.
- Decision stream validation: `runtime_m4_test/logs/decisions.csv` populated during loop.
- Dry-run safety validation: `runtime_m4_test/logs/trades.csv` remained absent while `QTBOT_ENABLE_LIVE_TRADING=false`.

### M5: Restart Reconciliation (NDAX as Truth)
- On startup, load prior state and query NDAX balances.
- Reconcile internal positions against NDAX holdings.
- Log reconciliation differences and actions.
- Block trading until reconciliation succeeds.

Exit criteria:
- Crash/restart does not corrupt trading state.
- Internal state converges to NDAX truth before new orders.

M5 implementation status:
- [x] Startup reconciliation service implemented (`StartupReconciler`) with NDAX balance sync and symbol-by-symbol quantity convergence.
- [x] State-store reconciliation primitives added (`reconcile_position`, `cap_bot_cash`, reconciliation event recording).
- [x] Runner startup now performs reconciliation before setting loop to RUN state.
- [x] Live mode startup is blocked when reconciliation/authentication fails.
- [x] Reconciliation audit trail added via `state_events` entries (`POSITION_RECONCILED`, `BOT_CASH_CAPPED`, `RECONCILIATION_COMPLETED` / `RECONCILIATION_SKIPPED`).

M5 validation evidence:
- Compile check: `python3 -m compileall src` passed after reconciliation changes.
- NDAX private/public health check: `PYTHONPATH=src python3 -m qtbot ndax-check --require-balances --symbol SOLCAD --from-date 2026-03-04 --to-date 2026-03-05 --interval 60` passed.
- Live-mode guarded startup test (no real orders): `QTBOT_RUNTIME_DIR=runtime_m5_check QTBOT_CADENCE_SECONDS=3 QTBOT_ENABLE_LIVE_TRADING=true QTBOT_MIN_ORDER_NOTIONAL_CAD=1000000000 PYTHONPATH=src python3 -m qtbot start --budget 1000` completed reconciliation and entered loop.
- Reconciliation persistence validated via `state_events`:
  - `STATUS_CHANGED|startup reconciliation started`
  - `BOT_CASH_CAPPED|...` (when NDAX CAD available was lower than internal cash)
  - `RECONCILIATION_COMPLETED|reconciliation_complete ...`

### M6: Go-Live Validation Gate
- Implement a pre-trade validation workflow that verifies:
  - credentials/auth,
  - NDAX API reachability,
  - CAD market coverage for universe,
  - candle warm-up sufficiency,
  - state DB read/write health,
  - control file integrity.
- Permit live order placement only if all checks pass.

Exit criteria:
- Failed preflight blocks live trading and reports exact failed checks.
- Successful preflight enables live execution path safely.

M6 implementation status:
- [x] Added `GoLivePreflight` module with structured check results and summary output.
- [x] Implemented all required checks:
  - `credentials_auth`
  - `ndax_api_reachability`
  - `cad_market_coverage`
  - `candle_warmup_sufficiency`
  - `state_db_health`
  - `control_file_integrity`
- [x] Added configurable warm-up coverage threshold (`QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE`, default `0.8`) to avoid blocking live mode on isolated sparse-candle symbols.
- [x] Integrated live startup gate in `runner` after reconciliation and before entering RUN mode.
- [x] Added explicit failure handling:
  - runner status transitions to `ERROR`,
  - startup is blocked in live mode,
  - failed checks are logged with exact details.
- [x] Added preflight event persistence (`GO_LIVE_PREFLIGHT_PASSED` / `GO_LIVE_PREFLIGHT_FAILED`).

M6 validation evidence:
- Compile check: `python3 -m compileall src` passed after preflight integration.
- Unit/integration suite: `PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"` passed (`75` tests).
- Coverage gate: `PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"` and `coverage report --show-missing --fail-under=85` passed at `86%`.
- Live-mode startup smoke run (order-notional guard enabled): `QTBOT_RUNTIME_DIR=runtime_m6_check QTBOT_CADENCE_SECONDS=2 QTBOT_ENABLE_LIVE_TRADING=true QTBOT_MIN_ORDER_NOTIONAL_CAD=1000000000 QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE=0.8 PYTHONPATH=src python3 -m qtbot start --budget 1000` completed preflight and entered loop.
- New M6 tests added:
  - `tests/test_preflight.py` covers pass/fail scenarios for all go-live checks.
  - `tests/test_runner_loop.py` includes live startup blocking when preflight fails.
- CI gate tightened: coverage fail-under raised from `84` to `85` in `.github/workflows/ci.yml`.

### M7: Risk Hardening
- Add production safeguards:
  - daily loss cap,
  - max slippage guard,
  - consecutive error kill-switch / auto-pause.
- Ensure all safeguards are deterministic and logged.

Exit criteria:
- Safety conditions block unsafe execution paths reliably.

M7 implementation status:
- [x] Added dedicated risk module (`RiskManager`) for deterministic risk enforcement.
- [x] Implemented daily loss cap guard:
  - reads daily realized PnL from persistent state,
  - auto-pauses trading when loss exceeds `QTBOT_DAILY_LOSS_CAP_CAD`.
- [x] Implemented slippage guard:
  - execution now computes realized slippage versus signal close for each fill,
  - breaches of `QTBOT_MAX_SLIPPAGE_PCT` halt further orders in-cycle and trigger auto-pause.
- [x] Implemented consecutive error kill-switch:
  - persistent consecutive error counter tracked in `risk_state`,
  - auto-pause when count reaches `QTBOT_CONSECUTIVE_ERROR_LIMIT`.
- [x] Added persistent risk state schema and events:
  - daily anchor rollover,
  - error increments/resets,
  - risk-triggered pause events.

M7 validation evidence:
- Compile check: `python3 -m compileall src` passed after M7 changes.
- Unit/integration suite: `PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"` passed (`82` tests).
- Coverage gate: `PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"` and `coverage report --show-missing --fail-under=85` passed at `86%`.
- New M7 test coverage:
  - `tests/test_risk.py` validates daily-loss, slippage, and error-limit pause actions.
  - `tests/test_execution.py` validates slippage breach handling and in-cycle order halt behavior.
  - `tests/test_state.py` validates risk-state persistence, daily anchor rollover, and error counter lifecycle.
  - `tests/test_runner_loop.py` validates pre-cycle risk pause behavior in runner loop.

### M8: Logging and Discord Alerting
- Finalize append-only runtime logs:
  - `runtime/logs/qtbot.log`
  - `runtime/logs/trades.csv`
  - `runtime/logs/decisions.csv`
- Integrate Discord alerts for:
  - stop/pause events,
  - repeated API failures,
  - reconciliation anomalies,
  - risk-triggered halts.

Exit criteria:
- Operator can diagnose critical issues from logs + alerts.

M8 implementation status:
- [x] Added Discord alert transport module (`DiscordAlerter`) with retry/backoff and non-blocking failure handling.
- [x] Added Discord runtime config:
  - `QTBOT_DISCORD_WEBHOOK_URL`
  - `QTBOT_DISCORD_TIMEOUT_SECONDS`
  - `QTBOT_DISCORD_MAX_RETRIES`
- [x] Integrated lifecycle alerts from runner for `PAUSE` and `STOP` transitions.
- [x] Integrated repeated-failure alerts for recurring NDAX/execution cycle failures.
- [x] Integrated reconciliation anomaly alerts when startup reconciliation mutates state/cash.
- [x] Integrated risk-halt alerts from `RiskManager` for slippage, daily-loss, and kill-switch pauses.
- [x] Kept runtime logs append-only and preserved decision/trade logging behavior.

M8 validation evidence:
- Compile check: `python3 -m compileall src` passed after M8 changes.
- Unit/integration suite: `PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"` passed (`89` tests).
- Coverage gate: `PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"` and `coverage report --show-missing --fail-under=85` passed at `86%`.
- Live-mode smoke run (alerts configured but no webhook required): `QTBOT_RUNTIME_DIR=runtime_m8_check QTBOT_CADENCE_SECONDS=2 QTBOT_ENABLE_LIVE_TRADING=true QTBOT_MIN_ORDER_NOTIONAL_CAD=1000000000 QTBOT_PREFLIGHT_MIN_WARMUP_COVERAGE=0.8 PYTHONPATH=src python3 -m qtbot start --budget 1000` started and stopped cleanly without runtime errors.
- New M8 tests added:
  - `tests/test_alerts.py` validates Discord alert delivery/retry/error handling.
  - `tests/test_reconciliation.py` validates anomaly alert emission on reconciliation changes.
  - `tests/test_risk.py` validates repeated-failure alert emission and risk pause behavior.
  - `tests/test_runner_loop.py` validates lifecycle alert path on stop transition.

### M9: Docker Production Packaging
- Build Python 3.11 Docker image.
- Add `docker-compose` runtime with persistent volume for `runtime/`.
- Provide environment template and startup commands.

Exit criteria:
- Bot is reproducible and operable in containerized production deployment.

M9 implementation status:
- [x] Added production Docker image definition in `Dockerfile` using Python 3.11 and `qtbot` entrypoint.
- [x] Added `.dockerignore` to prevent runtime artifacts, tests, VCS metadata, and local `.env` secrets from entering build context.
- [x] Added `docker-compose.yml` runtime service with:
  - persistent `./runtime:/app/runtime` volume mount,
  - `.env` loading,
  - startup command `start --budget ${QTBOT_START_BUDGET_CAD:-1000}`,
  - `init`, `restart`, `stop_signal`, and `stop_grace_period` operational settings.
- [x] Added compose startup-budget default to `.env.example` (`QTBOT_START_BUDGET_CAD=1000`).
- [x] Updated docs with container build/run and lifecycle control commands.
- [x] Added phase-appropriate test + CI coverage for packaging:
  - `tests/test_docker_packaging.py`
  - new `docker` job in `.github/workflows/ci.yml`.

M9 validation evidence:
- Compile check: `python3 -m compileall src` passed.
- Unit/integration suite: `PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"` passed (`92` tests).
- Coverage gate: `PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"` and `coverage report --show-missing --fail-under=85` passed at `86%`.
- Docker build: `docker build -t simplequanttrade:m9 .` succeeded.
- Containerized lifecycle command checks with persistent mount passed:
  - `docker run --rm -v "$PWD/runtime_m9_check:/app/runtime" simplequanttrade:m9 status`
  - `docker run --rm -v "$PWD/runtime_m9_check:/app/runtime" simplequanttrade:m9 pause`
  - `docker run --rm -v "$PWD/runtime_m9_check:/app/runtime" simplequanttrade:m9 resume`
  - `docker run --rm -v "$PWD/runtime_m9_check:/app/runtime" simplequanttrade:m9 stop`
- Container start/stop smoke run passed:
  - start container with `qtbot start --budget 1000`,
  - send external `stop` command from second container on shared runtime volume,
  - runner exited gracefully with persisted `STOPPED` status.
- Compose validation: `docker compose -f docker-compose.yml config` resolved successfully.

### M10: Staging Validation
- Run continuous dry-run in staging with live NDAX data.
- Exercise lifecycle commands and failure scenarios.
- Verify reconciliation and risk controls under simulated faults.

Exit criteria:
- Staging run proves operational stability before production.

M10 implementation status:
- [x] Added dedicated M10 staging orchestration module: `src/qtbot/staging.py`.
- [x] Added CLI command: `qtbot staging-validate` with controls:
  - `--budget`
  - `--cadence-seconds`
  - `--min-loops`
  - `--timeout-seconds`
  - `--offline-only` (CI/no-network mode)
- [x] Implemented full live staging workflow:
  - NDAX public health check (`ndax-pairs`),
  - continuous dry-run loop drill with lifecycle commands (`pause`, `resume`, `stop`) from separate CLI calls,
  - explicit failure-path drill (`ndax-check --symbol INVALIDCAD --skip-balances` expected failure),
  - reconciliation fault simulation (state mismatch + cash cap convergence checks),
  - risk fault simulation (daily-loss cap, consecutive-error kill-switch, slippage guard).
- [x] Added machine-readable staging report output:
  - stdout JSON summary
  - persisted file: `runtime/staging_validation/logs/staging_validation_report.json`.
- [x] Added/updated automated tests for M10:
  - new `tests/test_staging.py`
  - updates in `tests/test_cli.py` and `tests/test_cli_handlers.py`.
- [x] Added phase-appropriate CI workflow coverage:
  - offline staging validation command step in `.github/workflows/ci.yml`.

M10 validation evidence:
- Compile check: `python3 -m compileall src` passed.
- Unit/integration suite: `PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"` passed (`105` tests).
- Coverage gate: `PYTHONPATH=src coverage run --source=src/qtbot -m unittest discover -s tests -p "test_*.py"` and `coverage report --show-missing --fail-under=85` passed at `86%`.
- Full live staging run passed:
  - `PYTHONPATH=src python3 -m qtbot staging-validate --budget 1000 --cadence-seconds 3 --min-loops 2 --timeout-seconds 120`
  - result: `staging_validation_passed steps=5`
  - observed steps:
    - `public_ndax_health_check` passed (`instrument_count=87 tradable_count=16`)
    - `dry_run_lifecycle_drill` passed (`loop_count=4 decisions_rows=64`)
    - `cli_failure_scenario_invalid_symbol` passed (expected non-zero)
    - `reconciliation_fault_simulation` passed
    - `risk_fault_simulation` passed
- Offline staging run passed (for CI parity):
  - `PYTHONPATH=src python3 -m qtbot staging-validate --offline-only --budget 1000 --cadence-seconds 1 --min-loops 1 --timeout-seconds 30`
  - result: `staging_validation_passed steps=3`.

### M11: Production Cutover Checklist
- Confirm all acceptance criteria pass.
- Start with constrained budget and monitor first cycles.
- Verify first trade flow, ledger consistency, and alerting behavior.
- Define rollback/stop procedure and incident response path.

Exit criteria:
- Production launch checklist fully green and operator-ready.

## 4) CLI Lifecycle and Control-Plane Behavior

Lifecycle contract:
1. `start`:
   - initializes runtime artifacts,
   - sets/reads control command,
   - enters loop.
2. `pause`:
   - sets control to `PAUSE`,
   - runner stops new order placement and persists immediately.
3. `resume`:
   - sets control to `RUN`,
   - runner resumes normal evaluations.
4. `stop`:
   - sets control to `STOP`,
   - runner persists state and exits cleanly.
5. `status`:
   - reports mode, last loop timestamp, positions, cash, PnL, and health summary.

Control file requirements:
- Path: `runtime/control.json`
- Atomic write semantics: temp file + fsync + rename.
- Command values:
  - `RUN`
  - `PAUSE`
  - `STOP`

## 5) NDAX Integration and Reconciliation Plan

NDAX integration requirements:
- Signed/private API support for balances and orders.
- Public market data support for 1-minute candles and symbol metadata.
- Explicit timeout and retry policy with exponential backoff.
- Deterministic error classification and logging context.

Reconciliation flow:
1. Load local persisted state.
2. Query NDAX balances for all tracked symbols.
3. Compare internal vs NDAX quantities.
4. If mismatch, update internal positions to NDAX values.
5. Log reconciliation event with before/after details.
6. Only continue trading when reconciliation completes successfully.

## 6) Risk Controls and Observability

Risk controls (production baseline):
- BTC/ETH hard lock enforced in:
  - universe filtering,
  - signal generation,
  - execution preflight checks.
- Budget enforcement:
  - no order may exceed internal `bot_cash_cad`.
  - no order may exceed available exchange CAD:
    - `order_notional <= min(bot_cash_cad, ndax_available_cad)` with fee buffer.
- Daily loss cap:
  - pause execution when threshold is breached.
- Slippage guard:
  - reject orders when expected/realized slippage exceeds threshold.
- Consecutive error kill-switch:
  - auto-pause and alert when sustained API failures occur.

Observability:
- Human-readable operational log.
- Structured trade and decision CSV streams.
- Discord alerts for critical failures and lifecycle transitions.

## 7) Test Strategy and Acceptance Gates

Unit tests:
- Indicator math correctness.
- Signal rule determinism.
- Ledger fee/PnL accounting invariants.
- Risk control threshold triggers.
- Control-file atomicity behavior.

Integration tests:
- CLI lifecycle command behavior across terminals.
- NDAX client retry/backoff under transient failures.
- Reconciliation when local and NDAX state diverge.
- Dry-run to live gate behavior.
- Logging append-only guarantees.

Acceptance gates:
- M0-M11 exit criteria all passed.
- No prohibited asset trade path exists.
- Restart/pause/stop cannot corrupt state.
- Decisions and trades are fully traceable in logs.
- Every phase change must include or update automated tests for new/modified behavior.
- Every phase change must keep GitHub CI green (compile + test + coverage gates).

## 8) Rollout and Operations Runbook Checklist

Pre-deploy:
- Validate environment variables.
- Verify runtime volume permissions.
- Confirm Discord webhook connectivity.

Deploy:
- Launch container in dry-run mode.
- Verify loop heartbeat and decision logging.
- Perform control command drills (`pause`, `resume`, `stop`).

Go-live:
- Run full validation gate.
- Enable live execution only after all checks pass.
- Start with minimal budget and observe initial cycles.

Steady state:
- Monitor logs and alerts continuously.
- Apply pause/stop playbook on risk trigger or API instability.

Incident response:
- Stop trading gracefully.
- Capture state/log snapshot.
- Reconcile against NDAX balances before resuming.

## 9) Explicit Assumptions and Defaults

Assumptions:
- NDAX CAD spot markets exist for a subset of the configured top-20 universe.
- The exchange APIs support sufficient polling cadence for 60-second loops.

Defaults:
- Loop cadence: 60 seconds.
- Candle timeframe: 1 minute.
- EMA fast/slow: 60 / 360.
- ATR length: 60.
- ATR stop multiple: 2.5.
- Max hold duration: 48 hours.
- Cooldown after exit: 30 minutes.
- Max new entries per cycle: 3.
- Fee model: 0.4% per side.
- State store: `runtime/state.sqlite`.
- Go-live warmup coverage threshold: `0.8`.
- Daily loss cap (`QTBOT_DAILY_LOSS_CAP_CAD`): `250`.
- Max slippage guard (`QTBOT_MAX_SLIPPAGE_PCT`): `0.02`.
- Consecutive error kill-switch limit (`QTBOT_CONSECUTIVE_ERROR_LIMIT`): `3`.
- Discord alert timeout (`QTBOT_DISCORD_TIMEOUT_SECONDS`): `8`.
- Discord alert retries (`QTBOT_DISCORD_MAX_RETRIES`): `2`.
- Discord webhook (`QTBOT_DISCORD_WEBHOOK_URL`): unset by default (alerts disabled until configured).
- Docker compose startup budget (`QTBOT_START_BUDGET_CAD`): `1000`.

Documentation policy:
- NDAX naming remains canonical in all docs.
- No backward-compatibility aliasing for legacy exchange credential variables is documented.
