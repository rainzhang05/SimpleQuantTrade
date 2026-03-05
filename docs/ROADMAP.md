# SimpleQuantTrade Roadmap v2

This document is the authoritative system specification for SimpleQuantTrade.

Scope of this roadmap:
- Upgrade path from fixed-rule 1m strategy to ML-driven 15m strategy.
- Production-grade operational behavior on NDAX.
- Deterministic data, training, promotion, and live inference contracts.

Legacy 1m EMA/ATR behavior is archived in `docs/LEGACY_FIXED_RULE_ARCHIVE.md` and is not the active target architecture.

## 0) Non-Negotiable Invariants

### Exchange and account model
- Exchange is NDAX only.
- Trading is spot-only; no leverage, margin, borrowing, or shorting.
- Bot operates in CAD budget constraints through virtual sub-account accounting and exchange-balance reconciliation.
- NDAX remains source of truth for balances and holdings.

### Operational safety
- Control plane must support `start`, `pause`, `resume`, `stop`, and `status`.
- Startup must be reconciliation-first.
- Live order path must be blocked by preflight if safety checks fail.
- Risk controls remain mandatory:
- daily loss cap
- slippage guard
- consecutive-error kill-switch
- Runtime logs are append-only.
- Docker and staging validation remain first-class operational requirements.

### Execution model
- Market orders only.
- Live trading must remain gated behind explicit enable flag.
- Deterministic behavior is required in strategy, training, and runtime decision flow.

## 1) Target End-State Architecture

### 1.1 Data layer
- Canonical candle storage:
- `data/raw/ndax/15m/<SYMBOL>.parquet`
- Snapshot storage:
- `data/snapshots/<SNAPSHOT_ID>/`
- Runtime metadata and bot state:
- `runtime/state.sqlite`
- Runtime controls and logs:
- `runtime/control.json`
- `runtime/logs/`

### 1.2 Training layer (offline)
- Snapshot builder with deterministic dataset hashing.
- Deterministic feature pipeline.
- Walk-forward orchestrator.
- LightGBM trainers:
- per-coin model
- global pooled model
- Cost-aware evaluator aligned to live constraints.
- Promotion gate engine.
- Bundle publisher with integrity signature.

### 1.3 Inference layer (live)
- Load active promoted bundle from `models/bundles/LATEST`.
- Compute features on each closed 15m bar.
- Produce per-symbol prediction using global and optional per-coin model.
- Blend predictions deterministically and apply signal/risk/execution gates.

## 2) Model Bundle Contract

Promoted bundle path:
- `models/bundles/<BUNDLE_ID>/`

Required bundle files:
- `manifest.json`
- `global_model.txt`
- `per_coin/<SYMBOL>.txt` (optional per symbol)
- `feature_spec.json`
- `thresholds.json`
- `cost_model.json`
- `signature.sha256`

Optional bundle files:
- `calibration/<MODEL>.json` (for calibration artifacts)

`manifest.json` minimum fields:
- `bundle_id`
- `created_at_utc`
- `code_version`
- `config_hash`
- `dataset_hash`
- `feature_spec_hash`
- `training_window`
- `walk_forward`
- `lgbm_params`
- `metrics_summary`

Active-bundle pointer contract:
- Pointer lives at `models/bundles/LATEST`.
- Pointer update must be atomic.
- Rollback must be possible without deleting prior bundles.

## 3) Universe Policy (Universe V1)

Universe V1 is fixed and explicit:
- `BTC, ETH, XRP, SOL, ADA, DOGE, AVAX, LINK, DOT, BCH, LTC, XLM, TON, UNI, NEAR, ATOM, HBAR, AAVE, ALGO, APT, ARB, FET, FIL, ICP, INJ, OP, SUI, TIA, RUNE, SEI`

Runtime eligibility rules:
- Trade only symbols with active NDAX CAD spot pair.
- Missing CAD pair must be skipped and logged, not treated as fatal.

BTC/ETH lock policy:
- `QTBOT_BTC_ETH_LOCK=true` by default.
- When lock is enabled, BTC and ETH are excluded from entry/exit execution.
- BTC may still be used as market-context feature.
- Lock override requires explicit config change and successful staging validation.

## 4) Timeframe and Live Loop Behavior

### 4.1 Timeframe
- Feature generation, training labels, and inference use 15m bars.
- `QTBOT_TIMEFRAME=15m` is required default.

### 4.2 Loop cadence
- Runtime loop cadence stays 60 seconds for control-plane responsiveness.
- Trading decisions execute only on new closed 15m bars.

### 4.3 Bar-close processing
- Maintain `last_processed_bar_ts` per symbol.
- On each loop:
- fetch latest candle window
- detect newly closed 15m bar
- if no new close, skip prediction for symbol
- if new close exists, run feature -> inference -> gating -> decision

### 4.4 Frequency controls
- Cooldown after exit remains enabled, default 30 minutes.
- Trade on state transitions only (`ENTER` or `EXIT`), not every loop.
- `max_new_entries_per_cycle` remains enforced for operational burst control.

## 5) Data Acquisition and Snapshotting

### 5.1 Data source and ingestion
- Primary source is NDAX public candle endpoint.
- Backfill in chunks with retry/backoff and deterministic deduping.
- Timestamps must be normalized to exact 15m boundaries.

### 5.2 Storage policy
- Primary persisted market data format is Parquet.
- SQLite remains authoritative for runtime and training metadata tables.

### 5.3 Sealed snapshot contract
Snapshot ID must encode:
- `asof_bar_close_utc`
- `universe_hash`
- `schema_version`

Snapshot output must include:
- immutable manifest
- candle file manifest and checksums
- deterministic dataset hash over ordered rows

Reproducibility rule:
- Running snapshot builder with same `asof` and same source data must produce same `dataset_hash`.

## 6) Feature and Label Specification

### 6.1 Feature rules
- Historical-only features (no leakage).
- Deterministic windows and transforms.
- Deterministic missing-value handling.

### 6.2 Feature family targets
Target: 50 to 80 features from:
- returns/momentum
- volatility/range
- trend strength
- mean-reversion
- volume dynamics
- market context

Each bundle must carry exact feature contract in `feature_spec.json`:
- feature names
- windows
- transformations
- null/imputation policy
- required warmup bars

### 6.3 Labeling (V1)
- Binary classification target with cost-aware threshold.
- For each sample:
- `forward_return`
- `y`

Definition:
- `y = 1` if next-bar forward return is above cost-adjusted threshold, else `0`.
- Threshold must incorporate fees, slippage buffer, and safety margin.

## 7) Training and Walk-Forward Validation

### 7.1 Model family
- LightGBM only for V1.
- Per-coin model trained on symbol-local data.
- Global model trained on pooled universe data.

### 7.2 Determinism controls
- Fixed random seed.
- Fixed dataset snapshot.
- Fixed feature spec hash.
- Fixed fold boundaries.
- Hyperparameters logged in manifest.

### 7.3 Walk-forward default
Default scheme:
- train window: trailing 12 months
- validation window: next 1 month
- step: 1 month

Per fold outputs:
- fold boundaries
- prediction logs
- simulated trade logs
- metrics summary

## 8) Evaluator and Cost Model

Evaluator must align with live constraints:
- market orders only
- cooldown behavior
- max new entries per cycle
- CAD budget and exchange-availability checks
- risk halts (daily loss/slippage/error)

Default fee baseline:
- `QTBOT_FEE_PCT_PER_SIDE=0.002` (0.2% per side)

Slippage model:
- configurable (`QTBOT_SLIPPAGE_MODEL`)
- support fixed-bps baseline and volatility-aware variant

Minimum evaluation outputs:
- net PnL after fees/slippage
- max drawdown
- trade count
- win rate
- profit factor
- turnover
- fold stability metrics
- per-coin concentration and worst-coin analysis
- slippage stress test (+50% slippage)

## 9) Promotion Gates

Promotion command may only publish bundle if gates pass.

### 9.1 Hard gates
- fold count >= 12
- no data-quality/leakage violations
- aggregate net PnL > 0 after costs
- max drawdown <= 25%
- trade count >= 200
- slippage stress test remains net positive

### 9.2 Soft checks
- fold stability not concentrated in a single period
- worst-coin loss not catastrophic by configured threshold
- acceptable fold-variance profile

### 9.3 Promotion side effects
- write bundle at `models/bundles/<BUNDLE_ID>/`
- atomically update `models/bundles/LATEST`
- append promotion record to SQLite and runtime logs
- optional Discord promotion alert

## 10) Deterministic Blend and Signal Thresholds

Prediction blend:
- if per-coin model exists with sufficient coverage:
- `p = 0.7 * p_coin + 0.3 * p_global`
- otherwise:
- `p = p_global`

Default thresholds (conservative profile):
- `entry_threshold = 0.60`
- `exit_threshold = 0.48`

Thresholds and weight rules must be recorded in bundle `thresholds.json`.

## 11) Live Trading Logic (ML-Driven)

### 11.1 Entry conditions
Enter long only when all pass:
- symbol is allowed and NDAX CAD pair exists
- BTC/ETH lock policy respected
- no open position
- cooldown satisfied
- liquidity/spread proxy passes
- blended probability >= entry threshold
- optional edge-over-cost gate passes
- risk manager allows new entries

### 11.2 Exit conditions
Exit when any pass:
- blended probability <= exit threshold
- stop/trailing stop condition triggers
- risk halt or operator pause/stop

### 11.3 Position sizing
- Volatility-aware notional sizing.
- Cap by:
- max notional per trade
- max total exposure
- fee reserve buffer
- available CAD (`min(bot_cash_cad, ndax_available_cad)`)

### 11.4 Safe startup failure behavior
If bundle integrity or model preflight fails:
- block order placement
- run observe-only mode (control plane + logs active)

## 12) Preflight Extensions (ML Readiness)

In addition to existing operational checks, live preflight must validate:
- active bundle exists and signature is valid
- runtime code supports bundle feature spec version
- required candle warmup coverage exists for active universe
- bar-close alignment and clock sanity

Any failed safety-critical preflight check blocks live order path.

## 13) CLI Contract

### 13.1 Existing commands retained
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

### 13.2 New command surface (v2 target)
Data:
- `qtbot data-backfill --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m`
- `qtbot data-status`

Training/evaluation:
- `qtbot build-snapshot --asof YYYY-MM-DDTHH:MMZ`
- `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1`
- `qtbot eval --run <RUN_ID>`
- `qtbot promote --run <RUN_ID>`

Model/runtime:
- `qtbot model-status`
- `qtbot predict --symbol <SYM> --at latest`
- `qtbot set-active-bundle <BUNDLE_ID>`

All commands must operate in local runtime and Docker (`docker compose exec`).

## 14) SQLite Schema Extensions

Existing runtime tables remain; add:
- `training_runs`
- `training_folds`
- `fold_metrics`
- `promotions`
- `data_coverage`

Schema requirements:
- append-only or versioned writes
- atomic transaction boundaries
- migration-safe evolution

## 15) Data Quality Gates

Training and live preflight must enforce:
- monotonic timestamps per symbol
- no duplicate candle primary keys
- gap detection with configured thresholds
- minimum history coverage for feature warmup
- exact 15m boundary normalization
- consistent UTC handling

Hard-gate failures must abort training and block live order path.

## 16) Config Surface and Defaults

New/standardized settings:
- `QTBOT_TIMEFRAME=15m`
- `QTBOT_UNIVERSE=V1`
- `QTBOT_BTC_ETH_LOCK=true`
- `QTBOT_MODEL_BUNDLE_PATH=models/bundles/LATEST`
- `QTBOT_ENTRY_THRESHOLD=0.60`
- `QTBOT_EXIT_THRESHOLD=0.48`
- `QTBOT_SLIPPAGE_MODEL=fixed_bps`
- `QTBOT_FEE_PCT_PER_SIDE=0.002`
- `QTBOT_FEATURE_SPEC_VERSION=v1`

Keep existing runtime and risk settings unless explicitly superseded.
All defaults must be defined in one config module.

## 17) Milestones and Acceptance

### Milestone A: 15m data layer
- Deliver backfill and `data-status` with coverage/gap reporting.
- Acceptance: deterministic candle keys, best-effort full backfill with per-symbol status.

### Milestone B: feature pipeline
- Deliver deterministic ~60-feature generator and feature spec output.
- Acceptance: deterministic reruns and valid warmup/no steady-state NaNs.

### Milestone C: snapshot builder
- Deliver sealed snapshot manifest and dataset hash.
- Acceptance: identical hash for identical input/as-of.

### Milestone D: walk-forward orchestration
- Deliver fold generation/slicing and run tracking.
- Acceptance: reproducible folds and logged boundaries.

### Milestone E: LightGBM trainer
- Deliver per-coin and global trainers with fixed baseline params.
- Acceptance: deterministic artifacts given fixed seed/snapshot.

### Milestone F: evaluator
- Deliver cost-aware simulation and fold/aggregate metrics.
- Acceptance: fees/slippage enforced and output stable.

### Milestone G: promotion and bundle publisher
- Deliver gate engine, publisher, and atomic LATEST update.
- Acceptance: failed gates block promotion with explicit reasons.

### Milestone H: live inference integration
- Deliver bar-close inference, deterministic blend, and ML signal gating.
- Acceptance: observe-only and live paths both deterministic and safety-compliant.

### Milestone I: staging and cutover upgrade
- Deliver end-to-end ML staging validation and cutover checks.
- Acceptance: data->feature->inference->decision path validated before live launch.

## 18) Done Definition

Upgrade is done when all are true:
1. 15m universe backfill and coverage reporting works.
2. Snapshot hash is reproducible.
3. Walk-forward training/evaluation outputs deterministic fold metrics.
4. Promotion gates deterministically accept/reject runs.
5. Live runtime loads active bundle and makes deterministic 15m bar-close decisions.
6. Existing operational safety guarantees remain intact.

## 19) Rollback and Safe Operation

Rollback requirements:
- keep last N promoted bundles
- `set-active-bundle` requires trading paused
- rollback updates pointer atomically and logs event

Safe-mode requirements:
- if bundle integrity/readiness fails, switch to observe-only mode
- no new live orders while in observe-only
- control plane and logging must continue

## 20) Documentation Policy

- `docs/ROADMAP.md` is primary specification.
- `docs/PLAN.md` is implementation sequencing and delivery plan.
- `docs/PRODUCTION_RUNBOOK.md` is operations procedure.
- `docs/LEGACY_FIXED_RULE_ARCHIVE.md` is historical reference only.

All architecture/interface changes must update these documents in the same change set.
