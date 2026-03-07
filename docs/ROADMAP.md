# SimpleQuantTrade Roadmap v3 (Authoritative)

This roadmap is the canonical architecture and contract for the active system.

Active direction:
- NDAX execution venue (spot, CAD budget safety)
- 15m ML-oriented data pipeline
- multi-source training dataset (`ndax + kraken primary + binance fallback`) with deterministic CAD normalization
- conservative operational safety shell retained from legacy runtime

Legacy fixed-rule 1m EMA/ATR behavior is archived at:
- `docs/LEGACY_FIXED_RULE_ARCHIVE.md`

## 0) Non-Negotiable Invariants

### Exchange and account model
- Execution venue is NDAX only.
- Spot-only trading; no margin, leverage, shorting, or borrowing.
- CAD budget safety and reconciliation-first startup remain mandatory.
- NDAX balances remain source of truth.

### Operational safety
- Control plane remains mandatory: `start`, `pause`, `resume`, `stop`, `status`.
- Live order path is blocked by preflight on safety-critical failure.
- Risk halts remain mandatory:
  - daily loss cap
  - slippage guard
  - consecutive-error kill-switch
- Logs remain append-only.
- Docker and staging/cutover validation remain mandatory before production activation.

### Execution model
- Market orders only.
- Live trading remains explicitly gated.
- Deterministic behavior is required in data, calibration, training, and runtime decision flow.

## 1) End-State Architecture

### 1.1 Data layer (implemented core)
- Raw NDAX candles: `data/raw/ndax/15m/<SYMBOL>.parquet`
- Raw Kraken candles: `data/raw/kraken/15m/<PAIR>.parquet`
  - imported deterministically from local Kraken trade-history CSV plus paged REST trade top-up after archive end
  - exchange/ingest outage windows are sealed deterministically with carry-forward 15m rows (`source=kraken_gap_fill`, zero volume)
- Raw Binance candles: `data/raw/binance/15m/<SYMBOL>USDT.parquet`
  - exchange-wide outage windows are sealed deterministically with carry-forward 15m rows (`source=binance_gap_fill`, zero volume)
- Selected external context bars: `data/raw/external/15m/<SYMBOL>.parquet`
  - primary external-context cache for the deterministic preferred external source per symbol
- External-source selection manifest: `data/raw/external/15m/selection.json`
- Combined CAD dataset: `data/combined/15m/<SYMBOL>.parquet`
- Runtime metadata/state DB: `runtime/state.sqlite`
- Runtime controls/logs: `runtime/control.json`, `runtime/logs/`
- `data/` is a local-only artifact tree and is not version-controlled; each machine must build or refresh it locally through the CLI pipeline.

### 1.2 Training layer (Phase 7 promotion/bundles implemented; runtime inference remains phased)
- Sealed snapshot builder with deterministic dataset hash.
- Deterministic feature pipeline (50-80 features).
- Walk-forward orchestrator.
- LightGBM global + per-coin trainers.
- Cost-aware evaluator + deterministic coin attribution.
- Promotion gates + bundle publisher with integrity signature.

### 1.3 Inference layer (phased)
- Runtime loads active promoted bundle from `models/bundles/LATEST`.
- Predictions on closed 15m bars.
- Deterministic blend of per-coin/global probabilities.
- Signal gated by risk, liquidity, cooldown, and exposure limits.

## 2) Model Bundle Contract (Deployment Artifact)

Bundle path:
- `models/bundles/<BUNDLE_ID>/`

Required files:
- `manifest.json`
- `global_model.txt`
- `per_coin/<SYMBOL>.txt` (optional if insufficient symbol data)
- `feature_spec.json`
- `thresholds.json`
- `cost_model.json`
- `signature.sha256`

Optional files:
- `calibration/<MODEL>.json`

`manifest.json` minimum fields:
- `bundle_id`, `created_at_utc`, `code_version`, `config_hash`
- `dataset_hash`, `feature_spec_hash`
- `training_window`, `walk_forward`, `lgbm_params`
- `metrics_summary`
- `run_id`, `primary_scenario`
- `included_per_coin_symbols`, `omitted_symbols`

Active pointer contract:
- `models/bundles/LATEST` is updated atomically.
- Rollback via explicit active-bundle switch; old bundles are retained.

## 3) Universe V1 Policy

Universe V1 (fixed 27 tickers):
- `BTC, ETH, XRP, SOL, ADA, DOGE, AVAX, LINK, DOT, LTC, XLM, TON, UNI, NEAR, ATOM, HBAR, AAVE, ALGO, APT, ARB, FET, FIL, ICP, INJ, OP, SUI, SEI`

Eligibility rules:
- Runtime trading only on symbols with active NDAX CAD spot pairs.
- Missing CAD pair is skipped and logged, not fatal.

BTC/ETH lock policy:
- Default `QTBOT_BTC_ETH_LOCK=true`.
- Explicit override requires successful staging validation.

## 4) Timeframe and Loop Contract

- Training/features/inference timeframe is 15m.
- Runtime loop may remain 60s for responsiveness.
- Decisions execute only on new closed 15m bars.
- Trade frequency controls remain:
  - cooldown after exits
  - state-transition entries/exits only
  - `max_new_entries_per_cycle`

## 5) Multi-Source Data System (Implemented)

### 5.1 Sources
- NDAX public candles (CAD pairs, execution-aligned data).
- Kraken historical trade archive + REST trade top-up for the preferred external pair per symbol.
- Binance spot candles (`<COIN>USDT`) retained as deterministic fallback when Kraken is unavailable or not selected.

### 5.2 Deterministic ingestion behavior
- Boundary-safe chunking.
- 15m timestamp canonicalization.
- Deduplication by timestamp key.
- Resume via missing-window detection (not only high-water mark).
- Idempotent reruns.

### 5.3 External -> CAD normalization
For overlap timestamps where NDAX and the selected external source are both present:
- `R_t = NDAX_close_t / EXTERNAL_close_t`
- If NDAX `USDTCAD` exists, compute basis proxy `B_t = R_t / USDTCAD_t`
- Use robust monthly medians (clipped) for ratio/basis estimation.
- Conversion factor selection per timestamp:
  - preferred: `USDTCAD_t * basis_month`
  - fallback: per-symbol monthly ratio estimate
  - fallback: per-symbol global ratio estimate
  - fallback: universe monthly ratio estimate
  - fallback: universe global ratio estimate
  - otherwise missing

### 5.4 Combined precedence
Per symbol/timestamp:
1. Use NDAX row when present.
2. Else use normalized CAD row from the selected external source.
3. If the selected external source is missing a timestamp, use normalized Binance fallback for that timestamp when available.
4. Else mark missing.

Operational interpretation:
- raw NDAX history may remain empty or internally gapped for some listed instruments when NDAX does not return candles for those windows.
- raw Kraken history may lag after the archive end because REST trade top-up is slower than archive import; combined completeness is still preserved by deterministic Binance timestamp fallback where Kraken has not been topped up yet.
- training and readiness gates evaluate the `combined` dataset contract, not raw NDAX completeness.
- combined completeness is achieved by deterministic normalized external substitution wherever NDAX bars are absent.

Combined source tagging:
- regular normalized external rows use `source=synthetic`
- normalized gap-repair rows use `source=synthetic_gap_fill`

### 5.5 Combined health contract
Combined dataset is healthy when:
- `duplicate_count = 0`
- `misaligned_count = 0`
- `gap_count <= QTBOT_COMBINED_MAX_GAP_COUNT` (default `0`)
- `coverage_pct >= QTBOT_COMBINED_MIN_COVERAGE` (default `0.999`)

## 6) Calibration and Synthetic Weighting (Implemented)

### 6.1 Overlap quality metrics
Computed per symbol and month:
- `median_ape_close`
- `median_abs_ret_err`
- `ret_corr`
- `direction_match`
- `basis_median`, `basis_mad`
- overlap row count

### 6.2 Quality score
Normalized quality score `Q in [0,1]`:
- price error component 35%
- return error component 25%
- return correlation component 20%
- direction match component 10%
- basis stability component 10%

### 6.3 Weight computation
- `w_q = clamp(min + (max-min)*Q, min, max)`
- `w_bt`: deterministic grid-search proxy over overlap return behavior
- `w_raw = 0.60*w_q + 0.40*w_bt`
- overlap shrinkage:
  - `k = overlap_rows / (overlap_rows + 5000)`
  - `w_final = k*w_raw + (1-k)*w_global`
- guardrail:
  - if quality fails or overlap below threshold, force `w_final=0.25`

Refresh cadence:
- default monthly (`QTBOT_SYNTH_WEIGHT_REFRESH=monthly`).

Eligibility metadata persisted per symbol-month:
- `supervised_eligible`
- `eligibility_mode` in `{direct, carry_backward, carry_forward, blocked}`
- `anchor_month`

## 7) Training Label and Feature Contracts (Phased)

### 7.1 Features (target)
- 50-80 deterministic historical-only features.
- Families: momentum, volatility, trend, mean-reversion, volume, market context.
- Bundle carries exact `feature_spec.json`.

### 7.2 Label (target V1)
- Binary cost-aware next-bar classification.
- Persist `forward_return` and `y`.
- `y=1` only when forward return exceeds cost-adjusted threshold.
- snapshot experiments may override the label horizon with `--label-horizon-bars <N>`; labels then use the next contiguous `N` closed 15m bars.

### 7.3 Snapshot contract (implemented for Phase 5)
Snapshot path:
- `data/snapshots/<SNAPSHOT_ID>/`

Required files:
- `manifest.json`
- `rows.parquet`

Closed-bar cutoff rule:
- `build-snapshot --asof <ISO_TIME>` includes only bars with `timestamp_ms < floor(asof, 15m)`.
- labels are emitted only when the next contiguous 15m bar is also closed.
- `build-snapshot --label-horizon-bars <N>` requires the next contiguous `N` closed 15m bars.
- `build-snapshot --exclude-symbols BTC,ETHCAD,...` excludes those symbols from the sealed experiment snapshot and records the exclusion list in the manifest.

Row contract in `rows.parquet`:
- market columns retained: `open`, `high`, `low`, `close`, `volume`, `inside_bid`, `inside_ask`
- row metadata: `symbol`, `timestamp_ms`, `next_timestamp_ms`, `source`, `effective_month`
- quality/weight metadata: `quality_pass`, `weight_method_version`, `effective_monthly_weight`, `supervised_row_weight`
- supervision fields: `label_available`, `row_status`, `next_close`, `forward_return`, `y`

Phase 5 weighting rules:
- default training dataset source is `combined` via `QTBOT_DATASET_MODE=combined`
- NDAX rows use `effective_monthly_weight=1.0`
- synthetic rows use monthly `w_final`
- synthetic supervision is gated by `synthetic_weights.supervised_eligible`, not raw `quality_pass`
- direct qualifying months use `eligibility_mode=direct`
- zero-overlap months before the first qualifying same-symbol month may reuse that future anchor as `eligibility_mode=carry_backward`
- zero-overlap months after a qualifying same-symbol month may reuse the nearest prior anchor as `eligibility_mode=carry_forward`
- synthetic rows with no qualifying anchor remain `row_status=continuity_only`
- `synthetic_gap_fill` rows, and rows whose next bar is `synthetic_gap_fill`, remain continuity-only
- `supervised_row_weight=0.0` when `label_available=false`

Snapshot hash contract:
- dataset hash is computed over snapshot row order, source tags, weights, and label fields
- dataset hash also includes the snapshot experiment definition (`label_horizon_bars`, `excluded_symbols`)
- row order is deterministic: Universe V1 symbol order, then ascending `timestamp_ms`
- manifest includes per-symbol parity checks and source-mix audits

## 8) Training, Evaluation, Attribution, Promotion

### 8.1 Models
- LightGBM global + per-coin.
- Determinism controls:
  - fixed seed
  - fixed snapshot hash
  - fixed feature spec hash
  - fixed fold boundaries

### 8.2 Walk-forward defaults
- Train: 12 months
- Validate: 1 month
- Step: 1 month
- CLI:
  - `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1`
  - `qtbot eval --run <RUN_ID>`
  - `qtbot attribution --run <RUN_ID>`
  - `qtbot backtest --run <RUN_ID>`
- Scenario execution rule:
  - `weighted_combined` is required and must train on every built fold
  - `ndax_only` is best-effort and may be partial/skipped on folds that lack NDAX train/validate rows or both classes
  - skipped NDAX benchmark folds are recorded in `training_runs.scenario_status`

Training artifact layout:
- `runtime/research/training/<RUN_ID>/manifest.json`
- `runtime/research/training/<RUN_ID>/feature_spec.json`
- `runtime/research/training/<RUN_ID>/folds.json`
- `runtime/research/training/<RUN_ID>/metrics.json`
- `runtime/research/training/<RUN_ID>/coin_attribution.json`
- `runtime/research/training/<RUN_ID>/coin_attribution.md`
- `runtime/research/training/<RUN_ID>/backtests/<BACKTEST_ID>/summary.json`
- `runtime/research/training/<RUN_ID>/backtests/<BACKTEST_ID>/trades.parquet`
- `runtime/research/training/<RUN_ID>/predictions/fold_<NN>/<scenario>.parquet`
- `runtime/research/training/<RUN_ID>/models/global/<scenario>/fold_<NN>.txt`
- `runtime/research/training/<RUN_ID>/models/per_coin/<SYMBOL>/<scenario>/fold_<NN>.txt`

Feature contract (implemented v1):
- fixed 56-feature historical-only spec from combined rows plus raw NDAX/external context
- missing raw-source context is imputed to `0.0` and paired with availability flags
- exact feature schema is versioned in `feature_spec.json`

### 8.3 Cost model defaults
- Fee baseline: `QTBOT_FEE_PCT_PER_SIDE=0.002` (0.2% per side)
- Slippage model configurable.
- Research portfolio backtest defaults:
  - `QTBOT_BACKTEST_INITIAL_CAPITAL_CAD=10000`
  - `QTBOT_BACKTEST_MAX_ACTIVE_POSITIONS=3`
  - `QTBOT_BACKTEST_POSITION_FRACTION=0.25`
  - `QTBOT_BACKTEST_SLIPPAGE_PCT_PER_SIDE=0.0`

### 8.3a Portfolio backtest contract
- `qtbot backtest --run <RUN_ID>` replays persisted validation predictions as a cash-constrained long-only research portfolio.
- Default scenario is the accepted promotion scenario when present; otherwise it falls back to the training run `primary_scenario`.
- One open position per symbol, fixed fraction sizing, maximum concurrent positions, fee + optional slippage on both sides.
- Positions hold for exactly the snapshot label horizon and do not overlap on the same symbol.
- Backtest outputs:
  - final equity
  - total and annualized return percentages
  - max drawdown percentage
  - trade count / win rate
  - source mix PnL
  - symbol-level PnL
  - monthly returns

### 8.4 Conservative promotion defaults
- `entry_threshold=0.60`
- `exit_threshold=0.48`
- `min_folds=12`
- `min_trades=200`
- `max_drawdown=25%`
- `min_conversion_pass_rate=0.60`
- `slippage_stress_pct_per_side=0.001`
- slippage stress must remain net positive

### 8.5 Promotion/bundle contract (implemented)
- Promotion CLI:
  - `qtbot promote --run <RUN_ID>`
  - `qtbot model-status`
  - `qtbot set-active-bundle <BUNDLE_ID>`
- Evaluation still records a research `primary_scenario`, but promotion independently chooses the best scenario that passes hard gates under the configured promotion thresholds.
- Promotion gate metrics are recomputed from persisted prediction artifacts at `QTBOT_PROMOTION_ENTRY_THRESHOLD`, not the evaluator's default research threshold.
- Bundle models are refit on all trainable rows allowed by the promoted scenario; fold models are research artifacts and are not published directly.
- Per-coin models are optional:
  - passing global metrics can still promote a bundle
  - weak per-coin models are omitted individually based on attribution + promotion gates
- conversion pass-rate for synthetic scenarios is measured from the current `synthetic_weights.supervised_eligible` state for snapshot symbols; `ndax_only` bundles do not apply the synthetic conversion gate
- Attribution bad-kind precedence for per-coin omission:
  - `sparse_history`
  - `cost_fragility`
  - `synthetic_fragility`
  - `weak_signal`

## 9) Live Inference and Trading Logic (Phased)

Deterministic blend rule:
- if per-coin coverage sufficient:
  - `p = 0.7 * p_coin + 0.3 * p_global`
- else:
  - `p = p_global`

Entry requires all gates:
- symbol allowed and CAD pair exists
- BTC/ETH lock respected
- no open position
- cooldown satisfied
- liquidity/spread checks pass
- `p >= entry_threshold`
- risk manager allows new entry

Exit when any gate triggers:
- `p <= exit_threshold`
- stop/trailing stop
- risk halt or operator pause/stop

## 10) Safety, Preflight, Staging, Cutover

### 10.1 Existing safety shell retained
- reconciliation-first startup
- go-live preflight gate
- risk auto-pause controls
- append-only logs and optional alerts

### 10.2 ML/data readiness extensions
Preflight/staging/cutover must include:
- combined dataset freshness and coverage
- weight calibration presence/recency
- bundle integrity/compatibility checks (when model runtime is active)

Bundle integrity failure behavior:
- block live order path
- switch to observe-only mode

## 11) CLI Contract

### 11.1 Existing lifecycle/operator commands retained
- `start`, `pause`, `resume`, `stop`, `status`
- `ndax-pairs`, `ndax-candles`, `ndax-balances`, `ndax-check`
- `staging-validate`, `cutover-checklist`

### 11.2 Data commands (implemented)
- `qtbot data-backfill --from YYYY-MM-DD|earliest --to YYYY-MM-DD --timeframe 15m --sources ndax,kraken,binance`
- `qtbot data-status --timeframe 15m --dataset ndax|kraken|binance|combined|all`
- `qtbot data-build-combined --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m`
- `qtbot data-calibrate-weights --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m --refresh monthly`
- `qtbot data-weight-status --timeframe 15m`

Defaults:
- `data-backfill` sources default to `ndax,kraken,binance`
- `data-status` dataset default to `combined`

### 11.3 Snapshot command (implemented)
- `qtbot build-snapshot --asof <ISO_TIME> --timeframe 15m [--label-horizon-bars N] [--exclude-symbols BTC,ETHCAD,...]`

Defaults:
- dataset source comes from `QTBOT_DATASET_MODE` (default `combined`)
- if `<ISO_TIME>` omits an offset, UTC is assumed

### 11.4 Training/model commands
- `qtbot train --snapshot <SNAPSHOT_ID> --folds <N> --universe V1`
- `qtbot eval --run <RUN_ID>`
- `qtbot backtest --run <RUN_ID>`
- `qtbot attribution --run <RUN_ID>`
- `qtbot promote --run <RUN_ID>`
- `qtbot model-status`
- `qtbot set-active-bundle <BUNDLE_ID>`

### 11.5 Planned runtime inference command
- `qtbot predict --symbol <SYM> --at latest`

## 12) SQLite Schema Contract

Active multi-source tables:
- `data_sync_checkpoints`
- `data_coverage_v2`
- `conversion_quality`
- `synthetic_weights`
- `combined_builds`

Retained runtime tables:
- `bot_state`, `risk_state`, `positions`, `state_events`, `data_coverage`

Active ML training tables:
- `training_runs`, `training_folds`, `fold_metrics`

Active promotion/runtime tables:
- `promotions`

Schema rules:
- append-only or explicit upsert semantics
- deterministic writes
- migration-safe evolution

## 13) Config Surface

Required data variables:
- `QTBOT_DATA_SOURCES=ndax,kraken,binance`
- `QTBOT_DATASET_MODE=combined`
- `QTBOT_BINANCE_BASE_URL=https://api.binance.com`
- `QTBOT_KRAKEN_BASE_URL=https://api.kraken.com`
- `QTBOT_KRAKEN_ARCHIVE_DIR=data/kraken`
- `QTBOT_EXTERNAL_SOURCE_PRIORITY=kraken,binance`
- `QTBOT_BINANCE_QUOTE=USDT`
- `QTBOT_BRIDGE_FX_SYMBOL=USDTCAD`
- `QTBOT_SYNTH_WEIGHT_MIN=0.20`
- `QTBOT_SYNTH_WEIGHT_MAX=0.80`
- `QTBOT_SYNTH_WEIGHT_REFRESH=monthly`
- `QTBOT_SYNTH_WEIGHT_DEFAULT=0.60`
- `QTBOT_MIN_OVERLAP_ROWS_FOR_WEIGHT=1000`
- `QTBOT_CONVERSION_MAX_MEDIAN_APE=0.015`
- `QTBOT_COMBINED_MAX_GAP_COUNT=0`
- `QTBOT_COMBINED_MIN_COVERAGE=0.999`
- `QTBOT_TRAIN_SEED=42`
- `QTBOT_TRAIN_WINDOW_MONTHS=12`
- `QTBOT_VALID_WINDOW_MONTHS=1`
- `QTBOT_TRAIN_STEP_MONTHS=1`
- `QTBOT_FEE_PCT_PER_SIDE` defaults to `QTBOT_TAKER_FEE_RATE` when unset
- `QTBOT_PROMOTION_MIN_FOLDS=12`
- `QTBOT_PROMOTION_MIN_TRADES=200`
- `QTBOT_PROMOTION_MAX_DRAWDOWN=0.25`
- `QTBOT_PROMOTION_MIN_CONVERSION_PASS_RATE=0.60`
- `QTBOT_PROMOTION_SLIPPAGE_STRESS_PCT_PER_SIDE=0.001`
- `QTBOT_PROMOTION_ENTRY_THRESHOLD=0.60`
- `QTBOT_PROMOTION_EXIT_THRESHOLD=0.48`

Core trading/risk defaults remain in `src/qtbot/config.py`.

## 14) Milestones (A-I)

A. 15m NDAX hardening
- boundary-safe resume/idempotent retrieval

B. Binance raw pipeline
- resumable USDT kline ingestion

C. Combined builder
- deterministic normalized CAD merge with precedence

D. Calibration engine
- overlap quality metrics + monthly synthetic weights

E. Training integration
- weighted synthetic rows in model dataset

F. Cost-aware evaluator
- live-aligned constraints and sensitivity metrics

G. Promotion + bundle publisher
- deterministic gates, integrity signatures, atomic `LATEST`

H. Live inference integration
- bar-close prediction path + blend + gating

I. Staging/cutover ML readiness
- evidence-driven launch and rollback safety

## 15) Current Program Status and Next-Step Gates

Current status:
- Milestones A-G are implemented (data ingestion, combined build, calibration weighting, weighted snapshot integration, walk-forward training/evaluation, attribution, promotion, signed bundle publishing).
- Milestones H-I remain the official implementation runway to production ML.
- The immediate next milestone is **H (Live inference integration)**.

Mandatory gate sequence from current state to production:
1. **H: Live inference integration**
   - Entry: at least one promoted bundle and passing model integrity checks.
   - Exit: bar-close deterministic predictions with observe-only safety fallback.
2. **I: Staging/cutover readiness**
   - Entry: live inference path stable in observe-only.
   - Exit: staging + cutover reports pass with rollback drill evidence.

Production activation rule:
- Live ML order placement is allowed only after all milestone gates above pass in order and evidence is recorded in runtime logs/state.

## 16) Done Definition

System upgrade is complete when all are true:
1. Dual-source 15m backfill is resumable and idempotent.
2. Combined dataset build is deterministic and coverage-contract compliant.
3. Monthly calibration produces reproducible quality metrics and weights.
4. Training/eval/promotion flow is deterministic and gated.
5. Runtime loads active promoted bundle and decides on 15m bar closes.
6. Preflight/staging/cutover safety guarantees remain intact.
