# SimpleQuantTrade Roadmap v3 (Authoritative)

This roadmap is the canonical architecture and contract for the active system.

Active direction:
- NDAX execution venue (spot, CAD budget safety)
- 15m ML-oriented data pipeline
- dual-source training dataset (`ndax + binance`) with deterministic CAD normalization
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
- Raw Binance candles: `data/raw/binance/15m/<SYMBOL>USDT.parquet`
  - exchange-wide outage windows are sealed deterministically with carry-forward 15m rows (`source=binance_gap_fill`, zero volume)
- Combined CAD dataset: `data/combined/15m/<SYMBOL>.parquet`
- Runtime metadata/state DB: `runtime/state.sqlite`
- Runtime controls/logs: `runtime/control.json`, `runtime/logs/`
- `data/` is a local-only artifact tree and is not version-controlled; each machine must build or refresh it locally through the CLI pipeline.

### 1.2 Training layer (Phase 5 snapshot implemented; later phases remain)
- Sealed snapshot builder with deterministic dataset hash.
- Deterministic feature pipeline (50-80 features).
- Walk-forward orchestrator.
- LightGBM global + per-coin trainers.
- Cost-aware evaluator + promotion gates.
- Bundle publisher with integrity signature.

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

Active pointer contract:
- `models/bundles/LATEST` is updated atomically.
- Rollback via explicit active-bundle switch; old bundles are retained.

## 3) Universe V1 Policy

Universe V1 (fixed 30 tickers):
- `BTC, ETH, XRP, SOL, ADA, DOGE, AVAX, LINK, DOT, BCH, LTC, XLM, TON, UNI, NEAR, ATOM, HBAR, AAVE, ALGO, APT, ARB, FET, FIL, ICP, INJ, OP, SUI, TIA, RUNE, SEI`

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

## 5) Dual-Source Data System (Implemented)

### 5.1 Sources
- NDAX public candles (CAD pairs, execution-aligned data).
- Binance spot candles (`<COIN>USDT`) for historical structure and continuity.

### 5.2 Deterministic ingestion behavior
- Boundary-safe chunking.
- 15m timestamp canonicalization.
- Deduplication by timestamp key.
- Resume via missing-window detection (not only high-water mark).
- Idempotent reruns.

### 5.3 Binance -> CAD normalization
For overlap timestamps where NDAX and Binance are both present:
- `R_t = NDAX_close_t / BINANCE_close_t`
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
2. Else use normalized Binance CAD row.
3. Else mark missing.

Operational interpretation:
- raw NDAX history may remain empty or internally gapped for some listed instruments when NDAX does not return candles for those windows.
- training and readiness gates evaluate the `combined` dataset contract, not raw NDAX completeness.
- combined completeness is achieved by deterministic normalized-Binance substitution wherever NDAX bars are absent.

Combined source tagging:
- regular normalized Binance rows use `source=synthetic`
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

## 7) Training Label and Feature Contracts (Phased)

### 7.1 Features (target)
- 50-80 deterministic historical-only features.
- Families: momentum, volatility, trend, mean-reversion, volume, market context.
- Bundle carries exact `feature_spec.json`.

### 7.2 Label (target V1)
- Binary cost-aware next-bar classification.
- Persist `forward_return` and `y`.
- `y=1` only when forward return exceeds cost-adjusted threshold.

### 7.3 Snapshot contract (implemented for Phase 5)
Snapshot path:
- `data/snapshots/<SNAPSHOT_ID>/`

Required files:
- `manifest.json`
- `rows.parquet`

Closed-bar cutoff rule:
- `build-snapshot --asof <ISO_TIME>` includes only bars with `timestamp_ms < floor(asof, 15m)`.
- labels are emitted only when the next contiguous 15m bar is also closed.

Row contract in `rows.parquet`:
- market columns retained: `open`, `high`, `low`, `close`, `volume`, `inside_bid`, `inside_ask`
- row metadata: `symbol`, `timestamp_ms`, `next_timestamp_ms`, `source`, `effective_month`
- quality/weight metadata: `quality_pass`, `weight_method_version`, `effective_monthly_weight`, `supervised_row_weight`
- supervision fields: `label_available`, `row_status`, `next_close`, `forward_return`, `y`

Phase 5 weighting rules:
- default training dataset source is `combined` via `QTBOT_DATASET_MODE=combined`
- NDAX rows use `effective_monthly_weight=1.0`
- synthetic rows use monthly `w_final`
- synthetic rows with `quality_pass=false` remain in the snapshot as `row_status=continuity_only`
- `synthetic_gap_fill` rows, and rows whose next bar is `synthetic_gap_fill`, remain continuity-only
- `supervised_row_weight=0.0` when `label_available=false`

Snapshot hash contract:
- dataset hash is computed over snapshot row order, source tags, weights, and label fields
- row order is deterministic: Universe V1 symbol order, then ascending `timestamp_ms`
- manifest includes per-symbol parity checks and source-mix audits

## 8) Training, Evaluation, Promotion (Phased)

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

### 8.3 Cost model defaults
- Fee baseline: `QTBOT_FEE_PCT_PER_SIDE=0.002` (0.2% per side)
- Slippage model configurable.

### 8.4 Conservative promotion defaults
- `entry_threshold=0.60`
- `exit_threshold=0.48`
- `min_folds=12`
- `min_trades=200`
- `max_drawdown=25%`
- slippage stress must remain net positive

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
- `qtbot data-backfill --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m --sources ndax,binance`
- `qtbot data-status --timeframe 15m --dataset ndax|binance|combined|all`
- `qtbot data-build-combined --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m`
- `qtbot data-calibrate-weights --from YYYY-MM-DD --to YYYY-MM-DD --timeframe 15m --refresh monthly`
- `qtbot data-weight-status --timeframe 15m`

Defaults:
- `data-backfill` sources default to `ndax,binance`
- `data-status` dataset default to `combined`

### 11.3 Snapshot command (implemented)
- `qtbot build-snapshot --asof <ISO_TIME> --timeframe 15m`

Defaults:
- dataset source comes from `QTBOT_DATASET_MODE` (default `combined`)
- if `<ISO_TIME>` omits an offset, UTC is assumed

### 11.4 Training/model commands (phased)
- `train`, `eval`, `promote`
- `model-status`, `predict`, `set-active-bundle`

## 12) SQLite Schema Contract

Active dual-source tables:
- `data_sync_checkpoints`
- `data_coverage_v2`
- `conversion_quality`
- `synthetic_weights`
- `combined_builds`

Retained runtime tables:
- `bot_state`, `risk_state`, `positions`, `state_events`, `data_coverage`

Planned ML training tables:
- `training_runs`, `training_folds`, `fold_metrics`, `promotions`

Schema rules:
- append-only or explicit upsert semantics
- deterministic writes
- migration-safe evolution

## 13) Config Surface

Required dual-source variables:
- `QTBOT_DATA_SOURCES=ndax,binance`
- `QTBOT_DATASET_MODE=combined`
- `QTBOT_BINANCE_BASE_URL=https://api.binance.com`
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
- Milestones A-E are implemented (data ingestion, combined build, calibration weighting, weighted snapshot integration).
- Milestones F-I remain the official implementation runway to production ML.
- The immediate next milestone is **F (Cost-aware evaluator / walk-forward training)**.

Mandatory gate sequence from current state to production:
1. **F: Cost-aware evaluator**
   - Entry: snapshot builder and weighted dataset path complete.
   - Exit: deterministic walk-forward metrics with sensitivity runs.
2. **G: Promotion + bundle publisher**
   - Entry: evaluator outputs complete and gate inputs persisted.
   - Exit: deterministic promote accept/reject + atomic `LATEST` update + signature verification.
3. **H: Live inference integration**
   - Entry: at least one promoted bundle and passing model integrity checks.
   - Exit: bar-close deterministic predictions with observe-only safety fallback.
4. **I: Staging/cutover readiness**
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
