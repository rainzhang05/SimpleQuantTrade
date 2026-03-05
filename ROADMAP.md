# Final Roadmap + Instruction File: Minimal, Reliable CLI Quant Crypto Trader (NDAX, CAD, Spot-Only, Live)

This file instructs an AI agent to implement a **simple fixed-rule** quantitative crypto trading system that runs as a **command-line bot** on **NDAX**, using **CAD** only, with **live evaluation at the smallest practical cadence**, and **graceful pause/stop** from another terminal.

The agent may modify project structure/files as needed, but MUST preserve all constraints and behaviors below.

---

## 0) Hard Requirements (Non-Negotiable)

### Exchange / credentials
- Exchange: **NDAX**
- Load credentials from `.env`:
  - `NDAX_API_KEY`
  - `NDAX_API_SECRET`
  - `NDAX_USER_ID` (required for authenticated private calls such as balances)
  - `NDAX_USERNAME` (optional helper for user/account lookup calls)

### Spot-only (no borrowing)
- **Spot trading only**
- **No margin, no leverage, no shorting**
- Only buy/sell assets using available CAD and held crypto.

### Budget model (virtual sub-account)
- `--budget` specifies an **initial CAD allocation** that the bot is allowed to deploy.
- The bot must behave like it has a separate sub-account:
  - It must never spend more CAD than the bot’s internal `bot_cash_cad`.
  - `bot_cash_cad` starts as `initial_budget_cad` and changes with trades.
- The bot may reuse profits (i.e., if bot grows, it can deploy more than the initial CAD later).  
  Implementation: bot is capped only by its own internal cash + proceeds, not by the original amount.

### Locked assets
- **BTC and ETH are locked**:
  - Bot MUST NOT buy BTC/ETH.
  - Bot MUST NOT sell BTC/ETH.
  - Bot MUST ignore BTC/ETH in decisions and must not use BTC/ETH balances.

### Tradable universe
- Only trade the hardcoded “top 20 market coins” list in code.
- Exclude BTC and ETH → effectively **18 tradable coins**.
- Only trade coins that have a **CAD spot pair** on NDAX.
- If a configured coin has no CAD pair on NDAX, skip and log.

### Fees (must be applied)
- Assume taker fees:
  - **0.4% per side**
  - **0.8% round-trip**
- Fee must be deducted from PnL and tracked.

### Live-only
- No backtest required.
- Bot runs continuously, using live data and placing live orders.

### Control from another terminal (graceful)
- Must support:
  - start live trading
  - pause
  - resume
  - stop (graceful; persist state and exit cleanly)
- Pause/stop must not abruptly kill the process.

---

## 1) CLI Commands (Minimal UX)

Command name: `qtbot`

Required commands:

1) Start (budget must be set before running)
- `qtbot start --budget <CAD>`

2) Pause / Resume / Stop (usable while bot is running, from another terminal)
- `qtbot pause`
- `qtbot resume`
- `qtbot stop`

3) Status (works whether bot is running or not)
- `qtbot status`

### Control mechanism
Use a local control record (file or SQLite flag). Recommended simplest:
- `runtime/control.json` with:
  - `{ "command": "RUN" | "PAUSE" | "STOP" }`

Rules:
- `pause/resume/stop` update `runtime/control.json` atomically.
- Running bot checks this command each loop and behaves accordingly.
- STOP must:
  - stop placing new orders
  - persist state and logs
  - exit cleanly

---

## 2) Live Evaluation Cadence (Smallest Practical)

Goal: “evaluate the market live” at the smallest reasonable cadence.

### Execution plan (two time scales)
- **Polling / evaluation loop**: every **60 seconds**.
- **Signal timeframe**: use **1-minute candles** for core signals (since user wants smallest possible).
- To reduce noise-induced churn while still evaluating every minute, enforce:
  - **No re-entry into the same asset within a cooldown window** after exit (default 30 minutes).
  - **Trade only on state changes** (signal flips), not every minute.

This preserves “live evaluation” but avoids spamming orders.

---

## 3) Strategy (Fixed Simple Rules, No ML)

### Summary
Long-only spot strategy using:
- short/long EMA trend
- ATR-based stop loss
- time-based exit (since holding is hours to days)

### Indicators (1-minute candles)
- `EMA_fast = 60`   (approx 1 hour on 1m data)
- `EMA_slow = 360`  (approx 6 hours on 1m data)
- `ATR = 60`        (approx 1 hour ATR on 1m data)

(These are deliberately larger than typical 1m scalping settings to align with “hours to days” holding while still evaluating on 1m.)

### Entry (buy) conditions for a coin
Enter long if ALL are true:
1) Coin is in allowed universe and has CAD pair
2) Not BTC/ETH
3) Not currently holding that coin (qty == 0)
4) Trend up: `EMA_fast > EMA_slow`
5) Pullback: `close <= EMA_fast` (price at/below fast EMA)
6) Cooldown satisfied (no exit from this coin within last `COOLDOWN_MINUTES`, default 30)

### Exit (sell) conditions
Exit if ANY are true:
1) Trend break: `EMA_fast < EMA_slow`
2) Stop loss: `close < entry_price - STOP_K * ATR`, default `STOP_K = 2.5`
3) Time stop: holding duration > `MAX_HOLD_HOURS`, default 48 hours

### Position selection when multiple entries appear
Because user wants **no max positions limit** and **no per-coin cap**, the bot can deploy all available CAD.
Still, the bot needs deterministic allocation logic.

Use this simple deterministic policy:
- Compute an “entry score” for each candidate coin:
  - `score = (EMA_fast - EMA_slow) / close`  (relative trend strength)
- Sort candidates by score descending.
- Allocate available bot cash equally across all candidates that pass filters **up to a practical cap of N candidates per cycle** to avoid placing too many orders at once.
  - Default `MAX_NEW_ENTRIES_PER_CYCLE = 3` (operational safety; not a portfolio limit)
- For each selected coin, buy with:
  - `order_notional = bot_cash_cad / remaining_candidates`
- Continue until either:
  - no more candidates, or
  - bot_cash_cad falls below NDAX minimum order threshold + fees.

Note: This does not limit total positions overall; it only limits how many *new* buys are attempted in one minute to avoid API/order bursts.

---

## 4) Budget & Accounting (Virtual Sub-Account Ledger)

### Internal ledger fields (minimum)
Persist:
- `initial_budget_cad`
- `bot_cash_cad`
- positions table:
  - `symbol`
  - `qty`
  - `avg_entry_price`
  - `entry_time`
  - `last_exit_time` (for cooldown)
- totals:
  - `realized_pnl_cad`
  - `unrealized_pnl_cad` (computed live)
  - `fees_paid_cad`

### Fee handling (assumed)
For each filled trade:
- Buy fee: `fee = 0.004 * notional_cad`
- Sell fee: `fee = 0.004 * notional_cad`

Accounting:
- Buy:
  - `bot_cash_cad -= notional + fee`
  - position qty increases
- Sell:
  - `bot_cash_cad += proceeds - fee`
  - realized pnl computed against average cost basis

### Relationship to real NDAX balances
The bot must also ensure it never attempts to spend more CAD than NDAX actually has available.
Rule:
- `order_notional <= min(bot_cash_cad, ndax_available_cad)` after reserving estimated fees.

---

## 5) Live Trading & Execution (Simple, Reliable)

### Order type
- Market orders only (simplest, reduces fill uncertainty).

### Execution steps per order
1) Check control command is still RUN
2) Compute order size (notional CAD) with fee buffer
3) Place market order
4) Fetch order status / fills
5) Confirm filled quantity and average fill price
6) Update ledger + state
7) Append to `trades.csv`

### Operational safety
- API retry with exponential backoff on transient failures.
- Idempotency:
  - If the bot crashes after placing an order, on restart it must reconcile holdings before trading again.
- Live-mode preflight gate before any order placement:
  - validate credentials/authentication,
  - validate NDAX API reachability,
  - validate tradable CAD market coverage,
  - validate candle warm-up sufficiency for strategy indicators (coverage threshold, default 0.8),
  - validate state DB read/write health,
  - validate control-file integrity.
  - If any preflight check fails, the bot must block live startup and surface exact failed checks.

---

## 6) State Persistence (Required for Pause/Resume/Stop)

### Storage
Preferred: `runtime/state.sqlite` (single-file DB).
Acceptable: `runtime/state.json` if atomic write is implemented (temp file + rename + fsync).

### Must persist on:
- end of every loop iteration
- immediately upon transitioning to PAUSE
- immediately before exiting on STOP

### Resume behavior
On start:
1) If prior state exists and control is not STOPPED, load it
2) Sync NDAX balances for all tradable coins
3) Reconcile:
   - If NDAX holdings differ from internal positions, treat NDAX as truth:
     - update internal positions to match NDAX
     - log a reconciliation event
4) Do not place trades until reconciliation completes successfully

---

## 7) Logging (Append-Only)

Write logs into `runtime/logs/`:

1) `qtbot.log` (human-readable)
- loop timing
- control state changes
- errors/retries
- reconciliation notes
- risk/preflight state transitions

2) `trades.csv`
Columns (minimum):
- timestamp
- symbol
- side
- qty
- avg_price
- notional_cad
- fee_cad
- order_id

3) `decisions.csv`
Columns (minimum):
- timestamp
- symbol
- close
- ema_fast
- ema_slow
- atr
- signal (ENTER / EXIT / HOLD)
- reason

4) Optional Discord alerts (webhook-driven)
- lifecycle `PAUSE` / `STOP` transitions
- repeated API/execution failures
- reconciliation anomalies
- risk-triggered halts

---

## 8) Universe Definition (Hardcoded Top 20 Minus BTC/ETH)

The agent must:
- Provide a hardcoded list of 20 tickers in config (includes BTC, ETH).
- Filter out BTC/ETH.
- Map tickers to NDAX market/pair symbols.
- Validate CAD pair existence at startup:
  - if missing, skip coin
- Always exclude BTC and ETH regardless of exchange symbol aliasing.

---

## 9) Recommended Minimal Implementation Layout (Agent May Change)

Suggested components:
- `cli` (commands, control updates)
- `runner` (main loop)
- `ndax_client` (API wrapper)
- `strategy` (indicators + signals)
- `execution` (orders + fill confirmation)
- `state` (persistence + reconciliation)
- `ledger` (PnL + fee accounting)
- `universe` (symbols + CAD pairs + exclusions)
- `logging`
- `alerts` (Discord operational notifications)

---

## 10) Development Milestones (Deliver in This Order)

### M1: CLI + Control Plane + Persistence
- Implement `start/pause/resume/stop/status`
- Implement `runtime/control.json`
- Implement persistent state store and atomic updates
Acceptance:
- start runs a loop and writes logs/state every minute
- pause/resume/stop works from another terminal and is graceful

### M2: NDAX Integration
- Load `.env`
- Fetch balances
- Fetch OHLC candles (1m)
- Validate CAD pairs for universe
Acceptance:
- bot can list tradable CAD pairs and pull candle history reliably

### M3: Strategy Signals (Dry Run)
- Compute EMA/ATR
- Generate ENTER/EXIT decisions
- Write decisions.csv
Acceptance:
- bot makes consistent decisions but does not trade yet (feature flag)

### M4: Live Execution + Ledger
- Place market orders
- Confirm fills
- Update state/ledger
- Track fees
Acceptance:
- bot trades live and accounting remains consistent

### M5: Hardening
- robust retries/backoff
- reconciliation on restart
- duplicate prevention
- enforce BTC/ETH lock everywhere
Acceptance:
- bot can survive transient API failures and can resume after pause/stop without losing state

### M6: Go-Live Validation Gate
- Add deterministic go-live preflight checks before entering live loop.
- Block live startup if any safety-critical check fails.
- Persist and log preflight outcomes for operations review.
Acceptance:
- failed preflight blocks live order path with explicit failed checks
- successful preflight is required before live execution can run

### M7: Risk Hardening
- enforce daily loss cap with auto-pause
- enforce slippage guard with auto-pause
- enforce consecutive error kill-switch
Acceptance:
- risk constraints consistently block unsafe live execution paths

### M8: Logging + Discord Alerting
- keep append-only runtime logs for loop/decisions/trades
- send Discord alerts for lifecycle, failure, reconciliation, and risk-halt events
Acceptance:
- operator receives actionable operational alerts without blocking trading process safety paths

### M9: Docker Production Packaging
- build Python 3.11 production image with `qtbot` entrypoint
- add `docker-compose` service using `.env` + persistent `runtime/` volume
- document container lifecycle operations (`start`, `pause`, `resume`, `stop`, `status`)
Acceptance:
- reproducible container build and successful `qtbot status` execution in-container
- compose deployment preserves runtime state/log artifacts across restarts
- control-plane commands remain operable from another terminal via `docker compose exec`

### M10: Staging Validation
- add `qtbot staging-validate` control-plane command for operator-run staging checks
- run continuous dry-run loop in isolated staging runtime against live NDAX public data
- exercise `pause/resume/stop` lifecycle commands while staging loop is active
- execute explicit failure scenario checks (for example invalid symbol NDAX checks)
- verify reconciliation and risk controls under deterministic simulated fault injections
- persist machine-readable staging report to runtime logs
Acceptance:
- staging validation emits structured pass/fail report with per-step evidence
- lifecycle and failure-path drills are reproducible from CLI
- reconciliation and risk simulated-fault checks pass before production cutover

---

## 11) Defaults Summary (Chosen to Match “Fast Live Evaluation” + “Hours to Days Holding”)

- Loop cadence: **60 seconds**
- Candle timeframe: **1 minute**
- EMA fast/slow: **60 / 360**
- ATR length: **60**
- Stop: **2.5 * ATR**
- Time stop: **48 hours**
- Cooldown after exit: **30 minutes**
- Max new entries per cycle: **3** (operational burst control; not a portfolio limit)
- Fee: **0.4% per side**
- Go-live preflight warmup coverage threshold: **0.8**
- Daily loss cap: **250 CAD**
- Max slippage guard: **2%**
- Consecutive error kill-switch limit: **3**
- Discord alert timeout: **8s**
- Discord alert retries: **2**
- Discord webhook URL: unset by default (alerts disabled unless configured)
- Docker compose startup budget (`QTBOT_START_BUDGET_CAD`): **1000 CAD**

All defaults should be placed in a single config module/file.

---
