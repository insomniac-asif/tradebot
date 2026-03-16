# QQQBot Architecture Export — Blueprint for Replication

> **Purpose**: This document contains everything needed to replicate the QQQBot trading system for a different market (e.g., futures). Written for a developer or AI with zero knowledge of this codebase.
>
> **Source project**: Options trading bot with Discord interface, 40 paper sims, live trading via Alpaca, backtesting, ML predictions, FastAPI dashboard.
>
> **Target adaptation**: Futures bot targeting ES, NQ, GC on a prop firm (LucidFlex 50K).

---

## Table of Contents

1. [Project Structure](#1-project-structure)
2. [Sim System Architecture](#2-sim-system-architecture)
3. [Trade Lifecycle](#3-trade-lifecycle)
4. [Database Schema](#4-database-schema)
5. [Signal/Prediction System](#5-signalprediction-system)
6. [Dashboard Architecture](#6-dashboard-architecture)
7. [Reporter/Heartbeat System](#7-reporterheartbeat-system)
8. [Configuration](#8-configuration)
9. [Key Business Logic](#9-key-business-logic)
10. [What to Adapt for Futures](#10-what-to-adapt-for-futures)

---

## 1. Project Structure

### Directory Tree

```
project_root/
├── analytics/              # Prediction grading, trade journal, adaptive tuning
│   ├── grader.py           # Grades ML predictions against actual outcomes
│   ├── prediction_stats.py # Logs predictions to SQLite
│   ├── conviction_stats.py # Conviction tracking + expectancy
│   ├── market_structure.py # Support, resistance, VWAP, pivots, value area
│   ├── cross_asset_context.py  # SPY/QQQ/IWM divergence, momentum alignment
│   ├── options_positioning.py  # [OPTIONS-SPECIFIC] Max pain, call/put walls, gamma zones
│   ├── feature_drift.py    # Detects distribution shift in features
│   ├── adaptive_tuning.py  # Automatic Greeks parameter adjustment [OPTIONS-SPECIFIC]
│   ├── trade_journal.py    # Daily narrative of trades
│   ├── decision_gates.py   # Auto-enables/disables sims based on accuracy
│   └── sim_features.py     # Feature vector computation
│
├── backtest/               # Historical simulation engine
│   ├── engine.py           # Walk-forward backtester (main loop)
│   ├── runner.py           # CLI runner with parameter sweeps
│   ├── exit_adapter.py     # Time-decay TP/SL, IV crush exits [OPTIONS-SPECIFIC]
│   ├── signal_adapter.py   # Maps backtest signals to sim signal modes
│   ├── optimizer.py        # Grid-search parameter optimizer
│   ├── monte_carlo.py      # Monte Carlo simulations
│   └── data_fetcher.py     # Historical data retrieval
│
├── core/                   # Foundation layers
│   ├── data_service.py     # Market data: CSV + Alpaca fetch + indicators
│   ├── analytics_db.py     # SQLite WAL-mode store (predictions, signals, etc.)
│   ├── projects_db.py      # Cross-project SQLite store (trades, heartbeats)
│   ├── project_reporter.py # Local reporter: QQQbot → projects.db
│   ├── runtime_state.py    # State machine: BOOTING→READY→TRADING_ENABLED
│   ├── live_risk_supervisor.py  # Authorization gate + kill switch
│   ├── reconciliation.py   # Broker vs internal position check
│   ├── freshness_monitor.py# Data staleness detection → escalates state
│   ├── market_clock.py     # NYSE calendar + market hours check
│   ├── rate_limiter.py     # API rate limit enforcement
│   ├── slippage.py         # Spread-aware slippage model [OPTIONS-SPECIFIC]
│   ├── black_scholes.py    # Option pricing [OPTIONS-SPECIFIC]
│   ├── paths.py            # BASE_DIR, DATA_DIR, LOG_DIR constants
│   ├── singletons.py       # RUNTIME + RISK_SUPERVISOR imports
│   └── trade_db.py         # SQLite trade history (synced from sim JSONs)
│
├── dashboard/              # FastAPI web UI
│   ├── app.py              # Main server (40+ routes, port 8090)
│   ├── api_projects.py     # Cross-project ingest API (/api/projects/*)
│   ├── api_intelligence.py # Strategy rankings, ML accuracy, narratives
│   ├── app_helpers.py      # Config loading, sim stats computation
│   ├── app_helpers2.py     # Sim detail aggregation
│   ├── app_helpers3.py     # Symbol CSV loading
│   └── static/             # Frontend SPA
│       ├── index.html      # Main HTML shell (7 tabs)
│       ├── main.js         # 5600 lines vanilla JS + ApexCharts
│       └── style.css       # 3500 lines CSS (classroom theme)
│
├── interface/              # Discord bot + commands
│   ├── bot.py              # Entry point (191 lines), lifecycle + task registration
│   ├── watchers.py         # 15+ async background loops
│   ├── startup_checks.py   # Startup validation
│   ├── shared_state.py     # Constants, formatters shared across cogs
│   └── cogs/               # 65 Discord commands across 5 cogs
│       ├── live_commands.py    # !kill, !unkill, !reconcile, !status
│       ├── sim_commands.py     # !simstats, !simcompare, !simleaderboard
│       ├── market_commands.py  # !spy, !quote, !regime, !predict
│       ├── admin_commands.py   # !help, !system, !backfill, !retrain
│       └── research_commands.py # !ask, !askmore, !research
│
├── simulation/             # Core sim engine
│   ├── sim_config.yaml     # Strategy definitions (SIM00-SIM43)
│   ├── sim_portfolio.py    # SimPortfolio class: load/save JSON, trade lifecycle
│   ├── sim_engine.py       # Profile loading, circuit breaker, exit evaluation
│   ├── sim_entry_runner.py # Entry orchestration (944 lines)
│   ├── sim_exit_runner.py  # Exit logic (340 lines)
│   ├── sim_signals.py      # 28 signal modes dispatch
│   ├── sim_contract.py     # Option contract selection [OPTIONS-SPECIFIC]
│   ├── sim_account_mode.py # Small-account: sizing, death, post-mortem
│   ├── sim_watcher.py      # Main sim_entry_loop + sim_exit_loop
│   ├── sim_executor.py     # Execute trade on paper
│   ├── sim_portfolio.py    # Load/save SIM*.json
│   └── sim_signal_funcs*.py # Individual signal implementations
│
├── signals/                # ML predictions, regime, conviction
│   ├── predictor.py        # Neural net 15m/60m/daily forecasts
│   ├── regime.py           # Market regime (TREND/RANGE/VOLATILE)
│   ├── volatility.py       # Volatility state (HIGH/NORMAL/LOW)
│   ├── conviction.py       # Conviction score
│   └── opportunity.py      # Setup intelligence
│
├── decision/               # Live trade execution orchestration
│   ├── trader.py           # Main entry/exit decision logic
│   └── trader_contracts.py # Contract selection for live [OPTIONS-SPECIFIC]
│
├── data/                   # Runtime data
│   ├── *_1m.csv            # Per-symbol 1-minute OHLCV bars
│   ├── analytics.db        # SQLite WAL-mode (30.7 MB, 7 tables)
│   ├── projects.db         # Cross-project data store
│   ├── sims/SIM*.json      # Per-sim state files
│   ├── postmortems/        # Small-account death reports
│   └── heartbeat.json      # Bot alive check
│
├── tests/                  # 169 tests
└── .env                    # API keys, tokens
```

### Entry Points

```bash
# Main bot (Discord + all background tasks)
python -m interface.bot

# Auto-restart wrapper
./runbot.sh

# Dashboard only (no bot)
python -m dashboard.app

# Backtest
python -m backtest.runner --start 2024-06-01 --end 2024-12-31
```

### Startup Sequence

1. `bot.py` loads `.env`, creates Discord bot instance
2. `setup_hook()` runs:
   - State transition: BOOTING → RECONCILING → READY
   - Backfills CSV data from broker API
   - Reconciles live positions vs internal state
   - Registers 20+ background tasks
   - Loads 5 Discord cogs
   - Starts dashboard subprocess (port 8090)
3. Background tasks run continuously:
   - `sim_entry_loop` — every 60s, checks all sims for entry signals
   - `sim_exit_loop` — every 30s, checks all open trades for exit
   - `forecast_watcher` — every 10 min, generates ML predictions
   - `project_reporter_loop` — every 120s, syncs to projects.db
   - 15+ other watchers (health, grading, tuning, etc.)

---

## 2. Sim System Architecture

### How Sims Work

Each sim is an independent paper trading strategy defined in `sim_config.yaml`. They share the same market data but have different:
- Signal modes (how they decide to enter)
- Risk parameters (SL, TP, position sizing)
- Time horizons (scalp, intraday, swing)

### Creating a Sim

Add a block to `sim_config.yaml`:

```yaml
SIM01:
  name: "My Strategy"
  symbols: [SPY, QQQ]
  horizon: intraday           # scalp, intraday, swing
  signal_mode: TREND_PULLBACK # which signal function to use
  features_enabled: false
  dte_min: 0                  # [OPTIONS-SPECIFIC] days to expiry
  dte_max: 1
  balance_start: 3000
  small_account_mode: true
  death_threshold: 150
  max_position_pct: 0.25      # max 25% of balance in one trade
  risk_per_trade_pct: 0.04    # risk 4% per trade
  daily_loss_limit_pct: 0.08  # stop trading after 8% daily loss
  max_open_trades: 1
  stop_loss_pct: 0.10
  profit_target_pct: 0.35
  trailing_stop_activate_pct: 0.10
  trailing_stop_trail_pct: 0.04
```

On first access, a `data/sims/SIM01.json` state file is created automatically.

### Sim State File (JSON)

```json
{
  "sim_id": "SIM02",
  "schema_version": 1,
  "created_at": "2026-03-13T05:16:12-04:00",
  "last_updated_at": "2026-03-15T22:38:08-04:00",
  "profile_snapshot": {
    "name": "0DTE Scalp Breakout",
    "signal_mode": "BREAKOUT",
    "balance_start": 3000,
    "stop_loss_pct": 0.1,
    "profit_target_pct": 0.35
  },
  "balance": 3023.40,
  "open_trades": [],
  "trade_log": [
    {
      "trade_id": "SIM02__b0165017-e558-...",
      "sim_id": "SIM02",
      "symbol": "IWM",
      "option_symbol": "IWM260313P00246000",
      "entry_price": 0.6009,
      "qty": 1,
      "entry_time": "2026-03-13T12:37:28-04:00",
      "direction": "BEARISH",
      "signal_mode": "BREAKOUT",
      "strategy_family": "breakout",
      "regime_at_entry": "VOLATILE",
      "entry_notional": 60.09,
      "cash_adjusted": true,
      "mae_pct": 0.0235,
      "mfe_pct": 0.4062,
      "exit_price": 0.8349,
      "exit_time": "2026-03-13T12:45:14-04:00",
      "exit_reason": "profit_target",
      "realized_pnl_dollars": 23.40,
      "realized_pnl_pct": 0.389,
      "balance_after_trade": 3023.40,
      "peak_balance_after_trade": 3023.40,
      "account_phase": "SCALING"
    }
  ],
  "daily_loss": 0.0,
  "last_trade_day": "2026-03-15",
  "peak_balance": 3023.40,
  "is_dead": false,
  "death_time": null,
  "death_balance": null,
  "reset_count": 0
}
```

### Balance & Death Logic

**For QQQbot (options, $3K start, $150 death):**

```python
# Position sizing
risk_dollars = balance * risk_per_trade_pct  # e.g., 3000 * 0.04 = $120
risk_dollars = max(risk_dollars, max(3.0, balance * 0.01))  # floor
qty = floor(risk_dollars / (fill_price * 100))  # options are 100 shares/contract
qty = max(qty, 1)

# Block if single contract > max_position_pct of balance
if fill_price * 100 > balance * max_position_pct:
    block("contract_too_expensive_for_account")

# Block if balance <= death threshold
if balance <= death_threshold:
    block("balance_below_death_threshold")
```

**For futures adaptation (50K start, 2K drawdown):**

```python
# Equivalent logic:
risk_dollars = balance * risk_per_trade_pct  # e.g., 50000 * 0.02 = $1000
# Futures: qty = number of contracts (e.g., 1 MES = $5/pt, 1 ES = $50/pt)
qty = floor(risk_dollars / (stop_loss_dollars_per_contract))

# Death = blowout at $2K drawdown from peak
if peak_balance - balance >= 2000:
    trigger_death()
```

### Death Handling

When balance drops to death threshold:
1. `is_dead = True`, `death_time` stamped
2. Post-mortem JSON written to `data/postmortems/`
3. Sim stops taking new trades (exits still allowed)
4. Sim does NOT auto-reset — stays dead until manually reset
5. Post-mortem includes diagnosis: "low_win_rate", "unfavorable_risk_reward", "frequent_stop_outs", etc.

```python
def check_and_handle_death(sim) -> bool:
    if sim.is_dead:
        return True
    death_threshold = sim.profile.get("death_threshold", 150.0)
    if sim.balance <= death_threshold:
        sim.is_dead = True
        sim.death_time = now_et_iso()
        sim.death_balance = sim.balance
        _write_postmortem(sim)
        return True
    return False
```

### Concurrent Sim Management

- Each sim has an `asyncio.Lock` (per sim_id)
- Entry runner acquires lock → loads JSON → checks signal → executes → saves JSON → releases lock
- Exit runner does the same, interleaved on 30s loop
- Global guards prevent overexposure:
  - `max_directional_sims: 4` — max 4 sims trading same direction
  - `max_family_concurrent: 2` — max 2 sims from same signal family
  - `max_global_open_trades: 20` — across all sims

---

## 3. Trade Lifecycle

### Entry Flow

```
sim_entry_loop (every 60s)
  → For each enabled SIM:
    1. Acquire sim lock
    2. Load SIM*.json
    3. Check can_trade():
       - Not dead?
       - Daily loss limit not hit?
       - Max open trades not reached?
       - Exposure cap not exceeded?
       - Min time between entries elapsed?
       - Cooldown after stop-loss elapsed?
    4. Get market data (DataFrame with OHLCV + indicators)
    5. Derive signal: derive_sim_signal(df, signal_mode, ...)
       → Returns (direction, price, context) or (None, None, reason)
    6. If signal fires:
       a. Check global guards (directional limit, family limit)
       b. [OPTIONS] Select contract: find option with right DTE, strike, spread
       c. Compute position size via compute_small_account_qty()
       d. Get fill price (mid + slippage)
       e. Create trade dict with all entry context
       f. Call sim.record_open(trade):
          - Deducts notional from balance (cash_adjusted=True)
          - Appends to open_trades
       g. Save SIM*.json
       h. Post Discord embed
    7. Release sim lock
```

### P&L Calculation

```python
# Options P&L (per contract = 100 shares)
realized_pnl_dollars = (exit_price - entry_price) * qty * 100

# Futures equivalent
realized_pnl_dollars = (exit_price - entry_price) * qty * point_value
# ES: point_value = 50, MES: point_value = 5
# NQ: point_value = 20, MNQ: point_value = 2
# GC: point_value = 100, MGC: point_value = 10
```

### Exit Conditions (evaluated every 30s)

Exit conditions are checked in priority order:

1. **Stop Loss**: `loss_pct <= -stop_loss_pct` → exit "stop_loss"
2. **Theta Burn** [OPTIONS-SPECIFIC]: DTE ≤ 1 + same-day expiry + <2hrs to close + gain ≤ 2%
3. **IV Crush** [OPTIONS-SPECIFIC]: Loss > 30% of SL + gain ≤ -60% of SL
4. **Profit Target**: `gain_pct >= profit_target_pct` → exit "profit_target"
5. **Trailing Stop**: After `trailing_stop_activate_pct` hit, trail by `trailing_stop_trail_pct`
6. **Hold Max**: `elapsed >= hold_max_seconds` → forced exit
7. **EOD Forced**: Same-day expiry at 15:55 ET, or day-trade sims at 15:55 ET

### Close Recording

```python
def record_close(self, trade_id, exit_data):
    # 1. Remove from open_trades
    # 2. Calculate P&L
    realized_pnl = (exit_price - entry_price) * qty * 100  # [OPTIONS: *100]
    # 3. Update balance
    if cash_adjusted:
        self.balance += exit_price * qty * 100  # return exit value
    else:
        self.balance += realized_pnl
    # 4. Track daily loss
    if realized_pnl < 0:
        self.daily_loss += abs(realized_pnl)
    # 5. Update peak balance
    if self.balance > self.peak_balance:
        self.peak_balance = self.balance
    # 6. Stamp balance snapshot on trade record
    trade["balance_after_trade"] = self.balance
    trade["account_phase"] = get_account_phase(self.balance)
    # 7. Append to trade_log
    # 8. Check for death
    check_and_handle_death(self)
    # 9. Save to JSON
```

### Excursion Tracking (MAE/MFE)

Every 30 seconds while a trade is open:

```python
def update_open_trade_excursion(self, trade_id, current_price):
    excursion = (current_price - entry_price) / entry_price
    trade["mae_pct"] = min(old_mae, excursion)  # Maximum Adverse Excursion
    trade["mfe_pct"] = max(old_mfe, excursion)  # Maximum Favorable Excursion
```

---

## 4. Database Schema

### 4.1 analytics.db (data/analytics.db)

Main analytics store. WAL-mode SQLite with PRAGMA synchronous=NORMAL.

```sql
-- ML predictions (162K+ rows)
CREATE TABLE predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time TEXT, symbol TEXT, timeframe TEXT, direction TEXT,
    confidence REAL, high REAL, low REAL, regime TEXT,
    volatility TEXT, session TEXT, actual TEXT,
    correct INTEGER DEFAULT 0, checked INTEGER DEFAULT 0,
    high_hit INTEGER DEFAULT 0, low_hit INTEGER DEFAULT 0,
    price_at_check REAL, close_at_check REAL, confidence_band TEXT
);
CREATE INDEX idx_pred_checked ON predictions(checked);
CREATE INDEX idx_pred_time ON predictions(time);
CREATE INDEX idx_pred_symbol ON predictions(symbol);

-- Signals that were blocked (2.8K rows)
CREATE TABLE blocked_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT, spy_price REAL, regime TEXT, volatility TEXT,
    direction TEXT, confidence REAL, blended_score REAL,
    threshold REAL, threshold_delta REAL, block_reason TEXT,
    fwd_5m REAL, fwd_15m REAL, fwd_5m_price REAL, fwd_15m_price REAL,
    fwd_5m_status TEXT DEFAULT 'pending', fwd_15m_status TEXT DEFAULT 'pending'
);

-- Conviction/expectancy tracking (4.1K rows)
CREATE TABLE conviction_expectancy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time TEXT, direction TEXT, impulse REAL, follow REAL, price REAL,
    fwd_5m REAL, fwd_10m REAL,
    fwd_5m_price REAL, fwd_5m_time TEXT, fwd_5m_status TEXT,
    fwd_10m_price REAL, fwd_10m_time TEXT, fwd_10m_status TEXT
);

-- Signal decision log (2.8K rows)
CREATE TABLE signal_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT, outcome TEXT, block_reason TEXT,
    regime TEXT, volatility TEXT,
    direction_60m TEXT, confidence_60m REAL,
    direction_15m TEXT, confidence_15m REAL,
    dual_alignment TEXT, conviction_score REAL,
    impulse REAL, follow_through REAL,
    blended_score REAL, threshold REAL, threshold_delta REAL,
    ml_weight REAL, regime_samples INTEGER, expectancy_samples INTEGER,
    regime_transition TEXT, regime_transition_severity REAL, spy_price REAL
);

-- [OPTIONS-SPECIFIC] Contract selection log (6.5K rows)
CREATE TABLE contract_selection_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT, source TEXT, direction TEXT,
    underlying_price REAL, expiry TEXT, dte TEXT, strike REAL,
    result TEXT, reason TEXT,
    bid REAL, ask REAL, mid REAL, spread_pct REAL,
    iv REAL, delta REAL, gamma REAL, theta REAL, vega REAL
);

-- [OPTIONS-SPECIFIC] Execution quality log
CREATE TABLE execution_quality_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT, option_symbol TEXT, side TEXT, order_type TEXT,
    qty_requested INTEGER, qty_filled INTEGER, fill_ratio REAL,
    expected_mid REAL, fill_price REAL, slippage_pct REAL,
    bid_at_order REAL, ask_at_order REAL, spread_at_order_pct REAL
);

-- ML trade features (541 rows)
CREATE TABLE trade_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    regime_encoded INTEGER, volatility_encoded INTEGER,
    conviction_score REAL, impulse REAL, follow_through REAL,
    confidence REAL, style_encoded INTEGER, setup_encoded INTEGER,
    session_encoded INTEGER, setup_raw_avg_R REAL, regime_raw_avg_R REAL,
    ml_probability REAL, predicted_won INTEGER, won INTEGER
);
```

### 4.2 projects.db (data/projects.db)

Cross-project data store for unified dashboard.

```sql
CREATE TABLE project_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL,     -- 'qqq', 'crypto', 'futures'
    timestamp TEXT NOT NULL,
    instrument TEXT, direction TEXT, side TEXT,
    size REAL, entry_price REAL, exit_price REAL,
    pnl REAL, pnl_pct REAL,
    status TEXT DEFAULT 'closed',
    strategy TEXT, sim_id TEXT,
    metadata TEXT DEFAULT '{}'  -- JSON extensibility
);

CREATE TABLE project_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL, timestamp TEXT NOT NULL,
    signal_type TEXT, instrument TEXT, direction TEXT,
    confidence REAL, timeframe TEXT, source TEXT,
    metadata TEXT DEFAULT '{}'
);

CREATE TABLE project_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL, timestamp TEXT NOT NULL,
    daily_pnl REAL DEFAULT 0, cumulative_pnl REAL DEFAULT 0,
    win_rate REAL, total_trades INTEGER DEFAULT 0,
    open_positions INTEGER DEFAULT 0, balance REAL,
    metadata TEXT DEFAULT '{}'
);

CREATE TABLE project_heartbeats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project TEXT NOT NULL, timestamp TEXT NOT NULL,
    status TEXT DEFAULT 'online', version TEXT,
    uptime_seconds REAL, metadata TEXT DEFAULT '{}'
);
```

---

## 5. Signal/Prediction System

### Signal Dispatch

```python
def derive_sim_signal(df, signal_mode, context=None, feature_snapshot=None,
                      profile=None, signal_params=None,
                      structure_data=None, options_data=None):
    """
    Returns: (direction, price, context_dict)
      direction: "BULLISH", "BEARISH", or None
      price: float entry price or None
      context_dict: dict with reasoning/metadata
    """
```

### Available Signal Modes (28)

| Mode | Family | Description |
|------|--------|-------------|
| MEAN_REVERSION | reversal | RSI oversold/overbought bounce |
| BREAKOUT | breakout | Price breaks above/below range |
| TREND_PULLBACK | trend | Pullback to EMA in trending market |
| SWING_TREND | trend | Multi-day trend following |
| ORB_BREAKOUT | breakout | Opening range breakout (first 30 min) |
| VWAP_REVERSION | reversal | Price far from VWAP snaps back |
| ZSCORE_BOUNCE | reversal | Z-score extreme mean reversion |
| FAILED_BREAKOUT_REVERSAL | fade | False breakout reversal |
| VWAP_CONTINUATION | trend | Trend continuation near VWAP |
| OPENING_DRIVE | breakout | Strong open momentum |
| AFTERNOON_BREAKOUT | breakout | PM session breakout |
| TREND_RECLAIM | reclaim | Reclaims lost EMA level |
| EXTREME_EXTENSION_FADE | fade | Overextended move fade |
| GAP_FADE | fade | Gap fill trade |
| OPENING_RANGE_RECLAIM | reclaim | Reclaims ORB level |
| VOL_COMPRESSION_BREAKOUT | volatility | Low ATR → expansion |
| VOL_SPIKE_FADE | volatility | Fade vol spike |
| STRUCTURE_FADE | structure | Bounce off S/R levels |
| FVG_4H / FVG_5M | structure | Fair value gap fill |
| LIQUIDITY_SWEEP | structure | Sweep above/below swing point |
| FLOW_DIVERGENCE | flow | Price/volume divergence |
| MULTI_TF_CONFIRM | trend | Multi-timeframe EMA alignment |

### ML Prediction Pipeline

```
forecast_watcher (every 10 min)
  → make_prediction(df, symbol, timeframe)
    → Feature extraction (50+ features)
    → Model inference (scikit-learn, direction_model.pkl)
    → Returns: {direction, confidence, regime, volatility}
  → Logged to predictions table in analytics.db
  → Graded later by prediction_grader (compares predicted vs actual)
```

### Predictor Modes

```yaml
predictor_mode: veto_only  # In _global config
# "veto_only" — predictor can only BLOCK trades (>70% opposing confidence)
# "disabled" — predictor ignored entirely
# "full" — legacy mode, requires dual alignment
```

---

## 6. Dashboard Architecture

### Server: FastAPI (port 8090)

```python
app = FastAPI(title="SpyBot Dashboard", docs_url=None, redoc_url=None, lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"])
app.include_router(intelligence_router)  # /api/intelligence/*
app.include_router(projects_router)      # /api/projects/*
app.mount("/static", StaticFiles(directory=STATIC_DIR))
```

### All API Endpoints

| Method | Path | Auth | Returns | Data Source |
|--------|------|------|---------|-------------|
| GET | `/` | - | HTML page | static/index.html |
| GET | `/api/sims` | - | All sim configs + live stats | sim_config.yaml + data/sims/*.json |
| GET | `/api/sim/{sim_id}` | - | Detailed sim state | data/sims/{sim_id}.json |
| GET | `/api/chart` | - | 1-min candles (symbol, bars=480) | CSV + Alpaca live |
| GET | `/api/predictions` | - | Today's ML predictions | analytics.db |
| GET | `/api/status` | - | Bot heartbeat + market status | data/heartbeat.json |
| GET | `/api/trades/recent` | - | Closed + open trades | data/sims/*.json |
| GET | `/api/trades/{sim_id}/history` | - | Paginated trade log | data/sims/{sim_id}.json |
| GET | `/api/trades/{sim_id}/{trade_id}/chart` | - | PNG trade chart | Generated from CSV data |
| GET | `/api/symbols` | - | Available symbols | sim_config.yaml |
| GET | `/api/equity-curve` | - | Aggregate P&L over time | data/sims/*.json |
| GET | `/api/last-price` | - | Latest close price | CSV files |
| GET | `/api/system-health` | - | CPU, RAM, disk, DB size | System APIs |
| GET | `/api/backtest/results` | - | Backtest dashboard data | backtest/results/dashboard_data.json |
| GET | `/api/backtest/results/{sim_id}` | - | Sim backtest detail | Same file |
| GET | `/api/backtest/growth/{sim_id}` | - | Growth curve | backtest/results/growth_{sim_id}.json |
| GET | `/api/backtest/patterns/{sim_id}` | - | Pattern analysis | backtest/results/patterns_{sim_id}.json |
| GET | `/api/backtest/montecarlo/{sim_id}` | - | Monte Carlo sim | backtest/results/montecarlo_{sim_id}.json |
| GET | `/api/greeks/overview` | - | Greeks exit stats | analytics.db + sims |
| GET | `/api/greeks/sim/{sim_id}` | - | Per-sim Greeks detail | analytics.db |
| GET | `/api/greeks/heatmap` | - | Effectiveness heatmap | analytics.db |
| GET | `/api/intelligence/summary` | - | Overall strategy summary | Multiple sources |
| GET | `/api/intelligence/strategy-rankings` | - | Leaderboard | data/strategy_rankings.json |
| GET | `/api/intelligence/predictor-stats` | - | ML accuracy metrics | analytics.db |
| GET | `/api/intelligence/trade-narrative` | - | Recent trade stories | data/sims/*.json |
| GET | `/api/intelligence/decision-gates` | - | Active adjustments | decision_gates.py |
| GET | `/api/intelligence/blocked-signals` | - | Block reason analysis | analytics.db |
| GET | `/api/intelligence/ml-accuracy` | - | Rolling accuracy | analytics.db |
| GET | `/api/intelligence/feature-drift` | - | Drift detection | feature_drift.py |
| POST | `/api/projects/ingest/{project}` | X-API-Key | Ingest trades/signals | projects.db |
| GET | `/api/projects/status` | - | All project summaries | projects.db |
| GET | `/api/projects/status/{project}` | - | Single project | projects.db |
| GET | `/api/projects/health` | - | Heartbeat per project | projects.db |
| GET | `/api/projects/trades` | - | Cross-project trades | projects.db |
| GET | `/api/projects/signals` | - | Cross-project signals | projects.db |
| GET | `/api/projects/snapshots` | - | P&L snapshots | projects.db |

### Frontend Architecture

**Framework**: Vanilla JavaScript SPA + ApexCharts (CDN)
**No build tools** — served directly from `/static/`

**7 Tabs:**
1. **Charts** — Live 1-min candles for all symbols (ApexCharts)
2. **Trades** — Recent closed + open trades across all sims
3. **Roster** — Sim leaderboard grouped by strategy family with counts
4. **Backtest** — Historical test results, equity curves, optimizer
5. **Greeks** — [OPTIONS-SPECIFIC] Theta burn, IV crush, delta erosion exits
6. **Intel** — ML accuracy, strategy rankings, decision gates, feature drift
7. **Projects** — Cross-project hub (QQQbot + Crypto + Futures)

**Polling**: 30-second intervals via `fetch()` (no WebSockets)

---

## 7. Reporter/Heartbeat System

### Bot Heartbeat

File: `data/heartbeat.json`
```json
{
  "started_at": "2026-03-13T10:30:00Z",
  "last_update": "2026-03-15T22:00:00Z",
  "pid": 12345
}
```

Updated every 60s by `heart_monitor` watcher.

### Cross-Project Reporter

`project_reporter_loop()` runs every 120s:

```python
def full_sync():
    sync_trades()       # Mirror closed trades from SIM*.json → projects.db
    sync_predictions()  # Mirror predictions from analytics.db → projects.db
    write_pnl_snapshot() # Aggregate P&L across all active sims
    write_heartbeat()   # Write "online" heartbeat
```

**Trade sync is idempotent** — tracks synced trade_ids in memory set.

### Ingest API (for remote projects)

```bash
# Send data from laptop to desktop
curl -X POST https://dashboard.yourdomain.xyz/api/projects/ingest/futures \
  -H "Content-Type: application/json" \
  -H "X-API-Key: YOUR_KEY" \
  -d '{
    "trades": [{"instrument": "ES", "direction": "BULLISH", "pnl": 250}],
    "snapshot": {"daily_pnl": 250, "cumulative_pnl": 1500, "balance": 48500},
    "heartbeat": {"status": "online", "version": "futures-bot-1.0"}
  }'
```

---

## 8. Configuration

### Environment Variables (.env)

```bash
DISCORD_TOKEN=...              # Discord bot token
DISCORD_OWNER_ID=...           # Your Discord user ID
DASHBOARD_PORT=8090            # FastAPI port
APCA_API_KEY_ID=...            # Alpaca API key [OPTIONS/STOCKS SPECIFIC]
APCA_API_SECRET_KEY=...        # Alpaca secret [OPTIONS/STOCKS SPECIFIC]
OPENAI_API_KEY=...             # GPT-4 for trade narratives
GPT_MODEL=gpt-4
CLASSROOM_URL=https://...      # Public dashboard URL
PROJECTS_API_KEY=...           # API key for ingest endpoints
WATCHDOG_DISCORD_WEBHOOK=...   # Emergency alert webhook
```

### sim_config.yaml Structure

```yaml
# Symbol registry — maps ticker to data file
symbol_registry:
  SPY:
    data_file: data/spy_1m.csv
  QQQ:
    data_file: data/qqq_1m.csv

# Global defaults (inherited by all sims unless overridden)
_global:
  small_account_mode: true
  default_balance_start: 3000
  death_threshold: 150
  max_position_pct: 0.15
  max_risk_pct: 0.03
  max_daily_drawdown_pct: 0.06
  max_directional_sims: 4
  max_family_concurrent: 2
  max_global_open_trades: 20
  predictor_mode: veto_only

# Per-sim configs
SIM01:
  name: "Strategy Name"
  symbols: [SPY, QQQ]
  signal_mode: TREND_PULLBACK
  # ... (see full list in Section 2)
```

---

## 9. Key Business Logic

### P&L Calculation

```python
# Options (100 shares per contract)
pnl = (exit_price - entry_price) * qty * 100
pnl_pct = (exit_price - entry_price) / entry_price

# Cash-adjusted balance:
# On entry: balance -= entry_price * qty * 100 (reserve notional)
# On exit:  balance += exit_price * qty * 100  (return exit value)
```

### Win Rate & Performance Metrics

```python
closed_trades = [t for t in trade_log if t.get("realized_pnl_dollars") is not None]
wins = [t for t in closed_trades if t["realized_pnl_dollars"] > 0]
win_rate = len(wins) / len(closed_trades) * 100 if closed_trades else None

avg_win = mean([t["realized_pnl_dollars"] for t in wins]) if wins else 0
avg_loss = mean([abs(t["realized_pnl_dollars"]) for t in losses]) if losses else 0
profit_factor = sum_wins / sum_losses if sum_losses > 0 else float('inf')
```

### Risk Management Rules

1. **Max 1 position per sim** at a time (configurable via `max_open_trades`)
2. **Daily loss limit**: Stop trading when daily_loss >= balance * daily_loss_limit_pct
3. **Exposure cap**: Total notional of open trades < balance * exposure_cap_pct
4. **Directional guard**: Max N sims trading same direction simultaneously
5. **Family guard**: Max N sims from same signal family simultaneously
6. **Cooldown after stop-loss**: Wait N seconds before next entry
7. **Min time between entries**: Configurable per sim
8. **Death threshold**: Sim permanently stops when balance drops below threshold
9. **Circuit breaker**: Monitors source_sim win rate; trips if below threshold

### State Machine

```
BOOTING → RECONCILING → READY → TRADING_ENABLED
                                      ↓
                                  DEGRADED (stale data)
                                      ↓
                                  EXIT_ONLY (manual lock)
                                      ↓
                                PANIC_LOCKDOWN (emergency)
                                      ↓
                                RECONCILING (manual recovery)
```

- **TRADING_ENABLED**: New entries + exits allowed
- **DEGRADED**: Exits only, entries blocked (auto-escalated on stale data)
- **PANIC_LOCKDOWN**: Close everything, block all activity

### Kill Switch

```python
# Emergency kill via Discord !kill command or RISK_SUPERVISOR
RISK_SUPERVISOR.emergency_kill("reason")
# Blocks ALL live entries. Exits still allowed.
# Cleared via !unkill command.
```

---

## 10. What to Adapt for Futures

### OPTIONS-SPECIFIC Components (must be replaced)

| Component | File | What It Does | Futures Equivalent |
|-----------|------|-------------|-------------------|
| Contract selection | `sim_contract.py` | Finds option chain, selects strike/expiry | Select ES/NQ/GC front-month contract |
| Greeks tracking | `sim_engine.py`, exit logic | IV, delta, gamma, theta, vega at entry | Not applicable — remove or replace with futures-specific metrics (basis, roll cost) |
| Theta burn exit | `sim_engine.py` line ~200 | Exits if time decay eating profits | Not applicable — futures don't decay |
| IV crush exit | `sim_engine.py` line ~210 | Exits if IV drops killing premium | Not applicable |
| Black-Scholes | `core/black_scholes.py` | Option theoretical pricing | Not needed |
| Spread model | `core/slippage.py` | DTE × moneyness spread lookup | Replace with futures tick-based spread (ES: 0.25pt = $12.50) |
| Option chain health | `watchers.py` | Monitors Alpaca option snapshots | Not needed |
| OTM% / Strike logic | Throughout entry runner | Calculates out-of-the-money percentage | Not applicable |
| Contract multiplier | `* 100` everywhere | Options = 100 shares/contract | ES=$50/pt, MES=$5/pt, NQ=$20/pt, MNQ=$2/pt, GC=$100/pt |
| Expiry management | Exit runner | Same-day expiry forced close | Futures roll: close before expiry, open next month |
| Greeks tab (dashboard) | `app.py`, `main.js` | Theta/IV/delta exit analytics | Replace with futures-specific tab (margin, P&L per tick) |

### QQQ/EQUITY-SPECIFIC Components

| Component | What to Change |
|-----------|---------------|
| Symbols | SPY/QQQ/IWM/VXX → ES/NQ/GC (and micro variants MES/MNQ/MGC) |
| Data source | Alpaca stock/option API → Tradovate/CQG/manual CSV for futures |
| Market hours | 9:30-16:00 ET (equities) → 18:00-17:00 ET next day (futures, nearly 24h) |
| Balance model | $3K paper account → $50K prop firm (LucidFlex) |
| Death threshold | $150 (5% of $3K) → $2K drawdown from peak ($48K death) |
| P&L multiplier | `* 100` (options) → `* point_value` (ES=50, NQ=20, GC=100) |
| Position sizing | Based on option premium | Based on margin requirement per contract |
| Rate limiting | Alpaca 0.5s | Depends on futures data provider |

### What to Keep As-Is (Generic)

- **Sim framework** — SimPortfolio class, JSON state files, load/save logic
- **Signal system** — derive_sim_signal() dispatch, signal modes (TREND_PULLBACK works on any instrument)
- **Dashboard** — FastAPI structure, frontend tabs, ApexCharts
- **Projects DB** — Cross-project ingest/status/health system
- **State machine** — RuntimeState + SystemState enum
- **Risk supervisor** — Kill switch, freshness monitoring
- **ML pipeline** — Prediction, grading, feature drift (retrain on futures data)
- **Backtest engine** — Walk-forward, optimizer (just change data source + multiplier)
- **Death/post-mortem** — Same logic, different thresholds
- **Discord bot** — Commands, watchers, embeds
- **Reporter** — Heartbeat, trade sync to projects.db

### Prop Firm Adaptation Notes

For LucidFlex 50K:
- **Max drawdown**: Usually $2K-$2.5K trailing drawdown → this is the "death threshold"
- **Daily loss limit**: Often $1K/day → configure `daily_loss_limit_pct: 0.02` (2% of 50K)
- **Position limits**: Usually max 5 contracts MES or 2 contracts ES
- **No overnight holds** typically (close all before 16:00 ET or configure EOD force-close)
- **Scaling rules**: Some prop firms allow scaling after hitting profit targets

---

*Generated from QQQBot codebase at `/home/asif420/qqqbot` on 2026-03-16.*
*This document is self-contained — no access to the original repo is needed to rebuild the system.*
