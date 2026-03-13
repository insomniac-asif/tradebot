# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

QQQBot — a Python options trading bot with Discord interface, 36 paper trading simulations, live trading via Alpaca, backtesting engine, ML predictions, and a FastAPI dashboard.

## Commands

```bash
# Run the bot
python -m interface.bot              # Entry point (requires .env with DISCORD_TOKEN, ALPACA_API_KEY, ALPACA_SECRET_KEY)
./runbot.sh                          # Auto-restart wrapper

# Tests (150 tests, ~35s)
python -m pytest tests/ -v           # All tests
python -m pytest tests/test_sim_signals.py -v          # Single file
python -m pytest tests/test_sim_signals.py::test_name  # Single test

# Type checking
pyright

# Dashboard
python -m dashboard.app              # FastAPI on port 8080/8090

# Backtest
python -m backtest.runner --start 2024-06-01 --end 2024-12-31
```

## Architecture

**Entry flow:** `interface/bot.py` → loads 5 cogs → starts background watchers → reconciles with broker → begins trading loops.

### Layers

- **interface/** — Discord bot. `bot.py` (191 lines, lifecycle only). 5 cogs in `cogs/` (65 commands total). `watchers.py` runs ~14 background async tasks. `shared_state.py` has constants/formatters shared across cogs.
- **simulation/** — 36 strategies (SIM00–SIM35) defined in `sim_config.yaml`. `sim_engine.py` loads profiles, `sim_entry_runner.py`/`sim_exit_runner.py` handle trade lifecycle, `sim_signals.py` has 20+ signal modes, `sim_account_mode.py` manages small-account compounding ($500 start, 2% risk/trade, death at $25).
- **core/** — `data_service.py` (Alpaca data + CSV backfill), `analytics_db.py` (SQLite WAL-mode replacing 7 CSVs), `runtime_state.py` (state machine: BOOTING→READY→TRADING_ENABLED/DEGRADED/PANIC_LOCKDOWN), `live_risk_supervisor.py` (authorization gate + kill switch), `reconciliation.py` (broker vs internal position check), `freshness_monitor.py` (data staleness detection). Global singletons via `singletons.py`.
- **analytics/** — Prediction grading, conviction tracking, trade journal, adaptive tuning, market structure, composite scoring. All write to SQLite (`data/analytics.db`).
- **signals/** — `predictor.py` (ML predictions), `regime.py` (market regime), `volatility.py`, `conviction.py`.
- **decision/** — Live trade execution orchestration (`trader.py`, `trader_contracts.py`).
- **backtest/** — `engine.py` + `runner.py` for historical simulation with parameter sweeps.
- **research/** — `train_ai.py` (ML retraining), `walk_forward.py`, `pattern_pipeline.py`.
- **dashboard/** — FastAPI app (`app.py`) with sim stats, charts, trade history endpoints.

### Key Patterns

- **Async-first:** All blocking I/O wrapped with `asyncio.to_thread()` (file reads, YAML parsing, Alpaca API calls). Never block the event loop.
- **State machine:** `RUNTIME` singleton in `core/runtime_state.py` gates all entry/exit decisions. Thread-safe with enforced transitions.
- **YAML-driven sims:** Each sim in `sim_config.yaml` defines signal mode, risk params, Greeks handling, SL/TP. `signal_params:` key threads custom params through `derive_sim_signal()`.
- **SQLite analytics:** `core/analytics_db.py` — WAL-mode, thread-safe (each op opens own connection). Helpers: `get_conn()`, `transaction()`, `read_df()`, `insert()`, `update()`.
- **SIM00 graduation gate:** SIM00 (live) requires its `source_sim` to have logged `min_source_trades` before executing real orders.
- **Sim state files:** `data/sims/SIM*.json` hold balance, open/closed trades per sim. Loaded/saved by `sim_engine.py`.

### Data

- `data/*.csv` — 1-minute OHLCV bars (SPY, QQQ, IWM, VXX, etc.)
- `data/analytics.db` — SQLite with 7 tables (predictions, blocked_signals, conviction_expectancy, signal_log, contract_selection_log, execution_quality_log, trade_features)
- `data/sims/*.json` — Per-sim state
- `data/postmortems/*.json` — Small-account death reports

## Bot Rules (Hard Constraints)

- Max 1 position per sim at a time
- No trading outside 9:30 AM – 4:00 PM ET
- Minimum 20 bars before any signal fires
- SL/TP percentages apply to option prices, not underlying (15–50% range)
- SPY strikes: whole-dollar only (`round(base_strike)`)
- Alpaca rate limit: 0.5s minimum between API calls

## Logging

`system.log` at ERROR level only — INFO/WARNING suppressed. Watchers use `safe_task()` for crash recovery. ML retrain failures (`sim_ml_retrain_failed`) are normal when no training data exists.
