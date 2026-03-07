# Full Bot State Audit (Compact)

Generated: 2026-03-02T21:46:14.018590

## 1. Project Structure (all files, brief)
(Excluded: __pycache__, venv/.venv, node_modules, dist/build, caches)

├── .claude — Directory
│   └── settings.local.json — JSON config/state
├── .env — File
├── .gitignore — File
├── .vscode — Directory
│   └── settings.json — JSON config/state
├── analytics — Directory
│   ├── adaptive_threshold.py — Python module
│   ├── blocked_signal_tracker.py — analytics/blocked_signal_tracker.py
│   ├── capital_protection.py — analytics/capital_protection.py
│   ├── career_updater.py — analytics/career_updater.py
│   ├── contract_logger.py — analytics/contract_logger.py
│   ├── conviction_stats.py — Python module
│   ├── decision_analysis.py — Python module
│   ├── edge_compression.py — analytics/edge_compression.py
│   ├── edge_decay.py — Python module
│   ├── edge_momentum.py — analytics/edge_momentum.py
│   ├── edge_stability.py — analytics/edge_stability.py
│   ├── equity_curve.py — Python module
│   ├── execution_logger.py — analytics/execution_logger.py
│   ├── expectancy.py — Python module
│   ├── feature_drift.py — Python module
│   ├── feature_importance.py — Python module
│   ├── feature_logger.py — Python module
│   ├── grader.py — analytics/grader.py
│   ├── indicators.py — Python module
│   ├── iv_features.py — Python module
│   ├── market_regime.py — Python module
│   ├── ml_accuracy.py — Python module
│   ├── ml_loader.py — Python module
│   ├── options_greeks.py — Python module
│   ├── performance.py — analytics/performance.py
│   ├── prediction_stats.py — analytics/prediction_stats.py
│   ├── progressive_influence.py — Python module
│   ├── regime_expectancy.py — Python module
│   ├── regime_memory.py — analytics/regime_memory.py
│   ├── regime_persistence.py — analytics/regime_persistence.py
│   ├── regime_transition.py — analytics/regime_transition.py
│   ├── review_engine.py — Python module
│   ├── risk_control.py — analytics/risk_control.py
│   ├── risk_metrics.py — analytics/risk_metrics.py
│   ├── run_stats.py — analytics/run_stats.py
│   ├── setup_expectancy.py — Python module
│   ├── setup_intelligence.py — analytics/setup_intelligence.py
│   ├── signal_logger.py — analytics/signal_logger.py
│   ├── sim_features.py — Python module
│   ├── stability_mode.py — analytics/stability_mode.py
│   └── structure_pricing.py — Python module
├── audit_full.md — Documentation
├── charts — Directory
│   ├── chart.png — File
│   ├── chartqqq.png — File
│   └── live.png — File
├── core — Directory
│   ├── account_repository.py — core/account_repository.py
│   ├── data_integrity.py — Python module
│   ├── data_service.py — core/data_service.py
│   ├── debug.py — Python module
│   ├── decision_context.py — Python module
│   ├── market_clock.py — core/market_clock.py
│   ├── md_state.py — Python module
│   ├── paths.py — Python module
│   ├── rate_limiter.py — Python module
│   ├── session_scope.py — Python module
│   └── startup_sync.py — Python module
├── data — Directory
│   ├── account.json — JSON config/state
│   ├── account.json.bak1 — File
│   ├── account.json.bak2 — File
│   ├── account.json.bak3 — File
│   ├── blocked_signals.csv — CSV data
│   ├── career_stats.json — JSON config/state
│   ├── contract_selection_log.csv — CSV data
│   ├── conviction_expectancy.csv — CSV data
│   ├── edge_stats.json — JSON config/state
│   ├── execution_quality_log.csv — CSV data
│   ├── predictions.csv — CSV data
│   ├── qqq_1m.csv — CSV data
│   ├── signal_log.csv — CSV data
│   ├── sims — Directory
│   │   ├── SIM00.json — JSON config/state
│   │   ├── SIM01.json — JSON config/state
│   │   ├── SIM01.json.bak — File
│   │   ├── SIM02.json — JSON config/state
│   │   ├── SIM02.json.bak — File
│   │   ├── SIM03.json — JSON config/state
│   │   ├── SIM03.json.bak — File
│   │   ├── SIM04.json — JSON config/state
│   │   ├── SIM04.json.bak — File
│   │   ├── SIM05.json — JSON config/state
│   │   ├── SIM05.json.bak — File
│   │   ├── SIM06.json — JSON config/state
│   │   ├── SIM06.json.bak — File
│   │   ├── SIM07.json — JSON config/state
│   │   ├── SIM07.json.bak — File
│   │   ├── SIM08.json — JSON config/state
│   │   ├── SIM08.json.bak — File
│   │   ├── SIM09.json — JSON config/state
│   │   ├── SIM09.json.bak — File
│   │   ├── SIM10.json — JSON config/state
│   │   └── SIM11.json — JSON config/state
│   └── trade_features.csv — CSV data
├── decision — Directory
│   └── trader.py — decision/trader.py
├── execution — Directory
│   ├── ml_gate.py — Python module
│   └── option_executor.py — Python module
├── interface — Directory
│   ├── ai_assistant.py — Python module
│   ├── bot.py — ==============================
│   ├── charting.py — interface/charting.py
│   ├── fmt.py — ANSI formatting helpers for Discord embeds.
│   ├── health_monitor.py — interface/health_monitor.py
│   └── watchers.py — interface/watchers.py
├── logs — Directory
│   └── recorder.py — logs/recorder.py
├── package-lock.json — JSON config/state
├── package.json — JSON config/state
├── pyrightconfig.json — JSON config/state
├── research — Directory
│   └── train_ai.py — Python module
├── runbot.sh — Shell script
├── signals — Directory
│   ├── conviction.py — signals/conviction.py
│   ├── environment_filter.py — signals/environment_filter.py
│   ├── opportunity.py — signal/opportunity.py
│   ├── predictor.py — signals/predictor.py
│   ├── regime.py — signals/regime.py
│   ├── session_classifier.py — signals/session_classifier.py
│   ├── setup_classifier.py — signals/setup_classifier.py
│   ├── signal_evaluator.py — signals/signal_evaluator.py
│   └── volatility.py — signals/volatility.py
├── simulation — Directory
│   ├── sim_config.yaml — YAML config
│   ├── sim_contract.py — Python module
│   ├── sim_engine.py — Python module
│   ├── sim_evaluation.py — Python module
│   ├── sim_executor.py — Python module
│   ├── sim_live_router.py — Python module
│   ├── sim_metrics.py — Python module
│   ├── sim_ml.py — Python module
│   ├── sim_portfolio.py — simulation/sim_portfolio.py
│   ├── sim_signals.py — Python module
│   ├── sim_validator.py — Python module
│   └── sim_watcher.py — simulation/sim_watcher.py
├── start_recorder.sh — Shell script
└── system.log — Log file

## 2. Architecture Overview
- Market/instrument: SPY options (calls/puts).
- Modes: Hybrid (live trading + simulations).
- APIs: Alpaca Trading + Alpaca Market Data (option snapshots), CSV recorder fallback.
- Deployment: local loop via runbot.sh (restarts `python -m interface.bot`).
- Main flow: interface.bot starts Discord + watchers -> signals -> entry -> manage -> exit.

## 3. File-by-File Code Dump
- Omitted by request for size. All files listed above in the tree.
- Use repo paths to open full files.

## 4. Current State of Key Systems
### Entry Logic (Sims)
- Signals: `simulation/sim_signals.py` using `signal_mode`.
- Contract selection: `simulation/sim_contract.py` enforces DTE, OTM, spread, cutoff.
- Fills: `simulation/sim_executor.py` and `simulation/sim_engine.py`.
### Entry Logic (Live)
- `decision/trader.py` + `execution/option_executor.py`.
### Exit Reasons (global)
- eod_daytrade_close
- expiry_close
- hold_max_elapsed
- profit_lock
- profit_target
- profit_target_2
- stop_loss
- trailing_stop
### Trailing Stop
- Sim: `simulation/sim_engine.py` & `simulation/sim_live_router.py`.
- State: `trailing_stop_activated`, `trailing_stop_high` persisted; `sim.save()` on change.
### MAE/MFE
- `simulation/sim_portfolio.py:update_open_trade_excursion` tracks `mae_pct`/`mfe_pct`.
- Included in exit_data in sim_engine & sim_live_router.
### Simulation Manager
- State stored in `data/sims/SIMxx.json`.
- Core: `simulation/sim_engine.py`, `simulation/sim_portfolio.py`.
### Spread Guard
- Enforced during contract selection/fill; bypassed on forced exits.
### Regime
- Regime features computed in analytics + used in trader/sim contexts.

## 5. Data Flow
- Market data via CSV recorder with Alpaca fallback (`core/data_service.py`).
- Watchers poll in `interface/watchers.py` and `simulation/sim_watcher.py`.

## 6. Logging & Observability
- Python logging + Discord embeds + CSV logs.
- Analytics logs: signal_log.csv, blocked_signals.csv, contract_selection_log.csv, execution_quality_log.csv.

## 7. TODO / FIXME / HACK / BUG
- decision/trader.py:1334 — # TODO: trailing stop / take-profit for reconstructed trades.

## 8. Recent Changes
- Git history has only initial commit; recent changes are in working tree.
- Notable: sim TP2/profit_lock logic; DTE-tiered SL/TP; sim health/validator updates; feature snapshot reporting.

## 9. Dependencies
- No requirements.txt or pyproject.toml in repo root.
- `package.json` exists (pyright tooling).
- Python runtime: venv present (3.12).

## 10. Config & Parameters (SIMs)

### SIM00 — Intraday Trend Pullback (LIVE)
- balance_start: 25000
- capital_limit_dollars: 1500
- cutoff_time_et: 15:00
- daily_loss_limit: 300
- daily_loss_limit_pct: 0.03
- dte_max: 1
- dte_min: 0
- enabled: True
- entry_slippage: 0.01
- execution_mode: live
- exit_slippage: 0.01
- exposure_cap_pct: 0.15
- features_enabled: False
- hold_max_seconds: 5400
- hold_min_seconds: 600
- horizon: intraday
- max_open_trades: 1
- max_spread_pct: 0.15
- min_source_trades: 50
- name: Intraday Trend Pullback (LIVE)
- otm_pct: 0.003
- profit_target_pct: 0.55
- risk_per_trade_pct: 0.0075
- signal_mode: TREND_PULLBACK
- source_sim: SIM03
- stop_loss_pct: 0.3
- trailing_stop_activate_pct: 0.15
- trailing_stop_trail_pct: 0.08

### SIM01 — 0DTE Scalp Mean Reversion
- balance_start: 25000
- cutoff_time_et: 13:30
- daily_loss_limit_pct: 0.02
- dte_max: 0
- dte_min: 0
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.1
- features_enabled: False
- hold_max_seconds: 300
- hold_min_seconds: 60
- horizon: scalp
- max_open_trades: 2
- max_spread_pct: 0.15
- name: 0DTE Scalp Mean Reversion
- otm_pct: 0.008
- profit_target_pct: 0.35
- risk_per_trade_pct: 0.005
- signal_mode: MEAN_REVERSION
- stop_loss_pct: 0.25
- trailing_stop_activate_pct: None
- trailing_stop_trail_pct: None

### SIM02 — 0DTE Scalp Breakout
- balance_start: 25000
- cutoff_time_et: 13:30
- daily_loss_limit_pct: 0.02
- dte_max: 0
- dte_min: 0
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.1
- features_enabled: False
- hold_max_seconds: 600
- hold_min_seconds: 60
- horizon: scalp
- max_open_trades: 2
- max_spread_pct: 0.15
- name: 0DTE Scalp Breakout
- otm_pct: 0.01
- profit_target_pct: 0.35
- risk_per_trade_pct: 0.005
- signal_mode: BREAKOUT
- stop_loss_pct: 0.25
- trailing_stop_activate_pct: None
- trailing_stop_trail_pct: None

### SIM03 — Intraday Trend Pullback
- balance_start: 25000
- cutoff_time_et: 15:00
- daily_loss_limit: 300
- daily_loss_limit_pct: 0.03
- dte_max: 1
- dte_min: 0
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.15
- features_enabled: False
- hold_max_seconds: 5400
- hold_min_seconds: 600
- horizon: intraday
- max_open_trades: 1
- max_spread_pct: 0.15
- name: Intraday Trend Pullback
- otm_pct: 0.003
- profit_target_pct: 0.55
- risk_per_trade_pct: 0.0075
- signal_mode: TREND_PULLBACK
- stop_loss_pct: 0.3
- trailing_stop_activate_pct: 0.15
- trailing_stop_trail_pct: 0.08

### SIM04 — Intraday Range Fade
- balance_start: 25000
- cutoff_time_et: 15:00
- daily_loss_limit_pct: 0.03
- dte_max: 1
- dte_min: 0
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.15
- features_enabled: False
- hold_max_seconds: 2700
- hold_min_seconds: 300
- horizon: intraday
- max_open_trades: 2
- max_spread_pct: 0.15
- name: Intraday Range Fade
- otm_pct: 0.005
- profit_target_pct: 0.55
- risk_per_trade_pct: 0.0075
- signal_mode: MEAN_REVERSION
- stop_loss_pct: 0.3
- trailing_stop_activate_pct: 0.15
- trailing_stop_trail_pct: 0.08

### SIM05 — 1DTE Afternoon Continuation
- balance_start: 25000
- cutoff_time_et: 15:30
- daily_loss_limit_pct: 0.03
- dte_max: 1
- dte_min: 1
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.15
- features_enabled: False
- hold_max_seconds: 10800
- hold_min_seconds: 1800
- horizon: intraday
- max_open_trades: 2
- max_spread_pct: 0.15
- name: 1DTE Afternoon Continuation
- otm_pct: 0.003
- profit_target_pct: 0.55
- risk_per_trade_pct: 0.0075
- signal_mode: TREND_PULLBACK
- stop_loss_pct: 0.3
- trailing_stop_activate_pct: 0.15
- trailing_stop_trail_pct: 0.08

### SIM06 — 7-10 DTE Short Swing
- balance_start: 25000
- cutoff_time_et: 15:45
- daily_loss_limit_pct: 0.04
- dte_max: 10
- dte_min: 7
- entry_slippage: 0.008
- exit_slippage: 0.008
- exposure_cap_pct: 0.2
- features_enabled: False
- hold_max_seconds: 259200
- hold_min_seconds: 3600
- horizon: swing
- max_open_trades: 2
- max_spread_pct: 0.12
- name: 7-10 DTE Short Swing
- otm_pct: 0.0
- profit_target_pct: 0.9
- risk_per_trade_pct: 0.01
- signal_mode: SWING_TREND
- stop_loss_pct: 0.45
- trailing_stop_activate_pct: 0.12
- trailing_stop_trail_pct: 0.06

### SIM07 — 14-21 DTE Swing Trend
- balance_start: 25000
- cutoff_time_et: 15:45
- daily_loss_limit_pct: 0.04
- dte_max: 21
- dte_min: 14
- entry_slippage: 0.008
- exit_slippage: 0.008
- exposure_cap_pct: 0.2
- features_enabled: False
- hold_max_seconds: 604800
- hold_min_seconds: 86400
- horizon: swing
- max_open_trades: 2
- max_spread_pct: 0.12
- name: 14-21 DTE Swing Trend
- otm_pct: 0.0
- profit_target_pct: 1.0
- risk_per_trade_pct: 0.01
- signal_mode: SWING_TREND
- stop_loss_pct: 0.55
- trailing_stop_activate_pct: 0.12
- trailing_stop_trail_pct: 0.06

### SIM08 — Regime Filter Agent
- balance_start: 25000
- cutoff_time_et: 15:00
- daily_loss_limit_pct: 0.02
- dte_max: 1
- dte_min: 1
- entry_slippage: 0.008
- exit_slippage: 0.008
- exposure_cap_pct: 0.1
- features_enabled: False
- hold_max_seconds: 10800
- hold_min_seconds: 1800
- horizon: intraday
- max_open_trades: 1
- max_spread_pct: 0.12
- name: Regime Filter Agent
- otm_pct: 0.003
- profit_target_pct: 0.55
- regime_filter: TREND_ONLY
- risk_per_trade_pct: 0.0075
- signal_mode: TREND_PULLBACK
- stop_loss_pct: 0.3
- trailing_stop_activate_pct: 0.15
- trailing_stop_trail_pct: 0.08

### SIM09 — Opportunity Follower
- balance_start: 25000
- cutoff_time_et: 15:30
- daily_loss_limit_pct: 0.03
- dte_max: 7
- dte_min: 0
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.15
- features_enabled: False
- hold_max_seconds: 86400
- hold_min_seconds: 300
- horizon: adaptive
- max_open_trades: 1
- max_spread_pct: 0.15
- name: Opportunity Follower
- otm_pct: 0.005
- profit_target_pct: 0.6
- risk_per_trade_pct: 0.0075
- signal_mode: OPPORTUNITY
- stop_loss_pct: 0.4
- trailing_stop_activate_pct: 0.12
- trailing_stop_trail_pct: 0.06

### SIM10 — ORB Breakout
- balance_start: 25000
- cutoff_time_et: 11:00
- daily_loss_limit_pct: 0.02
- dte_max: 0
- dte_min: 0
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.1
- features_enabled: True
- hold_max_seconds: 900
- hold_min_seconds: 60
- horizon: scalp
- iv_series_window: 200
- max_open_trades: 1
- max_spread_pct: 0.15
- name: ORB Breakout
- orb_minutes: 30
- otm_pct: 0.01
- profit_target_pct: 0.35
- require_trend_bias: False
- risk_per_trade_pct: 0.005
- signal_mode: ORB_BREAKOUT
- stop_loss_pct: 0.25
- trailing_stop_activate_pct: None
- trailing_stop_trail_pct: None
- vol_z_min: 1.0
- zscore_window: 30

### SIM11 — Vol Expansion Trend
- atr_expansion_min: 1.2
- balance_start: 25000
- cutoff_time_et: 15:30
- daily_loss_limit_pct: 0.03
- dte_max: 3
- dte_min: 1
- entry_slippage: 0.01
- exit_slippage: 0.01
- exposure_cap_pct: 0.15
- features_enabled: True
- hold_max_seconds: 7200
- hold_min_seconds: 300
- horizon: intraday
- iv_series_window: 200
- max_open_trades: 2
- max_spread_pct: 0.18
- name: Vol Expansion Trend
- otm_pct: 0.01
- profit_target_pct: 0.6
- risk_per_trade_pct: 0.0075
- signal_mode: TREND_PULLBACK
- stop_loss_pct: 0.35
- trailing_stop_activate_pct: 0.12
- trailing_stop_trail_pct: 0.06
- zscore_window: 30
