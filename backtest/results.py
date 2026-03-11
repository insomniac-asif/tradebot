"""backtest/results.py -- Result dataclasses."""
from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class BacktestTrade:
    run_number: int
    trade_num: int
    date: str
    entry_time: str
    exit_time: str
    direction: str
    symbol: str
    contract: str
    entry_price: float
    exit_price: float
    qty: int
    pnl: float
    pnl_pct: float
    balance_before: float
    balance_after: float
    exit_reason: str
    signal_mode: str
    # Context fields for optimizer
    regime: str = ""
    entry_hour: int = 0
    entry_minute: int = 0
    day_of_week: int = 0       # 0=Mon, 4=Fri
    day_of_week_name: str = ""
    confidence: float = 0.0
    holding_seconds: int = 0


@dataclass
class BacktestRun:
    sim_profile: str
    signal_mode: str
    symbol: str
    run_number: int
    start_date: str
    end_date: str
    starting_balance: float
    final_balance: float
    peak_balance: float
    outcome: str  # "BLOWN" | "DATA_EXHAUSTED"
    hit_target: bool
    target_hit_date: str | None
    total_trades: int
    wins: int
    losses: int
    win_rate: float
    total_pnl: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    max_drawdown_pct: float
    max_drawdown_dollars: float
    days_active: int
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)


@dataclass
class BacktestSummary:
    sim_profile: str
    signal_mode: str
    symbol: str
    total_runs: int
    blown_count: int
    target_hit_count: int
    best_run_number: int
    worst_run_number: int
    avg_trades_per_run: float
    avg_win_rate: float
    avg_max_drawdown: float
    runs: list = field(default_factory=list)
