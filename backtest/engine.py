"""
backtest/engine.py
Fully synchronous historical backtesting engine.

Design:
- Iterates over 1-minute stock bars bar-by-bar
- On each bar, checks entry signal for each symbol in the profile's symbols list
- If signal fires: selects an option contract, fetches its bars from Alpaca/cache
- Tracks open position bar-by-bar, applying exit conditions
- Respects death_threshold and target_hit tracking
- Multiple runs: each "run" starts at balance_start=500, ends when blown (<= death_threshold)
  or data is exhausted; then resets to 500 and continues from the same bar
- Applies 2% slippage on entry (entry * 1.02), 2% slippage on exit (exit * 0.98)
"""
from __future__ import annotations

import logging
import math
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, date, timedelta
from typing import Optional

import pandas as pd
import pytz

from backtest.data_fetcher import fetch_stock_bars, fetch_option_bars, build_occ_symbol
from backtest.signal_adapter import get_signal, compute_features_for_backtest, _prepare_df_with_indicators
from backtest.exit_adapter import check_exit_conditions
from backtest.results import BacktestTrade, BacktestRun, BacktestSummary

ENTRY_SLIPPAGE = 0.02   # 2% worse than mid on entry
EXIT_SLIPPAGE = 0.02    # 2% worse than mid on exit

MARKET_OPEN = dt_time(9, 31)
ENTRY_CUTOFF = dt_time(15, 45)  # No new entries after this time
EOD_CLOSE = dt_time(15, 58)     # Force close all at this time

DEFAULT_BALANCE_START = 500.0
DEFAULT_DEATH_THRESHOLD = 25.0
TARGET_BALANCE = 10_000.0

BARS_WARMUP = 30  # Minimum bars needed before signal evaluation


def _get_et_time(ts) -> dt_time:
    """Extract time from a Timestamp or datetime (assumed ET naive or converted)."""
    if isinstance(ts, (pd.Timestamp, datetime)):
        return ts.time()
    return dt_time(0, 0)


def _is_trading_bar(ts) -> bool:
    """Return True if this bar is within trading hours for entries."""
    t = _get_et_time(ts)
    return MARKET_OPEN <= t <= ENTRY_CUTOFF


def _compute_regime(df: pd.DataFrame) -> str:
    """
    Compute a simple regime label from the last N bars.
    TREND if EMA9 > EMA20 or EMA9 < EMA20 and abs spread > 0.05%
    RANGE otherwise
    """
    if df is None or len(df) < 20:
        return "TREND"  # Default to TREND so signals fire
    try:
        last = df.iloc[-1]
        ema9 = float(last.get("ema9", 0) or 0)
        ema20 = float(last.get("ema20", 0) or 0)
        if ema20 == 0:
            return "TREND"
        spread = abs(ema9 - ema20) / ema20
        if spread > 0.0005:
            return "TREND"
        return "RANGE"
    except Exception:
        return "TREND"


def _check_regime_filter(profile: dict, regime: str) -> bool:
    """Return True if regime passes the profile's regime_filter."""
    regime_filter = profile.get("regime_filter")
    if regime_filter is None:
        return True
    if isinstance(regime_filter, list):
        return regime in regime_filter
    if regime_filter == "TREND_ONLY":
        return regime == "TREND"
    if regime_filter == "RANGE_ONLY":
        return regime == "RANGE"
    if regime_filter == "VOLATILE_ONLY":
        return regime == "VOLATILE"
    return True


def _select_option_strike(underlying_price: float, direction: str, otm_pct: float) -> float:
    """Select ATM/OTM strike as a whole-dollar integer."""
    if direction == "BULLISH":
        base = underlying_price * (1 + otm_pct)
    else:
        base = underlying_price * (1 - otm_pct)
    return float(round(base))


def _select_expiry(trade_date: date, dte_min: int, dte_max: int) -> Optional[date]:
    """
    Select the nearest valid expiry date (weekday) within [dte_min, dte_max] trading days.
    Prefers dte_min + 1 to avoid same-day expiry issues.
    """
    candidates = []
    trading_dte = 0
    expiry_date = trade_date
    while trading_dte <= dte_max + 2:
        if expiry_date.weekday() < 5:  # weekday only
            if dte_min <= trading_dte <= dte_max:
                candidates.append((expiry_date, trading_dte))
            trading_dte += 1
        expiry_date += timedelta(days=1)

    if not candidates:
        return None
    # Prefer shortest DTE
    return candidates[0][0]


def _position_size(balance: float, fill_price: float, profile: dict) -> tuple:
    """
    Compute qty using small-account sizing (same as sim_account_mode.compute_small_account_qty).
    Returns (qty, risk_dollars, block_reason).
    """
    if fill_price <= 0:
        return 0, 0.0, "invalid_fill_price"

    death_threshold = float(profile.get("death_threshold", DEFAULT_DEATH_THRESHOLD))
    if balance <= death_threshold:
        return 0, 0.0, "balance_below_death_threshold"

    risk_pct = float(profile.get("risk_per_trade_pct", 0.02))
    max_pos_pct = float(profile.get("max_position_pct", 0.15))

    one_contract_cost = fill_price * 100
    max_notional = balance * max_pos_pct

    if one_contract_cost > max_notional:
        return 0, 0.0, "contract_too_expensive_for_account"

    risk_dollars = balance * risk_pct
    min_risk = max(3.0, balance * 0.01)
    risk_dollars = max(risk_dollars, min_risk)

    qty = max(1, math.floor(risk_dollars / one_contract_cost))
    while qty > 1 and (fill_price * qty * 100) > max_notional:
        qty -= 1

    return qty, risk_dollars, None


@dataclass
class _OpenTrade:
    trade_id: str
    entry_time: str
    entry_price: float
    qty: int
    direction: str
    symbol: str
    contract: str
    expiry: str
    stop_loss_pct: float
    hold_min_seconds: float
    hold_max_seconds: float
    balance_before: float
    peak_price: float = 0.0
    trailing_stop_activated: bool = False
    trailing_stop_high: float = 0.0
    tp2_activated: bool = False
    tp2_target_pct: float = 0.0


class BacktestEngine:
    """
    Synchronous backtesting engine for a single sim profile.

    Usage:
        engine = BacktestEngine("SIM03", profile, start_date="2024-01-01", end_date="2024-12-31")
        summary = engine.run()
    """

    def __init__(
        self,
        profile_id: str,
        profile: dict,
        start_date: str,
        end_date: str,
        max_runs: int = 50,
        verbose: bool = True,
    ):
        self.profile_id = profile_id
        self.profile = profile
        self.start_date = start_date
        self.end_date = end_date
        self.max_runs = max_runs
        self.verbose = verbose

        self.signal_mode = profile.get("signal_mode", "TREND_PULLBACK")
        self.balance_start = float(profile.get("balance_start", DEFAULT_BALANCE_START))
        self.death_threshold = float(profile.get("death_threshold", DEFAULT_DEATH_THRESHOLD))
        self.max_daily_trades = profile.get("max_daily_trades") or profile.get("max_open_trades", 99)

        # Resolve symbol list (use 'symbols' key, or fall back to 'symbol', or SPY)
        symbols_raw = profile.get("symbols")
        if symbols_raw and isinstance(symbols_raw, list):
            self.symbols = [str(s).upper() for s in symbols_raw]
        elif profile.get("symbol"):
            self.symbols = [str(profile["symbol"]).upper()]
        elif profile.get("underlying"):
            self.symbols = [str(profile["underlying"]).upper()]
        elif profile.get("underlying_symbol"):
            self.symbols = [str(profile["underlying_symbol"]).upper()]
        else:
            self.symbols = ["SPY"]

        # Filter to tradeable symbols (have options)
        self._tradeable = [
            s for s in self.symbols
            if s in {"SPY", "QQQ", "IWM", "VXX", "TSLA", "AAPL", "NVDA", "MSFT"}
        ]
        if not self._tradeable:
            self._tradeable = self.symbols[:1] if self.symbols else ["SPY"]

        # Cache: {(contract, date_str): pd.DataFrame}
        self._option_cache: dict = {}

        self.runs: list[BacktestRun] = []
        self._current_run: Optional[BacktestRun] = None

    def run(self) -> BacktestSummary:
        """Execute the full backtest. Returns BacktestSummary."""
        # Use only the first symbol in the list for simplicity
        # (multi-symbol would be done in runner.py by iterating per-symbol)
        primary_symbol = self._tradeable[0]

        if self.verbose:
            print(f"\n[{self.profile_id}] Fetching {primary_symbol} bars {self.start_date} -> {self.end_date}...")

        stock_df = fetch_stock_bars(primary_symbol, self.start_date, self.end_date)
        if stock_df is None or stock_df.empty:
            logging.warning("backtest_no_data: %s %s", self.profile_id, primary_symbol)
            return self._build_summary(primary_symbol)

        # Add technical indicators
        stock_df = _prepare_df_with_indicators(stock_df)
        if stock_df is None or stock_df.empty:
            return self._build_summary(primary_symbol)

        if self.verbose:
            print(f"  Got {len(stock_df)} bars. Starting backtest...")

        # All bars as list for fast iteration
        all_bars = list(stock_df.iterrows())
        total_bars = len(all_bars)

        # State
        run_number = 0
        balance = self.balance_start
        peak_balance = self.balance_start
        open_trade: Optional[_OpenTrade] = None
        run_trades: list[BacktestTrade] = []
        equity_curve: list[dict] = []
        hit_target = False
        target_hit_date: Optional[str] = None
        daily_trades = 0
        last_trade_date: Optional[date] = None
        run_start_bar_idx = 0

        run_number += 1
        run_start_ts = all_bars[0][0] if all_bars else None

        for bar_idx in range(total_bars):
            ts, row = all_bars[bar_idx]

            # Record equity curve point (every 30 bars)
            if bar_idx % 30 == 0:
                equity_curve.append({
                    "timestamp": str(ts),
                    "balance": round(balance, 2),
                    "run_number": run_number,
                })

            current_date = ts.date() if isinstance(ts, (pd.Timestamp, datetime)) else None

            # Reset daily trade counter at day boundary
            if current_date and current_date != last_trade_date:
                daily_trades = 0
                last_trade_date = current_date

            close_price = float(row.get("close", 0) or 0)
            if close_price <= 0:
                continue

            bar_time = _get_et_time(ts)

            # ── Check existing open trade ────────────────────────────────
            if open_trade is not None:
                # Get current option price from cached bars
                opt_price = self._get_option_price_at(
                    open_trade.contract,
                    str(current_date) if current_date else "",
                    ts,
                )

                if opt_price is None or opt_price <= 0:
                    # No option data — use a synthetic price based on underlying move
                    # Simple proxy: option price decays with underlying
                    opt_price = open_trade.entry_price * max(0.05, (1 + (close_price / float(row.get("open", close_price)) - 1) * 5))

                # Convert open trade to dict for exit_adapter compatibility
                trade_dict = {
                    "trade_id": open_trade.trade_id,
                    "entry_price": open_trade.entry_price,
                    "stop_loss_pct": open_trade.stop_loss_pct,
                    "hold_min_seconds": open_trade.hold_min_seconds,
                    "hold_max_seconds": open_trade.hold_max_seconds,
                    "expiry": open_trade.expiry,
                    "peak_price": open_trade.peak_price,
                    "trailing_stop_activated": open_trade.trailing_stop_activated,
                    "trailing_stop_high": open_trade.trailing_stop_high,
                    "tp2_activated": open_trade.tp2_activated,
                    "tp2_target_pct": open_trade.tp2_target_pct,
                }

                entry_dt = datetime.fromisoformat(open_trade.entry_time)
                if entry_dt.tzinfo is None and isinstance(ts, pd.Timestamp) and ts.tzinfo is not None:
                    import pytz as _pytz
                    entry_dt = _pytz.timezone("America/New_York").localize(entry_dt)
                elapsed = (ts.to_pydatetime() if isinstance(ts, pd.Timestamp) else ts) - entry_dt
                if elapsed.total_seconds() < 0:
                    elapsed_seconds = 0
                else:
                    elapsed_seconds = elapsed.total_seconds()

                should_exit, exit_reason, exit_price = check_exit_conditions(
                    trade_dict,
                    self.profile,
                    opt_price,
                    elapsed_seconds,
                    ts,
                )

                # Sync mutable state back from trade_dict
                open_trade.peak_price = float(trade_dict.get("peak_price") or open_trade.peak_price)
                open_trade.trailing_stop_activated = bool(trade_dict.get("trailing_stop_activated", False))
                open_trade.trailing_stop_high = float(trade_dict.get("trailing_stop_high") or 0)

                if should_exit and exit_reason != "still_open":
                    # Apply exit slippage (worse price on exit)
                    actual_exit_price = exit_price * (1 - EXIT_SLIPPAGE)
                    pnl = (actual_exit_price - open_trade.entry_price) * open_trade.qty * 100
                    balance_after = balance + pnl
                    pnl_pct = (actual_exit_price - open_trade.entry_price) / open_trade.entry_price

                    bt_trade = BacktestTrade(
                        run_number=run_number,
                        trade_num=len(run_trades) + 1,
                        date=str(current_date),
                        entry_time=open_trade.entry_time,
                        exit_time=str(ts),
                        direction=open_trade.direction,
                        symbol=open_trade.symbol,
                        contract=open_trade.contract,
                        entry_price=open_trade.entry_price,
                        exit_price=round(actual_exit_price, 4),
                        qty=open_trade.qty,
                        pnl=round(pnl, 4),
                        pnl_pct=round(pnl_pct, 4),
                        balance_before=round(open_trade.balance_before, 2),
                        balance_after=round(balance_after, 2),
                        exit_reason=exit_reason,
                        signal_mode=self.signal_mode,
                    )
                    run_trades.append(bt_trade)
                    equity_curve.append({
                        "timestamp": str(ts),
                        "balance": round(balance_after, 2),
                        "run_number": run_number,
                        "trade_num": bt_trade.trade_num,
                        "pnl": round(pnl, 4),
                    })

                    balance = balance_after
                    if balance > peak_balance:
                        peak_balance = balance
                    open_trade = None

                    # Check target hit
                    if not hit_target and balance >= TARGET_BALANCE:
                        hit_target = True
                        target_hit_date = str(current_date)
                        if self.verbose:
                            print(f"  [{self.profile_id}] Run #{run_number}: TARGET HIT ${balance:.0f} on {current_date}")

                    # Check death
                    if balance <= self.death_threshold:
                        if self.verbose:
                            print(f"  [{self.profile_id}] Run #{run_number}: BLOWN ${balance:.2f} after {len(run_trades)} trades")

                        # Finalize this run
                        self._finalize_run(
                            run_number=run_number,
                            symbol=primary_symbol,
                            balance=balance,
                            peak_balance=peak_balance,
                            outcome="BLOWN",
                            hit_target=hit_target,
                            target_hit_date=target_hit_date,
                            trades=run_trades,
                            equity_curve=equity_curve,
                            start_ts=run_start_ts,
                            end_ts=ts,
                        )

                        # Reset for next run if max_runs not exceeded
                        run_number += 1
                        if run_number > self.max_runs:
                            break

                        balance = self.balance_start
                        peak_balance = self.balance_start
                        run_trades = []
                        equity_curve = []
                        hit_target = False
                        target_hit_date = None
                        run_start_ts = ts
                        daily_trades = 0
                        open_trade = None
                        continue

                    continue  # Done with this bar (trade was closed)

                continue  # Trade still open, keep monitoring

            # ── No open trade: check for new entry ──────────────────────
            # Only enter during trading hours
            if not _is_trading_bar(ts):
                continue

            # Need enough warmup bars
            if bar_idx < BARS_WARMUP:
                continue

            # Daily trade limit
            if daily_trades >= self.max_daily_trades:
                continue

            # Regime check
            window_df = stock_df.iloc[max(0, bar_idx - 200):bar_idx + 1]
            regime = _compute_regime(window_df)
            if not _check_regime_filter(self.profile, regime):
                continue

            # Compute features if needed
            feature_snapshot = compute_features_for_backtest(window_df, self.profile, self.signal_mode)

            # Derive signal
            direction, underlying_price, meta = get_signal(
                window_df,
                self.signal_mode,
                self.profile,
                feature_snapshot=feature_snapshot,
            )

            if direction is None or underlying_price is None:
                continue

            underlying_price = float(underlying_price)
            if underlying_price <= 0:
                underlying_price = close_price

            # Select expiry
            trade_date = current_date
            dte_min = int(self.profile.get("dte_min", 0))
            dte_max = int(self.profile.get("dte_max", 1))
            expiry = _select_expiry(trade_date, dte_min, dte_max)
            if expiry is None:
                continue

            # Select strike
            otm_pct = float(self.profile.get("otm_pct", 0.005))
            strike = _select_option_strike(underlying_price, direction, otm_pct)

            # Build OCC symbol
            contract = build_occ_symbol(primary_symbol, expiry, direction, strike)

            # Fetch option bars for this contract on this date
            date_str = str(trade_date)
            opt_df = self._get_or_fetch_option_bars(contract, date_str)

            if opt_df is None or opt_df.empty:
                continue

            # Find option price at current bar time
            entry_opt_price = self._get_option_price_at(contract, date_str, ts)
            if entry_opt_price is None or entry_opt_price <= 0:
                continue

            # Apply entry slippage (pay more on entry)
            fill_price = entry_opt_price * (1 + ENTRY_SLIPPAGE)

            # Position sizing
            qty, risk_dollars, block_reason = _position_size(balance, fill_price, self.profile)
            if block_reason or qty <= 0:
                continue

            # Reserve notional (deduct from balance)
            notional = fill_price * qty * 100
            if notional > balance:
                continue

            balance -= notional

            # Record trade open
            trade_id = str(uuid.uuid4())[:8]
            open_trade = _OpenTrade(
                trade_id=trade_id,
                entry_time=str(ts),
                entry_price=fill_price,
                qty=qty,
                direction=direction,
                symbol=primary_symbol,
                contract=contract,
                expiry=str(expiry),
                stop_loss_pct=float(self.profile.get("stop_loss_pct", 0.30)),
                hold_min_seconds=float(self.profile.get("hold_min_seconds", 60)),
                hold_max_seconds=float(self.profile.get("hold_max_seconds", 3600)),
                balance_before=balance + notional,  # balance BEFORE deducting notional
                peak_price=fill_price,
            )
            daily_trades += 1

        # End of data: close any open trade at last price
        if open_trade is not None and all_bars:
            last_ts, last_row = all_bars[-1]
            last_opt_price = self._get_option_price_at(
                open_trade.contract,
                str(last_ts.date() if isinstance(last_ts, (pd.Timestamp, datetime)) else date.today()),
                last_ts,
            )
            if last_opt_price is None or last_opt_price <= 0:
                last_opt_price = open_trade.entry_price * 0.80
            actual_exit = last_opt_price * (1 - EXIT_SLIPPAGE)
            pnl = (actual_exit - open_trade.entry_price) * open_trade.qty * 100
            balance_after = balance + pnl

            bt_trade = BacktestTrade(
                run_number=run_number,
                trade_num=len(run_trades) + 1,
                date=str(last_ts.date()) if isinstance(last_ts, (pd.Timestamp, datetime)) else "",
                entry_time=open_trade.entry_time,
                exit_time=str(last_ts),
                direction=open_trade.direction,
                symbol=open_trade.symbol,
                contract=open_trade.contract,
                entry_price=open_trade.entry_price,
                exit_price=round(actual_exit, 4),
                qty=open_trade.qty,
                pnl=round(pnl, 4),
                pnl_pct=round((actual_exit - open_trade.entry_price) / open_trade.entry_price, 4),
                balance_before=round(open_trade.balance_before, 2),
                balance_after=round(balance_after, 2),
                exit_reason="data_exhausted_close",
                signal_mode=self.signal_mode,
            )
            run_trades.append(bt_trade)
            balance = balance_after
            if balance > peak_balance:
                peak_balance = balance

        # Finalize last run
        if run_trades or run_number == 1:
            last_ts_val = all_bars[-1][0] if all_bars else None
            self._finalize_run(
                run_number=run_number,
                symbol=primary_symbol,
                balance=balance,
                peak_balance=peak_balance,
                outcome="DATA_EXHAUSTED",
                hit_target=hit_target,
                target_hit_date=target_hit_date,
                trades=run_trades,
                equity_curve=equity_curve,
                start_ts=run_start_ts,
                end_ts=last_ts_val,
            )

        if self.verbose:
            total_trades_all = sum(len(r.trades) for r in self.runs)
            total_wins = sum(r.wins for r in self.runs)
            print(f"  [{self.profile_id}] Backtest complete: {len(self.runs)} runs, {total_trades_all} total trades")
            if total_trades_all > 0:
                print(f"    Overall win rate: {total_wins/total_trades_all*100:.1f}%")

        return self._build_summary(primary_symbol)

    def _get_or_fetch_option_bars(self, contract: str, date_str: str) -> Optional[pd.DataFrame]:
        """Get option bars from cache or fetch from Alpaca."""
        cache_key = (contract, date_str)
        if cache_key in self._option_cache:
            return self._option_cache[cache_key]

        df = fetch_option_bars(contract, date_str)
        self._option_cache[cache_key] = df
        return df

    def _get_option_price_at(
        self,
        contract: str,
        date_str: str,
        ts,
    ) -> Optional[float]:
        """Look up the option close price at or nearest to the given timestamp."""
        df = self._get_or_fetch_option_bars(contract, date_str)
        if df is None or df.empty:
            return None

        # ts may be tz-aware or naive; normalize to naive for comparison
        ts_naive = ts
        if isinstance(ts, pd.Timestamp):
            if ts.tzinfo is not None:
                ts_naive = ts.tz_localize(None)
        elif isinstance(ts, datetime):
            if ts.tzinfo is not None:
                import pytz as _pytz
                ts_naive = ts.astimezone(_pytz.timezone("America/New_York")).replace(tzinfo=None)
            ts_naive = pd.Timestamp(ts_naive)

        # df.index should be naive ET after fetch_option_bars
        idx = df.index
        if idx.tz is not None:
            idx = idx.tz_localize(None)

        # Find closest bar at or before ts_naive
        mask = idx <= ts_naive
        if not mask.any():
            # Use first bar if ts is before all bars
            if not df.empty:
                return float(df["close"].iloc[0])
            return None
        row = df.loc[idx[mask][-1]]
        price = float(row.get("close", 0) or 0)
        return price if price > 0 else None

    def _finalize_run(
        self,
        run_number: int,
        symbol: str,
        balance: float,
        peak_balance: float,
        outcome: str,
        hit_target: bool,
        target_hit_date: Optional[str],
        trades: list[BacktestTrade],
        equity_curve: list[dict],
        start_ts,
        end_ts,
    ):
        """Build a BacktestRun and append to self.runs."""
        wins = [t for t in trades if t.pnl > 0]
        losses = [t for t in trades if t.pnl <= 0]
        total = len(trades)
        win_rate = len(wins) / total if total else 0.0
        total_pnl = sum(t.pnl for t in trades)
        avg_win = sum(t.pnl for t in wins) / len(wins) if wins else 0.0
        avg_loss = sum(t.pnl for t in losses) / len(losses) if losses else 0.0
        gross_profit = sum(t.pnl for t in wins)
        gross_loss = abs(sum(t.pnl for t in losses))
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

        # Max drawdown from equity curve
        max_dd_pct = 0.0
        max_dd_dollars = 0.0
        if trades:
            running = self.balance_start
            peak = self.balance_start
            for t in trades:
                running = t.balance_after
                if running > peak:
                    peak = running
                dd_d = peak - running
                dd_pct = dd_d / peak if peak > 0 else 0.0
                if dd_pct > max_dd_pct:
                    max_dd_pct = dd_pct
                    max_dd_dollars = dd_d

        # Days active
        days_active = 0
        if start_ts is not None and end_ts is not None:
            try:
                s = start_ts.to_pydatetime() if isinstance(start_ts, pd.Timestamp) else start_ts
                e = end_ts.to_pydatetime() if isinstance(end_ts, pd.Timestamp) else end_ts
                days_active = max(0, (e.date() - s.date()).days) if hasattr(s, "date") else 0
            except Exception:
                pass

        # Serialize trades to dicts for JSON compatibility
        trades_as_dicts = [
            {
                "run_number": t.run_number,
                "trade_num": t.trade_num,
                "date": t.date,
                "entry_time": t.entry_time,
                "exit_time": t.exit_time,
                "direction": t.direction,
                "symbol": t.symbol,
                "option_symbol": t.contract,
                "entry_price": t.entry_price,
                "exit_price": t.exit_price,
                "qty": t.qty,
                "realized_pnl_dollars": t.pnl,
                "pnl_pct": t.pnl_pct,
                "balance_before": t.balance_before,
                "balance_after_trade": t.balance_after,
                "exit_reason": t.exit_reason,
                "signal_mode": t.signal_mode,
            }
            for t in trades
        ]

        run = BacktestRun(
            sim_profile=self.profile_id,
            signal_mode=self.signal_mode,
            symbol=symbol,
            run_number=run_number,
            start_date=str(start_ts.date()) if start_ts is not None and hasattr(start_ts, "date") else self.start_date,
            end_date=str(end_ts.date()) if end_ts is not None and hasattr(end_ts, "date") else self.end_date,
            starting_balance=self.balance_start,
            final_balance=round(balance, 4),
            peak_balance=round(peak_balance, 4),
            outcome=outcome,
            hit_target=hit_target,
            target_hit_date=target_hit_date,
            total_trades=total,
            wins=len(wins),
            losses=len(losses),
            win_rate=round(win_rate, 4),
            total_pnl=round(total_pnl, 4),
            avg_win=round(avg_win, 4),
            avg_loss=round(avg_loss, 4),
            profit_factor=round(profit_factor, 4),
            max_drawdown_pct=round(max_dd_pct, 4),
            max_drawdown_dollars=round(max_dd_dollars, 4),
            days_active=days_active,
            trades=trades_as_dicts,
            equity_curve=equity_curve,
        )
        self.runs.append(run)

    def _build_summary(self, symbol: str) -> BacktestSummary:
        """Build BacktestSummary from all completed runs."""
        if not self.runs:
            return BacktestSummary(
                sim_profile=self.profile_id,
                signal_mode=self.signal_mode,
                symbol=symbol,
                total_runs=0,
                blown_count=0,
                target_hit_count=0,
                best_run_number=0,
                worst_run_number=0,
                avg_trades_per_run=0.0,
                avg_win_rate=0.0,
                avg_max_drawdown=0.0,
                runs=[],
            )

        blown = [r for r in self.runs if r.outcome == "BLOWN"]
        target_hits = [r for r in self.runs if r.hit_target]
        total_trades_list = [r.total_trades for r in self.runs if r.total_trades > 0]
        avg_trades = sum(total_trades_list) / len(total_trades_list) if total_trades_list else 0.0
        avg_wr = sum(r.win_rate for r in self.runs) / len(self.runs)
        avg_dd = sum(r.max_drawdown_pct for r in self.runs) / len(self.runs)

        # Best run: highest final balance
        best_run = max(self.runs, key=lambda r: r.final_balance)
        worst_run = min(self.runs, key=lambda r: r.final_balance)

        # Serialize runs to dicts
        runs_dicts = []
        for r in self.runs:
            runs_dicts.append({
                "sim_profile": r.sim_profile,
                "signal_mode": r.signal_mode,
                "symbol": r.symbol,
                "run_number": r.run_number,
                "start_date": r.start_date,
                "end_date": r.end_date,
                "starting_balance": r.starting_balance,
                "final_balance": r.final_balance,
                "peak_balance": r.peak_balance,
                "outcome": r.outcome,
                "hit_target": r.hit_target,
                "target_hit_date": r.target_hit_date,
                "total_trades": r.total_trades,
                "wins": r.wins,
                "losses": r.losses,
                "win_rate": r.win_rate,
                "total_pnl": r.total_pnl,
                "avg_win": r.avg_win,
                "avg_loss": r.avg_loss,
                "profit_factor": r.profit_factor,
                "max_drawdown_pct": r.max_drawdown_pct,
                "max_drawdown_dollars": r.max_drawdown_dollars,
                "days_active": r.days_active,
                "trades": r.trades,
                "equity_curve": r.equity_curve,
            })

        return BacktestSummary(
            sim_profile=self.profile_id,
            signal_mode=self.signal_mode,
            symbol=symbol,
            total_runs=len(self.runs),
            blown_count=len(blown),
            target_hit_count=len(target_hits),
            best_run_number=best_run.run_number,
            worst_run_number=worst_run.run_number,
            avg_trades_per_run=round(avg_trades, 1),
            avg_win_rate=round(avg_wr, 4),
            avg_max_drawdown=round(avg_dd, 4),
            runs=runs_dicts,
        )
