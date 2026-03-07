# ==============================
# INTERFACE LAYER ONLY
# ==============================

import os
import asyncio
import logging
import re
import csv
import json
import yaml
import discord
import pytz
from datetime import datetime, timedelta
from discord.ext import commands
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from typing import Any
import joblib

# -------- Interface --------
from interface.fmt import ab, lbl, A, pnl_col, conf_col, dir_col, regime_col, vol_col, delta_col, ml_col, exit_reason_col, balance_col, wr_col, tier_col, drawdown_col, signed_col
from interface.charting import generate_chart, generate_live_chart
from interface.health_monitor import start_heartbeat
from interface.watchers import (
    auto_trader,
    conviction_watcher,
    forecast_watcher,
    heart_monitor,
    prediction_grader,
    opportunity_watcher,
    get_decision_buffer_snapshot,
    preopen_check_loop,
    eod_open_trade_report_loop,
    option_chain_health_loop
    
)
from simulation.sim_watcher import (
    sim_entry_loop,
    sim_exit_loop,
    sim_eod_report_loop,
    sim_daily_summary_loop,
    sim_weekly_leaderboard_loop,
    sim_session_leaderboard_loop,
    set_sim_bot,
    get_sim_last_skip_state,
)
from core.md_state import set_md_enabled, get_md_state, md_needs_warning, set_md_auto
# -------- Core --------
from core.market_clock import market_is_open
from core.data_service import get_market_dataframe
from execution.option_executor import get_option_price
from core.session_scope import get_rth_session_view
from core.account_repository import load_account
from core.paths import DATA_DIR
from core.account_repository import load_account

# -------- Decision --------

# -------- Signal --------
from signals.conviction import calculate_conviction
from signals.opportunity import evaluate_opportunity
from signals.predictor import make_prediction
from signals.regime import get_regime
from signals.volatility import volatility_state

# -------- Analytics --------
from analytics.prediction_stats import calculate_accuracy
from analytics.run_stats import get_run_stats
from analytics.performance import get_paper_stats, get_career_stats
from analytics.equity_curve import generate_equity_curve
from analytics.risk_metrics import calculate_r_metrics, calculate_drawdown
from analytics.regime_expectancy import calculate_regime_expectancy
from analytics.expectancy import calculate_expectancy
from analytics.feature_importance import get_feature_importance
from analytics.ml_accuracy import ml_rolling_accuracy
from analytics.decision_analysis import analyze_decision_quality
from analytics.conviction_stats import update_expectancy
from analytics.feature_logger import FEATURE_FILE, FEATURE_HEADERS, ensure_feature_file
from analytics.prediction_stats import PRED_FILE, PRED_HEADERS
from simulation.sim_contract import select_sim_contract_with_reason

# -------- AI --------
from interface.ai_assistant import ask_ai
from interface.fmt import (
    ab,
    A,
    lbl,
    pnl_col,
    conf_col,
    dir_col,
    regime_col,
    vol_col,
    delta_col,
    ml_col,
    result_col,
    exit_reason_col,
    balance_col,
    wr_col,
    tier_col,
    drawdown_col,
    pct_col,
)
from research.train_ai import train_direction_model, train_edge_model
from logs.recorder import start_recorder_background
from core.startup_sync import perform_startup_broker_sync
from simulation.sim_portfolio import SimPortfolio


# ==============================
# CONFIG
# ==============================



logging.basicConfig(
    filename="system.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s:%(message)s"
)

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

if DISCORD_TOKEN is None or DISCORD_TOKEN.strip() == "":
    raise RuntimeError("DISCORD_TOKEN is missing or empty.")

intents = discord.Intents.default()
intents.message_content = True

PAPER_CHANNEL_ID = 1470599150843203636
ALERT_CHANNEL_ID = 1470846800423551230
FORECAST_CHANNEL_ID = 1470931720739098774
HEART_CHANNEL_ID = 1470992071514132562
EOD_REPORT_CHANNEL_ID = 1476863964473196586

BOT_TIMEZONE = "America/New_York"
ASK_CONTEXT_CACHE = {}

CONVICTION_HEADERS = [
    "time",
    "direction",
    "impulse",
    "follow",
    "price",
    "fwd_5m",
    "fwd_10m",
    "fwd_5m_price",
    "fwd_5m_time",
    "fwd_5m_status",
    "fwd_10m_price",
    "fwd_10m_time",
    "fwd_10m_status",
]
LEGACY_CONVICTION_HEADERS = [
    "time", "direction", "impulse", "follow", "price", "fwd_5m", "fwd_10m"
]
PREDICTION_REQUIRED_HEADERS = [
    "time", "timeframe", "direction", "confidence", "high", "low",
    "regime", "volatility", "session", "actual", "correct", "checked"
]
ACCOUNT_REQUIRED_KEYS = [
    "balance", "starting_balance", "open_trade", "trade_log", "wins",
    "losses", "day_trades", "risk_per_trade", "max_trade_size",
    "daily_loss", "max_daily_loss", "last_trade_day"
]


def _read_csv_headers(path):
    if not os.path.exists(path):
        return None
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        try:
            return next(reader)
        except StopIteration:
            return None


def run_startup_phase_gates():
    errors = []

    # 1) Timezone check
    try:
        tz = pytz.timezone(BOT_TIMEZONE)
        if tz.zone != BOT_TIMEZONE:
            errors.append(f"timezone_invalid:{tz.zone}")
    except Exception as e:
        errors.append(f"timezone_error:{e}")

    # 2) Env / config checks
    api_key = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")
    if not api_key or not secret_key:
        errors.append("alpaca_api_keys_missing")

    # 3) CSV header checks
    conviction_file = os.path.join(DATA_DIR, "conviction_expectancy.csv")
    pred_file = os.path.join(DATA_DIR, "predictions.csv")

    conviction_headers = _read_csv_headers(conviction_file)
    if conviction_headers not in (CONVICTION_HEADERS, LEGACY_CONVICTION_HEADERS):
        errors.append("conviction_csv_header_invalid")

    pred_headers = _read_csv_headers(pred_file)
    if pred_headers is None:
        errors.append("predictions_csv_header_missing")
    else:
        missing_pred = [h for h in PREDICTION_REQUIRED_HEADERS if h not in pred_headers]
        if missing_pred:
            errors.append(f"predictions_csv_header_missing_fields:{','.join(missing_pred)}")
        if pred_headers != PRED_HEADERS:
            try:
                with open(pred_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(PRED_HEADERS)
            except Exception:
                errors.append("predictions_csv_header_reset_failed")

    # 4) ML model load checks
    direction_model_path = os.path.join(DATA_DIR, "direction_model.pkl")
    edge_model_path = os.path.join(DATA_DIR, "edge_model.pkl")
    for model_path, model_name in [
        (direction_model_path, "direction_model"),
        (edge_model_path, "edge_model"),
    ]:
        if not os.path.exists(model_path):
            continue
        try:
            joblib.load(model_path)
        except Exception as e:
            errors.append(f"{model_name}_load_error:{e}")

    # 5) Account structure check
    account_file = os.path.join(DATA_DIR, "account.json")
    if not os.path.exists(account_file):
        errors.append("account_missing")
    else:
        try:
            with open(account_file, "r") as f:
                acc = json.load(f)
            missing_keys = [k for k in ACCOUNT_REQUIRED_KEYS if k not in acc]
            if missing_keys:
                errors.append(f"account_missing_keys:{','.join(missing_keys)}")
        except Exception as e:
            errors.append(f"account_read_error:{e}")

    return errors

class QQQBot(commands.Bot):

    async def safe_task(self, coro_func, *args):
        while True:
            try:
                await coro_func(*args)
            except Exception as e:
                logging.error(f"[CRASH] Background task {coro_func.__name__} crashed: {e}", exc_info=True)
                await asyncio.sleep(5)

    async def setup_hook(self):
        print("Launching background systems...")

        if not hasattr(self, "recorder_thread"):
            self.recorder_thread = start_recorder_background()

        self.trading_enabled = True
        self.startup_errors = run_startup_phase_gates()
        self.startup_notice_sent = False
        self.data_stale_state = False
        self.last_stale_warning_time = None
        self.data_integrity_state = False
        self.last_integrity_warning_time = None
        self.last_holiday_notice_date = None
        self.predictor_winrate_history = []
        self.predictor_baseline_winrate = None
        self.predictor_drift_state = False
        self.last_predictor_drift_warning_time = None
        self.recorder_stalled_state = False
        self.last_recorder_stall_warning_time = None
        self.block_reason_last_time = {}
        self.last_recorder_thread_dead_warning_time = None
        self.paper_channel_id = PAPER_CHANNEL_ID

        if self.startup_errors:
            self.trading_enabled = False
            for err in self.startup_errors:
                logging.error(f"startup_error: {err}")
        try:
            ensure_feature_file(reset_if_invalid=True)
        except Exception as e:
            logging.exception("feature_file_init_error: %s", e)

        await perform_startup_broker_sync(self)

        self.loop.create_task(
            self.safe_task(auto_trader, self, PAPER_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(conviction_watcher, self, ALERT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(forecast_watcher, self, FORECAST_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(heart_monitor, self, HEART_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(prediction_grader, self, PAPER_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(start_heartbeat)
        )
        self.loop.create_task(
            self.safe_task(opportunity_watcher, self, ALERT_CHANNEL_ID)
        )
        set_sim_bot(self)
        self.loop.create_task(
            self.safe_task(sim_entry_loop)
        )
        self.loop.create_task(
            self.safe_task(sim_exit_loop)
        )
        self.loop.create_task(
            self.safe_task(sim_eod_report_loop, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(sim_daily_summary_loop, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(sim_weekly_leaderboard_loop, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(sim_session_leaderboard_loop, 1478675253780807795)
        )
        self.loop.create_task(
            self.safe_task(preopen_check_loop, self, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(eod_open_trade_report_loop, self, EOD_REPORT_CHANNEL_ID)
        )
        self.loop.create_task(
            self.safe_task(option_chain_health_loop, self, EOD_REPORT_CHANNEL_ID)
        )
        

    async def safe_task_with_timeout(self, task_coro, name, timeout=10):
        try:
            await asyncio.wait_for(task_coro, timeout=timeout)
        except asyncio.TimeoutError:
            logging.error(f"[TIMEOUT] Task {name} exceeded timeout.")
        except Exception as e:
            logging.exception(f"[CRASH] Background task {name} crashed: {e}")


bot = QQQBot(command_prefix="!", intents=intents, help_command=None)
# ==============================
# START
# ==============================

@bot.event
async def on_ready():
    print(f"Bot online as {bot.user}")

print("TOKEN LOADED: ", DISCORD_TOKEN is not None)


BOT_START_TIME = datetime.now(pytz.timezone("US/Eastern"))


def _infer_embed_style(title: str | None, description: str | None):
    text = f"{title or ''} {description or ''}".lower()
    error_terms = ("error", "failed", "invalid", "unknown", "missing")
    warn_terms = ("warning", "warn", "blocked", "disabled", "limit", "skip")
    success_terms = ("success", "complete", "updated", "reset", "ok", "done")
    if any(t in text for t in error_terms):
        return 0xE74C3C, "❌"
    if any(t in text for t in warn_terms):
        return 0xF39C12, "⚠️"
    if any(t in text for t in success_terms):
        return 0x2ECC71, "✅"
    return 0x3498DB, "ℹ️"


def _maybe_prefix_emoji(title: str | None, emoji: str) -> str | None:
    if not title:
        return title
    emoji_prefixes = ("✅", "❌", "⚠️", "ℹ️", "📘", "📋", "📈", "📊", "🧠", "🖥", "🤖", "📥", "📤", "🧪", "🧾")
    if title.startswith(emoji_prefixes):
        return title
    return f"{emoji} {title}"

def _add_field_icons(name: str) -> str:
    icon_map = {
        "Total Trades": "📦",
        "Open Trades": "📂",
        "Win Rate": "🎯",
        "Total PnL": "💰",
        "Avg Win": "📈",
        "Avg Loss": "📉",
        "Expectancy": "🧮",
        "Best Trade": "🥇",
        "Worst Trade": "🧯",
        "Max Drawdown": "📉",
        "Regime Breakdown": "🧭",
        "Time Bucket Breakdown": "🕒",
        "Exit Reasons": "🚪",
        "Last Trade": "🕘",
        "Risk / Balance": "🧰",
        "Details": "📋",
        "Context": "🧠",
        "ML": "🤖",
        "Reason": "⚠️",
        "Status": "✅",
        "Trader": "🧭",
        "Sims": "🧪",
        "Start Balance": "💵",
        "Strike": "🎯",
        "Premium": "💵",
        "Contracts": "📦",
        "Total Cost": "🧾",
        "Expiry": "📅",
        "Market State": "🧠",
        "Structure Context": "📐",
        "Final Grade": "🏆",
        "Market": "🟢",
        "System Health": "🧠",
        "System Diagnostics": "🧪",
        "Balance": "💰",
        "Trade Activity": "📒",
        "Background Systems": "⚙️",
        "Analytics Status": "📊",
        "Last Price": "📈",
        "Data Freshness": "⏱",
        "Option Snapshot": "🧩",
    }
    icon = icon_map.get(name)
    if icon and not name.startswith(icon):
        return f"{icon} {name}"
    return name

def _format_ts(value) -> str:
    eastern = pytz.timezone("America/New_York")
    if value is None:
        return "N/A"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return value
    else:
        return str(value)
    if dt.tzinfo is None:
        dt = eastern.localize(dt)
    else:
        dt = dt.astimezone(eastern)
    return dt.strftime("%Y-%m-%d %H:%M:%S ET")

def _format_pct_signed(val) -> str:
    try:
        num = float(val) * 100
        return f"{'+' if num >= 0 else '-'}{abs(num):.1f}%"
    except (TypeError, ValueError):
        return "N/A"

def _format_duration_short(seconds) -> str:
    try:
        total = int(seconds)
    except (TypeError, ValueError):
        return "N/A"
    if total < 0:
        return "N/A"
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"

def _load_sim_profiles() -> dict:
    sim_config_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
    )
    try:
        with open(sim_config_path, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

def _collect_sim_metrics():
    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _parse_ts(val):
        try:
            if not val:
                return None
            return datetime.fromisoformat(str(val))
        except Exception:
            return None

    profiles = _load_sim_profiles()
    if not profiles:
        return [], {}

    metrics = []
    for sim_key, profile in profiles.items():
        try:
            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                continue
            sim = SimPortfolio(sim_key, profile)
            sim.load()
            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            pnl_vals = []
            wins = 0
            win_loss_seq = []
            regime_stats = {}
            time_stats = {}
            dte_stats = {}
            setup_stats = {}
            exit_counts = {}
            hold_times = []
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    continue
                pnl_vals.append(pnl_val)
                if pnl_val > 0:
                    wins += 1
                    win_loss_seq.append(1)
                else:
                    win_loss_seq.append(0)

                regime_key = t.get("regime_at_entry") or "UNKNOWN"
                regime_stats.setdefault(regime_key, {"wins": 0, "total": 0})
                regime_stats[regime_key]["total"] += 1
                if pnl_val > 0:
                    regime_stats[regime_key]["wins"] += 1

                time_key = t.get("time_of_day_bucket") or "UNKNOWN"
                time_stats.setdefault(time_key, {"wins": 0, "total": 0})
                time_stats[time_key]["total"] += 1
                if pnl_val > 0:
                    time_stats[time_key]["wins"] += 1

                dte_key = t.get("dte_bucket") or "UNKNOWN"
                dte_stats.setdefault(
                    dte_key,
                    {"wins": 0, "total": 0, "pnl_sum": 0.0, "pnl_pos": 0.0, "pnl_neg": 0.0},
                )
                dte_stats[dte_key]["total"] += 1
                dte_stats[dte_key]["pnl_sum"] += pnl_val
                if pnl_val > 0:
                    dte_stats[dte_key]["wins"] += 1
                    dte_stats[dte_key]["pnl_pos"] += pnl_val
                elif pnl_val < 0:
                    dte_stats[dte_key]["pnl_neg"] += abs(pnl_val)

                setup_key = t.get("setup") or t.get("setup_type") or "UNKNOWN"
                setup_stats.setdefault(
                    setup_key,
                    {"wins": 0, "total": 0, "pnl_sum": 0.0, "pnl_pos": 0.0, "pnl_neg": 0.0},
                )
                setup_stats[setup_key]["total"] += 1
                setup_stats[setup_key]["pnl_sum"] += pnl_val
                if pnl_val > 0:
                    setup_stats[setup_key]["wins"] += 1
                    setup_stats[setup_key]["pnl_pos"] += pnl_val
                elif pnl_val < 0:
                    setup_stats[setup_key]["pnl_neg"] += abs(pnl_val)

                reason = t.get("exit_reason", "unknown") or "unknown"
                exit_counts[reason] = exit_counts.get(reason, 0) + 1

                hold_sec = _safe_float(t.get("time_in_trade_seconds"))
                if hold_sec is not None:
                    hold_times.append(hold_sec)

            total_trades = len(trade_log)
            total_pnl = sum(pnl_vals) if pnl_vals else 0.0
            win_rate = wins / total_trades if total_trades > 0 else 0.0
            expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
            max_win = max(pnl_vals) if pnl_vals else 0.0
            max_loss = min(pnl_vals) if pnl_vals else 0.0
            win_sum = sum(p for p in pnl_vals if p > 0)
            loss_sum = abs(sum(p for p in pnl_vals if p < 0))
            profit_factor = win_sum / loss_sum if loss_sum > 0 else None

            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            balance = _safe_float(sim.balance) or 0.0
            return_pct = (total_pnl / start_balance) if start_balance > 0 else 0.0
            peak_balance = _safe_float(getattr(sim, "peak_balance", None)) or 0.0
            max_drawdown = peak_balance - balance if peak_balance > balance else 0.0

            times = []
            for t in trade_log:
                ts = _parse_ts(t.get("exit_time")) or _parse_ts(t.get("entry_time"))
                if ts:
                    times.append(ts)
            if len(times) >= 2:
                days_active = max((max(times) - min(times)).total_seconds() / 86400.0, 1 / 24)
            else:
                days_active = 1.0
            equity_speed = total_pnl / days_active if days_active else None

            max_win_streak = 0
            max_loss_streak = 0
            cur_win = 0
            cur_loss = 0
            for outcome in win_loss_seq:
                if outcome == 1:
                    cur_win += 1
                    cur_loss = 0
                else:
                    cur_loss += 1
                    cur_win = 0
                max_win_streak = max(max_win_streak, cur_win)
                max_loss_streak = max(max_loss_streak, cur_loss)

            avg_hold = sum(hold_times) / len(hold_times) if hold_times else None
            pnl_stdev = None
            pnl_median = None
            try:
                if len(pnl_vals) >= 2:
                    import statistics
                    pnl_stdev = statistics.pstdev(pnl_vals)
                    pnl_median = statistics.median(pnl_vals)
            except Exception:
                pnl_stdev = None
                pnl_median = None

            metrics.append({
                "sim_id": sim_key,
                "name": profile.get("name", sim_key),
                "trades": total_trades,
                "wins": wins,
                "win_rate": win_rate,
                "total_pnl": total_pnl,
                "return_pct": return_pct,
                "expectancy": expectancy,
                "max_win": max_win,
                "max_loss": max_loss,
                "profit_factor": profit_factor,
                "max_drawdown": max_drawdown,
                "equity_speed": equity_speed,
                "max_win_streak": max_win_streak,
                "max_loss_streak": max_loss_streak,
                "avg_hold": avg_hold,
                "pnl_stdev": pnl_stdev,
                "pnl_median": pnl_median,
                "regime_stats": regime_stats,
                "time_stats": time_stats,
                "dte_stats": dte_stats,
                "setup_stats": setup_stats,
                "exit_counts": exit_counts,
                "start_balance": start_balance,
                "balance": balance,
            })
        except Exception:
            continue

    return metrics, profiles

def _get_data_freshness_text() -> str | None:
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            return None
        last_ts = df.index[-1] if len(df.index) > 0 else None
        if last_ts is None:
            return None
        eastern = pytz.timezone("America/New_York")
        ts = last_ts.to_pydatetime() if hasattr(last_ts, "to_pydatetime") else last_ts
        if ts.tzinfo is None:
            ts = eastern.localize(ts)
        else:
            ts = ts.astimezone(eastern)
        now = datetime.now(eastern)
        age = (now - ts).total_seconds()
        if age < 0:
            age = 0
        def _fmt_age(seconds: float) -> str:
            total = int(seconds)
            hours = total // 3600
            minutes = (total % 3600) // 60
            secs = total % 60
            parts = []
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or hours > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)
        source = ""
        try:
            source = df.attrs.get("source")
        except Exception:
            source = ""
        source_text = f" | src: {source}" if source else ""
        market_open = None
        try:
            market_open = df.attrs.get("market_open")
        except Exception:
            market_open = None
        status_text = "Market open" if market_open else "Market closed"
        return f"{status_text} | Data age: {_fmt_age(age)} (last candle {ts.strftime('%H:%M:%S')} ET){source_text}"
    except Exception:
        return None

def _get_status_line() -> str | None:
    try:
        acc = load_account()
    except Exception:
        return None
    try:
        risk_mode = acc.get("risk_mode", "NORMAL")
        trade_log = acc.get("trade_log", [])
        last_trade = "None"
        if isinstance(trade_log, list) and trade_log:
            last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
        return f"Status: {risk_mode} | Last trade: {last_trade}"
    except Exception:
        return None

def _get_status_banner() -> str | None:
    try:
        acc = load_account()
    except Exception:
        return None
    try:
        balance = acc.get("balance")
        bal_text = f"${float(balance):,.2f}" if isinstance(balance, (int, float)) else "N/A"
        risk_mode = acc.get("risk_mode", "NORMAL")
        trade_log = acc.get("trade_log", [])
        last_trade = "None"
        if isinstance(trade_log, list) and trade_log:
            last_trade = _format_ts(trade_log[-1].get("exit_time", "Unknown"))
        freshness = _get_data_freshness_text() or "Data age: N/A"
        return (
            f"🧭 Risk: {risk_mode}\n"
            f"💰 Balance: {bal_text}\n"
            f"🕘 Last Trade: {last_trade}\n"
            f"⏱ {freshness}"
        )
    except Exception:
        return None

def _add_trend_arrow(val, good_when_high: bool = True) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return ""
    if num == 0:
        return "➖"
    if good_when_high:
        return "⬆️" if num > 0 else "⬇️"
    return "⬇️" if num > 0 else "⬆️"

def _tag_trade_mode(trade: dict) -> str:
    try:
        mode = (trade or {}).get("mode")
        if isinstance(mode, str) and mode:
            return mode.upper()
        if (trade or {}).get("reconstructed"):
            return "RECON"
    except Exception:
        pass
    return "SIM"

def _append_footer(embed: "discord.Embed", extra: str | None = None) -> None:
    try:
        parts = []
        footer_text = embed.footer.text if embed.footer and embed.footer.text else ""
        if footer_text:
            parts.append(footer_text)
        status_line = _get_status_line()
        freshness_line = _get_data_freshness_text()
        if status_line and status_line not in footer_text:
            parts.append(status_line)
        if freshness_line and freshness_line not in footer_text:
            parts.append(freshness_line)
        if extra:
            parts.append(extra)
        if parts:
            footer_text = " | ".join(p for p in parts if p)
            if len(footer_text) > 2000:
                footer_text = footer_text[:2000]
            embed.set_footer(text=footer_text)
    except Exception:
        return


async def _send_embed(ctx, description: str, title: str | None = None, color: int | None = None):
    inferred_color, inferred_emoji = _infer_embed_style(title, description)
    final_color = color or inferred_color
    final_title = _maybe_prefix_emoji(title, inferred_emoji)
    final_description = description or ""
    banner = _get_status_banner()
    if banner is None:
        if "Data age:" not in final_description and "Status:" not in final_description and "🧭" not in final_description:
            status_line = _get_status_line()
            freshness_line = _get_data_freshness_text()
            context_lines = []
            if status_line:
                context_lines.append(status_line)
            if freshness_line:
                context_lines.append(freshness_line)
            if context_lines:
                appended = "\n".join(context_lines)
                if len(final_description) + len(appended) + 2 <= 3500:
                    final_description = f"{final_description}\n{appended}" if final_description else appended
    if final_title is None and description:
        if not description.startswith(("✅", "❌", "⚠️", "ℹ️")):
            final_description = f"{inferred_emoji} {description}"
    embed = discord.Embed(title=final_title, description=final_description, color=final_color)
    await ctx.send(embed=embed)

# ==============================
# COMMANDS
# ==============================

@bot.command()
async def spy(ctx):
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            await _send_embed(ctx, "Market data unavailable.")
            return

        last = df.iloc[-1]
        recent = df.tail(120)

        high_price = recent["high"].max()
        low_price = recent["low"].min()

        high_time = recent["high"].idxmax()
        low_time = recent["low"].idxmin()

        high_time_str = high_time.strftime('%H:%M') if hasattr(high_time, "strftime") else str(high_time)
        low_time_str = low_time.strftime('%H:%M') if hasattr(low_time, "strftime") else str(low_time)

        plt.figure(figsize=(8, 4))
        plt.plot(df.index[-120:], df["close"][-120:], label="Price")
        plt.plot(df.index[-120:], df["ema9"][-120:], label="EMA9")
        plt.plot(df.index[-120:], df["ema20"][-120:], label="EMA20")
        plt.plot(df.index[-120:], df["vwap"][-120:], label="VWAP")
        plt.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig("charts/chartqqq.png")
        plt.close()

        spy_embed = discord.Embed(title="📡 SPY Snapshot", color=0x3498DB)
        spy_embed.add_field(name="💰 Price",    value=ab(A(f"${last['close']:.2f}", "white", bold=True)), inline=True)
        spy_embed.add_field(name="📊 VWAP",     value=ab(A(f"${last['vwap']:.2f}", "cyan")), inline=True)
        spy_embed.add_field(name="📈 EMA9",     value=ab(A(f"${last['ema9']:.2f}", "yellow")), inline=True)
        spy_embed.add_field(name="📉 EMA20",    value=ab(A(f"${last['ema20']:.2f}", "yellow")), inline=True)
        spy_embed.add_field(name="⬆️ Session High", value=ab(f"{A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}"), inline=True)
        spy_embed.add_field(name="⬇️ Session Low",  value=ab(f"{A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}"), inline=True)
        _append_footer(spy_embed)
        await ctx.send(embed=spy_embed)
        await ctx.send(file=discord.File("charts/chartqqq.png"))
    except Exception as e:
        logging.exception(f"!spy failed: {e}")
        await _send_embed(ctx, "⚠️ Chart error — data still warming up.")

@bot.command()
async def risk(ctx):

    r = calculate_r_metrics()
    dd = calculate_drawdown()

    if not r:
        await _send_embed(
            ctx,
            "⚠️ Need at least 10 closed trades to calculate R metrics.\n"
            "Close more trades before evaluating performance."
        )
        return

    if not dd:
        await _send_embed(
            ctx,
            "⚠️ Drawdown metrics unavailable.\n"
            "Close at least 1 trade to calculate drawdown."
        )
        return

    risk_embed = discord.Embed(title="📊 Risk Metrics", color=0x3498DB)
    risk_embed.add_field(name="📊 Avg R",       value=ab(A(str(r['avg_R']), "yellow", bold=True)), inline=True)
    risk_embed.add_field(name="✅ Avg Win R",   value=ab(A(str(r['avg_win_R']), "green", bold=True)), inline=True)
    risk_embed.add_field(name="❌ Avg Loss R",  value=ab(A(str(r['avg_loss_R']), "red", bold=True)), inline=True)
    risk_embed.add_field(name="🏆 Max R",       value=ab(A(str(r['max_R']), "green")), inline=True)
    risk_embed.add_field(name="💀 Min R",       value=ab(A(str(r['min_R']), "red")), inline=True)
    risk_embed.add_field(name="📉 Max Drawdown", value=ab(drawdown_col(dd['max_drawdown_dollars'])), inline=True)
    _append_footer(risk_embed)
    await ctx.send(embed=risk_embed)

@bot.command()
async def md(ctx, action: str | None = None, level: str | None = None):
    try:
        cmd = (action or "status").strip().lower()
        if cmd not in {"enable", "disable", "status", "auto"}:
            await _send_embed(ctx, "Usage: `!md enable`, `!md disable`, `!md status`, or `!md auto <low|medium|high>`")
            return

        if cmd == "enable":
            state = set_md_enabled(True)
            status_text = "ENABLED"
        elif cmd == "disable":
            state = set_md_enabled(False)
            status_text = "DISABLED"
        elif cmd == "auto":
            state = set_md_auto(level or "medium")
            status_text = "AUTO (ARMED OFF UNTIL DETECTION)"
        else:
            state = get_md_state()
            status_text = "ENABLED" if state.get("enabled") else "DISABLED"

        enabled = bool(state.get("enabled"))
        mode = str(state.get("mode", "manual")).upper()
        auto_level = str(state.get("auto_level", "medium")).upper()
        last_decay = state.get("last_decay")
        last_decay_level = state.get("last_decay_level")
        last_change = state.get("last_changed")
        market_open_prev = state.get("market_open_prev")

        embed = discord.Embed(
            title="🧭 Momentum Decay Strict Mode",
            color=0x2ECC71 if enabled else 0xE74C3C,
        )
        embed.add_field(
            name="Status",
            value=ab(A(status_text, "green" if enabled else "red", bold=True)),
            inline=False,
        )
        embed.add_field(
            name="Mode",
            value=ab(A(mode, "cyan", bold=True)),
            inline=True,
        )
        if mode == "AUTO":
            embed.add_field(
                name="Auto Level",
                value=ab(A(auto_level, "yellow", bold=True)),
                inline=True,
            )
            market_text = "OPEN" if market_open_prev else "CLOSED"
            embed.add_field(
                name="Market Session",
                value=ab(A(market_text, "green" if market_open_prev else "red")),
                inline=True,
            )
        embed.add_field(
            name="Last Decay",
            value=ab(A(_format_ts(last_decay) if last_decay else "None", "cyan")),
            inline=True,
        )
        embed.add_field(
            name="Last Decay Level",
            value=ab(A(str(last_decay_level).upper() if last_decay_level else "None", "cyan")),
            inline=True,
        )
        embed.add_field(
            name="Last Change",
            value=ab(A(_format_ts(last_change) if last_change else "None", "cyan")),
            inline=True,
        )

        if enabled and md_needs_warning(state):
            embed.add_field(
                name="⚠️ Warning",
                value=ab(A("MD strict is enabled but no recent decay detected. Consider disabling.", "yellow")),
                inline=False,
            )

        if not enabled:
            embed.add_field(
                name="How It Works",
                value=ab(A("When ON, stop losses tighten during momentum decay.", "yellow")),
                inline=False,
            )
        if mode == "AUTO":
            embed.add_field(
                name="Auto Rule",
                value=ab(A("MD stays OFF at session transitions, then turns ON only when decay is detected at/above the selected level. It turns OFF again if decay drops below level.", "yellow")),
                inline=False,
            )

        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("md_error")
        await _send_embed(ctx, "md failed due to an internal error.")

@bot.event
async def on_error(event, *args, **kwargs):
    print(f"An error occurred: {event}")
    print(f"Args: {args}")
    print(f"Kwargs: {kwargs}")

@bot.command()
async def expectancy(ctx):

    stats = calculate_expectancy()

    if not stats:
        await _send_embed(ctx, "Need at least 10 closed trades to calculate expectancy.")
        return

    exp_embed = discord.Embed(title="📊 Rolling Expectancy", color=0x3498DB)
    exp_embed.add_field(name="📊 Avg R",      value=ab(A(str(stats['avg_R']), "yellow", bold=True)), inline=True)
    exp_embed.add_field(name="🎯 Win Rate",   value=ab(wr_col(stats['winrate'] / 100.0)), inline=True)
    exp_embed.add_field(name="💰 Expectancy", value=ab(pnl_col(stats['expectancy'])), inline=True)
    exp_embed.add_field(name="📦 Trades",     value=ab(A(str(stats['samples']), "white")), inline=True)
    _append_footer(exp_embed)
    await ctx.send(embed=exp_embed)

@bot.command()
async def retrain(ctx):

    feature_file = os.path.join(DATA_DIR, "trade_features.csv")

    if not os.path.exists(feature_file):
        await _send_embed(
            ctx,
            "No trade feature data found.\n"
            "You need at least 50 logged trades before retraining."
        )
        return

    await _send_embed(ctx, "🔄 Retraining models...")

    train_direction_model()
    train_edge_model()

    await _send_embed(ctx, "✅ Models retrained successfully.")

@bot.command()
async def plan(ctx, side=None, strike=None, premium=None, contracts=None, expiry=None):

    if not all([side, strike, premium, contracts, expiry]):
        await _send_embed(
            ctx,
            "Usage:\n"
            "!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>\n\n"
            "Example:\n"
            "!plan call 435 1.20 2 2026-02-14"
        )
        return

    if not isinstance(side, str):
        await _send_embed(ctx, "Side must be provided as text.")
        return

    side = side.lower()

    if side not in ["call", "put"]:
        await _send_embed(ctx, "Side must be 'call' or 'put'.")
        return

    if strike is None or premium is None or contracts is None:
        await _send_embed(ctx, "Strike, premium, and contracts are required.")
        return

    if isinstance(strike, str):
        strike_text = strike.strip()
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", strike_text):
            await _send_embed(ctx, "Strike must be numeric.")
            return
        strike = float(strike_text)
    elif isinstance(strike, (int, float)):
        strike = float(strike)
    else:
        await _send_embed(ctx, "Strike must be numeric.")
        return

    if isinstance(premium, str):
        premium_text = premium.strip()
        if not re.fullmatch(r"[+-]?(\d+(\.\d*)?|\.\d+)", premium_text):
            await _send_embed(ctx, "Premium must be numeric.")
            return
        premium = float(premium_text)
    elif isinstance(premium, (int, float)):
        premium = float(premium)
    else:
        await _send_embed(ctx, "Premium must be numeric.")
        return

    if isinstance(contracts, str):
        contracts_text = contracts.strip()
        if not re.fullmatch(r"[+-]?\d+", contracts_text):
            await _send_embed(ctx, "Contracts must be a whole number.")
            return
        contracts = int(contracts_text)
    elif isinstance(contracts, int):
        contracts = int(contracts)
    else:
        await _send_embed(ctx, "Contracts must be a whole number.")
        return
    try:
        df = get_market_dataframe()
        if df is None:
            await _send_embed(ctx, "Market data unavailable.")
            return

        regime = get_regime(df)
        vol = volatility_state(df)
        score, impulse, follow, direction = calculate_conviction(df)

        price = df.iloc[-1]["close"]

        # Determine bias alignment
        bias_alignment = "Aligned" if (
            (side == "call" and direction == "bullish") or
            (side == "put" and direction == "bearish")
        ) else "Against Bias"

        # ATR context
        atr = df.iloc[-1]["atr"]
        distance_from_strike = abs(price - strike)

        # Basic risk math
        total_cost = premium * contracts * 100

        # Basic grade logic
        grade_score = 0

        if bias_alignment == "Aligned":
            grade_score += 1
        if regime == "TREND":
            grade_score += 1
        if vol == "NORMAL" or vol == "HIGH":
            grade_score += 1
        if score >= 4:
            grade_score += 1

        if grade_score >= 4:
            grade = "A"
        elif grade_score == 3:
            grade = "B"
        elif grade_score == 2:
            grade = "C"
        else:
            grade = "D"

        embed = discord.Embed(
            title="📋 Trade Plan Analysis",
            color=discord.Color.green() if grade in ["A", "B"] else discord.Color.orange()
        )

        embed.add_field(name="Side", value=side.upper(), inline=True)
        embed.add_field(name=_add_field_icons("Strike"), value=strike, inline=True)
        embed.add_field(name=_add_field_icons("Premium"), value=premium, inline=True)

        embed.add_field(name=_add_field_icons("Contracts"), value=contracts, inline=True)
        embed.add_field(name=_add_field_icons("Total Cost"), value=f"${total_cost:.2f}", inline=True)
        embed.add_field(name=_add_field_icons("Expiry"), value=expiry, inline=True)

        embed.add_field(
            name=_add_field_icons("Market State"),
            value=(
                f"Regime: {regime}\n"
                f"Volatility: {vol}\n"
                f"Conviction Score: {score}\n"
                f"Impulse: {round(impulse,2)}\n"
                f"Follow Through: {round(follow*100,1)}%\n"
            ),
            inline=False
        )

        embed.add_field(
            name=_add_field_icons("Structure Context"),
            value=(
                f"Current Price: {price:.2f}\n"
                f"Distance from Strike: {distance_from_strike:.2f}\n"
                f"ATR: {atr:.2f}\n"
                f"Bias Alignment: {bias_alignment}"
            ),
            inline=False
        )

        embed.add_field(name=_add_field_icons("Final Grade"), value=f"🏆 {grade}", inline=False)
        _append_footer(embed)

        await ctx.send(embed=embed)
    except Exception as e:
        logging.exception(f"!plan failed: {e}")
        await _send_embed(ctx, "⚠️ Plan error — data still warming up.")

@bot.command()
async def mlstats(ctx):

    acc = ml_rolling_accuracy()

    if acc is None:
        await _send_embed(
            ctx,
            "Not enough ML trade data.\n"
            "Need at least 30 ML-evaluated trades."
        )
    else:
        await _send_embed(ctx, f"🧠 ML Rolling Accuracy (Last 30 Trades): {acc}%")


@bot.command()
async def predict(ctx, minutes: str | None = None):

    if minutes is None:
        await _send_embed(
            ctx,
            "Usage: `!predict <minutes>`\n"
            "Allowed values: 30 or 60\n"
            "Example: `!predict 30`"
        )
        return

    if not isinstance(minutes, str):
        await _send_embed(
            ctx,
            "Minutes must be text input.\n"
            "Example: `!predict 60`"
        )
        return

    if not minutes.isdigit():
        await _send_embed(
            ctx,
            "Minutes must be a number.\n"
            "Example: `!predict 60`"
        )
        return

    timeframe_minutes = int(minutes)

    if timeframe_minutes not in [30, 60]:
        await _send_embed(
            ctx,
            "Invalid timeframe.\n"
            "Allowed values: 30 or 60."
        )
        return

    df = get_market_dataframe()

    if df is None:
        await _send_embed(ctx, "Market data unavailable.")
        return

    pred = make_prediction(timeframe_minutes, df)

    if not pred:
        await _send_embed(
            ctx,
            "Not enough graded predictions.\n"
            "Need at least 5 graded predictions."
        )
        return

    pred_color = 0x2ECC71 if pred['direction'] == "bullish" else 0xE74C3C if pred['direction'] == "bearish" else 0x95A5A6
    pred_embed = discord.Embed(title=f"📊 SPY {timeframe_minutes}m Forecast", color=pred_color)
    pred_embed.add_field(name="📍 Direction",   value=ab(dir_col(pred['direction'])), inline=True)
    pred_embed.add_field(name="💡 Confidence",  value=ab(conf_col(pred['confidence'])), inline=True)
    pred_embed.add_field(name="🎯 Predicted High", value=ab(A(str(pred['high']), "green", bold=True)), inline=True)
    pred_embed.add_field(name="🎯 Predicted Low",  value=ab(A(str(pred['low']), "red", bold=True)), inline=True)

    if timeframe_minutes == 30:
        last = df.iloc[-1]
        session_recent = get_rth_session_view(df)
        if session_recent is None or session_recent.empty:
            session_recent = df

        high_price = session_recent["high"].max()
        low_price = session_recent["low"].min()

        high_time = session_recent["high"].idxmax()
        low_time = session_recent["low"].idxmin()

        high_time_str = high_time.strftime("%H:%M") if hasattr(high_time, "strftime") else str(high_time)
        low_time_str = low_time.strftime("%H:%M") if hasattr(low_time, "strftime") else str(low_time)
        if isinstance(high_time_str, str) and "ET" not in high_time_str:
            high_time_str = f"{high_time_str} ET"
        if isinstance(low_time_str, str) and "ET" not in low_time_str:
            low_time_str = f"{low_time_str} ET"

        _pc = f"${last['close']:.2f}"
        _pv = f"${last['vwap']:.2f}"
        _pe9 = f"${last['ema9']:.2f}"
        _pe20 = f"${last['ema20']:.2f}"
        pred_embed.add_field(
            name="📍 Market Snapshot",
            value=ab(
                f"{lbl('Price')} {A(_pc, 'white', bold=True)}",
                f"{lbl('VWAP')}  {A(_pv, 'cyan')}",
                f"{lbl('EMA9')}  {A(_pe9, 'yellow')}  {lbl('EMA20')} {A(_pe20, 'yellow')}",
            ),
            inline=False
        )
        pred_embed.add_field(
            name="📈 Session Range",
            value=ab(
                f"{lbl('High')} {A(f'${high_price:.2f}', 'green')} @ {A(high_time_str, 'white')}",
                f"{lbl('Low')}  {A(f'${low_price:.2f}', 'red')} @ {A(low_time_str, 'white')}",
            ),
            inline=False
        )

    _append_footer(pred_embed)
    await ctx.send(embed=pred_embed)

@bot.command(name="help")
async def help_command(ctx, command_name: str | int | None = None):

    command_levels = {
        "spy": "basic",
        "predict": "basic",
        "regime": "basic",
        "conviction": "basic",
        "opportunity": "basic",
        "plan": "basic",
        "trades": "basic",
        "conviction_fix": "advanced",
        "features_reset": "advanced",
        "pred_reset": "advanced",
        "analysis": "advanced",
        "attempts": "advanced",
        "run": "advanced",
        "paperstats": "advanced",
        "career": "advanced",
        "equity": "advanced",
        "risk": "advanced",
        "expectancy": "advanced",
        "regimes": "advanced",
        "accuracy": "advanced",
        "mlstats": "advanced",
        "retrain": "advanced",
        "importance": "advanced",
        "md": "advanced",
        "simstats": "advanced",
        "simcompare": "advanced",
        "simtrades": "advanced",
        "simopen": "advanced",
        "simreset": "advanced",
        "simleaderboard": "advanced",
        "simstreaks": "advanced",
        "simregimes": "advanced",
        "simtimeofday": "advanced",
        "simpf": "advanced",
        "simconsistency": "advanced",
        "simexits": "advanced",
        "simhold": "advanced",
        "simdte": "advanced",
        "simsetups": "advanced",
        "simhealth": "advanced",
        "siminfo": "advanced",
        "preopen": "advanced",
        "lastskip": "advanced",
        "system": "advanced",
        "replay": "advanced",
        "helpplan": "advanced",
        "ask": "advanced",
        "askmore": "advanced",
    }

    def _send_help_page(page_num: int):
        pages = [
            {
                "title": "📘 Help — Page 1/3 (Market + Core)",
                "color": 0x3498DB,
                "fields": [
                    ("🟢 Market", "`!spy`, `!predict`, `!regime`, `!conviction`, `!opportunity`, `!plan`"),
                    ("🟦 Core Performance", "`!trades`, `!analysis`, `!attempts`, `!run`"),
                    ("🟣 Risk + Expectancy", "`!risk`, `!expectancy`, `!regimes`, `!accuracy`, `!md`"),
                    ("🧭 MD Controls", "`!md status`, `!md enable`, `!md disable`, `!md auto <low|medium|high>`"),
                ],
            },
            {
                "title": "📗 Help — Page 2/3 (ML + Sims)",
                "color": 0x2ECC71,
                "fields": [
                    ("🧠 ML", "`!mlstats`, `!retrain`, `!importance`"),
                    ("🧪 Sims", "`!simstats`, `!simcompare`, `!simtrades`, `!simopen`, `!simleaderboard`, `!simstreaks`, `!simregimes`, `!simtimeofday`, `!simdte`, `!simsetups`, `!simpf`, `!simconsistency`, `!simexits`, `!simhold`, `!simreset`, `!simhealth`, `!siminfo`"),
                    ("⏸ Skip Status", "`!lastskip`, `!preopen`"),
                ],
            },
            {
                "title": "📙 Help — Page 3/3 (System + AI)",
                "color": 0xF39C12,
                "fields": [
                    ("🖥 System", "`!system`, `!replay`, `!helpplan`"),
                    ("🧭 Momentum Decay", "`!md status`, `!md enable`, `!md disable`, `!md auto <low|medium|high>`"),
                    ("🤖 AI Coach", "`!ask`, `!askmore`"),
                    ("🧰 Maintenance", "`!conviction_fix`, `!features_reset`, `!pred_reset`"),
                ],
            },
        ]
        page_index = max(1, min(page_num, len(pages))) - 1
        page = pages[page_index]
        embed = discord.Embed(title=page["title"], color=page["color"])
        embed.description = "Use `!help <command>` for detailed usage. Use `!help 1|2|3` for pages."
        for name, value in page["fields"]:
            embed.add_field(name=name, value=value, inline=False)
        _append_footer(embed, extra=f"Page {page_index + 1}/{len(pages)}")
        return embed

    async def _send_help_paginated(start_page: int):
        pages_count = 3
        page = max(1, min(start_page, pages_count))
        message = await ctx.send(embed=_send_help_page(page))
        if pages_count <= 1:
            return
        try:
            for emoji in ("◀️", "▶️", "⏹️"):
                await message.add_reaction(emoji)
        except Exception:
            return

        def _check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
            )

        while True:
            try:
                reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
            except asyncio.TimeoutError:
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break

            emoji = str(reaction.emoji)
            if emoji == "⏹️":
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break
            if emoji == "◀️":
                page = pages_count if page == 1 else page - 1
            elif emoji == "▶️":
                page = 1 if page == pages_count else page + 1

            try:
                await message.edit(embed=_send_help_page(page))
            except Exception:
                pass
            try:
                await message.remove_reaction(reaction.emoji, user)
            except Exception:
                pass

    if command_name is None:
        await _send_help_paginated(1)
        return
    if isinstance(command_name, int):
        await _send_help_paginated(command_name)
        return
    if isinstance(command_name, str):
        page_text = command_name.strip().lower()
        if page_text.startswith("page"):
            page_text = page_text.replace("page", "").strip()
        if page_text.isdigit():
            await _send_help_paginated(int(page_text))
            return

    if not isinstance(command_name, str):
        await _send_embed(ctx, "Command name must be text.")
        return

    command_name = command_name.lower()

    command_guides = {
        "plan": """
`!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>`

Analyzes a proposed options trade using:

• Market Regime
• Volatility State
• Conviction Score
• Structure Alignment
• ATR Context
• Bias Alignment

Example:
`!plan call 435 1.20 2 2026-02-14`

Outputs:
• Market Diagnostics
• Strike Context
• Exposure Size
• AI Grade (A–D)

This does NOT place a trade.
It evaluates the idea against your engine.
""",
        "predict": """
`!predict <minutes>`

Forecasts SPY direction.

Allowed values:
30 or 60

Examples:
`!predict 30`
`!predict 60`
""",

        "risk": """
`!risk`

Displays:
• Avg R
• Avg Win R
• Avg Loss R
• Max R
• Drawdown

Requires:
Minimum 10 closed trades.
""",

        "expectancy": """
`!expectancy`

Displays rolling expectancy (R-based).

Requires:
Minimum 10 closed trades.
""",
        "spy": """
`!spy`

Shows current SPY price snapshot:
• Price, VWAP, EMA9, EMA20
• Recent high/low with timestamps
• Sends a chart image
""",
        "regime": """
`!regime`

Displays current market regime label.
""",
        "conviction": """
`!conviction`

Displays:
• Conviction score
• Direction
• Impulse
• Follow-through
""",
        "conviction_fix": """
`!conviction_fix`

Forces a backfill of conviction expectancy:
• Fills fwd_5m / fwd_10m where possible
• Adds price/time metadata and status markers
""",
        "features_reset": """
`!features_reset`

Resets trade_features.csv to a clean header.
Use when the feature file is malformed or legacy.
""",
        "pred_reset": """
`!pred_reset`

Resets predictions.csv to a clean header.
Use when old/stale predictions are present.
""",
        "opportunity": """
`!opportunity`

Returns current opportunity zone if available.
""",
        "run": """
`!run`

Shows runtime stats:
• Trades
• Wins/Losses
• Balance
""",
        "paperstats": """
`!paperstats`

Shows paper account stats:
• Balance
• PnL
• Winrate
""",
        "career": """
`!career`

Shows career stats:
• Total trades
• Winrate
• Best balance
""",
        "equity": """
`!equity`

Sends equity curve chart (requires closed trades).
""",
        "accuracy": """
`!accuracy`

Shows prediction accuracy (requires graded predictions).
""",
        "analysis": """
`!analysis`

Decision analysis summary:
• Trades analyzed
• Corr Delta vs R
• Corr Blended vs R
• Execution no-record exits (if present)
""",
        "attempts": """
`!attempts`

Decision attempt summary (runtime):
• Attempts / Opened / Blocked
• Top block reason
• ML weight
• Avg blended vs threshold
""",
        "trades": """
`!trades <page>`

Shows paginated trade log (5 per page).
Example: `!trades 2`
""",
        "simstats": """
`!simstats` or `!simstats SIM03`

Shows sim performance stats:
• Total trades, win rate, total PnL
• Avg win/loss, expectancy, drawdown
• Best/worst trade
• Regime/time-of-day breakdowns
""",
        "simcompare": """
`!simcompare`

Side-by-side sim comparison table.
""",
        "simleaderboard": """
`!simleaderboard`

Ranks sims by key performance metrics:
• Best win rate
• Best total return / PnL
• Fastest equity growth
• Best expectancy
• Biggest winner
• High-risk / high-reward
""",
        "simstreaks": """
`!simstreaks`

Win/loss streak leaders across sims.
""",
        "simregimes": """
`!simregimes`

Best sim by regime (win rate).
""",
        "simtimeofday": """
`!simtimeofday`

Best sim by time-of-day bucket (win rate).
""",
        "simpf": """
`!simpf`

Profit factor leaderboard.
""",
        "simconsistency": """
`!simconsistency`

Most consistent sims (lowest PnL volatility).
""",
        "simexits": """
`!simexits`

Best exit reason hit rates.
""",
        "simhold": """
`!simhold`

Fastest/slowest average hold time.
""",
        "md": """
`!md status`
`!md enable`
`!md disable`
`!md auto <low|medium|high>`

Toggles Momentum Decay strict mode:
• Enabled = tighter stops during decay
• Status shows last decay + warnings
• Auto mode: OFF at session transitions, ON only when detected decay meets/exceeds level
""",
        "simdte": """
`!simdte`

Best sim by DTE bucket (win rate).
""",
        "simsetups": """
`!simsetups`

Best sim by setup type (win rate).
""",
        "siminfo": """
`!siminfo 0-11`
`!siminfo SIM03`

Shows one sim's detailed strategy/config:
• Strategy intent + signal mode
• DTE/hold/cutoff profile
• Risk, stops, targets
• Optional gates (ORB/vol_z/atr_expansion/regime)
""",
        "preopen": """
`!preopen`

Runs a pre-open readiness check:
• Market open/closed status
• Data age + source
• Latest SPY close
• Option snapshot sanity (call/put + 3 OTM variants)
""",
        "simtrades": """
`!simtrades SIM03 [page]`

Shows paginated sim trade history.
""",
        "simopen": """
`!simopen` or `!simopen SIM03 [page]`

Shows open sim trades:
• Hold time
• SPY CALL/PUT expiry strike
• Entry cost + current PnL
""",
        "simreset": """
`!simreset SIM03`
`!simreset all`
`!simreset live`

Resets a sim to starting balance and clears trade history.
""",
        "lastskip": """
`!lastskip`

Shows the most recent skip reason
for trade attempts.
""",
        "regimes": """
`!regimes`

Regime expectancy stats (R-multiple).
""",
        "system": """
`!system`

Displays system health summary.
""",
        "replay": """
`!replay`

Sends recorded session chart and live chart if available.
""",
        "helpplan": """
`!helpplan`

Quick reference for `!plan` usage.
""",

        "mlstats": """
`!mlstats`

Displays rolling ML accuracy (last 30 trades).

Requires:
At least 30 ML-evaluated trades.
""",

        "retrain": """
`!retrain`

Retrains:
• Direction model
• Edge model

Requires:
Minimum 50 logged trades in feature file.
""",

        "importance": """
`!importance`

Displays feature importance from Edge ML model.

Model must be trained first.
""",

        "system": """
`!system`

Displays:
• Market status
• System health
• Active background systems
""",

        "ask": """
`!ask <question>`

AI reviews your performance.

Example:
`!ask Did I overtrade?`
""",
        "askmore": """
`!askmore <follow-up question>`

Continues from your previous `!ask` context.

Examples:
`!askmore break down the last 3 trades`
`!askmore include entry context and regime`
"""
    }

    if command_name in command_guides:
        await _send_embed(ctx, command_guides[command_name], title=f"!{command_name}")
    else:
        await _send_embed(
            ctx,
            "Unknown command.\n"
            "Type `!help` to view available commands.\n"
            "Type `!help <command>` for detailed usage."
        )


@bot.command(name="helpplan")
async def help_plan(ctx):
    await _send_embed(ctx, """
📋 **!plan Usage**

!plan <call/put> <strike> <premium> <contracts> <expiry YYYY-MM-DD>

Example:
!plan call 435 1.20 2 2026-02-14

Analyzes:
• Market structure
• Volatility
• Statistical alignment
• AI model grade (A–D)
""", title="Help")



@bot.command(name="replay")
async def replay(ctx):

    try:
        chart_path = generate_chart()

        if chart_path:
            await _send_embed(ctx, "📊 Recorded Session:")
            await ctx.send(file=discord.File(chart_path))
        else:
            await _send_embed(ctx, "No recorded session data available.")

        live_path = generate_live_chart()

        if live_path and os.path.exists(live_path):
            await _send_embed(ctx, "📈 Live Market:")
            await ctx.send(file=discord.File(live_path))
        else:
            await _send_embed(ctx, "Market closed — no live data available.")

    except Exception as e:
        logging.exception(f"Replay error: {e}")
        await _send_embed(ctx, "Replay failed — check logs.")

@bot.command()
async def importance(ctx):

    data = get_feature_importance()

    if not data:
        await _send_embed(
            ctx,
            "Model not trained yet.\n"
            "Run `!retrain` after 50+ trades."
        )
        return

    message = "📊 Feature Importance (Edge Model)\n\n"

    for name, score in data:
        message += f"{name}: {round(score, 3)}\n"

    await _send_embed(ctx, message, title="Feature Importance")


@bot.command()
async def regime(ctx):
    df = get_market_dataframe()
    if df is None:
        await _send_embed(ctx, "No data.")
        return
    await _send_embed(ctx, f"Market Regime: {get_regime(df)}", title="Regime")

@bot.command()
async def regimes(ctx):

    data = calculate_regime_expectancy()

    if not data:
        await _send_embed(ctx, "Need at least 10 closed trades to calculate meaningful expextancy metrics.")
        return

    message = "📊 Regime Expectancy (R-Multiple)\n\n"

    for regimes, stats in data.items():
        message += (
            f"{regimes}\n"
            f"Trades: {stats['trades']}\n"
            f"Avg R: {stats['avg_R']}\n"
            f"Winrate: {stats['winrate']}%\n\n"
        )

    await _send_embed(ctx, message, title="Regime Expectancy")


@bot.command()
async def conviction(ctx):
    df = get_market_dataframe()
    if df is None:
        await _send_embed(ctx, "No data.")
        return

    score, impulse, follow, direction = calculate_conviction(df)

    await _send_embed(
        ctx,
        (
            f"Conviction Score: {score}\n"
            f"Direction: {direction}\n"
            f"Impulse: {impulse:.2f}\n"
            f"Follow: {follow*100:.0f}%"
        ),
        title="Conviction"
    )


@bot.command()
async def opportunity(ctx):
    df = get_market_dataframe()
    if df is None:
        await _send_embed(ctx, "No data.")
        return

    result = evaluate_opportunity(df)

    if result:
        if not isinstance(result, (list, tuple)) or len(result) < 5:
            await _send_embed(ctx, "No opportunity right now.")
            return
        side = result[0]
        low = result[1]
        high = result[2]
        price = result[3]
        tp_low = tp_high = stop_loss = None
        if len(result) >= 8:
            tp_low = result[5]
            tp_high = result[6]
            stop_loss = result[7]
        lines = [
            f"{side} setup",
            f"Zone: {low:.2f}-{high:.2f}",
            f"Current: ${price:.2f}",
        ]
        if tp_low is not None and tp_high is not None:
            lines.append(f"Take-Profit: {tp_low:.2f}-{tp_high:.2f}")
        if stop_loss is not None:
            lines.append(f"Stop-Loss: {stop_loss:.2f}")
        await _send_embed(
            ctx,
            "\n".join(lines),
            title="Opportunity"
        )
    else:
        await _send_embed(ctx, "No opportunity right now.")


@bot.command()
async def run(ctx):
    s = get_run_stats()
    await _send_embed(
        ctx,
        (
            f"Trades: {s['trades']}\n"
            f"Wins: {s['wins']}\n"
            f"Losses: {s['losses']}\n"
            f"Balance: ${s['current']}"
        ),
        title="Run Stats"
    )


@bot.command()
async def paperstats(ctx):
    s = get_paper_stats()
    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    pnl_badge = "⚪"
    try:
        pnl_val = float(s["pnl"])
        if pnl_val > 0:
            pnl_badge = "✅"
        elif pnl_val < 0:
            pnl_badge = "❌"
    except (TypeError, ValueError, KeyError):
        pnl_badge = "⚪"
    lines = [
        f"💰 Balance: ${s['balance']:.2f}",
        f"📈 PnL: {pnl_badge} ${s['pnl']:.2f}",
        f"🎯 Winrate: {s['winrate']}%",
    ]
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)
    await _send_embed(
        ctx,
        "\n".join(lines),
        title="Paper Stats"
    )


@bot.command()
async def career(ctx):
    c = get_career_stats()
    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    win_badge = "⚪"
    try:
        winrate_val = float(c["winrate"])
        if winrate_val >= 50:
            win_badge = "✅"
        else:
            win_badge = "❌"
    except (TypeError, ValueError, KeyError):
        win_badge = "⚪"
    lines = [
        f"📦 Total Trades: {c['total_trades']}",
        f"🎯 Winrate: {win_badge} {c['winrate']}%",
        f"🏆 Best Balance: ${c['best_balance']:.2f}",
    ]
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)
    await _send_embed(
        ctx,
        "\n".join(lines),
        title="Career Stats"
    )


@bot.command()
async def equity(ctx):
    chart = generate_equity_curve()
    if chart:
        embed = discord.Embed(title="📈 Equity Curve", color=0x3498DB)
        _append_footer(embed)
        await ctx.send(embed=embed, file=discord.File(chart))
    else:
        await _send_embed(ctx, "Need at least 1 closed trade(s) to see equity.")



@bot.command()
async def accuracy(ctx):
    stats = calculate_accuracy()
    if stats:
        await _send_embed(ctx, str(stats), title="Accuracy")
    else:
        await _send_embed(ctx, "Need at least 5 graded predictions before accuracy can be calculated.")


@bot.command()
async def analysis(ctx):
    try:
        acc = load_account()
    except Exception:
        await _send_embed(ctx, "Could not load account data.")
        return

    results = analyze_decision_quality(acc.get("trade_log", []))
    total = results.get("total_trades_analyzed", 0)
    corr_delta = results.get("corr_threshold_delta_vs_R")
    corr_blended = results.get("corr_blended_vs_R")

    corr_delta_text = "N/A" if corr_delta is None else f"{corr_delta:.4f}"
    corr_blended_text = "N/A" if corr_blended is None else f"{corr_blended:.4f}"

    lines = [
        "📊 Decision Analysis Summary",
        f"📦 Trades Analyzed: {total}",
        f"📈 Corr Delta vs R: {corr_delta_text}",
        f"🧠 Corr Blended vs R: {corr_blended_text}",
    ]

    exec_stats = acc.get("execution_stats")
    no_record = exec_stats.get("no_record_exits") if isinstance(exec_stats, dict) else None
    if isinstance(no_record, dict) and no_record:
        no_record_lines = []
        for key in sorted(no_record.keys()):
            no_record_lines.append(f"{key}: {no_record.get(key)}")
        stats_text = "\n".join(no_record_lines)
        lines.append("Execution No-Record Exits:")
        lines.append(stats_text)

    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)

    await _send_embed(ctx, "\n".join(lines)[:1500], title="Analysis")


@bot.command()
async def attempts(ctx):
    snap = get_decision_buffer_snapshot()
    avg_delta = snap.get("avg_delta")
    avg_delta_text = "N/A" if avg_delta is None else f"{avg_delta:.3f}"
    ml_weight = snap.get("ml_weight")
    ml_weight_text = "N/A" if ml_weight is None else f"{ml_weight:.3f}"

    status_line = _get_status_line()
    freshness_line = _get_data_freshness_text()
    lines = [
        "📊 **Decision Attempts (Runtime)**",
        f"Attempts: {snap.get('attempts', 0)}",
        f"Opened: {snap.get('opened', 0)}",
        f"Blocked: {snap.get('blocked', 0)}",
        f"Top Block Reason: {snap.get('top_block_reason', 'N/A')}",
        f"ML Weight: {ml_weight_text}",
        f"Avg Blended vs Threshold (Last 20): {avg_delta_text}",
    ]
    if status_line:
        lines.append(status_line)
    if freshness_line:
        lines.append(freshness_line)

    await _send_embed(
        ctx,
        "\n".join(lines),
        title="Attempts"
    )


@bot.command()
async def simstats(ctx, sim_id: str | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_signed_money(val):
        try:
            num = float(val)
            return f"{'+' if num >= 0 else '-'}${abs(num):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_drawdown(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "N/A"
        if num <= 0:
            return "$0.00"
        return f"-${abs(num):,.2f}"

    def _format_pct(val):
        try:
            return f"{float(val) * 100:.1f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _format_pct_signed(val):
        try:
            num = float(val) * 100
            return f"{'+' if num >= 0 else '-'}{abs(num):.1f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _pnl_badge(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "⚪"
        if num > 0:
            return "✅"
        if num < 0:
            return "❌"
        return "⚪"

    def _extract_reason(ctx):
        if not ctx or not isinstance(ctx, str):
            return None
        if "reason=" not in ctx:
            return None
        try:
            return ctx.rsplit("reason=", 1)[-1].split("|")[0].strip()
        except Exception:
            return None

    def _format_feature_snapshot(fs: dict | None) -> str | None:
        if not isinstance(fs, dict) or not fs:
            return None

        def _f(key, fmt="{:.3f}"):
            val = fs.get(key)
            if val is None:
                return None
            try:
                return fmt.format(float(val))
            except Exception:
                return str(val)

        parts = []
        orb_h = _f("orb_high", "{:.2f}")
        orb_l = _f("orb_low", "{:.2f}")
        if orb_h and orb_l:
            parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
        vol_z = _f("vol_z")
        if vol_z:
            parts.append(f"{lbl('Vol Z')} {A(vol_z, 'yellow')}")
        atr_exp = _f("atr_expansion")
        if atr_exp:
            parts.append(f"{lbl('ATR Exp')} {A(atr_exp, 'magenta')}")
        vwap_z = _f("vwap_z")
        if vwap_z:
            parts.append(f"{lbl('VWAP Z')} {A(vwap_z, 'cyan')}")
        close_z = _f("close_z")
        if close_z:
            parts.append(f"{lbl('Close Z')} {A(close_z, 'cyan')}")
        iv_rank = _f("iv_rank_proxy")
        if iv_rank:
            parts.append(f"{lbl('IV Rank')} {A(iv_rank, 'white')}")
        if not parts:
            return None
        return "  |  ".join(parts)

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    def _compute_breakdown(trade_log, key, order=None):
        stats = {}
        for t in trade_log:
            bucket = t.get(key) if isinstance(t, dict) else None
            bucket = bucket if bucket not in (None, "") else "UNKNOWN"
            stats.setdefault(bucket, {"wins": 0, "total": 0})
            pnl_val = _safe_float(t.get("realized_pnl_dollars"))
            if pnl_val is None:
                stats[bucket]["total"] += 1
                continue
            stats[bucket]["total"] += 1
            if pnl_val > 0:
                stats[bucket]["wins"] += 1
        lines = []
        keys = order if order else sorted(stats.keys())
        for k in keys:
            if k not in stats:
                continue
            total = stats[k]["total"]
            wins = stats[k]["wins"]
            win_rate = wins / total if total > 0 else 0
            lines.append(f"{k}: {wins}/{total} ({win_rate * 100:.1f}%)")
        return "\n".join(lines) if lines else "N/A"

    def _ansi_breakdown(text: str):
        if not text or text == "N/A":
            return ab(A("N/A", "gray"))
        return ab(*[A(line, "cyan") for line in text.splitlines()])

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        if sim_id:
            sim_key = sim_id.strip().upper()
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID.")
                return

            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                await _send_embed(ctx, f"No data for {sim_key} yet.")
                return

            sim = SimPortfolio(sim_key, profile)
            sim.load()

            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            total_trades = len(trade_log)
            open_count = len(sim.open_trades) if isinstance(sim.open_trades, list) else 0

            wins = 0
            losses = 0
            pnl_vals = []
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    losses += 1
                    continue
                pnl_vals.append(pnl_val)
                if pnl_val > 0:
                    wins += 1
                else:
                    losses += 1

            win_rate = wins / total_trades if total_trades > 0 else 0
            total_pnl = sum(pnl_vals) if pnl_vals else 0.0
            avg_win = sum([p for p in pnl_vals if p > 0]) / len([p for p in pnl_vals if p > 0]) if any(p > 0 for p in pnl_vals) else 0.0
            avg_loss = sum([p for p in pnl_vals if p < 0]) / len([p for p in pnl_vals if p < 0]) if any(p < 0 for p in pnl_vals) else 0.0
            expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
            best_trade = max(pnl_vals) if pnl_vals else 0.0
            worst_trade = min(pnl_vals) if pnl_vals else 0.0

            peak_balance = _safe_float(sim.peak_balance) or 0.0
            balance = _safe_float(sim.balance) or 0.0
            max_drawdown = peak_balance - balance if peak_balance > balance else 0.0

            regime_breakdown = _compute_breakdown(
                trade_log,
                "regime_at_entry",
                order=["TREND", "RANGE", "VOLATILE", "UNKNOWN"]
            )
            time_breakdown = _compute_breakdown(
                trade_log,
                "time_of_day_bucket",
                order=["MORNING", "MIDDAY", "AFTERNOON", "CLOSE", "UNKNOWN"]
            )
            exit_counts = {}
            for t in trade_log:
                reason = t.get("exit_reason", "unknown")
                reason = reason if reason not in (None, "") else "unknown"
                exit_counts[reason] = exit_counts.get(reason, 0) + 1
            if total_trades > 0 and exit_counts:
                exit_lines = []
                for reason, count in sorted(exit_counts.items(), key=lambda x: x[1], reverse=True):
                    pct = (count / total_trades) * 100
                    exit_lines.append(f"{reason}: {count} ({pct:.1f}%)")
                exit_breakdown = "\n".join(exit_lines)
            else:
                exit_breakdown = "No exits recorded"

            name = profile.get("name", sim_key)
            total_pnl_badge = _pnl_badge(total_pnl)
            if total_pnl > 0:
                embed_color = 0x2ECC71
            elif total_pnl < 0:
                embed_color = 0xE74C3C
            else:
                embed_color = 0x3498DB
            embed = discord.Embed(title=f"📊 {name} ({sim_key})", color=embed_color)

            last_trade_text = "N/A"
            if trade_log:
                last = trade_log[-1]
                last_exit_time = _format_ts(last.get("exit_time", "N/A"))
                last_reason = last.get("exit_reason", "unknown")
                last_pnl = _safe_float(last.get("realized_pnl_dollars"))
                last_hold = _format_duration(last.get("time_in_trade_seconds"))
                last_trade_text = ab(
                    f"{lbl('Exit')} {A(last_exit_time, 'white')}",
                    f"{lbl('PnL')}  {pnl_col(last_pnl) if last_pnl is not None else A('N/A', 'gray')}  {lbl('Reason')} {exit_reason_col(last_reason)}",
                    *([ f"{lbl('Hold')} {A(last_hold, 'cyan')}" ] if last_hold != "N/A" else []),
                )

            embed.add_field(name=_add_field_icons("Last Trade"),   value=last_trade_text, inline=False)
            embed.add_field(name=_add_field_icons("Total Trades"), value=ab(A(str(total_trades), "white", bold=True)), inline=True)
            embed.add_field(name=_add_field_icons("Open Trades"),  value=ab(A(str(open_count), "cyan")), inline=True)
            embed.add_field(name=_add_field_icons("Win Rate"),     value=ab(wr_col(win_rate)), inline=True)
            embed.add_field(name=_add_field_icons("Total PnL"),    value=ab(pnl_col(total_pnl)), inline=True)
            embed.add_field(name=_add_field_icons("Avg Win"),      value=ab(pnl_col(avg_win)), inline=True)
            embed.add_field(name=_add_field_icons("Avg Loss"),     value=ab(pnl_col(avg_loss)), inline=True)
            embed.add_field(name=_add_field_icons("Expectancy"),   value=ab(pnl_col(expectancy)), inline=True)
            embed.add_field(name=_add_field_icons("Best Trade"),   value=ab(pnl_col(best_trade)), inline=True)
            embed.add_field(name=_add_field_icons("Worst Trade"),  value=ab(pnl_col(worst_trade)), inline=True)
            embed.add_field(name=_add_field_icons("Max Drawdown"), value=ab(drawdown_col(max_drawdown)), inline=True)
            embed.add_field(name=_add_field_icons("Regime Breakdown"), value=_ansi_breakdown(regime_breakdown), inline=False)
            embed.add_field(name=_add_field_icons("Time Bucket Breakdown"), value=_ansi_breakdown(time_breakdown), inline=False)
            embed.add_field(name=_add_field_icons("Exit Reasons"), value=_ansi_breakdown(exit_breakdown), inline=False)
            # Gates summary (if configured)
            gates = []
            if profile.get("orb_minutes") is not None:
                gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
            if profile.get("vol_z_min") is not None:
                gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
            if profile.get("atr_expansion_min") is not None:
                gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
            if gates:
                embed.add_field(name=_add_field_icons("SIM Gates"), value=ab("  |  ".join(gates)), inline=False)
            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            footer = f"Balance: { _format_money(balance) } | Start: { _format_money(start_balance) }"
            freshness_line = _get_data_freshness_text()
            if freshness_line:
                footer = f"{footer} | {freshness_line}"
            embed.set_footer(text=footer)
            _append_footer(embed)
            await ctx.send(embed=embed)
            return

        # summary for all sims
        embed = discord.Embed(title="📊 Sim Overview — All Portfolios", color=0x3498DB)
        max_abs_pnl = 0.0
        for sim_key, profile in profiles.items():
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    continue
                sim = SimPortfolio(sim_key, profile)
                sim.load()
                trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                pnl_vals = []
                for t in trade_log:
                    pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                    if pnl_val is not None:
                        pnl_vals.append(pnl_val)
                total_pnl = sum(pnl_vals) if pnl_vals else 0.0
                max_abs_pnl = max(max_abs_pnl, abs(total_pnl))
            except Exception:
                continue

        for sim_key, profile in profiles.items():
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    embed.add_field(name=sim_key, value="No data", inline=False)
                    continue
                sim = SimPortfolio(sim_key, profile)
                sim.load()
                trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                total_trades = len(trade_log)
                wins = 0
                pnl_vals = []
                exit_counts = {}
                for t in trade_log:
                    pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                    if pnl_val is None:
                        continue
                    pnl_vals.append(pnl_val)
                    if pnl_val > 0:
                        wins += 1
                    reason = t.get("exit_reason", "unknown")
                    reason = reason if reason not in (None, "") else "unknown"
                    exit_counts[reason] = exit_counts.get(reason, 0) + 1
                win_rate = wins / total_trades if total_trades > 0 else 0
                total_pnl = sum(pnl_vals) if pnl_vals else 0.0
                balance = _safe_float(sim.balance) or 0.0
                top_exit = None
                if exit_counts:
                    top_exit = max(exit_counts.items(), key=lambda x: x[1])[0]
                top_exit_text = top_exit if top_exit is not None else "unknown"
                badge = _pnl_badge(total_pnl)
                bar = ""
                if max_abs_pnl > 0:
                    ratio = abs(total_pnl) / max_abs_pnl
                    bars = int(round(ratio * 10))
                    bars = max(0, min(bars, 10))
                    bar = "█" * bars + "░" * (10 - bars)
                else:
                    bar = "░" * 10
                gates = []
                if profile.get("orb_minutes") is not None:
                    gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
                if profile.get("vol_z_min") is not None:
                    gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
                if profile.get("atr_expansion_min") is not None:
                    gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None

                summary = ab(
                    f"{A(str(total_trades), 'white', bold=True)} trades  "
                    f"{lbl('WR')} {wr_col(win_rate)}  "
                    f"{lbl('PnL')} {pnl_col(total_pnl)}",
                    f"{lbl('Bal')} {balance_col(balance)}  "
                    f"{lbl('Exit')} {exit_reason_col(top_exit_text)}  "
                    f"{A(bar, 'cyan')}",
                )
                if gate_line:
                    summary = ab(
                        f"{A(str(total_trades), 'white', bold=True)} trades  "
                        f"{lbl('WR')} {wr_col(win_rate)}  "
                        f"{lbl('PnL')} {pnl_col(total_pnl)}",
                        f"{lbl('Bal')} {balance_col(balance)}  "
                        f"{lbl('Exit')} {exit_reason_col(top_exit_text)}  "
                        f"{A(bar, 'cyan')}",
                        gate_line,
                    )
                embed.add_field(name=sim_key, value=summary, inline=False)
            except Exception:
                embed.add_field(name=sim_key, value="No data", inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simstats_error")
        await _send_embed(ctx, "simstats failed due to an internal error.")


@bot.command()
async def simcompare(ctx):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_signed_money(val):
        try:
            num = float(val)
            return f"{'+' if num >= 0 else '-'}${abs(num):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        rows = []
        agg_pnl = 0.0
        agg_pnl_count = 0
        for sim_key, profile in profiles.items():
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    rows.append({
                        "sim_id": sim_key,
                        "no_data": True,
                        "balance_start": profile.get("balance_start", 0.0),
                    })
                    continue
                sim = SimPortfolio(sim_key, profile)
                sim.load()
                trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                total_trades = len(trade_log)
                if total_trades == 0:
                    rows.append({
                        "sim_id": sim_key,
                        "no_data": True,
                        "balance_start": profile.get("balance_start", 0.0),
                    })
                    continue
                wins = 0
                pnl_vals = []
                for t in trade_log:
                    pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                    if pnl_val is None:
                        continue
                    pnl_vals.append(pnl_val)
                    if pnl_val > 0:
                        wins += 1
                win_rate = wins / total_trades if total_trades > 0 else 0.0
                total_pnl = sum(pnl_vals) if pnl_vals else 0.0
                balance = _safe_float(sim.balance) or 0.0
                peak_balance = _safe_float(sim.peak_balance) or balance
                max_dd = peak_balance - balance if peak_balance > balance else 0.0
                expectancy = total_pnl / total_trades if total_trades > 0 else 0.0
                rows.append({
                    "sim_id": sim_key,
                    "no_data": False,
                    "total_trades": total_trades,
                    "win_rate": win_rate,
                    "total_pnl": total_pnl,
                    "balance": balance,
                    "max_dd": max_dd,
                    "expectancy": expectancy,
                })
                agg_pnl += total_pnl
                agg_pnl_count += 1
            except Exception:
                rows.append({
                    "sim_id": sim_key,
                    "no_data": True,
                    "balance_start": profile.get("balance_start", 0.0),
                })

        # ANSI colors: build the header in gray, then color each data row
        _hdr = "\u001b[30mSIM   | Trades | WR%     | PnL          | Balance      | MaxDD       | Expectancy\u001b[0m"
        lines = [_hdr]
        for row in rows:
            sim_key = row["sim_id"]
            if row.get("no_data"):
                total_trades = "--"
                win_rate = None
                total_pnl = None
                balance = _safe_float(row.get("balance_start")) or 0.0
                max_dd = None
                expectancy = None
            else:
                total_trades = row["total_trades"]
                win_rate = row["win_rate"]
                total_pnl = row["total_pnl"]
                balance = row["balance"]
                max_dd = row["max_dd"]
                expectancy = row["expectancy"]

            trades_raw = f"{total_trades}" if isinstance(total_trades, str) else f"{total_trades:d}"

            # Pre-pad then color WR
            if win_rate is None:
                wr_display = f"{'--':>7}"
            else:
                wr_raw = f"{win_rate * 100:.1f}%"
                wr_padded = f"{wr_raw:>7}"
                wr_clr = "green" if win_rate >= 0.55 else "yellow" if win_rate >= 0.45 else "red"
                wr_display = A(wr_padded, wr_clr, bold=True)

            # Pre-pad then color PnL
            if total_pnl is None:
                pnl_display = f"{'--':>12}"
            else:
                pnl_raw = _format_signed_money(total_pnl)
                pnl_padded = f"{pnl_raw:>12}"
                pnl_clr = "green" if total_pnl > 0 else "red" if total_pnl < 0 else "white"
                pnl_display = A(pnl_padded, pnl_clr, bold=True)

            bal_display = f"{_format_money(balance):>12}"

            # Pre-pad then color MaxDD
            if max_dd is None:
                dd_display = f"{'--':>11}"
            else:
                dd_raw = _format_signed_money(-abs(max_dd)) if max_dd > 0 else _format_signed_money(0)
                dd_padded = f"{dd_raw:>11}"
                dd_display = A(dd_padded, "red" if max_dd > 0 else "white")

            # Pre-pad then color Expectancy
            if expectancy is None:
                exp_display = f"{'--':>10}"
            else:
                exp_raw = _format_signed_money(expectancy)
                exp_padded = f"{exp_raw:>10}"
                exp_clr = "green" if expectancy > 0 else "red" if expectancy < 0 else "white"
                exp_display = A(exp_padded, exp_clr)

            lines.append(
                f"{A(f'{sim_key:<5}', 'cyan')}|"
                f"{trades_raw:>7} | "
                f"{wr_display} | "
                f"{pnl_display} | "
                f"{bal_display} | "
                f"{dd_display} | "
                f"{exp_display}"
            )
        table = "```ansi\n" + "\n".join(lines) + "\n\u001b[0m```"
        if agg_pnl_count > 0:
            if agg_pnl > 0:
                color = 0x2ECC71
            elif agg_pnl < 0:
                color = 0xE74C3C
            else:
                color = 0x3498DB
        else:
            color = 0x95A5A6
        freshness_line = _get_data_freshness_text()
        description = table
        if freshness_line:
            description = f"{table}\n{freshness_line}"
        await _send_embed(ctx, description, title="Sim Compare", color=color)
    except Exception:
        logging.exception("simcompare_error")
        await _send_embed(ctx, "simcompare failed due to an internal error.")


@bot.command()
async def simleaderboard(ctx):
    def _color_pct(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return A("N/A", "gray")
        color = "green" if num >= 0 else "red"
        return A(f"{num:+.1f}%", color)

    def _pnl_or_na(val):
        return pnl_col(val) if val is not None else A("N/A", "gray")

    def _pick_best(items, key, prefer_high=True, filter_fn=None):
        pool = [m for m in items if (filter_fn(m) if filter_fn else True)]
        if not pool:
            return None
        return max(pool, key=lambda x: x.get(key, 0)) if prefer_high else min(pool, key=lambda x: x.get(key, 0))

    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        def _fmt_summary(m, extra_line=None):
            ret_pct = (m.get("return_pct", 0.0) or 0.0) * 100
            return ab(
                f"{lbl('Sim')} {A(m['sim_id'], 'cyan', bold=True)}  {lbl('Trades')} {A(str(m['trades']), 'white', bold=True)}",
                f"{lbl('WR')} {wr_col(m['win_rate'])}  {lbl('PnL')} {pnl_col(m['total_pnl'])}  {lbl('Return')} {_color_pct(ret_pct)}",
                *([extra_line] if extra_line else []),
            )

        eligible = lambda m: m["trades"] >= 3
        best_wr = _pick_best(metrics, "win_rate", filter_fn=lambda m: eligible(m))
        best_pnl = _pick_best(metrics, "total_pnl", filter_fn=lambda m: eligible(m))
        fastest = _pick_best(metrics, "equity_speed", filter_fn=lambda m: eligible(m) and m["equity_speed"] is not None)
        best_exp = _pick_best(metrics, "expectancy", filter_fn=lambda m: eligible(m))
        biggest_win = _pick_best(metrics, "max_win", filter_fn=lambda m: eligible(m))
        risky = _pick_best(metrics, "max_drawdown", filter_fn=lambda m: eligible(m) and m["total_pnl"] > 0)
        most_active = _pick_best(metrics, "trades", filter_fn=lambda m: True)

        embed = discord.Embed(title="🏁 Sim Leaderboard — Best At Each Role", color=0x3498DB)

        if best_wr:
            embed.add_field(name="🏆 Best Win Rate", value=_fmt_summary(best_wr), inline=False)
        if best_pnl:
            embed.add_field(name="💰 Best Total PnL", value=_fmt_summary(best_pnl), inline=False)
        if fastest:
            speed_line = f"{lbl('Speed')} {_pnl_or_na(fastest.get('equity_speed'))} {A('/day', 'cyan')}"
            embed.add_field(name="⚡ Fastest Equity Growth", value=_fmt_summary(fastest, speed_line), inline=False)
        if best_exp:
            exp_line = f"{lbl('Expectancy')} {_pnl_or_na(best_exp.get('expectancy'))}"
            embed.add_field(name="📈 Best Expectancy", value=_fmt_summary(best_exp, exp_line), inline=False)
        if biggest_win:
            win_line = f"{lbl('Max Win')} {_pnl_or_na(biggest_win.get('max_win'))}  {lbl('Max Loss')} {_pnl_or_na(biggest_win.get('max_loss'))}"
            embed.add_field(name="💥 Biggest Winner", value=_fmt_summary(biggest_win, win_line), inline=False)
        if risky:
            risk_line = f"{lbl('Drawdown')} {drawdown_col(risky.get('max_drawdown'))}  {lbl('PnL')} {pnl_col(risky.get('total_pnl'))}"
            embed.add_field(name="⚠️ High-Risk / High-Reward", value=_fmt_summary(risky, risk_line), inline=False)
        if most_active:
            embed.add_field(name="🧮 Most Active", value=_fmt_summary(most_active), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simleaderboard_error")
        await _send_embed(ctx, "simleaderboard failed due to an internal error.")


@bot.command()
async def simstreaks(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        eligible = [m for m in metrics if m.get("trades", 0) >= 3]
        if not eligible:
            await _send_embed(ctx, "Not enough trades to rank streaks (need 3+).")
            return

        win_rank = sorted(eligible, key=lambda m: m.get("max_win_streak", 0), reverse=True)[:5]
        loss_rank = sorted(eligible, key=lambda m: m.get("max_loss_streak", 0), reverse=True)[:5]

        embed = discord.Embed(title="🔁 Sim Streaks", color=0x9B59B6)
        win_lines = []
        for m in win_rank:
            win_lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {A(str(m.get('max_win_streak', 0)), 'green', bold=True)} "
                f"{lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            )
        loss_lines = []
        for m in loss_rank:
            loss_lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {A(str(m.get('max_loss_streak', 0)), 'red', bold=True)} "
                f"{lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            )
        embed.add_field(name="✅ Longest Win Streaks", value=ab(*win_lines) if win_lines else ab(A("N/A", "gray")), inline=False)
        embed.add_field(name="❌ Longest Loss Streaks", value=ab(*loss_lines) if loss_lines else ab(A("N/A", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simstreaks_error")
        await _send_embed(ctx, "simstreaks failed due to an internal error.")


@bot.command()
async def simregimes(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        regimes = ["TREND", "RANGE", "VOLATILE", "UNKNOWN"]
        lines = []
        for reg in regimes:
            best = None
            best_wr = -1.0
            for m in metrics:
                stats = m.get("regime_stats", {}).get(reg)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                if wr > best_wr:
                    best_wr = wr
                    best = (m["sim_id"], stats["wins"], stats["total"])
            if best:
                sim_id, wins, total = best
                lines.append(f"{A(reg, 'cyan')} {A(sim_id, 'white', bold=True)} {A(f'{wins}/{total}', 'white')} {wr_col(wins/total)}")

        embed = discord.Embed(title="🧭 Best by Regime (Win Rate)", color=0x1ABC9C)
        embed.add_field(name="Regime Leaders", value=ab(*lines) if lines else ab(A("No regime data", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simregimes_error")
        await _send_embed(ctx, "simregimes failed due to an internal error.")


@bot.command()
async def simtimeofday(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        buckets = ["MORNING", "MIDDAY", "AFTERNOON", "CLOSE", "UNKNOWN"]
        lines = []
        for bucket in buckets:
            best = None
            best_wr = -1.0
            for m in metrics:
                stats = m.get("time_stats", {}).get(bucket)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                if wr > best_wr:
                    best_wr = wr
                    best = (m["sim_id"], stats["wins"], stats["total"])
            if best:
                sim_id, wins, total = best
                lines.append(f"{A(bucket, 'cyan')} {A(sim_id, 'white', bold=True)} {A(f'{wins}/{total}', 'white')} {wr_col(wins/total)}")

        embed = discord.Embed(title="🕒 Best by Time‑of‑Day (Win Rate)", color=0x2980B9)
        embed.add_field(name="Time‑of‑Day Leaders", value=ab(*lines) if lines else ab(A("No time‑bucket data", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simtimeofday_error")
        await _send_embed(ctx, "simtimeofday failed due to an internal error.")


@bot.command()
async def simpf(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("profit_factor") is not None]
        if not eligible:
            await _send_embed(ctx, "Not enough data for profit factor (need 3+ trades).")
            return
        ranked = sorted(eligible, key=lambda m: m.get("profit_factor", 0), reverse=True)[:7]
        lines = []
        for m in ranked:
            pf = m.get("profit_factor")
            pf_text = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
            lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('PF')} {pf_text} {lbl('WR')} {wr_col(m.get('win_rate', 0))} {lbl('PnL')} {pnl_col(m.get('total_pnl'))}"
            )
        embed = discord.Embed(title="🧮 Profit Factor Leaders", color=0x16A085)
        embed.add_field(name="Top Profit Factors", value=ab(*lines), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simpf_error")
        await _send_embed(ctx, "simpf failed due to an internal error.")


@bot.command()
async def simconsistency(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("pnl_stdev") is not None]
        if not eligible:
            await _send_embed(ctx, "Not enough data for consistency (need 3+ trades).")
            return
        ranked = sorted(eligible, key=lambda m: m.get("pnl_stdev", 0))[:7]
        lines = []
        for m in ranked:
            sigma = m.get("pnl_stdev")
            med = m.get("pnl_median")
            lines.append(
                f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('σ')} {pnl_col(sigma)} "
                f"{lbl('Median')} {pnl_col(med)} {lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            )
        embed = discord.Embed(title="📏 Most Consistent Sims", color=0x8E44AD)
        embed.add_field(name="Lowest PnL Volatility", value=ab(*lines), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simconsistency_error")
        await _send_embed(ctx, "simconsistency failed due to an internal error.")


@bot.command()
async def simexits(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        reasons = ["profit_target", "trailing_stop", "stop_loss", "eod_daytrade_close", "hold_max_elapsed"]
        lines = []
        for reason in reasons:
            best = None
            best_rate = -1.0
            for m in metrics:
                total = m.get("trades", 0)
                if total < 3:
                    continue
                count = m.get("exit_counts", {}).get(reason, 0)
                rate = count / total if total > 0 else 0.0
                if rate > best_rate:
                    best_rate = rate
                    best = (m["sim_id"], count, total)
            if best:
                sim_id, count, total = best
                lines.append(f"{A(reason, 'cyan')} {A(sim_id, 'white', bold=True)} {A(f'{count}/{total}', 'white')} {wr_col(count/total)}")

        embed = discord.Embed(title="🎯 Best Exit Hit Rates", color=0xF39C12)
        embed.add_field(name="Exit Reason Leaders", value=ab(*lines) if lines else ab(A("No exit data", "gray")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simexits_error")
        await _send_embed(ctx, "simexits failed due to an internal error.")


@bot.command()
async def simhold(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        eligible = [m for m in metrics if m.get("trades", 0) >= 3 and m.get("avg_hold") is not None]
        if not eligible:
            await _send_embed(ctx, "Not enough data for hold‑time stats (need 3+ trades).")
            return
        fastest = sorted(eligible, key=lambda m: m.get("avg_hold", 0))[:5]
        slowest = sorted(eligible, key=lambda m: m.get("avg_hold", 0), reverse=True)[:5]
        fast_lines = [
            f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('Avg Hold')} {A(_format_duration_short(m.get('avg_hold')), 'white')} {lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            for m in fastest
        ]
        slow_lines = [
            f"{A(m['sim_id'], 'cyan', bold=True)} {lbl('Avg Hold')} {A(_format_duration_short(m.get('avg_hold')), 'white')} {lbl('WR')} {wr_col(m.get('win_rate', 0))}"
            for m in slowest
        ]
        embed = discord.Embed(title="⏱ Sim Hold‑Time Leaders", color=0x2C3E50)
        embed.add_field(name="Fastest Average Holds", value=ab(*fast_lines), inline=False)
        embed.add_field(name="Slowest Average Holds", value=ab(*slow_lines), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simhold_error")
        await _send_embed(ctx, "simhold failed due to an internal error.")


@bot.command()
async def simdte(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        def _add_lines_field(embed, title, lines):
            if not lines:
                embed.add_field(name=title, value=ab(A("No data", "gray")), inline=False)
                return
            chunks = []
            current = []
            current_len = 0
            for line in lines:
                est = len(line) + 1
                if current and current_len + est > 900:
                    chunks.append(current)
                    current = [line]
                    current_len = len(line)
                else:
                    current.append(line)
                    current_len += est
            if current:
                chunks.append(current)
            for idx, chunk in enumerate(chunks):
                name = title if idx == 0 else f"{title} (cont.)"
                embed.add_field(name=name, value=ab(*chunk), inline=False)

        # Build a combined list of DTE buckets across sims
        bucket_totals = {}
        for m in metrics:
            for k, v in (m.get("dte_stats") or {}).items():
                bucket_totals[k] = bucket_totals.get(k, 0) + v.get("total", 0)

        # Sort by overall sample size, top 8 for readability
        buckets = sorted(bucket_totals.items(), key=lambda x: x[1], reverse=True)
        buckets = [b for b, _ in buckets][:8]
        if not buckets:
            await _send_embed(ctx, "No DTE bucket data yet.")
            return

        lines = []
        for bucket in buckets:
            candidates = []
            for m in metrics:
                stats = (m.get("dte_stats") or {}).get(bucket)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                candidates.append((wr, m["sim_id"], stats))
            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:3]
            bottom = sorted(candidates, key=lambda x: x[0])[:3]

            def _fmt_row(tag, sim_id, stats):
                wins = stats.get("wins", 0)
                total = stats.get("total", 0)
                pf_text = A("N/A", "gray")
                exp_text = A("N/A", "gray")
                if stats.get("pnl_neg", 0) > 0:
                    pf = stats.get("pnl_pos", 0) / stats.get("pnl_neg", 1)
                    pf_text = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
                exp = stats.get("pnl_sum", 0) / max(stats.get("total", 1), 1)
                exp_text = pnl_col(exp)
                return (
                    f"{A(tag, 'cyan')} {A(sim_id, 'white', bold=True)} "
                    f"{A(f'{wins}/{total}', 'white')} {wr_col(wins/total)} "
                    f"{lbl('PF')} {pf_text} {lbl('Exp')} {exp_text}"
                )

            bucket_total = bucket_totals.get(bucket, 0)
            lines.append(A(f"DTE {bucket} | n={bucket_total}", "magenta", bold=True))
            for idx, (_, sim_id, stats) in enumerate(top, start=1):
                lines.append(_fmt_row(f"Top{idx}", sim_id, stats))
            for idx, (_, sim_id, stats) in enumerate(bottom, start=1):
                lines.append(_fmt_row(f"Bot{idx}", sim_id, stats))

        embed = discord.Embed(
            title="📆 Best by DTE Bucket (Win Rate)",
            description="Ranking by win rate (min 3 trades per sim+bucket). PF=profit factor, Exp=avg PnL per trade.",
            color=0x27AE60,
        )
        _add_lines_field(embed, "DTE Leaders (Top/Bottom 3)", lines)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simdte_error")
        await _send_embed(ctx, "simdte failed due to an internal error.")


@bot.command()
async def simsetups(ctx):
    try:
        metrics, profiles = _collect_sim_metrics()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return
        if not metrics:
            await _send_embed(ctx, "No sim data available yet.")
            return

        def _add_lines_field(embed, title, lines):
            if not lines:
                embed.add_field(name=title, value=ab(A("No data", "gray")), inline=False)
                return
            chunks = []
            current = []
            current_len = 0
            for line in lines:
                est = len(line) + 1
                if current and current_len + est > 900:
                    chunks.append(current)
                    current = [line]
                    current_len = len(line)
                else:
                    current.append(line)
                    current_len += est
            if current:
                chunks.append(current)
            for idx, chunk in enumerate(chunks):
                name = title if idx == 0 else f"{title} (cont.)"
                embed.add_field(name=name, value=ab(*chunk), inline=False)

        setup_totals = {}
        for m in metrics:
            for k, v in (m.get("setup_stats") or {}).items():
                setup_totals[k] = setup_totals.get(k, 0) + v.get("total", 0)

        setups = sorted(setup_totals.items(), key=lambda x: x[1], reverse=True)
        setups = [s for s, _ in setups][:8]
        if not setups:
            await _send_embed(ctx, "No setup data yet.")
            return

        lines = []
        for setup in setups:
            candidates = []
            for m in metrics:
                stats = (m.get("setup_stats") or {}).get(setup)
                if not stats or stats.get("total", 0) < 3:
                    continue
                wr = stats["wins"] / stats["total"] if stats["total"] > 0 else 0.0
                candidates.append((wr, m["sim_id"], stats))
            if not candidates:
                continue
            candidates.sort(key=lambda x: x[0], reverse=True)
            top = candidates[:3]
            bottom = sorted(candidates, key=lambda x: x[0])[:3]

            def _fmt_row(tag, sim_id, stats):
                wins = stats.get("wins", 0)
                total = stats.get("total", 0)
                pf_text = A("N/A", "gray")
                exp_text = A("N/A", "gray")
                if stats.get("pnl_neg", 0) > 0:
                    pf = stats.get("pnl_pos", 0) / stats.get("pnl_neg", 1)
                    pf_text = A(f"{pf:.2f}x", "green" if pf >= 1 else "red", bold=True)
                exp = stats.get("pnl_sum", 0) / max(stats.get("total", 1), 1)
                exp_text = pnl_col(exp)
                return (
                    f"{A(tag, 'cyan')} {A(sim_id, 'white', bold=True)} "
                    f"{A(f'{wins}/{total}', 'white')} {wr_col(wins/total)} "
                    f"{lbl('PF')} {pf_text} {lbl('Exp')} {exp_text}"
                )

            setup_total = setup_totals.get(setup, 0)
            lines.append(A(f"Setup {setup} | n={setup_total}", "magenta", bold=True))
            for idx, (_, sim_id, stats) in enumerate(top, start=1):
                lines.append(_fmt_row(f"Top{idx}", sim_id, stats))
            for idx, (_, sim_id, stats) in enumerate(bottom, start=1):
                lines.append(_fmt_row(f"Bot{idx}", sim_id, stats))

        embed = discord.Embed(
            title="🧩 Best by Setup Type (Win Rate)",
            description="Ranking by win rate (min 3 trades per sim+setup). PF=profit factor, Exp=avg PnL per trade.",
            color=0xE67E22,
        )
        _add_lines_field(embed, "Setup Leaders (Top/Bottom 3)", lines)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simsetups_error")
        await _send_embed(ctx, "simsetups failed due to an internal error.")
@bot.command()
async def simtrades(ctx, sim_id: str | None = None, page: str | int = 1):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    def _pnl_badge(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "⚪"
        if num > 0:
            return "✅"
        if num < 0:
            return "❌"
        return "⚪"

    try:
        profiles = _load_profiles()
        profile_map = profiles if isinstance(profiles, dict) else {}
        # Aggregate mode if sim_id is missing or "all"
        if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
            all_trades = []
            for sim_key, profile in profiles.items():
                try:
                    sim_path = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                    )
                    if not os.path.exists(sim_path):
                        continue
                    sim = SimPortfolio(sim_key, profile)
                    sim.load()
                    trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
                    if not trade_log:
                        continue
                    start_balance = _safe_float(profile.get("balance_start")) or 0.0
                    running_balance = start_balance
                    for t in trade_log:
                        pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                        if pnl_val is None:
                            pnl_val = 0.0
                        running_balance += pnl_val
                        t_copy: dict[str, Any] = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                        t_copy["sim_id"] = sim_key
                        t_copy["balance_after"] = running_balance
                        all_trades.append(t_copy)
                except Exception:
                    continue

            if not all_trades:
                await _send_embed(ctx, "No trades recorded yet.")
                return

            def _parse_ts(val):
                if val is None:
                    return None
                if isinstance(val, datetime):
                    return val
                try:
                    return datetime.fromisoformat(str(val))
                except Exception:
                    return None

            def _sort_key(t):
                ts = _parse_ts(t.get("exit_time")) or _parse_ts(t.get("entry_time"))
                return ts or datetime.min

            all_trades = sorted(all_trades, key=_sort_key, reverse=True)
            trade_log = all_trades
            sim_key = "ALL"
            balance_after = {str(t.get("trade_id")): t.get("balance_after") for t in trade_log}
        else:
            sim_key = sim_id.strip().upper()
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID.")
                return

            sim_path = os.path.abspath(
                os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
            )
            if not os.path.exists(sim_path):
                await _send_embed(ctx, f"No data for {sim_key} yet.")
                return

            sim = SimPortfolio(sim_key, profile)
            sim.load()

            trade_log = sim.trade_log if isinstance(sim.trade_log, list) else []
            if not trade_log:
                await _send_embed(ctx, "No trades recorded yet.")
                return

            start_balance = _safe_float(profile.get("balance_start")) or 0.0
            running_balance = start_balance
            balance_after = {}
            for t in trade_log:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is None:
                    pnl_val = 0.0
                running_balance += pnl_val
                trade_id_val = t.get("trade_id")
                if trade_id_val:
                    balance_after[str(trade_id_val)] = running_balance

        per_page = 5
        total = len(trade_log)
        total_pages = (total + per_page - 1) // per_page
        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!simtrades {sim_key} 1` to `!simtrades {sim_key} {total_pages}`.")
            return

        newest_first = list(trade_log)

        def _build_simtrades_embed(page_num: int) -> "discord.Embed":
            page_num = max(1, min(page_num, total_pages))
            start = (page_num - 1) * per_page
            end = start + per_page
            page_trades = newest_first[start:end]

            page_pnl = 0.0
            page_pnl_count = 0
            for t in page_trades:
                pnl_val = _safe_float(t.get("realized_pnl_dollars"))
                if pnl_val is not None:
                    page_pnl += pnl_val
                    page_pnl_count += 1
            if page_pnl_count > 0:
                if page_pnl > 0:
                    page_color = 0x2ECC71
                elif page_pnl < 0:
                    page_color = 0xE74C3C
                else:
                    page_color = 0x3498DB
            else:
                page_color = 0x3498DB

            embed = discord.Embed(title=f"🧾 Sim Trades — {sim_key} (Page {page_num}/{total_pages})", color=page_color)
            for idx, t in enumerate(page_trades, start=start + 1):
                trade_id = str(t.get("trade_id", "N/A"))
                direction = str(t.get("direction", "unknown")).upper()
                entry_price = _safe_float(t.get("entry_price"))
                exit_price = _safe_float(t.get("exit_price"))
                pnl = _safe_float(t.get("realized_pnl_dollars"))
                qty = t.get("qty")
                entry_time = _format_ts(t.get("entry_time", "N/A"))
                exit_time = _format_ts(t.get("exit_time", "N/A"))
                exit_reason = t.get("exit_reason", "unknown")
                hold_text = _format_duration(t.get("time_in_trade_seconds"))
                balance_text = _format_money(balance_after.get(trade_id))
                sim_label = t.get("sim_id") or sim_key
                prof = profile_map.get(sim_label)
                gates = []
                if isinstance(prof, dict):
                    if prof.get("orb_minutes") is not None:
                        gates.append(f"{lbl('orb_minutes')} {A(str(prof.get('orb_minutes')), 'white')}")
                    if prof.get("vol_z_min") is not None:
                        gates.append(f"{lbl('vol_z_min')} {A(str(prof.get('vol_z_min')), 'white')}")
                    if prof.get("atr_expansion_min") is not None:
                        gates.append(f"{lbl('atr_expansion_min')} {A(str(prof.get('atr_expansion_min')), 'white')}")
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None

                def _format_opt_price(val):
                    try:
                        return f"${float(val):.4f}"
                    except (TypeError, ValueError):
                        return "N/A"
                entry_text = _format_opt_price(entry_price) if entry_price is not None else "N/A"
                exit_text = _format_opt_price(exit_price) if exit_price is not None else "N/A"
                pnl_text = _format_money(pnl) if pnl is not None else "N/A"
                pnl_pct_text = "N/A"
                pnl_pct_val = _safe_float(t.get("realized_pnl_pct"))
                if pnl_pct_val is None and entry_price is not None and exit_price is not None and entry_price != 0:
                    try:
                        pnl_pct_val = (float(exit_price) - float(entry_price)) / float(entry_price)
                    except (TypeError, ValueError):
                        pnl_pct_val = None
                if pnl_pct_val is not None:
                    pnl_pct_text = _format_pct_signed(pnl_pct_val)

                badge = _pnl_badge(pnl)
                mode_tag = _tag_trade_mode(t)
                reason_text = _extract_reason(t.get("entry_context"))
                fs_text = _format_feature_snapshot(t.get("feature_snapshot"))
                mfe = _safe_float(t.get("mfe"))
                mae = _safe_float(t.get("mae"))
                field_name = f"{badge} {sim_label} #{idx} {direction} | {trade_id[:8]}"
                pct_suffix = f" ({A(pnl_pct_text, 'cyan')})" if pnl_pct_text != "N/A" else ""
                lines = [
                    f"{lbl('Mode')} {A(mode_tag, 'magenta')}  "
                    f"{lbl('PnL')} {pnl_col(pnl) if pnl is not None else A('N/A', 'gray')}{pct_suffix}  "
                    f"{lbl('Qty')} {A(str(qty), 'white')}",
                    f"{lbl('Entry')} {A(entry_text, 'white')} @ {A(entry_time, 'gray')}",
                    f"{lbl('Exit')}  {A(exit_text, 'white')} @ {A(exit_time, 'gray')}  {lbl('Reason')} {exit_reason_col(exit_reason)}",
                    f"{lbl('Hold')} {A(hold_text, 'cyan')}  {lbl('Bal')} {balance_col(balance_after.get(trade_id))}",
                ]
                if gate_line:
                    lines.append(gate_line)
                if reason_text:
                    lines.append(f"{lbl('Signal reason')} {A(reason_text, 'yellow')}")
                if fs_text:
                    lines.append(f"{lbl('Feature')} {A(fs_text, 'white')}")
                if mfe is not None or mae is not None:
                    mfe_text = f"{mfe:.2%}" if mfe is not None else "N/A"
                    mae_text = f"{mae:.2%}" if mae is not None else "N/A"
                    lines.append(f"{lbl('MFE')} {A(mfe_text, 'green')}  {lbl('MAE')} {A(mae_text, 'red')}")
                field_value = ab(*lines)
                embed.add_field(name=field_name, value=field_value, inline=False)

            _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
            return embed

        async def _send_simtrades_paginated(start_page: int):
            page_num = max(1, min(start_page, total_pages))
            message = await ctx.send(embed=_build_simtrades_embed(page_num))
            if total_pages <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page_num = total_pages if page_num == 1 else page_num - 1
                elif emoji == "▶️":
                    page_num = 1 if page_num == total_pages else page_num + 1

                try:
                    await message.edit(embed=_build_simtrades_embed(page_num))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        await _send_simtrades_paginated(page_num)
    except Exception:
        logging.exception("simtrades_error")
        await _send_embed(ctx, "simtrades failed due to an internal error.")


@bot.command()
async def simopen(ctx, sim_id: str | None = None, page: str | int = 1):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _format_money(val):
        try:
            return f"${float(val):,.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    def _parse_strike_from_symbol(symbol):
        if not symbol or not isinstance(symbol, str):
            return None
        try:
            strike_part = symbol[-8:]
            return int(strike_part) / 1000.0
        except Exception:
            return None

    def _contract_label(symbol, direction, expiry, strike):
        cp = None
        if isinstance(direction, str):
            d = direction.lower()
            if d == "bullish":
                cp = "CALL"
            elif d == "bearish":
                cp = "PUT"
        if cp is None and isinstance(symbol, str) and len(symbol) >= 10:
            try:
                cp_char = symbol[9]
                if cp_char == "C":
                    cp = "CALL"
                elif cp_char == "P":
                    cp = "PUT"
            except Exception:
                cp = None
        expiry_text = expiry[:10] if isinstance(expiry, str) and len(expiry) >= 10 else ""
        label = "SPY"
        if cp:
            label = f"{label} {cp}"
        if expiry_text:
            label = f"{label} {expiry_text}"
        if strike is None:
            strike = _parse_strike_from_symbol(symbol)
        if isinstance(strike, (int, float)):
            label = f"{label} {strike:g}"
        return label

    def _extract_reason(ctx):
        if not ctx or not isinstance(ctx, str):
            return None
        if "reason=" not in ctx:
            return None
        try:
            return ctx.rsplit("reason=", 1)[-1].split("|")[0].strip()
        except Exception:
            return None

    def _format_feature_snapshot(fs: dict | None) -> str | None:
        if not isinstance(fs, dict) or not fs:
            return None

        def _f(key, fmt="{:.3f}"):
            val = fs.get(key)
            if val is None:
                return None
            try:
                return fmt.format(float(val))
            except Exception:
                return str(val)

        parts = []
        orb_h = _f("orb_high", "{:.2f}")
        orb_l = _f("orb_low", "{:.2f}")
        if orb_h and orb_l:
            parts.append(f"{lbl('ORB')} {A(f'{orb_l}-{orb_h}', 'white')}")
        vol_z = _f("vol_z")
        if vol_z:
            parts.append(f"{lbl('Vol Z')} {A(vol_z, 'yellow')}")
        atr_exp = _f("atr_expansion")
        if atr_exp:
            parts.append(f"{lbl('ATR Exp')} {A(atr_exp, 'magenta')}")
        vwap_z = _f("vwap_z")
        if vwap_z:
            parts.append(f"{lbl('VWAP Z')} {A(vwap_z, 'cyan')}")
        close_z = _f("close_z")
        if close_z:
            parts.append(f"{lbl('Close Z')} {A(close_z, 'cyan')}")
        iv_rank = _f("iv_rank_proxy")
        if iv_rank:
            parts.append(f"{lbl('IV Rank')} {A(iv_rank, 'white')}")
        if not parts:
            return None
        return "  |  ".join(parts)

    try:
        profiles = _load_profiles()
        profile_map = profiles if isinstance(profiles, dict) else {}
        trades = []

        if sim_id is None or str(sim_id).strip().lower() in {"all", "all_sims", "allsims"}:
            sim_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
        else:
            sim_key = sim_id.strip().upper()
            if sim_key not in profiles:
                await _send_embed(ctx, "Unknown sim ID.")
                return
            sim_keys = [sim_key]

        for sim_key in sim_keys:
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_key}.json")
                )
                if not os.path.exists(sim_path):
                    continue
                sim = SimPortfolio(sim_key, profiles.get(sim_key, {}))
                sim.load()
                open_trades = sim.open_trades if isinstance(sim.open_trades, list) else []
                for t in open_trades:
                    t_copy = dict(t) if isinstance(t, dict) else {"trade_id": str(t)}
                    t_copy["sim_id"] = sim_key
                    trades.append(t_copy)
            except Exception:
                continue

        if not trades:
            await _send_embed(ctx, "No open sim trades.")
            return

        def _parse_ts(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            try:
                return datetime.fromisoformat(str(val))
            except Exception:
                return None

        def _sort_key(t):
            ts = _parse_ts(t.get("entry_time"))
            return ts or datetime.min

        trades = sorted(trades, key=_sort_key, reverse=True)

        per_page = 5
        total = len(trades)
        total_pages = (total + per_page - 1) // per_page
        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        if page_num < 1 or page_num > total_pages:
            await _send_embed(ctx, f"Invalid page. Use `!simopen {sim_id or 'all'} 1` to `!simopen {sim_id or 'all'} {total_pages}`.")
            return

        async def _build_embed(page_num: int) -> "discord.Embed":
            page_num = max(1, min(page_num, total_pages))
            start = (page_num - 1) * per_page
            end = start + per_page
            page_trades = trades[start:end]
            embed = discord.Embed(title=f"📌 Open Sim Trades (Page {page_num}/{total_pages})", color=0x3498DB)
            now_et = datetime.now(pytz.timezone("US/Eastern"))
            for idx, t in enumerate(page_trades, start=start + 1):
                trade_id = str(t.get("trade_id", "N/A"))
                sim_label = t.get("sim_id") or "SIM"
                prof = profile_map.get(sim_label)
                gates = []
                if isinstance(prof, dict):
                    if prof.get("orb_minutes") is not None:
                        gates.append(f"{lbl('orb_minutes')} {A(str(prof.get('orb_minutes')), 'white')}")
                    if prof.get("vol_z_min") is not None:
                        gates.append(f"{lbl('vol_z_min')} {A(str(prof.get('vol_z_min')), 'white')}")
                    if prof.get("atr_expansion_min") is not None:
                        gates.append(f"{lbl('atr_expansion_min')} {A(str(prof.get('atr_expansion_min')), 'white')}")
                gate_line = f"{lbl('Gates')} {A(' | '.join(gates), 'white')}" if gates else None
                direction = str(t.get("direction") or t.get("type") or "unknown").upper()
                option_symbol = t.get("option_symbol")
                expiry = t.get("expiry")
                strike = _safe_float(t.get("strike"))
                qty = t.get("qty") or t.get("quantity")
                entry_price = _safe_float(t.get("entry_price"))
                entry_notional = _safe_float(t.get("entry_notional"))
                if entry_notional is None and entry_price is not None and qty is not None:
                    try:
                        entry_notional = float(entry_price) * float(qty) * 100
                    except Exception:
                        entry_notional = None
                entry_time = _format_ts(t.get("entry_time", "N/A"))
                hold_secs = None
                try:
                    dt = _parse_ts(t.get("entry_time"))
                    if dt is not None:
                        eastern = pytz.timezone("America/New_York")
                        if dt.tzinfo is None:
                            dt = eastern.localize(dt)
                        else:
                            dt = dt.astimezone(eastern)
                        hold_secs = (now_et - dt).total_seconds()
                except Exception:
                    hold_secs = None

                current_price = None
                if option_symbol:
                    try:
                        current_price = await asyncio.to_thread(get_option_price, option_symbol)
                    except Exception:
                        current_price = None
                pnl_val = None
                if current_price is not None and entry_price is not None and qty is not None:
                    try:
                        pnl_val = (float(current_price) - float(entry_price)) * float(qty) * 100
                    except Exception:
                        pnl_val = None

                entry_text = f"${entry_price:.4f}" if entry_price is not None else "N/A"
                now_text = f"${float(current_price):.4f}" if current_price is not None else "N/A"
                cost_text = _format_money(entry_notional) if entry_notional is not None else "N/A"
                hold_text = _format_duration(hold_secs)
                contract_label = _contract_label(option_symbol, direction, expiry, strike)
                reason_text = _extract_reason(t.get("entry_context"))
                fs_text = _format_feature_snapshot(t.get("feature_snapshot"))
                mfe = _safe_float(t.get("mfe_pct"))
                mae = _safe_float(t.get("mae_pct"))

                field_name = f"🟡 {sim_label} #{idx} {direction} | {trade_id[:8]}"
                lines = [
                    f"{lbl('Contract')} {A(contract_label, 'magenta', bold=True)}",
                    f"{lbl('Qty')} {A(str(qty), 'white')}  {lbl('Entry')} {A(entry_text, 'white')}  {lbl('Cost')} {A(cost_text, 'white')}",
                    f"{lbl('Now')} {A(now_text, 'white')}  {lbl('PnL')} {pnl_col(pnl_val) if pnl_val is not None else A('N/A', 'gray')}",
                    f"{lbl('Hold')} {A(hold_text, 'cyan')}  {lbl('Entry Time')} {A(entry_time, 'gray')}",
                ]
                if gate_line:
                    lines.append(gate_line)
                if reason_text:
                    lines.append(f"{lbl('Signal reason')} {A(reason_text, 'yellow')}")
                if fs_text:
                    lines.append(f"{lbl('Feature')} {A(fs_text, 'white')}")
                if mfe is not None or mae is not None:
                    mfe_text = f"{mfe:.2%}" if mfe is not None else "N/A"
                    mae_text = f"{mae:.2%}" if mae is not None else "N/A"
                    lines.append(f"{lbl('MFE')} {A(mfe_text, 'green')}  {lbl('MAE')} {A(mae_text, 'red')}")
                field_value = ab(*lines)
                embed.add_field(name=field_name, value=field_value, inline=False)
            _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
            return embed

        async def _send_paginated(start_page: int):
            page_num = max(1, min(start_page, total_pages))
            message = await ctx.send(embed=await _build_embed(page_num))
            if total_pages <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page_num = total_pages if page_num == 1 else page_num - 1
                elif emoji == "▶️":
                    page_num = 1 if page_num == total_pages else page_num + 1

                try:
                    await message.edit(embed=await _build_embed(page_num))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        await _send_paginated(page_num)
    except Exception:
        logging.exception("simopen_error")
        await _send_embed(ctx, "simopen failed due to an internal error.")


@bot.command()
async def simreset(ctx, sim_id: str | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    if sim_id is None:
        await _send_embed(ctx, "Usage: `!simreset SIM03`, `!simreset all`, or `!simreset live`")
        return

    try:
        profiles = _load_profiles()
        sim_key = sim_id.strip().upper()

        def _reset_one(_sim_key: str, _profile: dict) -> tuple[bool, str]:
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{_sim_key}.json")
                )
                if os.path.exists(sim_path):
                    try:
                        os.remove(sim_path)
                    except Exception:
                        logging.exception("simreset_remove_failed")
                sim = SimPortfolio(_sim_key, _profile)
                sim.load()
                sim.save()
                return True, "reset"
            except Exception:
                logging.exception("simreset_one_failed")
                return False, "error"

        target_keys = []
        if sim_key == "ALL":
            target_keys = sorted([k for k in profiles.keys() if k.upper().startswith("SIM")])
        elif sim_key == "LIVE":
            for k, p in profiles.items():
                try:
                    if p.get("execution_mode") == "live":
                        target_keys.append(k)
                except Exception:
                    continue
            target_keys = sorted(target_keys)
        else:
            profile = profiles.get(sim_key)
            if profile is None:
                await _send_embed(ctx, "Unknown sim ID.")
                return
            target_keys = [sim_key]

        if not target_keys:
            await _send_embed(ctx, "No sims matched your reset request.")
            return

        results = []
        for key in target_keys:
            profile = profiles.get(key, {})
            ok, status = _reset_one(key, profile)
            results.append((key, ok, status))

        title = f"✅ Sim Reset — {sim_key}" if sim_key in {"ALL", "LIVE"} else f"✅ Sim Reset — {target_keys[0]}"
        embed = discord.Embed(title=title, color=0x2ECC71)
        ok_keys = [k for k, ok, _ in results if ok]
        fail_keys = [k for k, ok, _ in results if not ok]
        for key, ok, status in results:
            start_balance = profiles.get(key, {}).get("balance_start", 0.0)
            status_text = "Reset to starting balance." if ok else "Reset failed."
            embed.add_field(
                name=_add_field_icons(key),
                value=f"{status_text} Start: ${float(start_balance):,.2f}",
                inline=False
            )
        if len(results) > 1:
            summary_parts = []
            if ok_keys:
                summary_parts.append(f"Reset: {', '.join(ok_keys)}")
            if fail_keys:
                summary_parts.append(f"Failed: {', '.join(fail_keys)}")
            if summary_parts:
                embed.add_field(name=_add_field_icons("Summary"), value=" | ".join(summary_parts), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("simreset_error")
        await _send_embed(ctx, "simreset failed due to an internal error.")


@bot.command(name="conviction_fix")
async def conviction_fix(ctx):
    try:
        df = get_market_dataframe()
        update_expectancy(df)
        await _send_embed(
            ctx,
            "Conviction expectancy backfill complete.\n"
            "fwd_5m / fwd_10m now updated where possible with price/time metadata.",
            title="Conviction Fix",
            color=0x2ecc71,
        )
    except Exception as e:
        logging.exception("conviction_fix_error: %s", e)
        await _send_embed(
            ctx,
            f"Conviction fix failed: {e}",
            title="Conviction Fix",
            color=0xe74c3c,
        )


@bot.command(name="features_reset")
async def features_reset(ctx):
    try:
        with open(FEATURE_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(FEATURE_HEADERS)
        await _send_embed(
            ctx,
            "trade_features.csv reset to clean header.",
            title="Features Reset",
            color=0x2ecc71,
        )
    except Exception as e:
        logging.exception("features_reset_error: %s", e)
        await _send_embed(
            ctx,
            f"Features reset failed: {e}",
            title="Features Reset",
            color=0xe74c3c,
        )


@bot.command(name="pred_reset")
async def pred_reset(ctx):
    try:
        with open(PRED_FILE, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(PRED_HEADERS)
        await _send_embed(
            ctx,
            "predictions.csv reset to clean header.",
            title="Predictions Reset",
            color=0x2ecc71,
        )
    except Exception as e:
        logging.exception("pred_reset_error: %s", e)
        await _send_embed(
            ctx,
            f"Predictions reset failed: {e}",
            title="Predictions Reset",
            color=0xe74c3c,
        )


@bot.command()
async def simhealth(ctx, page: str | int | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    required_keys = [
        "signal_mode",
        "dte_min",
        "dte_max",
        "balance_start",
        "risk_per_trade_pct",
        "daily_loss_limit_pct",
        "max_open_trades",
        "exposure_cap_pct",
        "max_spread_pct",
        "cutoff_time_et",
    ]

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        sim_items = list(profiles.items())
        sim_items = [item for item in sim_items if item[0].startswith("SIM")]

        # Run validator for a quick config health summary
        validator_summary = None
        validator_details = []
        try:
            from simulation.sim_validator import collect_sim_validation
            errors, total_errors = collect_sim_validation()
            if total_errors == 0:
                validator_summary = "OK"
            else:
                validator_summary = f"FAIL ({total_errors} issues)"
                validator_details = errors[:3]
        except Exception:
            validator_summary = "FAIL (validator error)"

        def _build_simhealth_embed(page_num: int) -> "discord.Embed":
            page_total = max(1, (len(sim_items) + 2) // 3)
            page_index = max(1, min(page_num, page_total)) - 1
            start = page_index * 3
            end = start + 3
            embed = discord.Embed(
                title=f"🧪 Sim Health Check — Page {page_index + 1}/{page_total}",
                color=0x3498DB
            )
            if validator_summary:
                lines = [A(validator_summary, "green" if validator_summary.startswith("OK") else "red", bold=True)]
                for line in validator_details:
                    severity = "yellow"
                    if any(token in line for token in ("missing:", "cutoff_format_invalid", "orb_requires_features")):
                        severity = "red"
                    lines.append(A(line, severity))
                embed.add_field(
                    name="SIM Validator",
                    value=ab(*lines),
                    inline=False
                )
            embed.add_field(
                name="SIM Profiles Loaded",
                value=ab(A(str(len(sim_items)), "white", bold=True)),
                inline=True
            )
            for sim_id, profile in sim_items[start:end]:
                try:
                    sim_path = os.path.abspath(
                        os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sim_id}.json")
                    )
                    file_exists = os.path.exists(sim_path)
                    file_status = "✅" if file_exists else "❌"
                    missing_keys = [k for k in required_keys if k not in profile]
                    missing_text = ", ".join(missing_keys) if missing_keys else "None"

                    if not file_exists:
                        value = ab(
                            f"{lbl('File')} {A(file_status, 'red' if not file_exists else 'green', bold=True)}",
                            f"{A('Not initialized', 'yellow')}",
                            f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                        )
                        embed.add_field(name=sim_id, value=value, inline=False)
                        continue

                    try:
                        with open(sim_path, "r", encoding="utf-8") as f:
                            data = json.load(f)
                    except Exception:
                        value = ab(
                            f"{lbl('File')} {A(file_status, 'green' if file_exists else 'red', bold=True)}",
                            f"{lbl('Schema')} {A('⚠️', 'yellow', bold=True)}",
                            f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                        )
                        embed.add_field(name=sim_id, value=value, inline=False)
                        continue

                    schema_ok = "✅" if data.get("schema_version") is not None else "⚠️"
                    open_trades = data.get("open_trades")
                    trade_log = data.get("trade_log")
                    open_count = len(open_trades) if isinstance(open_trades, list) else 0
                    trade_count = len(trade_log) if isinstance(trade_log, list) else 0
                    balance_val = _safe_float(data.get("balance"))
                    peak_balance = _safe_float(data.get("peak_balance"))
                    daily_loss_val = _safe_float(data.get("daily_loss"))
                    # Compute daily PnL from trade log for accuracy
                    daily_pnl = None
                    try:
                        today = datetime.now(pytz.timezone("US/Eastern")).date()
                        total = 0.0
                        start_balance = _safe_float(profile.get("balance_start")) or 0.0
                        running_balance = start_balance
                        computed_peak = start_balance
                        for t in (trade_log if isinstance(trade_log, list) else []):
                            pnl_val = _safe_float(t.get("realized_pnl_dollars")) or 0.0
                            running_balance += pnl_val
                            if running_balance > computed_peak:
                                computed_peak = running_balance
                            exit_time = t.get("exit_time")
                            if not exit_time:
                                continue
                            dt = datetime.fromisoformat(str(exit_time))
                            if dt.tzinfo is None:
                                dt = pytz.timezone("US/Eastern").localize(dt)
                            if dt.date() == today:
                                total += pnl_val
                        daily_pnl = total
                        if peak_balance is None:
                            peak_balance = computed_peak
                        else:
                            peak_balance = max(peak_balance, computed_peak)
                    except Exception:
                        daily_pnl = None

                    last_reason = "N/A"
                    try:
                        ctx = None
                        if isinstance(trade_log, list):
                            for t in reversed(trade_log):
                                if isinstance(t, dict) and t.get("entry_context"):
                                    ctx = t.get("entry_context")
                                    break
                        if ctx is None and isinstance(open_trades, list):
                            for t in reversed(open_trades):
                                if isinstance(t, dict) and t.get("entry_context"):
                                    ctx = t.get("entry_context")
                                    break
                        if ctx and "reason=" in ctx:
                            last_reason = ctx.split("reason=")[-1].split("|")[0].strip()
                    except Exception:
                        last_reason = "N/A"

                    gates = []
                    if profile.get("orb_minutes") is not None:
                        gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
                    if profile.get("vol_z_min") is not None:
                        gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
                    if profile.get("atr_expansion_min") is not None:
                        gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
                    gate_text = "  |  ".join(gates) if gates else None

                    value = ab(
                        f"{lbl('File')} {A(file_status, 'green' if file_exists else 'red', bold=True)}",
                        f"{lbl('Schema')} {A(schema_ok, 'green' if schema_ok == '✅' else 'yellow', bold=True)}",
                        f"{lbl('Open trades')} {A(str(open_count), 'white')}",
                        f"{lbl('Trade log')} {A(str(trade_count), 'white')}",
                        f"{lbl('Balance')} {balance_col(balance_val)}",
                        f"{lbl('Peak balance')} {balance_col(peak_balance)}",
                        f"{lbl('Daily PnL')} {pnl_col(daily_pnl) if daily_pnl is not None else A('N/A','gray')}",
                        f"{lbl('Daily loss')} {pnl_col(-abs(daily_loss_val)) if daily_loss_val is not None else A('N/A','gray')}",
                        f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                        f"{lbl('features_enabled')} {A(str(profile.get('features_enabled', False)), 'cyan')}",
                        f"{lbl('signal_mode')} {A(str(profile.get('signal_mode', 'N/A')), 'magenta', bold=True)}",
                        f"{lbl('last_reason')} {A(last_reason, 'yellow')}",
                    )
                    if gate_text:
                        value = ab(
                            f"{lbl('File')} {A(file_status, 'green' if file_exists else 'red', bold=True)}",
                            f"{lbl('Schema')} {A(schema_ok, 'green' if schema_ok == '✅' else 'yellow', bold=True)}",
                            f"{lbl('Open trades')} {A(str(open_count), 'white')}",
                            f"{lbl('Trade log')} {A(str(trade_count), 'white')}",
                            f"{lbl('Balance')} {balance_col(balance_val)}",
                            f"{lbl('Peak balance')} {balance_col(peak_balance)}",
                            f"{lbl('Daily PnL')} {pnl_col(daily_pnl) if daily_pnl is not None else A('N/A','gray')}",
                            f"{lbl('Daily loss')} {pnl_col(-abs(daily_loss_val)) if daily_loss_val is not None else A('N/A','gray')}",
                            f"{lbl('Missing keys')} {A(missing_text, 'cyan')}",
                            f"{lbl('features_enabled')} {A(str(profile.get('features_enabled', False)), 'cyan')}",
                            f"{lbl('signal_mode')} {A(str(profile.get('signal_mode', 'N/A')), 'magenta', bold=True)}",
                            f"{lbl('gates')} {A(gate_text, 'white')}",
                            f"{lbl('last_reason')} {A(last_reason, 'yellow')}",
                        )
                    embed.add_field(name=sim_id, value=value, inline=False)
                except Exception:
                    embed.add_field(name=sim_id, value="Error reading sim data", inline=False)

            checked_text = f"Checked: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}"
            embed.set_footer(text=checked_text)
            _append_footer(embed)
            return embed

        async def _send_simhealth_paginated(start_page: int):
            pages_count = max(1, (len(sim_items) + 2) // 3)
            page = max(1, min(start_page, pages_count))
            message = await ctx.send(embed=_build_simhealth_embed(page))
            if pages_count <= 1:
                return
            try:
                for emoji in ("◀️", "▶️", "⏹️"):
                    await message.add_reaction(emoji)
            except Exception:
                return

            def _check(reaction, user):
                return (
                    user == ctx.author
                    and reaction.message.id == message.id
                    and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
                )

            while True:
                try:
                    reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
                except asyncio.TimeoutError:
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break

                emoji = str(reaction.emoji)
                if emoji == "⏹️":
                    try:
                        await message.clear_reactions()
                    except Exception:
                        pass
                    break
                if emoji == "◀️":
                    page = pages_count if page == 1 else page - 1
                elif emoji == "▶️":
                    page = 1 if page == pages_count else page + 1

                try:
                    await message.edit(embed=_build_simhealth_embed(page))
                except Exception:
                    pass
                try:
                    await message.remove_reaction(reaction.emoji, user)
                except Exception:
                    pass

        page_num = 1
        if isinstance(page, str):
            page_text = page.strip().lower()
            if page_text.startswith("page"):
                page_text = page_text.replace("page", "").strip()
            if page_text.isdigit():
                page_num = int(page_text)
        elif isinstance(page, int):
            page_num = int(page)
        await _send_simhealth_paginated(page_num)
    except Exception:
        logging.exception("simhealth_error")
        await _send_embed(ctx, "simhealth failed due to an internal error.")


@bot.command(name="siminfo")
async def siminfo(ctx, sim_id: str | int | None = None):
    def _load_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _norm_sim_id(raw: str | int | None) -> str | None:
        if raw is None:
            return None
        text = str(raw).strip().upper()
        if not text:
            return None
        if text.startswith("SIM"):
            suffix = text.replace("SIM", "").strip()
            if suffix.isdigit():
                return f"SIM{int(suffix):02d}"
            return text
        if text.isdigit():
            return f"SIM{int(text):02d}"
        return None

    def _fmt_secs(seconds) -> str:
        try:
            s = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if s < 60:
            return f"{s}s"
        if s < 3600:
            return f"{s // 60}m"
        if s < 86400:
            return f"{s / 3600:.1f}h"
        return f"{s / 86400:.1f}d"

    def _dte_tier(dte_min, dte_max) -> str:
        try:
            dmin = int(dte_min)
            dmax = int(dte_max)
        except (TypeError, ValueError):
            return "DTE: N/A"
        if dmin == 0 and dmax == 0:
            return "0DTE only"
        if dmin == 0 and dmax == 1:
            return "0–1 DTE intraday"
        if dmin == 1 and dmax == 1:
            return "1 DTE intraday"
        if dmin == 1 and dmax == 3:
            return "1–3 DTE intraday"
        if dmin >= 7 and dmax <= 10:
            return "7–10 DTE swing"
        if dmin >= 14 and dmax <= 21:
            return "14–21 DTE swing"
        return f"DTE range {dmin}–{dmax}"

    strategy_intents = {
        "SIM00": "Live intraday trend‑pullback execution. Mirrors SIM03 logic with live routing and graduation gate.",
        "SIM01": "0DTE mean‑reversion scalp. Designed to fade short‑term extensions with short holds.",
        "SIM02": "0DTE breakout scalp. Momentum‑style entries with short holds.",
        "SIM03": "Intraday trend‑pullback. Looks for pullback entries in trend and rides continuation.",
        "SIM04": "Intraday range fade. Mean‑reversion inside range regimes, moderate holds.",
        "SIM05": "1DTE afternoon continuation. Trend‑pullback bias later session, longer holds.",
        "SIM06": "7–10 DTE short swing trend. Wider targets/stops, multi‑day holds.",
        "SIM07": "14–21 DTE swing trend. Longest holds, widest targets/stops.",
        "SIM08": "Regime‑filtered trend pullback. Only engages in TREND regime.",
        "SIM09": "Opportunity follower. Uses opportunity outputs to set direction, DTE, and hold context.",
        "SIM10": "ORB breakout. Requires features; trades breaks of opening range with volume impulse.",
        "SIM11": "Vol‑expansion trend. TREND_PULLBACK gated by ATR expansion; short swing.",
    }

    try:
        profiles = _load_profiles()
        if not profiles:
            await _send_embed(ctx, "No sim profiles found.")
            return

        sid = _norm_sim_id(sim_id)
        if sid is None:
            await _send_embed(ctx, "Usage: `!siminfo 0-11` or `!siminfo SIM03`.")
            return
        profile = profiles.get(sid)
        if not profile:
            await _send_embed(ctx, f"{sid} not found in sim_config.yaml.")
            return

        name = profile.get("name", sid)
        mode = str(profile.get("signal_mode", "N/A"))
        horizon = str(profile.get("horizon", "N/A"))
        dte_min = profile.get("dte_min")
        dte_max = profile.get("dte_max")
        hold_min = _fmt_secs(profile.get("hold_min_seconds"))
        hold_max = _fmt_secs(profile.get("hold_max_seconds"))
        cutoff = profile.get("cutoff_time_et", "N/A")
        features_enabled = profile.get("features_enabled", False)
        exec_mode = profile.get("execution_mode", "sim").upper()
        risk_pct_text = "N/A"
        daily_loss_pct_text = "N/A"
        exposure_pct_text = "N/A"
        stop_pct_text = "N/A"
        target_pct_text = "N/A"
        try:
            risk_pct_text = f"{float(profile.get('risk_per_trade_pct', 0)) * 100:.2f}%"
        except (TypeError, ValueError):
            pass
        try:
            daily_loss_pct_text = f"{float(profile.get('daily_loss_limit_pct', 0)) * 100:.2f}%"
        except (TypeError, ValueError):
            pass
        try:
            exposure_pct_text = f"{float(profile.get('exposure_cap_pct', 0)) * 100:.1f}%"
        except (TypeError, ValueError):
            pass
        try:
            stop_pct_text = f"{float(profile.get('stop_loss_pct', 0)) * 100:.1f}%"
        except (TypeError, ValueError):
            pass
        try:
            target_pct_text = f"{float(profile.get('profit_target_pct', 0)) * 100:.1f}%"
        except (TypeError, ValueError):
            pass
        slippage_text = "N/A"
        try:
            slippage_text = f"in {profile.get('entry_slippage', 'N/A')} / out {profile.get('exit_slippage', 'N/A')}"
        except Exception:
            pass

        gates = []
        if profile.get("regime_filter"):
            gates.append(f"{lbl('regime')} {A(str(profile.get('regime_filter')), 'yellow')}")
        if profile.get("orb_minutes") is not None:
            gates.append(f"{lbl('orb_minutes')} {A(str(profile.get('orb_minutes')), 'white')}")
        if profile.get("vol_z_min") is not None:
            gates.append(f"{lbl('vol_z_min')} {A(str(profile.get('vol_z_min')), 'white')}")
        if profile.get("atr_expansion_min") is not None:
            gates.append(f"{lbl('atr_expansion_min')} {A(str(profile.get('atr_expansion_min')), 'white')}")
        gate_text = "  |  ".join(gates) if gates else "None"

        embed = discord.Embed(
            title=f"🧠 {sid} — {name}",
            color=0x3498DB
        )
        embed.add_field(
            name="Strategy Intent",
            value=ab(A(strategy_intents.get(sid, "Configured strategy profile."), "white")),
            inline=False
        )
        embed.add_field(
            name="Profile",
            value=ab(
                f"{lbl('signal_mode')} {A(mode, 'magenta', bold=True)}",
                f"{lbl('horizon')} {A(horizon, 'cyan')}",
                f"{lbl('execution')} {A(exec_mode, 'yellow')}",
                f"{lbl('features_enabled')} {A(str(features_enabled), 'cyan')}",
                f"{lbl('dte_tier')} {A(_dte_tier(dte_min, dte_max), 'white')}",
            ),
            inline=False
        )
        embed.add_field(
            name="Timing",
            value=ab(
                f"{lbl('DTE range')} {A(f'{dte_min}–{dte_max}', 'white')}",
                f"{lbl('hold_min')} {A(hold_min, 'white')}  |  {lbl('hold_max')} {A(hold_max, 'white')}",
                f"{lbl('cutoff')} {A(str(cutoff), 'white')}",
            ),
            inline=False
        )
        embed.add_field(
            name="Risk / Exposure",
            value=ab(
                f"{lbl('risk/trade')} {A(risk_pct_text, 'white')}",
                f"{lbl('daily_loss')} {A(daily_loss_pct_text, 'white')}",
                f"{lbl('max_open')} {A(str(profile.get('max_open_trades', 'N/A')), 'white')}",
                f"{lbl('exposure_cap')} {A(exposure_pct_text, 'white')}",
            ),
            inline=False
        )
        embed.add_field(
            name="Stops / Targets",
            value=ab(
                f"{lbl('stop_loss')} {A(stop_pct_text, 'red')}",
                f"{lbl('profit_target')} {A(target_pct_text, 'green')}",
                f"{lbl('trail_activate')} {A(str(profile.get('trailing_stop_activate_pct', 'N/A')), 'white')}",
                f"{lbl('trail_pct')} {A(str(profile.get('trailing_stop_trail_pct', 'N/A')), 'white')}",
            ),
            inline=False
        )
        embed.add_field(
            name="Entry / Selection",
            value=ab(
                f"{lbl('otm_pct')} {A(str(profile.get('otm_pct', 'N/A')), 'white')}",
                f"{lbl('max_spread')} {A(str(profile.get('max_spread_pct', 'N/A')), 'white')}",
                f"{lbl('slippage')} {A(slippage_text, 'white')}",
                f"{lbl('gates')} {A(gate_text, 'white')}",
            ),
            inline=False
        )

        embed.set_footer(text=f"Loaded: {_format_ts(datetime.now(pytz.timezone('US/Eastern')))}")
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("siminfo_error")
        await _send_embed(ctx, "siminfo failed due to an internal error.")


@bot.command(name="lastskip")
async def lastskip(ctx):
    try:
        reason = getattr(bot, "last_skip_reason", None)
        ts = getattr(bot, "last_skip_time", None)
        if ts is not None:
            ts_text = _format_ts(ts)
        else:
            ts_text = "N/A"

        sim_state = get_sim_last_skip_state()
        sim_lines = []
        for sim_id in sorted(sim_state.keys()):
            item = sim_state.get(sim_id, {})
            sim_reason = item.get("reason") or "N/A"
            sim_time = item.get("time")
            if sim_time is not None:
                sim_time_text = _format_ts(sim_time)
            else:
                sim_time_text = "N/A"
            sim_lines.append(f"{sim_id}: {sim_reason} ({sim_time_text})")
        sim_text = "\n".join(sim_lines) if sim_lines else "None"

        embed = discord.Embed(title="⏸ Last Skip Reasons", color=0xF39C12)
        trader_text = "None"
        if reason:
            trader_text = f"{reason} ({ts_text})"
        embed.add_field(name=_add_field_icons("Trader"), value=ab(A(trader_text, "yellow")), inline=False)
        embed.add_field(name=_add_field_icons("Sims"), value=ab(A(sim_text, "yellow")), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception:
        logging.exception("lastskip_error")
        await _send_embed(ctx, "lastskip failed due to an internal error.")

@bot.command(name="preopen")
async def preopen(ctx):
    """
    Pre-open readiness check:
    - Market status
    - Data freshness
    - Option contract snapshot sanity (best-effort)
    """
    try:
        df = get_market_dataframe()
        if df is None or df.empty:
            await _send_embed(ctx, "Market data unavailable.", title="Pre-Open Check", color=0xE74C3C)
            return

        market_open = df.attrs.get("market_open")
        market_status = "OPEN" if market_open else "CLOSED"
        data_freshness = _get_data_freshness_text() or "Data age: N/A"

        last_close = df.iloc[-1].get("close") if len(df) > 0 else None
        last_close_val = float(last_close) if isinstance(last_close, (int, float)) else None
        close_text = f"{last_close_val:.2f}" if isinstance(last_close_val, (int, float)) else "N/A"

        profile_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        profile = None
        try:
            with open(profile_path, "r") as f:
                profiles = yaml.safe_load(f) or {}
                profile = profiles.get("SIM03") or profiles.get("SIM01")
        except Exception:
            profile = None

        def _format_contract_table(rows):
            header = f"{'OTM':<6} {'Status':<8} Detail"
            lines = [header]
            for row in rows:
                label = row.get("label", "")
                ok = row.get("ok", False)
                if ok:
                    status = "🟢 OK"
                    detail = f"{row.get('symbol', 'N/A')} spr {row.get('spread', 'N/A')}"
                else:
                    status = "🔴 FAIL"
                    detail = row.get("reason", "unavailable")
                lines.append(f"{label:<6} {status:<8} {detail}")
            return "```\n" + "\n".join(lines) + "\n```"

        def _check_contracts(direction: str, base_profile: dict) -> tuple[str, bool]:
            rows = []
            any_ok = False
            if not base_profile:
                return "Profile unavailable", False
            if last_close_val is None:
                return "Price unavailable", False
            try:
                base_otm = float(base_profile.get("otm_pct", 0.0))
            except (TypeError, ValueError):
                base_otm = 0.0
            otm_variants = [
                ("OTM x1.0", base_otm),
                ("OTM x1.5", base_otm * 1.5),
            ]
            for label, otm_val in otm_variants:
                try:
                    prof = dict(base_profile)
                    prof["otm_pct"] = max(0.0, float(otm_val))
                    contract, reason = select_sim_contract_with_reason(direction, last_close_val, prof)
                    if contract:
                        any_ok = True
                        symbol = contract.get("option_symbol", "symbol")
                        spread = contract.get("spread_pct")
                        spread_text = f"{spread:.3f}" if isinstance(spread, (int, float)) else "N/A"
                        rows.append({
                            "label": label,
                            "ok": True,
                            "symbol": symbol,
                            "spread": spread_text,
                        })
                    else:
                        rows.append({
                            "label": label,
                            "ok": False,
                            "reason": reason or "unavailable",
                        })
                except Exception:
                    rows.append({
                        "label": label,
                        "ok": False,
                        "reason": "error",
                    })
            return _format_contract_table(rows), any_ok

        contract_status = "Not checked"
        contract_reason = None
        bull_ok = False
        bear_ok = False
        bull_text = "Not checked"
        bear_text = "Not checked"
        if profile and isinstance(last_close, (int, float)) and last_close > 0:
            bull_text, bull_ok = _check_contracts("BULLISH", profile)
            bear_text, bear_ok = _check_contracts("BEARISH", profile)
            if bull_ok or bear_ok:
                contract_status = "OK"
            else:
                contract_status = "Unavailable"
                contract_reason = "no_contracts_found"

        color = 0x2ECC71
        if bull_ok or bear_ok:
            color = 0x2ECC71
        elif market_status == "CLOSED":
            color = 0xF39C12
        else:
            color = 0xE74C3C

        title_prefix = "✅" if color == 0x2ECC71 else ("⚠️" if color == 0xF39C12 else "❌")
        embed = discord.Embed(title=f"{title_prefix} Pre-Open Check", color=color)
        embed.add_field(name=_add_field_icons("Market"), value=ab(A(f"{market_status}", "green" if market_status == "OPEN" else "yellow", bold=True)), inline=True)
        embed.add_field(name=_add_field_icons("Last Price"), value=ab(A(f"${close_text}", "white", bold=True)), inline=True)
        embed.add_field(name=_add_field_icons("Data Freshness"), value=ab(A(data_freshness, "cyan")), inline=False)
        embed.add_field(name=_add_field_icons("Option Snapshot"), value=ab(A(contract_status, "green" if contract_status == "OK" else "yellow", bold=True)), inline=False)
        embed.add_field(name="📈 Bullish Checks", value=bull_text, inline=False)
        embed.add_field(name="📉 Bearish Checks", value=bear_text, inline=False)
        if contract_reason:
            embed.add_field(name=_add_field_icons("Reason"), value=ab(A(contract_reason, "red", bold=True)), inline=False)
        _append_footer(embed)
        await ctx.send(embed=embed)
    except Exception as e:
        logging.exception("preopen_error: %s", e)
        await _send_embed(ctx, "Pre-open check failed.", title="Pre-Open Check", color=0xE74C3C)

@bot.command()
async def trades(ctx, page: str | int = 1):
    def _safe_money(val, decimals=2):
        try:
            return f"${float(val):.{decimals}f}"
        except (TypeError, ValueError):
            return "N/A"

    def _safe_float(val):
        try:
            if val is None:
                return None
            return float(val)
        except (TypeError, ValueError):
            return None

    def _safe_r(val):
        try:
            return f"{float(val):.3f}R"
        except (TypeError, ValueError):
            return "N/A"

    def _badge_from_pnl(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return "⚪"
        if num > 0:
            return "✅"
        if num < 0:
            return "❌"
        return "⚪"

    def _format_duration(seconds):
        try:
            total = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if total < 0:
            return "N/A"
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        return f"{minutes}m {secs}s"

    try:
        acc = load_account()
    except Exception:
        await _send_embed(ctx, "Could not load account data.")
        return

    trade_log = acc.get("trade_log", [])
    if not trade_log:
        await _send_embed(ctx, "No closed trades yet.")
        return

    per_page = 5
    total = len(trade_log)
    total_pages = (total + per_page - 1) // per_page

    page_num = 1
    if isinstance(page, str):
        page_text = page.strip().lower()
        if page_text.startswith("page"):
            page_text = page_text.replace("page", "").strip()
        if page_text.isdigit():
            page_num = int(page_text)
    elif isinstance(page, int):
        page_num = int(page)
    if page_num < 1 or page_num > total_pages:
        await _send_embed(ctx, f"Invalid page. Use `!trades 1` to `!trades {total_pages}`.")
        return

    # Show newest trades first
    newest_first = list(reversed(trade_log))

    def _build_trades_embed(page_num: int) -> "discord.Embed":
        page_num = max(1, min(page_num, total_pages))
        start = (page_num - 1) * per_page
        end = start + per_page
        page_trades = newest_first[start:end]

        lines = [f"📒 **Trade Log** (Page {page_num}/{total_pages})"]
        page_pnl = 0.0
        page_pnl_count = 0
        for idx, t in enumerate(page_trades, start=start + 1):
            trade_type = str(t.get("type", "unknown")).upper()
            style = t.get("style", "unknown")
            result = str(t.get("result", "unknown")).upper()
            entry_time = _format_ts(t.get("entry_time", "N/A"))
            exit_time = _format_ts(t.get("exit_time", "N/A"))
            exit_reason = t.get("result_reason") or t.get("exit_reason") or "N/A"

            risk = t.get("risk")
            pnl = t.get("pnl")
            r_mult = t.get("R")
            balance_after = t.get("balance_after")
            hold_text = _format_duration(t.get("time_in_trade_seconds"))
            mode_tag = _tag_trade_mode(t)
            pnl_arrow = _add_trend_arrow(pnl, good_when_high=True)

            risk_text = _safe_money(risk)
            pnl_text = _safe_money(pnl)
            r_text = _safe_r(r_mult)
            bal_text = _safe_money(balance_after)
            badge = _badge_from_pnl(pnl)
            pnl_pct_text = "N/A"
            entry_price = _safe_float(t.get("entry_price"))
            exit_price = _safe_float(t.get("exit_price"))
            try:
                if entry_price is not None and exit_price is not None and entry_price != 0:
                    pnl_pct = (float(exit_price) - float(entry_price)) / float(entry_price)
                    pnl_pct_text = _format_pct_signed(pnl_pct)
            except (TypeError, ValueError):
                pnl_pct_text = "N/A"

            snapshot = t.get("decision_snapshot", {})
            delta = snapshot.get("threshold_delta") if isinstance(snapshot, dict) else None
            blended = snapshot.get("blended_score") if isinstance(snapshot, dict) else None
            delta_text = None
            try:
                if delta is not None and blended is not None:
                    delta_val = float(delta)
                    blended_val = float(blended)
                    delta_text = f"Delta: {delta_val:+.4f} | Blended: {blended_val:.4f}"
            except (TypeError, ValueError):
                delta_text = None

            symbol = t.get("option_symbol")
            strike = t.get("strike")
            expiry = t.get("expiry")
            qty = t.get("quantity")

            contract_line = ""
            if symbol:
                contract_line = f"Contract: {symbol}\n"
            elif strike and expiry:
                contract_line = f"{strike} | Exp: {expiry} | Qty: {qty}\n"

            lines.append(
                f"\n{badge} **#{idx}** {trade_type} ({style}) - {result} | {mode_tag}\n"
                f"{contract_line}"
                f"Risk: {risk_text} | PnL: {pnl_text} ({pnl_pct_text}) {pnl_arrow} | R: {r_text}\n"
                f"{delta_text + '\n' if delta_text else ''}"
                f"Entry: {entry_time}\n"
                f"Exit: {exit_time}\n"
                f"Hold: {hold_text} | Reason: {exit_reason}\n"
                f"Balance After: {bal_text}"
            )

            try:
                pnl_val = float(pnl) if pnl is not None else None
            except (TypeError, ValueError):
                pnl_val = None
            if pnl_val is not None:
                page_pnl += pnl_val
                page_pnl_count += 1

        final_message = "\n".join(lines)
        if page_pnl_count > 0:
            if page_pnl > 0:
                color = 0x2ECC71
            elif page_pnl < 0:
                color = 0xE74C3C
            else:
                color = 0x3498DB
        else:
            color = 0x3498DB

        embed = discord.Embed(title=f"📒 Trade Log (Page {page_num}/{total_pages})", description=final_message, color=color)
        banner = _get_status_banner()
        if banner:
            embed.add_field(name="🧭 Status Banner", value=banner, inline=False)
        _append_footer(embed, extra=f"Page {page_num}/{total_pages}")
        return embed

    async def _send_trades_paginated(start_page: int):
        page_num = max(1, min(start_page, total_pages))
        message = await ctx.send(embed=_build_trades_embed(page_num))
        if total_pages <= 1:
            return
        try:
            for emoji in ("◀️", "▶️", "⏹️"):
                await message.add_reaction(emoji)
        except Exception:
            return

        def _check(reaction, user):
            return (
                user == ctx.author
                and reaction.message.id == message.id
                and str(reaction.emoji) in {"◀️", "▶️", "⏹️"}
            )

        while True:
            try:
                reaction, user = await bot.wait_for("reaction_add", timeout=60, check=_check)
            except asyncio.TimeoutError:
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break

            emoji = str(reaction.emoji)
            if emoji == "⏹️":
                try:
                    await message.clear_reactions()
                except Exception:
                    pass
                break
            if emoji == "◀️":
                page_num = total_pages if page_num == 1 else page_num - 1
            elif emoji == "▶️":
                page_num = 1 if page_num == total_pages else page_num + 1

            try:
                await message.edit(embed=_build_trades_embed(page_num))
            except Exception:
                pass
            try:
                await message.remove_reaction(reaction.emoji, user)
            except Exception:
                pass

    await _send_trades_paginated(page_num)



@bot.command()
async def system(ctx):

    import pytz
    from datetime import datetime
    from core.account_repository import load_account
    from interface.health_monitor import check_health

    eastern = pytz.timezone("US/Eastern")
    now = datetime.now(eastern)

    # ---------------------------
    # Safe Account Load
    # ---------------------------
    try:
        acc = load_account()
    except:
        acc = {}

    trade_log = acc.get("trade_log", [])

    # ---------------------------
    # Safe Trade Count
    # ---------------------------
    total_trades = len(trade_log)

    if total_trades == 0:
        trade_status = "No closed trades yet."
    else:
        trade_status = f"{total_trades} closed trades"

    # ---------------------------
    # Health Check (Safe)
    # ---------------------------
    try:
        status, report = check_health()
    except:
        status = "UNKNOWN"
        report = "Health monitor unavailable."

    market_status = "🟢 OPEN" if market_is_open() else "🔴 CLOSED"

    # ---------------------------
    # Embed
    # ---------------------------
    embed = discord.Embed(
        title="🧠 SPY AI Control Center",
        color=discord.Color.green() if status == "HEALTHY" else discord.Color.orange()
    )
    embed.add_field(name=_add_field_icons("Market"), value=market_status, inline=True)
    embed.add_field(name=_add_field_icons("System Health"), value=status, inline=True)
    embed.add_field(name=_add_field_icons("System Diagnostics"), value=f"```\n{report}\n```", inline=False)

    embed.add_field(
        name=_add_field_icons("Trade Activity"),
        value=f"{trade_status}",
        inline=False
    )

    embed.add_field(
        name=_add_field_icons("Background Systems"),
        value=(
            "Auto Trader: Running\n"
            "Forecast Engine: Active\n"
            "Conviction Watcher: Active\n"
            "Prediction Grader: Active\n"
            "Heart Monitor: Active"
        ),
        inline=False
    )

    if total_trades < 10:
        embed.add_field(
            name=_add_field_icons("Analytics Status"),
            value=(
                "⚠️ Not enough trade data for:\n"
                "• Expectancy\n"
                "• Risk Metrics\n"
                "• Edge Stability\n"
                "System is collecting data."
            ),
            inline=False
        )

    embed.set_footer(text=f"System time: {_format_ts(now)}")
    _append_footer(embed)

    await ctx.send(embed=embed)




@bot.command()
async def ask(ctx, *, question=None):

    if not question:
        await _send_embed(ctx, "Usage: !ask <question>\nExample: !ask Did I overtrade?")
        return

    def _load_sim_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _norm_sim_id(raw: str) -> str | None:
        text = str(raw).strip().upper()
        if not text:
            return None
        if text.startswith("SIM"):
            suffix = text.replace("SIM", "").strip()
            if suffix.isdigit():
                return f"SIM{int(suffix):02d}"
            return text
        if text.isdigit():
            return f"SIM{int(text):02d}"
        return None

    def _fmt_pct(val, decimals=1) -> str:
        try:
            return f"{float(val) * 100:.{decimals}f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _fmt_secs(seconds) -> str:
        try:
            s = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if s < 60:
            return f"{s}s"
        if s < 3600:
            return f"{s // 60}m"
        if s < 86400:
            return f"{s / 3600:.1f}h"
        return f"{s / 86400:.1f}d"

    strategy_intents = {
        "SIM00": "Live intraday trend‑pullback execution. Mirrors SIM03 with live routing and graduation gate.",
        "SIM01": "0DTE mean‑reversion scalp. Fades short‑term extensions; short holds.",
        "SIM02": "0DTE breakout scalp. Momentum‑style entries; short holds.",
        "SIM03": "Intraday trend pullback. Pullback entries in trend and continuation holds.",
        "SIM04": "Intraday range fade. Mean‑reversion inside range regimes.",
        "SIM05": "1DTE afternoon continuation. Trend bias later session.",
        "SIM06": "7–10 DTE short swing trend. Multi‑day holds.",
        "SIM07": "14–21 DTE swing trend. Longest holds.",
        "SIM08": "Regime‑filtered trend pullback. Only trades TREND regime.",
        "SIM09": "Opportunity follower. Uses opportunity output to set direction/DTE/hold.",
        "SIM10": "ORB breakout. Trades opening‑range breaks; requires features.",
        "SIM11": "Vol‑expansion trend. TREND_PULLBACK gated by ATR expansion; requires features.",
    }

    def _build_sim_context(question_text: str) -> str | None:
        q = question_text.lower()
        if "sim" not in q:
            return None

        profiles = _load_sim_profiles()
        if not profiles:
            return None

        found = set()
        for match in re.findall(r"\bSIM\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
            norm = _norm_sim_id(match)
            if norm:
                found.add(norm)
        for match in re.findall(r"\bsim\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
            norm = _norm_sim_id(match.replace("sim", "SIM"))
            if norm:
                found.add(norm)

        include_all = ("all sims" in q) or ("all sim" in q) or (len(found) == 0)
        sim_ids = sorted([k for k in profiles.keys() if k.startswith("SIM")]) if include_all else sorted(found)

        lines = []
        for sid in sim_ids:
            profile = profiles.get(sid)
            if not isinstance(profile, dict):
                continue
            name = profile.get("name", sid)
            mode = profile.get("signal_mode", "N/A")
            horizon = profile.get("horizon", "N/A")
            exec_mode = str(profile.get("execution_mode", "sim")).upper()
            dte_min = profile.get("dte_min", "N/A")
            dte_max = profile.get("dte_max", "N/A")
            hold_min = _fmt_secs(profile.get("hold_min_seconds"))
            hold_max = _fmt_secs(profile.get("hold_max_seconds"))
            cutoff = profile.get("cutoff_time_et", "N/A")
            stop_pct = _fmt_pct(profile.get("stop_loss_pct"))
            target_pct = _fmt_pct(profile.get("profit_target_pct"))
            risk_pct = _fmt_pct(profile.get("risk_per_trade_pct"), 2)
            daily_loss = _fmt_pct(profile.get("daily_loss_limit_pct"), 2)
            max_open = profile.get("max_open_trades", "N/A")
            max_spread = profile.get("max_spread_pct", "N/A")
            features = profile.get("features_enabled", False)

            gates = []
            if profile.get("regime_filter"):
                gates.append(f"regime={profile.get('regime_filter')}")
            if profile.get("orb_minutes") is not None:
                gates.append(f"orb_minutes={profile.get('orb_minutes')}")
            if profile.get("vol_z_min") is not None:
                gates.append(f"vol_z_min={profile.get('vol_z_min')}")
            if profile.get("atr_expansion_min") is not None:
                gates.append(f"atr_expansion_min={profile.get('atr_expansion_min')}")
            gate_text = ("gates: " + ", ".join(gates)) if gates else "gates: none"

            # Sim state (if available)
            state_line = None
            try:
                sim_path = os.path.abspath(
                    os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sid}.json")
                )
                if os.path.exists(sim_path):
                    with open(sim_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    open_count = len(data.get("open_trades", [])) if isinstance(data.get("open_trades"), list) else 0
                    trade_count = len(data.get("trade_log", [])) if isinstance(data.get("trade_log"), list) else 0
                    balance = data.get("balance")
                    peak = data.get("peak_balance")
                    state_line = f"state: open={open_count} trades={trade_count} balance={balance} peak={peak}"
            except Exception:
                state_line = None

            lines.append(f"{sid} — {name}")
            lines.append(f"strategy: {strategy_intents.get(sid, 'Configured strategy profile.')}")
            lines.append(f"mode={mode} horizon={horizon} exec={exec_mode} features={features}")
            lines.append(f"DTE {dte_min}-{dte_max} | hold {hold_min}-{hold_max} | cutoff {cutoff}")
            lines.append(f"risk {risk_pct} | daily_loss {daily_loss} | max_open {max_open} | max_spread {max_spread}")
            lines.append(f"stop {stop_pct} | target {target_pct}")
            lines.append(gate_text)
            if state_line:
                lines.append(state_line)
            lines.append("—")

        context = "\n".join(lines).strip()
        if len(context) > 2500:
            context = context[:2500] + "\n…(truncated)"
        return context

    def _extract_target_sims(question_text: str) -> list[str]:
        found = set()
        for match in re.findall(r"\bsim\s*\d{1,2}\b", question_text, flags=re.IGNORECASE):
            norm = _norm_sim_id(match)
            if norm:
                found.add(norm)
        return sorted(found)

    def _build_sim_fallback_answer(question_text: str) -> str | None:
        profiles = _load_sim_profiles()
        targets = _extract_target_sims(question_text)
        if not targets:
            return None

        sid = targets[0]
        profile = profiles.get(sid, {}) if isinstance(profiles, dict) else {}
        sim_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sid}.json")
        )
        if not os.path.exists(sim_path):
            return f"{sid}: no sim state file found yet. It may not have initialized trades yet."

        try:
            with open(sim_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return f"{sid}: unable to read sim state, so I cannot analyze its performance yet."

        trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
        open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
        eastern = pytz.timezone("US/Eastern")

        def _parse_trade_dt(raw):
            if not raw:
                return None
            try:
                dt = datetime.fromisoformat(str(raw))
                if dt.tzinfo is None:
                    dt = eastern.localize(dt)
                else:
                    dt = dt.astimezone(eastern)
                return dt
            except Exception:
                return None

        def _parse_strike(symbol):
            if not symbol or not isinstance(symbol, str):
                return None
            try:
                return int(symbol[-8:]) / 1000.0
            except Exception:
                return None

        closed = []
        for t in trade_log:
            try:
                pnl = float(t.get("realized_pnl_dollars"))
            except (TypeError, ValueError):
                continue
            dt = _parse_trade_dt(t.get("exit_time")) or _parse_trade_dt(t.get("entry_time"))
            closed.append({"trade": t, "pnl": pnl, "dt": dt})

        if not closed:
            mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
            dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
            dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"
            return (
                f"{sid} has no closed trades yet (open: {len(open_trades)}). "
                f"Configured mode={mode}, DTE={dte_min}-{dte_max}. "
                "No performance diagnosis is possible until more closes are logged."
            )

        q = question_text.lower()
        scope = closed
        if "yesterday" in q:
            yday = (datetime.now(eastern).date() - timedelta(days=1))
            scoped = [x for x in closed if x["dt"] and x["dt"].date() == yday]
            if scoped:
                scope = scoped

        total = len(scope)
        wins = sum(1 for x in scope if x["pnl"] > 0)
        losses = total - wins
        wr = (wins / total) * 100 if total else 0.0
        total_pnl = sum(x["pnl"] for x in scope)
        avg = total_pnl / total if total else 0.0
        avg_win = (sum(x["pnl"] for x in scope if x["pnl"] > 0) / wins) if wins else 0.0
        avg_loss = (sum(x["pnl"] for x in scope if x["pnl"] <= 0) / losses) if losses else 0.0

        exit_counts = {}
        for item in scope:
            t = item["trade"]
            reason = (t.get("exit_reason") or "unknown").strip()
            exit_counts[reason] = exit_counts.get(reason, 0) + 1
        top_exit = sorted(exit_counts.items(), key=lambda x: x[1], reverse=True)[:2]
        top_exit_text = ", ".join([f"{k}:{v}" for k, v in top_exit]) if top_exit else "none"

        mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
        stop_pct = _fmt_pct(profile.get("stop_loss_pct")) if isinstance(profile, dict) else "N/A"
        target_pct = _fmt_pct(profile.get("profit_target_pct")) if isinstance(profile, dict) else "N/A"
        dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
        dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"
        strategy_text = strategy_intents.get(sid, "Configured strategy profile.")

        scope_text = "yesterday" if ("yesterday" in q and scope is not closed) else "selected period"

        diagnosis = []
        if wr >= 70 and "trailing_stop" in exit_counts:
            diagnosis.append("entries aligned with trend continuation and exits locked gains via trailing stops")
        if wr < 45:
            diagnosis.append("entry quality or regime alignment is weak in this period")
        if avg_win > 0 and abs(avg_loss) > avg_win:
            diagnosis.append("losses are larger than wins; stop/position sizing pressure remains")
        if any(k in {"stop_loss", "iv_crush_stop", "theta_burn"} for k, _ in top_exit):
            diagnosis.append("protective exits are active, likely from pullback or premium decay pressure")
        if not diagnosis:
            diagnosis.append("results are positive, but edge may be regime- and timing-dependent")

        recent = sorted(scope, key=lambda x: x["dt"] or datetime.min.replace(tzinfo=pytz.UTC), reverse=True)[:3]
        trade_lines = []
        for item in recent:
            t = item["trade"]
            pnl = item["pnl"]
            dt = item["dt"]
            dt_text = dt.strftime("%m-%d %H:%M") if dt else "N/A"
            direction = (t.get("direction") or "N/A")
            strike = t.get("strike")
            if not isinstance(strike, (int, float)):
                strike = _parse_strike(t.get("option_symbol"))
            strike_text = f"{strike:g}" if isinstance(strike, (int, float)) else "N/A"
            entry = t.get("entry_price")
            exit_px = t.get("exit_price")
            entry_text = f"{float(entry):.3f}" if isinstance(entry, (int, float)) else "N/A"
            exit_text = f"{float(exit_px):.3f}" if isinstance(exit_px, (int, float)) else "N/A"
            reason = t.get("exit_reason") or "unknown"
            hold = _fmt_secs(t.get("time_in_trade_seconds"))
            trade_lines.append(
                f"{dt_text} | {direction} {strike_text} | {entry_text}->{exit_text} | PnL {pnl:+.2f} | {reason} | hold {hold}"
            )

        lines = [
            f"Assessment: {sid} performed strongly in {scope_text} with WR {wr:.1f}% ({wins}W/{losses}L) and PnL ${total_pnl:.2f}.",
            f"Strategy: {strategy_text} (mode={mode}, DTE={dte_min}-{dte_max}, stop={stop_pct}, target={target_pct}).",
            f"Evidence: avg/trade ${avg:.2f}, avg win ${avg_win:.2f}, avg loss ${avg_loss:.2f}, top exits {top_exit_text}.",
        ]
        if trade_lines:
            lines.append("Recent trades:")
            lines.extend(trade_lines)
        lines.append(f"Likely Causes: {'; '.join(diagnosis)}.")
        lines.append("Next step: use `!askmore break down each trade with entry context and regime` for deeper trade-by-trade analysis.")
        return "\n".join(lines)

    paper = get_paper_stats()
    career = get_career_stats()
    acc = load_account()
    sim_context = _build_sim_context(question)

    try:
        answer = ask_ai(
            question,
            "Live market snapshot",
            paper,
            career,
            acc.get("trade_log", [])[-5:],
            sim_context,
            prior_context=None,
        )
    except Exception:
        answer = None

    # If AI returns an empty/weak/non-sim-specific reply for SIM questions, provide deterministic local analysis.
    answer_text = str(answer).strip() if answer is not None else ""
    weak_for_sim = (
        sim_context is not None
        and (
            not answer_text
            or len(answer_text) < 120
            or "sim" not in answer_text.lower()
        )
    )
    if weak_for_sim:
        fallback = _build_sim_fallback_answer(question)
        if fallback:
            answer = fallback

    if answer is None or not str(answer).strip():
        answer = "No response available."
    else:
        answer = str(answer).strip()
        if "!askmore" not in answer.lower():
            answer = (
                f"{answer}\n\n"
                "Follow-up: use `!askmore <question>` to continue from this exact context."
            )
        # Discord embed description hard limit is 4096; keep headroom for status lines.
        if len(answer) > 3400:
            answer = answer[:3400].rstrip() + "\n\n[truncated]"

    ASK_CONTEXT_CACHE[str(ctx.author.id)] = {
        "question": str(question),
        "answer": str(answer),
        "sim_context": sim_context,
        "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
    }

    await _send_embed(ctx, answer, title="AI Coach")


@bot.command()
async def askmore(ctx, *, question=None):
    if not question:
        await _send_embed(ctx, "Usage: `!askmore <follow-up question>`")
        return

    cache_key = str(ctx.author.id)
    last_ctx = ASK_CONTEXT_CACHE.get(cache_key)
    if not isinstance(last_ctx, dict):
        await _send_embed(ctx, "No prior `!ask` context found. Use `!ask` first.")
        return

    paper = get_paper_stats()
    career = get_career_stats()
    acc = load_account()

    prev_q = str(last_ctx.get("question") or "")
    prev_a = str(last_ctx.get("answer") or "")
    prev_ts = str(last_ctx.get("timestamp") or "N/A")
    sim_context = last_ctx.get("sim_context")

    def _load_sim_profiles():
        sim_config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "simulation", "sim_config.yaml")
        )
        try:
            with open(sim_config_path, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            return {}

    def _fmt_pct(val, decimals=1) -> str:
        try:
            return f"{float(val) * 100:.{decimals}f}%"
        except (TypeError, ValueError):
            return "N/A"

    def _fmt_secs(seconds) -> str:
        try:
            s = int(seconds)
        except (TypeError, ValueError):
            return "N/A"
        if s < 60:
            return f"{s}s"
        if s < 3600:
            return f"{s // 60}m"
        if s < 86400:
            return f"{s / 3600:.1f}h"
        return f"{s / 86400:.1f}d"

    def _parse_trade_dt(raw):
        if not raw:
            return None
        try:
            eastern = pytz.timezone("US/Eastern")
            dt = datetime.fromisoformat(str(raw))
            if dt.tzinfo is None:
                dt = eastern.localize(dt)
            else:
                dt = dt.astimezone(eastern)
            return dt
        except Exception:
            return None

    def _parse_strike(symbol):
        if not symbol or not isinstance(symbol, str):
            return None
        try:
            return int(symbol[-8:]) / 1000.0
        except Exception:
            return None

    def _extract_reason(entry_context):
        if not entry_context or "reason=" not in str(entry_context):
            return None
        try:
            return str(entry_context).split("reason=", 1)[1].split("|")[0].strip()
        except Exception:
            return None

    def _resolve_sim_id(q_text: str) -> str | None:
        # 1) explicit SIM reference in follow-up
        m = re.search(r"\bsim\s*(\d{1,2})\b", q_text, flags=re.IGNORECASE)
        if m:
            return f"SIM{int(m.group(1)):02d}"
        # 2) first SIM id from prior question
        m = re.search(r"\bsim\s*(\d{1,2})\b", prev_q, flags=re.IGNORECASE)
        if m:
            return f"SIM{int(m.group(1)):02d}"
        # 3) first SIM id in cached sim_context text
        if isinstance(sim_context, str):
            m = re.search(r"\bSIM\d{2}\b", sim_context)
            if m:
                return m.group(0)
        return None

    def _build_askmore_sim_fallback(q_text: str) -> str | None:
        sid = _resolve_sim_id(q_text)
        if not sid:
            return None
        profiles = _load_sim_profiles()
        profile = profiles.get(sid, {}) if isinstance(profiles, dict) else {}
        sim_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data", "sims", f"{sid}.json")
        )
        if not os.path.exists(sim_path):
            return f"{sid}: sim state file not found."

        try:
            with open(sim_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return f"{sid}: unable to read sim state file."

        trade_log = data.get("trade_log", []) if isinstance(data.get("trade_log"), list) else []
        open_trades = data.get("open_trades", []) if isinstance(data.get("open_trades"), list) else []
        closed = []
        for t in trade_log:
            try:
                pnl = float(t.get("realized_pnl_dollars"))
            except (TypeError, ValueError):
                continue
            dt = _parse_trade_dt(t.get("exit_time")) or _parse_trade_dt(t.get("entry_time"))
            closed.append({"trade": t, "pnl": pnl, "dt": dt})
        if not closed:
            return f"{sid}: no closed trades available yet (open={len(open_trades)})."

        q = q_text.lower()
        n = 3
        m = re.search(r"last\s+(\d+)\s+trades", q)
        if m:
            try:
                n = max(1, min(20, int(m.group(1))))
            except Exception:
                n = 3

        scope = closed
        if "yesterday" in q:
            yday = (datetime.now(pytz.timezone("US/Eastern")).date() - timedelta(days=1))
            scoped = [x for x in closed if x["dt"] and x["dt"].date() == yday]
            if scoped:
                scope = scoped

        scope_sorted = sorted(
            scope,
            key=lambda x: x["dt"] or datetime.min.replace(tzinfo=pytz.UTC),
            reverse=True,
        )
        picks = scope_sorted[:n]

        wins = sum(1 for x in scope if x["pnl"] > 0)
        total = len(scope)
        total_pnl = sum(x["pnl"] for x in scope)
        wr = (wins / total) * 100 if total else 0.0

        stop_pct = _fmt_pct(profile.get("stop_loss_pct")) if isinstance(profile, dict) else "N/A"
        target_pct = _fmt_pct(profile.get("profit_target_pct")) if isinstance(profile, dict) else "N/A"
        mode = profile.get("signal_mode", "N/A") if isinstance(profile, dict) else "N/A"
        dte_min = profile.get("dte_min", "N/A") if isinstance(profile, dict) else "N/A"
        dte_max = profile.get("dte_max", "N/A") if isinstance(profile, dict) else "N/A"

        lines = [
            f"Assessment: {sid} follow-up for {len(picks)} trade(s). Period WR={wr:.1f}% ({wins}/{total}) and PnL={total_pnl:+.2f}.",
            f"Strategy frame: mode={mode}, DTE={dte_min}-{dte_max}, stop={stop_pct}, target={target_pct}.",
            "Trade breakdown:",
        ]

        want_context = any(k in q for k in ["entry context", "regime", "context", "why"])
        for item in picks:
            t = item["trade"]
            dt = item["dt"]
            dt_text = dt.strftime("%m-%d %H:%M") if dt else "N/A"
            pnl = item["pnl"]
            direction = t.get("direction") or "N/A"
            strike = t.get("strike")
            if not isinstance(strike, (int, float)):
                strike = _parse_strike(t.get("option_symbol"))
            strike_text = f"{strike:g}" if isinstance(strike, (int, float)) else "N/A"
            entry = t.get("entry_price")
            exit_px = t.get("exit_price")
            entry_text = f"{float(entry):.3f}" if isinstance(entry, (int, float)) else "N/A"
            exit_text = f"{float(exit_px):.3f}" if isinstance(exit_px, (int, float)) else "N/A"
            exit_reason = t.get("exit_reason") or "unknown"
            hold = _fmt_secs(t.get("time_in_trade_seconds"))
            lines.append(
                f"- {dt_text} | {direction} {strike_text} | {entry_text}->{exit_text} | PnL {pnl:+.2f} | exit={exit_reason} | hold={hold}"
            )
            if want_context:
                regime = t.get("regime_at_entry") or "N/A"
                bucket = t.get("time_of_day_bucket") or "N/A"
                ectx = t.get("entry_context") or "N/A"
                reason = _extract_reason(ectx) or "N/A"
                lines.append(f"  context: regime={regime}, bucket={bucket}, signal_reason={reason}")
                if "entry context" in q:
                    lines.append(f"  entry_context: {ectx}")

        lines.append("Next step: use `!askmore compare winners vs losers for this SIM by regime/time bucket`.")
        return "\n".join(lines)

    prior_context = (
        f"Previous question: {prev_q}\n"
        f"Previous answer: {prev_a}\n"
        f"Previous timestamp ET: {prev_ts}\n"
        "Continue from this context and directly answer the follow-up."
    )

    try:
        answer = ask_ai(
            question,
            "Live market snapshot",
            paper,
            career,
            acc.get("trade_log", [])[-5:],
            sim_context,
            prior_context=prior_context,
        )
    except Exception:
        answer = None

    answer_text = str(answer).strip() if answer is not None else ""
    weak_for_sim = (
        sim_context is not None
        and (
            not answer_text
            or len(answer_text) < 120
            or "sim" not in answer_text.lower()
        )
    )

    if weak_for_sim:
        fallback = _build_askmore_sim_fallback(question)
        if fallback:
            answer = fallback
        else:
            answer = (
                "I could not produce a full continuation from model output this turn. "
                f"Prior question was: `{prev_q}`. "
                "Try `!askmore break down the last 3 trades with entry context and regime`."
            )
    elif answer is None or not str(answer).strip() or len(str(answer).strip()) < 40:
        answer = (
            "I could not produce a full continuation from model output this turn. "
            f"Prior question was: `{prev_q}`. "
            "Try a narrower follow-up like `!askmore break down the last 3 trades and exit reasons`."
        )
    else:
        answer = str(answer).strip()
        if "!askmore" not in answer.lower():
            answer = (
                f"{answer}\n\n"
                "Follow-up: use `!askmore <question>` to keep this same context."
            )
        if len(answer) > 3400:
            answer = answer[:3400].rstrip() + "\n\n[truncated]"

    ASK_CONTEXT_CACHE[cache_key] = {
        "question": str(question),
        "answer": str(answer),
        "sim_context": sim_context,
        "timestamp": datetime.now(pytz.timezone("US/Eastern")).isoformat(),
    }
    await _send_embed(ctx, answer, title="AI Coach")


@bot.event
async def on_command_error(ctx, error):

    logging.exception(f"[COMMAND ERROR] {error}")

    if isinstance(error, commands.MissingRequiredArgument):
        await _send_embed(ctx, "Missing required argument. Use `!help <command>`.")
        return

    if isinstance(error, commands.BadArgument):
        await _send_embed(ctx, "Invalid argument type. Use `!help <command>`.")
        return

    if isinstance(error, commands.CommandNotFound):
        await _send_embed(ctx, "Unknown command. Type `!help`.")
        return

    await _send_embed(ctx, "⚠️ Internal error occurred. Logged for review.")

bot.run(DISCORD_TOKEN)
