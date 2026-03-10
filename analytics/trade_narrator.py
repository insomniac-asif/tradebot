"""
Trade Narrator — GPT-powered trade analysis module.
Generates human-readable reasoning for why a trade was entered/exited
and whether the strategy thesis played out.

Usage:
    from analytics.trade_narrator import narrate_trade
    narrative = await narrate_trade(trade, candle_data, sim_config)
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

BASE_DIR      = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass
CACHE_DIR     = BASE_DIR / "data" / "trade_narratives"
LOG_PATH      = BASE_DIR / "logs" / "narrator.log"
CACHE_MAX_AGE = 1800  # 30 minutes — auto-expire cached narratives

# Ensure directories exist
CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# Logger — separate log file for narrator
_log = logging.getLogger("trade_narrator")
if not _log.handlers:
    _fh = logging.FileHandler(LOG_PATH)
    _fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _log.addHandler(_fh)
    _log.setLevel(logging.WARNING)

# Semaphore: max 3 concurrent GPT calls
_SEM = asyncio.Semaphore(3)

# ---------------------------------------------------------------------------
# Strategy descriptions for the GPT prompt
# ---------------------------------------------------------------------------
_STRATEGY_DESCRIPTIONS = {
    "MEAN_REVERSION":               "fades overextended moves back toward VWAP/mean; looks for overbought/oversold conditions with RSI extremes and failed momentum",
    "VWAP_REVERSION":               "expects price to revert to VWAP after deviation; trades the rubber-band snap back to VWAP",
    "ZSCORE_BOUNCE":                "statistical mean-reversion using Z-score of price relative to rolling mean; enters when Z-score exceeds ±2σ",
    "FAILED_BREAKOUT_REVERSAL":     "fades failed breakout attempts; enters when a breakout above/below key level reverses back through the level",
    "EXTREME_EXTENSION_FADE":       "fades extreme 1-minute candle extensions; enters counter-trend when a single bar extends far beyond normal ATR",
    "BREAKOUT":                     "enters on confirmed breakouts above/below consolidation ranges with volume confirmation",
    "ORB_BREAKOUT":                 "Opening Range Breakout — trades breakouts above/below the first 30-minute opening range",
    "AFTERNOON_BREAKOUT":           "breakout strategy focused on afternoon session range expansions",
    "OPENING_DRIVE":                "trades the momentum of the opening drive continuation in the first 30 minutes",
    "TREND_PULLBACK":               "trend-following pullback strategy; enters on retracements within an established trend at EMA or VWAP support/resistance",
    "SWING_TREND":                  "multi-bar swing trend continuation; holds positions through normal retracements targeting larger moves",
    "VWAP_CONTINUATION":            "trend continuation using VWAP as dynamic support/resistance; enters when price bounces off VWAP in trend direction",
    "TREND_RECLAIM":                "enters when price reclaims a key level (VWAP, EMA, prior high/low) after losing it, signaling trend resumption",
    "OPPORTUNITY":                  "opportunistic strategy that adapts to current market conditions across multiple signal types",
    "FVG_4H":                       "Smart Money Concept — enters when price retests an unfilled Fair Value Gap on 4-hour aggregated bars; the gap marks an imbalance where institutional order flow moved price so fast it left an unfilled range, and the thesis is that price returns to fill it",
    "FVG_5M":                       "Smart Money Concept — same FVG logic as FVG_4H but on 5-minute bars for intraday entries; requires volume above the 20-bar average at gap formation to confirm institutional participation",
    "LIQUIDITY_SWEEP":              "Smart Money Concept — fades stop-hunt reversals; enters when price spikes beyond a structural swing high/low, then closes back inside it within 3 bars on a full-bodied candle, indicating the sweep collected retail stops and smart money is reversing",
    "FVG_SWEEP_COMBO":              "high-conviction Smart Money confluence — requires both a liquidity sweep and an unfilled 5-minute FVG pointing in the same direction simultaneously; the sweep provides the directional catalyst and the FVG acts as a nearby price magnet",
    "FLOW_DIVERGENCE":              "counter-trend divergence signal — uses EMA alignment as a bullish baseline but requires three contradicting flow factors (negative price momentum, elevated volume, declining VWAP slope) to all confirm before fading; targets institutional distribution or accumulation hidden within a trending EMA structure",
    "MULTI_TF_CONFIRM":             "multi-timeframe trend alignment — requires EMA9 above EMA20 on both 5-minute and 15-minute aggregated bars simultaneously, plus current price above VWAP; fires less frequently than single-timeframe signals but with higher directional confidence",
    "GAP_FADE":                     "fades the opening gap back toward the pre-gap close; fires when a gap of 0.3% or more from prior close has not yet been filled, trading in the direction of reversion; best on moderate intraday gaps that represent overreaction rather than genuine directional continuation",
    "VPOC_REVERSION":               "volume Point of Control reversion — builds a 30-bar volume profile in $0.50 buckets, identifies the highest-volume price level (VPOC), then enters when price is more than 0.5% displaced from it with RSI confirming the dislocation; thesis is that price gravitates back to the session's highest-volume fair-value level",
    "OPENING_RANGE_RECLAIM":        "fades failed opening-range breakouts — the first 6 bars define the OR high/low; enters when price broke out of the range then closes back inside it, confirming the breakout failed and trapped momentum traders; requires VWAP alignment and RSI within non-extreme bounds",
    "VOL_COMPRESSION_BREAKOUT":     "volatility regime transition — detects ATR compression (prior bar ATR below 60% of 20-bar mean) followed by expansion (current bar ATR above 90% of mean); enters in the direction of the expansion bar relative to EMA9; targets the first sustained directional move out of a tight consolidation period",
    "VOL_SPIKE_FADE":               "fades extreme ATR spikes — requires current ATR above 1.5× the 20-bar mean and price more than 1.5× ATR mean away from VWAP, with RSI above 70 (bearish fade) or below 30 (bullish fade); thesis is that capitulation or short-squeeze spikes in liquid index ETFs tend to exhaust quickly and revert",
}

# ---------------------------------------------------------------------------
# Fallback narrative (when GPT is unavailable)
# ---------------------------------------------------------------------------
_FALLBACK = {
    "entry_reasoning":   "Analysis unavailable — GPT service unreachable or API key not set.",
    "exit_reasoning":    "Analysis unavailable.",
    "outcome_analysis":  "Analysis unavailable.",
    "strategy_summary":  "Analysis unavailable.",
    "key_levels": {
        "entry": None, "exit": None,
        "stop_loss": None, "take_profit_1": None, "take_profit_2": None,
        "support": None, "resistance": None,
    },
    "grade": "N/A",
    "tags": [],
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cache_path(sim_id: str, trade_id: str) -> Path:
    safe = re.sub(r"[^\w\-]", "_", trade_id)
    return CACHE_DIR / f"{sim_id}_{safe}.json"


def _load_cache(sim_id: str, trade_id: str) -> dict | None:
    p = _cache_path(sim_id, trade_id)
    if not p.exists():
        return None
    # Expire cache after CACHE_MAX_AGE seconds
    age = time.time() - p.stat().st_mtime
    if age > CACHE_MAX_AGE:
        try:
            p.unlink()
        except Exception:
            pass
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _save_cache(sim_id: str, trade_id: str, narrative: dict) -> None:
    try:
        _cache_path(sim_id, trade_id).write_text(json.dumps(narrative, indent=2))
    except Exception as e:
        _log.warning("Cache write failed: %s", e)


def _fmt_candles(candle_data: list[dict]) -> str:
    """Format candle list as a compact OHLCV table (≤40 rows, keeps token count low)."""
    if not candle_data:
        return "(no candle data available)"
    rows = candle_data[-40:] if len(candle_data) > 40 else candle_data
    lines = ["Time         Open    High    Low     Close   Vol"]
    for c in rows:
        t = str(c.get("t", c.get("timestamp", c.get("time", ""))))[:16]
        try:
            lines.append(
                f"{t:<16} {float(c.get('o', c.get('open',0))):.2f}  "
                f"{float(c.get('h', c.get('high',0))):.2f}  "
                f"{float(c.get('l', c.get('low',0))):.2f}  "
                f"{float(c.get('c', c.get('close',0))):.2f}  "
                f"{int(c.get('v', c.get('volume',0)))}"
            )
        except Exception:
            pass
    return "\n".join(lines)


def _fmt_time(ts: str | None) -> str:
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts)
        return dt.strftime("%H:%M ET")
    except Exception:
        return str(ts)[:16]


def _build_prompt(trade: dict, candle_data: list[dict], sim_config: dict) -> str:
    signal_mode = trade.get("signal_mode") or sim_config.get("signal_mode", "UNKNOWN")
    strat_desc  = _STRATEGY_DESCRIPTIONS.get(signal_mode, "a quantitative options trading strategy")

    # Resolve underlying symbol from trade dict (set by sim_engine) or OCC prefix fallback
    underlying = trade.get("symbol") or ""
    if not underlying:
        opt_prefix = re.match(r'^([A-Z]{1,6})', (trade.get("option_symbol") or "").upper())
        underlying = opt_prefix.group(1) if opt_prefix else "SPY"

    direction   = trade.get("direction", "?")
    opt_sym     = trade.get("option_symbol", "?")
    strike      = trade.get("strike", "?")
    ctype       = trade.get("contract_type", "C")
    expiry      = trade.get("expiry", "?")
    entry_p     = trade.get("entry_price", "?")
    exit_p      = trade.get("exit_price", "?")
    entry_t     = _fmt_time(trade.get("entry_time"))
    exit_t      = _fmt_time(trade.get("exit_time"))
    pnl_d       = trade.get("realized_pnl_dollars", 0) or 0
    pnl_pct     = round((trade.get("realized_pnl_pct") or 0) * 100, 1)
    exit_reason = trade.get("exit_reason", "?")
    stop_price  = trade.get("stop")
    target_p    = trade.get("target")
    mae         = trade.get("mae_pct") or trade.get("mae")
    mfe         = trade.get("mfe_pct") or trade.get("mfe")
    regime      = trade.get("regime_at_entry", "?")
    bucket      = trade.get("time_of_day_bucket", "?")
    struct_sc   = trade.get("structure_score", "?")
    ml_prob     = trade.get("ml_probability", "?")

    candle_table = _fmt_candles(candle_data)

    return f"""You are a professional options trading analyst reviewing a completed trade for a quantitative sim.

STRATEGY: {signal_mode}
Description: {strat_desc}

TRADE DETAILS:
  Underlying:   {underlying}
  Contract:     {opt_sym}
  Direction:    {direction} ({'CALL' if ctype == 'C' else 'PUT'} @ ${strike}, expires {expiry})
  Entry:        ${entry_p} @ {entry_t}
  Exit:         ${exit_p} @ {exit_t}
  P&L:          ${pnl_d:+.2f} ({pnl_pct:+.1f}%)
  Exit Reason:  {exit_reason}
  Stop Loss:    ${stop_price if stop_price else '—'}
  Take Profit:  ${target_p if target_p else '—'}
  MAE:          {f'{float(mae)*100:.1f}%' if mae is not None else '—'}  (max adverse excursion)
  MFE:          {f'{float(mfe)*100:.1f}%' if mfe is not None else '—'}  (max favorable excursion)
  Regime:       {regime}
  Time Bucket:  {bucket}
  Struct Score: {struct_sc}
  ML Prob:      {ml_prob}

1-MINUTE {underlying} CANDLES (covering trade window ± context):
{candle_table}

Respond ONLY with valid JSON matching this exact schema — no markdown, no commentary:
{{
  "entry_reasoning": "2-4 sentences: why did {signal_mode} fire here? What did price/indicators show?",
  "exit_reasoning": "2-3 sentences: why was the trade exited this way ({exit_reason})? Was it clean?",
  "outcome_analysis": "2-4 sentences: why did it work or fail? Did the thesis play out? What could have been better?",
  "strategy_summary": "1 sentence max: plain English summary of what this trade was doing",
  "key_levels": {{
    "entry": {entry_p},
    "exit": {exit_p},
    "stop_loss": {stop_price if stop_price else 'null'},
    "take_profit_1": {target_p if target_p else 'null'},
    "take_profit_2": null,
    "support": <identify from candles or null>,
    "resistance": <identify from candles or null>
  }},
  "grade": "<A|B|C|D|F based on execution quality, not just outcome>",
  "tags": ["<2-5 tags from: clean_entry, early_exit, thesis_held, thesis_failed, momentum_fade, chased_entry, tight_stop, held_through_dd, textbook, full_target, partial_target, stopped_out, time_decay_exit, eod_close>"]
}}"""


# ---------------------------------------------------------------------------
# Main async function
# ---------------------------------------------------------------------------

async def narrate_trade(
    trade: dict,
    candle_data: list[dict],
    sim_config: dict,
    force_refresh: bool = False,
) -> dict:
    """
    Generate a GPT narrative for a completed trade.
    Returns cached result if available (unless force_refresh=True).
    """
    sim_id   = trade.get("sim_id", "UNKNOWN")
    trade_id = trade.get("trade_id", "")

    # Check cache
    if not force_refresh and trade_id:
        cached = _load_cache(sim_id, trade_id)
        if cached:
            return cached

    # Re-read .env on each call so key rotations don't require a process restart
    try:
        from dotenv import load_dotenv as _ldenv
        _ldenv(BASE_DIR / ".env", override=True)
    except Exception:
        pass
    api_key = os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        _log.warning("OPENAI_API_KEY not set — returning fallback narrative")
        return _FALLBACK.copy()

    prompt = _build_prompt(trade, candle_data, sim_config)

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=api_key)
        model  = os.environ.get("GPT_MODEL", "gpt-4o-mini")

        async with _SEM:
            resp = await client.chat.completions.create(
                model=model,
                max_tokens=800,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )

        raw = resp.choices[0].message.content.strip()

        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        narrative = json.loads(raw)

        # Ensure all required keys exist (merge with fallback for safety)
        for k, v in _FALLBACK.items():
            if k not in narrative:
                narrative[k] = v
        if "key_levels" not in narrative or not isinstance(narrative["key_levels"], dict):
            narrative["key_levels"] = _FALLBACK["key_levels"].copy()
        for kl_key, kl_val in _FALLBACK["key_levels"].items():
            if kl_key not in narrative["key_levels"]:
                narrative["key_levels"][kl_key] = kl_val

        # Cache result
        if trade_id:
            _save_cache(sim_id, trade_id, narrative)

        return narrative

    except json.JSONDecodeError as e:
        _log.error("GPT returned invalid JSON for %s/%s: %s", sim_id, trade_id, e)
        return _FALLBACK.copy()
    except Exception as e:
        err_str = str(e)
        if "401" in err_str or "Incorrect API key" in err_str or "AuthenticationError" in type(e).__name__:
            _log.error("GPT auth failed for %s/%s — API key invalid or expired: %s", sim_id, trade_id, err_str[:120])
            fb = _FALLBACK.copy()
            fb["entry_reasoning"] = "Analysis unavailable — OpenAI API key invalid or expired. Update OPENAI_API_KEY in .env"
            return fb
        _log.error("GPT call failed for %s/%s: %s", sim_id, trade_id, e)
        return _FALLBACK.copy()
