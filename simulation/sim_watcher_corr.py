# simulation/sim_watcher_corr.py
"""
Directional-crowding / correlation tracker extracted from sim_watcher.py.
All mutable state lives here; sim_watcher imports helpers from this module.
"""
import logging
import pytz
import discord
from datetime import datetime, time

from simulation.sim_report_helpers import _format_et
from interface.fmt import ab, A, lbl

# ── Correlation / Directional-Crowding Tracker ──────────────────────────────
_CORR_WINDOW_SEC = 600       # 10-min rolling window
_CORR_COOLDOWN_SEC = 1800    # 30-min between alerts
_CORR_MIN_SIMS = 4           # alert if ≥ this many sims entered same direction
_CORR_MIN_FAMILIES = 3       # …and came from ≥ this many distinct strategy families
_CORR_HIST_CAP = 20          # max entries kept in history for !correlation history
_CORR_CLOSE_GUARD = time(15, 50)  # stop alerting at/after 15:50 ET

_recent_entries: list[dict] = []          # {sim_id, direction, family, ts}
_correlation_alert_hist: list[dict] = []  # {ts, direction, sims, families, count}
_corr_last_alert_ts: datetime | None = None
_corr_channel_id: int | None = None


def _now_et() -> datetime:
    return datetime.now(pytz.timezone("US/Eastern"))


def _corr_family(sim_id: str) -> str:
    """Return the strategy-family string for a sim, or 'unknown'."""
    try:
        from simulation.sim_signals import _SIGNAL_MODE_FAMILY
        from simulation import sim_watcher as _sw
        profile = _sw._SIM_PROFILES.get(sim_id, {})
        mode = str(profile.get("signal_mode", "")).upper()
        return _SIGNAL_MODE_FAMILY.get(mode, "unknown")
    except Exception:
        return "unknown"


def _record_corr_entry(sim_id: str, direction: str) -> None:
    """Record a new entry in the rolling window, evicting stale records."""
    global _recent_entries
    now = _now_et()
    cutoff = now.timestamp() - _CORR_WINDOW_SEC
    _recent_entries = [e for e in _recent_entries if e["ts"].timestamp() >= cutoff]
    # Deduplicate: remove any earlier entry for this sim so it doesn't double-count
    _recent_entries = [e for e in _recent_entries if e["sim_id"] != sim_id]
    _recent_entries.append({
        "sim_id": sim_id,
        "direction": direction,
        "family": _corr_family(sim_id),
        "ts": now,
    })


async def _maybe_fire_corr_alert() -> None:
    """Check rolling window; send Discord alert if crowding thresholds met."""
    global _corr_last_alert_ts, _correlation_alert_hist
    from simulation import sim_watcher as _sw
    now = _now_et()
    # Market close guard
    if now.time() >= _CORR_CLOSE_GUARD:
        return
    # Cooldown guard
    if _corr_last_alert_ts is not None:
        elapsed = (now - _corr_last_alert_ts).total_seconds()
        if elapsed < _CORR_COOLDOWN_SEC:
            return

    cutoff = now.timestamp() - _CORR_WINDOW_SEC
    fresh = [e for e in _recent_entries if e["ts"].timestamp() >= cutoff]

    for direction in ("call", "put"):
        same_dir = [e for e in fresh if str(e["direction"]).lower() == direction]
        families = {e["family"] for e in same_dir}
        if len(same_dir) >= _CORR_MIN_SIMS and len(families) >= _CORR_MIN_FAMILIES:
            sim_ids = [e["sim_id"] for e in same_dir]
            _corr_last_alert_ts = now

            hist_entry = {
                "ts": now,
                "direction": direction.upper(),
                "sims": sim_ids,
                "families": sorted(families),
                "count": len(same_dir),
            }
            _correlation_alert_hist.append(hist_entry)
            if len(_correlation_alert_hist) > _CORR_HIST_CAP:
                _correlation_alert_hist = _correlation_alert_hist[-_CORR_HIST_CAP:]

            if _sw._SIM_BOT is not None and _corr_channel_id:
                channel = _sw._SIM_BOT.get_channel(_corr_channel_id)
                if channel:
                    dir_emoji = "📈" if direction == "call" else "📉"
                    dir_label = "CALLS" if direction == "call" else "PUTS"
                    embed = discord.Embed(
                        title=f"⚠️ Directional Crowding — {len(same_dir)} Sims {dir_emoji} {dir_label}",
                        color=0xE67E22,
                    )
                    embed.add_field(
                        name="Sims",
                        value=ab(A("  ".join(sim_ids), "yellow", bold=True)),
                        inline=False,
                    )
                    embed.add_field(
                        name="Strategy Families",
                        value=ab(A("  ·  ".join(sorted(families)), "cyan")),
                        inline=False,
                    )
                    embed.add_field(
                        name="Window",
                        value=ab(
                            f"{lbl('Sims')} {A(str(len(same_dir)), 'white', bold=True)}  |  "
                            f"{lbl('Families')} {A(str(len(families)), 'white', bold=True)}  |  "
                            f"{lbl('Window')} {A('10 min', 'gray')}"
                        ),
                        inline=False,
                    )
                    embed.set_footer(text=f"Correlation alert · {_format_et(now)}")
                    try:
                        await channel.send(embed=embed)
                    except Exception:
                        logging.exception("corr_alert_send_error")
            break  # only one direction can trip per cycle


def set_corr_channel(channel_id: int) -> None:
    global _corr_channel_id
    _corr_channel_id = channel_id


async def correlation_tracker_loop(channel_id: int) -> None:
    """Idle loop — real work happens in _maybe_fire_corr_alert() called from sim_entry_loop.
    This loop exists so the bot can register it as a safe_task and it can be exported cleanly.
    It also prunes stale _recent_entries on a 60-second heartbeat."""
    import asyncio
    global _recent_entries
    set_corr_channel(channel_id)
    while True:
        try:
            now_ts = _now_et().timestamp()
            cutoff = now_ts - _CORR_WINDOW_SEC
            _recent_entries = [e for e in _recent_entries if e["ts"].timestamp() >= cutoff]
        except Exception:
            logging.exception("correlation_tracker_loop_error")
        await asyncio.sleep(60)


def get_correlation_state() -> dict:
    """Return current correlation state for the !correlation command."""
    now = _now_et()
    cutoff = now.timestamp() - _CORR_WINDOW_SEC
    fresh = [e for e in _recent_entries if e["ts"].timestamp() >= cutoff]
    return {
        "window_entries": fresh,
        "alert_history": list(_correlation_alert_hist),
        "last_alert_ts": _corr_last_alert_ts,
        "cooldown_remaining": max(
            0,
            _CORR_COOLDOWN_SEC - (now - _corr_last_alert_ts).total_seconds()
        ) if _corr_last_alert_ts else 0,
    }
