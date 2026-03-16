"""
core/freshness_monitor.py — Auto-transitions RuntimeState on stale data.

Escalation:
  bars > 300s  → DEGRADED (entries blocked, exits OK)
  bars > 600s  → EXIT_ONLY + alert
  broker > 300s → PANIC_LOCKDOWN + alert

Recovery:
  bars < 150s AND state is DEGRADED → TRADING_ENABLED
  broker heartbeat succeeds in PANIC_LOCKDOWN → RECONCILING → READY
"""

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

_BAR_DEGRADE_SEC   = 300   # 5 min — genuine outage, not just a slow bar
_BAR_EXIT_ONLY_SEC = 600   # 10 min — extended outage
_BROKER_PANIC_SEC  = 300
_BAR_RECOVER_SEC   = 150   # recover once data is < 2.5 min old
_HEARTBEAT_INTERVAL = 30   # broker ping every 30s

_cached_trading_client = None


def _broker_heartbeat_sync() -> bool:
    """Lightweight broker ping — calls get_clock() which is fast and free."""
    global _cached_trading_client
    try:
        if _cached_trading_client is None:
            from alpaca.trading.client import TradingClient
            api_key = os.getenv("APCA_API_KEY_ID")
            secret_key = os.getenv("APCA_API_SECRET_KEY")
            if not api_key or not secret_key:
                return False
            _cached_trading_client = TradingClient(api_key, secret_key, paper=True)
        _cached_trading_client.get_clock()
        return True
    except Exception:
        _cached_trading_client = None  # force re-create on next attempt
        return False


class FreshnessMonitor:
    """
    Background task. Checks data freshness every 10s and triggers state
    transitions. Register via setup_hook() using the same safe_task pattern.
    """

    def __init__(self, runtime, risk_supervisor) -> None:
        self.runtime = runtime
        self.supervisor = risk_supervisor
        self._alerted: dict[str, bool] = {}
        self._last_heartbeat: float = 0.0
        self._start_time: float = time.time()
        self._broker_ever_ok: bool = False  # set True after first successful ping

    async def _ping_broker(self) -> bool:
        """Async broker heartbeat. Stamps health on success."""
        now = time.time()
        if now - self._last_heartbeat < _HEARTBEAT_INTERVAL:
            return True  # too soon, skip
        self._last_heartbeat = now
        ok = await asyncio.to_thread(_broker_heartbeat_sync)
        if ok:
            self.supervisor.update_broker_health(time.time())
            self._broker_ever_ok = True
        return ok

    async def run(self, send_alert_fn) -> None:
        """
        send_alert_fn: async callable(str) — posts to the alert Discord channel.
        """
        from core.runtime_state import SystemState
        while True:
            try:
                await asyncio.sleep(10)
                from core.market_clock import market_is_open
                if not market_is_open():
                    continue

                # ── Broker heartbeat ──────────────────────────────────────
                await self._ping_broker()

                status = self.supervisor.get_status()
                bar_age    = status.get("bar_age_seconds")    or float("inf")
                broker_age = status.get("broker_age_seconds") or float("inf")

                current = self.runtime.state

                # ── Recovery from PANIC_LOCKDOWN ──────────────────────────
                if current == SystemState.PANIC_LOCKDOWN and broker_age < _BROKER_PANIC_SEC:
                    self.runtime.force_transition(
                        SystemState.RECONCILING,
                        "broker reachable, auto-recovering",
                    )
                    self.runtime.force_transition(
                        SystemState.READY,
                        "broker recovered",
                    )
                    self._alerted.pop("panic", None)
                    try:
                        await send_alert_fn(
                            "✅ **PANIC RECOVERED** — broker reachable again. State → READY."
                        )
                    except Exception:
                        pass
                    continue

                # ── Broker panic ──────────────────────────────────────────
                # Skip broker panic if we've never had a successful ping yet
                # (grace period — heartbeat needs time to complete first call)
                if (broker_age > _BROKER_PANIC_SEC
                        and self._broker_ever_ok
                        and current != SystemState.PANIC_LOCKDOWN
                        and current != SystemState.BOOTING
                        and current != SystemState.RECONCILING):
                    self.runtime.force_transition(
                        SystemState.PANIC_LOCKDOWN,
                        f"broker unreachable {broker_age:.0f}s",
                    )
                    if not self._alerted.get("panic"):
                        try:
                            await send_alert_fn(
                                f"⛔ **PANIC LOCKDOWN** — broker unreachable for {broker_age:.0f}s\n"
                                "All live entries blocked. Exits still allowed."
                            )
                        except Exception:
                            pass
                        self._alerted["panic"] = True
                    continue

                # ── Clear panic alert flag when no longer in PANIC_LOCKDOWN ─
                if current != SystemState.PANIC_LOCKDOWN and self._alerted.get("panic"):
                    self._alerted.pop("panic", None)

                # ── Bar EXIT_ONLY ─────────────────────────────────────────
                if bar_age > _BAR_EXIT_ONLY_SEC and current == SystemState.DEGRADED:
                    if self.runtime.transition(SystemState.EXIT_ONLY, f"bars {bar_age:.0f}s stale"):
                        if not self._alerted.get("exit_only"):
                            try:
                                await send_alert_fn(
                                    f"🔴 **EXIT ONLY** — market data stale for {bar_age:.0f}s\n"
                                    "Managing existing positions only. No new entries."
                                )
                            except Exception:
                                pass
                            self._alerted["exit_only"] = True
                    continue

                # ── Bar DEGRADED ──────────────────────────────────────────
                if bar_age > _BAR_DEGRADE_SEC and current == SystemState.TRADING_ENABLED:
                    self.runtime.add_degradation("stale_bars")
                    if self.runtime.transition(SystemState.DEGRADED, f"bars {bar_age:.0f}s stale"):
                        if not self._alerted.get("degraded"):
                            try:
                                await send_alert_fn(
                                    f"⚠️ **DEGRADED** — market data stale for {bar_age:.0f}s\n"
                                    "New entries blocked; exits still allowed."
                                )
                            except Exception:
                                pass
                            self._alerted["degraded"] = True
                    continue

                # ── READY → TRADING_ENABLED on market open + fresh data ──
                if bar_age < _BAR_RECOVER_SEC and current == SystemState.READY:
                    if self.runtime.transition(SystemState.TRADING_ENABLED, "market open, data fresh"):
                        self._alerted.clear()
                        try:
                            await send_alert_fn("✅ **TRADING ENABLED** — market open, data fresh.")
                        except Exception:
                            pass
                    continue

                # ── Recovery from DEGRADED ────────────────────────────────
                if bar_age < _BAR_RECOVER_SEC and current == SystemState.DEGRADED:
                    self.runtime.clear_degradation("stale_bars")
                    if not self.runtime.degradation_reasons():
                        if self.runtime.transition(SystemState.TRADING_ENABLED, "freshness_recovered"):
                            self._alerted.clear()
                            try:
                                await send_alert_fn("✅ **TRADING ENABLED** — data fresh, trading re-enabled.")
                            except Exception:
                                pass

            except Exception as e:
                logger.error("freshness_monitor_error: %s", e, exc_info=True)
                await asyncio.sleep(30)
