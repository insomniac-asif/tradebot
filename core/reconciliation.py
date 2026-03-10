"""
core/reconciliation.py — Startup broker vs internal state reconciliation.

Compares Alpaca's live option positions against SIM00's open_trades JSON.
Must be run at startup before any live order can be placed.
"""

import json
import logging
import os
from pathlib import Path
from typing import List, Dict

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
SIM00_PATH = BASE_DIR / "data" / "sims" / "SIM00.json"


class ReconciliationResult:
    def __init__(self):
        self.broker_positions: List[Dict] = []
        self.internal_positions: List[Dict] = []
        self.orphaned_broker: List[Dict] = []     # in broker, not in JSON
        self.orphaned_internal: List[Dict] = []    # in JSON, not in broker
        self.matched: List[Dict] = []
        self.mismatched: List[Dict] = []           # same symbol, different qty
        self.clean: bool = False
        self.error: str = ""

    def summary(self) -> str:
        lines = [
            f"Broker positions:   {len(self.broker_positions)}",
            f"Internal positions: {len(self.internal_positions)}",
            f"Matched:            {len(self.matched)}",
            f"Orphaned broker:    {len(self.orphaned_broker)}",
            f"Orphaned internal:  {len(self.orphaned_internal)}",
            f"Mismatched qty:     {len(self.mismatched)}",
            f"Clean:              {self.clean}",
        ]
        if self.error:
            lines.append(f"Error:              {self.error}")
        return "\n".join(lines)


async def reconcile_live_positions() -> ReconciliationResult:
    """
    Compare Alpaca broker state against SIM00's internal open_trades.

    Uses the same TradingClient credentials as option_executor.py.
    Matches by OCC option symbol.
    result.clean = True only if all orphan/mismatch lists are empty.
    """
    result = ReconciliationResult()

    try:
        # ── 1. Load SIM00 internal open_trades ────────────────────────────
        internal_map: dict[str, int] = {}   # option_symbol → qty
        if SIM00_PATH.exists():
            try:
                raw = json.loads(SIM00_PATH.read_text(encoding="utf-8"))
                for t in raw.get("open_trades", []):
                    if not isinstance(t, dict):
                        continue
                    sym = t.get("option_symbol")
                    qty = t.get("qty")
                    if sym and qty is not None:
                        try:
                            internal_map[sym] = int(qty)
                            result.internal_positions.append({"symbol": sym, "qty": int(qty)})
                        except (TypeError, ValueError):
                            pass
            except Exception as e:
                logger.warning("reconciliation_sim00_load_error: %s", e)

        # ── 2. Fetch live broker positions ─────────────────────────────────
        api_key = os.getenv("APCA_API_KEY_ID", "")
        secret_key = os.getenv("APCA_API_SECRET_KEY", "")

        broker_map: dict[str, int] = {}   # symbol → qty
        if api_key and secret_key:
            try:
                from alpaca.trading.client import TradingClient
                import asyncio
                client = TradingClient(api_key, secret_key, paper=True)
                positions = await asyncio.to_thread(client.get_all_positions)
                for pos in (positions or []):
                    sym = getattr(pos, "symbol", None)
                    qty_raw = getattr(pos, "qty", None)
                    if sym and qty_raw is not None:
                        try:
                            qty_int = int(float(qty_raw))
                        except (TypeError, ValueError):
                            qty_int = 0
                        # Only track options (OCC symbols: uppercase + 6-digit date + C/P + 8-digit strike)
                        if len(sym) > 10:
                            broker_map[sym] = qty_int
                            result.broker_positions.append({"symbol": sym, "qty": qty_int})
            except Exception as e:
                logger.warning("reconciliation_broker_fetch_error: %s", e)
                result.error = f"broker_fetch_failed: {e}"
                # If we can't reach broker, treat as non-clean (conservative)
                result.clean = False
                return result
        else:
            logger.info("reconciliation_skipped: no Alpaca keys configured")
            result.clean = True   # no live trading configured → trivially clean
            return result

        # ── 3. Match broker vs internal ────────────────────────────────────
        all_symbols = set(broker_map) | set(internal_map)
        for sym in all_symbols:
            in_broker = sym in broker_map
            in_internal = sym in internal_map
            b_qty = broker_map.get(sym, 0)
            i_qty = internal_map.get(sym, 0)

            if in_broker and in_internal:
                if b_qty == i_qty:
                    result.matched.append({"symbol": sym, "qty": b_qty})
                else:
                    result.mismatched.append({
                        "symbol": sym,
                        "broker_qty": b_qty,
                        "internal_qty": i_qty,
                    })
            elif in_broker and not in_internal:
                result.orphaned_broker.append({"symbol": sym, "qty": b_qty})
            elif in_internal and not in_broker:
                result.orphaned_internal.append({"symbol": sym, "qty": i_qty})

        result.clean = (
            len(result.orphaned_broker) == 0
            and len(result.orphaned_internal) == 0
            and len(result.mismatched) == 0
        )

        if result.clean:
            logger.info("reconciliation_clean: %d matched positions", len(result.matched))
        else:
            logger.error("reconciliation_mismatch:\n%s", result.summary())

    except Exception as e:
        logger.error("reconciliation_failed: %s", e, exc_info=True)
        result.clean = False
        result.error = str(e)

    return result
