# interface/watcher_preopen_probe.py
#
# Snapshot probe logic extracted from watcher_preopen.py.
# Contains: _run_snapshot_probe

import asyncio
import os
from datetime import datetime

from core.data_service import _load_symbol_registry
from core.debug import debug_log


async def _run_snapshot_probe(last_close, now_et: datetime) -> str | None:
    """
    Attempt a diagnostic probe of the Alpaca option snapshot API.
    Returns a probe summary string, or None on failure.
    Only called when contract_status != "OK".
    """
    try:
        from alpaca.data.historical import OptionHistoricalDataClient
        from alpaca.data.requests import OptionChainRequest, OptionSnapshotRequest
        import alpaca.data.enums as alpaca_enums
        from core.rate_limiter import rate_limit_wait
        api_key = os.getenv("APCA_API_KEY_ID")
        secret_key = os.getenv("APCA_API_SECRET_KEY")
        if not (api_key and secret_key and isinstance(last_close, (int, float))):
            return None

        client = OptionHistoricalDataClient(api_key, secret_key)
        expiry_date = now_et.date()
        wait = rate_limit_wait("alpaca_option_chain", 0.5)
        if wait > 0:
            await asyncio.sleep(wait)
        contract_type = getattr(alpaca_enums, "ContractType", None)
        options_feed = getattr(alpaca_enums, "OptionsFeed", None)
        feed_val = None
        try:
            desired = os.getenv("ALPACA_OPTIONS_FEED", "").strip().lower()
            if options_feed is not None:
                if desired == "opra":
                    feed_val = options_feed.OPRA
                elif desired == "indicative":
                    feed_val = options_feed.INDICATIVE
        except Exception:
            feed_val = None
        type_call = None
        try:
            if contract_type is not None and hasattr(contract_type, "CALL"):
                type_call = contract_type.CALL
        except Exception:
            type_call = None
        # Use first optionable symbol from registry for probe (SPY fallback)
        try:
            _reg = _load_symbol_registry()
            _probe_sym = next(
                (s for s, v in _reg.items() if v.get("options")),
                next(iter(_reg), "")
            )
        except Exception:
            _probe_sym = ""
        chain = client.get_option_chain(
            OptionChainRequest(
                underlying_symbol=_probe_sym,
                type=type_call,
                feed=feed_val,
                expiration_date=expiry_date
            )
        )
        symbol = None
        if isinstance(chain, dict):
            symbol = next(iter(chain.keys()), None)
        else:
            data = getattr(chain, "data", None)
            if isinstance(data, dict):
                symbol = next(iter(data.keys()), None)
            elif isinstance(data, list) and data:
                symbol = getattr(data[0], "symbol", None) or (data[0].get("symbol") if isinstance(data[0], dict) else None)
            chains = getattr(chain, "chains", None)
            if symbol is None and isinstance(chains, dict):
                symbol = next(iter(chains.keys()), None)
            df_chain = getattr(chain, "df", None)
            if symbol is None and df_chain is not None:
                try:
                    if "symbol" in df_chain.columns and not df_chain.empty:
                        symbol = df_chain["symbol"].iloc[0]
                except Exception:
                    symbol = None
        if not symbol:
            return None

        snapshot_probe_lines = []

        def _snap_meta(resp, label: str):
            try:
                if isinstance(resp, dict):
                    keys = list(resp.keys())[:3]
                    size = len(resp)
                    keys_text = ",".join(keys) if keys else "none"
                    snapshot_probe_lines.append(f"{label}: size={size} keys={keys_text}")
                else:
                    snapshot_probe_lines.append(f"{label}: type={type(resp).__name__}")
            except Exception:
                snapshot_probe_lines.append(f"{label}: error")

        try:
            debug_log(
                "preopen_snapshot_probe_request",
                symbol=symbol,
                expiry=expiry_date.isoformat(),
            )
        except Exception:
            pass
        wait = rate_limit_wait("alpaca_option_snapshot", 0.5)
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            if feed_val is not None:
                req = OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=feed_val)
            else:
                req = OptionSnapshotRequest(symbol_or_symbols=[symbol])
        except Exception:
            req = OptionSnapshotRequest(symbol_or_symbols=[symbol])
        snap_resp = client.get_option_snapshot(req)
        _snap_meta(snap_resp, "default")
        if isinstance(snap_resp, dict) and len(snap_resp) == 0 and feed_val is None:
            try:
                options_feed = getattr(alpaca_enums, "OptionsFeed", None)
                if options_feed is not None and hasattr(options_feed, "INDICATIVE"):
                    debug_log("preopen_snapshot_probe_retry", symbol=symbol, feed="indicative")
                    req = OptionSnapshotRequest(symbol_or_symbols=[symbol], feed=options_feed.INDICATIVE)
                    snap_resp = client.get_option_snapshot(req)
                    _snap_meta(snap_resp, "indicative")
            except Exception:
                pass
        snap_type = type(snap_resp).__name__
        keys_hint = ""
        if isinstance(snap_resp, dict):
            keys_hint = f" keys={list(snap_resp.keys())[:5]}"
        result = f"probe_symbol={symbol} resp_type={snap_type}{keys_hint}"
        if snapshot_probe_lines:
            result = f"{result} | feeds: " + " / ".join(snapshot_probe_lines)
        return result
    except Exception as e:
        return f"probe_error={str(e).splitlines()[0]}"
