import time
import threading
import logging

# Simple shared rate limiter for Alpaca REST calls.
# Use rate_limit_wait() to get a suggested delay and optionally sleep.
# Singleton accessors for the richer resilience primitives live in api_resilience.py;
# re-exported here for convenience so callers only need one import.
from core.api_resilience import get_bucket, get_cache, get_breaker, resilience_stats  # noqa: F401

_LOCK = threading.Lock()
_LAST_CALL = {}


def rate_limit_wait(key: str, min_interval: float) -> float:
    """
    Returns seconds to wait before the next call for this key.
    Also reserves the next slot to prevent thundering herds.
    """
    now = time.monotonic()
    with _LOCK:
        last = _LAST_CALL.get(key, 0.0)
        wait = max(0.0, float(min_interval) - (now - last))
        # Reserve the next slot even if we need to wait
        _LAST_CALL[key] = now + wait if wait > 0 else now
    return wait


def rate_limit_sleep(key: str, min_interval: float, sleep_fn=time.sleep) -> float:
    """
    Sleep for the required wait time (if any) and return the wait.
    """
    wait = rate_limit_wait(key, min_interval)
    if wait > 0:
        logging.warning("alpaca_rate_limit_wait: key=%s wait=%.2fs", key, wait)
        sleep_fn(wait)
    return wait
