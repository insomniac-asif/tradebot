"""
API resilience primitives for Alpaca REST calls.
- TokenBucket: smooth out burst traffic (180 tokens, 3/s refill)
- ResponseCache: TTL-based per-key cache (quotes 5s, chains 30s, snapshots 10s)
- AlpacaCircuitBreaker: open after 3 consecutive failures, recover after 60s
- retry_with_backoff: decorator for 1s/2s/4s exponential retry (exits skip retry)
"""

import time
import threading
import functools
import logging
from typing import Any, Callable, Optional


# ---------------------------------------------------------------------------
# Token Bucket
# ---------------------------------------------------------------------------

class TokenBucket:
    """Thread-safe token bucket. Default: 180 tokens, refill 3/s."""

    def __init__(self, max_tokens: float = 180.0, refill_rate: float = 3.0):
        self._max = float(max_tokens)
        self._rate = float(refill_rate)
        self._tokens = float(max_tokens)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def acquire(self, n: float = 1.0) -> bool:
        """Consume n tokens. Returns True immediately if available, False if bucket is empty."""
        with self._lock:
            self._refill()
            if self._tokens >= n:
                self._tokens -= n
                return True
            return False

    def acquire_wait(self, n: float = 1.0) -> None:
        """Block until n tokens are available, then consume them."""
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= n:
                    self._tokens -= n
                    return
                wait_for = (n - self._tokens) / self._rate
            time.sleep(min(wait_for, 0.1))

    def tokens_available(self) -> float:
        with self._lock:
            self._refill()
            return self._tokens

    def stats(self) -> dict:
        return {
            "tokens_available": round(self.tokens_available(), 2),
            "max_tokens": self._max,
            "refill_rate": self._rate,
        }


# ---------------------------------------------------------------------------
# Response Cache
# ---------------------------------------------------------------------------

class ResponseCache:
    """
    TTL-based in-memory cache keyed by arbitrary string keys.

    Recommended TTLs:
      quotes    → 5s
      chains    → 30s
      snapshots → 10s
    """

    def __init__(self):
        self._store: dict[str, tuple[Any, float]] = {}  # key → (value, expires_at)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expires_at = entry
            if time.monotonic() > expires_at:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any, ttl: float) -> None:
        with self._lock:
            self._store[key] = (value, time.monotonic() + ttl)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> dict:
        with self._lock:
            now = time.monotonic()
            valid = sum(1 for _, exp in self._store.values() if exp > now)
            return {"cached_entries": valid, "total_entries": len(self._store)}


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

_CB_CLOSED = "closed"
_CB_OPEN = "open"
_CB_HALF_OPEN = "half_open"


class AlpacaCircuitBreaker:
    """
    Per-operation circuit breaker.
    States: closed → open (after failure_threshold failures) → half_open → closed.

    IMPORTANT: exit operations always pass through (is_exit=True bypasses the breaker).
    """

    def __init__(self, failure_threshold: int = 3, recovery_timeout: float = 60.0):
        self._threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._failures = 0
        self._state = _CB_CLOSED
        self._opened_at: Optional[float] = None
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            return self._get_state()

    def _get_state(self) -> str:
        """Must be called with lock held."""
        if self._state == _CB_OPEN:
            if time.monotonic() - (self._opened_at or 0) >= self._recovery_timeout:
                self._state = _CB_HALF_OPEN
        return self._state

    def allow_request(self, is_exit: bool = False) -> bool:
        """Returns True if the request should proceed."""
        if is_exit:
            return True
        with self._lock:
            st = self._get_state()
            if st == _CB_CLOSED:
                return True
            if st == _CB_HALF_OPEN:
                return True  # allow one probe
            return False  # open

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = _CB_CLOSED
            self._opened_at = None

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._failures >= self._threshold:
                if self._state != _CB_OPEN:
                    logging.warning(
                        "alpaca_circuit_breaker_opened: failures=%d", self._failures
                    )
                self._state = _CB_OPEN
                self._opened_at = time.monotonic()

    def stats(self) -> dict:
        with self._lock:
            st = self._get_state()
            age = None
            if self._opened_at is not None:
                age = round(time.monotonic() - self._opened_at, 1)
            return {
                "state": st,
                "failures": self._failures,
                "threshold": self._threshold,
                "opened_age_seconds": age,
            }


# ---------------------------------------------------------------------------
# retry_with_backoff decorator
# ---------------------------------------------------------------------------

def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    is_exit: bool = False,
    breaker: Optional["AlpacaCircuitBreaker"] = None,
):
    """
    Decorator: retry on exception with exponential back-off (1s, 2s, 4s).
    If is_exit=True, skip the circuit breaker check and don't retry (exits must not be delayed).
    """
    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            cb = breaker or _get_breaker()
            if not cb.allow_request(is_exit=is_exit):
                raise RuntimeError("alpaca_circuit_breaker_open")
            last_exc: Optional[Exception] = None
            retries = 0 if is_exit else max_retries
            for attempt in range(retries + 1):
                try:
                    result = fn(*args, **kwargs)
                    cb.record_success()
                    return result
                except Exception as exc:
                    last_exc = exc
                    cb.record_failure()
                    if is_exit or attempt >= retries:
                        raise
                    delay = base_delay * (2 ** attempt)
                    logging.warning(
                        "alpaca_retry: fn=%s attempt=%d/%d delay=%.1fs err=%s",
                        fn.__name__, attempt + 1, retries, delay, exc,
                    )
                    time.sleep(delay)
            raise last_exc  # unreachable, but satisfies type checkers
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Singletons (module-level, lazy-init)
# ---------------------------------------------------------------------------

_bucket: Optional[TokenBucket] = None
_cache: Optional[ResponseCache] = None
_breaker: Optional[AlpacaCircuitBreaker] = None
_singleton_lock = threading.Lock()


def _get_bucket() -> TokenBucket:
    global _bucket
    if _bucket is None:
        with _singleton_lock:
            if _bucket is None:
                _bucket = TokenBucket(max_tokens=180.0, refill_rate=3.0)
    return _bucket


def _get_cache() -> ResponseCache:
    global _cache
    if _cache is None:
        with _singleton_lock:
            if _cache is None:
                _cache = ResponseCache()
    return _cache


def _get_breaker() -> AlpacaCircuitBreaker:
    global _breaker
    if _breaker is None:
        with _singleton_lock:
            if _breaker is None:
                _breaker = AlpacaCircuitBreaker(failure_threshold=3, recovery_timeout=60.0)
    return _breaker


def get_bucket() -> TokenBucket:
    return _get_bucket()


def get_cache() -> ResponseCache:
    return _get_cache()


def get_breaker() -> AlpacaCircuitBreaker:
    return _get_breaker()


def resilience_stats() -> dict:
    """Combined stats dict — used by !ratelimit command."""
    return {
        "bucket": _get_bucket().stats(),
        "cache": _get_cache().stats(),
        "breaker": _get_breaker().stats(),
    }
