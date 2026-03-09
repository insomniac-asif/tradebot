import time

DEBUG_MODE = True
_last_debug_times = {}


def debug_log(event, **fields):
    if not DEBUG_MODE:
        return
    now = time.time()
    last_time = _last_debug_times.get(event)
    if last_time is not None and (now - last_time) < 60:
        return
    _last_debug_times[event] = now

    if not fields:
        print(f"[DEBUG] {event}")
    else:
        ordered = ", ".join(f"{k}={fields[k]}" for k in sorted(fields))
        print(f"[DEBUG] {event} | {ordered}")

    # Mirror to structured log (best-effort, never raises)
    try:
        from core.structured_logger import slog
        slog(event, level="debug", **fields)
    except Exception:
        pass
