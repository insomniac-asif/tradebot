"""
ANSI formatting helpers for Discord embeds.

Usage:
    from interface.fmt import ab, A, lbl, pnl_col, conf_col, ...
    embed.add_field(name="PnL", value=ab(pnl_col(pnl)))

Notes:
    - ANSI colors render in Discord desktop/web inside ```ansi``` blocks.
    - Mobile shows plain text in a code block (still readable).
"""

_RST = "\u001b[0m"
_BLD = "\u001b[1m"
_GRN = "\u001b[32m"
_RED = "\u001b[31m"
_YLW = "\u001b[33m"
_BLU = "\u001b[34m"
_MGT = "\u001b[35m"
_CYN = "\u001b[36m"
_WHT = "\u001b[37m"
_GRY = "\u001b[30m"


def _color_code(name: str) -> str:
    return {
        "gray": _GRY,
        "red": _RED,
        "green": _GRN,
        "yellow": _YLW,
        "blue": _BLU,
        "magenta": _MGT,
        "cyan": _CYN,
        "white": _WHT,
    }.get(name, _WHT)


def A(text, color: str = "white", bold: bool = False) -> str:
    """Wrap text in ANSI color codes."""
    c = _color_code(color)
    b = _BLD if bold else ""
    return f"{b}{c}{text}{_RST}"


def ab(*lines) -> str:
    """Wrap lines in a Discord ```ansi``` block."""
    body = "\n".join(str(ln) for ln in lines)
    return f"```ansi\n{body}\n{_RST}```"


def lbl(text: str) -> str:
    """Cyan label for key: value pairs."""
    return A(f"{text}:", "cyan")


def pnl_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    color = "green" if num > 0 else "red" if num < 0 else "yellow"
    sign = "+" if num >= 0 else "-"
    return A(f"{sign}${abs(num):,.2f}", color, bold=True)


def signed_col(val, prefix: str = "$") -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    color = "green" if num > 0 else "red" if num < 0 else "yellow"
    sign = "+" if num >= 0 else "-"
    return A(f"{sign}{prefix}{abs(num):,.2f}", color)


def pct_col(val, good_when_high: bool = True, multiply: bool = True) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    pct = num * 100 if multiply else num
    if good_when_high:
        color = "green" if pct >= 55 else "yellow" if pct >= 45 else "red"
    else:
        color = "red" if pct >= 55 else "yellow" if pct >= 45 else "green"
    return A(f"{pct:.1f}%", color, bold=True)


def conf_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    if num >= 0.65:
        return A(f"{num:.2f} (HIGH)", "green", bold=True)
    if num >= 0.52:
        return A(f"{num:.2f} (MED)", "yellow", bold=True)
    return A(f"{num:.2f} (LOW)", "red", bold=True)


def dir_col(direction: str) -> str:
    d = (direction or "").upper()
    if d in {"BULLISH", "BULL", "CALL", "LONG"}:
        return A(d, "green", bold=True)
    if d in {"BEARISH", "BEAR", "PUT", "SHORT"}:
        return A(d, "red", bold=True)
    if d in {"RANGE", "NEUTRAL", "FLAT"}:
        return A(d, "yellow", bold=True)
    return A(d or "N/A", "gray")


def result_col(result: str) -> str:
    r = (result or "").upper()
    if r in {"WIN", "PROFIT"}:
        return A(r, "green", bold=True)
    if r in {"LOSS", "LOSE"}:
        return A(r, "red", bold=True)
    return A(r or "N/A", "yellow")


def regime_col(regime: str) -> str:
    r = (regime or "").upper()
    if r == "TREND":
        return A(r, "green", bold=True)
    if r == "RANGE":
        return A(r, "yellow", bold=True)
    if r == "VOLATILE":
        return A(r, "red", bold=True)
    if r in {"COMPRESSION", "QUIET"}:
        return A(r, "gray", bold=True)
    return A(r or "N/A", "gray")


def vol_col(vol: str) -> str:
    v = (vol or "").upper()
    if v == "HIGH":
        return A(v, "red", bold=True)
    if v == "NORMAL":
        return A(v, "green", bold=True)
    if v == "LOW":
        return A(v, "yellow", bold=True)
    if v in {"DEAD", "QUIET"}:
        return A(v, "gray", bold=True)
    return A(v or "N/A", "gray")


def delta_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    color = "green" if num >= 0 else "red"
    return A(f"{num:+.2f}", color, bold=True)


def drawdown_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    if num <= 0:
        return A("$0.00", "green")
    return A(f"-${abs(num):,.2f}", "red", bold=True)


def tier_col(tier: str) -> str:
    t = (tier or "").upper()
    if t == "HIGH":
        return A(t, "red", bold=True)
    if t == "MEDIUM":
        return A(t, "yellow", bold=True)
    if t == "LOW":
        return A(t, "gray", bold=True)
    return A(t or "N/A", "gray")


def exit_reason_col(reason: str) -> str:
    r = (reason or "").lower()
    if any(k in r for k in ["profit", "target", "win"]):
        return A(reason, "green", bold=True)
    if "breakeven" in r:
        return A(reason, "yellow", bold=True)
    if any(k in r for k in ["stop", "loss"]):
        return A(reason, "red", bold=True)
    if any(k in r for k in ["trailing", "timeout", "hold_max", "expiry"]):
        return A(reason, "yellow", bold=True)
    return A(reason or "unknown", "gray")


def ml_col(val) -> str:
    if val is None:
        return A("Warming Up", "gray")
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    if num >= 0.6:
        return A(f"{num:.2f}", "green", bold=True)
    if num >= 0.5:
        return A(f"{num:.2f}", "yellow", bold=True)
    return A(f"{num:.2f}", "red", bold=True)


def balance_col(val) -> str:
    try:
        num = float(val)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    return A(f"${num:,.2f}", "white", bold=True)


def wr_col(win_rate) -> str:
    try:
        num = float(win_rate)
    except (TypeError, ValueError):
        return A("N/A", "gray")
    pct = num * 100
    if pct >= 55:
        color = "green"
    elif pct >= 45:
        color = "yellow"
    else:
        color = "red"
    return A(f"{pct:.1f}%", color, bold=True)
