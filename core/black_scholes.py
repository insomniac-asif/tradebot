"""core/black_scholes.py
Lightweight Black-Scholes option pricing and Greeks.
No external dependencies beyond math (scipy.stats.norm available as fallback).
"""
import math

# Standard normal CDF approximation (Abramowitz & Stegun 26.2.17, max error 7.5e-8)
_A1 = 0.254829592
_A2 = -0.284496736
_A3 = 1.421413741
_A4 = -1.453152027
_A5 = 1.061405429
_P = 0.3275911


def _norm_cdf(x: float) -> float:
    sign = 1.0 if x >= 0 else -1.0
    x = abs(x)
    t = 1.0 / (1.0 + _P * x)
    y = 1.0 - (((((_A5 * t + _A4) * t + _A3) * t + _A2) * t + _A1) * t) * math.exp(-x * x / 2.0)
    return 0.5 * (1.0 + sign * y)


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def bs_price(
    S: float,       # underlying price
    K: float,       # strike
    T: float,       # time to expiry in years
    r: float,       # risk-free rate
    sigma: float,   # implied volatility
    option_type: str = "call",
) -> float:
    """Black-Scholes option price. Returns 0 if T <= 0."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        # At expiry: intrinsic value
        if option_type == "call":
            return max(0.0, S - K)
        return max(0.0, K - S)

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "call":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def bs_theta(
    S: float, K: float, T: float, r: float, sigma: float,
    option_type: str = "call",
) -> float:
    """Daily theta (price change per calendar day). Always negative for long options."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0

    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    common = -(S * _norm_pdf(d1) * sigma) / (2 * math.sqrt(T))

    if option_type == "call":
        theta_annual = common - r * K * math.exp(-r * T) * _norm_cdf(d2)
    else:
        theta_annual = common + r * K * math.exp(-r * T) * _norm_cdf(-d2)

    return theta_annual / 365.0  # per calendar day
