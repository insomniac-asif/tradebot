# Strategy Registry

## Signal Modes and Their Sims

| Signal Mode | Sims | Family | Thesis |
|---|---|---|---|
| MEAN_REVERSION | SIM01, SIM04(disabled) | reversal | RSI + VWAP oversold/overbought fade |
| BREAKOUT | SIM02, SIM16 | breakout | 20-bar high/low break |
| TREND_PULLBACK | SIM00(live), SIM03, SIM05(disabled), SIM08, SIM11, SIM13, SIM14, SIM29 | trend | EMA9/20 pullback in established trend |
| SWING_TREND | SIM06, SIM07 | trend | EMA20 slope + price position, longer holds |
| OPPORTUNITY | SIM09 | adaptive | Evaluates multiple signals, picks best |
| ORB_BREAKOUT | SIM10 | breakout | Opening range breakout |
| VWAP_REVERSION | SIM15 | reversal | VWAP z-score extreme fade |
| ZSCORE_BOUNCE | SIM17 | reversal | Close z-score extreme |
| FAILED_BREAKOUT_REVERSAL | SIM18 | reversal | Breakout failure → reversal |
| VWAP_CONTINUATION | SIM19 | continuation | Price near VWAP, directional confirm |
| OPENING_DRIVE | SIM20 | breakout | Trade with gap/open move |
| AFTERNOON_BREAKOUT | SIM21 | breakout | Afternoon expansion ratio |
| TREND_RECLAIM | SIM22 | reclaim | EMA spread + reclaim pattern |
| EXTREME_EXTENSION_FADE | SIM23 | fade | VWAP z ≥ 2.5 + RSI extreme |
| FVG_4H | SIM24 | structure | 4-hour Fair Value Gap |
| FVG_5M | SIM25 | structure | 5-minute FVG scalp |
| LIQUIDITY_SWEEP | SIM26 | structure | Swing high/low sweep |
| FVG_SWEEP_COMBO | SIM27 | structure | FVG + sweep confluence |
| FLOW_DIVERGENCE | SIM28 | fade | Trend + flow disagree |
| MULTI_TF_CONFIRM | SIM30 | trend | 5m + 15m EMA alignment |
| VPOC_REVERSION | SIM31 | reversal | Volume POC fade, $0.50 buckets |
| GAP_FADE | SIM32 | fade | Overnight gap mean-reversion |
| OPENING_RANGE_RECLAIM | SIM33 | structure | Post-flush reclaim of opening range |
| VOL_COMPRESSION_BREAKOUT | SIM34 | volatility | ATR compression → expansion |
| VOL_SPIKE_FADE | SIM35 | volatility | Fade during vol spikes, stretched RSI |

## Disabled Sims
- SIM04: MEAN_REVERSION (redundant with SIM01/SIM12)
- SIM05: TREND_PULLBACK (redundant with SIM08/SIM14)
- SIM33 (previous): ORB_BREAKOUT clone of SIM10 (replaced with OPENING_RANGE_RECLAIM)
