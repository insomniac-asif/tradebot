# Bot Rules — Hard Constraints

## Execution Rules
- Never open more than 1 position per sim at a time
- Never trade outside market hours (9:30 AM – 4:00 PM ET)
- SIM00 (live) only executes when circuit breaker allows
- All sims must respect hold_min_seconds before any exit
- Forced close at hold_max_seconds regardless of P&L

## Risk Rules
- No sim may risk more than 10% of its balance per trade
- 0DTE trades get theta burn acceleration exit
- IV crush detection tightens stops by 40%

## Data Rules
- Candle data must have minimum 20 bars before any signal can fire
- All option prices come from Alpaca snapshots with 1-second rate limit
- Missing data → skip, never hallucinate prices
