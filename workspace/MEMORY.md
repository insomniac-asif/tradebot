# Long-Term Memory

## Discoveries
- (populated as sims produce data)

## Decisions
- SIM04/SIM05 disabled as low-differentiation clutter
- SIM33 ORB clone disabled and reclaimed for OPENING_RANGE_RECLAIM
- SIM29 session_filter: POWER is config-only — engine does not consume it yet
- MULTI_TF_CONFIRM cross-day aggregation is intentional, not a bug

## Known Issues
- session_filter field in config is not consumed by sim_engine — informational only
- 150-bar minimum on MULTI_TF_CONFIRM is not a problem due to CSV historical backfill
