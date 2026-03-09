import argparse
import os
import yaml
from datetime import datetime

from core.data_service import get_market_dataframe
from simulation.sim_signals import _KNOWN_SIGNAL_MODES

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "simulation", "sim_config.yaml")



def _load_profiles() -> dict:
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def _safe_float(val):
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _validate_global_config(profiles: dict) -> list[str]:
    errors = []
    global_cfg = profiles.get("_global")
    if global_cfg is None:
        errors.append("_global_missing")
        return errors
    if not isinstance(global_cfg, dict):
        errors.append("_global_invalid_type")
        return errors
    if "cross_sim_guard_enabled" not in global_cfg:
        errors.append("_global_missing:cross_sim_guard_enabled")
    else:
        if not isinstance(global_cfg.get("cross_sim_guard_enabled"), bool):
            errors.append("_global_invalid:cross_sim_guard_enabled")
    if "max_directional_sims" not in global_cfg:
        errors.append("_global_missing:max_directional_sims")
    else:
        try:
            val = int(global_cfg.get("max_directional_sims"))
            if val < 1 or val > 20:
                errors.append("_global_out_of_range:max_directional_sims")
        except (TypeError, ValueError):
            errors.append("_global_invalid:max_directional_sims")
    return errors


def _validate_sim(sim_id: str, profile: dict, df=None) -> list[str]:
    errors = []
    required = [
        "signal_mode",
        "dte_min",
        "dte_max",
        "otm_pct",
        "hold_min_seconds",
        "hold_max_seconds",
        "max_spread_pct",
        "risk_per_trade_pct",
        "stop_loss_pct",
        "profit_target_pct",
        "cutoff_time_et",
    ]
    for key in required:
        if key not in profile:
            errors.append(f"missing:{key}")

    dte_min = _safe_float(profile.get("dte_min"))
    dte_max = _safe_float(profile.get("dte_max"))
    if dte_min is not None and dte_max is not None and dte_min > dte_max:
        errors.append("dte_window_invalid")

    hold_min = _safe_float(profile.get("hold_min_seconds"))
    hold_max = _safe_float(profile.get("hold_max_seconds"))
    if hold_min is not None and hold_max is not None and hold_min > hold_max:
        errors.append("hold_window_invalid")

    stop_loss = _safe_float(profile.get("stop_loss_pct"))
    if stop_loss is not None and not (0.01 <= stop_loss <= 2.0):
        errors.append("stop_loss_out_of_range")

    profit_target = _safe_float(profile.get("profit_target_pct"))
    if profit_target is not None and not (0.05 <= profit_target <= 5.0):
        errors.append("profit_target_out_of_range")

    max_spread = _safe_float(profile.get("max_spread_pct"))
    if max_spread is not None and not (0.01 <= max_spread <= 0.50):
        errors.append("max_spread_out_of_range")

    risk_pct = _safe_float(profile.get("risk_per_trade_pct"))
    if risk_pct is not None and not (0.001 <= risk_pct <= 0.05):
        errors.append("risk_pct_out_of_range")

    # Cutoff format
    cutoff = profile.get("cutoff_time_et")
    try:
        datetime.strptime(str(cutoff), "%H:%M")
    except Exception:
        errors.append("cutoff_format_invalid")

    # entry_start_time_et format (optional field)
    entry_start = profile.get("entry_start_time_et")
    if entry_start is not None:
        try:
            datetime.strptime(str(entry_start), "%H:%M")
        except Exception:
            errors.append("entry_start_time_format_invalid")

    # Feature gating: if enabled, require indicators.
    if profile.get("features_enabled"):
        if df is None:
            errors.append("features_enabled_no_df")
        else:
            zwin = _safe_float(profile.get("zscore_window", 30))
            min_bars = max(20, int(zwin) + 2 if zwin is not None else 32)
            if len(df) < min_bars:
                errors.append("features_insufficient_bars")
            for col in ("ema9", "ema20", "rsi", "atr", "vwap"):
                if col not in df.columns:
                    errors.append(f"features_missing:{col}")

    mode = str(profile.get("signal_mode", "")).upper()

    # Unknown signal mode detection
    if mode and mode not in _KNOWN_SIGNAL_MODES:
        errors.append(f"unknown_signal_mode:{mode}")

    if mode == "ORB_BREAKOUT":
        if not profile.get("features_enabled"):
            errors.append("orb_requires_features")
        orb_minutes = _safe_float(profile.get("orb_minutes"))
        if orb_minutes is None or not (5 <= orb_minutes <= 120):
            errors.append("orb_minutes_invalid")

    if mode == "EXTREME_EXTENSION_FADE":
        if not profile.get("features_enabled"):
            errors.append("extreme_extension_fade_requires_features")

    return errors


def validate_sims() -> int:
    errors, total_errors = collect_sim_validation()
    profiles = _load_profiles()
    global_errors = _validate_global_config(profiles)
    if global_errors:
        errors = global_errors + errors
        total_errors += len(global_errors)
    error_map = {}
    for err in errors:
        if ":" in err:
            sim_id, msg = err.split(":", 1)
            error_map[sim_id.strip()] = msg.strip()
        else:
            error_map[err.strip()] = ""

    if profiles:
        for sim_id in sorted(profiles):
            if str(sim_id).startswith("_"):
                continue
            if sim_id in error_map:
                if error_map[sim_id]:
                    print(f"{sim_id}: {error_map[sim_id]}")
                else:
                    print(f"{sim_id}: ERROR")
            else:
                print(f"{sim_id}: OK")
        if global_errors:
            for err in global_errors:
                print(f"_global: {err}")

    if not errors and total_errors == 0:
        print("SIM validation OK.")
        return 0
    if total_errors:
        print(f"Total issues: {total_errors}")
    return 2


def collect_sim_validation(df=None) -> tuple[list[str], int]:
    profiles = _load_profiles()
    if not profiles:
        return ["no_profiles_found"], 1
    if df is None:
        df = get_market_dataframe()
    errors = []
    errors.extend(_validate_global_config(profiles))
    total_errors = 0
    for sim_id in sorted(profiles):
        if str(sim_id).startswith("_"):
            continue
        profile = profiles[sim_id]
        errs = _validate_sim(sim_id, profile, df=df)
        if errs:
            total_errors += len(errs)
            errors.append(f"{sim_id}: {', '.join(errs)}")
    return errors, total_errors


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SIM config validator")
    parser.add_argument("--validate", action="store_true", help="Validate SIM profiles")
    args = parser.parse_args()
    if args.validate:
        raise SystemExit(validate_sims())
