# research/hypothesis_builder.py
#
# Structures strategy hypotheses as plain data — no AI calls, no network.

import json
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_VALID_STATUSES = frozenset({
    "proposed", "sim_generated", "testing",
    "validated", "rejected", "archived",
})


def build_hypothesis(
    source: str,
    claim: str,
    counter_hypothesis: str,
    features: list,
    timeframe: str = "intraday",
) -> dict:
    """
    Assemble a hypothesis dict.  Pure data — no I/O.
    id is unique to the second; caller should save quickly if uniqueness matters.
    """
    return {
        "id":                 f"HYP_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "source":             source,
        "claim":              claim,
        "counter_hypothesis": counter_hypothesis,
        "features":           features,
        "timeframe":          timeframe,
        "status":             "proposed",
        "created_at":         datetime.now().isoformat(),
        "sim_id":             None,
        "results":            None,
    }


def save_hypothesis(hyp: dict, path: str = "research/hypotheses/") -> str:
    """
    Save a hypothesis dict to {path}/{id}.json.
    Creates the directory if needed.  Returns the filepath on success, '' on failure.
    """
    try:
        os.makedirs(path, exist_ok=True)
        filepath = os.path.join(path, f"{hyp['id']}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(hyp, f, indent=2)
        return filepath
    except Exception as exc:
        logger.error("save_hypothesis failed: %s", exc)
        return ""


def load_hypotheses(path: str = "research/hypotheses/") -> list:
    """
    Load all .json files from path, sorted by created_at descending.
    Returns empty list if directory is missing or empty.
    """
    if not os.path.isdir(path):
        return []
    hyps = []
    for fname in os.listdir(path):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(path, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                hyps.append(json.load(f))
        except Exception as exc:
            logger.warning("load_hypotheses: skipping %s: %s", fpath, exc)
    hyps.sort(key=lambda h: h.get("created_at", ""), reverse=True)
    return hyps


def update_hypothesis_status(
    hyp_id: str,
    status: str,
    sim_id: Optional[str] = None,
    results: Optional[dict] = None,
    path: str = "research/hypotheses/",
) -> bool:
    """
    Update the status (and optionally sim_id / results) on a saved hypothesis.
    Rejects invalid status strings.  Returns True on success, False on failure.
    """
    if status not in _VALID_STATUSES:
        logger.warning("update_hypothesis_status: invalid status %r", status)
        return False
    filepath = os.path.join(path, f"{hyp_id}.json")
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            hyp = json.load(f)
        hyp["status"] = status
        if sim_id is not None:
            hyp["sim_id"] = sim_id
        if results is not None:
            hyp["results"] = results
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(hyp, f, indent=2)
        return True
    except Exception as exc:
        logger.error("update_hypothesis_status failed for %s: %s", hyp_id, exc)
        return False
