import json
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
LOG_DIR = "data/candidates"


def log_candidate(candidate) -> None:
    """Append one candidate to today's JSONL file."""
    os.makedirs(LOG_DIR, exist_ok=True)
    path = os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.jsonl")
    try:
        with open(path, "a") as f:
            f.write(json.dumps(candidate.to_dict()) + "\n")
    except Exception as e:
        logger.error("candidate_logger: %s", e)
