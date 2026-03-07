# core/account_repository.py
import os
import json
import shutil
import logging
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

ACCOUNT_FILE = os.path.join(DATA_DIR, "account.json")
CAREER_FILE = os.path.join(DATA_DIR, "career_stats.json")


def _backup_paths():
    return (
        f"{ACCOUNT_FILE}.bak",
        f"{ACCOUNT_FILE}.bak1",
        f"{ACCOUNT_FILE}.bak2",
        f"{ACCOUNT_FILE}.bak3",
    )


def _rotate_account_backups():
    bak, bak1, bak2, bak3 = _backup_paths()

    if os.path.exists(bak3):
        os.remove(bak3)
    if os.path.exists(bak2):
        os.replace(bak2, bak3)
    if os.path.exists(bak1):
        os.replace(bak1, bak2)

    if os.path.exists(ACCOUNT_FILE):
        shutil.copy2(ACCOUNT_FILE, bak)
        os.replace(bak, bak1)


def _load_newest_valid_account_backup():
    _, bak1, bak2, bak3 = _backup_paths()
    for path in (bak1, bak2, bak3):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
    return None


def load_account():
    try:
        with open(ACCOUNT_FILE, "r") as f:
            acc = json.load(f)
    except json.JSONDecodeError as e:
        recovered = _load_newest_valid_account_backup()
        if recovered is None:
            raise RuntimeError("account_primary_corrupt_no_valid_backup") from e
        logging.error("account_recovered_from_backup")
        acc = recovered

    today = date.today().isoformat()

    if acc.get("last_trade_day") != today:
        acc["daily_loss"] = 0
        acc["last_trade_day"] = today
        save_account(acc)

    return acc


def save_account(acc):
    tmp_path = f"{ACCOUNT_FILE}.tmp"

    try:
        # Inline test idea: interrupt process before os.replace to verify primary file remains intact.
        with open(tmp_path, "w") as f:
            json.dump(acc, f, indent=4)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, ACCOUNT_FILE)
        dir_fd = os.open(os.path.dirname(ACCOUNT_FILE), os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        # Inline test idea: corrupt primary account.json and ensure load_account restores from bak1/bak2/bak3.
        _rotate_account_backups()
        # Inline test idea: after repeated saves, verify only bak1..bak3 remain and oldest rolls off.
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def load_career():
    with open(CAREER_FILE, "r") as f:
        return json.load(f)


def save_career(data):
    with open(CAREER_FILE, "w") as f:
        json.dump(data, f, indent=4)
