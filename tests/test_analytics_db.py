"""Tests for core/analytics_db.py — SQLite analytics store."""
import os
import tempfile
import pytest
from unittest.mock import patch

import pandas as pd


@pytest.fixture
def temp_db(tmp_path):
    """Patch DB_PATH to use a temp directory."""
    db_path = str(tmp_path / "test_analytics.db")
    with patch("core.analytics_db.DB_PATH", db_path):
        from core.analytics_db import ensure_schema
        ensure_schema()
        yield db_path


class TestSchema:
    def test_ensure_schema_creates_tables(self, temp_db):
        from core.analytics_db import get_conn
        conn = get_conn()
        try:
            tables = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            table_names = {r[0] for r in tables}
            expected = {
                "predictions", "blocked_signals", "conviction_expectancy",
                "signal_log", "contract_selection_log", "execution_quality_log",
                "trade_features",
            }
            assert expected.issubset(table_names)
        finally:
            conn.close()

    def test_ensure_schema_idempotent(self, temp_db):
        from core.analytics_db import ensure_schema
        ensure_schema()  # second call should not error
        ensure_schema()  # third call should not error


class TestInsert:
    def test_insert_and_read(self, temp_db):
        from core.analytics_db import insert, read_df
        insert("predictions", {
            "time": "2026-03-12T10:00:00",
            "symbol": "SPY",
            "timeframe": "60",
            "direction": "bullish",
            "confidence": 0.75,
            "high": 571.0,
            "low": 569.0,
            "checked": 0,
        })
        df = read_df("SELECT * FROM predictions")
        assert len(df) == 1
        assert df.iloc[0]["symbol"] == "SPY"
        assert df.iloc[0]["confidence"] == 0.75

    def test_insert_many(self, temp_db):
        from core.analytics_db import insert_many, row_count
        rows = [
            {"timestamp": f"2026-03-12T10:{i:02d}:00", "outcome": "blocked"}
            for i in range(10)
        ]
        insert_many("signal_log", rows)
        assert row_count("signal_log") == 10


class TestUpdate:
    def test_update_single_row(self, temp_db):
        from core.analytics_db import insert, update, read_df
        insert("predictions", {
            "time": "2026-03-12T10:00:00",
            "symbol": "SPY",
            "checked": 0,
            "correct": 0,
        })
        df = read_df("SELECT id FROM predictions")
        row_id = int(df.iloc[0]["id"])

        update("predictions", {"checked": 1, "correct": 1, "actual": "bullish"}, "id = ?", (row_id,))

        df2 = read_df("SELECT * FROM predictions WHERE id = ?", [row_id])
        assert df2.iloc[0]["checked"] == 1
        assert df2.iloc[0]["correct"] == 1
        assert df2.iloc[0]["actual"] == "bullish"


class TestDeleteAll:
    def test_delete_all(self, temp_db):
        from core.analytics_db import insert, delete_all, row_count
        insert("trade_features", {"won": 1, "predicted_won": 1})
        insert("trade_features", {"won": 0, "predicted_won": 1})
        assert row_count("trade_features") == 2
        delete_all("trade_features")
        assert row_count("trade_features") == 0


class TestScalar:
    def test_scalar_count(self, temp_db):
        from core.analytics_db import insert, scalar
        insert("predictions", {"time": "2026-03-12T10:00:00", "checked": 1})
        insert("predictions", {"time": "2026-03-12T10:10:00", "checked": 0})
        result = scalar("SELECT COUNT(*) FROM predictions WHERE checked = 1")
        assert result == 1

    def test_scalar_empty_table(self, temp_db):
        from core.analytics_db import scalar
        result = scalar("SELECT MAX(time) FROM predictions")
        assert result is None


class TestLastWriteTime:
    def test_last_write_time(self, temp_db):
        from core.analytics_db import insert, last_write_time
        insert("predictions", {"time": "2026-03-12T09:00:00"})
        insert("predictions", {"time": "2026-03-12T10:00:00"})
        insert("predictions", {"time": "2026-03-12T08:00:00"})
        result = last_write_time("predictions", "time")
        assert result == "2026-03-12T10:00:00"


class TestTransaction:
    def test_transaction_commits_on_success(self, temp_db):
        from core.analytics_db import transaction, row_count
        with transaction() as conn:
            conn.execute("INSERT INTO trade_features (won) VALUES (1)")
            conn.execute("INSERT INTO trade_features (won) VALUES (0)")
        assert row_count("trade_features") == 2

    def test_transaction_rolls_back_on_error(self, temp_db):
        from core.analytics_db import transaction, row_count, insert
        insert("trade_features", {"won": 1})  # pre-existing
        try:
            with transaction() as conn:
                conn.execute("INSERT INTO trade_features (won) VALUES (0)")
                raise ValueError("test error")
        except ValueError:
            pass
        assert row_count("trade_features") == 1  # rollback reverted the insert


class TestSafeConversions:
    def test_safe_float(self):
        from core.analytics_db import _safe_float
        assert _safe_float(3.14) == 3.14
        assert _safe_float("2.5") == 2.5
        assert _safe_float(None) is None
        assert _safe_float("") is None
        assert _safe_float("abc") is None

    def test_safe_int(self):
        from core.analytics_db import _safe_int
        assert _safe_int(5) == 5
        assert _safe_int("3") == 3
        assert _safe_int("True") == 1
        assert _safe_int("False") == 0
        assert _safe_int(None) is None
        assert _safe_int("") is None

    def test_safe_str(self):
        from core.analytics_db import _safe_str
        assert _safe_str("hello") == "hello"
        assert _safe_str(None) is None
        assert _safe_str("") is None
        assert _safe_str(float("nan")) is None
