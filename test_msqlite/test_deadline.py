"""All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note."""

import sqlite3
import time

import pytest

from msqlite import Database, MSQLite, MSQLiteMaxRetriesError, MSQLiteTimeoutError
from test_msqlite.paths import get_temp_dir


def _hold_write_lock(db_path):
    holder = sqlite3.connect(db_path, autocommit=True)
    holder.execute("BEGIN IMMEDIATE")
    return holder


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_database_deadline():
    db_path = get_temp_dir() / "test_deadline_database.sqlite"
    db_path.unlink(missing_ok=True)
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
    holder = _hold_write_lock(db_path)
    try:
        with Database(db_path, timeout_s=0.3) as db:  # busy_timeout_s (5 s) is capped to the remaining deadline
            start = time.monotonic()
            with pytest.raises(MSQLiteTimeoutError) as exc_info:
                with db.write():
                    pass
            elapsed = time.monotonic() - start
            assert 0.3 <= elapsed < 2.0, f"{elapsed=}"
            assert exc_info.value.elapsed_s >= 0.3
            assert exc_info.value.attempts >= 1
            assert not isinstance(exc_info.value, MSQLiteMaxRetriesError)
            assert db.stats.max_wait_s is not None and db.stats.max_wait_s >= 0.3
    finally:
        holder.execute("ROLLBACK")
        holder.close()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_msqlite_deadline():
    db_path = get_temp_dir() / "test_deadline_msqlite.sqlite"
    db_path.unlink(missing_ok=True)
    schema = {"x": int}
    with MSQLite(db_path, "t", schema) as db:
        db.execute("INSERT INTO t VALUES (1)")
    holder = _hold_write_lock(db_path)
    try:
        start = time.monotonic()
        with pytest.raises(MSQLiteTimeoutError):
            with MSQLite(db_path, "t", schema, timeout_s=0.2):
                pass
        assert 0.2 <= time.monotonic() - start < 2.0
    finally:
        holder.execute("ROLLBACK")
        holder.close()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_error_hierarchy():
    assert issubclass(MSQLiteMaxRetriesError, MSQLiteTimeoutError)
    assert issubclass(MSQLiteTimeoutError, sqlite3.OperationalError)
    error = MSQLiteMaxRetriesError(1.5, 3, 2)
    assert error.elapsed_s == 1.5 and error.attempts == 3 and error.retry_limit == 2
    assert "2" in str(error)
