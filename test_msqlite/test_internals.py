"""Unit tests for the pure building blocks (retry arithmetic, SQL helpers, error classification) and the compatibility module.

All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note.
"""

import sqlite3

import pytest

import msqlite
import msqlite.msqlite as compat
from msqlite import Database, MSQLiteMaxRetriesError, MSQLiteTimeoutError
from msqlite._errors import is_lock_error, is_missing_table_error, is_stale_snapshot_error
from msqlite._retry import RetryPolicy, WaitState
from msqlite._sql import create_index_sql, create_table_sql, normalize_pragma
from test_msqlite.paths import get_temp_dir


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_compat_module_exports_the_same_objects():
    assert compat.__all__ == msqlite.__all__
    for name in msqlite.__all__:
        assert getattr(compat, name) is getattr(msqlite, name)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lock_error_classification_without_a_result_code():
    """Errors raised by the Python layer carry no SQLite result code; the message is the fallback."""
    assert is_lock_error(sqlite3.OperationalError("database is locked"))
    assert not is_lock_error(sqlite3.OperationalError("no such table: t"))
    assert not is_stale_snapshot_error(sqlite3.OperationalError("database is locked"))
    assert is_missing_table_error(sqlite3.OperationalError("no such table: t"))


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_retry_policy_budget_and_caps():
    policy = RetryPolicy(timeout_s=1.0, retry_limit=2, retry_scale=0.1, retry_cap_s=0.5, busy_timeout_s=5.0)
    fresh = WaitState()
    policy.check_budget(fresh)  # nothing spent yet
    assert policy.remaining_s(fresh) == 1.0
    assert policy.busy_timeout_ms(fresh) == 1000  # busy timeout capped to the remaining deadline
    assert policy.busy_timeout_ms(WaitState(waited_s=0.75)) == 250
    assert policy.busy_timeout_ms(WaitState(waited_s=5.0)) == 0
    for _unused in range(100):
        assert 0.0 <= policy.backoff_s(WaitState(retries=1)) < 0.2  # first retry: [0, 2 * retry_scale)
        assert 0.0 <= policy.backoff_s(WaitState(retries=10)) <= 0.5  # capped by retry_cap_s
        assert policy.backoff_s(WaitState(waited_s=0.9, retries=10)) <= 0.1  # and by the deadline
    with pytest.raises(MSQLiteTimeoutError):
        policy.check_budget(WaitState(waited_s=1.0, retries=1))
    with pytest.raises(MSQLiteMaxRetriesError):
        policy.check_budget(WaitState(waited_s=0.1, retries=3))


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_retry_policy_without_a_deadline():
    policy = RetryPolicy(timeout_s=None, busy_timeout_s=2.5)
    state = WaitState(waited_s=1e9, retries=1_000_000)
    assert policy.remaining_s(state) is None
    assert policy.busy_timeout_ms(state) == 2500
    policy.check_budget(state)  # unbounded


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_retry_policy_validation():
    with pytest.raises(ValueError):
        RetryPolicy(retry_scale=0.0)
    with pytest.raises(ValueError):
        RetryPolicy(retry_cap_s=-1.0)
    with pytest.raises(ValueError):
        RetryPolicy(timeout_s=-0.1)
    with pytest.raises(ValueError):
        RetryPolicy(busy_timeout_s=-0.1)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_sql_helpers():
    assert normalize_pragma("  PRAGMA synchronous=FULL ") == "synchronous=FULL"
    assert normalize_pragma("cache_size=-2000") == "cache_size=-2000"
    with pytest.raises(ValueError):
        normalize_pragma("PRAGMA ")
    with pytest.raises(ValueError):
        normalize_pragma("synchronous=FULL; DROP TABLE t")
    assert create_table_sql("t", {"id PRIMARY KEY": int, "name": str}) == "CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY,name TEXT)"
    assert create_index_sql("t", "name") == "CREATE INDEX IF NOT EXISTS t_name_idx ON t(name)"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_database_exposes_its_configuration():
    db_path = get_temp_dir() / "test_internals_config.sqlite"
    with Database(db_path, wal=False, timeout_s=3.0, retry_limit=4, retry_scale=0.05, retry_cap_s=0.2, busy_timeout_s=0.5, persistent=False) as db:
        assert db.db_path == db_path
        assert db.wal is False
        assert db.persistent is False
        assert db.timeout_s == 3.0
        assert db.retry_limit == 4
        assert db.retry_scale == 0.05
        assert db.retry_cap_s == 0.2
        assert db.busy_timeout_s == 0.5
