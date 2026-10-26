"""Error paths and misuse that the main tests do not reach: argument validation, calls outside a context, commit failure.

All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note.
"""

import logging
import sqlite3
import threading
import time

import pytest

from msqlite import Database, MSQLite, MSQLiteTimeoutError
from test_msqlite.paths import get_temp_dir

HOLD_S = 0.5


def _fresh_db_path(name: str):
    db_path = get_temp_dir() / f"{name}.sqlite"
    for suffix in ("", "-wal", "-shm", "-journal"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)
    return db_path


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_invalid_identifiers_are_rejected():
    db_path = _fresh_db_path("test_edge_invalid_identifiers")
    with pytest.raises(ValueError):
        MSQLite(db_path, "bad name", {"id": int})
    with pytest.raises(ValueError):
        MSQLite(db_path, "t", {"id": int}, indexes=["id; DROP TABLE t"])


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_schema_errors_surface_on_table_creation():
    db_path = _fresh_db_path("test_edge_schema_errors")
    with pytest.raises(TypeError):  # a value, not a type
        with MSQLite(db_path, "t", {"id": 42}) as db:
            db.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(ValueError):  # empty column spec
        with MSQLite(db_path, "t", {"": int}) as db:
            db.execute("INSERT INTO t VALUES (1)")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_on_open_failure_propagates_and_leaves_the_database_usable():
    db_path = _fresh_db_path("test_edge_on_open_failure")

    def fail(conn: sqlite3.Connection) -> None:
        raise RuntimeError("on_open failed")

    with Database(db_path, on_open=fail) as db:
        with pytest.raises(RuntimeError, match="on_open failed"):
            with db.write():
                pass
    with Database(db_path) as db:  # the failed connection was closed, nothing lingers
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_transaction_exit_without_enter_only_warns(caplog):
    db_path = _fresh_db_path("test_edge_tx_exit_without_enter")
    with Database(db_path) as db:
        tx = db.write()
        with caplog.at_level(logging.WARNING, logger="msqlite"):
            tx.__exit__(None, None, None)
    assert any("without an active connection" in record.message for record in caplog.records)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_msqlite_properties_and_connection_lifetime():
    db_path = _fresh_db_path("test_edge_msqlite_properties")
    db = MSQLite(db_path, "t", {"id": int}, retry_scale=0.02, retry_limit=7)
    assert db.db_path == db_path
    assert db.retry_scale == 0.02
    assert db.retry_limit == 7
    assert db.conn is None
    with db:
        assert isinstance(db.conn, sqlite3.Connection)
        db.execute("INSERT INTO t VALUES (1)")
    assert db.conn is None
    assert db.stats.executions >= 1


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_msqlite_misuse_outside_or_inside_a_context():
    db_path = _fresh_db_path("test_edge_msqlite_misuse")
    db = MSQLite(db_path, "t", {"id": int})
    with pytest.raises(RuntimeError):
        db.execute("SELECT 1")  # not entered
    with pytest.raises(RuntimeError):
        db.create_table()  # not entered
    with db:
        with pytest.raises(RuntimeError):
            with db:  # re-entering an active context
                pass


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_msqlite_exit_without_enter_only_warns(caplog):
    db_path = _fresh_db_path("test_edge_msqlite_exit_without_enter")
    db = MSQLite(db_path, "t", {"id": int})
    with caplog.at_level(logging.WARNING, logger="msqlite"):
        db.__exit__(None, None, None)
    assert any("__exit__" in record.message for record in caplog.records)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_msqlite_passes_other_operational_errors_through():
    db_path = _fresh_db_path("test_edge_msqlite_other_errors")
    with pytest.raises(sqlite3.OperationalError, match="syntax error"):
        with MSQLite(db_path, "t", {"id": int}) as db:
            db.execute("SELEC 1")


def _hold_shared_lock(db_path, released: threading.Event, holding: threading.Event):
    holder = sqlite3.connect(db_path, autocommit=True)
    holder.execute("BEGIN DEFERRED")
    holder.execute("SELECT count(*) FROM t").fetchall()  # takes the SHARED lock
    holding.set()
    released.wait()
    holder.execute("ROLLBACK")
    holder.close()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_commit_timeout_rolls_back_and_frees_the_connection():
    """Rollback journal: COMMIT stays BUSY while a reader holds SHARED, so the deadline fires on COMMIT and the write is rolled back."""
    db_path = _fresh_db_path("test_edge_commit_timeout")
    with Database(db_path, wal=False, persistent=False) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
            tx.execute("INSERT INTO t VALUES (1)")
    released, holding = threading.Event(), threading.Event()
    thread = threading.Thread(target=_hold_shared_lock, args=(db_path, released, holding))
    thread.start()
    assert holding.wait(5.0)
    try:
        with Database(db_path, wal=False, busy_timeout_s=0.0, timeout_s=HOLD_S / 4) as db:
            start = time.monotonic()
            with pytest.raises(MSQLiteTimeoutError):
                with db.write() as tx:
                    tx.execute("INSERT INTO t VALUES (2)")
            assert time.monotonic() - start < HOLD_S
            released.set()
            thread.join()
            with db.write() as tx:  # the persistent connection is not left inside the failed transaction
                assert tx.execute("SELECT x FROM t ORDER BY x").fetchall() == [(1,)]
    finally:
        released.set()
        thread.join()
