"""All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note."""

import sqlite3
import threading
import time

import pytest
from beartype.roar import BeartypeCallHintParamViolation

from msqlite import Database, MSQLiteMaxRetriesError, MSQLiteTimeoutError, Stats
from test_msqlite.paths import get_temp_dir


def _fresh_db_path(name: str):
    db_path = get_temp_dir() / f"{name}.sqlite"
    for suffix in ("", "-wal", "-shm", "-journal"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)
    return db_path


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_write_and_read():
    db_path = _fresh_db_path("test_database_write_and_read")
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE things(id INTEGER PRIMARY KEY, name TEXT)")
            tx.execute("INSERT INTO things(name) VALUES (?)", ("plate",))
            assert tx.lastrowid == 1
            assert tx.rowcount == 1
            tx.executemany("INSERT INTO things(name) VALUES (?)", [("chair",), ("table",)])
            assert tx.rowcount == 2
            tx.executescript("INSERT INTO things(name) VALUES ('lamp'); INSERT INTO things(name) VALUES ('rug');")
        with db.read() as tx:
            rows = tx.execute("SELECT name FROM things ORDER BY id").fetchall()
            assert rows == [("plate",), ("chair",), ("table",), ("lamp",), ("rug",)]
            tx.execute("SELECT count(*) FROM things")
            assert tx.fetchone() == (5,)
            tx.execute("SELECT name FROM things WHERE id > 3 ORDER BY id")
            assert tx.fetchall() == [("lamp",), ("rug",)]
        assert isinstance(db.stats, Stats)
        assert db.stats.executions == 7
        assert db.stats.retries == 0
        assert db.stats.max_execution_s is not None
        assert db.stats.max_wait_s is None


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_wal_is_enabled_by_default():
    db_path = _fresh_db_path("test_database_wal_default")
    with Database(db_path) as db:
        with db.read() as tx:
            assert tx.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    with Database(db_path, wal=False, persistent=False) as db:
        with db.read() as tx:
            assert tx.execute("PRAGMA journal_mode").fetchone() == ("wal",)  # wal=False leaves the file alone


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_row_factory():
    db_path = _fresh_db_path("test_database_row_factory")
    with Database(db_path, row_factory=sqlite3.Row) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(a INTEGER, b TEXT)")
            tx.execute("INSERT INTO t VALUES (1, 'x')")
        with db.read() as tx:
            row = tx.execute("SELECT a, b FROM t").fetchone()
            assert row["a"] == 1 and row["b"] == "x"
            tx.row_factory = None  # per-transaction override
            assert tx.execute("SELECT a, b FROM t").fetchone() == (1, "x")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_persistent_connection_is_reused_per_thread():
    db_path = _fresh_db_path("test_database_persistent")
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
            first = tx.connection
        with db.read() as tx:
            assert tx.connection is first
        seen_in_thread = []

        def worker():
            with db.read() as tx:
                seen_in_thread.append(tx.connection)

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join()
        assert seen_in_thread[0] is not first
    # after close() a new connection is opened
    with db.read() as tx:
        assert tx.connection is not first
    db.close()
    db.close()  # idempotent


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_non_persistent_opens_a_connection_per_transaction():
    db_path = _fresh_db_path("test_database_non_persistent")
    with Database(db_path, persistent=False) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
            first = tx.connection
        with pytest.raises(sqlite3.ProgrammingError):
            first.execute("SELECT 1")  # closed at the end of the transaction
        with db.read() as tx:
            assert tx.connection is not first


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_nested_transaction_on_one_thread_raises():
    db_path = _fresh_db_path("test_database_nested")
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
            with pytest.raises(RuntimeError):
                with db.read():
                    pass
        # the outer transaction is still usable and committed
        with db.read() as tx:
            assert tx.execute("SELECT count(*) FROM t").fetchone() == (0,)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_rollback_on_exception():
    db_path = _fresh_db_path("test_database_rollback")
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
        with pytest.raises(ZeroDivisionError):
            with db.write() as tx:
                tx.execute("INSERT INTO t VALUES (1)")
                1 / 0
        with db.read() as tx:
            assert tx.execute("SELECT count(*) FROM t").fetchone() == (0,)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_transaction_misuse():
    db_path = _fresh_db_path("test_database_misuse")
    with Database(db_path) as db:
        tx = db.read()
        with pytest.raises(RuntimeError):
            tx.execute("SELECT 1")  # not entered
        with pytest.raises(RuntimeError):
            tx.connection
        with tx:
            with pytest.raises(RuntimeError):
                tx.fetchone()  # nothing executed yet
            with pytest.raises(RuntimeError):
                with tx:  # re-entering an active transaction
                    pass
        with pytest.raises((ValueError, BeartypeCallHintParamViolation)):  # beartype (tests) rejects the literal before the ValueError
            db.write(mode="deferred")  # type: ignore[arg-type]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_invalid_arguments():
    db_path = _fresh_db_path("test_database_invalid_arguments")
    with pytest.raises(ValueError):
        Database(db_path, retry_scale=0.0)
    with pytest.raises(ValueError):
        Database(db_path, retry_cap_s=0.0)
    with pytest.raises(ValueError):
        Database(db_path, timeout_s=-1.0)
    with pytest.raises(ValueError):
        Database(db_path, busy_timeout_s=-1.0)
    with pytest.raises(ValueError):
        Database(db_path, pragmas=("synchronous=FULL; DROP TABLE t",))


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pragmas_and_on_open():
    db_path = _fresh_db_path("test_database_pragmas")
    opened = []
    with Database(db_path, pragmas=("synchronous=FULL", "PRAGMA cache_size=-2000"), on_open=opened.append) as db:
        with db.read() as tx:
            assert tx.execute("PRAGMA synchronous").fetchone() == (2,)
            assert tx.execute("PRAGMA cache_size").fetchone() == (-2000,)
        with db.read():
            pass
        assert len(opened) == 1  # one persistent connection for this thread
        assert isinstance(opened[0], sqlite3.Connection)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_write_inside_read_transaction_is_not_retried():
    """A read transaction that tries to write on a stale snapshot gets SQLITE_BUSY_SNAPSHOT; retrying can never help."""
    db_path = _fresh_db_path("test_database_stale_snapshot")
    with Database(db_path) as db, Database(db_path) as other:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
        with db.read() as tx:
            tx.execute("SELECT count(*) FROM t").fetchall()
            with other.write() as otx:
                otx.execute("INSERT INTO t VALUES (1)")
            start = time.monotonic()
            with pytest.raises(sqlite3.OperationalError) as exc_info:
                tx.execute("INSERT INTO t VALUES (2)")
            assert exc_info.value.sqlite_errorname == "SQLITE_BUSY_SNAPSHOT"
            assert time.monotonic() - start < 5.0  # not retried until timeout_s (30 s)
        assert db.stats.retries == 0


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_retry_limit_and_error_hierarchy():
    db_path = _fresh_db_path("test_database_retry_limit")
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
    holder = sqlite3.connect(db_path, autocommit=True)
    holder.execute("BEGIN IMMEDIATE")
    retries_seen = []
    try:
        with Database(db_path, retry_limit=2, busy_timeout_s=0.0, on_retry=lambda attempt, elapsed_s, error: retries_seen.append((attempt, elapsed_s, error))) as db:
            with pytest.raises(MSQLiteMaxRetriesError) as exc_info:
                with db.write():
                    pass
            assert isinstance(exc_info.value, MSQLiteTimeoutError)
            assert isinstance(exc_info.value, sqlite3.OperationalError)
            assert exc_info.value.retry_limit == 2
            assert exc_info.value.attempts == 3  # the first attempt plus retry_limit retries, all failed
            assert db.stats.retries == 3
            assert db.stats.max_wait_s is not None
            assert [attempt for attempt, _unused_elapsed, _unused_error in retries_seen] == [1, 2]  # called before each backoff sleep, not on the final failure
            assert all(isinstance(error, sqlite3.OperationalError) for _unused_attempt, _unused_elapsed, error in retries_seen)
    finally:
        holder.execute("ROLLBACK")
        holder.close()
