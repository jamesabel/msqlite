"""Lock errors after BEGIN (on a statement, or on COMMIT) are retried in place; the transaction is kept.

All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note.
"""

import sqlite3
import threading
import time

from msqlite import Database
from test_msqlite.paths import get_temp_dir

HOLD_S = 0.4


def _fresh_rollback_journal_db(name: str):
    db_path = get_temp_dir() / f"{name}.sqlite"
    for suffix in ("", "-wal", "-shm", "-journal"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)
    with Database(db_path, wal=False, persistent=False) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
            tx.execute("INSERT INTO t VALUES (1)")
    return db_path


def _hold(db_path, begin: str, released: threading.Event, holding: threading.Event):
    holder = sqlite3.connect(db_path, autocommit=True)
    holder.execute(begin)
    holder.execute("SELECT count(*) FROM t").fetchall()  # a DEFERRED holder needs a read to take its SHARED lock
    holding.set()
    released.wait()
    holder.execute("ROLLBACK")
    holder.close()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_statement_retry_in_read_transaction():
    """Rollback journal: a writer holding EXCLUSIVE makes the reader's first SELECT BUSY (BEGIN DEFERRED takes no lock)."""
    db_path = _fresh_rollback_journal_db("test_statement_retry_read")
    released, holding = threading.Event(), threading.Event()
    thread = threading.Thread(target=_hold, args=(db_path, "BEGIN EXCLUSIVE", released, holding))
    thread.start()
    assert holding.wait(5.0)
    threading.Timer(HOLD_S, released.set).start()
    retries_seen = []
    with Database(db_path, wal=False, busy_timeout_s=0.0, on_retry=lambda *args: retries_seen.append(args)) as db:
        with db.read() as tx:
            rows = tx.execute("SELECT count(*) FROM t").fetchall()
        assert rows == [(1,)]
        assert db.stats.retries > 0
        assert db.stats.max_wait_s is not None and db.stats.max_wait_s >= HOLD_S / 2
    thread.join()
    attempts = [attempt for attempt, _unused_elapsed, _unused_error in retries_seen]
    assert attempts == list(range(1, len(attempts) + 1))
    assert all(isinstance(error, sqlite3.OperationalError) and error.sqlite_errorcode == sqlite3.SQLITE_BUSY for _unused_attempt, _unused_elapsed, error in retries_seen)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_commit_retry_while_reader_holds_shared_lock():
    """Rollback journal: COMMIT needs EXCLUSIVE, which is BUSY while a reader holds SHARED; earlier statements are kept."""
    db_path = _fresh_rollback_journal_db("test_statement_retry_commit")
    released, holding = threading.Event(), threading.Event()
    thread = threading.Thread(target=_hold, args=(db_path, "BEGIN DEFERRED", released, holding))
    thread.start()
    assert holding.wait(5.0)
    threading.Timer(HOLD_S, released.set).start()
    with Database(db_path, wal=False, busy_timeout_s=0.0) as db:
        start = time.monotonic()
        with db.write() as tx:  # BEGIN IMMEDIATE succeeds: RESERVED does not conflict with SHARED
            tx.execute("INSERT INTO t VALUES (2)")
            tx.execute("INSERT INTO t VALUES (3)")
        elapsed = time.monotonic() - start
        assert elapsed >= HOLD_S / 2, f"{elapsed=}"
        assert db.stats.retries > 0
        with db.read() as tx:
            assert tx.execute("SELECT x FROM t ORDER BY x").fetchall() == [(1,), (2,), (3,)]
    thread.join()
