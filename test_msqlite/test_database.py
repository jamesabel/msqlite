"""All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note."""

import gc
import os
import sqlite3
import threading
import time

import pytest
from beartype.roar import BeartypeCallHintParamViolation

from msqlite import Database, MSQLiteMaxRetriesError, MSQLiteTimeoutError, Stats
from msqlite._connections import close_dead_thread_connection
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


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_persistent_connection_is_closed_when_its_thread_ends():
    """A finished thread must not leave its persistent connection open until close()."""
    db_path = _fresh_db_path("test_database_dead_thread")
    opened: list[sqlite3.Connection] = []
    with Database(db_path, on_open=opened.append) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x INTEGER)")

        def worker() -> None:
            with db.write() as tx:
                tx.execute("INSERT INTO t VALUES (1)")

        threads = [threading.Thread(target=worker) for _unused in range(50)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        gc.collect()  # CPython frees the slots as each thread exits; this covers interpreters that defer it

        assert len(opened) == 51  # the main thread's connection plus one per worker
        assert len(db._connections._slots) == 1  # only the main thread's slot survives
        for conn in opened[1:]:  # the workers' connections are closed ...
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")
        opened[0].execute("SELECT 1")  # ... and the main thread's is still usable
        with db.read() as tx:
            assert tx.execute("SELECT count(*) FROM t").fetchone() == (50,)
    with pytest.raises(sqlite3.ProgrammingError):
        opened[0].execute("SELECT 1")  # close() shut the surviving connection


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_close_after_threads_ended_is_harmless():
    """close() after the finalizers have already closed the workers' connections must not raise."""
    db_path = _fresh_db_path("test_database_close_after_threads")
    db = Database(db_path)
    with db.write() as tx:
        tx.execute("CREATE TABLE t(x INTEGER)")

    def worker() -> None:
        with db.read():
            pass

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    gc.collect()
    db.close()
    db.close()  # idempotent


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dead_thread_finalizer_leaves_a_forked_parents_connection_alone():
    """The finalizer closes only connections opened in this process; one inherited across a fork is the parent's."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    close_dead_thread_connection(conn, os.getpid() + 1)  # looks like the parent's connection in a forked child
    conn.execute("SELECT 1")  # still open
    close_dead_thread_connection(conn, os.getpid())
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")
    close_dead_thread_connection(conn, os.getpid())  # closing twice (e.g. after Database.close()) is a no-op


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_close_leaves_another_threads_transaction_running():
    """close() must not close a connection under a running transaction; it is closed when the transaction ends."""
    db_path = _fresh_db_path("test_database_close_during_transaction")
    db = Database(db_path)
    with db.write() as tx:
        tx.execute("CREATE TABLE t(x INTEGER)")
    in_transaction = threading.Event()
    closed = threading.Event()
    connections: list[sqlite3.Connection] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            with db.write() as tx:
                connections.append(tx.connection)
                tx.execute("INSERT INTO t VALUES (1)")
                in_transaction.set()
                assert closed.wait(10.0)
                tx.execute("INSERT INTO t VALUES (2)")  # the connection is still open after close()
            with pytest.raises(sqlite3.ProgrammingError):
                connections[0].execute("SELECT 1")  # closed as the transaction ended
            with db.read() as tx:  # the next transaction opens a new connection
                connections.append(tx.connection)
                assert tx.execute("SELECT count(*) FROM t").fetchone() == (2,)
        except (AssertionError, sqlite3.Error, RuntimeError) as e:
            errors.append(e)

    thread = threading.Thread(target=worker)
    thread.start()
    assert in_transaction.wait(10.0)
    db.close()
    closed.set()
    thread.join()
    assert errors == []
    assert len(connections) == 2
    assert connections[1] is not connections[0]
    db.close()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_close_inside_own_transaction_is_deferred():
    """close() from inside a transaction on the same thread lets that transaction finish and commit."""
    db_path = _fresh_db_path("test_database_close_inside_transaction")
    db = Database(db_path)
    with db.write() as tx:
        tx.execute("CREATE TABLE t(x INTEGER)")
        conn = tx.connection
        db.close()
        tx.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute("SELECT 1")
    with db.read() as tx:
        assert tx.execute("SELECT count(*) FROM t").fetchone() == (1,)
    db.close()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_close_while_threads_transact():
    """Repeated close() while other threads write: no crash, no error, no lost write, and the file ends up unlocked."""
    db_path = _fresh_db_path("test_database_close_race")
    db = Database(db_path)
    with db.write() as tx:
        tx.execute("CREATE TABLE t(x INTEGER)")
    stop = threading.Event()
    written = []
    errors: list[BaseException] = []

    def worker() -> None:
        count = 0
        try:
            while not stop.is_set():
                with db.write() as tx:
                    tx.execute("INSERT INTO t VALUES (1)")
                count += 1
        except (sqlite3.Error, RuntimeError) as e:
            errors.append(e)
        written.append(count)

    threads = [threading.Thread(target=worker) for _unused in range(8)]
    for thread in threads:
        thread.start()
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
        db.close()
        time.sleep(0.001)
    stop.set()
    for thread in threads:
        thread.join()
    db.close()
    assert errors == []
    with Database(db_path, timeout_s=5.0) as fresh, fresh.write("exclusive") as tx:  # nothing still holds a lock
        assert tx.execute("SELECT count(*) FROM t").fetchone() == (sum(written),)
    assert sum(written) > 0


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_failed_begin_releases_the_connection():
    """A BEGIN that fails with something other than a lock error must not leave the thread's connection marked in use."""
    db_path = _fresh_db_path("test_database_failed_begin")
    with Database(db_path, on_open=lambda conn: conn.execute("BEGIN")) as db:  # every BEGIN from msqlite now fails
        for _unused in range(2):  # the second attempt fails the same way, not as a nested transaction
            with pytest.raises(sqlite3.OperationalError, match="within a transaction"):
                with db.write():
                    pass


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_connection_from_another_process_is_neither_reused_nor_closed(monkeypatch):
    """A connection opened under another pid (as after a fork) is the parent's: not reused, and not closed by close()."""
    real_pid = os.getpid()
    db_path = _fresh_db_path("test_database_other_pid")
    opened: list[sqlite3.Connection] = []
    db = Database(db_path, on_open=opened.append)
    try:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x INTEGER)")
            parents = tx.connection

        monkeypatch.setattr(os, "getpid", lambda: real_pid + 1)  # from here on this looks like a forked child
        with db.write() as tx:
            assert tx.connection is not parents  # the child opened its own
            childs = tx.connection
            tx.execute("INSERT INTO t VALUES (1)")
        assert len(db._connections._slots) == 1  # the inherited slot was dropped, the child's is registered
        parents.execute("SELECT 1")  # dropping it did not close it
        db.close()
        with pytest.raises(sqlite3.ProgrammingError):
            childs.execute("SELECT 1")  # close() shut the child's own connection

        monkeypatch.setattr(os, "getpid", lambda: real_pid)  # back in the "parent": open and register a connection
        with db.read() as tx:
            parents = tx.connection
        monkeypatch.setattr(os, "getpid", lambda: real_pid + 1)
        db.close()  # in the "child", with only the inherited connection registered
        assert parents.execute("SELECT count(*) FROM t").fetchone() == (1,)  # close() left it alone
    finally:
        monkeypatch.undo()
        for conn in opened:
            conn.close()  # the "parent's" connections were deliberately left open above


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.skipif(not hasattr(os, "fork"), reason="os.fork is not available on this platform")
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")  # pytest plugins may have left threads running
def test_forked_child_opens_its_own_connection():
    """A real fork: the child must not use or close the parent's connection, and both keep working on the file."""
    db_path = _fresh_db_path("test_database_fork")
    db = Database(db_path)
    with db.write() as tx:
        tx.execute("CREATE TABLE t(who TEXT)")
        parents = tx.connection

    child_pid = os.fork()
    if child_pid == 0:  # the child: report through the exit status and never return into pytest
        status = 1
        try:
            db.close()  # must leave the inherited connection alone
            with db.write() as tx:
                assert tx.connection is not parents
                tx.execute("INSERT INTO t VALUES ('child')")
            db.close()
            status = 0
        finally:
            os._exit(status)

    _unused_pid, wait_status = os.waitpid(child_pid, 0)
    assert os.waitstatus_to_exitcode(wait_status) == 0
    with db.write() as tx:
        assert tx.connection is parents  # still the parent's, still open
        tx.execute("INSERT INTO t VALUES ('parent')")
    with db.read() as tx:
        assert tx.execute("SELECT who FROM t ORDER BY who").fetchall() == [("child",), ("parent",)]
    db.close()
