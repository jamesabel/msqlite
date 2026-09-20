"""Under WAL, readers never wait for a writer (and a writer never waits for readers).

All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note.
"""

import threading
import time
from multiprocessing import Pool
from pathlib import Path

from msqlite import Database
from test_msqlite.paths import get_temp_dir

HOLD_S = 0.5


def _fresh_db_path(name: str) -> Path:
    db_path = get_temp_dir() / f"{name}.sqlite"
    for suffix in ("", "-wal", "-shm", "-journal"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)
    return db_path


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_reader_does_not_wait_for_writer_thread():
    db_path = _fresh_db_path("test_wal_reader_thread")
    db = Database(db_path)
    with db.write() as tx:
        tx.execute("CREATE TABLE t(x)")
        tx.execute("INSERT INTO t VALUES (1)")
    writer_started = threading.Event()

    def writer():
        with db.write() as wtx:
            wtx.execute("INSERT INTO t VALUES (2)")
            writer_started.set()
            time.sleep(HOLD_S)  # hold the write lock with an uncommitted row

    thread = threading.Thread(target=writer)
    thread.start()
    assert writer_started.wait(5.0)
    start = time.monotonic()
    with db.read() as rtx:
        rows = rtx.execute("SELECT x FROM t ORDER BY x").fetchall()
    elapsed = time.monotonic() - start
    thread.join()
    assert rows == [(1,)]  # snapshot from before the uncommitted write
    assert elapsed < HOLD_S / 2, f"reader waited {elapsed:.3f} s for the writer"
    with db.read() as rtx:
        assert rtx.execute("SELECT x FROM t ORDER BY x").fetchall() == [(1,), (2,)]
    db.close()


def _mp_db_path() -> Path:
    return get_temp_dir() / "test_wal_reader_processes.sqlite"


def _writer(index: int) -> float:
    db = Database(_mp_db_path())
    max_s = 0.0
    for value in range(3):
        start = time.monotonic()
        with db.write() as tx:
            tx.execute("INSERT INTO t VALUES (?)", (index * 100 + value,))
            time.sleep(HOLD_S)
        max_s = max(max_s, time.monotonic() - start - HOLD_S)
    db.close()
    return max_s


def _reader(_unused_index: int) -> float:
    db = Database(_mp_db_path())
    max_s = 0.0
    for _unused_repeat in range(20):
        start = time.monotonic()
        with db.read() as tx:
            tx.execute("SELECT count(*) FROM t").fetchone()
        max_s = max(max_s, time.monotonic() - start)
        time.sleep(0.05)
    db.close()
    return max_s


def _task(args: tuple[str, int]) -> tuple[str, float]:
    kind, index = args
    return kind, (_writer(index) if kind == "writer" else _reader(index))


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_readers_do_not_wait_for_writer_processes():
    db_path = _fresh_db_path("test_wal_reader_processes")
    with Database(db_path) as db:
        with db.write() as tx:
            tx.execute("CREATE TABLE t(x)")
    tasks = [("writer", index) for index in range(3)] + [("reader", index) for index in range(3)]
    with Pool(len(tasks)) as pool:
        results = pool.map(_task, tasks)
    reader_max_s = max(seconds for kind, seconds in results if kind == "reader")
    writer_wait_s = max(seconds for kind, seconds in results if kind == "writer")
    print(f"{reader_max_s=:.3f} {writer_wait_s=:.3f}")
    assert reader_max_s < HOLD_S / 2, f"a reader waited {reader_max_s:.3f} s while writers held the lock for {HOLD_S} s at a time"
    assert writer_wait_s >= HOLD_S / 2  # writers did contend with each other, so the readers' result is meaningful
