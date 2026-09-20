"""Write-latency benchmark: N processes contend for one database, each doing K single-row writes.

Prints a Markdown table (used in the README). Run from the repository root::

    python scripts/benchmark.py --processes 1 10 100 --writes 20
"""

import argparse
import multiprocessing
import platform
import sqlite3
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from msqlite import Database, MSQLite

TABLE = "bench"


def _database_writer(db_path: Path) -> Callable[[int], None]:
    db = Database(db_path)  # one instance per process: its connection is reused across writes

    def write(value: int) -> None:
        with db.write() as tx:
            tx.execute(f"INSERT INTO {TABLE} VALUES (?, ?)", (value, time.time()))

    return write


def _msqlite_writer(db_path: Path) -> Callable[[int], None]:
    def write(value: int) -> None:
        with MSQLite(db_path, TABLE, {"value": int, "timestamp": float}) as db:
            db.execute(f"INSERT INTO {TABLE} VALUES (?, ?)", (value, time.time()))

    return write


MODES = {
    "Database.write() (WAL, IMMEDIATE, persistent connection)": _database_writer,
    "MSQLite (rollback journal, EXCLUSIVE, connection per access)": _msqlite_writer,
}


def _worker(mode: str, db_path: Path, writes: int, barrier, queue) -> None:
    write = MODES[mode](db_path)
    barrier.wait()
    latencies = []
    for value in range(writes):
        start = time.perf_counter()
        write(value)
        latencies.append(time.perf_counter() - start)
    queue.put(latencies)


def _prepare(mode: str, db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm", "-journal"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)
    if mode.startswith("Database"):
        with Database(db_path) as db, db.write() as tx:
            tx.execute(f"CREATE TABLE {TABLE}(value INTEGER, timestamp REAL)")
    else:
        with MSQLite(db_path, TABLE, {"value": int, "timestamp": float}) as db:
            db.execute(f"SELECT count(*) FROM {TABLE}")  # auto-creates the table


def run(mode: str, db_path: Path, processes: int, writes: int) -> dict[str, float]:
    _prepare(mode, db_path)
    barrier = multiprocessing.Barrier(processes + 1)
    queue: multiprocessing.Queue = multiprocessing.Queue()
    workers = [multiprocessing.Process(target=_worker, args=(mode, db_path, writes, barrier, queue)) for _unused_index in range(processes)]
    for worker in workers:
        worker.start()
    barrier.wait()
    start = time.perf_counter()
    latencies = [latency for _unused_index in range(processes) for latency in queue.get()]
    elapsed = time.perf_counter() - start
    for worker in workers:
        worker.join()
    latencies.sort()
    return {
        "median_ms": statistics.median(latencies) * 1000.0,
        "p95_ms": latencies[int(len(latencies) * 0.95) - 1] * 1000.0,
        "max_ms": latencies[-1] * 1000.0,
        "writes_per_s": len(latencies) / elapsed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--processes", type=int, nargs="+", default=[1, 10, 100])
    parser.add_argument("--writes", type=int, default=20, help="writes per process")
    parser.add_argument("--db", type=Path, default=Path("temp", "benchmark.sqlite"))
    args = parser.parse_args()
    args.db.parent.mkdir(exist_ok=True)
    print(f"{platform.platform()}, Python {platform.python_version()}, SQLite {sqlite3.sqlite_version}, {args.writes} writes per process")
    print()
    print("| Mode | Processes | Median (ms) | p95 (ms) | Max (ms) | Writes/s |")
    print("|------|----------:|------------:|---------:|---------:|---------:|")
    for mode in MODES:
        for processes in args.processes:
            result = run(mode, args.db, processes, args.writes)
            print(f"| {mode} | {processes} | {result['median_ms']:.1f} | {result['p95_ms']:.1f} | {result['max_ms']:.0f} | {result['writes_per_s']:.0f} |", flush=True)


if __name__ == "__main__":
    main()
