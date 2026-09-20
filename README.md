![Tests](https://github.com/jamesabel/msqlite/actions/workflows/tests.yml/badge.svg)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/jamesabel/073de6201c5dc8c7d26d464af31c2a4b/raw/coverage.json)
![Publish to PyPI](https://github.com/jamesabel/msqlite/actions/workflows/publish.yml/badge.svg)

# msqlite

SQLite access that is safe across threads and processes. msqlite takes the lock for you,
retries with jittered backoff when another connection holds it, and gives up with a clear
error at a deadline instead of hanging or raising `database is locked` at random.

- Zero dependencies beyond the standard library; ships `py.typed` and passes mypy strict.
- `Database`: multi-table access with `write()` and `read()` transactions, WAL by default so
  readers never wait for a writer, and one reused connection per thread.
- `MSQLite`: the original single-table helper with an auto-created schema. Unchanged API.
- Python 3.12 and later.

```bash
pip install msqlite
```

## Quick start

```python
from pathlib import Path
from msqlite import Database

db = Database(Path("app.sqlite"))  # WAL, 30 s lock deadline, one connection per thread

db.migrate([
    (1, "CREATE TABLE journal(id INTEGER PRIMARY KEY, ts REAL, entry TEXT);"),
    (2, "CREATE INDEX journal_ts ON journal(ts);"),
])

with db.write() as tx:                       # BEGIN IMMEDIATE, retried until the deadline
    tx.execute("INSERT INTO journal(ts, entry) VALUES (?, ?)", (1.0, "started"))
    tx.executemany("INSERT INTO journal(ts, entry) VALUES (?, ?)", [(2.0, "a"), (3.0, "b")])
    print(tx.lastrowid, tx.rowcount)

with db.read() as tx:                        # BEGIN DEFERRED; never blocks on a writer under WAL
    rows = tx.execute("SELECT entry FROM journal ORDER BY ts").fetchall()

db.close()                                   # or use `with Database(...) as db:`
```

A transaction commits when the `with` block exits normally and rolls back if an exception
propagates. `tx` exposes `execute`, `executemany`, `executescript`, `fetchone`, `fetchall`,
`lastrowid`, `rowcount`, `row_factory` and the raw `connection`.

### The single-table helper

`MSQLite` opens a fresh connection per `with` block, holds an `EXCLUSIVE` transaction for the
whole block, and creates its table on first use from a `schema` dict. It is the right tool for
a small status or results table that several processes write to.

```python
import time
from pathlib import Path
from msqlite import MSQLite

table_name = "example"
schema = {"id PRIMARY KEY": int, "name": str, "color": str, "year": int}
db_path = Path("temp", "example.sqlite")
db_path.parent.mkdir(exist_ok=True)

with MSQLite(db_path, table_name, schema) as db:
    now = time.monotonic_ns()  # some index value
    db.execute(f"INSERT INTO {table_name} VALUES (?, ?, ?, ?), (?, ?, ?, ?)", (now, "plate", "red", 2020, now + 1, "chair", "green", 2019))
    for row in db.execute(f"SELECT * FROM {table_name}"):
        print(row)

with MSQLite(db_path, table_name) as db:  # reading an existing table needs no schema
    for row in db.execute(f"SELECT * FROM {table_name}"):
        print(row)
```

Column types map as `int`→INTEGER, `float`→REAL, `str`→TEXT, `bytes`→BLOB, `bool`→INTEGER,
`json`→JSON and `None`→NULL. Constraints go after the column name in the key, as in
`"id PRIMARY KEY"`. `indexes=["name"]` creates one index per listed column.

## Concurrency model

SQLite allows one writer at a time per database file. Everything msqlite does is about
acquiring that write lock predictably:

1. **Take the lock at `BEGIN`.** A write transaction starts with `BEGIN IMMEDIATE` (or
   `BEGIN EXCLUSIVE`), so the lock is acquired before any statement runs and lock errors happen
   in one predictable place.
2. **Let SQLite wait first.** Each connection has a busy timeout (`busy_timeout_s`, default 5 s).
   SQLite's own busy handler absorbs short waits inside the engine without a Python round trip.
3. **Then retry in Python.** A lock error (`SQLITE_BUSY` or `SQLITE_LOCKED`, detected by result
   code, not message text) is retried with jittered exponential backoff: the first sleep is
   uniform in `[0, 2 * retry_scale)` and the range doubles per retry up to `retry_cap_s`.
4. **Stop at a deadline.** `timeout_s` bounds the total time spent waiting for locks within one
   transaction. When it is exceeded, `MSQLiteTimeoutError(elapsed_s, attempts)` is raised.
   `retry_limit` bounds the number of retries instead, raising `MSQLiteMaxRetriesError`, a
   subclass of `MSQLiteTimeoutError`. The busy timeout is capped to the time remaining before
   the deadline, so a single attempt cannot overshoot it.
5. **Retry after `BEGIN` too.** A statement inside the transaction, or the `COMMIT` itself, can
   still hit a lock error (in rollback-journal mode a commit waits for readers). These are retried
   in place: only the failed statement is re-executed and the transaction stays open, so earlier
   statements are kept. `executemany` and `executescript` are not retried, since a partial run
   could not be re-applied safely; inside a write transaction they already hold the write lock,
   so they do not hit lock errors in practice.

`Database` keeps one connection per thread and reuses it across transactions. Connections are
never shared between threads and never carried across a fork (a forked child opens its own).
`MSQLite` opens and closes a connection per `with` block, which is what makes it process-safe
with zero setup.

### The three transaction modes

| Mode | Started by | Lock taken at `BEGIN` | Use for |
|------|-----------|------------------------|---------|
| `DEFERRED` | `db.read()` | none (a shared lock on first read) | reads only |
| `IMMEDIATE` | `db.write()` (default) | write lock (RESERVED) | any transaction that writes |
| `EXCLUSIVE` | `db.write(mode="exclusive")`, `MSQLite` | write lock, and blocks readers in rollback-journal mode | single-writer files, rollback journal |

**Why `IMMEDIATE` for writes.** A `DEFERRED` transaction that reads and then writes must upgrade
its shared lock to a write lock mid-transaction. If another writer is waiting, both are stuck:
the writer waits for readers to leave, and the reader waits for the writer. SQLite reports
`SQLITE_BUSY` without invoking the busy handler, and under WAL the upgrade fails outright with
`SQLITE_BUSY_SNAPSHOT` once another writer has committed, because the transaction's snapshot is
stale. Retrying cannot help, so msqlite raises that error immediately. `IMMEDIATE` takes the
write lock up front and the case never arises. Rule: anything that writes goes in `db.write()`.

**Under WAL** (the `Database` default) readers work from a snapshot and never wait for a writer,
and a writer never waits for readers. `PRAGMA journal_mode=WAL` is persisted in the database
file once set, so files created by `Database` stay in WAL mode for every other tool that opens
them. WAL creates `-wal` and `-shm` files next to the database.

**When a rollback journal is still right.** WAL needs shared memory between the processes that
open the file, so it does not work over network filesystems (NFS, SMB shares). If the database
lives on one of those, or if there is a single writer and readers can tolerate waiting, use a rollback journal:
`Database(path, wal=False)` (which leaves an existing file's journal mode untouched). `MSQLite` leaves it alone
unless asked (`wal=True`), so existing databases see no change.

## Configuration

```python
Database(
    path,
    wal=True,               # PRAGMA journal_mode=WAL on every connection
    timeout_s=30.0,         # total lock wait per transaction; None waits forever
    retry_limit=None,       # retries per transaction; None is unbounded
    retry_scale=0.01,       # base backoff unit in seconds
    retry_cap_s=1.0,        # upper bound of one backoff sleep
    busy_timeout_s=5.0,     # SQLite's own busy timeout per attempt
    pragmas=("synchronous=FULL",),
    on_open=lambda conn: conn.execute("PRAGMA foreign_keys=ON"),
    on_retry=lambda attempt, elapsed_s, error: log.warning("retry %d after %.2fs: %s", attempt, elapsed_s, error),
    row_factory=sqlite3.Row,
    persistent=True,        # one connection per thread, reused; False opens one per transaction
)
```

`MSQLite(path, table, schema, indexes, *, retry_scale, retry_limit, timeout_s, retry_cap_s,
busy_timeout_s, wal, mode, on_retry)` takes the same knobs. Its defaults keep the historical
behaviour: rollback journal, `EXCLUSIVE`, no deadline.

### Migrations

`db.migrate(steps)` applies `(version, sql_script)` pairs keyed on `PRAGMA user_version`.
Steps at or below the current version are skipped; the rest run in one write transaction, so a
failing step rolls everything back and leaves `user_version` untouched. Projects with their own
migrations table can ignore it.

### Observability

`db.stats` (and `MSQLite.stats`) is a `Stats` dataclass with `executions`, `retries`,
`max_execution_s` and `max_wait_s`. `on_retry(attempt, elapsed_s, error)` is called before each
backoff sleep. Log records go to the `msqlite.msqlite` logger.

## Benchmark

Write latency for one single-row insert per transaction while N processes contend for the same
file. Each process does 20 writes; the table shows the latency distribution of the `with` block
(lock acquisition, the insert and the commit) and the aggregate throughput.

Measured on Windows-11-10.0.26200-SP0, Python 3.14.3, SQLite 3.50.4 (a desktop with an NVMe SSD; numbers on CI runners or laptops will differ):

| Mode | Processes | Median (ms) | p95 (ms) | Max (ms) | Writes/s |
|------|----------:|------------:|---------:|---------:|---------:|
| Database.write() (WAL, IMMEDIATE, persistent connection) | 1 | 0.6 | 1.1 | 3 | 1160 |
| Database.write() (WAL, IMMEDIATE, persistent connection) | 10 | 0.6 | 1.1 | 256 | 747 |
| Database.write() (WAL, IMMEDIATE, persistent connection) | 100 | 0.6 | 35.2 | 5787 | 345 |
| MSQLite (rollback journal, EXCLUSIVE, connection per access) | 1 | 2.3 | 2.5 | 3 | 419 |
| MSQLite (rollback journal, EXCLUSIVE, connection per access) | 10 | 2.3 | 49.0 | 814 | 233 |
| MSQLite (rollback journal, EXCLUSIVE, connection per access) | 100 | 2.4 | 1503.9 | 17106 | 117 |

Per-write latency is dominated by the commit's fsync and, under contention, by waiting for the
other writers: with 100 processes each write queues behind up to 99 others. The rollback-journal
helper pays for opening a connection per access and for `EXCLUSIVE` locking. Reproduce with
`python scripts/benchmark.py`.

## When not to use this

- **Many concurrent writers with high throughput needs.** SQLite serializes writers; past a few
  hundred small writes per second across processes, a client-server database is the better fit.
- **Network filesystems.** SQLite locking is unreliable on NFS and SMB, WAL does not work there
  at all, and retrying cannot fix a lock that the filesystem reports incorrectly.
- **Long read-modify-write sequences in one transaction.** The write lock is held for the whole
  `with` block. Keep transactions short or every other writer waits.
- **Code that needs a connection pool or async access.** msqlite is synchronous and hands out
  one connection per thread.

## Development

```bash
pip install -r requirements-dev.txt
pip install -e .
pytest                         # includes a 200-process contention test; -m "not slow" skips the slowest
ruff format src test_msqlite scripts && ruff check src test_msqlite scripts
mypy                           # strict, configured in pyproject.toml
python -m build                # or: uv build
```

Tests run on the minimum supported Python, the latest stable release and the upcoming
pre-release. Releases are published to PyPI by the GitHub release workflow through trusted
publishing. See [CHANGELOG.md](CHANGELOG.md) for the release history.
