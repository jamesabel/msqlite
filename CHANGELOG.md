# Changelog

All notable changes to msqlite are recorded here. The project follows
[Semantic Versioning](https://semver.org/): breaking changes bump the major version, and a
signature that changes keeps a deprecation shim for at least one minor version.

## [Unreleased]

### Changed

- CI runs on an explicit `ubuntu-26.04` image instead of `ubuntu-latest`, and the actions are current: `actions/checkout@v7` and `schneegans/dynamic-badges-action@v1.9.0` (both on Node 24).

## [1.1.0] - 2026-10-26

### Changed

- The implementation is split into private modules by concern (`_retry`, `_connections`, `_stats`, `_sql`,
  `_errors`, `_database`, `_single_table`). The public API is unchanged and `from msqlite.msqlite import ...`
  still works. `BEGIN`, single statements and `COMMIT` now go through one retry loop (`LockRetrier`) over a pure
  `RetryPolicy`; `Transaction.execute`, `executemany` and `executescript` share one timed cursor path.
- Log records go to the `msqlite` logger. They used to go to `msqlite.msqlite`, a child of it, so a handler or
  level on `msqlite` already received them; only a filter on the exact old name needs updating.

### Fixed

- Lock-error detection assumed every `sqlite3.OperationalError` carries `sqlite_errorcode`. One constructed in
  Python rather than raised by the engine has no such attribute, so the documented message fallback raised
  `AttributeError` instead of running. Found by the new unit test for the fallback.
- `pragmas=("PRAGMA",)` (the prefix with nothing after it) was accepted and failed later when the connection
  opened; it is now rejected up front like an empty pragma.

## [1.0.2] - 2026-10-26

### Changed

- Test matrix moved to Python 3.12 (minimum), 3.15 (latest stable) and 3.16 (pre-release); lint, type
  checking and the coverage badge run on 3.15. Added the 3.16 classifier.
- The project is managed with uv: the dev dependencies are a `dev` dependency group in `pyproject.toml`
  pinned by `uv.lock` (`requirements-dev.txt` is gone), the build backend is `uv_build`, and both workflows
  run on `astral-sh/setup-uv`, which carries pre-release Pythons before `actions/setup-python` does. CI
  installs with `uv sync --locked --no-build`, so a dependency without a wheel fails instead of being
  built from source; releases run `uv build` and `uv publish`.
- License metadata uses the PEP 639 `license` and `license-files` fields instead of the deprecated classifier.

## [1.0.1] - 2026-10-26

### Fixed

- `Database(persistent=True)` kept a finished thread's connection open until `close()` or interpreter
  exit, so a long-lived `Database` used from short-lived threads (a recycling pool, a server, a library
  called from its users' threads) accumulated one open connection per thread. A thread's connection is
  now closed when the thread ends: the slot registry is a `WeakSet` and each connection has a
  `weakref.finalize`. A connection inherited across a fork is left alone, as `close()` already did.

## [1.0.0] - 2026-09-20

The general-purpose release: a multi-table `Database` layer next to the original
single-table `MSQLite` helper, both on one retry implementation. Existing
`MSQLite(path, table, schema)` code keeps working unchanged.

### Added

- `Database(path, *, wal=True, timeout_s=30.0, ...)` with `write()` (`BEGIN IMMEDIATE`, or
  `mode="exclusive"`) and `read()` (`BEGIN DEFERRED`) transactions. Under WAL, the default,
  readers never wait for a writer.
- `Transaction` with `execute`, `executemany`, `executescript`, `fetchone`, `fetchall`,
  `lastrowid`, `rowcount`, `row_factory` and `connection`.
- One connection per thread reused across transactions (`persistent=True`); `close()` and
  context-manager support release them. Connections are never carried across a fork.
- Lock errors detected by SQLite result code (`SQLITE_BUSY`, `SQLITE_LOCKED`) instead of
  message text; `SQLITE_BUSY_SNAPSHOT` is raised immediately since a retry cannot succeed.
- Retry on `BEGIN`, on each single `execute` inside the transaction and on `COMMIT`, with
  jittered exponential backoff capped at `retry_cap_s`. Statement retry re-executes only the
  failed statement; the transaction stays open.
- `timeout_s` wall-clock deadline on total lock wait, raising `MSQLiteTimeoutError(elapsed_s, attempts)`.
  `MSQLiteMaxRetriesError` is now its subclass, so `except MSQLiteTimeoutError` catches both.
- SQLite's own busy timeout (`busy_timeout_s`, default 5 s) set on every connection and capped to
  the remaining deadline.
- `pragmas=(...)` and `on_open(connection)` for per-connection setup.
- `Database.migrate(steps)` keyed on `PRAGMA user_version`.
- `on_retry(attempt, elapsed_s, error)` callback and a `Stats` dataclass
  (`executions`, `retries`, `max_execution_s`, `max_wait_s`) exposed as `.stats` on both classes.
- `MSQLite` accepts `timeout_s`, `retry_cap_s`, `busy_timeout_s`, `wal`, `mode` and `on_retry`,
  and a `str` path as well as a `Path`.
- Logging goes to the `msqlite.msqlite` logger instead of the root logger.
- Tests for readers during writes under WAL (threads and processes), the deadline error,
  statement-level and commit retry, migrations and the deprecation shims.
- `scripts/benchmark.py` and a benchmark table in the README.
- Python 3.15 in the test matrix (the tests run without beartype on a Python it does not support yet); ruff (lint and format) and strict mypy in CI.

### Changed

- Connections are opened with `autocommit=True` and the library issues `BEGIN`/`COMMIT`/`ROLLBACK`
  itself, so `executescript` no longer risks an implicit commit.
- `MSQLite.__exit__` closes every cursor it handed out before closing the connection, so an
  unconsumed `SELECT` can no longer keep the file lock alive until garbage collection.
- Backoff is exponential with jitter (`retry_scale` remains the base unit) rather than a fixed
  jittered sleep.

### Deprecated

- `MSQLite.max_execution_time`, `MSQLite.execution_count` and `MSQLite.retry_count` now warn
  with `DeprecationWarning`; read `MSQLite.stats.max_execution_s`, `.executions` and `.retries`.
  They will be removed in 1.1.

## [0.7.2] and earlier

See the git history.
