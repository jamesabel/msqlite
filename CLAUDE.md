# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

msqlite is a zero-dependency Python library that wraps SQLite with multi-threaded/multi-process safe access. It prevents "database is locked" errors with retried, bounded lock acquisition: `Database` is the general-purpose multi-table layer (WAL, `BEGIN IMMEDIATE` writes, non-blocking `DEFERRED` reads, per-thread connection reuse) and `MSQLite` is the original single-table helper (`EXCLUSIVE` transaction held for the whole context manager, fresh connection per access, auto-created table).

## Commands

The project is managed with uv: dev dependencies are the `dev` group in `pyproject.toml`, pinned by `uv.lock`.
Install with `uv sync --no-build` (wheels only; never build a dependency from source) and run tools with `uv run`.

```bash
# Run all tests
uv run pytest

# Run a single test file
uv run pytest test_msqlite/test_example.py

# Run a single test function
uv run pytest test_msqlite/test_msqlite.py::test_msqlite_single_thread

# Run tests with coverage
uv run pytest --cov-report=html --cov-report=xml:cov/coverage.xml --cov src/msqlite

# Format and lint (ruff, line-length 192, configured in pyproject.toml)
uv run ruff format src test_msqlite scripts
uv run ruff check src test_msqlite scripts

# Type check (mypy strict, files configured in pyproject.toml)
uv run mypy

# Write-latency benchmark (prints the README table)
uv run python scripts/benchmark.py --processes 1 10 100 --writes 20

# Build the sdist and wheel (uv_build backend)
uv build
```

## Architecture

**Single module design:** The entire implementation is in `src/msqlite/msqlite.py`. The public API (`Database`, `Transaction`, `MSQLite`, `Stats`, `MSQLiteTimeoutError`, `MSQLiteMaxRetriesError`, `MSQLiteNoSchemaException`, `WriteMode`, `type_to_sqlite_type`) is re-exported from `src/msqlite/__init__.py`. `MSQLite` is a thin wrapper over `Database(persistent=False, wal=False)` so both share one retry implementation.

**Retry engine:** Lock errors are detected by SQLite result code (`SQLITE_BUSY`/`SQLITE_LOCKED`, primary code masked from the extended code); `SQLITE_BUSY_SNAPSHOT` is never retried. `_WaitState` tracks per-transaction waited time and retries across `BEGIN`, single `execute` calls and `COMMIT`. `timeout_s` bounds total wait (raises `MSQLiteTimeoutError`), `retry_limit` bounds retries (raises the subclass `MSQLiteMaxRetriesError`). SQLite's own busy timeout is set per connection and capped to the remaining deadline. Backoff is jittered exponential: `random() * min(retry_scale * 2**retries, retry_cap_s)`.

**Connections:** Opened with `autocommit=True`; the library issues `BEGIN`/`COMMIT`/`ROLLBACK` itself. Persistent connections live in a `threading.local` tagged with the pid so a forked child never reuses the parent's; the `_slots` registry is a `WeakSet` and each slot has a `weakref.finalize` that closes its connection when the owning thread ends (skipped for a connection inherited across a fork). `Transaction` closes every cursor it handed out on exit, because CPython's `Connection.close()` keeps the file lock until all statements are finalized.

**Automatic schema (`MSQLite`):** Pass a `schema` dict (column spec → Python type) and optional `indexes`. The table is created on the first `execute()` that hits "no such table". Omit `schema` when reading from an existing table.

**Type mappings:** `int`→INTEGER, `float`→REAL, `str`→TEXT, `bytes`→BLOB, `bool`→INTEGER, `json`→JSON, `None`→NULL. Unsupported types raise `ValueError`.

## Test Structure

Tests are in `test_msqlite/` with `conftest.py` enabling beartype runtime type checking. Tests write to a `temp/` directory. Key tests: 200-process contention (`test_msqlite.py`), readers during writes under WAL in threads and processes (`test_wal_readers.py`), deadline errors (`test_deadline.py`), statement-level and commit retry in rollback-journal mode (`test_statement_retry.py`), migrations (`test_migrate.py`), and the deprecation shims (`test_deprecations.py`). Timing-based tests use generous margins (hold times of 0.4 to 0.5 s against thresholds at half that).

## Release policy

Semantic versioning with `CHANGELOG.md`; a changed signature keeps a deprecation shim for one minor version. Python versions tested: minimum supported (3.12), latest stable (3.15) and the pre-release (3.16), with the policy noted next to the matrix in `.github/workflows/tests.yml`. Publishing runs from the GitHub release workflow (`uv build` then `uv publish` with trusted publishing); never upload to PyPI by hand.
