# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

msqlite is a zero-dependency Python library that wraps SQLite with multi-threaded/multi-process safe access. It prevents "database is locked" errors by using `EXCLUSIVE` transaction isolation, holding the lock for the entire context manager lifetime, with built-in retry and exponential backoff.

## Commands

```bash
# Run all tests
pytest

# Run a single test file
pytest test_msqlite/test_example.py

# Run a single test function
pytest test_msqlite/test_msqlite.py::test_msqlite_single_thread

# Run tests with coverage
pytest --cov-report=html --cov-report=xml:cov/coverage.xml --cov src/msqlite

# Format code (Black, line-length 192)
black --line-length 192 .

# Lint
flake8
mypy src/msqlite
```

## Architecture

**Single module design:** The entire implementation is in `src/msqlite/msqlite.py` (~180 lines). The public API (`MSQLite`, `MSQLiteMaxRetriesError`, `MSQLiteNoSchemaException`, `type_to_sqlite_type`) is re-exported from `src/msqlite/__init__.py`.

**Concurrency model:** `MSQLite` is a context manager. On `__enter__`, it opens a connection with `BEGIN EXCLUSIVE TRANSACTION`, acquiring a file-level lock. On `__exit__`, it commits and closes. If the lock is held by another process, it retries with randomized exponential backoff (`retry_scale * 2.0 * random.random()`). An optional `retry_limit` parameter caps retries and raises `MSQLiteMaxRetriesError`.

**Automatic schema:** Pass a `schema` dict (column name → Python type) and optional `column_spec` dict (column name → constraint like `"PRIMARY KEY"`) to the constructor. The table is created on first `execute()` if it doesn't exist. Omit `schema` when reading from an existing table.

**Type mappings:** `int`→INTEGER, `float`→REAL, `str`→TEXT, `bytes`→BLOB, `bool`→INTEGER, `json`→JSON, `None`→NULL. Unsupported types raise `TypeError`.

## Test Structure

Tests are in `test_msqlite/` with `conftest.py` enabling beartype runtime type checking. Tests write to a `temp/` directory. Key tests include multi-process concurrency (200 processes), retry limits, JSON columns, indexes, and error handling.
