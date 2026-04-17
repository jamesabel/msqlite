"""msqlite — SQLite access that is safe across threads and processes.

Serializes writes with ``BEGIN EXCLUSIVE TRANSACTION`` held for the lifetime of
the :class:`MSQLite` context manager, retrying with randomized backoff when the
file-level lock is held by another connection.
"""

import sqlite3
from logging import getLogger
import time
import random
import re
from pathlib import Path
from typing import Any
from collections.abc import Mapping, Sequence
import json

log = getLogger()

_valid_identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str) -> str:
    """Return ``name`` unchanged if it's a safe SQL identifier; otherwise raise ``ValueError``."""
    if not _valid_identifier_re.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _is_locked_error(exc: sqlite3.OperationalError) -> bool:
    """True if ``exc`` indicates the database is locked by another connection."""
    return "database is locked" in str(exc).lower()


class MSQLiteNoSchemaException(Exception):
    """Raised when an operation needs to auto-create a table but no schema was supplied."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        super().__init__(f'No schema provided for table "{table_name}"')


class MSQLiteMaxRetriesError(sqlite3.OperationalError):
    """Raised when lock-acquire retries exceed ``retry_limit``."""


# Mapping of Python types (or their string names) to SQLite storage classes, used
# to build ``CREATE TABLE`` column definitions. Both type objects and string names
# are accepted as keys so schemas can be expressed either way.
type_to_sqlite_type = {
    int: "INTEGER",
    "int": "INTEGER",
    float: "REAL",
    "float": "REAL",
    str: "TEXT",
    "str": "TEXT",
    bytes: "BLOB",
    "bytes": "BLOB",
    bool: "INTEGER",
    "bool": "INTEGER",
    json: "JSON",
    "json": "JSON",
    "JSON": "JSON",
    type(None): "NULL",
    "None": "NULL",
    "none": "NULL",
}


def _convert_column_spec_to_sqlite(column_spec: str, column_type: Any) -> str:
    """
    Build a single SQLite column definition from a column spec and Python type.

    :param column_spec: column name followed by optional constraints (e.g. ``"id PRIMARY KEY"``)
    :param column_type: Python type (``int``, ``float``, ``str``, ``bytes``, ``bool``, ``NoneType``)
        or the ``json`` module
    :return: a SQLite column definition, e.g. ``"id INTEGER PRIMARY KEY"``
    """

    # ``json`` is a module, not a type — accept it explicitly since ``isinstance(json, type)`` is False.
    if not (isinstance(column_type, type) or column_type is json):
        raise TypeError(f"column_type must be a type or json, got {column_type!r}")

    parts = column_spec.split()
    if not parts:
        raise ValueError("column_spec must not be empty")
    column_name = _validate_identifier(parts[0])
    sqlite_type = type_to_sqlite_type.get(column_type)
    if sqlite_type is None:
        raise ValueError(f"{column_type} (type={type(column_type)}) is not a supported SQLite column type (see msqlite.type_to_sqlite_type for supported types)")
    return " ".join([column_name, sqlite_type, *parts[1:]])


class MSQLite:
    """
    Context manager around ``sqlite3`` that serializes access across threads and processes.

    On ``__enter__`` an EXCLUSIVE transaction is started, acquiring a file-level write lock;
    if the lock is held by another connection the attempt is retried with randomized jittered
    backoff until it succeeds or ``retry_limit`` is exceeded (raises :class:`MSQLiteMaxRetriesError`).
    On ``__exit__`` the transaction is committed on clean exit, or rolled back if an exception
    propagates, and the connection is closed.

    The target table is auto-created on the first ``execute`` that hits "no such table" if a
    ``schema`` was supplied. Read-only use against an existing table may omit the schema.
    """

    def __init__(self, db_path: Path, table_name: str, schema: dict[str, type] | None = None, indexes: list[str] | None = None, *, retry_scale: float = 0.01, retry_limit: int | None = None):
        """
        :param db_path: database file path
        :param table_name: table name
        :param schema: dict mapping column spec -> Python type. Example: ``{"id PRIMARY KEY": int, "name": str, "color": str, "year": int}``
        :param indexes: list of column names to index; one index is created per column on auto-create
        :param retry_scale: scale factor for the sleep between lock-retry attempts; 1.0 averages ~1 second (keyword only)
        :param retry_limit: maximum retry attempts before raising :class:`MSQLiteMaxRetriesError` (keyword only)
        """
        self.db_path = db_path
        self.table_name = _validate_identifier(table_name)
        self.schema = schema
        if indexes is not None:
            for index in indexes:
                _validate_identifier(index)
        self.indexes = indexes
        self.retry_scale = retry_scale
        self.retry_limit = retry_limit
        self.max_execution_time: float | None = None
        self.execution_count = 0
        self.retry_count = 0
        self.artificial_delay: float | None = None
        self.conn: sqlite3.Connection | None = None

    def __enter__(self):
        while self.conn is None:
            if self.retry_limit is not None and self.retry_count > self.retry_limit:
                raise MSQLiteMaxRetriesError(f"Exceeded maximum retries of {self.retry_limit}")
            try:
                self.conn = sqlite3.connect(self.db_path, isolation_level="EXCLUSIVE")
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")  # lock the database
            except sqlite3.OperationalError as e:
                if not _is_locked_error(e):
                    raise
                if self.conn is not None:
                    self.conn.close()
                self.conn = None
                self.retry_count += 1
                time.sleep(self.retry_scale * 2.0 * random.random())
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.conn is None:
            log.warning(f'Connection is None in __exit__ for "{self.db_path}" and table={self.table_name}')
        else:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
            self.conn.close()
            self.conn = None
        log.debug(f"{self.max_execution_time=}")
        if self.retry_count > 0:
            log.info(f"{self.retry_count=}")
        else:
            log.debug(f"{self.retry_count=}")

    def create_table(self):
        """Create the configured table and any indexes using the schema passed to ``__init__``.

        Idempotent — uses ``CREATE TABLE IF NOT EXISTS`` and ``CREATE INDEX IF NOT EXISTS``.
        """
        if self.conn is None:
            raise RuntimeError("create_table called without an active connection")
        if self.schema is None:
            raise MSQLiteNoSchemaException(self.table_name)
        columns = ",".join(_convert_column_spec_to_sqlite(spec, col_type) for spec, col_type in self.schema.items())
        cursor = self.conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name}({columns})")
        if self.indexes:
            for index in self.indexes:
                cursor.execute(f"CREATE INDEX IF NOT EXISTS {self.table_name}_{index}_idx ON {self.table_name}({index})")

    def set_artificial_delay(self, delay: float):
        """
        Inject a per-``execute`` sleep that holds the write lock open. Intended for tests that
        exercise retry/contention paths; not for production use.

        :param delay: delay in seconds applied at the start of each ``execute``
        """
        self.artificial_delay = delay

    @staticmethod
    def _run(cursor: sqlite3.Cursor, statement: str, parameters: Mapping | Sequence | None) -> sqlite3.Cursor:
        """Single dispatch into ``Cursor.execute`` so callers don't branch on ``parameters is None``."""
        if parameters is None:
            return cursor.execute(statement)
        return cursor.execute(statement, parameters)

    def execute(self, statement: str, parameters: Mapping | Sequence | None = None) -> sqlite3.Cursor:
        """
        Execute a single SQL statement on the active transaction.

        If the target table is missing and a ``schema`` was supplied, the table (and any configured
        indexes) is created on the fly and the statement is re-run once.

        :param statement: SQL statement to execute
        :param parameters: parameters for the SQL statement (iterable or mapping, per ``sqlite3``)
        :return: the cursor produced by :meth:`sqlite3.Cursor.execute`
        """

        if self.conn is None:
            raise RuntimeError("execute called without an active connection")
        if self.artificial_delay is not None:
            time.sleep(self.artificial_delay)  # only for testing
        start = time.time()
        cursor = self.conn.cursor()
        try:
            new_cursor = self._run(cursor, statement, parameters)
        except sqlite3.OperationalError as e:
            if "no such table" not in str(e).lower():
                raise
            self.create_table()
            new_cursor = self._run(cursor, statement, parameters)
        execution_time = time.time() - start
        self.execution_count += 1
        if self.max_execution_time is None or execution_time > self.max_execution_time:
            self.max_execution_time = execution_time
        return new_cursor
