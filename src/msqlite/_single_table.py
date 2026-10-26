"""The original single-table helper, :class:`MSQLite`, as a thin wrapper over :class:`Database`."""

import sqlite3
import time
import warnings
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from ._database import Database, Transaction
from ._errors import MSQLiteNoSchemaException, is_missing_table_error
from ._log import log
from ._sql import create_index_sql, create_table_sql, validate_identifier
from ._stats import Stats
from ._types import OnRetry, Parameters, WriteMode


def _warn_deprecated(old: str, new: str) -> None:
    """Emit the ``DeprecationWarning`` for a counter property, attributed to the caller of that property."""
    warnings.warn(f"MSQLite.{old} is deprecated; use MSQLite.{new}", DeprecationWarning, stacklevel=3)


class MSQLite:
    """
    Context manager around ``sqlite3`` that serializes access to one table across threads and processes.

    On ``__enter__`` an ``EXCLUSIVE`` transaction is started on a fresh connection, acquiring the write
    lock; if the lock is held by another connection the attempt is retried with randomized jittered
    backoff until it succeeds, ``retry_limit`` is exceeded (:class:`MSQLiteMaxRetriesError`) or
    ``timeout_s`` of waiting has elapsed (:class:`MSQLiteTimeoutError`). On ``__exit__`` the transaction
    is committed on clean exit, or rolled back if an exception propagates, and the connection is closed.

    The target table is auto-created on the first ``execute`` that hits "no such table" if a
    ``schema`` was supplied. Read-only use against an existing table may omit the schema.

    This is a thin wrapper over :class:`Database`; use that directly for multi-table databases,
    non-blocking readers (WAL) or long-lived connections.
    """

    def __init__(
        self,
        db_path: Path | str,
        table_name: str,
        schema: dict[str, Any] | None = None,
        indexes: list[str] | None = None,
        *,
        retry_scale: float = 0.01,
        retry_limit: int | None = None,
        timeout_s: float | None = None,
        retry_cap_s: float = 1.0,
        busy_timeout_s: float = 5.0,
        wal: bool = False,
        mode: WriteMode = "exclusive",
        on_retry: OnRetry | None = None,
    ):
        """
        :param db_path: database file path
        :param table_name: table name
        :param schema: dict mapping column spec -> Python type. Example: ``{"id PRIMARY KEY": int, "name": str, "color": str, "year": int}``
        :param indexes: list of column names to index; one index is created per column on auto-create
        :param retry_scale: base unit of the backoff sleep in seconds (keyword only)
        :param retry_limit: maximum retry attempts before raising :class:`MSQLiteMaxRetriesError`; ``None`` is unbounded (keyword only)
        :param timeout_s: maximum total lock wait before raising :class:`MSQLiteTimeoutError`; ``None`` waits forever (keyword only)
        :param retry_cap_s: upper bound of the backoff sleep range in seconds (keyword only)
        :param busy_timeout_s: SQLite's own busy timeout per attempt (keyword only)
        :param wal: switch the database to WAL journal mode; off by default so existing files are unchanged (keyword only)
        :param mode: ``"exclusive"`` (default, the historical behaviour) or ``"immediate"`` (keyword only)
        :param on_retry: called as ``on_retry(attempt, elapsed_s, error)`` before each backoff sleep (keyword only)
        """
        self.table_name = validate_identifier(table_name)
        self.schema = schema
        if indexes is not None:
            for index in indexes:
                validate_identifier(index)
        self.indexes = indexes
        self.mode: WriteMode = mode
        self.database = Database(
            db_path,
            wal=wal,
            timeout_s=timeout_s,
            retry_limit=retry_limit,
            retry_scale=retry_scale,
            retry_cap_s=retry_cap_s,
            busy_timeout_s=busy_timeout_s,
            on_retry=on_retry,
            persistent=False,
        )
        self.artificial_delay: float | None = None
        self._tx: Transaction | None = None

    @property
    def db_path(self) -> Path:
        return self.database.db_path

    @property
    def retry_scale(self) -> float:
        return self.database.retry_scale

    @property
    def retry_limit(self) -> int | None:
        return self.database.retry_limit

    @property
    def stats(self) -> Stats:
        """Counters accumulated over the lifetime of this instance (see :class:`Stats`)."""
        return self.database.stats

    @property
    def conn(self) -> sqlite3.Connection | None:
        """The active connection, or ``None`` outside the ``with`` block."""
        if self._tx is None:
            return None
        return self._tx.connection

    # Deprecated counters kept for one minor version; read ``stats`` instead.

    @property
    def max_execution_time(self) -> float | None:
        _warn_deprecated("max_execution_time", "stats.max_execution_s")
        return self.database.stats.max_execution_s

    @property
    def execution_count(self) -> int:
        _warn_deprecated("execution_count", "stats.executions")
        return self.database.stats.executions

    @property
    def retry_count(self) -> int:
        _warn_deprecated("retry_count", "stats.retries")
        return self.database.stats.retries

    # ----------------------------------------------------------------- context manager

    def __enter__(self) -> Self:
        if self._tx is not None:
            raise RuntimeError("MSQLite context is already active")
        self._tx = self.database.write(self.mode).__enter__()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        tx = self._tx
        if tx is None:
            log.warning(f'Connection is None in __exit__ for "{self.db_path}" and table={self.table_name}')
        else:
            self._tx = None
            tx.__exit__(exc_type, exc_value, traceback)
        stats = self.database.stats
        log.debug(f"max_execution_s={stats.max_execution_s}")
        if stats.retries > 0:
            log.info(f"retries={stats.retries}")
        else:
            log.debug(f"retries={stats.retries}")

    # ----------------------------------------------------------------- statements

    def _active_transaction(self, operation: str) -> Transaction:
        if self._tx is None:
            raise RuntimeError(f"{operation} called without an active connection")
        return self._tx

    def create_table(self) -> None:
        """Create the configured table and any indexes using the schema passed to ``__init__``.

        Idempotent: uses ``CREATE TABLE IF NOT EXISTS`` and ``CREATE INDEX IF NOT EXISTS``.
        """
        tx = self._active_transaction("create_table")
        if self.schema is None:
            raise MSQLiteNoSchemaException(self.table_name)
        tx.execute(create_table_sql(self.table_name, self.schema))
        for index in self.indexes or ():
            tx.execute(create_index_sql(self.table_name, index))

    def set_artificial_delay(self, delay: float) -> None:
        """
        Inject a per-``execute`` sleep that holds the write lock open. Intended for tests that
        exercise retry/contention paths; not for production use.

        :param delay: delay in seconds applied at the start of each ``execute``
        """
        self.artificial_delay = delay

    def execute(self, statement: str, parameters: Parameters | None = None) -> sqlite3.Cursor:
        """
        Execute a single SQL statement on the active transaction.

        If the target table is missing and a ``schema`` was supplied, the table (and any configured
        indexes) is created on the fly and the statement is re-run once.

        :param statement: SQL statement to execute
        :param parameters: parameters for the SQL statement (iterable or mapping, per ``sqlite3``)
        :return: the cursor produced by :meth:`sqlite3.Cursor.execute`
        """
        tx = self._active_transaction("execute")
        if self.artificial_delay is not None:
            time.sleep(self.artificial_delay)  # only for testing
        try:
            return tx.execute(statement, parameters)
        except sqlite3.OperationalError as e:
            if not is_missing_table_error(e):
                raise
            self.create_table()
            return tx.execute(statement, parameters)
