"""msqlite — SQLite access that is safe across threads and processes.

Two layers share one retry implementation:

* :class:`Database` — a general-purpose concurrency layer for any multi-table database.
  ``db.write()`` opens an ``IMMEDIATE`` (or ``EXCLUSIVE``) transaction and ``db.read()`` a
  ``DEFERRED`` one. Lock waits are bounded by a wall-clock deadline and a retry limit, the
  journal is WAL by default so readers never wait on a writer, and one connection per thread
  is reused across transactions.
* :class:`MSQLite` — the original single-table helper. It opens a fresh connection per
  context, holds an ``EXCLUSIVE`` transaction for the lifetime of the ``with`` block, and
  auto-creates its table from a ``schema`` dict.
"""

import json
import os
import random
import re
import sqlite3
import threading
import time
import warnings
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self

log = getLogger(__name__)

__all__ = [
    "Database",
    "MSQLite",
    "MSQLiteMaxRetriesError",
    "MSQLiteNoSchemaException",
    "MSQLiteTimeoutError",
    "Stats",
    "Transaction",
    "WriteMode",
    "type_to_sqlite_type",
]

WriteMode = Literal["immediate", "exclusive"]
Parameters = Mapping[str, Any] | Sequence[Any]
RowFactory = Callable[[sqlite3.Cursor, tuple[Any, ...]], Any]
OnOpen = Callable[[sqlite3.Connection], None]
OnRetry = Callable[[int, float, sqlite3.OperationalError], None]

_valid_identifier_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Primary SQLite result codes that mean "another connection holds a lock; try again later".
_LOCK_ERROR_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})


def _validate_identifier(name: str) -> str:
    """Return ``name`` unchanged if it's a safe SQL identifier; otherwise raise ``ValueError``."""
    if not _valid_identifier_re.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _is_lock_error(exc: sqlite3.OperationalError) -> bool:
    """True if ``exc`` is ``SQLITE_BUSY`` or ``SQLITE_LOCKED`` (including their extended codes)."""
    code = exc.sqlite_errorcode
    if code is None:
        # Raised by the Python layer rather than the SQLite engine; fall back to the message.
        return "database is locked" in str(exc).lower()
    return (code & 0xFF) in _LOCK_ERROR_CODES


def _is_stale_snapshot_error(exc: sqlite3.OperationalError) -> bool:
    """True for ``SQLITE_BUSY_SNAPSHOT``: a read transaction tried to write after another writer committed.

    Retrying the statement can never succeed inside the same transaction, so it is not retried.
    """
    return exc.sqlite_errorcode == sqlite3.SQLITE_BUSY_SNAPSHOT


class MSQLiteNoSchemaException(Exception):
    """Raised when an operation needs to auto-create a table but no schema was supplied."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        super().__init__(f'No schema provided for table "{table_name}"')


class MSQLiteTimeoutError(sqlite3.OperationalError):
    """Raised when the database lock could not be acquired within ``timeout_s`` of waiting.

    :ivar elapsed_s: total time spent waiting for the lock (backoff sleeps plus failed attempts)
    :ivar attempts: number of attempts made, including the first
    """

    def __init__(self, elapsed_s: float, attempts: int, message: str | None = None):
        self.elapsed_s = elapsed_s
        self.attempts = attempts
        if message is None:
            message = f"Timed out after waiting {elapsed_s:.3f} s over {attempts} attempt(s) for the database lock"
        super().__init__(message)


class MSQLiteMaxRetriesError(MSQLiteTimeoutError):
    """Raised when lock-acquire retries exceed ``retry_limit``.

    A subclass of :class:`MSQLiteTimeoutError`, so ``except MSQLiteTimeoutError`` catches both.
    """

    def __init__(self, elapsed_s: float, attempts: int, retry_limit: int, message: str | None = None):
        self.retry_limit = retry_limit
        if message is None:
            message = f"Exceeded maximum retries of {retry_limit} after waiting {elapsed_s:.3f} s over {attempts} attempt(s) for the database lock"
        super().__init__(elapsed_s, attempts, message)


# Mapping of Python types (or their string names) to SQLite storage classes, used
# to build ``CREATE TABLE`` column definitions. Both type objects and string names
# are accepted as keys so schemas can be expressed either way.
type_to_sqlite_type: dict[Any, str] = {
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


@dataclass
class Stats:
    """Counters accumulated over the lifetime of a :class:`Database` (or :class:`MSQLite`) instance.

    :ivar executions: statements executed (``execute``, ``executemany`` and ``executescript`` each count once)
    :ivar retries: lock attempts that failed (each is followed by a backoff and retry, or by the final error) across ``BEGIN``, statements and ``COMMIT``
    :ivar max_execution_s: longest single statement execution, in seconds
    :ivar max_wait_s: longest total lock wait within one transaction, in seconds
    """

    executions: int = 0
    retries: int = 0
    max_execution_s: float | None = None
    max_wait_s: float | None = None


@dataclass
class _WaitState:
    """Lock-wait bookkeeping for one transaction: shared by its ``BEGIN``, statements and ``COMMIT``."""

    waited_s: float = 0.0
    retries: int = 0


class _ConnectionSlot:
    """A connection owned by one thread, tagged with the pid that opened it (connections must not survive a fork)."""

    def __init__(self, conn: sqlite3.Connection, busy_timeout_ms: int):
        self.conn = conn
        self.pid = os.getpid()
        self.busy_timeout_ms = busy_timeout_ms


class Database:
    """
    Multi-table SQLite access with bounded, retried locking.

    * ``write()`` starts a ``BEGIN IMMEDIATE`` transaction (``mode="exclusive"`` for ``BEGIN EXCLUSIVE``).
      ``IMMEDIATE`` takes the write lock up front, which avoids the deferred-to-write upgrade
      deadlock and ``SQLITE_BUSY_SNAPSHOT`` under WAL.
    * ``read()`` starts a ``BEGIN DEFERRED`` transaction. Under WAL (the default) readers never wait
      on a writer and writers never wait on readers.
    * Lock errors (``SQLITE_BUSY``/``SQLITE_LOCKED``, detected by error code) are retried with
      jittered exponential backoff on ``BEGIN``, on each single ``execute`` and on ``COMMIT``,
      until ``timeout_s`` of total waiting or ``retry_limit`` retries. Statement retry re-executes
      only the failed statement; the transaction stays open, so earlier statements are kept.
    * With ``persistent=True`` one connection per thread is kept open and reused across transactions.
      Connections are never shared across threads or across a fork.

    Instances are context managers: leaving the ``with`` block calls :meth:`close`.
    """

    def __init__(
        self,
        db_path: Path | str,
        *,
        wal: bool = True,
        timeout_s: float | None = 30.0,
        retry_limit: int | None = None,
        retry_scale: float = 0.01,
        retry_cap_s: float = 1.0,
        busy_timeout_s: float = 5.0,
        pragmas: Sequence[str] = (),
        on_open: OnOpen | None = None,
        on_retry: OnRetry | None = None,
        row_factory: RowFactory | None = None,
        persistent: bool = True,
    ):
        """
        :param db_path: database file path
        :param wal: set ``PRAGMA journal_mode=WAL`` on every connection (keyword only). ``False`` leaves the
            file's journal mode untouched. WAL is persisted in the database file once set.
        :param timeout_s: maximum total time to wait for locks within one transaction before raising
            :class:`MSQLiteTimeoutError`; ``None`` waits forever (keyword only)
        :param retry_limit: maximum lock-wait retries within one transaction before raising
            :class:`MSQLiteMaxRetriesError`; ``None`` is unbounded (keyword only)
        :param retry_scale: base unit of the backoff sleep in seconds; the first retry sleeps a random
            time in ``[0, 2 * retry_scale)`` and the range doubles on each further retry (keyword only)
        :param retry_cap_s: upper bound of the backoff sleep range in seconds (keyword only)
        :param busy_timeout_s: SQLite's own busy timeout per attempt, absorbed inside the engine without a
            Python round trip; capped to the time remaining before ``timeout_s`` (keyword only)
        :param pragmas: extra ``PRAGMA`` settings applied to every new connection, e.g. ``("synchronous=FULL",)``
            (keyword only)
        :param on_open: called with each newly opened ``sqlite3.Connection`` after the pragmas are applied (keyword only)
        :param on_retry: called as ``on_retry(attempt, elapsed_s, error)`` before each backoff sleep (keyword only)
        :param row_factory: ``sqlite3`` row factory applied to every cursor, e.g. ``sqlite3.Row`` (keyword only)
        :param persistent: keep one connection per thread open across transactions; ``False`` opens and closes
            a connection per transaction (keyword only)
        """
        if retry_scale <= 0.0:
            raise ValueError("retry_scale must be positive")
        if retry_cap_s <= 0.0:
            raise ValueError("retry_cap_s must be positive")
        if timeout_s is not None and timeout_s < 0.0:
            raise ValueError("timeout_s must not be negative")
        if busy_timeout_s < 0.0:
            raise ValueError("busy_timeout_s must not be negative")
        self.db_path = Path(db_path)
        self.wal = wal
        self.timeout_s = timeout_s
        self.retry_limit = retry_limit
        self.retry_scale = retry_scale
        self.retry_cap_s = retry_cap_s
        self.busy_timeout_s = busy_timeout_s
        self.pragmas = tuple(_normalize_pragma(pragma) for pragma in pragmas)
        self.on_open = on_open
        self.on_retry = on_retry
        self.row_factory = row_factory
        self.persistent = persistent
        self._stats = Stats()
        self._stats_lock = threading.Lock()
        self._local = threading.local()
        self._slots: set[_ConnectionSlot] = set()
        self._slots_lock = threading.Lock()

    # ----------------------------------------------------------------- public API

    @property
    def stats(self) -> Stats:
        """Counters accumulated over the lifetime of this instance (see :class:`Stats`)."""
        return self._stats

    def write(self, mode: WriteMode = "immediate") -> "Transaction":
        """
        Return a context manager that runs a write transaction.

        :param mode: ``"immediate"`` (default) takes the write lock at ``BEGIN``; ``"exclusive"`` additionally
            blocks readers in rollback-journal mode (under WAL the two behave the same)
        """
        if mode not in ("immediate", "exclusive"):
            raise ValueError(f'mode must be "immediate" or "exclusive", got {mode!r}')
        return Transaction(self, mode)

    def read(self) -> "Transaction":
        """Return a context manager that runs a ``DEFERRED`` read transaction. Use :meth:`write` for anything that writes."""
        return Transaction(self, "deferred")

    def migrate(self, steps: Sequence[tuple[int, str]]) -> int:
        """
        Apply schema migrations keyed on ``PRAGMA user_version``.

        Each step is ``(version, sql)``; versions must be positive and strictly increasing. Steps whose
        version is at or below the database's current ``user_version`` are skipped. Each applied step runs
        as a script inside one write transaction, and ``user_version`` is set to the step's version.

        :param steps: sequence of ``(version, sql_script)``
        :return: the ``user_version`` after migration
        """
        previous = 0
        for version, _unused_sql in steps:
            if version <= previous:
                raise ValueError(f"migration versions must be positive and strictly increasing, got {version} after {previous}")
            previous = version
        with self.write() as tx:
            current = int(tx.execute("PRAGMA user_version").fetchone()[0])
            for version, sql in steps:
                if version <= current:
                    continue
                tx.executescript(sql)
                tx.execute(f"PRAGMA user_version = {int(version)}")
                current = version
                log.debug(f'migrated "{self.db_path}" to user_version {version}')
        return current

    def close(self) -> None:
        """Close every connection this instance has opened (in any thread). Safe to call more than once."""
        with self._slots_lock:
            slots = list(self._slots)
            self._slots.clear()
        for slot in slots:
            if slot.pid == os.getpid():
                slot.conn.close()
        self._local = threading.local()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        self.close()

    # ----------------------------------------------------------------- connections

    def _open_connection(self) -> _ConnectionSlot:
        """Open and configure a new connection. Lock errors propagate so the caller can retry."""
        busy_timeout_ms = int(self.busy_timeout_s * 1000.0)
        # check_same_thread=False so close() can release connections opened by other threads; per-thread use is
        # guaranteed by the threading.local slot, not by sqlite3's check.
        conn = sqlite3.connect(self.db_path, timeout=self.busy_timeout_s, autocommit=True, check_same_thread=False)
        configured = False
        try:
            if self.wal:
                conn.execute("PRAGMA journal_mode=WAL")
            for pragma in self.pragmas:
                conn.execute(f"PRAGMA {pragma}")
            if self.on_open is not None:
                self.on_open(conn)
            configured = True
        finally:
            if not configured:
                conn.close()
        return _ConnectionSlot(conn, busy_timeout_ms)

    def _acquire_slot(self) -> _ConnectionSlot:
        """Return this thread's connection, opening one if needed (or a fresh one when not persistent)."""
        if not self.persistent:
            return self._open_connection()
        slot: _ConnectionSlot | None = getattr(self._local, "slot", None)
        if slot is not None and slot.pid != os.getpid():
            # Inherited across a fork: the parent's connection must not be used (or closed) here.
            self._forget_slot(slot)
            slot = None
        if slot is None:
            slot = self._open_connection()
            self._local.slot = slot
            with self._slots_lock:
                self._slots.add(slot)
        return slot

    def _forget_slot(self, slot: _ConnectionSlot) -> None:
        self._local.slot = None
        with self._slots_lock:
            self._slots.discard(slot)

    def _release_slot(self, slot: _ConnectionSlot) -> None:
        """Give a connection back after a transaction: closed when not persistent, kept otherwise."""
        if not self.persistent:
            slot.conn.close()

    def _discard_slot(self, slot: _ConnectionSlot) -> None:
        """Close a connection that failed during ``BEGIN`` (only when not persistent; a persistent one is reused)."""
        if not self.persistent:
            slot.conn.close()

    def _apply_busy_timeout(self, slot: _ConnectionSlot, state: _WaitState) -> None:
        """Cap SQLite's busy timeout so a single attempt cannot wait past the remaining deadline."""
        wanted_ms = int(self.busy_timeout_s * 1000.0)
        if self.timeout_s is not None:
            remaining_ms = int(max(0.0, self.timeout_s - state.waited_s) * 1000.0)
            wanted_ms = min(wanted_ms, remaining_ms)
        if wanted_ms != slot.busy_timeout_ms:
            slot.conn.execute(f"PRAGMA busy_timeout = {wanted_ms}")
            slot.busy_timeout_ms = wanted_ms

    # ----------------------------------------------------------------- retry engine

    def _check_retry_budget(self, state: _WaitState) -> None:
        """Raise if the retry limit or deadline has been reached; called before every attempt."""
        # Every attempt so far has failed, so ``state.retries`` is also the number of attempts made.
        if self.retry_limit is not None and state.retries > self.retry_limit:
            raise MSQLiteMaxRetriesError(state.waited_s, state.retries, self.retry_limit)
        if self.timeout_s is not None and state.retries > 0 and state.waited_s >= self.timeout_s:
            raise MSQLiteTimeoutError(state.waited_s, state.retries)

    def _backoff(self, state: _WaitState, failed_attempt_s: float, error: sqlite3.OperationalError) -> None:
        """Account for a failed attempt, then sleep with jittered exponential backoff (bounded by the deadline)."""
        state.waited_s += failed_attempt_s
        state.retries += 1
        with self._stats_lock:
            self._stats.retries += 1
        self._check_retry_budget(state)
        sleep_s = random.random() * min(self.retry_scale * (2.0**state.retries), self.retry_cap_s)
        if self.timeout_s is not None:
            sleep_s = min(sleep_s, max(0.0, self.timeout_s - state.waited_s))
        if self.on_retry is not None:
            self.on_retry(state.retries, state.waited_s, error)
        log.debug(f'lock retry {state.retries} for "{self.db_path}" after {state.waited_s:.3f} s: {error}')
        time.sleep(sleep_s)
        state.waited_s += sleep_s

    def _record_wait(self, state: _WaitState) -> None:
        if state.retries == 0:
            return
        with self._stats_lock:
            if self._stats.max_wait_s is None or state.waited_s > self._stats.max_wait_s:
                self._stats.max_wait_s = state.waited_s

    def _record_execution(self, execution_s: float) -> None:
        with self._stats_lock:
            self._stats.executions += 1
            if self._stats.max_execution_s is None or execution_s > self._stats.max_execution_s:
                self._stats.max_execution_s = execution_s

    def _begin(self, statement: str, state: _WaitState) -> _ConnectionSlot:
        """Acquire a connection (opening one if needed) and run ``BEGIN ...``, retrying lock errors as one unit."""
        while True:
            self._check_retry_budget(state)
            attempt_start = time.monotonic()
            slot: _ConnectionSlot | None = None
            try:
                slot = self._acquire_slot()
                if self.persistent and slot.conn.in_transaction:
                    raise RuntimeError(f'a transaction is already active on this thread for "{self.db_path}"; nested transactions are not supported')
                self._apply_busy_timeout(slot, state)
                slot.conn.execute(statement)
                return slot
            except sqlite3.OperationalError as e:
                if slot is not None:
                    self._discard_slot(slot)
                if not _is_lock_error(e):
                    raise
                self._backoff(state, time.monotonic() - attempt_start, e)

    def _execute_with_retry(self, slot: _ConnectionSlot, target: sqlite3.Cursor | sqlite3.Connection, statement: str, parameters: Parameters | None, state: _WaitState) -> sqlite3.Cursor:
        """Run one statement inside an open transaction, retrying lock errors in place (the transaction stays open)."""
        while True:
            self._check_retry_budget(state)
            attempt_start = time.monotonic()
            try:
                if parameters is None:
                    return target.execute(statement)
                return target.execute(statement, parameters)
            except sqlite3.OperationalError as e:
                if not _is_lock_error(e) or _is_stale_snapshot_error(e):
                    raise
                self._backoff(state, time.monotonic() - attempt_start, e)
                self._apply_busy_timeout(slot, state)


def _normalize_pragma(pragma: str) -> str:
    """Accept ``"synchronous=FULL"`` or ``"PRAGMA synchronous=FULL"``; reject anything that could smuggle a second statement."""
    text = pragma.strip()
    if text.upper().startswith("PRAGMA "):
        text = text[len("PRAGMA ") :].strip()
    if not text or ";" in text:
        raise ValueError(f"Invalid pragma: {pragma!r}")
    return text


class Transaction:
    """
    One transaction on a :class:`Database`, created by :meth:`Database.write` or :meth:`Database.read`.

    ``__enter__`` runs ``BEGIN`` (retrying lock errors) and ``__exit__`` commits on clean exit or rolls
    back if an exception propagates. Statements run through :meth:`execute` are retried in place on
    lock errors; :meth:`executemany` and :meth:`executescript` are not retried because a partial run
    could not be re-applied safely (inside a write transaction the write lock is already held, so
    they do not hit lock errors in practice).
    """

    def __init__(self, database: Database, mode: Literal["immediate", "exclusive", "deferred"]):
        self.database = database
        self.mode = mode
        self.row_factory: RowFactory | None = database.row_factory
        self._slot: _ConnectionSlot | None = None
        self._state = _WaitState()
        self._cursor: sqlite3.Cursor | None = None
        # Every cursor handed out is closed on exit: an unconsumed SELECT would otherwise keep the connection's
        # file lock alive after ``close()`` until the cursor is garbage collected.
        self._cursors: weakref.WeakSet[sqlite3.Cursor] = weakref.WeakSet()

    # ----------------------------------------------------------------- context manager

    def __enter__(self) -> Self:
        if self._slot is not None:
            raise RuntimeError("Transaction is already active")
        self._state = _WaitState()
        try:
            self._slot = self.database._begin(f"BEGIN {self.mode.upper()}", self._state)
        finally:
            if self._slot is None:  # BEGIN gave up: __exit__ will not run, so record the wait here
                self.database._record_wait(self._state)
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        slot = self._slot
        if slot is None:
            log.warning(f'Transaction __exit__ without an active connection for "{self.database.db_path}"')
            return
        try:
            if exc_type is None:
                self._commit(slot)
            else:
                self._rollback(slot.conn)
        finally:
            self._close_cursors()
            self._slot = None
            self._cursor = None
            self.database._record_wait(self._state)
            self.database._release_slot(slot)

    def _close_cursors(self) -> None:
        for cursor in list(self._cursors):
            cursor.close()
        self._cursors.clear()

    def _commit(self, slot: _ConnectionSlot) -> None:
        """``COMMIT`` with lock retry. In rollback-journal mode a commit can be BUSY while readers hold shared locks."""
        committed = False
        try:
            self._close_cursors()  # a pending write statement would block COMMIT
            self.database._execute_with_retry(slot, slot.conn, "COMMIT", None, self._state)
            committed = True
        finally:
            if not committed:
                self._rollback(slot.conn)

    @staticmethod
    def _rollback(conn: sqlite3.Connection) -> None:
        if not conn.in_transaction:
            return
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error as e:
            # Never mask the exception that is already propagating out of the ``with`` block.
            log.error(f"ROLLBACK failed: {e}")

    # ----------------------------------------------------------------- statements

    def _active_slot(self) -> _ConnectionSlot:
        if self._slot is None:
            raise RuntimeError("Transaction is not active")
        return self._slot

    @property
    def connection(self) -> sqlite3.Connection:
        """The underlying ``sqlite3.Connection`` (only valid while the transaction is active)."""
        return self._active_slot().conn

    def _new_cursor(self) -> sqlite3.Cursor:
        cursor = self.connection.cursor()
        if self.row_factory is not None:
            cursor.row_factory = self.row_factory
        self._cursor = cursor
        self._cursors.add(cursor)
        return cursor

    def execute(self, statement: str, parameters: Parameters | None = None) -> sqlite3.Cursor:
        """
        Execute one SQL statement, retrying in place on lock errors.

        :param statement: SQL statement
        :param parameters: parameters for the statement (sequence or mapping, per ``sqlite3``)
        :return: the cursor, so ``tx.execute(...).fetchall()`` works
        """
        slot = self._active_slot()
        cursor = self._new_cursor()
        start = time.monotonic()
        self.database._execute_with_retry(slot, cursor, statement, parameters, self._state)
        self.database._record_execution(time.monotonic() - start)
        return cursor

    def executemany(self, statement: str, parameters: Iterable[Parameters]) -> sqlite3.Cursor:
        """Execute ``statement`` once per parameter set (not retried; see the class docstring)."""
        cursor = self._new_cursor()
        start = time.monotonic()
        cursor.executemany(statement, parameters)
        self.database._record_execution(time.monotonic() - start)
        return cursor

    def executescript(self, script: str) -> sqlite3.Cursor:
        """Execute a multi-statement script inside this transaction (not retried; see the class docstring)."""
        cursor = self._new_cursor()
        start = time.monotonic()
        cursor.executescript(script)
        self.database._record_execution(time.monotonic() - start)
        return cursor

    def fetchone(self) -> Any:
        """Fetch the next row from the most recent statement."""
        return self._last_cursor().fetchone()

    def fetchall(self) -> list[Any]:
        """Fetch all remaining rows from the most recent statement."""
        return self._last_cursor().fetchall()

    @property
    def lastrowid(self) -> int | None:
        """``lastrowid`` of the most recent statement."""
        return self._last_cursor().lastrowid

    @property
    def rowcount(self) -> int:
        """``rowcount`` of the most recent statement."""
        return self._last_cursor().rowcount

    def _last_cursor(self) -> sqlite3.Cursor:
        if self._cursor is None:
            raise RuntimeError("no statement has been executed in this transaction")
        return self._cursor


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
        self.table_name = _validate_identifier(table_name)
        self.schema = schema
        if indexes is not None:
            for index in indexes:
                _validate_identifier(index)
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
        warnings.warn("MSQLite.max_execution_time is deprecated; use MSQLite.stats.max_execution_s", DeprecationWarning, stacklevel=2)
        return self.database.stats.max_execution_s

    @property
    def execution_count(self) -> int:
        warnings.warn("MSQLite.execution_count is deprecated; use MSQLite.stats.executions", DeprecationWarning, stacklevel=2)
        return self.database.stats.executions

    @property
    def retry_count(self) -> int:
        warnings.warn("MSQLite.retry_count is deprecated; use MSQLite.stats.retries", DeprecationWarning, stacklevel=2)
        return self.database.stats.retries

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

    def create_table(self) -> None:
        """Create the configured table and any indexes using the schema passed to ``__init__``.

        Idempotent — uses ``CREATE TABLE IF NOT EXISTS`` and ``CREATE INDEX IF NOT EXISTS``.
        """
        if self._tx is None:
            raise RuntimeError("create_table called without an active connection")
        if self.schema is None:
            raise MSQLiteNoSchemaException(self.table_name)
        columns = ",".join(_convert_column_spec_to_sqlite(spec, col_type) for spec, col_type in self.schema.items())
        self._tx.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name}({columns})")
        if self.indexes:
            for index in self.indexes:
                self._tx.execute(f"CREATE INDEX IF NOT EXISTS {self.table_name}_{index}_idx ON {self.table_name}({index})")

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
        if self._tx is None:
            raise RuntimeError("execute called without an active connection")
        if self.artificial_delay is not None:
            time.sleep(self.artificial_delay)  # only for testing
        try:
            return self._tx.execute(statement, parameters)
        except sqlite3.OperationalError as e:
            if "no such table" not in str(e).lower():
                raise
            self.create_table()
            return self._tx.execute(statement, parameters)
