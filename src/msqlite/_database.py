"""The general-purpose layer: :class:`Database` and the :class:`Transaction` objects it hands out.

``Database`` holds the configuration and wires together the three collaborators that do the work: a
:class:`~._retry.LockRetrier` (bounded, retried lock acquisition), a :class:`~._connections.ConnectionManager`
(one connection per thread, fork-safe) and a :class:`~._stats.StatsCollector`. ``Transaction`` is the context
manager around ``BEGIN``/``COMMIT``/``ROLLBACK`` and the cursors used inside it.
"""

import sqlite3
import time
import weakref
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from types import TracebackType
from typing import Any, Self

from ._connections import ConnectionManager, ConnectionSlot
from ._log import log
from ._retry import LockRetrier, RetryPolicy, WaitState
from ._stats import Stats, StatsCollector
from ._types import OnOpen, OnRetry, Parameters, RowFactory, TransactionMode, WriteMode


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
    * With ``persistent=True`` one connection per thread is kept open and reused across transactions, and
      closed when that thread ends. Connections are never shared across threads or across a fork.

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
        :param persistent: keep one connection per thread open across transactions, closing it when the thread
            ends; ``False`` opens and closes a connection per transaction (keyword only)
        """
        self.db_path = Path(db_path)
        self.row_factory = row_factory
        self._retry_policy = RetryPolicy(timeout_s=timeout_s, retry_limit=retry_limit, retry_scale=retry_scale, retry_cap_s=retry_cap_s, busy_timeout_s=busy_timeout_s)
        self._stats = StatsCollector()
        self._retrier = LockRetrier(self._retry_policy, self._stats, on_retry, str(self.db_path))
        self._connections = ConnectionManager(self.db_path, wal=wal, pragmas=pragmas, on_open=on_open, busy_timeout_s=busy_timeout_s, persistent=persistent)

    # ----------------------------------------------------------------- configuration (read-only)

    @property
    def wal(self) -> bool:
        """Whether new connections switch the file to WAL."""
        return self._connections.wal

    @property
    def persistent(self) -> bool:
        """Whether each thread keeps one connection across transactions."""
        return self._connections.persistent

    @property
    def timeout_s(self) -> float | None:
        """Maximum total lock wait per transaction; ``None`` waits forever."""
        return self._retry_policy.timeout_s

    @property
    def retry_limit(self) -> int | None:
        """Maximum lock-wait retries per transaction; ``None`` is unbounded."""
        return self._retry_policy.retry_limit

    @property
    def retry_scale(self) -> float:
        """Base unit of the backoff sleep in seconds."""
        return self._retry_policy.retry_scale

    @property
    def retry_cap_s(self) -> float:
        """Upper bound of one backoff sleep in seconds."""
        return self._retry_policy.retry_cap_s

    @property
    def busy_timeout_s(self) -> float:
        """SQLite's own busy timeout per attempt in seconds."""
        return self._retry_policy.busy_timeout_s

    @property
    def stats(self) -> Stats:
        """Counters accumulated over the lifetime of this instance (see :class:`Stats`)."""
        return self._stats.stats

    # ----------------------------------------------------------------- public API

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
        self._connections.close_all()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> None:
        self.close()

    # ----------------------------------------------------------------- retried operations (used by Transaction)

    def _begin(self, mode: TransactionMode, state: WaitState) -> ConnectionSlot:
        """Acquire a connection and run ``BEGIN <mode>`` as one retried unit, since opening a connection can hit a lock error too."""
        statement = f"BEGIN {mode.upper()}"

        def attempt() -> ConnectionSlot:
            slot = self._connections.acquire()
            try:
                if slot.conn.in_transaction:
                    raise RuntimeError(f'a transaction is already active on this thread for "{self.db_path}"; nested transactions are not supported')
                slot.set_busy_timeout(self._retry_policy.busy_timeout_ms(state))
                slot.conn.execute(statement)
            except sqlite3.OperationalError:
                self._connections.release(slot)  # a non-persistent connection is closed; a persistent one is reused
                raise
            return slot

        return self._retrier.run(state, attempt)

    def _execute(self, slot: ConnectionSlot, target: sqlite3.Cursor | sqlite3.Connection, statement: str, parameters: Parameters | None, state: WaitState) -> sqlite3.Cursor:
        """Run one statement on ``target`` inside an open transaction, retrying lock errors in place (the transaction stays open)."""

        def attempt() -> sqlite3.Cursor:
            slot.set_busy_timeout(self._retry_policy.busy_timeout_ms(state))
            if parameters is None:
                return target.execute(statement)
            return target.execute(statement, parameters)

        return self._retrier.run(state, attempt)


class Transaction:
    """
    One transaction on a :class:`Database`, created by :meth:`Database.write` or :meth:`Database.read`.

    ``__enter__`` runs ``BEGIN`` (retrying lock errors) and ``__exit__`` commits on clean exit or rolls
    back if an exception propagates. Statements run through :meth:`execute` are retried in place on
    lock errors; :meth:`executemany` and :meth:`executescript` are not retried because a partial run
    could not be re-applied safely (inside a write transaction the write lock is already held, so
    they do not hit lock errors in practice).
    """

    def __init__(self, database: Database, mode: TransactionMode):
        self.database = database
        self.mode = mode
        self.row_factory: RowFactory | None = database.row_factory
        self._slot: ConnectionSlot | None = None
        self._state = WaitState()
        self._cursor: sqlite3.Cursor | None = None
        # Every cursor handed out is closed on exit: an unconsumed SELECT would otherwise keep the connection's
        # file lock alive after ``close()`` until the cursor is garbage collected.
        self._cursors: weakref.WeakSet[sqlite3.Cursor] = weakref.WeakSet()

    # ----------------------------------------------------------------- context manager

    def __enter__(self) -> Self:
        if self._slot is not None:
            raise RuntimeError("Transaction is already active")
        self._state = WaitState()
        try:
            self._slot = self.database._begin(self.mode, self._state)
        finally:
            if self._slot is None:  # BEGIN gave up: __exit__ will not run, so record the wait here
                self._record_wait()
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
            self._record_wait()
            self.database._connections.release(slot)

    def _record_wait(self) -> None:
        if self._state.retries > 0:
            self.database._stats.record_wait(self._state.waited_s)

    def _close_cursors(self) -> None:
        for cursor in list(self._cursors):
            cursor.close()
        self._cursors.clear()

    def _commit(self, slot: ConnectionSlot) -> None:
        """``COMMIT`` with lock retry; rolled back if it finally fails. In rollback-journal mode a commit can be BUSY while readers hold shared locks."""
        committed = False
        try:
            self._close_cursors()  # a pending write statement would block COMMIT
            self.database._execute(slot, slot.conn, "COMMIT", None, self._state)
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

    def _active_slot(self) -> ConnectionSlot:
        if self._slot is None:
            raise RuntimeError("Transaction is not active")
        return self._slot

    @property
    def connection(self) -> sqlite3.Connection:
        """The underlying ``sqlite3.Connection`` (only valid while the transaction is active)."""
        return self._active_slot().conn

    def _run(self, operation: Callable[[sqlite3.Cursor], object]) -> sqlite3.Cursor:
        """Run ``operation`` on a fresh cursor, timing and counting it; that cursor is then the one ``fetch*`` reads from."""
        cursor = self.connection.cursor()
        if self.row_factory is not None:
            cursor.row_factory = self.row_factory
        self._cursor = cursor
        self._cursors.add(cursor)
        start = time.monotonic()
        operation(cursor)
        self.database._stats.record_execution(time.monotonic() - start)
        return cursor

    def execute(self, statement: str, parameters: Parameters | None = None) -> sqlite3.Cursor:
        """
        Execute one SQL statement, retrying in place on lock errors.

        :param statement: SQL statement
        :param parameters: parameters for the statement (sequence or mapping, per ``sqlite3``)
        :return: the cursor, so ``tx.execute(...).fetchall()`` works
        """
        slot = self._active_slot()
        return self._run(lambda cursor: self.database._execute(slot, cursor, statement, parameters, self._state))

    def executemany(self, statement: str, parameters: Iterable[Parameters]) -> sqlite3.Cursor:
        """Execute ``statement`` once per parameter set (not retried; see the class docstring)."""
        return self._run(lambda cursor: cursor.executemany(statement, parameters))

    def executescript(self, script: str) -> sqlite3.Cursor:
        """Execute a multi-statement script inside this transaction (not retried; see the class docstring)."""
        return self._run(lambda cursor: cursor.executescript(script))

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
