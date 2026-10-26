"""The exceptions msqlite raises, and the classification of ``sqlite3`` errors the retry engine depends on."""

import sqlite3


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


# Primary SQLite result codes that mean "another connection holds a lock; try again later".
LOCK_ERROR_CODES = frozenset({sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})


def _result_code(exc: sqlite3.OperationalError) -> int | None:
    """The extended SQLite result code, or ``None`` for an error the engine did not raise.

    Only errors raised by the ``sqlite3`` C layer carry ``sqlite_errorcode``; one constructed in Python
    (``sqlite3.OperationalError("database is locked")``) has no such attribute at all.
    """
    code: int | None = getattr(exc, "sqlite_errorcode", None)
    return code


def is_lock_error(exc: sqlite3.OperationalError) -> bool:
    """True if ``exc`` is ``SQLITE_BUSY`` or ``SQLITE_LOCKED`` (including their extended codes)."""
    code = _result_code(exc)
    if code is None:
        return "database is locked" in str(exc).lower()
    return (code & 0xFF) in LOCK_ERROR_CODES


def is_stale_snapshot_error(exc: sqlite3.OperationalError) -> bool:
    """True for ``SQLITE_BUSY_SNAPSHOT``: a read transaction tried to write after another writer committed.

    Retrying the statement can never succeed inside the same transaction, so it is not retried.
    """
    return _result_code(exc) == sqlite3.SQLITE_BUSY_SNAPSHOT


def is_missing_table_error(exc: sqlite3.OperationalError) -> bool:
    """True when a statement failed because its table does not exist.

    SQLite reports this as the generic ``SQLITE_ERROR``, so the message text is the only signal.
    """
    return "no such table" in str(exc).lower()
