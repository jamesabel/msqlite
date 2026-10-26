"""Type aliases shared across the package."""

import sqlite3
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Literal

WriteMode = Literal["immediate", "exclusive"]
"""Lock taken at ``BEGIN`` by a write transaction (see :meth:`Database.write`)."""

TransactionMode = Literal["immediate", "exclusive", "deferred"]
"""Any transaction mode: the write modes plus ``deferred`` for :meth:`Database.read`."""

Parameters = Mapping[str, Any] | Sequence[Any]
"""Statement parameters as ``sqlite3`` accepts them: a sequence for ``?`` or a mapping for ``:name``."""

RowFactory = Callable[[sqlite3.Cursor, tuple[Any, ...]], Any]
"""A ``sqlite3`` row factory, e.g. ``sqlite3.Row``."""

OnOpen = Callable[[sqlite3.Connection], None]
"""Called with each newly opened connection after the pragmas are applied."""

OnRetry = Callable[[int, float, sqlite3.OperationalError], None]
"""Called as ``on_retry(attempt, elapsed_s, error)`` before each backoff sleep."""
