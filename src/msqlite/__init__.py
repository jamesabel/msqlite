"""msqlite: SQLite access that is safe across threads and processes.

Two layers share one retry implementation:

* :class:`Database`, a general-purpose concurrency layer for any multi-table database.
  ``db.write()`` opens an ``IMMEDIATE`` (or ``EXCLUSIVE``) transaction and ``db.read()`` a
  ``DEFERRED`` one. Lock waits are bounded by a wall-clock deadline and a retry limit, the
  journal is WAL by default so readers never wait on a writer, and one connection per thread
  is reused across transactions.
* :class:`MSQLite`, the original single-table helper. It opens a fresh connection per
  context, holds an ``EXCLUSIVE`` transaction for the lifetime of the ``with`` block, and
  auto-creates its table from a ``schema`` dict.

Log records go to the ``msqlite`` logger.
"""

from ._database import Database, Transaction
from ._errors import MSQLiteMaxRetriesError, MSQLiteNoSchemaException, MSQLiteTimeoutError
from ._single_table import MSQLite
from ._sql import type_to_sqlite_type
from ._stats import Stats
from ._types import WriteMode

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
