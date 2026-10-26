"""Compatibility module: ``from msqlite.msqlite import ...`` keeps working.

The implementation lives in private submodules (``_database``, ``_single_table``, ``_retry``, ``_connections``,
``_stats``, ``_sql``, ``_errors``). Import the public API from the ``msqlite`` package instead.
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
