"""SQL text helpers: identifier and pragma validation, and the DDL behind :class:`MSQLite`'s auto-created table.

Everything here is pure string handling with no database access, so it is unit-testable on its own.
"""

import json
import re
from collections.abc import Mapping
from typing import Any

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PRAGMA_PREFIX_RE = re.compile(r"^PRAGMA\b\s*", re.IGNORECASE)

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


def validate_identifier(name: str) -> str:
    """Return ``name`` unchanged if it is a safe SQL identifier (letters, digits, underscores); otherwise raise ``ValueError``.

    Table, column and index names are interpolated into SQL text, so this is what keeps them from smuggling in statements.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def normalize_pragma(pragma: str) -> str:
    """Accept ``"synchronous=FULL"`` or ``"PRAGMA synchronous=FULL"`` and return the part after ``PRAGMA``.

    A ``;`` is rejected because the text is run as ``PRAGMA <text>`` and must stay a single statement.
    """
    text = _PRAGMA_PREFIX_RE.sub("", pragma.strip())
    if not text or ";" in text:
        raise ValueError(f"Invalid pragma: {pragma!r}")
    return text


def column_definition(column_spec: str, column_type: Any) -> str:
    """
    Build a single SQLite column definition from a column spec and Python type.

    :param column_spec: column name followed by optional constraints (e.g. ``"id PRIMARY KEY"``)
    :param column_type: Python type (``int``, ``float``, ``str``, ``bytes``, ``bool``, ``NoneType``)
        or the ``json`` module
    :return: a SQLite column definition, e.g. ``"id INTEGER PRIMARY KEY"``
    """
    # ``json`` is a module, not a type: accept it explicitly since ``isinstance(json, type)`` is False.
    if not (isinstance(column_type, type) or column_type is json):
        raise TypeError(f"column_type must be a type or json, got {column_type!r}")
    parts = column_spec.split()
    if not parts:
        raise ValueError("column_spec must not be empty")
    column_name = validate_identifier(parts[0])
    sqlite_type = type_to_sqlite_type.get(column_type)
    if sqlite_type is None:
        raise ValueError(f"{column_type} (type={type(column_type)}) is not a supported SQLite column type (see msqlite.type_to_sqlite_type for supported types)")
    return " ".join([column_name, sqlite_type, *parts[1:]])


def create_table_sql(table_name: str, schema: Mapping[str, Any]) -> str:
    """``CREATE TABLE IF NOT EXISTS`` for ``table_name`` from a schema dict (column spec -> Python type)."""
    columns = ",".join(column_definition(spec, column_type) for spec, column_type in schema.items())
    return f"CREATE TABLE IF NOT EXISTS {table_name}({columns})"


def create_index_sql(table_name: str, column: str) -> str:
    """``CREATE INDEX IF NOT EXISTS`` on one column, named ``<table>_<column>_idx``."""
    return f"CREATE INDEX IF NOT EXISTS {table_name}_{column}_idx ON {table_name}({column})"
