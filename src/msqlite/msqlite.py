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
    """Validate that a string is a safe SQL identifier (table name, column name, etc.)."""
    if not _valid_identifier_re.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


class MSQLiteNoSchemaException(Exception):
    def __init__(self, table_name: str):
        self.table_name = table_name
        super().__init__(f'No schema provided for table "{table_name}"')


class MSQLiteMaxRetriesError(sqlite3.OperationalError):
    pass


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
    Convert a column specification to a SQLite column specification.
    :param column_spec: column name and optional constraints. Example: "id PRIMARY KEY"
    :param column_type: column type (Python types such as int, float, str, etc.). Example: int
    :return: column specification string for SQLite
    """

    # manually check that column_type can be a Type or a json object (mypy doesn't like json as a type and beartype doesn't allow Dict for a json object)
    if not (isinstance(column_type, type) or column_type is json):
        raise TypeError(f"column_type must be a type or json, got {column_type!r}")

    column_spec_parts = column_spec.split()
    if len(column_spec_parts) == 0:
        raise ValueError("column_spec must not be empty")
    column_name = column_spec_parts[0]
    _validate_identifier(column_name)
    if (column_type_string := type_to_sqlite_type.get(column_type)) is None:
        raise ValueError(f"{column_type} (type={type(column_type)}) is not a supported SQLite column type (see msqlite.type_to_sqlite_type for supported types)")
    if len(column_spec_parts) > 1:
        constraints = column_spec_parts[1:]
    else:
        constraints = []
    spec_components = [column_name, column_type_string]
    spec_components.extend(constraints)
    spec = " ".join(spec_components)
    return spec


class MSQLite:
    """
    A context manager around sqlite3 access that handles multithreading and multiprocessing. Also, automatically creates a table if it does not exist.
    """

    def __init__(self, db_path: Path, table_name: str, schema: dict[str, type] | None = None, indexes: list[str] | None = None, *, retry_scale: float = 0.01, retry_limit: int | None = None):
        """
        :param db_path: database file path
        :param table_name: table name
        :param schema: dictionary of column names and types. Example: {"id PRIMARY KEY": int, "name": str, "color": str, "year": int}
        :param indexes: list of column names to create indexes on
        :param retry_scale: scale factor for retrying to connect to the database (1.0 is an average of 1 second) (keyword only parameter)
        :param retry_limit: maximum number of retries to connect to the database (keyword only parameter)
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
        self.max_execution_time = None  # type: float | None
        self.execution_count = 0
        self.retry_count = 0
        self.artificial_delay = None  # type: float | None
        self.conn = None  # type: sqlite3.Connection | None

    def __enter__(self):
        while self.conn is None:
            if self.retry_limit is not None and self.retry_count > self.retry_limit:
                raise MSQLiteMaxRetriesError(f"Exceeded maximum retries of {self.retry_limit}")
            try:
                self.conn = sqlite3.connect(self.db_path, isolation_level="EXCLUSIVE")
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")  # lock the database
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    if self.conn is not None:
                        self.conn.close()
                    self.retry_count += 1
                    self.conn = None
                    time.sleep(self.retry_scale * 2.0 * random.random())
                else:
                    # some other exception
                    raise
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
        """
        Create a table with the schema provided in the constructor.
        """
        if self.conn is None:
            raise RuntimeError("create_table called without an active connection")
        cursor = self.conn.cursor()
        if self.schema is None:
            raise MSQLiteNoSchemaException(self.table_name)
        else:
            # create table (if the table does not already exist)
            columns = ",".join([_convert_column_spec_to_sqlite(column_spec, column_type) for column_spec, column_type in self.schema.items()])
            statement = f"CREATE TABLE IF NOT EXISTS {self.table_name}({columns})"
            cursor.execute(statement)

            if self.indexes is not None:
                for index in self.indexes:
                    statement = f"CREATE INDEX IF NOT EXISTS {self.table_name}_{index}_idx ON {self.table_name}({index})"
                    cursor.execute(statement)

    def set_artificial_delay(self, delay: float):
        """
        Set an artificial delay for testing purposes to keep the DB file locked for a period of time. Useful for testing, but not to be used in normal operation.
        :param delay: delay in seconds
        """
        self.artificial_delay = delay

    def execute(self, statement: str, parameters: Mapping | Sequence | None = None) -> sqlite3.Cursor:
        """
        Execute statements on a sqlite3 database, with an auto-commit and a retry mechanism to handle multiple threads/processes.

        :param statement: SQL statement to execute
        :param parameters: parameters for the SQL statement
        :return: sqlite3.Cursor after execute statement
        """

        start = time.time()
        if self.conn is None:
            raise RuntimeError("execute called without an active connection")
        if self.artificial_delay is not None:
            time.sleep(self.artificial_delay)  # only for testing
        cursor = self.conn.cursor()
        try:
            if parameters is None:
                new_cursor = cursor.execute(statement)
            else:
                new_cursor = cursor.execute(statement, parameters)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e).lower():
                # tried an operation but the table does not exist, so create the table and try again
                self.create_table()
                if parameters is None:
                    new_cursor = cursor.execute(statement)
                else:
                    new_cursor = cursor.execute(statement, parameters)
            else:
                raise
        execution_time = time.time() - start
        self.execution_count += 1
        if self.max_execution_time is None or execution_time > self.max_execution_time:
            self.max_execution_time = execution_time
        return new_cursor
