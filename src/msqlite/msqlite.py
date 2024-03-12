import sqlite3
from logging import getLogger
import time
import random
from pathlib import Path
from typing import Type

log = getLogger()


class MSQLiteMaxRetriesError(sqlite3.OperationalError):
    ...


type_to_sqlite_type = {
    int: "INTEGER",
    float: "REAL",
    str: "TEXT",
    bytes: "BLOB",
    bool: "INTEGER",
    type(None): "NULL",
}


def _convert_column_dict_to_sqlite(column_spec: str, column_type: Type) -> str:
    column_spec_parts = column_spec.split()
    assert len(column_spec_parts) > 0  # at least the column name
    column_name = column_spec_parts[0]
    column_type = type_to_sqlite_type[column_type]
    if len(column_spec_parts) > 1:
        constraints = column_spec_parts[1:]
    else:
        constraints = []
    spec_components = [column_name, column_type]
    spec_components.extend(constraints)
    spec = " ".join(spec_components)
    return spec


class MSQLite:
    """
    A wrapper around sqlite3 that handles multithreading and multiprocessing.
    """

    def __init__(self, db_path: Path, table_name: str | None = None, table_columns: dict[str, Type] = None):
        """
        :param db_path: database file path
        """
        self.db_path = db_path
        self.table_name = table_name
        if table_columns is None:
            self.table_columns = None
        else:
            self.table_columns = table_columns
        self.execution_times = []
        self.retry_count = 0
        self.artificial_delay = None
        self.conn = None  # type: sqlite3.Connection | None
        self.cursor = None  # type: sqlite3.Cursor | None

    def __enter__(self):
        while self.conn is None:
            try:
                self.conn = sqlite3.connect(self.db_path, isolation_level="EXCLUSIVE")
                self.cursor = self.conn.cursor()
                if self.table_columns is not None:
                    columns = ",".join([_convert_column_dict_to_sqlite(column_spec, column_type) for column_spec, column_type in self.table_columns.items()])
                    statement = f"CREATE TABLE IF NOT EXISTS {self.table_name}({columns})"
                    self.cursor.execute(statement)
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")  # lock the database
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    self.conn.rollback()
                    self.retry_count += 1
                    self.conn = None
                    time.sleep(random.random())
                else:
                    # some other exception
                    raise
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.conn.commit()
        if self.conn is not None:
            self.conn.close()
        if len(self.execution_times) > 0:
            max_execution_time = max(self.execution_times)
        else:
            max_execution_time = None
        log.info(f"{max_execution_time=}")
        log.info(f"{self.retry_count=}")

    def set_artificial_delay(self, delay: float):
        """
        Set an artificial delay for testing purposes to keep the DB file locked for a period of time. Useful for testing, but not to be used in normal operation.
        :param delay: delay in seconds
        """
        self.artificial_delay = delay

    def execute(self, statement: str) -> sqlite3.Cursor:
        """
        Execute statements on a sqlite3 database, with an auto-commit and a retry mechanism to handle multiple threads/processes.

        :param statement: SQL statement to execute
        :return: sqlite3.Cursor
        """

        start = time.time()
        assert self.conn is not None
        new_cursor = self.cursor.execute(statement)
        if self.artificial_delay is not None:
            time.sleep(self.artificial_delay)  # only for testing
        self.execution_times.append(time.time() - start)
        return new_cursor


