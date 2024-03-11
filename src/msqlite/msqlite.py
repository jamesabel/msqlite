import sqlite3
from logging import getLogger
import time
import random
from pathlib import Path

log = getLogger()

MAX_TRIES = 200
BACKOFF = 0.1  # seconds


class MSQLiteMaxRetriesError(sqlite3.OperationalError):
    ...


class MSQLite:
    """
    A wrapper around sqlite3 that handles multithreading and multiprocessing.
    """

    def __init__(self, db_path: Path):
        """
        :param db_path: database file path
        """
        self.db_path = db_path
        self.execution_times = []
        self.retry_count = 0
        self.backoff = BACKOFF
        self.artificial_delay = None
        self.conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
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
        Set an artificial delay for testing purposes. Is not normally used.
        :param delay: delay in seconds
        """
        self.artificial_delay = delay

    def execute_multiple(self, statements: list[str]) -> sqlite3.Cursor:
        """
        Execute statements on a sqlite3 database, with an auto-commit and a retry mechanism to handle multiple threads/processes.

        :param statements: list of SQL statements
        :return: sqlite3.Cursor
        """

        start = time.time()
        self.conn = sqlite3.connect(self.db_path, isolation_level="EXCLUSIVE")
        cursor = self.conn.cursor()

        count = 0
        new_cursor = None
        while new_cursor is None:
            count += 1
            try:
                self.conn.execute("BEGIN EXCLUSIVE TRANSACTION")  # lock the database
                for statement in statements:
                    new_cursor = cursor.execute(statement)
                if self.artificial_delay is not None:
                    time.sleep(self.artificial_delay)
                self.conn.commit()
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    self.conn.rollback()
                    if count >= MAX_TRIES:
                        duration = time.time() - start
                        raise MSQLiteMaxRetriesError(f"Database is still locked after {count} tries,{duration=}")
                    new_cursor = None
                    self.retry_count += 1
                    sleep_time = random.random() * self.backoff
                    self.backoff *= 1.01  # increase the backoff a little each time we encounter a locked database
                    log.info(f"Database is locked, retrying {count}/{MAX_TRIES},{sleep_time=}")
                    time.sleep(sleep_time)
                else:
                    # something other than locked database
                    raise
        self.execution_times.append(time.time() - start)
        return new_cursor

    def execute(self, statement: str) -> sqlite3.Cursor:
        """
        Execute a statement on a sqlite3 database, with an auto-commit and a retry mechanism to handle multiple threads/processes.

        :param statement: SQL statement
        :return: sqlite3.Cursor from last statement
        """

        return self.execute_multiple([statement])
