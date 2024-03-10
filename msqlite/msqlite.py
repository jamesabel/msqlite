import sqlite3
from logging import getLogger
import time
import random
from pathlib import Path

log = getLogger(__name__)

MAX_TRIES = 10000
MAX_BACKOFF = 0.1  # seconds


class MSQLiteMaxRetriesError(sqlite3.OperationalError):
    ...


class MSQLite:
    """
    A wrapper around sqlite3 that handles multithreading and multiprocessing.
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def execute(self, *args, **kwargs):
        """
        Execute a command on a sqlite3 database, with an auto-commit and a retry mechanism to handle multiple threads/processes.
        """

        conn = sqlite3.connect(self.db_path, isolation_level="EXCLUSIVE")
        cursor = conn.cursor()
        conn.execute("BEGIN EXCLUSIVE TRANSACTION")  # lock the database

        count = 0
        new_cursor = None
        while new_cursor is None and count <= MAX_TRIES:
            count += 1
            try:
                new_cursor = cursor.execute(*args, **kwargs)
                conn.commit()
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e):
                    conn.rollback()
                    new_cursor = None
                    time.sleep(random.random() * MAX_BACKOFF)
                else:
                    log.warning(e)
        if new_cursor is None:
            raise MSQLiteMaxRetriesError(f"Database is locked after {count} tries")
        return new_cursor
