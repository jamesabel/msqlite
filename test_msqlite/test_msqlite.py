import sqlite3
from pathlib import Path
from multiprocessing import Pool
import time

from msqlite import MSQLite
from msqlite.msqlite import MAX_BACKOFF


def get_temp_dir() -> Path:
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


def test_msqlite_single_thread():
    print(f"{sqlite3.threadsafety=}")

    db_path = Path(get_temp_dir(), "test_msqlite_db")
    db_path.unlink(missing_ok=True)
    db = MSQLite(db_path)
    db.execute("CREATE TABLE stuff(name, color, year)")
    db.execute("INSERT INTO stuff VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
    response = db.execute("SELECT * FROM stuff")
    for row in response:
        print(row)
    db.execute("UPDATE stuff SET color='red' WHERE name='table'")
    response = db.execute("SELECT * FROM stuff")
    for row in response:
        print(row)
    print(f"{db.retry_count=}")
    print(f"{db.execution_times=}")


mp_db_path = Path(get_temp_dir(), "test_msqlite_multi_process.db")


def _access_db(value: int):
    db = MSQLite(mp_db_path)
    db.set_artificial_delay(MAX_BACKOFF)  # delay so we'll get some retries
    db.execute(f"INSERT INTO stuff VALUES ({value}, {time.time()})")
    if (retry_count := db.retry_count) > 0:
        print(f"{value=}:{retry_count=}")
    return db.retry_count


def test_msqllite_multi_thread():
    print(f"{sqlite3.threadsafety=}")

    mp_db_path.unlink(missing_ok=True)
    db = MSQLite(mp_db_path)
    db.execute("CREATE TABLE stuff(thing INTEGER PRIMARY KEY, ts NUMERIC)")
    processes = 1000  # enough so that we'll have at least a few retries
    with Pool() as pool:
        results = pool.map(_access_db, range(processes))
        retries = sum(results)
        assert retries > 10
