from pathlib import Path
from multiprocessing import Pool
import time

from src.msqlite import MSQLite
from test_msqlite.paths import get_temp_dir

table_name = "stuff"


def test_msqlite_single_thread():
    db_path = Path(get_temp_dir(), "test_msqlite.sqlite")
    db_path.unlink(missing_ok=True)
    schema = {"name": str, "color": str, "year": int}
    with MSQLite(db_path, table_name, schema) as db:
        # insert
        db.execute(f"INSERT INTO {table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
        _response = db.execute(f"SELECT * FROM {table_name}")
        response = list(_response)
        assert response == [("plate", "brown", 2020), ("chair", "black", 2019)]
        # update table
        db.execute(f"UPDATE {table_name} SET color='red' WHERE name='plate'")
        _response = db.execute(f"SELECT * FROM {table_name}")
        response = list(_response)
        assert response == [("plate", "red", 2020), ("chair", "black", 2019)]
        max_execution_time = max(db.execution_times)
        print(f"{max_execution_time=}")
        assert max_execution_time < 1.0  # 0.016998291015625 has been observed


def test_msqlite_single_thread_execute_multiple():
    db_path = Path(get_temp_dir(), "test_msqlite_execute_multiple.sqlite")
    db_path.unlink(missing_ok=True)
    schema = {"name": str, "color": str, "ts UNIQUE": int}
    with MSQLite(db_path, table_name, schema) as db:
        time_a = int(round(time.time()))
        db.execute(f"INSERT INTO {table_name} VALUES ('plate', 'brown', {time_a})")
        time.sleep(2)
        time_b = int(round(time.time()))
        db.execute(f"INSERT INTO {table_name} VALUES ('chair', 'black', {time_b})")
        _response = db.execute(f"SELECT * FROM {table_name}")
        response = list(_response)
        assert response == [("plate", "brown", time_a), ("chair", "black", time_b)]


def test_msqlite_do_nothing():
    db_path = Path(get_temp_dir(), "test_msqlite_do_nothing")
    db_path.unlink(missing_ok=True)
    schema = {"name": str, "color": str, "year": int}
    with MSQLite(db_path, table_name, schema) as db:
        # make sure we can exit the context manager without doing anything
        assert len(db.execution_times) == 0


mp_db_path = Path(get_temp_dir(), "test_msqlite_multi_process.sqlite")


def _write_db(value: int):
    schema = {"value": int, "timestamp": float}
    with MSQLite(mp_db_path, table_name, schema) as db:
        db.set_artificial_delay(0.1)  # delay so we'll get some retries (just for testing)
        db.execute(f"INSERT INTO {table_name} VALUES ({value}, {time.time()})")
        if (retry_count := db.retry_count) > 0:
            print(f"{value=}:{retry_count=},{db.execution_times=}", flush=True)
        retry_count = db.retry_count
    return retry_count


def test_msqlite_multi_process():
    mp_db_path.unlink(missing_ok=True)
    processes = 200  # enough to have at least several retries
    with Pool(16) as pool:
        print(f"{pool._processes=}")
        results = pool.map(_write_db, range(processes))
        processes_with_retries = len([r for r in results if r > 0])
        overall_retries = sum(results)
        max_retries = max(results)
        print(f"{overall_retries=},{max_retries=},{processes_with_retries=}")
        assert overall_retries > 5  # 27 has been observed after 24 seconds
        assert processes_with_retries > 2  # 16 has been observed
        assert max_retries > 1  # should be enough processes so that at least one has to retry at least twice
