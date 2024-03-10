from pathlib import Path
from multiprocessing import Pool
import time

from msqlite import MSQLite
from msqlite.msqlite import MAX_BACKOFF


def get_temp_dir() -> Path:
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


table_name = "stuff"


def test_msqlite_single_thread():
    db_path = Path(get_temp_dir(), "test_msqlite.sqlite")
    db_path.unlink(missing_ok=True)
    db = MSQLite(db_path)
    # create table
    db.execute(f"CREATE TABLE {table_name}(name, color, year)")
    # insert
    db.execute(f"INSERT INTO {table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
    _response = db.execute(f"SELECT * FROM {table_name}")
    response = list(_response)
    assert response == [('plate', 'brown', 2020), ('chair', 'black', 2019)]
    # update table
    db.execute(f"UPDATE {table_name} SET color='red' WHERE name='plate'")
    _response = db.execute(f"SELECT * FROM {table_name}")
    response = list(_response)
    assert response == [('plate', 'red', 2020), ('chair', 'black', 2019)]
    max_execution_time = max(db.execution_times)
    print(f"{max_execution_time=}")
    assert max_execution_time < 1.0  # 0.016998291015625 has been observed


def test_msqlite_single_thread_execute_multiple():
    db_path = Path(get_temp_dir(), "test_msqlite_execute_multiple.sqlite")
    db_path.unlink(missing_ok=True)
    db = MSQLite(db_path)
    db.execute(f"CREATE TABLE {table_name}(name, color, year)")
    statements = []
    statements.append(f"INSERT INTO {table_name} VALUES ('plate', 'brown', 2020)")
    statements.append(f"INSERT INTO {table_name} VALUES ('chair', 'black', 2019)")
    db.execute_multiple(statements)
    _response = db.execute(f"SELECT * FROM {table_name}")
    response = list(_response)
    assert response == [('plate', 'brown', 2020), ('chair', 'black', 2019)]


mp_db_path = Path(get_temp_dir(), "test_msqlite_multi_process.sqlite")


def _write_db(value: int):
    db = MSQLite(mp_db_path)
    db.set_artificial_delay(MAX_BACKOFF)  # delay so we'll get some retries (just for testing)
    db.execute(f"INSERT INTO {table_name} VALUES ({value}, {time.time()})")
    if (retry_count := db.retry_count) > 0:
        print(f"{value=}:{retry_count=}", flush=True)
    return db.retry_count


def test_msqlite_multi_process():
    mp_db_path.unlink(missing_ok=True)
    db = MSQLite(mp_db_path)
    db.execute(f"CREATE TABLE {table_name}(thing INTEGER PRIMARY KEY, ts NUMERIC)")
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
