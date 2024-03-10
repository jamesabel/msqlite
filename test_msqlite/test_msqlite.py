import sqlite3
from pathlib import Path

from msqlite import MSQLite


def get_temp_dir() -> Path:
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    return temp_dir


def test_msqlite_single_thread():
    print(f"{sqlite3.threadsafety=}")

    db_path = Path(get_temp_dir(), "test_msqlite_single_thread.db")
    db_path.unlink(missing_ok=True)
    db = MSQLite(db_path)
    db.execute("CREATE TABLE stuff(name, color, year)")
    db.execute("INSERT INTO stuff VALUES ('table', 'brown', 2020), ('chair', 'black', 2019)")
    response = db.execute("SELECT * FROM stuff")
    for row in response:
        print(row)
    db.execute("UPDATE stuff SET color='red' WHERE name='table'")
    response = db.execute("SELECT * FROM stuff")
    for row in response:
        print(row)


def test_msqllite_multi_thread():
    print(f"{sqlite3.threadsafety=}")

    db_path = Path(get_temp_dir(), "test_msqlite_multi_thread.db")
    db_path.unlink(missing_ok=True)
    db = MSQLite(db_path)
    db.execute("CREATE TABLE stuff(name, color, year)")
    db.execute("INSERT INTO stuff VALUES ('table', 'brown', 2020), ('chair', 'black', 2019)")
    response = db.execute("SELECT * FROM stuff")
    for row in response:
        print(row)
    db.execute("UPDATE stuff SET color='red' WHERE name='table'")
    response = db.execute("SELECT * FROM stuff")
    for row in response:
        print(row)