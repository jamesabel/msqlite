import time
from pathlib import Path

from src.msqlite import MSQLite


def test_example():
    db_path = Path("temp", "example.sqlite")
    db_path.parent.mkdir(exist_ok=True)
    db = MSQLite(db_path)
    # crate DB file in case it doesn't already exist
    db.execute(f"CREATE TABLE IF NOT EXISTS stuff(id INTEGER PRIMARY KEY, name, color, year)")
    now = time.monotonic_ns()  # some index value
    # insert some data
    db.execute(f"INSERT INTO stuff VALUES ({now}, 'plate', 'red', 2020), ({now + 1}, 'chair', 'green', 2019)")
    # read the data back out
    response = db.execute(f"SELECT * FROM stuff")
    for row in response:
        print(row)
