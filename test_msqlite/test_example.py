import time
from pathlib import Path

from src.msqlite import MSQLite


def test_example():
    """
    This is an example of how to use MSQLite.
    """
    table_name = "example"
    columns = {"id PRIMARY KEY": int, "name": str, "color": str, "year": int}
    db_path = Path("temp", "example.sqlite")
    db_path.parent.mkdir(exist_ok=True)

    # Write and read data.
    with MSQLite(db_path, table_name, columns) as db:
        now = time.monotonic_ns()  # some index value
        # insert some data
        db.execute(f"INSERT INTO {table_name} VALUES ({now}, 'plate', 'red', 2020), ({now + 1}, 'chair', 'green', 2019)")
        # read the data back out
        response = db.execute(f"SELECT * FROM {table_name}")
        for row in response:
            print(row)

    # Read data out. No longer needs the columns.
    with MSQLite(db_path, table_name) as db:
        response = db.execute(f"SELECT * FROM {table_name}")
        for row in response:
            print(row)
