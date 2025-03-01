from pathlib import Path
import time

from msqlite import MSQLite

from test_msqlite.paths import get_temp_dir


def test_artificial_delay():

    table_name = "example"
    schema = {"id PRIMARY KEY": int, "name": str, "color": str, "year": int}
    db_path = Path(get_temp_dir(), "test_artificial_delay.sqlite")
    db_path.parent.mkdir(exist_ok=True)

    delay = 10.0
    start = time.time()

    # Write and read data.
    with MSQLite(db_path, table_name, schema) as db:
        db.set_artificial_delay(delay)
        now = time.monotonic_ns()  # some index value
        db.execute(f"INSERT INTO {table_name} VALUES ({now}, 'plate', 'red', 2020), ({now + 1}, 'chair', 'green', 2019)")
        db.execute(f"SELECT * FROM {table_name}")

    end = time.time()

    assert end - start > delay
