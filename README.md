# msqlite

Multi-threaded/multi-process support on top of SQLite. The intent is to ensure a SQL statement
will get executed, even if other threads or processes are trying to access the DB. Avoids 
`database is locked` issues. 

No additional package dependencies beyond regular Python.

Intended for relatively simple SQL statement execution. Locks the DB file on every access, with 
built-in retry mechanism.

Even though the DB is locked on every access, typically simple writes are much less than 1 second
(more like 1 mS), so this latency is still usable for many use cases.

```python
import time
from pathlib import Path

from msqlite import MSQLite

    db_path = Path("temp", "example.sqlite")
    db_path.parent.mkdir(exist_ok=True)
    with MSQLite(db_path) as db:
        # create DB file in case it doesn't already exist
        db.execute(f"CREATE TABLE IF NOT EXISTS stuff(id INTEGER PRIMARY KEY, name, color, year)")
        now = time.monotonic_ns()  # some index value
        # insert some data
        db.execute(f"INSERT INTO stuff VALUES ({now}, 'plate', 'red', 2020), ({now + 1}, 'chair', 'green', 2019)")
        # read the data back out
        response = db.execute(f"SELECT * FROM stuff")
        for row in response:
            print(row)
```
