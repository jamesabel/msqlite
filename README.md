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
    table_name = "example"
    schema = {"id PRIMARY KEY": int, "name": str, "color": str, "year": int}
    db_path = Path("temp", "example.sqlite")
    db_path.parent.mkdir(exist_ok=True)

    # Write and read data.
    with MSQLite(db_path, table_name, schema) as db:
        now = time.monotonic_ns()  # some index value
        # insert some data
        db.execute(f"INSERT INTO {table_name} VALUES ({now}, 'plate', 'red', 2020), ({now + 1}, 'chair', 'green', 2019)")
        # read the data back out
        response = db.execute(f"SELECT * FROM {table_name}")
        for row in response:
            print(row)

    # Read data out. No longer needs the schema.
    with MSQLite(db_path, table_name) as db:
        response = db.execute(f"SELECT * FROM {table_name}")
        for row in response:
            print(row)
```
