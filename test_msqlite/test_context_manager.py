from msqlite import MSQLite

from test_msqlite.paths import get_temp_dir


def test_context_manager():
    db_path = get_temp_dir() / "test_context_manager.sqlite"

    db_path.unlink(missing_ok=True)
    schema = {"name": str, "color": str, "year": int}
    table_name = "test_context_manager"

    # DB doesn't exist
    with MSQLite(db_path, table_name, schema, retry_limit=5) as db:
        # insert something into the database
        db.execute(f"INSERT INTO {db.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
        db.execute(f"SELECT * FROM {db.table_name}")

    # test that the context manager cleans up and closes the DB
    with MSQLite(db_path, table_name, schema, retry_limit=5) as db:
        # insert something into the database
        db.execute(f"INSERT INTO {db.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
        db.execute(f"SELECT * FROM {db.table_name}")
