import pytest

from msqlite import MSQLite, MSQLiteMaxRetriesError

from test_msqlite.paths import get_temp_dir


def test_max_retries_value_error():
    db_path = get_temp_dir() / "test_max_retries_value_error.sqlite"
    db_path.unlink(missing_ok=True)
    with pytest.raises(MSQLiteMaxRetriesError):
        with MSQLite(db_path, "test_max_retries_value_error", retry_limit=-1) as db:
            # insert something into the database
            db.execute(f"INSERT INTO {db.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
            db.execute(f"SELECT * FROM {db.table_name}")


def test_db_already_locked():
    db_path = get_temp_dir() / "test_db_already_locked.sqlite"
    db_path.unlink(missing_ok=True)
    schema = {"name": str, "color": str, "year": int}
    table_name = "test_db_already_locked"
    with pytest.raises(MSQLiteMaxRetriesError):
        with MSQLite(db_path, table_name, schema, retry_limit=2) as db_a:
            db_a.execute(f"INSERT INTO {db_a.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
            # won't work since DB already locked
            with MSQLite(db_path, table_name, schema, retry_limit=2) as db_b:
                db_b.execute(f"INSERT INTO {db_b.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")


def test_retry_limit():

    db_path = get_temp_dir() / "test_retry_limit.sqlite"

    db_path.unlink(missing_ok=True)
    schema = {"name": str, "color": str, "year": int}
    table_name = "test_retry_limit"

    # DB doesn't exist
    with MSQLite(db_path, table_name, schema, retry_limit=5) as db:
        # insert something into the database
        db.execute(f"INSERT INTO {db.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
        db.execute(f"SELECT * FROM {db.table_name}")

    # try again when the DB exists
    with MSQLite(db_path, table_name, schema, retry_limit=5) as db:
        # insert something into the database
        db.execute(f"INSERT INTO {db.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
        db.execute(f"SELECT * FROM {db.table_name}")
