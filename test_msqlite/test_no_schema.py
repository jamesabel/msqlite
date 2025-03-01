import pytest

from msqlite import MSQLite, MSQLiteNoSchemaException

from test_msqlite.paths import get_temp_dir

db_path = get_temp_dir() / "test_no_schema.sqlite"


def test_no_schema():
    with pytest.raises(MSQLiteNoSchemaException):
        with MSQLite(db_path, "test_no_schema") as db:
            # try to insert something into the database (it won't work)
            db.execute(f"INSERT INTO {db.table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
            db.execute(f"SELECT * FROM {db.table_name}")
