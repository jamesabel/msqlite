from pathlib import Path

from msqlite import MSQLite
from test_msqlite.paths import get_temp_dir


table_name = "test"


def _get_db_path():
    return Path(get_temp_dir(), "test_indexes.sqlite")


class DBWithIndexes(MSQLite):

    def __init__(self):
        schema = {"name": str, "color": str, "year": int}
        super().__init__(_get_db_path(), "test", schema, indexes=["name"])


def test_indexes():
    _get_db_path().unlink(missing_ok=True)
    with DBWithIndexes() as db:
        # insert
        db.execute(f"INSERT INTO {table_name} VALUES ('plate', 'brown', 2020), ('chair', 'black', 2019)")
        _response = db.execute(f"SELECT * FROM {table_name}")
        response = list(_response)
        assert response == [("plate", "brown", 2020), ("chair", "black", 2019)]
        # update table
        db.execute(f"UPDATE {table_name} SET color='red' WHERE name='plate'")
        _response = db.execute(f"SELECT * FROM {table_name}")
        response = list(_response)
        assert response == [("plate", "red", 2020), ("chair", "black", 2019)]
        print(f"{db.max_execution_time=}")
        assert db.max_execution_time is not None and db.max_execution_time < 1.0  # 0.0007305145263671875 has been observed
