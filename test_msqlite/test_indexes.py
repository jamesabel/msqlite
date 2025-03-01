from pathlib import Path

from msqlite import MSQLite
from test_msqlite.paths import get_temp_dir


db_path = Path(get_temp_dir(), "test_indexes.sqlite")
table_name = "test"


class DBWithIndexes(MSQLite):

    def __init__(self):
        schema = {"name": str, "color": str, "year": int}
        super().__init__(db_path, "test", schema, indexes=["name"])


def test_indexes():
    db_path.unlink(missing_ok=True)
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
        max_execution_time = max(db.execution_times)
        print(f"{max_execution_time=}")
        assert max_execution_time < 1.0  # 0.0007305145263671875 has been observed
