from pathlib import Path
from pprint import pprint

import json

from src.msqlite import MSQLite

from test_msqlite.paths import get_temp_dir

table_name = "stuff"


def test_msqlite_json():
    expected_results = [("Jenny", '{"name": "Jenny", "phone": "867-5309", "year": 1981}'), ("Wilson", '{"name": "Wilson", "phone": "634-5789", "year": 1966}')]

    db_path = Path(get_temp_dir(), "test_msqlite_json.sqlite")
    db_path.unlink(missing_ok=True)
    schema = {"name": str, "data": json}
    datum = [{"name": "Jenny", "phone": "867-5309", "year": 1981}, {"name": "Wilson", "phone": "634-5789", "year": 1966}]
    with MSQLite(db_path, table_name, schema) as db:
        for d in datum:
            d_json = json.dumps(d)
            name = d["name"]
            insert_string = f"INSERT INTO {table_name} VALUES ('{name}', '{d_json}')"
            print(insert_string)
            db.execute(insert_string)
        _response = db.execute(f"SELECT * FROM {table_name}")
        response = list(_response)
        pprint(response)
        assert response == expected_results
