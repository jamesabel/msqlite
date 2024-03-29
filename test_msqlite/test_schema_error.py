from pathlib import Path

import pytest

from test_msqlite.paths import get_temp_dir

from src.msqlite import MSQLite

table_name = "stuff"


def test_schema_error():
    db_path = Path(get_temp_dir(), "test_schema_error.sqlite")
    db_path.unlink(missing_ok=True)
    schema = {"name": str, "number": complex}  # SQLite doesn't support complex
    with pytest.raises(ValueError):
        with MSQLite(db_path, table_name, schema) as db:
            db.execute(f"INSERT INTO {table_name} VALUES ('name', 42)")
