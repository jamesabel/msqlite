import pytest

from src.msqlite import MSQLite, MSQLiteMaxRetriesError

from test_msqlite.paths import get_temp_dir


def test_max_retries():
    with pytest.raises(MSQLiteMaxRetriesError):
        with MSQLite(get_temp_dir(), "test_max_retries", retry_limit=-1) as db:
            db.execute(f"SELECT * FROM {db.table_name}")
