"""All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note."""

import pytest

from msqlite import MSQLite
from test_msqlite.paths import get_temp_dir


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_deprecated_counters_still_work():
    db_path = get_temp_dir() / "test_deprecations.sqlite"
    db_path.unlink(missing_ok=True)
    with MSQLite(db_path, "t", {"x": int}) as db:
        db.execute("INSERT INTO t VALUES (1)")
        with pytest.deprecated_call():
            assert db.execution_count == db.stats.executions
        with pytest.deprecated_call():
            assert db.retry_count == db.stats.retries
        with pytest.deprecated_call():
            assert db.max_execution_time == db.stats.max_execution_s
