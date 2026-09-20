"""All tests in this module are AI-generated (Claude Code); its private helpers and constants are covered by this note."""

import sqlite3

import pytest

from msqlite import Database
from test_msqlite.paths import get_temp_dir

STEPS = [
    (1, "CREATE TABLE things(id INTEGER PRIMARY KEY, name TEXT);"),
    (2, "ALTER TABLE things ADD COLUMN color TEXT; CREATE INDEX things_color ON things(color);"),
]


def _fresh_db_path(name: str):
    db_path = get_temp_dir() / f"{name}.sqlite"
    for suffix in ("", "-wal", "-shm"):
        (db_path.parent / (db_path.name + suffix)).unlink(missing_ok=True)
    return db_path


def _columns(db: Database, table: str) -> list[str]:
    with db.read() as tx:
        return [row[1] for row in tx.execute(f"PRAGMA table_info({table})").fetchall()]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_migrate():
    db_path = _fresh_db_path("test_migrate")
    with Database(db_path) as db:
        assert db.migrate(STEPS) == 2
        assert _columns(db, "things") == ["id", "name", "color"]
        assert db.migrate(STEPS) == 2  # idempotent: nothing to do
        with db.read() as tx:
            assert tx.execute("PRAGMA user_version").fetchone() == (2,)
        assert db.migrate([*STEPS, (3, "ALTER TABLE things ADD COLUMN year INTEGER;")]) == 3
        assert _columns(db, "things") == ["id", "name", "color", "year"]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_migrate_partial_start():
    db_path = _fresh_db_path("test_migrate_partial")
    with Database(db_path) as db:
        assert db.migrate(STEPS[:1]) == 1
        assert db.migrate(STEPS) == 2
        assert _columns(db, "things") == ["id", "name", "color"]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_migrate_failed_step_is_rolled_back():
    db_path = _fresh_db_path("test_migrate_failed")
    with Database(db_path) as db:
        assert db.migrate(STEPS[:1]) == 1
        with pytest.raises(sqlite3.OperationalError):
            db.migrate([*STEPS, (3, "CREATE TABLE ok(x); THIS IS NOT SQL;")])
        with db.read() as tx:
            assert tx.execute("PRAGMA user_version").fetchone() == (1,)  # steps 2 and 3 ran in one transaction that rolled back
            tables = {row[0] for row in tx.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert tables == {"things"}
        assert _columns(db, "things") == ["id", "name"]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_migrate_rejects_bad_versions():
    db_path = _fresh_db_path("test_migrate_bad_versions")
    with Database(db_path) as db:
        with pytest.raises(ValueError):
            db.migrate([(0, "SELECT 1;")])
        with pytest.raises(ValueError):
            db.migrate([(2, "SELECT 1;"), (1, "SELECT 1;")])
        with pytest.raises(ValueError):
            db.migrate([(1, "SELECT 1;"), (1, "SELECT 1;")])
        assert db.migrate([]) == 0
