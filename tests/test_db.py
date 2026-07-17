from datetime import UTC, datetime

from github_innovation.db import Warehouse


def test_schema_and_upsert(tmp_path):
    with Warehouse(tmp_path / "test.duckdb") as db:
        db.initialize()
        row = {
            "org_id": 1,
            "login": "example",
            "name": "Old",
            "collected_at": datetime.now(UTC),
        }
        db.upsert("organization", row, ["org_id"])
        db.upsert("organization", {**row, "name": "New"}, ["org_id"])
        assert db.connection.execute("SELECT name FROM organization").fetchone()[0] == "New"


def test_collection_run_records_failure(tmp_path):
    with Warehouse(tmp_path / "test.duckdb") as db:
        db.initialize()
        try:
            with db.run("test", "example"):
                raise ValueError("boom")
        except ValueError:
            pass
        assert db.connection.execute("SELECT status FROM collection_run").fetchone()[0] == "failed"
