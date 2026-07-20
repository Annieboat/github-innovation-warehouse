from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb


def utcnow() -> datetime:
    return datetime.now(UTC)


class Warehouse:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(str(self.path))

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Warehouse:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def initialize(self) -> None:
        schema = Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
        self.connection.execute(schema)

    @contextmanager
    def run(self, source: str, target: str) -> Iterator[int]:
        run_id = self.connection.execute(
            "INSERT INTO collection_run (source, target) VALUES (?, ?) RETURNING run_id",
            [source, target],
        ).fetchone()[0]
        try:
            yield run_id
        except Exception as exc:
            self.connection.execute(
                "UPDATE collection_run SET finished_at=?, status='failed', error_message=? WHERE run_id=?",
                [utcnow(), str(exc)[:4000], run_id],
            )
            raise
        else:
            self.connection.execute(
                "UPDATE collection_run SET finished_at=?, status='succeeded' WHERE run_id=?",
                [utcnow(), run_id],
            )

    def upsert(self, table: str, row: dict[str, Any], keys: Sequence[str]) -> None:
        if not row:
            return
        row = {key: self._adapt(value) for key, value in row.items()}
        columns = list(row)
        placeholders = ", ".join("?" for _ in columns)
        key_sql = ", ".join(keys)
        updates = [column for column in columns if column not in keys]
        if updates:
            conflict = "DO UPDATE SET " + ", ".join(
                f"{column}=excluded.{column}" for column in updates
            )
        else:
            conflict = "DO NOTHING"
        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({key_sql}) {conflict}"
        )
        self.connection.execute(sql, list(row.values()))

    def insert_ignore(self, table: str, row: dict[str, Any]) -> None:
        row = {key: self._adapt(value) for key, value in row.items()}
        columns = list(row)
        placeholders = ", ".join("?" for _ in columns)
        self.connection.execute(
            f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            list(row.values()),
        )

    def upsert_many(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]], keys: Sequence[str]
    ) -> None:
        """Bulk upsert already-normalized rows using one prepared statement."""
        if not rows:
            return
        placeholders = ", ".join("?" for _ in columns)
        updates = [column for column in columns if column not in keys]
        conflict = "DO UPDATE SET " + ", ".join(
            f"{column}=excluded.{column}" for column in updates
        )
        sql = (
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
            f"ON CONFLICT ({', '.join(keys)}) {conflict}"
        )
        self.connection.executemany(sql, rows)

    @staticmethod
    def _adapt(value: Any) -> Any:
        if isinstance(value, dict | list):
            return json.dumps(value, ensure_ascii=False)
        return value
