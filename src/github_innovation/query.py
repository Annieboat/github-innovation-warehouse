from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import duckdb
from rich.console import Console
from rich.table import Table

READ_ONLY_PREFIXES = ("select", "with", "describe", "show", "summarize", "explain")


def ensure_read_only(sql: str) -> None:
    normalized = sql.strip().lower()
    if not normalized.startswith(READ_ONLY_PREFIXES):
        raise ValueError("Query tool accepts read-only SQL only")
    forbidden = (" insert ", " update ", " delete ", " drop ", " alter ", " create ", " copy ")
    padded = f" {normalized} "
    if any(token in padded for token in forbidden):
        raise ValueError("Potentially mutating SQL is not allowed")


def execute_query(database: Path, sql: str) -> tuple[list[str], list[tuple[Any, ...]]]:
    ensure_read_only(sql)
    connection = duckdb.connect(str(database), read_only=True)
    try:
        cursor = connection.execute(sql)
        columns = [item[0] for item in cursor.description]
        return columns, cursor.fetchall()
    finally:
        connection.close()


def print_table(columns: list[str], rows: list[tuple[Any, ...]], console: Console) -> None:
    table = Table(show_lines=False)
    for column in columns:
        table.add_column(column)
    for row in rows:
        table.add_row(*(str(value) if value is not None else "" for value in row))
    console.print(table)


def write_csv(path: Path, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)
