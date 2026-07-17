from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from .config import Settings, load_pipeline_config
from .db import Warehouse
from .gharchive import GHArchiveCollector
from .github import GitHubClient
from .metrics import metrics_sql
from .pipeline import GitHubPipeline
from .query import execute_query, print_table, write_csv

app = typer.Typer(no_args_is_help=True, help="GitHub innovation data warehouse CLI")
console = Console()


@app.command("init-db")
def init_db(
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Create or migrate the local DuckDB warehouse."""
    path = database or Settings().database
    with Warehouse(path) as warehouse:
        warehouse.initialize()
    console.print(f"[green]Initialized[/green] {path}")


@app.command()
def collect(
    config: Annotated[Path, typer.Option("--config", "-c")] = Path("config/targets.example.yml"),
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Collect repositories, commits, issues, pull requests, and setup.py metadata."""
    settings = Settings()
    pipeline_config = load_pipeline_config(config)
    with (
        Warehouse(database or settings.database) as warehouse,
        GitHubClient(settings.github_token, settings.user_agent, settings.request_timeout) as api,
    ):
        GitHubPipeline(api, warehouse, console).collect(pipeline_config)
    console.print("[green]Collection complete[/green]")


@app.command("search-repositories")
def search_repositories(
    search_query: Annotated[
        str, typer.Option("--query", "-q", help="GitHub repository search syntax")
    ],
    limit: Annotated[int, typer.Option("--limit", min=1, max=1000)] = 100,
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Discover repositories with GitHub Search API and persist the ranked hit list."""
    settings = Settings()
    observed_at = datetime.now().astimezone()
    with (
        Warehouse(database or settings.database) as warehouse,
        GitHubClient(settings.github_token, settings.user_agent, settings.request_timeout) as api,
    ):
        warehouse.initialize()
        count = 0
        with warehouse.run("github_search_api", search_query):
            for rank, repo in enumerate(api.search_repositories(search_query, limit), start=1):
                warehouse.insert_ignore(
                    "repository_search_hit",
                    {
                        "query": search_query,
                        "repo_id": repo["id"],
                        "rank": rank,
                        "full_name": repo["full_name"],
                        "owner_login": (repo.get("owner") or {}).get("login"),
                        "owner_type": (repo.get("owner") or {}).get("type"),
                        "matched_at": observed_at,
                        "raw": repo,
                    },
                )
                count += 1
    console.print(f"[green]Stored[/green] {count} search hits")


@app.command("collect-gharchive")
def collect_gharchive(
    org: Annotated[str, typer.Option("--org")],
    start: Annotated[datetime, typer.Option("--start", help="Inclusive ISO timestamp")],
    end: Annotated[datetime, typer.Option("--end", help="Exclusive ISO timestamp")],
    keep_raw: Annotated[bool, typer.Option("--keep-raw")] = False,
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Stream selected public events from hourly GH Archive files."""
    settings = Settings()
    with Warehouse(database or settings.database) as warehouse:
        count = GHArchiveCollector(warehouse, settings.raw_dir, console=console).collect(
            org, start, end, keep_raw
        )
    console.print(f"[green]Stored[/green] {count} matching events")


@app.command()
def query(
    sql: Annotated[str | None, typer.Option("--sql", "-s")] = None,
    file: Annotated[Path | None, typer.Option("--file", "-f")] = None,
    output: Annotated[Path | None, typer.Option("--output", "-o")] = None,
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Run read-only DuckDB SQL and optionally export CSV."""
    if bool(sql) == bool(file):
        raise typer.BadParameter("Provide exactly one of --sql or --file")
    statement = sql if sql is not None else file.read_text(encoding="utf-8")
    columns, rows = execute_query(database or Settings().database, statement)
    if output:
        write_csv(output, columns, rows)
        console.print(f"[green]Wrote[/green] {len(rows)} rows to {output}")
    else:
        print_table(columns, rows, console)


@app.command()
def metrics(
    org: Annotated[str, typer.Option("--org")],
    cutoff: Annotated[str, typer.Option("--cutoff", help="ISO fundraising cutoff")],
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Calculate cumulative pre-cutoff innovation and signalling proxies."""
    columns, rows = execute_query(database or Settings().database, metrics_sql(org, cutoff))
    print_table(columns, rows, console)


if __name__ == "__main__":
    app()
