from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from .bulk import BigQueryMonthlyCollector, OrganizationJSONExporter, YearMonth
from .config import Settings, load_pipeline_config
from .db import Warehouse
from .gharchive import GHArchiveCollector
from .github import GitHubClient
from .metrics import metrics_sql
from .pipeline import GitHubPipeline
from .query import execute_query, print_table, write_csv
from .targeted import (
    DEFAULT_EXPORT_SHARDS,
    GCSJsonSink,
    LocalJsonSink,
    TargetedBigQueryPipeline,
    TargetOrganizationJSONExporter,
    parse_shard_spec,
    plan_throughput,
    split_shards,
)

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


@app.command("collect-org-monthly")
def collect_org_monthly(
    project: Annotated[
        str | None, typer.Option("--project", help="Google Cloud billing/project ID")
    ] = None,
    start_month: Annotated[str, typer.Option("--start-month")] = "2015-01",
    end_month: Annotated[str, typer.Option("--end-month")] = "2025-12",
    location: Annotated[str, typer.Option("--location")] = "US",
    maximum_bytes_billed: Annotated[
        int | None, typer.Option("--maximum-bytes-billed", min=1)
    ] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Aggregate all public organization activity with GH Archive BigQuery tables."""
    settings = Settings()
    billing_project = project or settings.gcp_project
    if not billing_project:
        raise typer.BadParameter("Provide --project or set GCP_PROJECT in .env")
    start, end = YearMonth.parse(start_month), YearMonth.parse(end_month)
    with Warehouse(database or settings.database) as warehouse:
        rows, bytes_processed = BigQueryMonthlyCollector(billing_project, warehouse).collect(
            start,
            end,
            location=location,
            maximum_bytes_billed=maximum_bytes_billed,
            dry_run=dry_run,
        )
    gib = (bytes_processed or 0) / (1024**3)
    if dry_run:
        console.print(f"[yellow]Dry run[/yellow]: estimated {gib:,.2f} GiB processed")
    else:
        console.print(f"[green]Stored[/green] {rows:,} organization-month rows")
        console.print(f"BigQuery processed {gib:,.2f} GiB")


@app.command("export-org-json")
def export_org_json(
    start_month: Annotated[str, typer.Option("--start-month")] = "2015-01",
    end_month: Annotated[str, typer.Option("--end-month")] = "2025-12",
    output_dir: Annotated[Path | None, typer.Option("--output-dir", "-o")] = None,
    database: Annotated[Path | None, typer.Option("--database", "-d")] = None,
) -> None:
    """Write one zero-filled monthly JSON file for each observed organization."""
    settings = Settings()
    start, end = YearMonth.parse(start_month), YearMonth.parse(end_month)
    with Warehouse(database or settings.database) as warehouse:
        warehouse.initialize()
        manifest = OrganizationJSONExporter(warehouse, output_dir or settings.org_json_dir).export(
            start, end
        )
    console.print(
        f"[green]Wrote[/green] {manifest['organization_count']:,} organization JSON files"
    )


@app.command("load-org-targets")
def load_org_targets(
    csv_path: Annotated[Path, typer.Option("--csv", exists=True, dir_okay=False)],
    project: Annotated[
        str | None, typer.Option("--project", help="Google Cloud billing/project ID")
    ] = None,
    dataset: Annotated[str | None, typer.Option("--dataset")] = None,
    table: Annotated[str, typer.Option("--table")] = "organization_targets",
    export_shards: Annotated[int, typer.Option("--export-shards", min=1)] = DEFAULT_EXPORT_SHARDS,
    location: Annotated[str, typer.Option("--location")] = "US",
) -> None:
    """Load a large organization-ID CSV into a deduplicated BigQuery target table."""
    settings = Settings()
    billing_project = project or settings.gcp_project
    if not billing_project:
        raise typer.BadParameter("Provide --project or set GCP_PROJECT in .env")
    pipeline = TargetedBigQueryPipeline(
        billing_project, dataset or settings.target_dataset, location
    )
    stats = pipeline.load_targets(csv_path, table=table, export_shards=export_shards)
    console.print(f"[green]Loaded[/green] {stats.rows:,} valid target rows")
    console.print("BigQuery removes duplicate organization IDs in the final target table")


@app.command("collect-target-org-monthly")
def collect_target_org_monthly(
    project: Annotated[
        str | None, typer.Option("--project", help="Google Cloud billing/project ID")
    ] = None,
    dataset: Annotated[str | None, typer.Option("--dataset")] = None,
    targets_table: Annotated[str, typer.Option("--targets-table")] = "organization_targets",
    output_table: Annotated[
        str, typer.Option("--output-table")
    ] = "organization_target_monthly_sparse",
    start_month: Annotated[str, typer.Option("--start-month")] = "2015-01",
    end_month: Annotated[str, typer.Option("--end-month")] = "2025-12",
    workers: Annotated[int, typer.Option("--workers", min=1, max=12)] = 4,
    resume: Annotated[bool, typer.Option("--resume/--no-resume")] = True,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    maximum_bytes_billed: Annotated[
        int | None, typer.Option("--maximum-bytes-billed", min=1)
    ] = None,
    location: Annotated[str, typer.Option("--location")] = "US",
) -> None:
    """Scan GH Archive once by year and aggregate only the supplied organization IDs."""
    settings = Settings()
    billing_project = project or settings.gcp_project
    if not billing_project:
        raise typer.BadParameter("Provide --project or set GCP_PROJECT in .env")
    pipeline = TargetedBigQueryPipeline(
        billing_project, dataset or settings.target_dataset, location
    )
    result = pipeline.collect_monthly(
        YearMonth.parse(start_month),
        YearMonth.parse(end_month),
        targets_table=targets_table,
        output_table=output_table,
        workers=workers,
        resume=resume,
        dry_run=dry_run,
        maximum_bytes_billed=maximum_bytes_billed,
    )
    gib = result.bytes_processed / (1024**3)
    if dry_run:
        console.print(f"[yellow]Dry run[/yellow]: estimated {gib:,.2f} GiB processed")
    else:
        console.print(
            f"[green]Created[/green] {result.yearly_tables_created} yearly aggregates; "
            f"skipped {result.yearly_tables_skipped} completed years"
        )
        console.print(f"BigQuery processed {gib:,.2f} GiB")


@app.command("export-target-org-json")
def export_target_org_json(
    project: Annotated[
        str | None, typer.Option("--project", help="Google Cloud billing/project ID")
    ] = None,
    dataset: Annotated[str | None, typer.Option("--dataset")] = None,
    targets_table: Annotated[str, typer.Option("--targets-table")] = "organization_targets",
    monthly_table: Annotated[
        str, typer.Option("--monthly-table")
    ] = "organization_target_monthly_sparse",
    output: Annotated[
        str | None, typer.Option("--output", "-o", help="Local directory or gs:// URI")
    ] = None,
    start_month: Annotated[str, typer.Option("--start-month")] = "2015-01",
    end_month: Annotated[str, typer.Option("--end-month")] = "2025-12",
    workers: Annotated[int, typer.Option("--workers", min=1, max=64)] = 16,
    export_shards: Annotated[int, typer.Option("--export-shards", min=1)] = DEFAULT_EXPORT_SHARDS,
    shards: Annotated[str | None, typer.Option("--shards", help="Example: 0-31,40,52")] = None,
    resume: Annotated[bool, typer.Option("--resume/--no-resume")] = True,
    gzip_output: Annotated[bool, typer.Option("--gzip/--no-gzip")] = False,
    expected_organizations: Annotated[
        int, typer.Option("--expected-organizations", min=1)
    ] = 3_000_000,
    deadline_days: Annotated[float, typer.Option("--deadline-days", min=0.01)] = 20.0,
    location: Annotated[str, typer.Option("--location")] = "US",
) -> None:
    """Write one zero-filled 132-month JSON file per target organization ID."""
    settings = Settings()
    billing_project = project or settings.gcp_project
    if not billing_project:
        raise typer.BadParameter("Provide --project or set GCP_PROJECT in .env")
    destination = output or settings.target_json_output
    sink = (
        GCSJsonSink(destination)
        if destination.startswith("gs://")
        else LocalJsonSink(Path(destination))
    )
    exporter = TargetOrganizationJSONExporter(
        billing_project,
        dataset or settings.target_dataset,
        sink,
        location=location,
        targets_table=targets_table,
        monthly_table=monthly_table,
        export_shards=export_shards,
        compressed=gzip_output,
    )
    try:
        selected_shards = parse_shard_spec(shards, export_shards)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    required_rate = plan_throughput(expected_organizations, deadline_days).required_per_second

    def report_progress(completed: int, total: int, written: int, elapsed: float) -> None:
        rate = written / elapsed if written and elapsed else 0.0
        projected = expected_organizations / rate / 86_400 if rate else None
        eta = f"{projected:.2f} projected days" if projected is not None else "measuring rate"
        status = "on pace" if rate >= required_rate else "below required rate"
        console.print(
            f"Shards {completed}/{total}: {written:,} files, {rate:,.2f}/s, "
            f"{eta}, {status} ({required_rate:,.2f}/s required)"
        )

    manifest = exporter.export(
        YearMonth.parse(start_month),
        YearMonth.parse(end_month),
        workers=workers,
        resume=resume,
        shards=selected_shards,
        expected_organizations=expected_organizations,
        deadline_days=deadline_days,
        progress_callback=report_progress,
    )
    console.print(
        f"[green]Wrote[/green] {manifest['organization_count_written']:,} organization files; "
        f"skipped {manifest['export_shards_skipped']} completed shards"
    )


@app.command("plan-target-run")
def plan_target_run(
    organizations: Annotated[int, typer.Option("--organizations", min=1)] = 3_000_000,
    deadline_days: Annotated[float, typer.Option("--deadline-days", min=0.01)] = 20.0,
    safety_factor: Annotated[float, typer.Option("--safety-factor", min=1.0)] = 3.0,
    workers: Annotated[int, typer.Option("--workers", min=1)] = 32,
    machines: Annotated[int, typer.Option("--machines", min=1)] = 1,
    export_shards: Annotated[int, typer.Option("--export-shards", min=1)] = DEFAULT_EXPORT_SHARDS,
    sample_organizations: Annotated[
        int | None, typer.Option("--sample-organizations", min=1)
    ] = None,
    sample_seconds: Annotated[float | None, typer.Option("--sample-seconds", min=0.01)] = None,
) -> None:
    """Plan and validate the throughput required for a targeted JSON export."""
    try:
        plan = plan_throughput(
            organizations,
            deadline_days,
            safety_factor=safety_factor,
            sample_organizations=sample_organizations,
            sample_seconds=sample_seconds,
        )
        assignments = split_shards(export_shards, machines)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(f"Required aggregate rate: [bold]{plan.required_per_second:,.2f} files/s[/bold]")
    console.print(
        f"Safety target ({safety_factor:g}x): [bold]{plan.target_per_second:,.2f} files/s[/bold]"
    )
    console.print(f"Required daily output: {plan.required_per_day:,.0f} files/day")
    console.print(
        f"Required per worker at {machines * workers} total workers: "
        f"{plan.required_per_second / (machines * workers):,.4f} files/s"
    )
    if plan.observed_per_second is not None:
        colour = "green" if plan.meets_deadline else "red"
        console.print(
            f"Pilot result: [{colour}]{plan.observed_per_second:,.2f} files/s; "
            f"{plan.projected_days:,.2f} projected days[/{colour}]"
        )
    table = Table("Machine", "Shard range", "Workers")
    for assignment in assignments:
        table.add_row(str(assignment.machine), assignment.shard_spec, str(workers))
    console.print(table)


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
