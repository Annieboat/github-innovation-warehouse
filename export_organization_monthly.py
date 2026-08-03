#!/usr/bin/env python3
"""Directly export GH Archive monthly activity for organization IDs in a CSV.

This intentionally creates no DuckDB database, BigQuery dataset, or persistent
BigQuery table. BigQuery is used only to run a read-only query against the public
GH Archive monthly tables. Outputs are ordinary JSON and CSV files.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

METRICS = (
    "public_events",
    "active_repositories",
    "distinct_actors",
    "push_events",
    "commits_in_pushes",
    "issues_opened",
    "issues_closed",
    "issue_comments_created",
    "pull_requests_opened",
    "pull_requests_closed",
    "pull_requests_merged",
    "original_repositories_created",
    "forked_repositories_created",
    "forks_received",
    "stars_received",
    "releases_published",
)

ADDITIVE_METRICS = tuple(
    metric for metric in METRICS if metric not in {"active_repositories", "distinct_actors"}
)
MAX_DIRECT_IDS = 10_000


@dataclass(frozen=True, order=True)
class YearMonth:
    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> YearMonth:
        try:
            parsed = datetime.strptime(value, "%Y-%m")
        except ValueError as exc:
            raise ValueError(f"Invalid month {value!r}; use YYYY-MM") from exc
        return cls(parsed.year, parsed.month)

    def iso(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    def next(self) -> YearMonth:
        return (
            YearMonth(self.year + 1, 1)
            if self.month == 12
            else YearMonth(self.year, self.month + 1)
        )


def iter_months(start: YearMonth, end: YearMonth) -> Iterable[YearMonth]:
    current = start
    while current <= end:
        yield current
        current = current.next()


def read_targets(path: Path) -> dict[int, str | None]:
    """Read organization_id and optional historical_login from a BOM-safe CSV."""
    targets: dict[int, str | None] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("The input CSV has no header")
        columns = {column.strip().lower(): column for column in reader.fieldnames}
        id_column = columns.get("organization_id") or columns.get("org_id")
        login_column = columns.get("historical_login") or columns.get("login")
        if id_column is None:
            raise ValueError("The input CSV must contain organization_id or org_id")
        for line, row in enumerate(reader, start=2):
            raw_id = (row.get(id_column) or "").strip()
            try:
                organization_id = int(raw_id)
                if organization_id <= 0:
                    raise ValueError
            except ValueError as exc:
                raise ValueError(f"Invalid organization_id at CSV line {line}: {raw_id!r}") from exc
            login = (row.get(login_column) or "").strip() if login_column else ""
            targets[organization_id] = login or None
    if not targets:
        raise ValueError("The input CSV contains no organization IDs")
    if len(targets) > MAX_DIRECT_IDS:
        raise ValueError(
            f"Direct mode supports at most {MAX_DIRECT_IDS:,} IDs per run; got {len(targets):,}. "
            "A multi-million-ID census requires the repository's scalable targeted workflow."
        )
    return targets


def monthly_query(start: YearMonth, end: YearMonth) -> str:
    start_suffix = f"{start.year:04d}{start.month:02d}"
    end_suffix = f"{end.year:04d}{end.month:02d}"
    return f"""
WITH archive_events AS (
  SELECT
    id AS event_id,
    type AS event_type,
    actor.id AS actor_id,
    repo.id AS repo_id,
    SAFE_CAST(org.id AS INT64) AS event_org_id,
    IF(
      type = 'ForkEvent',
      SAFE_CAST(JSON_VALUE(payload, '$.forkee.owner.id') AS INT64),
      NULL
    ) AS fork_destination_org_id,
    JSON_VALUE(payload, '$.action') AS event_action,
    JSON_VALUE(payload, '$.ref_type') AS ref_type,
    IF(
      type = 'PushEvent',
      COALESCE(
        SAFE_CAST(JSON_VALUE(payload, '$.size') AS INT64),
        ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.commits')),
        0
      ),
      0
    ) AS commits_in_push,
    IF(
      type = 'PullRequestEvent',
      LOWER(JSON_VALUE(payload, '$.pull_request.merged')),
      NULL
    ) AS pull_request_merged,
    DATE_TRUNC(DATE(created_at), MONTH) AS month
  FROM `githubarchive.month.*`
  WHERE _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
    AND (
      SAFE_CAST(org.id AS INT64) IN UNNEST(@organization_ids)
      OR (
        type = 'ForkEvent'
        AND SAFE_CAST(JSON_VALUE(payload, '$.forkee.owner.id') AS INT64)
            IN UNNEST(@organization_ids)
      )
    )
), attributed AS (
  SELECT
    event_id, event_type, actor_id, repo_id, event_action, ref_type,
    commits_in_push, pull_request_merged, month,
    attribution.organization_id, attribution.attribution_type
  FROM archive_events
  CROSS JOIN UNNEST([
    STRUCT(event_org_id AS organization_id, 'repository_activity' AS attribution_type),
    STRUCT(fork_destination_org_id AS organization_id, 'fork_destination' AS attribution_type)
  ]) AS attribution
  WHERE attribution.organization_id IN UNNEST(@organization_ids)
)
SELECT
  organization_id,
  month,
  COUNTIF(attribution_type = 'repository_activity') AS public_events,
  COUNT(DISTINCT IF(attribution_type = 'repository_activity', repo_id, NULL))
    AS active_repositories,
  COUNT(DISTINCT IF(attribution_type = 'repository_activity', actor_id, NULL))
    AS distinct_actors,
  COUNTIF(attribution_type = 'repository_activity' AND event_type = 'PushEvent')
    AS push_events,
  SUM(IF(
    attribution_type = 'repository_activity' AND event_type = 'PushEvent',
    commits_in_push, 0
  )) AS commits_in_pushes,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'IssuesEvent'
    AND event_action = 'opened'
  ) AS issues_opened,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'IssuesEvent'
    AND event_action = 'closed'
  ) AS issues_closed,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'IssueCommentEvent'
    AND event_action = 'created'
  ) AS issue_comments_created,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'PullRequestEvent'
    AND event_action = 'opened'
  ) AS pull_requests_opened,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'PullRequestEvent'
    AND event_action = 'closed'
  ) AS pull_requests_closed,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'PullRequestEvent'
    AND event_action = 'closed' AND pull_request_merged = 'true'
  ) AS pull_requests_merged,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'CreateEvent'
    AND ref_type = 'repository'
  ) AS original_repositories_created,
  COUNTIF(attribution_type = 'fork_destination' AND event_type = 'ForkEvent')
    AS forked_repositories_created,
  COUNTIF(attribution_type = 'repository_activity' AND event_type = 'ForkEvent')
    AS forks_received,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'WatchEvent'
    AND event_action = 'started'
  ) AS stars_received,
  COUNTIF(
    attribution_type = 'repository_activity' AND event_type = 'ReleaseEvent'
    AND event_action = 'published'
  ) AS releases_published
FROM attributed
GROUP BY organization_id, month
ORDER BY organization_id, month
""".strip()


def collect_rows(
    project: str,
    organization_ids: list[int],
    start: YearMonth,
    end: YearMonth,
    *,
    location: str = "US",
    dry_run: bool = False,
    maximum_bytes_billed: int | None = None,
) -> tuple[Iterable[Mapping[str, Any]], int]:
    try:
        from google.cloud import bigquery
    except ImportError as exc:
        raise RuntimeError(
            "Missing dependency. Run: python -m pip install google-cloud-bigquery"
        ) from exc
    client = bigquery.Client(project=project, location=location)
    config = bigquery.QueryJobConfig(
        dry_run=dry_run,
        use_query_cache=not dry_run,
        query_parameters=[
            bigquery.ArrayQueryParameter("organization_ids", "INT64", organization_ids)
        ],
    )
    # Some google-cloud-bigquery/Python combinations serialize an explicitly
    # supplied None as the string "None", which the API rejects as TYPE_INT64.
    # Omitting the property preserves BigQuery's normal unlimited default.
    if maximum_bytes_billed is not None:
        config.maximum_bytes_billed = maximum_bytes_billed
    job = client.query(monthly_query(start, end), job_config=config, location=location)
    bytes_processed = int(job.total_bytes_processed or 0)
    return ([] if dry_run else job.result(page_size=10_000)), bytes_processed


def build_panel(
    targets: Mapping[int, str | None],
    rows: Iterable[Mapping[str, Any]],
    start: YearMonth,
    end: YearMonth,
) -> dict[int, dict[str, Any]]:
    observed: dict[int, dict[str, dict[str, int]]] = {org_id: {} for org_id in targets}
    for row in rows:
        org_id = int(row["organization_id"])
        raw_month = row["month"]
        month = raw_month.strftime("%Y-%m") if isinstance(raw_month, date) else str(raw_month)[:7]
        observed[org_id][month] = {metric: int(row.get(metric) or 0) for metric in METRICS}

    panel: dict[int, dict[str, Any]] = {}
    for org_id, login in targets.items():
        monthly = []
        totals = {metric: 0 for metric in ADDITIVE_METRICS}
        for month in iter_months(start, end):
            values = {
                metric: observed[org_id].get(month.iso(), {}).get(metric, 0) for metric in METRICS
            }
            monthly.append({"month": month.iso(), **values})
            for metric in ADDITIVE_METRICS:
                totals[metric] += values[metric]
        panel[org_id] = {
            "organization_id": org_id,
            "historical_login": login,
            "period": {"start": start.iso(), "end": end.iso(), "months": len(monthly)},
            "source": "GH Archive public monthly tables via a read-only BigQuery query",
            "coverage": {
                "public_activity_only": True,
                "zero_filled_months": True,
                "persistent_database_created": False,
            },
            "totals": totals,
            "monthly": monthly,
        }
    return panel


def write_outputs(panel: Mapping[int, Mapping[str, Any]], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    for org_id, payload in panel.items():
        with (output_dir / f"{org_id}.json").open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

    csv_path = output_dir / "organization_monthly.csv"
    fieldnames = ["organization_id", "historical_login", "month", *METRICS]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for org_id, payload in panel.items():
            for month in payload["monthly"]:
                writer.writerow(
                    {
                        "organization_id": org_id,
                        "historical_login": payload["historical_login"] or "",
                        **month,
                    }
                )

    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "organization_count": len(panel),
        "monthly_row_count": sum(len(payload["monthly"]) for payload in panel.values()),
        "json_files": [f"{org_id}.json" for org_id in panel],
        "combined_csv": csv_path.name,
        "persistent_database_created": False,
    }
    with (output_dir / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return manifest


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="CSV organization IDs -> monthly GH Archive JSON and CSV; no database created"
    )
    result.add_argument("--csv", required=True, type=Path, help="Input CSV with organization_id")
    result.add_argument("--project", required=True, help="Google Cloud billing project ID")
    result.add_argument("--output", type=Path, default=Path("outputs/organization_monthly"))
    result.add_argument("--start", default="2015-01", help="First month, YYYY-MM")
    result.add_argument("--end", default="2025-12", help="Last month, YYYY-MM")
    result.add_argument("--location", default="US")
    result.add_argument("--dry-run", action="store_true", help="Estimate bytes; write no outputs")
    result.add_argument("--maximum-bytes-billed", type=int)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        start, end = YearMonth.parse(args.start), YearMonth.parse(args.end)
        if end < start:
            raise ValueError("--end must not precede --start")
        targets = read_targets(args.csv)
        print(f"Read {len(targets):,} unique organization IDs from {args.csv}")
        rows, bytes_processed = collect_rows(
            args.project,
            sorted(targets),
            start,
            end,
            location=args.location,
            dry_run=args.dry_run,
            maximum_bytes_billed=args.maximum_bytes_billed,
        )
        gib = bytes_processed / 1024**3
        if args.dry_run:
            print(f"Dry-run estimate: {bytes_processed:,} bytes ({gib:,.2f} GiB)")
            print("No database, table, or output file was created.")
            return 0
        panel = build_panel(targets, rows, start, end)
        manifest = write_outputs(panel, args.output)
        print(f"Wrote {manifest['organization_count']:,} JSON files to {args.output}")
        print(f"Wrote {manifest['monthly_row_count']:,} rows to organization_monthly.csv")
        print(f"BigQuery processed {bytes_processed:,} bytes ({gib:,.2f} GiB)")
        print("No persistent database or BigQuery table was created.")
        return 0
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
