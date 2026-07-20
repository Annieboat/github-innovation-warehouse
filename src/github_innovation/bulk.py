from __future__ import annotations

import json
import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from .db import Warehouse, utcnow

METRICS = (
    "public_events",
    "active_repositories",
    "distinct_actors",
    "push_events",
    "commits_in_pushes",
    "issues_opened",
    "pull_requests_opened",
    "repositories_created",
    "fork_events",
    "star_events",
    "release_events",
)
ADDITIVE_METRICS = tuple(
    metric for metric in METRICS if metric not in {"active_repositories", "distinct_actors"}
)

MONTHLY_SQL = r"""
SELECT
  LOWER(org.login) AS org_login,
  DATE_TRUNC(DATE(created_at), MONTH) AS month,
  COUNT(*) AS public_events,
  COUNT(DISTINCT repo.id) AS active_repositories,
  COUNT(DISTINCT actor.id) AS distinct_actors,
  COUNTIF(type = 'PushEvent') AS push_events,
  SUM(IF(
    type = 'PushEvent',
    COALESCE(ARRAY_LENGTH(JSON_QUERY_ARRAY(payload, '$.commits')), 0),
    0
  )) AS commits_in_pushes,
  COUNTIF(type = 'IssuesEvent' AND JSON_VALUE(payload, '$.action') = 'opened') AS issues_opened,
  COUNTIF(type = 'PullRequestEvent' AND JSON_VALUE(payload, '$.action') = 'opened') AS pull_requests_opened,
  COUNTIF(type = 'CreateEvent' AND JSON_VALUE(payload, '$.ref_type') = 'repository') AS repositories_created,
  COUNTIF(type = 'ForkEvent') AS fork_events,
  COUNTIF(type = 'WatchEvent' AND JSON_VALUE(payload, '$.action') = 'started') AS star_events,
  COUNTIF(type = 'ReleaseEvent' AND JSON_VALUE(payload, '$.action') = 'published') AS release_events
FROM `githubarchive.month.*`
WHERE _TABLE_SUFFIX BETWEEN @start_suffix AND @end_suffix
  AND org.login IS NOT NULL
  AND type IN (
    'PushEvent', 'IssuesEvent', 'PullRequestEvent', 'CreateEvent',
    'ForkEvent', 'WatchEvent', 'ReleaseEvent'
  )
GROUP BY org_login, month
ORDER BY org_login, month
""".strip()

_MONTH_PATTERN = re.compile(r"^(20\d{2})-(0[1-9]|1[0-2])$")
_SAFE_LOGIN = re.compile(r"[^a-z0-9-]+")


@dataclass(frozen=True, order=True)
class YearMonth:
    year: int
    month: int

    @classmethod
    def parse(cls, value: str) -> YearMonth:
        match = _MONTH_PATTERN.fullmatch(value)
        if not match:
            raise ValueError(f"Invalid month {value!r}; expected YYYY-MM")
        result = cls(int(match.group(1)), int(match.group(2)))
        if result.year < 2015:
            raise ValueError("The normalized monthly GH Archive workflow starts at 2015-01")
        return result

    def next(self) -> YearMonth:
        return YearMonth(self.year + 1, 1) if self.month == 12 else YearMonth(self.year, self.month + 1)

    def iso(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"

    def suffix(self) -> str:
        return f"{self.year:04d}{self.month:02d}"

    def as_date(self) -> date:
        return date(self.year, self.month, 1)


def iter_months(start: YearMonth, end: YearMonth) -> Iterator[YearMonth]:
    if end < start:
        raise ValueError("End month must not precede start month")
    current = start
    while current <= end:
        yield current
        current = current.next()


class QueryRow(Protocol):
    def __getitem__(self, key: str) -> Any: ...


class BigQueryMonthlyCollector:
    columns = ("org_login", "month", *METRICS, "source", "collected_at")

    def __init__(self, project: str, warehouse: Warehouse, batch_size: int = 5_000):
        self.project = project
        self.warehouse = warehouse
        self.batch_size = batch_size

    def collect(
        self,
        start: YearMonth,
        end: YearMonth,
        *,
        location: str = "US",
        maximum_bytes_billed: int | None = None,
        dry_run: bool = False,
    ) -> tuple[int, int | None]:
        """Return (rows written, estimated bytes for dry-run or bytes processed)."""
        if end < start:
            raise ValueError("End month must not precede start month")
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError(
                'BigQuery support is not installed; run pip install -e ".[bigquery]"'
            ) from exc

        client = bigquery.Client(project=self.project, location=location)
        params = [
            bigquery.ScalarQueryParameter("start_suffix", "STRING", start.suffix()),
            bigquery.ScalarQueryParameter("end_suffix", "STRING", end.suffix()),
        ]
        job_config = bigquery.QueryJobConfig(
            query_parameters=params,
            dry_run=dry_run,
            use_query_cache=not dry_run,
            maximum_bytes_billed=maximum_bytes_billed,
        )
        job = client.query(MONTHLY_SQL, job_config=job_config, location=location)
        if dry_run:
            return 0, int(job.total_bytes_processed or 0)

        observed_at = utcnow()
        written = 0
        batch: list[Sequence[Any]] = []
        self.warehouse.initialize()
        with self.warehouse.run("gharchive_bigquery", f"{start.iso()}:{end.iso()}"):
            for row in job.result(page_size=self.batch_size):
                batch.append(self._warehouse_row(row, observed_at))
                if len(batch) >= self.batch_size:
                    self._write(batch)
                    written += len(batch)
                    batch.clear()
            if batch:
                self._write(batch)
                written += len(batch)
        return written, int(job.total_bytes_processed or 0)

    def _write(self, rows: Sequence[Sequence[Any]]) -> None:
        self.warehouse.upsert_many(
            "organization_public_monthly", self.columns, rows, ["org_login", "month"]
        )

    @staticmethod
    def _warehouse_row(row: QueryRow, observed_at: datetime) -> Sequence[Any]:
        return (
            str(row["org_login"]).lower(),
            row["month"],
            *(int(row[name] or 0) for name in METRICS),
            "gharchive_bigquery",
            observed_at,
        )


class OrganizationJSONExporter:
    schema_version = "1.0"

    def __init__(self, warehouse: Warehouse, output_dir: Path):
        self.warehouse = warehouse
        self.output_dir = output_dir

    def export(self, start: YearMonth, end: YearMonth) -> dict[str, Any]:
        months = list(iter_months(start, end))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        cursor = self.warehouse.connection.execute(
            f"""
            SELECT org_login, strftime(month, '%Y-%m') AS month_key, {', '.join(METRICS)}
            FROM organization_public_monthly
            WHERE month BETWEEN ? AND ?
            ORDER BY org_login, month
            """,
            [start.as_date(), end.as_date()],
        )
        count = 0
        current_login: str | None = None
        current_rows: dict[str, dict[str, int]] = {}
        while batch := cursor.fetchmany(10_000):
            for raw in batch:
                login = str(raw[0])
                if current_login is not None and login != current_login:
                    self._write_org(current_login, current_rows, months)
                    count += 1
                    current_rows = {}
                current_login = login
                current_rows[str(raw[1])] = {
                    name: int(raw[index + 2] or 0) for index, name in enumerate(METRICS)
                }
        if current_login is not None:
            self._write_org(current_login, current_rows, months)
            count += 1

        manifest = {
            "schema_version": self.schema_version,
            "generated_at": datetime.now(UTC).isoformat(),
            "source": "GH Archive public events via BigQuery",
            "definition": "Organizations with org.login present in at least one selected public event",
            "period": {"start": start.iso(), "end": end.iso(), "months": len(months)},
            "organization_count": count,
            "file_layout": "<first-two-login-characters>/<organization-login>.json",
            "metrics": list(METRICS),
        }
        self._write_json(self.output_dir / "manifest.json", manifest)
        return manifest

    def _write_org(
        self, login: str, observed: dict[str, dict[str, int]], months: Iterable[YearMonth]
    ) -> None:
        monthly = []
        totals = {metric: 0 for metric in ADDITIVE_METRICS}
        months_with_activity = 0
        monthly_active_repositories: list[int] = []
        monthly_distinct_actors: list[int] = []
        for month in months:
            values = observed.get(month.iso(), {metric: 0 for metric in METRICS})
            item = {"month": month.iso(), **values}
            monthly.append(item)
            if values["public_events"] > 0:
                months_with_activity += 1
            monthly_active_repositories.append(values["active_repositories"])
            monthly_distinct_actors.append(values["distinct_actors"])
            for metric in ADDITIVE_METRICS:
                totals[metric] += values[metric]
        safe_login = _SAFE_LOGIN.sub("-", login.lower()).strip("-") or "unknown"
        shard = (safe_login + "__")[:2]
        payload = {
            "schema_version": self.schema_version,
            "organization": {"login": login},
            "period": {
                "start": monthly[0]["month"],
                "end": monthly[-1]["month"],
                "months": len(monthly),
            },
            "source": "GH Archive public events via BigQuery",
            "coverage": {
                "definition": "Public events where GitHub supplied org.login",
                "zero_filled_months": True,
                "private_activity_included": False,
            },
            "totals": totals,
            "monthly_activity_summary": {
                "months_with_activity": months_with_activity,
                "sum_monthly_active_repositories": sum(monthly_active_repositories),
                "max_monthly_active_repositories": max(monthly_active_repositories, default=0),
                "sum_monthly_distinct_actors": sum(monthly_distinct_actors),
                "max_monthly_distinct_actors": max(monthly_distinct_actors, default=0),
            },
            "monthly": monthly,
        }
        self._write_json(self.output_dir / shard / f"{safe_login}.json", payload)

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
        temporary.replace(path)
