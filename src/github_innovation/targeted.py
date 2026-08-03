from __future__ import annotations

import csv
import gzip
import json
import re
import tempfile
from collections.abc import Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

from .bulk import YearMonth, iter_months

TARGET_METRICS = (
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

NON_ADDITIVE_METRICS = {"active_repositories", "distinct_actors"}
ADDITIVE_TARGET_METRICS = tuple(
    metric for metric in TARGET_METRICS if metric not in NON_ADDITIVE_METRICS
)
DEFAULT_EXPORT_SHARDS = 256
DEFAULT_OUTPUT_PREFIXES = 4096
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class TargetCSVStats:
    rows: int
    invalid_rows: int


@dataclass(frozen=True)
class CollectionResult:
    yearly_tables_created: int
    yearly_tables_skipped: int
    bytes_processed: int


@dataclass(frozen=True)
class ExportShardResult:
    shard: int
    organizations: int
    skipped: bool = False


class QueryJob(Protocol):
    total_bytes_processed: int | None

    def result(self, **kwargs: Any) -> Iterable[Mapping[str, Any]]: ...


class BigQueryLike(Protocol):
    def query(self, query: str, **kwargs: Any) -> QueryJob: ...


def normalize_target_csv(source: Path, destination: Path) -> TargetCSVStats:
    """Write the two required target columns as a normalized UTF-8 CSV.

    The input may contain the full observed-organization extract. Duplicates are
    intentionally retained here and removed by BigQuery, avoiding a multi-million-ID
    in-memory set in the client process.
    """
    rows = 0
    invalid: list[int] = []
    with (
        source.open("r", encoding="utf-8-sig", newline="") as input_handle,
        destination.open("w", encoding="utf-8", newline="") as output_handle,
    ):
        reader = csv.DictReader(input_handle)
        if not reader.fieldnames:
            raise ValueError("Target CSV has no header")
        lookup = {name.strip().lower(): name for name in reader.fieldnames}
        id_column = lookup.get("organization_id") or lookup.get("org_id")
        if not id_column:
            raise ValueError("Target CSV must contain organization_id or org_id")
        login_column = lookup.get("historical_login") or lookup.get("login")
        writer = csv.DictWriter(output_handle, fieldnames=["org_id", "historical_login"])
        writer.writeheader()
        for line_number, record in enumerate(reader, start=2):
            raw_id = (record.get(id_column) or "").strip()
            try:
                org_id = int(raw_id)
                if org_id <= 0:
                    raise ValueError
            except ValueError:
                invalid.append(line_number)
                continue
            login = (record.get(login_column) or "").strip() if login_column else ""
            writer.writerow({"org_id": org_id, "historical_login": login})
            rows += 1
    if invalid:
        preview = ", ".join(str(line) for line in invalid[:10])
        raise ValueError(
            f"Target CSV contains {len(invalid)} invalid organization IDs at line(s) {preview}"
        )
    if rows == 0:
        raise ValueError("Target CSV contains no valid organization IDs")
    return TargetCSVStats(rows=rows, invalid_rows=0)


def _qualified(project: str, dataset: str, table: str) -> str:
    for label, value in (("dataset", dataset), ("table", table)):
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"Unsafe BigQuery {label} identifier: {value!r}")
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", project):
        raise ValueError(f"Unsafe Google Cloud project identifier: {project!r}")
    return f"`{project}.{dataset}.{table}`"


def yearly_aggregation_sql(
    project: str,
    dataset: str,
    targets_table: str,
    output_table: str,
    year: int,
) -> str:
    targets = _qualified(project, dataset, targets_table)
    output = _qualified(project, dataset, output_table)
    start_suffix, end_suffix = f"{year}01", f"{year}12"
    return f"""
CREATE OR REPLACE TABLE {output}
PARTITION BY month
CLUSTER BY export_shard, org_id AS
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
), attributed AS (
  SELECT
    event_id, event_type, actor_id, repo_id, event_action, ref_type,
    commits_in_push, pull_request_merged, month,
    attribution.org_id, attribution.attribution_type
  FROM archive_events
  CROSS JOIN UNNEST([
    STRUCT(event_org_id AS org_id, 'repository_activity' AS attribution_type),
    STRUCT(fork_destination_org_id AS org_id, 'fork_destination' AS attribution_type)
  ]) AS attribution
  WHERE attribution.org_id IS NOT NULL
), matched AS (
  SELECT t.export_shard, a.*
  FROM attributed AS a
  INNER JOIN {targets} AS t USING (org_id)
)
SELECT
  org_id,
  ANY_VALUE(export_shard) AS export_shard,
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
FROM matched
GROUP BY org_id, month
""".strip()


def consolidation_sql(
    project: str,
    dataset: str,
    output_table: str,
    yearly_tables: Sequence[str],
) -> str:
    output = _qualified(project, dataset, output_table)
    sources = "\nUNION ALL\n".join(
        f"SELECT * FROM {_qualified(project, dataset, table)}" for table in yearly_tables
    )
    return f"""
CREATE OR REPLACE TABLE {output}
PARTITION BY month
CLUSTER BY export_shard, org_id AS
{sources}
""".strip()


class TargetedBigQueryPipeline:
    def __init__(self, project: str, dataset: str, location: str = "US"):
        self.project = project
        self.dataset = dataset
        self.location = location

    @staticmethod
    def _imports() -> tuple[Any, Any]:
        try:
            from google.api_core.exceptions import NotFound
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError(
                'Targeted BigQuery support is not installed; run pip install -e ".[targeted]"'
            ) from exc
        return bigquery, NotFound

    def _client(self) -> Any:
        bigquery, _ = self._imports()
        return bigquery.Client(project=self.project, location=self.location)

    def ensure_dataset(self, client: Any) -> None:
        bigquery, NotFound = self._imports()
        dataset_id = f"{self.project}.{self.dataset}"
        try:
            client.get_dataset(dataset_id)
        except NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.location
            client.create_dataset(dataset)

    def load_targets(
        self,
        csv_path: Path,
        *,
        table: str = "organization_targets",
        export_shards: int = DEFAULT_EXPORT_SHARDS,
    ) -> TargetCSVStats:
        if export_shards < 1:
            raise ValueError("export_shards must be positive")
        bigquery, _ = self._imports()
        client = self._client()
        self.ensure_dataset(client)
        with tempfile.TemporaryDirectory(prefix="ghiw-targets-") as temporary_dir:
            normalized = Path(temporary_dir) / "targets.csv"
            stats = normalize_target_csv(csv_path, normalized)
            staging_name = f"{table}_load"
            staging = f"{self.project}.{self.dataset}.{staging_name}"
            config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField("org_id", "INT64", mode="REQUIRED"),
                    bigquery.SchemaField("historical_login", "STRING"),
                ],
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            with normalized.open("rb") as handle:
                client.load_table_from_file(
                    handle, staging, job_config=config, location=self.location
                ).result()
            target = _qualified(self.project, self.dataset, table)
            staging_qualified = _qualified(self.project, self.dataset, staging_name)
            client.query(
                f"""
                CREATE OR REPLACE TABLE {target}
                PARTITION BY RANGE_BUCKET(export_shard, GENERATE_ARRAY(0, {export_shards}, 1))
                CLUSTER BY org_id AS
                SELECT
                  org_id,
                  MAX(NULLIF(TRIM(historical_login), '')) AS historical_login,
                  MOD(org_id, {export_shards}) AS export_shard
                FROM {staging_qualified}
                GROUP BY org_id
                """,
                location=self.location,
            ).result()
            client.delete_table(staging, not_found_ok=True)
        return stats

    def collect_monthly(
        self,
        start: YearMonth,
        end: YearMonth,
        *,
        targets_table: str = "organization_targets",
        output_table: str = "organization_target_monthly_sparse",
        workers: int = 4,
        resume: bool = True,
        dry_run: bool = False,
        maximum_bytes_billed: int | None = None,
    ) -> CollectionResult:
        if end < start:
            raise ValueError("End month must not precede start month")
        bigquery, NotFound = self._imports()
        years = list(range(start.year, end.year + 1))

        def run_year(year: int) -> tuple[str, int, bool]:
            table = f"{output_table}_{year}"
            client = self._client()
            table_id = f"{self.project}.{self.dataset}.{table}"
            if resume and not dry_run:
                try:
                    client.get_table(table_id)
                    return table, 0, True
                except NotFound:
                    pass
            query = yearly_aggregation_sql(self.project, self.dataset, targets_table, table, year)
            config = bigquery.QueryJobConfig(
                dry_run=dry_run,
                use_query_cache=not dry_run,
                maximum_bytes_billed=maximum_bytes_billed,
            )
            job = client.query(query, job_config=config, location=self.location)
            if not dry_run:
                job.result()
            return table, int(job.total_bytes_processed or 0), False

        outcomes: list[tuple[str, int, bool]] = []
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {executor.submit(run_year, year): year for year in years}
            for future in as_completed(futures):
                outcomes.append(future.result())
        outcomes.sort(key=lambda item: item[0])
        if not dry_run:
            client = self._client()
            client.query(
                consolidation_sql(
                    self.project,
                    self.dataset,
                    output_table,
                    [item[0] for item in outcomes],
                ),
                location=self.location,
            ).result()
        return CollectionResult(
            yearly_tables_created=sum(not item[2] for item in outcomes),
            yearly_tables_skipped=sum(item[2] for item in outcomes),
            bytes_processed=sum(item[1] for item in outcomes),
        )


def export_query_sql(
    project: str,
    dataset: str,
    targets_table: str,
    monthly_table: str,
) -> str:
    targets = _qualified(project, dataset, targets_table)
    monthly = _qualified(project, dataset, monthly_table)
    columns = ",\n  ".join(f"m.{metric}" for metric in TARGET_METRICS)
    return f"""
SELECT
  t.org_id,
  t.historical_login,
  m.month,
  {columns}
FROM {targets} AS t
LEFT JOIN {monthly} AS m USING (org_id, export_shard)
WHERE t.export_shard = @export_shard
ORDER BY t.org_id, m.month
""".strip()


class JsonSink(Protocol):
    def shard_complete(self, shard: int) -> bool: ...

    def write_organization(self, org_id: int, payload: bytes, *, compressed: bool) -> None: ...

    def mark_shard_complete(self, shard: int, organizations: int) -> None: ...

    def write_manifest(self, payload: bytes) -> None: ...


class LocalJsonSink:
    def __init__(self, root: Path, output_prefixes: int = DEFAULT_OUTPUT_PREFIXES):
        self.root = root
        self.output_prefixes = output_prefixes
        self.root.mkdir(parents=True, exist_ok=True)

    def shard_complete(self, shard: int) -> bool:
        return (self.root / "_shards" / f"{shard:04d}.json").exists()

    def write_organization(self, org_id: int, payload: bytes, *, compressed: bool) -> None:
        prefix = org_id % self.output_prefixes
        suffix = ".json.gz" if compressed else ".json"
        path = self.root / f"{prefix:03x}" / f"{org_id}{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(payload)
        temporary.replace(path)

    def mark_shard_complete(self, shard: int, organizations: int) -> None:
        path = self.root / "_shards" / f"{shard:04d}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"shard": shard, "organizations": organizations}) + "\n")

    def write_manifest(self, payload: bytes) -> None:
        (self.root / "manifest.json").write_bytes(payload)


class GCSJsonSink:
    def __init__(self, uri: str, output_prefixes: int = DEFAULT_OUTPUT_PREFIXES):
        try:
            from google.cloud import storage
        except ImportError as exc:
            raise RuntimeError(
                'Google Cloud Storage support is not installed; run pip install -e ".[targeted]"'
            ) from exc
        without_scheme = uri.removeprefix("gs://")
        bucket_name, _, prefix = without_scheme.partition("/")
        if not bucket_name:
            raise ValueError("GCS output must use gs://bucket/prefix")
        self.bucket = storage.Client().bucket(bucket_name)
        self.prefix = prefix.strip("/")
        self.output_prefixes = output_prefixes

    def _name(self, relative: str) -> str:
        return f"{self.prefix}/{relative}" if self.prefix else relative

    def shard_complete(self, shard: int) -> bool:
        return self.bucket.blob(self._name(f"_shards/{shard:04d}.json")).exists()

    def write_organization(self, org_id: int, payload: bytes, *, compressed: bool) -> None:
        prefix = org_id % self.output_prefixes
        suffix = ".json.gz" if compressed else ".json"
        blob = self.bucket.blob(self._name(f"{prefix:03x}/{org_id}{suffix}"))
        if compressed:
            blob.content_encoding = "gzip"
        blob.upload_from_string(
            payload,
            content_type="application/json",
        )

    def mark_shard_complete(self, shard: int, organizations: int) -> None:
        self.bucket.blob(self._name(f"_shards/{shard:04d}.json")).upload_from_string(
            json.dumps({"shard": shard, "organizations": organizations}) + "\n",
            content_type="application/json",
        )

    def write_manifest(self, payload: bytes) -> None:
        self.bucket.blob(self._name("manifest.json")).upload_from_string(
            payload, content_type="application/json"
        )


class TargetOrganizationJSONExporter:
    schema_version = "2.0"

    def __init__(
        self,
        project: str,
        dataset: str,
        sink: JsonSink,
        *,
        location: str = "US",
        targets_table: str = "organization_targets",
        monthly_table: str = "organization_target_monthly_sparse",
        export_shards: int = DEFAULT_EXPORT_SHARDS,
        compressed: bool = False,
        client_factory: Any | None = None,
    ):
        self.project = project
        self.dataset = dataset
        self.sink = sink
        self.location = location
        self.targets_table = targets_table
        self.monthly_table = monthly_table
        self.export_shards = export_shards
        self.compressed = compressed
        self.client_factory = client_factory or self._default_client

    def _default_client(self) -> Any:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError(
                'Targeted BigQuery support is not installed; run pip install -e ".[targeted]"'
            ) from exc
        return bigquery.Client(project=self.project, location=self.location)

    def export(
        self,
        start: YearMonth,
        end: YearMonth,
        *,
        workers: int = 16,
        resume: bool = True,
        shards: Sequence[int] | None = None,
    ) -> dict[str, Any]:
        selected = list(shards) if shards is not None else list(range(self.export_shards))
        results: list[ExportShardResult] = []
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {
                executor.submit(self._export_shard, shard, start, end, resume): shard
                for shard in selected
            }
            for future in as_completed(futures):
                results.append(future.result())
        manifest = {
            "schema_version": self.schema_version,
            "generated_at": datetime.now(UTC).isoformat(),
            "period": {
                "start": start.iso(),
                "end": end.iso(),
                "months": len(list(iter_months(start, end))),
            },
            "organization_count_written": sum(item.organizations for item in results),
            "export_shards_selected": len(selected),
            "export_shards_skipped": sum(item.skipped for item in results),
            "compressed": self.compressed,
            "metrics": list(TARGET_METRICS),
            "file_layout": "<org-id-mod-4096-hex>/<organization-id>.json[.gz]",
        }
        self.sink.write_manifest(_json_bytes(manifest))
        return manifest

    def _export_shard(
        self, shard: int, start: YearMonth, end: YearMonth, resume: bool
    ) -> ExportShardResult:
        if shard < 0 or shard >= self.export_shards:
            raise ValueError(f"Invalid export shard {shard}")
        if resume and self.sink.shard_complete(shard):
            return ExportShardResult(shard=shard, organizations=0, skipped=True)
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise RuntimeError(
                'Targeted BigQuery support is not installed; run pip install -e ".[targeted]"'
            ) from exc
        client = self.client_factory()
        config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("export_shard", "INT64", shard)]
        )
        rows = client.query(
            export_query_sql(
                self.project,
                self.dataset,
                self.targets_table,
                self.monthly_table,
            ),
            job_config=config,
            location=self.location,
        ).result(page_size=10_000)
        organizations = self._write_rows(rows, start, end)
        self.sink.mark_shard_complete(shard, organizations)
        return ExportShardResult(shard=shard, organizations=organizations)

    def _write_rows(
        self,
        rows: Iterable[Mapping[str, Any]],
        start: YearMonth,
        end: YearMonth,
    ) -> int:
        current_id: int | None = None
        current_login: str | None = None
        observed: dict[str, dict[str, int]] = {}
        count = 0
        for row in rows:
            org_id = int(row["org_id"])
            if current_id is not None and org_id != current_id:
                self._write_one(current_id, current_login, observed, start, end)
                count += 1
                observed = {}
            current_id = org_id
            current_login = row.get("historical_login")
            month = row.get("month")
            if month is not None:
                month_key = month.strftime("%Y-%m") if isinstance(month, date) else str(month)[:7]
                observed[month_key] = {
                    metric: int(row.get(metric) or 0) for metric in TARGET_METRICS
                }
        if current_id is not None:
            self._write_one(current_id, current_login, observed, start, end)
            count += 1
        return count

    def _write_one(
        self,
        org_id: int,
        login: str | None,
        observed: Mapping[str, Mapping[str, int]],
        start: YearMonth,
        end: YearMonth,
    ) -> None:
        monthly = []
        totals = {metric: 0 for metric in ADDITIVE_TARGET_METRICS}
        months_with_activity = 0
        max_active_repositories = 0
        max_distinct_actors = 0
        for month in iter_months(start, end):
            values = {
                metric: int(observed.get(month.iso(), {}).get(metric, 0))
                for metric in TARGET_METRICS
            }
            monthly.append({"month": month.iso(), **values})
            months_with_activity += int(values["public_events"] > 0)
            max_active_repositories = max(max_active_repositories, values["active_repositories"])
            max_distinct_actors = max(max_distinct_actors, values["distinct_actors"])
            for metric in ADDITIVE_TARGET_METRICS:
                totals[metric] += values[metric]
        payload = {
            "schema_version": self.schema_version,
            "organization": {"id": org_id, "historical_login": login},
            "period": {"start": start.iso(), "end": end.iso(), "months": len(monthly)},
            "source": "GH Archive monthly tables via BigQuery",
            "coverage": {
                "matched_by": "stable GitHub organization ID",
                "zero_filled_months": True,
                "private_activity_included": False,
            },
            "totals": totals,
            "monthly_activity_summary": {
                "months_with_activity": months_with_activity,
                "max_monthly_active_repositories": max_active_repositories,
                "max_monthly_distinct_actors": max_distinct_actors,
            },
            "monthly": monthly,
        }
        encoded = _json_bytes(payload)
        self.sink.write_organization(
            org_id,
            gzip.compress(encoded) if self.compressed else encoded,
            compressed=self.compressed,
        )


def parse_shard_spec(value: str | None, export_shards: int) -> list[int] | None:
    if value is None:
        return None
    selected: set[int] = set()
    for item in value.split(","):
        part = item.strip()
        if not part:
            continue
        if "-" in part:
            left, right = part.split("-", 1)
            selected.update(range(int(left), int(right) + 1))
        else:
            selected.add(int(part))
    result = sorted(selected)
    if not result or result[0] < 0 or result[-1] >= export_shards:
        raise ValueError(f"Shard selection must be between 0 and {export_shards - 1}")
    return result


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
