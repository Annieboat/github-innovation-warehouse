# Bulk public-organization monthly panel, 2015–2025

## Coverage contract

The bulk pipeline includes an organization when GitHub supplied a non-null `org.login` in at least one selected public GH Archive event between the inclusive start and end months. It does not include private activity or guarantee coverage of deleted events, and it cannot construct a historical census of organizations with no public activity.

The default event set is:

- `PushEvent`
- `IssuesEvent`
- `PullRequestEvent`
- `CreateEvent`
- `ForkEvent`
- `WatchEvent`
- `ReleaseEvent`

The source tables are `githubarchive.month.*`. The January 2015 start is deliberate: GH Archive used the deprecated Timeline API through December 2014 and the Events API from January 2015, so earlier years require a separate schema adapter.

## Why BigQuery

January 2015 through December 2025 contains 96,432 hourly intervals. Downloading every global archive would transfer data unrelated to the research sample and create a fragile long-running job. The bulk command instead submits one server-side aggregate query and downloads only organization-month result rows.

BigQuery charges by bytes processed under on-demand pricing. Always run `--dry-run`, inspect the estimate, and set `--maximum-bytes-billed`. The value is an upper limit, not an expected charge.

## Authentication

Install and authenticate:

```bash
python -m pip install -e ".[bigquery]"
gcloud auth application-default login
gcloud config set project YOUR_PROJECT
```

Alternatively set `GOOGLE_APPLICATION_CREDENTIALS` to a service-account JSON file with permission to run BigQuery jobs in the billing project. Do not commit that file.

## Complete workflow

```bash
cp .env.example .env
# Set GCP_PROJECT in .env.

ghiw init-db

ghiw collect-org-monthly \
  --start-month 2015-01 \
  --end-month 2025-12 \
  --dry-run

ghiw collect-org-monthly \
  --start-month 2015-01 \
  --end-month 2025-12 \
  --maximum-bytes-billed 5000000000000

ghiw export-org-json \
  --start-month 2015-01 \
  --end-month 2025-12 \
  --output-dir exports/organizations
```

Both collection and export are restart-safe. Organization-month rows are upserted by `(org_login, month)`, and each JSON file is written through a temporary file followed by an atomic replacement.

## Output layout

```text
exports/organizations/
  manifest.json
  ra/
    rapidfuzz.json
  ve/
    vuejs.json
```

Prefix sharding avoids placing a potentially very large number of files in one directory.

## JSON contract

Each organization file follows this structure; the real default output contains 132 monthly objects:

```json
{
  "schema_version": "1.0",
  "organization": {"login": "example-org"},
  "period": {"start": "2015-01", "end": "2025-12", "months": 132},
  "source": "GH Archive public events via BigQuery",
  "coverage": {
    "definition": "Public events where GitHub supplied org.login",
    "zero_filled_months": true,
    "private_activity_included": false
  },
  "totals": {
    "public_events": 100,
    "push_events": 40,
    "commits_in_pushes": 75,
    "issues_opened": 10,
    "pull_requests_opened": 12,
    "repositories_created": 2,
    "fork_events": 5,
    "star_events": 25,
    "release_events": 6
  },
  "monthly_activity_summary": {
    "months_with_activity": 18,
    "sum_monthly_active_repositories": 30,
    "max_monthly_active_repositories": 4,
    "sum_monthly_distinct_actors": 80,
    "max_monthly_distinct_actors": 12
  },
  "monthly": [
    {
      "month": "2015-01",
      "public_events": 0,
      "active_repositories": 0,
      "distinct_actors": 0,
      "push_events": 0,
      "commits_in_pushes": 0,
      "issues_opened": 0,
      "pull_requests_opened": 0,
      "repositories_created": 0,
      "fork_events": 0,
      "star_events": 0,
      "release_events": 0
    }
  ]
}
```

`active_repositories` and `distinct_actors` are monthly distinct counts. They are not unique counts across the complete period, so the JSON does not present their sums as period-level unique totals.

## Querying the warehouse

```bash
ghiw query --sql "
SELECT *
FROM organization_public_activity_totals
ORDER BY public_events DESC
LIMIT 100
"
```

```bash
ghiw query --sql "
SELECT month, count(*) organizations, sum(public_events) public_events
FROM organization_public_monthly
GROUP BY month
ORDER BY month
"
```

## Practical safeguards

- Use a dedicated Google Cloud project with a budget alert.
- Dry-run before every changed query or expanded date window.
- Set `--maximum-bytes-billed` on the production command.
- Keep the DuckDB file and JSON output on storage with sufficient inode capacity.
- For very large organization counts, consider producing Parquet as the primary analytical artifact and JSON only for downstream systems that require it.
