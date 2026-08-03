# Targeted monthly collection for roughly three million GitHub organizations

## Outcome

This workflow accepts a CSV containing `organization_id`, aggregates public GH Archive activity from
January 2015 through December 2025, and writes one JSON file per target organization. Every file has
exactly 132 monthly records. Months without qualifying public events are explicit zero rows.

Stable numeric organization IDs are the primary key. A historical login can change and is retained
only as descriptive metadata.

## Why this architecture is fast

Do not call the GitHub REST API once per organization and do not run one BigQuery query per ID. At
three million organizations, either approach is impractical. The targeted pipeline instead:

1. streams and normalizes the CSV without holding three million IDs in memory;
2. loads the IDs into a range-partitioned BigQuery table;
3. scans each GH Archive year once, with multiple years allowed to run concurrently;
4. joins archive events to targets by stable `org.id` inside BigQuery;
5. stores only nonzero organization-month rows in a partitioned and clustered sparse table;
6. reads one of 256 prunable export shards at a time;
7. zero-fills months while streaming one organization at a time; and
8. writes files across 4,096 prefixes with per-shard `_SUCCESS` markers.

The eleven yearly tables are checkpoints. `--resume` skips completed years, and the export skips
completed shards. A failure therefore does not restart the eleven-year scan or three-million-file
export.

Checkpoint tables belong to the target census that created them. After replacing
`organization_targets` with a different CSV, run collection once with `--no-resume` (or use a new
`--output-table` name) so annual tables from the previous census are not reused.

## Input contract

Required column:

```text
organization_id
```

Optional descriptive column:

```text
historical_login
```

`org_id` and `login` are accepted aliases. UTF-8 files with or without a BOM are supported. Extra
columns are ignored. IDs must be positive integers. BigQuery removes duplicate IDs after loading.

Example:

```csv
historical_login,organization_id
skillmap,62525946
skillmapper,23345238
skillmappr,142762711
```

## Installation and authentication

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,targeted]"
gcloud auth application-default login
```

Set `.env`:

```dotenv
GCP_PROJECT=your-google-cloud-project
GHIW_TARGET_DATASET=github_data
GHIW_TARGET_JSON_OUTPUT=gs://your-bucket/github-organizations-2015-2025
```

The BigQuery dataset and Cloud Storage bucket should both be in `US`, because the GH Archive
BigQuery tables are in the US multi-region.

## Step 1: load targets

```bash
ghiw load-org-targets \
  --csv /path/to/organization_id.csv \
  --dataset github_data \
  --table organization_targets \
  --export-shards 256
```

Output table:

```text
PROJECT.github_data.organization_targets
```

Fields:

| Field | Type | Definition |
|---|---|---|
| `org_id` | INT64 | Stable GitHub organization ID |
| `historical_login` | STRING | Optional historical login from the input |
| `export_shard` | INT64 | `MOD(org_id, 256)` for parallel and resumable export |

Confirm the deduplicated population:

```sql
SELECT COUNT(*) AS organizations, COUNT(DISTINCT org_id) AS unique_organizations
FROM `PROJECT.github_data.organization_targets`;
```

## Step 2: estimate and aggregate

The dry run is free and reports the sum of estimated bytes across the eleven year jobs:

```bash
ghiw collect-target-org-monthly \
  --dataset github_data \
  --start-month 2015-01 \
  --end-month 2025-12 \
  --workers 4 \
  --dry-run
```

`--maximum-bytes-billed` is applied to each yearly query, not to the eleven-query total. Use the dry
run total to approve the complete job. Then run:

```bash
ghiw collect-target-org-monthly \
  --dataset github_data \
  --start-month 2015-01 \
  --end-month 2025-12 \
  --workers 4 \
  --resume
```

Checkpoint tables are named:

```text
organization_target_monthly_sparse_2015
...
organization_target_monthly_sparse_2025
```

The consolidated table is:

```text
organization_target_monthly_sparse
```

It is partitioned by `month` and clustered by `export_shard, org_id`.

### Monthly fields

- `public_events`
- `active_repositories`
- `distinct_actors`
- `push_events`
- `commits_in_pushes`
- `issues_opened`
- `issues_closed`
- `issue_comments_created`
- `pull_requests_opened`
- `pull_requests_closed`
- `pull_requests_merged`
- `original_repositories_created`
- `forked_repositories_created`
- `forks_received`
- `stars_received`
- `releases_published`

`commits_in_pushes` uses `payload.size` and falls back to the displayed commit-array length. Commit
time is the enclosing public push-event time, not an individual Git commit timestamp.

## Step 3: export one JSON per ID

Cloud Storage is recommended for three million files:

```bash
ghiw export-target-org-json \
  --dataset github_data \
  --output gs://YOUR_BUCKET/github-organizations-2015-2025 \
  --workers 32 \
  --gzip \
  --resume
```

For a local test:

```bash
ghiw export-target-org-json \
  --dataset github_data \
  --output exports/target-organizations \
  --workers 4 \
  --shards 0-3 \
  --no-gzip
```

`--shards` accepts comma-separated numbers and ranges, such as `0-31,40,52`. This makes it easy to
distribute export shards across separate VMs without overlap.

Output paths use 4,096 hexadecimal prefixes:

```text
00a/62525946.json.gz
f31/142762711.json.gz
manifest.json
_shards/0000.json
...
```

The completion marker for a shard is written only after every organization in that shard succeeds.
With `--resume`, completed shards require one existence check rather than one check per organization.

## JSON contract

```json
{
  "schema_version": "2.0",
  "organization": {
    "id": 62525946,
    "historical_login": "skillmap"
  },
  "period": {
    "start": "2015-01",
    "end": "2025-12",
    "months": 132
  },
  "source": "GH Archive monthly tables via BigQuery",
  "coverage": {
    "matched_by": "stable GitHub organization ID",
    "zero_filled_months": true,
    "private_activity_included": false
  },
  "totals": {},
  "monthly_activity_summary": {},
  "monthly": []
}
```

## Production performance guidance

Use `gs://` output rather than a laptop filesystem. Three million files create substantial metadata
and inode pressure locally. Gzip is recommended because each file repeats 132 field names and many
zero values.

Recommended starting settings:

```text
BigQuery year workers: 4
JSON workers:          32
Export shards:         256
Output prefixes:       4,096
```

Increase BigQuery workers only after checking project slot contention. Increase JSON workers
gradually while monitoring Cloud Storage throttling and BigQuery concurrent query usage. GitHub REST
API tokens are not used in this workflow.

Wall-clock time cannot be inferred from ID count alone. The archive scan depends mainly on selected
years and BigQuery capacity, while the export depends on sparse-row count, object size, compression,
worker count, and Cloud Storage request throughput. Benchmark shards `0-3`, then estimate:

```text
full JSON time ~= sample elapsed time * 256 / 4
```

This extrapolation is more reliable than a generic estimate. Three million targets imply three
million objects and 396 million explicit monthly entries, regardless of how sparse public activity
is.

## Coverage limits

- Only public events preserved by GH Archive are included.
- Private activity and private-only organizations are unavailable.
- An event without a usable `org.id` cannot be matched by stable ID.
- Deleted historical events or repositories may not be recoverable.
- Repository metadata, licenses, exact commit objects, and current metric snapshots require the
  separate GitHub REST collection workflow; they are not monthly GH Archive measures.
