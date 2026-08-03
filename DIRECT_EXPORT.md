# Direct CSV-to-JSON export (no database)

Use this workflow when you have a CSV containing GitHub `organization_id` and want ordinary files,
not a DuckDB warehouse or persistent BigQuery tables.

## What it writes

For an input with six organizations, the script writes:

```text
outputs/organization_monthly/
  23345238.json
  55314291.json
  62525946.json
  142762711.json
  244375280.json
  251042228.json
  organization_monthly.csv
  manifest.json
```

Each organization JSON contains exactly 132 monthly observations from January 2015 through December
2025. Months without matching public GH Archive activity contain zeros. The combined CSV contains one
row per organization-month, so six organizations produce 792 rows.

The script sends one read-only query to the public `githubarchive.month.*` tables. It does not execute
`CREATE TABLE`, create a BigQuery dataset, or create a local database. Google may use temporary query
storage internally, which expires automatically.

## Terminal instructions

Requirements: Python 3.11+, Google Cloud CLI, and a Google Cloud billing project.

```bash
git clone https://github.com/Annieboat/github-innovation-warehouse.git
cd github-innovation-warehouse

python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-direct.txt

gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

Estimate scanned bytes first. A dry run writes nothing:

```bash
python export_organization_monthly.py \
  --csv "/path/to/organizaton_id.csv" \
  --project YOUR_PROJECT_ID \
  --dry-run
```

Run the export:

```bash
python export_organization_monthly.py \
  --csv "/path/to/organizaton_id.csv" \
  --project YOUR_PROJECT_ID \
  --output outputs/organization_monthly
```

List and inspect the results:

```bash
find outputs/organization_monthly -maxdepth 1 -type f | sort
python -m json.tool outputs/organization_monthly/62525946.json | less
head outputs/organization_monthly/organization_monthly.csv
```

The input can be a direct BigQuery export with a UTF-8 BOM and extra columns. Only
`organization_id` and optional `historical_login` are used. `org_id` and `login` are accepted aliases.

## Fields

The monthly output contains:

- public events, active repositories, and distinct actors;
- push events and commits contained in pushes;
- issues opened/closed and issue comments created;
- pull requests opened/closed/merged;
- original repositories created and repositories forked into the organization;
- forks received, stars received, and releases published.

Only public activity preserved by GH Archive is available. Private activity, exact Git commit objects,
current repository metadata, licenses, and current star/fork snapshots are outside this direct monthly
event export.

## Scale boundary

Direct mode is intentionally simple and supports up to 10,000 unique IDs. BigQuery query parameters
cannot efficiently carry a multi-million-ID census. For millions of IDs, a server-side target table is
unavoidable if the archive is to be scanned efficiently; use the separate scalable targeted workflow.
