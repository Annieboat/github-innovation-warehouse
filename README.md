# GitHub Innovation Warehouse

A research-oriented pipeline that collects public GitHub organization activity, preserves it in a local DuckDB warehouse, and exposes reproducible SQL/CLI measures for repositories, commits, issues, and pull requests.

It implements the empirical concepts in the supplied research specification:

- repository creation as the closest proxy for stand-alone innovation;
- original versus forked repositories;
- commits as code-production/improvement activity;
- issues and pull requests as collaboration and quality signals;
- cumulative, strictly pre-cutoff measures and `ln(1 + x)` transformations;
- zero-commit “open-source washer” versus producer indicators;
- organization-account verification;
- AI-keyword screening; and
- the optional Python + `setup.py` + more-than-one dependency sample restriction.

## What is collected

| Entity | Important fields | Primary source |
|---|---|---|
| Organization | GitHub ID, login, profile, account dates, public repository count | GitHub REST API |
| Repository | name, creation date, license, original/fork, parent/source, language, default branch | GitHub REST API |
| Metric snapshot | stars, forks, watchers, open issues, subscribers, network size, observation time | GitHub REST API |
| Commit | SHA, author/committer identity and time, message, parent count, timestamp precision | GitHub REST API and GH Archive |
| Issue | author, state, labels, timestamps, comment count | GitHub REST API |
| Pull request | author, branch repos, state, merge time, commits/additions/deletions/reviews | GitHub REST API |
| Search hit | query, rank, repository and owner identity, raw result | GitHub Search API |
| Public event | event type, actor, repository, action, entity number, event time, payload | GH Archive |
| Dependency | normalized package name and original requirement text | static `setup.py` AST parsing |

Only public data is collected. The pipeline never executes repository code. It parses only literal `install_requires` lists in `setup.py`.

## Quick start

Requirements: Python 3.11+ and a GitHub token. A token is strongly recommended because unauthenticated API limits are very low.

```bash
git clone <this-repository>
cd github-innovation-warehouse
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
cp .env.example .env
# Add a read-only fine-grained token to .env as GITHUB_TOKEN=...
ghiw init-db
ghiw collect --config config/targets.example.yml
ghiw query --sql "SELECT * FROM organization_innovation_summary"
```

The example configuration targets the organizations that own the supplied repositories: `rapidfuzz/RapidFuzz` and `vuejs/vitepress`. Edit `config/targets.example.yml` to add organizations, restrict repository names, or set observation windows.

### Search API discovery

Search and persist up to 1,000 ranked results from a GitHub repository query:

```bash
ghiw search-repositories \
  --query '"machine learning" language:Python stars:>=10 fork:false' \
  --limit 500
```

Search is best used for discovery. Organization repository enumeration uses `/orgs/{org}/repos`, because Search API queries are capped at 1,000 results and are not a complete census for large organizations.

### GH Archive history

GH Archive files are hourly and can be large. Start with a small UTC interval:

```bash
ghiw collect-gharchive \
  --org rapidfuzz \
  --start 2025-01-01T00:00:00Z \
  --end 2025-01-02T00:00:00Z
```

The interval is start-inclusive and end-exclusive. The command streams each compressed hour, keeps only relevant `PushEvent`, `IssuesEvent`, `PullRequestEvent`, `CreateEvent`, and `ForkEvent` records, and deletes the downloaded file unless `--keep-raw` is set. Re-running is safe because event IDs and commit SHAs are deduplicated.

### Query and export

```bash
# Table output
ghiw query --sql "SELECT * FROM repository_current ORDER BY stargazers_count DESC LIMIT 20"

# SQL file to CSV
ghiw query --file sql/example_queries.sql --output exports/repositories.csv

# Research measures strictly before a fundraising cutoff
ghiw metrics --org rapidfuzz --cutoff 2025-01-01T00:00:00Z
```

The query command accepts read-only SQL only. DuckDB can also query the database directly:

```bash
duckdb data/github_warehouse.duckdb
```

See `sql/research_queries.sql` for monthly activity, the Python/setup.py restriction, AI keyword discovery, and GH Archive event counts.

## Research-measure definitions

For organization `o` and cutoff `t`, `ghiw metrics` computes:

- `repos`: repositories created before `t`;
- `original_repos` / `forked_repos`: split using GitHub’s fork flag;
- `commits`: commits whose best available timestamp is before `t`;
- `issues` / `pulls`: records created before `t`;
- `pull_log = ln(1 + pulls)` and `issues_log = ln(1 + issues)`;
- `pull_dummy` / `issues_dummy`: one when the corresponding count is positive;
- `open_source_washer`: one when pre-cutoff commits equal zero; and
- `open_source_producer`: one when pre-cutoff commits are positive.

For a defensible washer classification, distinguish “observed zero” from “not observed.” The collection window must cover the organization account’s entire period before the cutoff, all in-scope repositories, and the relevant default branches/history.

## Important measurement boundaries

1. **Organization identification.** `GET /orgs/{login}` must return `type=Organization`; personal accounts are rejected. An off-platform firm-to-GitHub mapping still requires a documented matching procedure.
2. **Commit coverage.** The REST commit endpoint traverses the repository’s default-branch history. It does not automatically enumerate every branch. GH Archive `PushEvent` commits cover observed public pushes, not an authoritative git history.
3. **Commit time.** REST records have author and committer timestamps. GH Archive commits have only the enclosing push-event time, stored as `event_time` with `timestamp_precision='push_event'` and never mislabelled as author time.
4. **Issues endpoint.** GitHub’s issue listing includes pull requests; the collector explicitly excludes objects containing `pull_request` and collects PRs separately.
5. **Mutable metrics.** Stars and forks change over time. Each collection creates a dated snapshot; `repository_current` selects the latest.
6. **Historical availability.** GitHub API state is current and may omit deleted/private repositories or deleted events. GH Archive starts in 2011 and records public events, not private activity.
7. **Identity.** A commit author email/name is not necessarily a GitHub organization member. Firm-level attribution rules should be specified before treating all commits to firm-owned repos as employee production.
8. **Search completeness.** Search API returns no more than 1,000 results per query. Partition broad discovery queries by time, stars, language, or another qualifier.
9. **Licensing and privacy.** Respect GitHub’s terms, API rate limits, researcher ethics requirements, and personal-data minimization. Author emails are stored because the specification requests user activity; remove that column if not essential.

## Project layout

```text
src/github_innovation/
  cli.py          CLI entry points
  config.py       typed YAML/environment configuration
  github.py       GitHub REST/Search client and safe setup.py parser
  gharchive.py    hourly GH Archive streaming collector
  pipeline.py     normalized API-to-warehouse pipeline
  db.py           DuckDB lifecycle, runs, and idempotent writes
  schema.sql      tables and analytic views
  metrics.py      pre-cutoff empirical measures
  query.py        read-only query and CSV export
config/           target definitions
sql/              reusable research queries
tests/            unit and warehouse tests
```

## Operations

Collection is incremental and idempotent. Entity tables use stable GitHub IDs or `(repo_id, sha)`, GH Archive uses event IDs, and metric snapshots use `(repo_id, observed_at)`. Every job is logged in `collection_run` as running, succeeded, or failed.

For scheduled production runs, execute `ghiw collect` daily/weekly for state and snapshots, and `ghiw collect-gharchive` in bounded hourly/date chunks. Keep a durable copy of the `.duckdb` file and, if auditability requires it, raw archive files.

Docker is optional:

```bash
cp .env.example .env
docker compose run --rm collector init-db
docker compose run --rm collector collect --config config/targets.example.yml
```

## Development

```bash
make install
make lint
make test
```

The default CI workflow runs Ruff and pytest on every push and pull request.

