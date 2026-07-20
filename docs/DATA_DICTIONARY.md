# Data dictionary

All datetimes are stored as DuckDB `TIMESTAMPTZ`. JSON columns preserve arrays or the source payload when flattening would discard information.

## Core dimensions and facts

| Table | Grain | Key | Purpose |
|---|---|---|---|
| `organization` | one verified organization | `org_id` | focal organization identity and profile |
| `repository` | one current repository record | `repo_id` | innovation unit and original/fork classification |
| `repository_metric_snapshot` | repository-observation | `repo_id, observed_at` | historical stars/forks and other mutable counts |
| `setup_dependency` | repository-requirement | `repo_id, package_name, requirement` | safe static extraction from `setup.py` |
| `commit_activity` | repository-commit | `repo_id, sha` | code-production activity with provenance/precision |
| `issue` | issue | `issue_id` | non-PR issue creation and state |
| `pull_request` | pull request | `pull_id` | collaborative code review and integration |
| `gharchive_event` | public GitHub event | `event_id` | historical event record and raw payload |
| `repository_search_hit` | query-repo-observation | `query, repo_id, matched_at` | reproducible Search API discovery |
| `organization_public_monthly` | organization-month | `org_login, month` | BigQuery aggregation of qualifying public GH Archive events |
| `collection_run` | collection attempt | `run_id` | operational audit trail |

## Commit timestamp semantics

| Source | `author_time` | `committer_time` | `event_time` | `timestamp_precision` |
|---|---:|---:|---:|---|
| `github_api` | commit author timestamp | commit committer timestamp | null | `commit` |
| `gharchive` | null | null | enclosing PushEvent time | `push_event` |

When both sources contain the same `(repo_id, sha)`, full GitHub API data takes precedence. GH Archive ingestion uses insert-if-absent; API ingestion enriches via upsert.

## Analytic views

| View | Purpose |
|---|---|
| `repository_current` | repository dimension plus its latest metric snapshot |
| `organization_monthly_activity` | monthly commits, issue creation, and PR creation |
| `organization_innovation_summary` | organization-level portfolio/activity totals without join inflation |
| `organization_public_activity_totals` | additive public-event totals for each observed organization |

## Bulk monthly measures

| Field | Definition |
|---|---|
| `public_events` | qualifying public events with a non-null `org.login` |
| `active_repositories` | distinct repository IDs observed during that month; not additive across months |
| `distinct_actors` | distinct actor IDs observed during that month; not additive across months |
| `push_events` | number of `PushEvent` records |
| `commits_in_pushes` | number of commit objects carried inside `PushEvent` payloads; not a complete git history |
| `issues_opened` | `IssuesEvent` records with action `opened` |
| `pull_requests_opened` | `PullRequestEvent` records with action `opened` |
| `repositories_created` | `CreateEvent` records whose `ref_type` is `repository` |
| `fork_events` | `ForkEvent` records |
| `star_events` | `WatchEvent` records with action `started` |
| `release_events` | `ReleaseEvent` records with action `published` |
