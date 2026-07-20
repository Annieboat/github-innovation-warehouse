CREATE SEQUENCE IF NOT EXISTS collection_run_seq START 1;

CREATE TABLE IF NOT EXISTS collection_run (
    run_id BIGINT PRIMARY KEY DEFAULT nextval('collection_run_seq'),
    source VARCHAR NOT NULL,
    target VARCHAR NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    finished_at TIMESTAMPTZ,
    status VARCHAR NOT NULL DEFAULT 'running',
    rows_written BIGINT DEFAULT 0,
    error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS organization (
    org_id BIGINT PRIMARY KEY,
    login VARCHAR NOT NULL UNIQUE,
    name VARCHAR,
    description VARCHAR,
    company VARCHAR,
    blog VARCHAR,
    location VARCHAR,
    email VARCHAR,
    html_url VARCHAR,
    public_repos INTEGER,
    followers INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS repository (
    repo_id BIGINT PRIMARY KEY,
    org_id BIGINT NOT NULL,
    name VARCHAR NOT NULL,
    full_name VARCHAR NOT NULL UNIQUE,
    html_url VARCHAR,
    description VARCHAR,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    pushed_at TIMESTAMPTZ,
    default_branch VARCHAR,
    language VARCHAR,
    license_spdx VARCHAR,
    is_fork BOOLEAN NOT NULL,
    parent_full_name VARCHAR,
    source_full_name VARCHAR,
    archived BOOLEAN NOT NULL DEFAULT false,
    disabled BOOLEAN NOT NULL DEFAULT false,
    visibility VARCHAR,
    setup_py_present BOOLEAN,
    dependency_count INTEGER,
    python_setup_eligible BOOLEAN,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS repository_metric_snapshot (
    repo_id BIGINT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    stargazers_count INTEGER,
    forks_count INTEGER,
    watchers_count INTEGER,
    open_issues_count INTEGER,
    subscribers_count INTEGER,
    network_count INTEGER,
    PRIMARY KEY (repo_id, observed_at)
);

CREATE TABLE IF NOT EXISTS setup_dependency (
    repo_id BIGINT NOT NULL,
    package_name VARCHAR NOT NULL,
    requirement VARCHAR NOT NULL,
    source VARCHAR NOT NULL DEFAULT 'setup.py',
    collected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (repo_id, package_name, requirement)
);

CREATE TABLE IF NOT EXISTS commit_activity (
    repo_id BIGINT NOT NULL,
    sha VARCHAR NOT NULL,
    author_login VARCHAR,
    committer_login VARCHAR,
    author_name VARCHAR,
    author_email VARCHAR,
    author_time TIMESTAMPTZ,
    committer_time TIMESTAMPTZ,
    message VARCHAR,
    parents_count INTEGER,
    source VARCHAR NOT NULL,
    event_time TIMESTAMPTZ,
    timestamp_precision VARCHAR NOT NULL DEFAULT 'commit',
    collected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (repo_id, sha)
);

CREATE TABLE IF NOT EXISTS issue (
    issue_id BIGINT PRIMARY KEY,
    repo_id BIGINT NOT NULL,
    number INTEGER NOT NULL,
    author_login VARCHAR,
    assignee_logins JSON,
    state VARCHAR,
    state_reason VARCHAR,
    title VARCHAR,
    body VARCHAR,
    labels JSON,
    comments_count INTEGER,
    locked BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ,
    UNIQUE (repo_id, number)
);

CREATE TABLE IF NOT EXISTS pull_request (
    pull_id BIGINT PRIMARY KEY,
    repo_id BIGINT NOT NULL,
    number INTEGER NOT NULL,
    author_login VARCHAR,
    state VARCHAR,
    draft BOOLEAN,
    title VARCHAR,
    body VARCHAR,
    head_repo_full_name VARCHAR,
    base_repo_full_name VARCHAR,
    commits_count INTEGER,
    additions INTEGER,
    deletions INTEGER,
    changed_files INTEGER,
    comments_count INTEGER,
    review_comments_count INTEGER,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    merged_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ,
    UNIQUE (repo_id, number)
);

CREATE TABLE IF NOT EXISTS gharchive_event (
    event_id VARCHAR PRIMARY KEY,
    event_type VARCHAR NOT NULL,
    actor_login VARCHAR,
    repo_id BIGINT,
    repo_full_name VARCHAR,
    org_login VARCHAR,
    action VARCHAR,
    entity_number INTEGER,
    created_at TIMESTAMPTZ NOT NULL,
    payload JSON NOT NULL,
    archive_hour TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS repository_search_hit (
    query VARCHAR NOT NULL,
    repo_id BIGINT NOT NULL,
    rank INTEGER NOT NULL,
    full_name VARCHAR NOT NULL,
    owner_login VARCHAR,
    owner_type VARCHAR,
    matched_at TIMESTAMPTZ NOT NULL,
    raw JSON NOT NULL,
    PRIMARY KEY (query, repo_id, matched_at)
);

CREATE TABLE IF NOT EXISTS organization_public_monthly (
    org_login VARCHAR NOT NULL,
    month DATE NOT NULL,
    public_events BIGINT NOT NULL,
    active_repositories BIGINT NOT NULL,
    distinct_actors BIGINT NOT NULL,
    push_events BIGINT NOT NULL,
    commits_in_pushes BIGINT NOT NULL,
    issues_opened BIGINT NOT NULL,
    pull_requests_opened BIGINT NOT NULL,
    repositories_created BIGINT NOT NULL,
    fork_events BIGINT NOT NULL,
    star_events BIGINT NOT NULL,
    release_events BIGINT NOT NULL,
    source VARCHAR NOT NULL DEFAULT 'gharchive_bigquery',
    collected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (org_login, month)
);

CREATE OR REPLACE VIEW organization_public_activity_totals AS
SELECT org_login, min(month) first_active_month, max(month) last_active_month,
       sum(public_events) public_events,
       sum(push_events) push_events,
       sum(commits_in_pushes) commits_in_pushes,
       sum(issues_opened) issues_opened,
       sum(pull_requests_opened) pull_requests_opened,
       sum(repositories_created) repositories_created,
       sum(fork_events) fork_events, sum(star_events) star_events,
       sum(release_events) release_events
FROM organization_public_monthly
GROUP BY org_login;

CREATE OR REPLACE VIEW repository_current AS
SELECT r.*, s.stargazers_count, s.forks_count, s.watchers_count,
       s.open_issues_count, s.subscribers_count, s.network_count, s.observed_at
FROM repository r
LEFT JOIN repository_metric_snapshot s
  ON r.repo_id = s.repo_id
 AND s.observed_at = (
     SELECT max(s2.observed_at) FROM repository_metric_snapshot s2 WHERE s2.repo_id = r.repo_id
 );

CREATE OR REPLACE VIEW organization_monthly_activity AS
WITH months AS (
    SELECT r.org_id, date_trunc('month', c.author_time) AS month,
           count(*) AS commits, 0 AS issues, 0 AS pull_requests
    FROM commit_activity c JOIN repository r USING (repo_id)
    GROUP BY 1, 2
    UNION ALL
    SELECT r.org_id, date_trunc('month', i.created_at), 0, count(*), 0
    FROM issue i JOIN repository r USING (repo_id)
    GROUP BY 1, 2
    UNION ALL
    SELECT r.org_id, date_trunc('month', p.created_at), 0, 0, count(*)
    FROM pull_request p JOIN repository r USING (repo_id)
    GROUP BY 1, 2
)
SELECT o.login, month, sum(commits)::BIGINT AS commits,
       sum(issues)::BIGINT AS issues, sum(pull_requests)::BIGINT AS pull_requests
FROM months JOIN organization o USING (org_id)
WHERE month IS NOT NULL
GROUP BY 1, 2;

CREATE OR REPLACE VIEW organization_innovation_summary AS
WITH repo_stats AS (
  SELECT org_id, count(*) repositories,
         count(*) FILTER (WHERE NOT is_fork) original_repositories,
         count(*) FILTER (WHERE is_fork) forked_repositories,
         coalesce(sum(stargazers_count), 0) current_stars
  FROM repository_current GROUP BY org_id
), commit_stats AS (
  SELECT r.org_id, count(*) commits FROM commit_activity c JOIN repository r USING(repo_id) GROUP BY 1
), issue_stats AS (
  SELECT r.org_id, count(*) issues FROM issue i JOIN repository r USING(repo_id) GROUP BY 1
), pull_stats AS (
  SELECT r.org_id, count(*) pull_requests FROM pull_request p JOIN repository r USING(repo_id) GROUP BY 1
)
SELECT o.login, coalesce(rs.repositories, 0) repositories,
       coalesce(rs.original_repositories, 0) original_repositories,
       coalesce(rs.forked_repositories, 0) forked_repositories,
       coalesce(cs.commits, 0) commits, coalesce(isx.issues, 0) issues,
       coalesce(ps.pull_requests, 0) pull_requests, coalesce(rs.current_stars, 0) current_stars
FROM organization o
LEFT JOIN repo_stats rs USING(org_id)
LEFT JOIN commit_stats cs USING(org_id)
LEFT JOIN issue_stats isx USING(org_id)
LEFT JOIN pull_stats ps USING(org_id);
