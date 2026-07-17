from __future__ import annotations

AI_KEYWORDS = (
    "machine learning",
    "artificial intelligence",
    "natural language processing",
    "deep learning",
    "predictive api",
    "cognitive computing",
    "image recognition",
    "speech recognition",
)


def metrics_sql(org: str, cutoff: str) -> str:
    """Return startup-level proxies measured strictly before a cutoff timestamp."""
    escaped_org = org.replace("'", "''")
    escaped_cutoff = cutoff.replace("'", "''")
    return f"""
WITH scoped_repos AS (
  SELECT r.repo_id, r.is_fork, r.created_at
  FROM repository r JOIN organization o USING (org_id)
  WHERE lower(o.login) = lower('{escaped_org}')
), measures AS (
  SELECT
    (SELECT count(*) FROM scoped_repos WHERE created_at < TIMESTAMPTZ '{escaped_cutoff}') repos,
    (SELECT count(*) FROM scoped_repos WHERE NOT is_fork AND created_at < TIMESTAMPTZ '{escaped_cutoff}') original_repos,
    (SELECT count(*) FROM scoped_repos WHERE is_fork AND created_at < TIMESTAMPTZ '{escaped_cutoff}') forked_repos,
    (SELECT count(*) FROM commit_activity c JOIN scoped_repos s USING(repo_id)
       WHERE coalesce(c.author_time, c.event_time) < TIMESTAMPTZ '{escaped_cutoff}') commits,
    (SELECT count(*) FROM issue i JOIN scoped_repos s USING(repo_id)
       WHERE i.created_at < TIMESTAMPTZ '{escaped_cutoff}') issues,
    (SELECT count(*) FROM pull_request p JOIN scoped_repos s USING(repo_id)
       WHERE p.created_at < TIMESTAMPTZ '{escaped_cutoff}') pulls
)
SELECT *, ln(1 + pulls) AS pull_log, ln(1 + issues) AS issues_log,
       (pulls > 0)::INTEGER AS pull_dummy, (issues > 0)::INTEGER AS issues_dummy,
       (commits = 0)::INTEGER AS open_source_washer,
       (commits > 0)::INTEGER AS open_source_producer
FROM measures
""".strip()
