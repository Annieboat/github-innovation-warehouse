-- Current repository portfolio for each organization.
SELECT
  o.login,
  r.name,
  CASE WHEN r.is_fork THEN 'forked' ELSE 'original' END AS origin,
  r.created_at,
  r.license_spdx,
  r.stargazers_count,
  r.forks_count,
  r.python_setup_eligible
FROM repository_current r
JOIN organization o USING (org_id)
ORDER BY o.login, r.created_at;

