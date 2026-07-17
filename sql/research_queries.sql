-- Monthly code production and collaboration.
SELECT * FROM organization_monthly_activity ORDER BY login, month;

-- Python repositories that pass the setup.py / more-than-one-library screen.
SELECT o.login, r.full_name, r.dependency_count, list(d.requirement ORDER BY d.package_name) dependencies
FROM repository r
JOIN organization o USING (org_id)
JOIN setup_dependency d USING (repo_id)
WHERE r.python_setup_eligible
GROUP BY o.login, r.full_name, r.dependency_count
ORDER BY o.login, r.full_name;

-- AI-keyword screen across name and description (case-insensitive).
SELECT o.login, r.full_name, r.description
FROM repository r JOIN organization o USING (org_id)
WHERE regexp_matches(
  lower(coalesce(r.name, '') || ' ' || coalesce(r.description, '')),
  'machine learning|artificial intelligence|natural language processing|deep learning|predictive api|cognitive computing|image recognition|speech recognition'
);

-- Daily GH Archive activity by type.
SELECT org_login, date_trunc('day', created_at) day, event_type, count(*) events
FROM gharchive_event
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

