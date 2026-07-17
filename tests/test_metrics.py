from github_innovation.metrics import metrics_sql
from github_innovation.query import ensure_read_only


def test_metrics_query_is_read_only():
    ensure_read_only(metrics_sql("example", "2025-01-01T00:00:00Z"))


def test_query_rejects_mutation():
    try:
        ensure_read_only("DELETE FROM repository")
    except ValueError:
        return
    raise AssertionError("mutating SQL should be rejected")
