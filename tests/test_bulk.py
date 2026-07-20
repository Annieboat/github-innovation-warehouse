import json
from datetime import UTC, date, datetime

import pytest

from github_innovation.bulk import METRICS, OrganizationJSONExporter, YearMonth, iter_months
from github_innovation.db import Warehouse


def test_year_month_range_is_inclusive():
    months = list(iter_months(YearMonth.parse("2025-11"), YearMonth.parse("2026-02")))
    assert [month.iso() for month in months] == ["2025-11", "2025-12", "2026-01", "2026-02"]


@pytest.mark.parametrize("value", ["2014-12", "2025-13", "2025-1", "bad"])
def test_invalid_months_are_rejected(value):
    with pytest.raises(ValueError):
        YearMonth.parse(value)


def test_export_writes_one_zero_filled_json_per_organization(tmp_path):
    database = tmp_path / "warehouse.duckdb"
    output = tmp_path / "organizations"
    columns = ("org_login", "month", *METRICS, "source", "collected_at")
    values = {
        "public_events": 10,
        "active_repositories": 2,
        "distinct_actors": 3,
        "push_events": 4,
        "commits_in_pushes": 6,
        "issues_opened": 1,
        "pull_requests_opened": 2,
        "repositories_created": 1,
        "fork_events": 1,
        "star_events": 1,
        "release_events": 0,
    }
    row = (
        "example-org",
        date(2025, 1, 1),
        *(values[name] for name in METRICS),
        "gharchive_bigquery",
        datetime.now(UTC),
    )
    with Warehouse(database) as warehouse:
        warehouse.initialize()
        warehouse.upsert_many(
            "organization_public_monthly", columns, [row], ["org_login", "month"]
        )
        manifest = OrganizationJSONExporter(warehouse, output).export(
            YearMonth.parse("2025-01"), YearMonth.parse("2025-03")
        )

    assert manifest["organization_count"] == 1
    payload = json.loads((output / "ex" / "example-org.json").read_text())
    assert len(payload["monthly"]) == 3
    assert payload["monthly"][0]["commits_in_pushes"] == 6
    assert payload["monthly"][1]["public_events"] == 0
    assert payload["totals"]["public_events"] == 10
    assert payload["monthly_activity_summary"]["max_monthly_active_repositories"] == 2
