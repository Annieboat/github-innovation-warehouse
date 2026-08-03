import csv
import json
from datetime import date

import pytest

from github_innovation.bulk import YearMonth
from github_innovation.targeted import (
    TARGET_METRICS,
    LocalJsonSink,
    TargetOrganizationJSONExporter,
    normalize_target_csv,
    parse_shard_spec,
    plan_throughput,
    split_shards,
    yearly_aggregation_sql,
)


def test_normalize_target_csv_accepts_bom_and_full_extract(tmp_path):
    source = tmp_path / "targets.csv"
    destination = tmp_path / "normalized.csv"
    source.write_text(
        "\ufeffhistorical_login,organization_id,public_event_count\n"
        "skillmap,62525946,14\n"
        "skillmapper,23345238,1\n",
        encoding="utf-8",
    )

    stats = normalize_target_csv(source, destination)

    assert stats.rows == 2
    with destination.open(newline="", encoding="utf-8") as handle:
        assert list(csv.DictReader(handle)) == [
            {"org_id": "62525946", "historical_login": "skillmap"},
            {"org_id": "23345238", "historical_login": "skillmapper"},
        ]


def test_normalize_target_csv_rejects_invalid_ids(tmp_path):
    source = tmp_path / "targets.csv"
    source.write_text("organization_id\n123\nbad\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"line\(s\) 3"):
        normalize_target_csv(source, tmp_path / "normalized.csv")


def test_normalize_target_csv_accepts_observed_organization_bigquery_export(tmp_path):
    source = tmp_path / "observed_organizations.csv"
    destination = tmp_path / "normalized.csv"
    source.write_text(
        "\ufeffhistorical_login,first_observed_at,public_event_count,organization_id,"
        "is_observed_organization,event_types\n"
        "skillmap,2020-03-23 00:02:07 UTC,14,62525946,TRUE,CreateEvent|PushEvent\n"
        "skillmapper,2016-11-08 20:16:48 UTC,1,23345238,TRUE,CreateEvent\n",
        encoding="utf-8",
    )

    stats = normalize_target_csv(source, destination)

    assert stats.rows == 2
    assert destination.read_text(encoding="utf-8").splitlines() == [
        "org_id,historical_login",
        "62525946,skillmap",
        "23345238,skillmapper",
    ]


def test_yearly_sql_matches_stable_ids_and_one_year_only():
    query = yearly_aggregation_sql(
        "example-project", "github_data", "organization_targets", "monthly_2020", 2020
    )

    assert "_TABLE_SUFFIX BETWEEN '202001' AND '202012'" in query
    assert "SAFE_CAST(org.id AS INT64)" in query
    assert "INNER JOIN `example-project.github_data.organization_targets`" in query
    assert "fork_destination_org_id" in query


def test_target_export_zero_fills_and_keys_file_by_id(tmp_path):
    sink = LocalJsonSink(tmp_path, output_prefixes=16)
    exporter = TargetOrganizationJSONExporter(
        "example-project", "github_data", sink, export_shards=1
    )
    first = {metric: 0 for metric in TARGET_METRICS}
    first.update(
        {
            "public_events": 5,
            "active_repositories": 2,
            "distinct_actors": 3,
            "push_events": 2,
            "commits_in_pushes": 7,
        }
    )
    row = {
        "org_id": 62525946,
        "historical_login": "skillmap",
        "month": date(2020, 3, 1),
        **first,
    }

    count = exporter._write_rows([row], YearMonth.parse("2020-01"), YearMonth.parse("2020-04"))

    assert count == 1
    path = tmp_path / "00a" / "62525946.json"
    payload = json.loads(path.read_text())
    assert payload["organization"] == {"id": 62525946, "historical_login": "skillmap"}
    assert len(payload["monthly"]) == 4
    assert payload["monthly"][0]["public_events"] == 0
    assert payload["monthly"][2]["commits_in_pushes"] == 7
    assert payload["totals"]["commits_in_pushes"] == 7


@pytest.mark.parametrize(
    ("spec", "expected"),
    [("0-2,5", [0, 1, 2, 5]), ("7", [7]), (None, None)],
)
def test_parse_shard_spec(spec, expected):
    assert parse_shard_spec(spec, 8) == expected


def test_throughput_plan_for_three_million_files_in_twenty_days():
    plan = plan_throughput(3_000_000, 20, safety_factor=3)

    assert plan.required_per_second == pytest.approx(1.7361111111)
    assert plan.target_per_second == pytest.approx(5.2083333333)
    assert plan.required_per_day == 150_000


def test_throughput_plan_projects_pilot_and_deadline_status():
    plan = plan_throughput(
        3_000_000,
        20,
        sample_organizations=10_000,
        sample_seconds=1_000,
    )

    assert plan.observed_per_second == 10
    assert plan.projected_days == pytest.approx(3.4722222222)
    assert plan.meets_deadline is True


def test_split_shards_across_four_machines():
    assignments = split_shards(256, 4)

    assert [assignment.shard_spec for assignment in assignments] == [
        "0-63",
        "64-127",
        "128-191",
        "192-255",
    ]
