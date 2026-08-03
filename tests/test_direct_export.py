import csv
import importlib.util
import json
import sys
from datetime import date
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "export_organization_monthly.py"
SPEC = importlib.util.spec_from_file_location("export_organization_monthly", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
direct_export = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = direct_export
SPEC.loader.exec_module(direct_export)

METRICS = direct_export.METRICS
YearMonth = direct_export.YearMonth
build_panel = direct_export.build_panel
monthly_query = direct_export.monthly_query
read_targets = direct_export.read_targets
write_outputs = direct_export.write_outputs


def test_attached_bigquery_csv_shape_is_accepted(tmp_path):
    source = tmp_path / "organizaton_id.csv"
    source.write_text(
        "\ufeffhistorical_login,first_observed_at,public_event_count,organization_id,"
        "is_observed_organization,event_types\n"
        "skillmap,2020-03-23 00:02:07 UTC,14,62525946,TRUE,CreateEvent|PushEvent\n"
        "skillmapper,2016-11-08 20:16:48 UTC,1,23345238,TRUE,CreateEvent\n",
        encoding="utf-8",
    )

    assert read_targets(source) == {62525946: "skillmap", 23345238: "skillmapper"}


def test_query_is_read_only_and_filters_supplied_ids():
    query = monthly_query(YearMonth.parse("2015-01"), YearMonth.parse("2025-12"))

    assert "CREATE " not in query.upper()
    assert "INSERT " not in query.upper()
    assert "_TABLE_SUFFIX BETWEEN '201501' AND '202512'" in query
    assert "IN UNNEST(@organization_ids)" in query


def test_none_maximum_bytes_is_not_sent_to_bigquery(monkeypatch):
    import google.cloud

    captured = {}

    class FakeJobConfig:
        def __init__(self, **kwargs):
            captured["config_kwargs"] = kwargs

    class FakeJob:
        total_bytes_processed = 0

        @staticmethod
        def result(page_size):
            assert page_size == 10_000
            return []

    class FakeClient:
        def __init__(self, project, location):
            assert project == "example-project"
            assert location == "US"

        @staticmethod
        def query(query, job_config, location):
            assert query
            assert job_config
            assert location == "US"
            return FakeJob()

    class FakeBigQuery:
        Client = FakeClient
        QueryJobConfig = FakeJobConfig

        @staticmethod
        def ArrayQueryParameter(name, kind, values):
            return name, kind, values

    monkeypatch.setattr(google.cloud, "bigquery", FakeBigQuery, raising=False)

    rows, processed = direct_export.collect_rows(
        "example-project",
        [62525946],
        YearMonth.parse("2015-01"),
        YearMonth.parse("2025-12"),
        maximum_bytes_billed=None,
    )

    assert list(rows) == []
    assert processed == 0
    assert "maximum_bytes_billed" not in captured["config_kwargs"]


def test_panel_zero_fills_and_writes_json_and_combined_csv(tmp_path):
    values = {metric: 0 for metric in METRICS}
    values.update(public_events=3, push_events=1, commits_in_pushes=4)
    panel = build_panel(
        {62525946: "skillmap", 23345238: "skillmapper"},
        [
            {
                "organization_id": 62525946,
                "month": date(2020, 2, 1),
                **values,
            }
        ],
        YearMonth.parse("2020-01"),
        YearMonth.parse("2020-03"),
    )

    manifest = write_outputs(panel, tmp_path)

    assert manifest["organization_count"] == 2
    assert manifest["monthly_row_count"] == 6
    assert manifest["persistent_database_created"] is False
    first = json.loads((tmp_path / "62525946.json").read_text())
    second = json.loads((tmp_path / "23345238.json").read_text())
    assert len(first["monthly"]) == 3
    assert first["monthly"][1]["commits_in_pushes"] == 4
    assert second["monthly"][1]["public_events"] == 0
    with (tmp_path / "organization_monthly.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6
    assert rows[0]["organization_id"] == "62525946"
