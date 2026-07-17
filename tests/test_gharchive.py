from datetime import UTC, datetime

from github_innovation.gharchive import archive_commit_rows, event_row, iter_hours


def test_hour_range_is_start_inclusive_end_exclusive():
    hours = list(
        iter_hours(
            datetime(2025, 1, 1, 0, 30, tzinfo=UTC),
            datetime(2025, 1, 1, 2, 0, tzinfo=UTC),
        )
    )
    assert [item.hour for item in hours] == [0, 1]


def test_event_mapping():
    event = {
        "id": "1",
        "type": "IssuesEvent",
        "created_at": "2025-01-01T00:01:00Z",
        "actor": {"login": "alice"},
        "repo": {"id": 2, "name": "acme/app"},
        "org": {"login": "acme"},
        "payload": {"action": "opened", "issue": {"number": 3}},
    }
    row = event_row(event, datetime(2025, 1, 1, tzinfo=UTC))
    assert row["entity_number"] == 3
    assert row["repo_full_name"] == "acme/app"


def test_push_commit_uses_event_time_not_author_time():
    event = {
        "id": "2",
        "type": "PushEvent",
        "created_at": "2025-01-01T00:01:00Z",
        "repo": {"id": 7, "name": "acme/app"},
        "payload": {
            "commits": [
                {"sha": "abc", "message": "change", "author": {"name": "A", "email": "a@x"}}
            ]
        },
    }
    row = list(archive_commit_rows(event))[0]
    assert row["author_time"] is None
    assert row["event_time"] == event["created_at"]
    assert row["timestamp_precision"] == "push_event"
