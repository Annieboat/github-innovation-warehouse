from __future__ import annotations

import gzip
import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO

import httpx
from rich.console import Console

from .db import Warehouse, utcnow


class GHArchiveCollector:
    event_types = {"PushEvent", "IssuesEvent", "PullRequestEvent", "CreateEvent", "ForkEvent"}

    def __init__(
        self,
        warehouse: Warehouse,
        raw_dir: Path,
        timeout: float = 120,
        console: Console | None = None,
    ):
        self.db = warehouse
        self.raw_dir = raw_dir
        self.timeout = timeout
        self.console = console or Console()

    def collect(self, org: str, start: datetime, end: datetime, keep_raw: bool = False) -> int:
        """Collect [start, end) UTC hours for repos owned by org or events tagged with org."""
        self.db.initialize()
        total = 0
        with self.db.run("gharchive", org):
            for hour in iter_hours(start, end):
                path = self._download(hour)
                self.console.print(f"[cyan]Scanning[/cyan] {hour:%Y-%m-%d %H}:00 UTC")
                with gzip.open(path, "rb") as stream:
                    for event in self._matching_events(stream, org):
                        self.db.insert_ignore("gharchive_event", event_row(event, hour))
                        for commit in archive_commit_rows(event):
                            # Preserve richer GitHub API data if it already exists. A later API
                            # collection will upsert and enrich this event-time-only record.
                            self.db.insert_ignore("commit_activity", commit)
                        total += 1
                if not keep_raw:
                    path.unlink(missing_ok=True)
        return total

    def _download(self, hour: datetime) -> Path:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        name = f"{hour:%Y-%m-%d-%-H}.json.gz"
        path = self.raw_dir / name
        if path.exists():
            return path
        url = f"https://data.gharchive.org/{name}"
        with httpx.stream("GET", url, timeout=self.timeout, follow_redirects=True) as response:
            response.raise_for_status()
            with path.open("wb") as output:
                for chunk in response.iter_bytes():
                    output.write(chunk)
        return path

    def _matching_events(self, stream: BinaryIO, org: str) -> Iterator[dict[str, Any]]:
        prefix = org.lower() + "/"
        for line in stream:
            try:
                event = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if event.get("type") not in self.event_types:
                continue
            repo_name = ((event.get("repo") or {}).get("name") or "").lower()
            event_org = ((event.get("org") or {}).get("login") or "").lower()
            if repo_name.startswith(prefix) or event_org == org.lower():
                yield event


def event_row(event: dict[str, Any], archive_hour: datetime) -> dict[str, Any]:
    payload = event.get("payload") or {}
    entity = payload.get("issue") or payload.get("pull_request") or {}
    return {
        "event_id": str(event["id"]),
        "event_type": event["type"],
        "actor_login": (event.get("actor") or {}).get("login"),
        "repo_id": (event.get("repo") or {}).get("id"),
        "repo_full_name": (event.get("repo") or {}).get("name"),
        "org_login": (event.get("org") or {}).get("login"),
        "action": payload.get("action"),
        "entity_number": entity.get("number"),
        "created_at": event["created_at"],
        "payload": payload,
        "archive_hour": archive_hour,
        "collected_at": utcnow(),
    }


def archive_commit_rows(event: dict[str, Any]) -> Iterator[dict[str, Any]]:
    if event.get("type") != "PushEvent":
        return
    payload = event.get("payload") or {}
    observed_at = utcnow()
    for commit in payload.get("commits") or []:
        if not commit.get("sha") or not (event.get("repo") or {}).get("id"):
            continue
        author = commit.get("author") or {}
        yield {
            "repo_id": event["repo"]["id"],
            "sha": commit["sha"],
            "author_login": None,
            "committer_login": None,
            "author_name": author.get("name"),
            "author_email": author.get("email"),
            "author_time": None,
            "committer_time": None,
            "message": commit.get("message"),
            "parents_count": None,
            "source": "gharchive",
            "event_time": event.get("created_at"),
            "timestamp_precision": "push_event",
            "collected_at": observed_at,
        }


def iter_hours(start: datetime, end: datetime) -> Iterator[datetime]:
    start = _utc(start).replace(minute=0, second=0, microsecond=0)
    end = _utc(end)
    current = start
    while current < end:
        yield current
        current += timedelta(hours=1)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
