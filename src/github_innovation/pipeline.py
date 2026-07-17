from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from rich.console import Console

from .config import OrganizationTarget, PipelineConfig
from .db import Warehouse, utcnow
from .github import GitHubClient, parse_setup_requirements


class GitHubPipeline:
    def __init__(self, api: GitHubClient, warehouse: Warehouse, console: Console | None = None):
        self.api = api
        self.db = warehouse
        self.console = console or Console()

    def collect(self, config: PipelineConfig) -> None:
        self.db.initialize()
        for target in config.organizations:
            with self.db.run("github_api", target.login):
                self._collect_organization(target, config)

    def _collect_organization(self, target: OrganizationTarget, config: PipelineConfig) -> None:
        observed_at = utcnow()
        org = self.api.organization(target.login)
        self.db.upsert("organization", organization_row(org, observed_at), ["org_id"])
        selected = {name.lower() for name in target.repositories or []}
        repos = self.api.repositories(target.login)
        for summary in repos:
            if selected and summary["name"].lower() not in selected:
                continue
            repo = self.api.repository(target.login, summary["name"])
            if repo.get("archived") and not config.collect.include_archived:
                continue
            if repo.get("fork") and not config.collect.include_forks:
                continue
            self.console.print(f"[cyan]Collecting[/cyan] {repo['full_name']}")
            dependencies: list[tuple[str, str]] = []
            setup_present: bool | None = None
            if config.collect.setup_dependencies:
                setup = self.api.setup_py(target.login, repo["name"], repo["default_branch"])
                setup_present = setup is not None
                dependencies = parse_setup_requirements(setup or "")
            self.db.upsert(
                "repository",
                repository_row(repo, org["id"], observed_at, setup_present, len(dependencies)),
                ["repo_id"],
            )
            self.db.insert_ignore(
                "repository_metric_snapshot", repository_snapshot(repo, observed_at)
            )
            for package, requirement in dependencies:
                self.db.upsert(
                    "setup_dependency",
                    {
                        "repo_id": repo["id"],
                        "package_name": package,
                        "requirement": requirement,
                        "source": "setup.py",
                        "collected_at": observed_at,
                    },
                    ["repo_id", "package_name", "requirement"],
                )
            if config.collect.commits:
                for commit in self.api.commits(
                    target.login, repo["name"], target.since, config.until
                ):
                    self.db.upsert(
                        "commit_activity",
                        commit_row(commit, repo["id"], observed_at),
                        ["repo_id", "sha"],
                    )
            if config.collect.issues:
                for issue in self.api.issues(target.login, repo["name"], target.since):
                    if target.since and _before(issue.get("created_at"), target.since):
                        continue
                    if _after_cutoff(issue.get("created_at"), config.until):
                        continue
                    self.db.upsert("issue", issue_row(issue, repo["id"], observed_at), ["issue_id"])
            if config.collect.pull_requests:
                for pull in self.api.pulls(target.login, repo["name"]):
                    if target.since and _before(pull.get("created_at"), target.since):
                        continue
                    if _after_cutoff(pull.get("created_at"), config.until):
                        continue
                    self.db.upsert(
                        "pull_request", pull_row(pull, repo["id"], observed_at), ["pull_id"]
                    )


def organization_row(org: dict[str, Any], observed_at: datetime) -> dict[str, Any]:
    return {
        "org_id": org["id"],
        "login": org["login"].lower(),
        "name": org.get("name"),
        "description": org.get("description"),
        "company": org.get("company"),
        "blog": org.get("blog"),
        "location": org.get("location"),
        "email": org.get("email"),
        "html_url": org.get("html_url"),
        "public_repos": org.get("public_repos"),
        "followers": org.get("followers"),
        "created_at": org.get("created_at"),
        "updated_at": org.get("updated_at"),
        "collected_at": observed_at,
    }


def repository_row(
    repo: dict[str, Any],
    org_id: int,
    observed_at: datetime,
    setup_present: bool | None,
    dependency_count: int,
) -> dict[str, Any]:
    parent = repo.get("parent") or {}
    source = repo.get("source") or {}
    language = repo.get("language")
    return {
        "repo_id": repo["id"],
        "org_id": org_id,
        "name": repo["name"],
        "full_name": repo["full_name"],
        "html_url": repo.get("html_url"),
        "description": repo.get("description"),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
        "default_branch": repo.get("default_branch"),
        "language": language,
        "license_spdx": (repo.get("license") or {}).get("spdx_id"),
        "is_fork": bool(repo.get("fork")),
        "parent_full_name": parent.get("full_name"),
        "source_full_name": source.get("full_name"),
        "archived": bool(repo.get("archived")),
        "disabled": bool(repo.get("disabled")),
        "visibility": repo.get("visibility"),
        "setup_py_present": setup_present,
        "dependency_count": dependency_count,
        "python_setup_eligible": language == "Python" and setup_present and dependency_count > 1,
        "collected_at": observed_at,
    }


def repository_snapshot(repo: dict[str, Any], observed_at: datetime) -> dict[str, Any]:
    return {
        "repo_id": repo["id"],
        "observed_at": observed_at,
        "stargazers_count": repo.get("stargazers_count"),
        "forks_count": repo.get("forks_count"),
        "watchers_count": repo.get("watchers_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "subscribers_count": repo.get("subscribers_count"),
        "network_count": repo.get("network_count"),
    }


def commit_row(commit: dict[str, Any], repo_id: int, observed_at: datetime) -> dict[str, Any]:
    detail = commit.get("commit") or {}
    author = detail.get("author") or {}
    committer = detail.get("committer") or {}
    return {
        "repo_id": repo_id,
        "sha": commit["sha"],
        "author_login": (commit.get("author") or {}).get("login"),
        "committer_login": (commit.get("committer") or {}).get("login"),
        "author_name": author.get("name"),
        "author_email": author.get("email"),
        "author_time": author.get("date"),
        "committer_time": committer.get("date"),
        "message": detail.get("message"),
        "parents_count": len(commit.get("parents") or []),
        "source": "github_api",
        "event_time": None,
        "timestamp_precision": "commit",
        "collected_at": observed_at,
    }


def issue_row(issue: dict[str, Any], repo_id: int, observed_at: datetime) -> dict[str, Any]:
    return {
        "issue_id": issue["id"],
        "repo_id": repo_id,
        "number": issue["number"],
        "author_login": (issue.get("user") or {}).get("login"),
        "assignee_logins": [a.get("login") for a in issue.get("assignees") or []],
        "state": issue.get("state"),
        "state_reason": issue.get("state_reason"),
        "title": issue.get("title"),
        "body": issue.get("body"),
        "labels": [label.get("name") for label in issue.get("labels") or []],
        "comments_count": issue.get("comments"),
        "locked": issue.get("locked"),
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "collected_at": observed_at,
    }


def pull_row(pull: dict[str, Any], repo_id: int, observed_at: datetime) -> dict[str, Any]:
    return {
        "pull_id": pull["id"],
        "repo_id": repo_id,
        "number": pull["number"],
        "author_login": (pull.get("user") or {}).get("login"),
        "state": pull.get("state"),
        "draft": pull.get("draft"),
        "title": pull.get("title"),
        "body": pull.get("body"),
        "head_repo_full_name": ((pull.get("head") or {}).get("repo") or {}).get("full_name"),
        "base_repo_full_name": ((pull.get("base") or {}).get("repo") or {}).get("full_name"),
        "commits_count": pull.get("commits"),
        "additions": pull.get("additions"),
        "deletions": pull.get("deletions"),
        "changed_files": pull.get("changed_files"),
        "comments_count": pull.get("comments"),
        "review_comments_count": pull.get("review_comments"),
        "created_at": pull.get("created_at"),
        "updated_at": pull.get("updated_at"),
        "closed_at": pull.get("closed_at"),
        "merged_at": pull.get("merged_at"),
        "collected_at": observed_at,
    }


def _parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _after_cutoff(value: str | None, cutoff: datetime | None) -> bool:
    parsed = _parse_time(value)
    return bool(parsed and cutoff and parsed > cutoff)


def _before(value: str | None, cutoff: datetime) -> bool:
    parsed = _parse_time(value)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=UTC)
    return bool(parsed and parsed < cutoff)
