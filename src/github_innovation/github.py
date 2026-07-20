from __future__ import annotations

import ast
import base64
import re
import time
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class GitHubAPIError(RuntimeError):
    pass


class GitHubClient:
    api_url = "https://api.github.com"

    def __init__(self, token: str | None, user_agent: str, timeout: float = 30):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": user_agent,
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        self.client = httpx.Client(headers=headers, timeout=timeout, follow_redirects=True)

    def close(self) -> None:
        self.client.close()

    def __enter__(self) -> GitHubClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=1, max=20),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        response = self.client.request(method, f"{self.api_url}{path}", **kwargs)
        if response.status_code in {403, 429}:
            reset = response.headers.get("x-ratelimit-reset")
            remaining = response.headers.get("x-ratelimit-remaining")
            if remaining == "0" and reset:
                delay = max(0, int(reset) - int(time.time())) + 1
                if delay <= 300:
                    time.sleep(delay)
                    response = self.client.request(method, f"{self.api_url}{path}", **kwargs)
        if response.status_code == 404:
            raise GitHubAPIError(f"GitHub resource not found: {path}")
        if response.is_error:
            message = response.text[:500]
            raise GitHubAPIError(f"GitHub API {response.status_code} for {path}: {message}")
        return response

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        return self.request("GET", path, params=params).json()

    def paginate(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        page = 1
        query = dict(params or {})
        query["per_page"] = 100
        while True:
            query["page"] = page
            response = self.request("GET", path, params=query)
            items = response.json()
            if not isinstance(items, list):
                raise GitHubAPIError(f"Expected a list from {path}")
            yield from items
            if "next" not in response.links:
                break
            page += 1

    def organization(self, login: str) -> dict[str, Any]:
        data = self.get(f"/orgs/{login}")
        if data.get("type") != "Organization":
            raise GitHubAPIError(f"{login!r} is not an organization account")
        return data

    def repositories(self, login: str) -> Iterator[dict[str, Any]]:
        yield from self.paginate(
            f"/orgs/{login}/repos",
            {"type": "all", "sort": "created", "direction": "asc"},
        )

    def search_repositories(self, query: str, limit: int = 1000) -> Iterator[dict[str, Any]]:
        """Use GitHub Search API; GitHub exposes at most 1,000 results per query."""
        page = 1
        yielded = 0
        while yielded < min(limit, 1000):
            response = self.get(
                "/search/repositories",
                {"q": query, "sort": "updated", "order": "desc", "per_page": 100, "page": page},
            )
            items = response.get("items") or []
            for item in items:
                yield item
                yielded += 1
                if yielded >= min(limit, 1000):
                    return
            if len(items) < 100:
                return
            page += 1

    def repository(self, owner: str, name: str) -> dict[str, Any]:
        return self.get(f"/repos/{owner}/{name}")

    def commits(
        self, owner: str, repo: str, since: datetime | None, until: datetime | None
    ) -> Iterator[dict[str, Any]]:
        params: dict[str, Any] = {}
        if since:
            params["since"] = since.isoformat()
        if until:
            params["until"] = until.isoformat()
        yield from self.paginate(f"/repos/{owner}/{repo}/commits", params)

    def issues(self, owner: str, repo: str, since: datetime | None) -> Iterator[dict[str, Any]]:
        params: dict[str, Any] = {"state": "all", "sort": "created", "direction": "asc"}
        if since:
            params["since"] = since.isoformat()
        for item in self.paginate(f"/repos/{owner}/{repo}/issues", params):
            if "pull_request" not in item:
                yield item

    def pulls(self, owner: str, repo: str) -> Iterator[dict[str, Any]]:
        for pull in self.paginate(
            f"/repos/{owner}/{repo}/pulls",
            {"state": "all", "sort": "created", "direction": "asc"},
        ):
            # List responses omit additions/deletions/commit counts.
            yield self.get(f"/repos/{owner}/{repo}/pulls/{pull['number']}")

    def setup_py(self, owner: str, repo: str, ref: str) -> str | None:
        response = self.client.get(
            f"{self.api_url}/repos/{owner}/{repo}/contents/setup.py", params={"ref": ref}
        )
        if response.status_code == 404:
            return None
        if response.is_error:
            raise GitHubAPIError(f"GitHub API {response.status_code} while reading setup.py")
        data = response.json()
        if data.get("encoding") != "base64":
            return None
        return base64.b64decode(data["content"]).decode("utf-8", errors="replace")


_NAME = re.compile(r"^[A-Za-z0-9_.-]+")


def parse_setup_requirements(source: str) -> list[tuple[str, str]]:
    """Extract literal install_requires safely, without executing repository code."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    values: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign | ast.AnnAssign):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = _literal_string_list(node.value)
            for target in targets:
                if isinstance(target, ast.Name) and value is not None:
                    values[target.id] = value
    requirements: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func_name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
        if func_name != "setup":
            continue
        for keyword in node.keywords:
            if keyword.arg != "install_requires":
                continue
            literal = _literal_string_list(keyword.value)
            if literal is not None:
                requirements.extend(literal)
            elif isinstance(keyword.value, ast.Name):
                requirements.extend(values.get(keyword.value.id, []))
    parsed = []
    for requirement in requirements:
        match = _NAME.match(requirement.strip())
        if match:
            parsed.append((match.group(0).lower().replace("_", "-"), requirement.strip()))
    return sorted(set(parsed))


def _literal_string_list(node: ast.AST | None) -> list[str] | None:
    if not isinstance(node, ast.List | ast.Tuple | ast.Set):
        return None
    result: list[str] = []
    for element in node.elts:
        if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
            return None
        result.append(element.value)
    return result
