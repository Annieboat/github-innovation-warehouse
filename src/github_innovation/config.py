from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="", extra="ignore")

    github_token: str | None = Field(default=None, alias="GITHUB_TOKEN")
    database: Path = Field(default=Path("data/github_warehouse.duckdb"), alias="GHIW_DATABASE")
    raw_dir: Path = Field(default=Path("data/raw"), alias="GHIW_RAW_DIR")
    user_agent: str = Field(default="github-innovation-warehouse/0.1", alias="GHIW_USER_AGENT")
    request_timeout: float = Field(default=30.0, alias="GHIW_REQUEST_TIMEOUT")


class OrganizationTarget(BaseModel):
    login: str
    repositories: list[str] | None = None
    since: datetime | None = None

    @field_validator("login")
    @classmethod
    def normalize_login(cls, value: str) -> str:
        return value.strip().lower()


class CollectOptions(BaseModel):
    repositories: bool = True
    commits: bool = True
    issues: bool = True
    pull_requests: bool = True
    setup_dependencies: bool = True
    include_archived: bool = True
    include_forks: bool = True


class PipelineConfig(BaseModel):
    organizations: list[OrganizationTarget]
    collect: CollectOptions = Field(default_factory=CollectOptions)
    until: datetime | None = None


def load_pipeline_config(path: Path) -> PipelineConfig:
    with path.open(encoding="utf-8") as handle:
        raw: dict[str, Any] = yaml.safe_load(handle) or {}
    return PipelineConfig.model_validate(raw)
