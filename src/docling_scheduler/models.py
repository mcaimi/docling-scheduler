#!/usr/bin/env python
"""Data shapes shared by the scheduler service, executors and API."""

from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, Field

# --- API request bodies ---


class RunTarget(BaseModel):
    # must match the scheduler's executor (see /api/targets)
    type: Literal["podman", "kubernetes"] = "podman"
    image: str | None = None
    # Mounted read-only as the document folder; required for
    # source_backend=local, unused for s3 (and not supported by kubernetes).
    host_folder: str | None = None
    shards: int = Field(default=1, ge=1)
    cpus: float | None = Field(default=None, gt=0)
    memory: str | None = None


class RunRequest(BaseModel):
    # Partial overrides on top of the scheduler's .env/spec defaults; sensitive
    # keys may be omitted to use the operator's .env values.
    settings: dict[str, Any] = Field(default_factory=dict)
    target: RunTarget


# backwards-compatible name
PodmanTarget = RunTarget


# --- executor contract ---


@dataclass
class RunSpec:
    run_id: str
    image: str
    shard_count: int
    # Shard indices to start (all of them on submit, failed ones on retry).
    shards: list[int]
    # Non-secret environment, without SHARD_INDEX (set per container).
    env: dict[str, str]
    # Env var name -> secret value, injected as runtime secrets.
    secret_env: dict[str, str]
    # Document folder mounted read-only at /data/in (None: S3 source).
    host_folder: str | None = None
    cpus: float | None = None
    memory: str | None = None
    # Container path -> host file, mounted read-only (e.g. the S3 CA bundle).
    file_mounts: dict[str, str] = field(default_factory=dict)


@dataclass
class ShardRuntime:
    shard_index: int
    attempt: int
    container_id: str | None
    name: str | None
    # created | running | exited | missing (container removed) | unknown
    state: str
    exit_code: int | None = None
    oom_killed: bool = False
    started_at: str | None = None
    finished_at: str | None = None
    error: str | None = None

    @property
    def terminal(self) -> bool:
        return self.state in ("exited", "missing")


@dataclass
class RuntimeStatus:
    shards: list[ShardRuntime] = field(default_factory=list)
    error: str | None = None

    def latest_by_shard(self) -> dict[int, ShardRuntime]:
        latest: dict[int, ShardRuntime] = {}
        for shard in self.shards:
            current = latest.get(shard.shard_index)
            if current is None or shard.attempt > current.attempt:
                latest[shard.shard_index] = shard
        return latest


@dataclass
class ResourceSample:
    shard_index: int
    timestamp: float
    cpu_percent: float | None
    memory_bytes: int | None
    memory_limit_bytes: int | None


@dataclass
class RuntimeEvent:
    timestamp: float
    shard_index: int | None
    type: str
    action: str
    message: str = ""


def to_dict(obj: Any) -> Any:
    """Dataclass (or list of them) -> JSON-serialisable dict."""
    if isinstance(obj, list):
        return [to_dict(o) for o in obj]
    return asdict(obj)


__all__ = [
    "RunTarget",
    "PodmanTarget",
    "RunRequest",
    "RunSpec",
    "ShardRuntime",
    "RuntimeStatus",
    "ResourceSample",
    "RuntimeEvent",
    "to_dict",
]
