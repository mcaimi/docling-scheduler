#!/usr/bin/env python
"""Executor contract: how the scheduler launches and observes containers."""

from dataclasses import dataclass
from typing import Any, Iterator, Protocol

from ..models import ResourceSample, RunSpec, RuntimeEvent, RuntimeStatus

# Executor-specific, JSON-serialisable state persisted in the registry
# (container ids per shard attempt, secret names, ...).
RunHandle = dict[str, Any]

# Labels put on every runtime object, so runs can be found (and the registry
# rebuilt) from the runtime alone.
LABEL_RUN_ID = "docling-agent/run-id"
LABEL_SHARD = "docling-agent/shard"
LABEL_ATTEMPT = "docling-agent/attempt"
LABEL_MANAGED = "docling-agent/managed"


@dataclass(frozen=True)
class ExecutorCapabilities:
    """What pre-flight may assume about where containers run."""

    # Host that loopback endpoints are rewritten to; None for a remote runtime,
    # where loopback endpoints are rejected instead.
    host_gateway: str | None
    # Host folders can be mounted (source_backend=local).
    local_source: bool
    # image_exists() sees the runtime's image store; when False, the image is
    # pulled by the runtime from a registry and can't be checked up front.
    local_images: bool


class ExecutorError(RuntimeError):
    """The runtime rejected an operation (unreachable, image missing, ...)."""


class Executor(Protocol):
    name: str
    capabilities: ExecutorCapabilities

    def available(self) -> dict[str, Any]:
        """``{"available": bool, ...details}`` for the targets endpoint."""

    def image_exists(self, image: str) -> bool: ...

    def submit(self, run: RunSpec, handle: RunHandle | None = None) -> RunHandle:
        """Start one container per ``run.shards`` index.

        With ``handle`` (a retry), new attempts are appended to it.
        """

    def status(self, handle: RunHandle) -> RuntimeStatus: ...

    def logs(
        self, handle: RunHandle, shard: int = 0, follow: bool = True, tail: int | None = None
    ) -> Iterator[str]: ...

    def telemetry(self, handle: RunHandle) -> list[ResourceSample]: ...

    def events(self, handle: RunHandle, since: float) -> list[RuntimeEvent]: ...

    def cancel(self, handle: RunHandle) -> None: ...

    def cleanup(self, handle: RunHandle, remove_containers: bool = False) -> None:
        """Remove per-run secrets; with ``remove_containers``, containers too."""


__all__ = [
    "Executor",
    "ExecutorCapabilities",
    "ExecutorError",
    "RunHandle",
    "LABEL_RUN_ID",
    "LABEL_SHARD",
    "LABEL_ATTEMPT",
    "LABEL_MANAGED",
]
