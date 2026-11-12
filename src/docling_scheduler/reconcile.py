#!/usr/bin/env python
"""Merge runtime + MLflow state and decide which MLflow runs to close.

Pure functions (no I/O) so the rules are unit-testable:

- A shard's MLflow child run that is still RUNNING after its container has
  exited non-zero (OOM-killed, crashed, evicted) is a zombie -> FAILED.
- A RUNNING child that is no longer the latest run of its shard (a retry
  superseded it) is a zombie -> FAILED.
- On cancel, RUNNING children of stopped shards -> KILLED.
- Once every shard is terminal, the parent run is closed: FINISHED only if
  every shard exited 0, KILLED when cancelled, FAILED otherwise.
"""

from dataclasses import dataclass, field
from typing import Any

from .models import RuntimeStatus

# merged statuses
PENDING = "PENDING"
RUNNING = "RUNNING"
SUCCEEDED = "SUCCEEDED"
FAILED = "FAILED"
CANCELLED = "CANCELLED"
UNKNOWN = "UNKNOWN"

_PARENT_STATUS = {SUCCEEDED: "FINISHED", FAILED: "FAILED", CANCELLED: "KILLED"}


@dataclass
class ChildRun:
    run_id: str
    shard_index: int | None
    status: str
    start_time: int
    end_time: int | None = None
    metrics: dict[str, float] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)


def latest_children(children: list[ChildRun]) -> dict[int, ChildRun]:
    latest: dict[int, ChildRun] = {}
    for child in children:
        if child.shard_index is None:
            continue
        current = latest.get(child.shard_index)
        if current is None or child.start_time > current.start_time:
            latest[child.shard_index] = child
    return latest


def merged_status(runtime: RuntimeStatus, shard_count: int, cancelled: bool) -> str:
    """Single run status from the runtime's view of each shard's latest container."""
    if runtime.error:
        return UNKNOWN
    latest = runtime.latest_by_shard()
    shards = [latest.get(i) for i in range(shard_count)]
    if all(s is not None and s.terminal for s in shards):
        if cancelled:
            return CANCELLED
        if all(s.state == "exited" and s.exit_code == 0 for s in shards):
            return SUCCEEDED
        return FAILED
    if cancelled:
        # stop requested, containers still winding down
        return RUNNING
    if any(s is not None and s.state == "running" for s in shards):
        return RUNNING
    return PENDING


def reconcile_actions(
    runtime: RuntimeStatus,
    children: list[ChildRun],
    parent_status: str | None,
    parent_run_id: str | None,
    status: str,
    cancelled: bool,
) -> list[tuple[str, str]]:
    """``(mlflow_run_id, terminal_status)`` pairs to apply."""
    if runtime.error or status == UNKNOWN:
        return []
    actions: list[tuple[str, str]] = []
    latest_runtime = runtime.latest_by_shard()
    latest_child = latest_children(children)
    for child in children:
        if child.status != "RUNNING" or child.shard_index is None:
            continue
        container = latest_runtime.get(child.shard_index)
        stopped = container is None or container.terminal
        if latest_child.get(child.shard_index) is not child:
            actions.append((child.run_id, "FAILED"))
        elif cancelled and stopped:
            actions.append((child.run_id, "KILLED"))
        elif container is not None and container.terminal and container.exit_code != 0:
            actions.append((child.run_id, "FAILED"))
    if parent_run_id and parent_status == "RUNNING" and status in _PARENT_STATUS:
        actions.append((parent_run_id, _PARENT_STATUS[status]))
    return actions


def child_from_mlflow(run: Any) -> ChildRun:
    tags = dict(run.data.tags)
    shard = tags.get("shard_index")
    return ChildRun(
        run_id=run.info.run_id,
        shard_index=int(shard) if shard is not None and shard.isdigit() else None,
        status=run.info.status,
        start_time=run.info.start_time or 0,
        end_time=run.info.end_time,
        metrics=dict(run.data.metrics),
        tags=tags,
    )


__all__ = [
    "ChildRun",
    "latest_children",
    "merged_status",
    "reconcile_actions",
    "child_from_mlflow",
    "PENDING",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "UNKNOWN",
]
