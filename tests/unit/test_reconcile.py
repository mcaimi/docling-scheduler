#!/usr/bin/env python
"""Merged status and zombie-run reconciliation rules."""

from docling_scheduler.models import RuntimeStatus, ShardRuntime
from docling_scheduler.reconcile import (
    ChildRun,
    latest_children,
    merged_status,
    reconcile_actions,
)


def shard(index, state, exit_code=None, attempt=0):
    return ShardRuntime(index, attempt, f"c{index}-{attempt}", None, state, exit_code)


def child(run_id, shard_index, status="RUNNING", start=1):
    return ChildRun(run_id, shard_index, status, start)


def test_merged_status():
    assert merged_status(RuntimeStatus([shard(0, "created")]), 1, False) == "PENDING"
    assert merged_status(RuntimeStatus([shard(0, "running"), shard(1, "exited", 0)]), 2, False) == "RUNNING"
    assert merged_status(RuntimeStatus([shard(0, "exited", 0), shard(1, "exited", 0)]), 2, False) == "SUCCEEDED"
    assert merged_status(RuntimeStatus([shard(0, "exited", 0), shard(1, "exited", 137)]), 2, False) == "FAILED"
    assert merged_status(RuntimeStatus([shard(0, "missing")]), 1, False) == "FAILED"
    assert merged_status(RuntimeStatus([shard(0, "exited", 0)]), 2, False) == "PENDING"  # shard 1 not created
    assert merged_status(RuntimeStatus([], error="down"), 1, False) == "UNKNOWN"


def test_merged_status_uses_latest_attempt():
    runtime = RuntimeStatus([shard(0, "exited", 1, attempt=0), shard(0, "exited", 0, attempt=1)])
    assert merged_status(runtime, 1, False) == "SUCCEEDED"


def test_merged_status_cancel():
    assert merged_status(RuntimeStatus([shard(0, "running")]), 1, True) == "RUNNING"
    assert merged_status(RuntimeStatus([shard(0, "exited", 143)]), 1, True) == "CANCELLED"


def test_crashed_shard_child_marked_failed_and_parent_closed():
    runtime = RuntimeStatus([shard(0, "exited", 0), shard(1, "exited", 137)])
    children = [child("a", 0, "FINISHED"), child("b", 1, "RUNNING")]
    status = merged_status(runtime, 2, False)
    actions = reconcile_actions(runtime, children, "RUNNING", "parent", status, False)
    assert actions == [("b", "FAILED"), ("parent", "FAILED")]


def test_successful_run_closes_parent_finished():
    runtime = RuntimeStatus([shard(0, "exited", 0)])
    actions = reconcile_actions(runtime, [child("a", 0, "FINISHED")], "RUNNING", "parent", "SUCCEEDED", False)
    assert actions == [("parent", "FINISHED")]


def test_running_shard_left_alone():
    runtime = RuntimeStatus([shard(0, "running")])
    assert reconcile_actions(runtime, [child("a", 0)], "RUNNING", "parent", "RUNNING", False) == []


def test_superseded_child_is_failed():
    runtime = RuntimeStatus([shard(0, "exited", 1, 0), shard(0, "running", None, 1)])
    children = [child("old", 0, "RUNNING", start=1), child("new", 0, "RUNNING", start=2)]
    actions = reconcile_actions(runtime, children, "RUNNING", "parent", "RUNNING", False)
    assert actions == [("old", "FAILED")]


def test_cancel_kills_children_and_parent():
    runtime = RuntimeStatus([shard(0, "exited", 143)])
    actions = reconcile_actions(runtime, [child("a", 0)], "RUNNING", "parent", "CANCELLED", True)
    assert actions == [("a", "KILLED"), ("parent", "KILLED")]


def test_no_actions_when_runtime_unknown():
    runtime = RuntimeStatus([], error="podman down")
    assert reconcile_actions(runtime, [child("a", 0)], "RUNNING", "parent", "UNKNOWN", False) == []


def test_parent_already_closed_not_touched():
    runtime = RuntimeStatus([shard(0, "exited", 0)])
    assert reconcile_actions(runtime, [], "FINISHED", "parent", "SUCCEEDED", False) == []


def test_latest_children():
    latest = latest_children([child("a", 0, start=1), child("b", 0, start=5), child("x", None)])
    assert {k: v.run_id for k, v in latest.items()} == {0: "b"}
