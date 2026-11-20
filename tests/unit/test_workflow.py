#!/usr/bin/env python
"""Shard workflow graph rebuilt from MLflow traces."""

from docling_scheduler.reconcile import ChildRun
from docling_scheduler.workflow import shard_workflow


def child(status="RUNNING", **metrics):
    return ChildRun("a" * 32, 0, status, 1, metrics=metrics)


def trace(name, state="OK", start=1):
    return {"name": name, "state": state, "started_at_ms": start, "duration_ms": 5, "error": None}


def row(n, stages=("convert_pdf", "chunk_doc", "embed_doc", "store_doc"), failed=None):
    return {
        "trace_id": f"t{n}", "file": f"d{n}.pdf", "status": "ERROR" if failed else "OK",
        "stage_failed": failed, "error": "boom" if failed else None, "duration_ms": 10,
        "stages_ms": {s: 2.0 for s in stages},
    }


def statuses(view):
    return [s["status"] for s in view["steps"]]


def test_not_started():
    view = shard_workflow(0, None, 0, [], [])
    assert statuses(view) == ["pending"] * 4
    assert view["run_id"] is None
    assert [s["status"] for s in view["steps"][2]["stages"]] == ["pending"] * 4


def test_first_step_without_trace_is_running():
    view = shard_workflow(0, child(document_count=3), 1, [trace("NLP prompt extraction"), trace("document discovery")], [row(1, stages=("convert_pdf", "chunk_doc"))])
    assert statuses(view) == ["ok", "ok", "running", "pending"]
    stages = view["steps"][2]["stages"]
    assert [s["status"] for s in stages] == ["running", "running", "pending", "pending"]
    assert stages[0]["ok"] == 1 and stages[0]["avg_ms"] == 2.0
    assert view["documents"] == {"total": 3, "completed": 1, "failed": 0}


def test_finished_shard():
    traces = [trace(n) for n in ("NLP prompt extraction", "document discovery", "Parallel pipeline", "Results aggregation")]
    rows = [row(1, stages=("convert_pdf", "chunk_doc", "embed_doc")), row(2, stages=("convert_pdf",), failed="convert")]
    view = shard_workflow(0, child("FINISHED", docs_completed=2, docs_failed=1), 1, traces, rows)
    assert statuses(view) == ["ok"] * 4
    stages = view["steps"][2]["stages"]
    assert [s["status"] for s in stages] == ["error", "ok", "ok", "skipped"]
    assert (stages[0]["ok"], stages[0]["failed"]) == (1, 1)
    assert [d["file"] for d in view["recent_documents"]] == ["d2.pdf", "d1.pdf"]


def test_failed_step_blocks_the_rest():
    view = shard_workflow(0, child("FAILED"), 1, [trace("NLP prompt extraction"), trace("document discovery", "ERROR")], [])
    assert statuses(view) == ["ok", "error", "pending", "pending"]


def test_dead_shard_marks_the_step_it_died_in():
    view = shard_workflow(0, child("FAILED"), 2, [trace("NLP prompt extraction"), trace("document discovery")], [])
    assert statuses(view) == ["ok", "ok", "error", "pending"]
    assert view["steps"][2]["error"].startswith("interrupted")
    killed = shard_workflow(0, child("KILLED"), 1, [], [])
    assert killed["steps"][0]["error"] == "cancelled"
