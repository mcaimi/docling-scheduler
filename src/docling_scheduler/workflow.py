#!/usr/bin/env python
"""One shard's agent workflow graph, rebuilt from its MLflow traces.

Pure functions (no I/O). The agent opens one top-level span per step (see
``_run_workflow_with_mlflow`` in docling-agent), each its own trace, plus one
``document_pipeline`` trace per document with a child span per stage. MLflow
only writes a trace when its root span ends, so a step that is still running
has no trace yet: its state is inferred from the steps before it and the
status of the shard's MLflow run.
"""

from typing import Any

from .reconcile import ChildRun

# (key, top-level span name), in execution order
RUN_STEPS = (
    ("prompt_extraction", "NLP prompt extraction"),
    ("discovery", "document discovery"),
    ("pipeline", "Parallel pipeline"),
    ("aggregation", "Results aggregation"),
)
PIPELINE_STEP = "pipeline"
# (stage span, document_pipeline's stage_failed value), in execution order
DOCUMENT_STAGES = (
    ("convert_pdf", "convert"),
    ("chunk_doc", "chunk"),
    ("embed_doc", "embed"),
    ("store_doc", "store"),
)
RECENT_DOCUMENTS = 10

PENDING = "pending"
RUNNING = "running"
OK = "ok"
ERROR = "error"
SKIPPED = "skipped"

_TRACE_STATES = {"OK": OK, "ERROR": ERROR, "IN_PROGRESS": RUNNING}
_INTERRUPTED = {"FAILED": "interrupted: the shard exited before this step finished", "KILLED": "cancelled"}


def _run_steps(child: ChildRun | None, traces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for trace in sorted(traces, key=lambda t: t.get("started_at_ms") or 0):
        by_name[trace["name"]] = trace  # a re-entered step keeps its latest trace
    steps = []
    blocked = False  # an earlier step failed or is still running
    for key, name in RUN_STEPS:
        step: dict[str, Any] = {
            "key": key, "name": name, "status": PENDING,
            "started_at_ms": None, "duration_ms": None, "error": None,
        }
        trace = by_name.get(name)
        if trace is not None and not blocked:
            step["status"] = _TRACE_STATES.get(trace.get("state") or "", PENDING)
            step["started_at_ms"] = trace.get("started_at_ms")
            step["duration_ms"] = trace.get("duration_ms")
            step["error"] = trace.get("error")
        elif child is not None and not blocked:
            # the first step without a trace is the one in progress, or the one
            # the shard died in
            if child.status == "RUNNING":
                step["status"] = RUNNING
            elif child.status in _INTERRUPTED:
                step["status"] = ERROR
                step["error"] = _INTERRUPTED[child.status]
        if step["status"] != OK:
            blocked = True
        steps.append(step)
    return steps


def _stages(rows: list[dict[str, Any]], pipeline_status: str) -> list[dict[str, Any]]:
    stages = []
    for index, (key, short) in enumerate(DOCUMENT_STAGES):
        durations = [r["stages_ms"][key] for r in rows if key in (r.get("stages_ms") or {})]
        failed = sum(1 for r in rows if r.get("stage_failed") == short)
        reached = max(len(durations), failed)
        if failed:
            status = ERROR
        elif pipeline_status == RUNNING:
            # documents enter at the first stage; later ones wait for a document
            status = RUNNING if reached or index == 0 else PENDING
        elif pipeline_status == PENDING:
            status = PENDING
        elif reached:
            status = OK
        else:
            # e.g. no vector store configured, or every document stopped earlier
            status = SKIPPED if pipeline_status == OK else PENDING
        stages.append(
            {
                "key": key,
                "status": status,
                "ok": reached - failed,
                "failed": failed,
                "avg_ms": round(sum(durations) / len(durations), 1) if durations else None,
            }
        )
    return stages


def shard_workflow(
    shard_index: int,
    child: ChildRun | None,
    attempts: int,
    step_traces: list[dict[str, Any]],
    document_rows: list[dict[str, Any]],
    link: str | None = None,
) -> dict[str, Any]:
    """Workflow graph of a shard's latest run.

    ``step_traces``: the run's top-level traces as ``{name, state, started_at_ms,
    duration_ms, error}``. ``document_rows``: its finished documents
    (``mlflow_monitor.trace_to_row``), in discovery order.
    """
    steps = _run_steps(child, step_traces)
    pipeline = next(s for s in steps if s["key"] == PIPELINE_STEP)
    pipeline["stages"] = _stages(document_rows, pipeline["status"])
    metrics = child.metrics if child else {}
    document_count = metrics.get("document_count")
    recent = document_rows[-RECENT_DOCUMENTS:][::-1]
    return {
        "shard_index": shard_index,
        "run_id": child.run_id if child else None,
        "run_status": child.status if child else None,
        "attempts": attempts,
        "link": link,
        "documents": {
            "total": int(document_count) if document_count is not None else None,
            # the pipeline logs these after every batch; traces land per document
            "completed": int(max(metrics.get("docs_completed", 0), len(document_rows))),
            "failed": int(
                max(metrics.get("docs_failed", 0), sum(1 for r in document_rows if r.get("status") == "ERROR"))
            ),
        },
        "steps": steps,
        "recent_documents": [
            {
                "trace_id": r["trace_id"],
                "file": r.get("file"),
                "status": r.get("status"),
                "stage_failed": r.get("stage_failed"),
                "error": r.get("error"),
                "duration_ms": r.get("duration_ms"),
                "stages_ms": r.get("stages_ms") or {},
            }
            for r in recent
        ],
    }


__all__ = ["RUN_STEPS", "DOCUMENT_STAGES", "shard_workflow"]
