#!/usr/bin/env python
"""MLflow side of the scheduler: parent runs, shard roll-ups, document traces."""

import json
import logging
import re
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

from .reconcile import ChildRun, child_from_mlflow, latest_children

logger = logging.getLogger(__name__)

# Cumulative per-shard counters logged by the pipeline (document_pipeline.progress_metrics).
PROGRESS_KEYS = (
    "docs_completed",
    "docs_failed",
    "total_chunks",
    "total_embeddings",
    "non_null_embeddings",
    "vectors_stored",
    "vectors_upserted",
    "vector_store_retry_count",
    "failed_chunk_count",
    "error_count",
    "vector_store_error_count",
)
HISTORY_KEYS = ("docs_completed", "total_chunks", "vectors_stored", "error_count")
DOCUMENT_SPAN = "document_pipeline"
STAGE_SPANS = ("convert_pdf", "chunk_doc", "embed_doc", "store_doc")
# Max traces fetched in full per documents() call, to bound request latency.
_TRACE_FETCH_LIMIT = 200

_RUN_ID_RE = re.compile(r"^[0-9a-f]{32}$")


def _check_run_id(run_id: str) -> str:
    # ids are interpolated into MLflow filter strings
    if not _RUN_ID_RE.match(run_id):
        raise ValueError(f"invalid MLflow run id: {run_id!r}")
    return run_id


class _TraceCache:
    """Document rows of one logical run, numbered in discovery order.

    MLflow writes a trace when its root span *ends*, but orders/filters traces
    by start time -- a slow document can appear after documents that started
    later, so a timestamp cursor would skip it. Instead every newly seen trace
    gets the next sequence number and clients page with ``after=<seq>``.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.seen: set[str] = set()
        self.rows: list[dict[str, Any]] = []


def trace_to_row(trace: Any) -> dict[str, Any] | None:
    spans = trace.data.spans
    root = next((s for s in spans if s.name == DOCUMENT_SPAN and s.parent_id is None), None)
    if root is None:
        return None
    attributes = root.attributes
    stages: dict[str, float] = {}
    for span in spans:
        if span.name in STAGE_SPANS and span.end_time_ns:
            stages[span.name] = round((span.end_time_ns - span.start_time_ns) / 1e6, 1)
    status = getattr(root.status.status_code, "value", str(root.status.status_code))
    duration = (
        round((root.end_time_ns - root.start_time_ns) / 1e6, 1) if root.end_time_ns else None
    )
    return {
        "trace_id": trace.info.trace_id,
        "file": attributes.get("file"),
        "file_format": attributes.get("file_format"),
        "shard_index": attributes.get("shard_index"),
        "status": status,
        "stage_failed": attributes.get("stage_failed"),
        "error": attributes.get("error"),
        "chunk_count": attributes.get("chunk_count"),
        "stored_count": attributes.get("stored_count"),
        "embedding_failed": attributes.get("embedding_failed"),
        "started_at_ms": root.start_time_ns // 1_000_000,
        "duration_ms": duration,
        "stages_ms": stages,
    }


class MlflowMonitor:
    def __init__(self, tracking_uri: str, ui_uri: str | None = None):
        from mlflow import MlflowClient

        self.tracking_uri = tracking_uri
        self.ui_uri = ui_uri
        self.client = MlflowClient(tracking_uri=tracking_uri)
        # identifies this process's trace caches; clients restart paging when it changes
        self.generation = uuid.uuid4().hex[:12]
        self._caches: dict[str, _TraceCache] = {}
        self._caches_lock = threading.Lock()

    # --- runs ------------------------------------------------------------

    def ensure_experiment(self, name: str) -> str:
        experiment = self.client.get_experiment_by_name(name)
        if experiment is not None:
            return experiment.experiment_id
        return self.client.create_experiment(name)

    def create_parent_run(
        self, experiment_id: str, run_name: str, tags: dict[str, Any], config: dict[str, Any]
    ) -> str:
        run = self.client.create_run(
            experiment_id, run_name=run_name, tags={k: str(v) for k, v in tags.items()}
        )
        run_id = run.info.run_id
        self.client.set_tag(run_id, "mlflow.trace.session", run_id)
        self.client.log_dict(run_id, config, "scheduler_config.json")
        return run_id

    def run_status(self, run_id: str) -> str:
        return self.client.get_run(run_id).info.status

    def set_terminated(self, run_id: str, status: str) -> None:
        self.client.set_terminated(run_id, status)

    def reopen(self, run_id: str) -> None:
        self.client.update_run(run_id, status="RUNNING")

    def children(self, experiment_id: str, parent_run_id: str) -> list[ChildRun]:
        runs = self.client.search_runs(
            [experiment_id],
            filter_string=f"tags.mlflow.parentRunId = '{_check_run_id(parent_run_id)}'",
            max_results=1000,
        )
        return [child_from_mlflow(r) for r in runs]

    def run_link(self, experiment_id: str, run_id: str) -> str | None:
        if not self.ui_uri:
            return None
        return f"{self.ui_uri}/#/experiments/{experiment_id}/runs/{run_id}"

    def overview(self, experiment_id: str, parent_run_id: str, shard_count: int) -> dict[str, Any]:
        parent = self.client.get_run(parent_run_id)
        children = self.children(experiment_id, parent_run_id)
        latest = latest_children(children)
        attempts: dict[int, int] = {}
        for child in children:
            if child.shard_index is not None:
                attempts[child.shard_index] = attempts.get(child.shard_index, 0) + 1

        totals = {key: 0.0 for key in PROGRESS_KEYS}
        document_count = 0.0
        total_document_count: int | None = None
        shards = []
        # per shard and metric: [step, value, timestamp_ms] points
        history: dict[str, dict[str, list[tuple[int, float, int]]]] = {}
        for index in range(shard_count):
            child = latest.get(index)
            if child is None:
                shards.append({"shard_index": index, "run_id": None, "status": None, "attempts": 0})
                continue
            for key in PROGRESS_KEYS:
                totals[key] += child.metrics.get(key, 0.0)
            document_count += child.metrics.get("document_count", 0.0)
            if total_document_count is None and child.tags.get("total_document_count", "").isdigit():
                total_document_count = int(child.tags["total_document_count"])
            history[str(index)] = {
                key: [
                    (m.step, m.value, m.timestamp)
                    for m in self.client.get_metric_history(child.run_id, key)
                ]
                for key in HISTORY_KEYS
            }
            shards.append(
                {
                    "shard_index": index,
                    "run_id": child.run_id,
                    "status": child.status,
                    "attempts": attempts.get(index, 0),
                    "start_time": child.start_time,
                    "end_time": child.end_time,
                    "document_count": child.metrics.get("document_count"),
                    "metrics": {k: child.metrics.get(k) for k in PROGRESS_KEYS},
                    "link": self.run_link(experiment_id, child.run_id),
                }
            )
        expected = total_document_count or document_count
        return {
            "parent": {
                "run_id": parent_run_id,
                "status": parent.info.status,
                "start_time": parent.info.start_time,
                "end_time": parent.info.end_time,
                "link": self.run_link(experiment_id, parent_run_id),
            },
            "session_id": parent_run_id,
            "totals": totals,
            "total_document_count": total_document_count,
            "progress": (totals["docs_completed"] / expected) if expected else None,
            "shards": shards,
            "history": history,
        }

    def error_reports(self, experiment_id: str, parent_run_id: str) -> list[dict[str, Any]]:
        """``error_reports.json`` of each shard's latest run, merged."""
        merged: list[dict[str, Any]] = []
        for shard, child in sorted(latest_children(self.children(experiment_id, parent_run_id)).items()):
            names = {a.path for a in self.client.list_artifacts(child.run_id)}
            if "error_reports.json" not in names:
                continue
            with tempfile.TemporaryDirectory() as tmp:
                path = self.client.download_artifacts(child.run_id, "error_reports.json", tmp)
                for report in json.loads(Path(path).read_text()):
                    merged.append({**report, "shard_index": shard})
        return merged

    # --- documents ---------------------------------------------------------

    def _cache(self, parent_run_id: str) -> _TraceCache:
        with self._caches_lock:
            return self._caches.setdefault(parent_run_id, _TraceCache())

    def documents(
        self, experiment_id: str, parent_run_id: str, after: int = 0, generation: str | None = None
    ) -> dict[str, Any]:
        """Per-document rows (one per finished document trace) with ``seq > after``."""
        if generation != self.generation:
            after = 0
        cache = self._cache(parent_run_id)
        with cache.lock:
            self._refresh(cache, experiment_id, parent_run_id)
            rows = [r for r in cache.rows if r["seq"] > after]
            return {
                "generation": self.generation,
                "rows": rows,
                "last_seq": cache.rows[-1]["seq"] if cache.rows else 0,
                "total": len(cache.rows),
            }

    def _refresh(self, cache: _TraceCache, experiment_id: str, parent_run_id: str) -> None:
        session_filter = f"metadata.`mlflow.trace.session` = '{_check_run_id(parent_run_id)}'"
        new_ids: list[str] = []
        page_token = None
        while True:
            page = self.client.search_traces(
                locations=[experiment_id],
                filter_string=session_filter,
                include_spans=False,
                max_results=500,
                page_token=page_token,
            )
            new_ids.extend(t.info.trace_id for t in page if t.info.trace_id not in cache.seen)
            page_token = page.token
            if not page_token:
                break
        for trace_id in new_ids[:_TRACE_FETCH_LIMIT]:
            try:
                trace = self.client.get_trace(trace_id)
            except Exception as e:
                logger.debug("trace %s not readable yet: %s", trace_id, e)
                continue
            cache.seen.add(trace_id)
            row = trace_to_row(trace)
            if row is not None:  # run-level spans share the session; skip them
                row["seq"] = len(cache.rows) + 1
                cache.rows.append(row)


__all__ = ["MlflowMonitor", "PROGRESS_KEYS", "trace_to_row"]
