#!/usr/bin/env python
"""Scheduler service: launch, observe, reconcile, cancel and retry runs."""

import json
import logging
import threading
import time
import uuid
from dataclasses import asdict
from typing import Any, Callable, Iterator

from .config import SchedulerConfig
from .contract import SETTINGS_BY_KEY, settings_schema
from .executors import EXECUTOR_NAMES
from .executors.base import Executor, ExecutorError
from .mlflow_monitor import MlflowMonitor
from .models import RunRequest, RunSpec, RuntimeStatus, to_dict
from .discovery import list_s3_documents
from .preflight import Preflight, check_mlflow_http, run_preflight
from .reconcile import (
    CANCELLED,
    FAILED,
    RUNNING,
    UNKNOWN,
    merged_status,
    reconcile_actions,
)
from .registry import TERMINAL_STATUSES, Registry, RunRecord
from .settings_io import MANAGED_KEYS, load_defaults, secret_presence, non_secret_settings

logger = logging.getLogger(__name__)


class RunNotFound(KeyError):
    pass


class PreflightFailed(Exception):
    def __init__(self, report: Preflight):
        super().__init__("pre-flight validation failed")
        self.report = report


class InvalidState(Exception):
    """Operation not allowed in the run's current state."""


class SchedulerService:
    def __init__(
        self,
        config: SchedulerConfig,
        registry: Registry,
        executor: Executor,
        monitor: MlflowMonitor,
        check_mlflow: Callable[[str], str | None] = check_mlflow_http,
        list_s3: Callable[[dict[str, Any]], list[str]] = list_s3_documents,
    ):
        self.config = config
        self.registry = registry
        self.executor = executor
        self.monitor = monitor
        self._check_mlflow = check_mlflow
        self._list_s3 = list_s3
        # serialises refresh/cancel/retry per run (UI polling + reconciler thread)
        self._run_locks: dict[str, threading.Lock] = {}
        self._run_locks_guard = threading.Lock()

    def _lock(self, run_id: str) -> threading.Lock:
        with self._run_locks_guard:
            return self._run_locks.setdefault(run_id, threading.Lock())

    def _record(self, run_id: str) -> RunRecord:
        record = self.registry.get(run_id)
        if record is None:
            raise RunNotFound(run_id)
        return record

    # --- settings & targets ----------------------------------------------

    def settings_schema(self) -> list[dict[str, Any]]:
        # "managed" settings are set by the scheduler; the form hides them
        return [{**entry, "managed": entry["key"] in MANAGED_KEYS} for entry in settings_schema()]

    def settings_defaults(self) -> dict[str, Any]:
        defaults = load_defaults(self.config.envfile)
        return {"values": non_secret_settings(defaults), "secrets": secret_presence(defaults)}

    def targets(self) -> dict[str, Any]:
        inactive = {"available": False, "error": f"this scheduler runs the {self.executor.name} executor"}
        return {
            "executor": {
                "name": self.executor.name,
                "capabilities": asdict(self.executor.capabilities),
            },
            **{name: inactive for name in EXECUTOR_NAMES},
            self.executor.name: self.executor.available(),
            "mlflow": {
                "tracking_uri": self.config.mlflow_tracking_uri,
                "ui_uri": self.config.effective_mlflow_ui_uri,
            },
            "default_image": self.config.default_image,
        }

    # --- launch ------------------------------------------------------------

    def preflight(self, request: RunRequest) -> Preflight:
        return run_preflight(
            request,
            config=self.config,
            defaults=load_defaults(self.config.envfile),
            executor=self.executor,
            check_mlflow=self._check_mlflow,
            list_s3=self._list_s3,
        )

    def create_run(self, request: RunRequest) -> dict[str, Any]:
        report = self.preflight(request)
        if not report.ok:
            raise PreflightFailed(report)

        run_id = uuid.uuid4().hex[:12]
        config = {
            "settings": non_secret_settings(report.settings),
            # the request's own non-secret overrides, replayed on retry
            "overrides": {
                k: v
                for k, v in request.settings.items()
                if k in SETTINGS_BY_KEY and not SETTINGS_BY_KEY[k].sensitive
            },
            "target": request.target.model_dump(),
            "warnings": report.warnings,
            "document_count": report.document_count,
        }
        experiment_name = report.settings.get("mlflow_experiment_name") or "Docling Agent"
        experiment_id = self.monitor.ensure_experiment(experiment_name)
        parent_run_id = self.monitor.create_parent_run(
            experiment_id,
            run_name=f"docling_{run_id}",
            tags={
                "scheduler_run_id": run_id,
                "scheduler_executor": self.executor.name,
                "shard_count": request.target.shards,
                "document_count": report.document_count,
            },
            config=config,
        )
        record = self.registry.create(
            RunRecord(
                id=run_id,
                created_at=time.time(),
                target_type=self.executor.name,
                image=report.image,
                shard_count=request.target.shards,
                mlflow_experiment_id=experiment_id,
                mlflow_parent_run_id=parent_run_id,
                config=config,
            )
        )
        spec = RunSpec(
            run_id=run_id,
            image=report.image,
            shard_count=request.target.shards,
            shards=list(range(request.target.shards)),
            env={
                **report.env,
                "MLFLOW_PARENT_RUN_ID": parent_run_id,
                "MLFLOW_SESSION_ID": parent_run_id,
                "SCHEDULER_RUN_ID": run_id,
            },
            secret_env=report.secret_env,
            host_folder=report.host_folder or None,
            file_mounts=report.file_mounts,
            cpus=request.target.cpus,
            memory=request.target.memory,
        )
        try:
            handle = self.executor.submit(spec)
        except ExecutorError as e:
            self.monitor.set_terminated(parent_run_id, "FAILED")
            self.registry.update(run_id, error=str(e))
            self.registry.mark_final(run_id, "SUBMIT_FAILED")
            raise
        self.registry.update(run_id, handle=handle, status="PENDING")
        return self.registry.get(record.id).to_dict()

    def _spec_for_retry(self, record: RunRecord, shards: list[int]) -> RunSpec:
        """Re-render the container spec of an existing run for a retry.

        Replays the original request's non-secret overrides. Secrets are never
        persisted, so they are re-resolved from the current ``.env`` (secrets
        typed into the form at launch are not reused).
        """
        request = RunRequest(
            settings=record.config.get("overrides", {}),
            target=record.config["target"],
        )
        report = self.preflight(request)
        if not report.ok:
            raise PreflightFailed(report)
        return RunSpec(
            run_id=record.id,
            image=record.image,
            shard_count=record.shard_count,
            shards=shards,
            env={
                **report.env,
                "MLFLOW_PARENT_RUN_ID": record.mlflow_parent_run_id,
                "MLFLOW_SESSION_ID": record.mlflow_parent_run_id,
                "SCHEDULER_RUN_ID": record.id,
            },
            secret_env=report.secret_env,
            host_folder=report.host_folder or None,
            file_mounts=report.file_mounts,
            cpus=request.target.cpus,
            memory=request.target.memory,
        )

    # --- observation -------------------------------------------------------

    def refresh(self, run_id: str) -> tuple[RunRecord, RuntimeStatus | None]:
        """Re-derive the merged status, close zombie MLflow runs, and finalise
        terminal runs (secrets removed, status frozen)."""
        with self._lock(run_id):
            record = self._record(run_id)
            if record.final:
                return record, None
            runtime = self.executor.status(record.handle)
            status = merged_status(runtime, record.shard_count, record.cancelled)
            try:
                children = self.monitor.children(
                    record.mlflow_experiment_id, record.mlflow_parent_run_id
                )
                parent_status = self.monitor.run_status(record.mlflow_parent_run_id)
            except Exception as e:
                logger.warning("MLflow unavailable while refreshing %s: %s", run_id, e)
                self.registry.update(run_id, status=status)
                return self._record(run_id), runtime
            for mlflow_run_id, terminal in reconcile_actions(
                runtime,
                children,
                parent_status,
                record.mlflow_parent_run_id,
                status,
                record.cancelled,
            ):
                logger.info("Reconcile %s: MLflow run %s -> %s", run_id, mlflow_run_id, terminal)
                self.monitor.set_terminated(mlflow_run_id, terminal)
            if status in TERMINAL_STATUSES:
                self.executor.cleanup(record.handle)
                self.registry.update(run_id, handle=record.handle)
                self.registry.mark_final(run_id, status)
            else:
                self.registry.update(run_id, status=status)
            return self._record(run_id), runtime

    def reconcile_all(self) -> None:
        for record in self.registry.list_active():
            if record.status == "SUBMITTING":
                continue
            try:
                self.refresh(record.id)
            except Exception as e:
                logger.warning("Reconcile of %s failed: %s", record.id, e)

    def get_run(self, run_id: str) -> dict[str, Any]:
        record, runtime = self.refresh(run_id)
        view = record.to_dict()
        if runtime is None:
            runtime = self.executor.status(record.handle)
        view["runtime_error"] = runtime.error
        view["shards"] = to_dict(
            sorted(runtime.latest_by_shard().values(), key=lambda s: s.shard_index)
        )
        view["mlflow_link"] = self.monitor.run_link(
            record.mlflow_experiment_id, record.mlflow_parent_run_id
        )
        return view

    def list_runs(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return [r.to_dict() for r in self.registry.query(status=status, limit=limit)]

    def mlflow_overview(self, run_id: str) -> dict[str, Any]:
        record = self._record(run_id)
        return self.monitor.overview(
            record.mlflow_experiment_id, record.mlflow_parent_run_id, record.shard_count
        )

    def documents(self, run_id: str, after: int = 0, generation: str | None = None) -> dict[str, Any]:
        record = self._record(run_id)
        return self.monitor.documents(
            record.mlflow_experiment_id, record.mlflow_parent_run_id, after, generation
        )

    def error_reports(self, run_id: str) -> list[dict[str, Any]]:
        record = self._record(run_id)
        return self.monitor.error_reports(record.mlflow_experiment_id, record.mlflow_parent_run_id)

    def runtime(self, run_id: str) -> dict[str, Any]:
        record = self._record(run_id)
        runtime = self.executor.status(record.handle)
        return {
            "error": runtime.error,
            "containers": to_dict(sorted(runtime.shards, key=lambda s: (s.shard_index, s.attempt))),
            "events": to_dict(self.executor.events(record.handle, since=record.created_at)),
            "resources": to_dict(self.executor.telemetry(record.handle)),
        }

    def logs(
        self,
        run_id: str,
        shard: int = 0,
        follow: bool = True,
        tail: int | None = None,
        level: str | None = None,
        file: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Log records of a shard's latest container; JSON lines are parsed,
        anything else is passed through as ``{"msg": line}``."""
        record = self._record(run_id)
        if not 0 <= shard < record.shard_count:
            raise ValueError(f"shard must be in [0, {record.shard_count})")
        wanted_level = level.upper() if level else None
        for line in self.executor.logs(record.handle, shard=shard, follow=follow, tail=tail):
            try:
                entry = json.loads(line)
                if not isinstance(entry, dict):
                    raise ValueError
            except ValueError:
                entry = {"msg": line}
            if wanted_level and entry.get("level") != wanted_level:
                continue
            if file and entry.get("file") != file:
                continue
            yield entry

    # --- control -----------------------------------------------------------

    def cancel(self, run_id: str) -> dict[str, Any]:
        with self._lock(run_id):
            record = self._record(run_id)
            if record.final:
                raise InvalidState(f"run is already {record.status}")
            self.registry.update(run_id, cancelled=True)
            self.executor.cancel(record.handle)
        self.refresh(run_id)
        return self.get_run(run_id)

    def retry_failed(self, run_id: str) -> dict[str, Any]:
        with self._lock(run_id):
            record = self._record(run_id)
            if record.status != FAILED or not record.final:
                raise InvalidState("only failed runs can be retried")
            runtime = self.executor.status(record.handle)
            latest = runtime.latest_by_shard()
            failed = [
                i
                for i in range(record.shard_count)
                if latest.get(i) is None or latest[i].exit_code != 0
            ]
            spec = self._spec_for_retry(record, failed)
            handle = self.executor.submit(spec, handle=record.handle)
            self.monitor.reopen(record.mlflow_parent_run_id)
            self.registry.update(
                run_id, handle=handle, status=RUNNING, final=False, finished_at=None, error=None
            )
        return self.get_run(run_id)

    def delete(self, run_id: str) -> None:
        with self._lock(run_id):
            record = self._record(run_id)
            if not record.final:
                raise InvalidState("cancel the run before deleting it")
            self.executor.cleanup(record.handle, remove_containers=True)
            self.registry.delete(run_id)


__all__ = [
    "SchedulerService",
    "RunNotFound",
    "PreflightFailed",
    "InvalidState",
    "CANCELLED",
    "UNKNOWN",
]
