#!/usr/bin/env python
"""Kubernetes executor: one Job per shard attempt, on a remote cluster.

Uses the official ``kubernetes`` client against the API server (no ``kubectl``
or ``oc``). Every object of a run is owned by one anchor ConfigMap, so deleting
the anchor garbage-collects the run. Sensitive settings go into a per-submission
Secret referenced with ``secretKeyRef``, removed on cleanup. Nothing on the
scheduler's host is mounted: documents come from S3, and file settings (the S3
CA bundle) are copied into a ConfigMap.

Pods run under the namespace's restricted policy (OpenShift ``restricted-v2``,
Pod Security ``restricted``): non-root with an arbitrary UID, no capabilities,
no privilege escalation, ``RuntimeDefault`` seccomp, no service account token.
"""

import base64
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterator

from ..models import ResourceSample, RunSpec, RuntimeEvent, RuntimeStatus, ShardRuntime
from .base import (
    LABEL_ATTEMPT,
    LABEL_MANAGED,
    LABEL_RUN_ID,
    LABEL_SHARD,
    ExecutorCapabilities,
    ExecutorError,
    RunHandle,
)

logger = logging.getLogger(__name__)

CONTAINER_NAME = "agent"
# Why a Job was suspended (cancel, or stopped once its run ended).
ANNOTATION_STOP_REASON = "docling-agent/stop-reason"
# Pending reasons that never resolve on their own: the shard counts as failed
# (and can be retried) instead of leaving the run pending forever.
_FATAL_WAITING = frozenset(
    {
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
        "CreateContainerConfigError",
        "CreateContainerError",
    }
)
_STARTING = frozenset({"ContainerCreating", "PodInitializing"})
# What the scheduler must be allowed to do in its namespace:
# (verb, API group, resource, subresource).
_REQUIRED_ACCESS = (
    ("create", "batch", "jobs", ""),
    ("patch", "batch", "jobs", ""),
    ("create", "", "secrets", ""),
    ("delete", "", "secrets", ""),
    ("create", "", "configmaps", ""),
    ("delete", "", "configmaps", ""),
    ("list", "", "pods", ""),
    ("get", "", "pods", "log"),
)
# ConfigMaps are capped at 1 MiB.
_MAX_FILES_BYTES = 1_000_000
_QUANTITY_RE = re.compile(r"^([+-]?[0-9.]+(?:[eE][+-]?[0-9]+)?)([A-Za-z]*)$")
_QUANTITY_SUFFIXES = {
    "n": 1e-9, "u": 1e-6, "m": 1e-3, "": 1.0,
    "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15, "E": 1e18,
    "Ki": 2.0**10, "Mi": 2.0**20, "Gi": 2.0**30, "Ti": 2.0**40, "Pi": 2.0**50, "Ei": 2.0**60,
}
# Docker-style size suffixes (binary, as Podman reads them) -> Kubernetes.
_MEMORY_SUFFIXES = {"": "", "b": "", "k": "Ki", "m": "Mi", "g": "Gi"}


@dataclass
class KubeApis:
    """The API groups the executor uses, sharing one ``ApiClient``."""

    client: Any
    core: Any
    batch: Any
    custom: Any
    version: Any
    authz: Any


def _default_api_factory(kubeconfig: str | None, context: str | None) -> Callable[[], KubeApis]:
    def factory() -> KubeApis:
        from kubernetes import client, config

        try:
            api_client = config.new_client_from_config(
                config_file=kubeconfig, context=context, persist_config=False
            )
        except config.ConfigException:
            if kubeconfig or context:
                raise
            # no kubeconfig: the scheduler itself runs in a pod
            configuration = client.Configuration()
            config.load_incluster_config(client_configuration=configuration)
            api_client = client.ApiClient(configuration)
        return KubeApis(
            client=api_client,
            core=client.CoreV1Api(api_client),
            batch=client.BatchV1Api(api_client),
            custom=client.CustomObjectsApi(api_client),
            version=client.VersionApi(api_client),
            authz=client.AuthorizationV1Api(api_client),
        )

    return factory


def parse_quantity(value: Any) -> float | None:
    """Kubernetes quantity (``250m``, ``1.5``, ``512Mi``, ``1e3``) -> number."""
    match = _QUANTITY_RE.match(str(value).strip()) if value is not None else None
    if match is None or match.group(2) not in _QUANTITY_SUFFIXES:
        return None
    try:
        return float(match.group(1)) * _QUANTITY_SUFFIXES[match.group(2)]
    except ValueError:
        return None


def memory_quantity(memory: str) -> str:
    """Docker-style size (``512m``, ``1.5g``) -> Kubernetes quantity (``512Mi``)."""
    unit = memory[-1].lower() if memory[-1].isalpha() else ""
    number = memory[:-1] if unit else memory
    return number + _MEMORY_SUFFIXES[unit]


def _epoch(value: Any) -> float | None:
    if isinstance(value, datetime):
        return value.timestamp()
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return None


def _reason(obj: dict[str, Any]) -> str | None:
    reason, message = obj.get("reason"), obj.get("message")
    if reason and message:
        return f"{reason}: {message}"
    return reason or message or None


def _not_found(error: Exception) -> bool:
    return getattr(error, "status", None) == 404


def _job_conditions(job: dict[str, Any]) -> set[str]:
    return {
        c.get("type")
        for c in (job.get("status") or {}).get("conditions") or []
        if c.get("status") == "True"
    }


def _job_finished(job: dict[str, Any]) -> bool:
    return bool(_job_conditions(job) & {"Complete", "Failed"})


class KubernetesExecutor:
    name = "kubernetes"
    capabilities = ExecutorCapabilities(host_gateway=None, local_source=False, local_images=False)

    def __init__(
        self,
        namespace: str,
        kubeconfig: str | None = None,
        context: str | None = None,
        image_pull_secret: str | None = None,
        active_deadline: int | None = None,
        api_factory: Callable[[], KubeApis] | None = None,
    ):
        self.namespace = namespace
        self._image_pull_secret = image_pull_secret
        self._active_deadline = active_deadline
        self._api_factory = api_factory or _default_api_factory(kubeconfig, context)
        self._apis: KubeApis | None = None

    @property
    def apis(self) -> KubeApis:
        if self._apis is None:
            self._apis = self._api_factory()
        return self._apis

    def _to_dict(self, obj: Any) -> dict[str, Any]:
        """API model -> plain dict with the API's camelCase field names."""
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        return self.apis.client.sanitize_for_serialization(obj)

    # --- discovery -------------------------------------------------------

    def available(self) -> dict[str, Any]:
        try:
            version = self._to_dict(self.apis.version.get_code()).get("gitVersion")
            missing = [
                f"{verb} {resource}{'/' + subresource if subresource else ''}"
                for verb, group, resource, subresource in _REQUIRED_ACCESS
                if not self._allowed(verb, group, resource, subresource)
            ]
        except Exception as e:
            self._apis = None
            return {"available": False, "error": str(e), "namespace": self.namespace}
        details = {"version": version, "namespace": self.namespace}
        if missing:
            return {
                "available": False,
                "error": f"missing permissions in namespace {self.namespace}: {', '.join(missing)}",
                **details,
            }
        return {"available": True, **details}

    def _allowed(self, verb: str, group: str, resource: str, subresource: str) -> bool:
        review = self.apis.authz.create_self_subject_access_review(
            {
                "apiVersion": "authorization.k8s.io/v1",
                "kind": "SelfSubjectAccessReview",
                "spec": {
                    "resourceAttributes": {
                        "namespace": self.namespace,
                        "verb": verb,
                        "group": group,
                        "resource": resource,
                        "subresource": subresource,
                    }
                },
            }
        )
        return bool(self._to_dict(review).get("status", {}).get("allowed"))

    def image_exists(self, image: str) -> bool:
        # the cluster pulls from its registry; nothing to look at from here
        return True

    # --- lifecycle -------------------------------------------------------

    def submit(self, run: RunSpec, handle: RunHandle | None = None) -> RunHandle:
        previous = handle or {}
        handle = {
            "run_id": run.run_id,
            "namespace": self.namespace,
            "anchor": previous.get("anchor"),
            "secrets": list(previous.get("secrets", [])),
            "attempts": {k: list(v) for k, v in previous.get("attempts", {}).items()},
        }
        # one Secret/ConfigMap set per submission (secrets are removed when a
        # run ends, so a retry creates fresh ones)
        token = uuid.uuid4().hex[:8]
        created: list[tuple[str, str]] = []
        try:
            owner = self._ensure_anchor(run, handle, created)
            secret_name = None
            if run.secret_env:
                secret_name = self._create_secret(run, token, owner, created)
                handle["secrets"].append(secret_name)
            files = self._create_files(run, token, owner, created) if run.file_mounts else None
            for shard in run.shards:
                attempts = handle["attempts"].setdefault(str(shard), [])
                attempt = len(attempts)
                job_name = f"docling-agent-{run.run_id}-s{shard}-a{attempt}"
                self.apis.batch.create_namespaced_job(
                    self.namespace,
                    self._job(run, job_name, shard, attempt, owner, secret_name, files),
                )
                created.append(("job", job_name))
                attempts.append(job_name)
        except Exception as e:
            # leave nothing half-created behind: delete what this call made
            self._rollback(created)
            raise ExecutorError(f"failed to create jobs: {e}") from e
        return handle

    def _labels(self, run_id: str, **extra: str) -> dict[str, str]:
        return {
            "app.kubernetes.io/name": "docling-agent",
            "app.kubernetes.io/managed-by": "docling-scheduler",
            LABEL_RUN_ID: run_id,
            LABEL_MANAGED: "true",
            **extra,
        }

    def _ensure_anchor(
        self, run: RunSpec, handle: RunHandle, created: list[tuple[str, str]]
    ) -> dict[str, Any]:
        """Owner reference to the run's anchor ConfigMap, created if missing.

        Objects owned by a deleted anchor would be garbage-collected right
        away, so a retry checks that the recorded anchor still exists.
        """
        name = f"docling-run-{run.run_id}"
        anchor = handle.get("anchor")
        if anchor:
            try:
                existing = self._to_dict(self.apis.core.read_namespaced_config_map(name, self.namespace))
                if existing.get("metadata", {}).get("uid") == anchor["uid"]:
                    return self._owner(anchor)
            except Exception as e:
                if not _not_found(e):
                    raise
        body = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "labels": self._labels(run.run_id)},
            "data": {"run-id": run.run_id, "image": run.image, "shard-count": str(run.shard_count)},
        }
        result = self._to_dict(self.apis.core.create_namespaced_config_map(self.namespace, body))
        created.append(("configmap", name))
        handle["anchor"] = {"name": name, "uid": result["metadata"]["uid"]}
        return self._owner(handle["anchor"])

    @staticmethod
    def _owner(anchor: dict[str, str]) -> dict[str, Any]:
        return {"apiVersion": "v1", "kind": "ConfigMap", "name": anchor["name"], "uid": anchor["uid"]}

    def _create_secret(
        self, run: RunSpec, token: str, owner: dict[str, Any], created: list[tuple[str, str]]
    ) -> str:
        name = f"docling-run-{run.run_id}-{token}"
        self.apis.core.create_namespaced_secret(
            self.namespace,
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": name,
                    "labels": self._labels(run.run_id),
                    "ownerReferences": [owner],
                },
                "type": "Opaque",
                "stringData": dict(run.secret_env),
            },
        )
        created.append(("secret", name))
        return name

    def _create_files(
        self, run: RunSpec, token: str, owner: dict[str, Any], created: list[tuple[str, str]]
    ) -> tuple[str, list[dict[str, Any]]]:
        """Copy ``run.file_mounts`` into a ConfigMap; returns its name and the
        read-only volume mounts (one ``subPath`` per file)."""
        name = f"docling-run-{run.run_id}-files-{token}"
        data: dict[str, str] = {}
        binary: dict[str, str] = {}
        mounts: list[dict[str, Any]] = []
        total = 0
        for i, (target, source) in enumerate(sorted(run.file_mounts.items())):
            content = Path(source).read_bytes()
            total += len(content)
            key = f"file-{i}"
            try:
                data[key] = content.decode("utf-8")
            except UnicodeDecodeError:
                binary[key] = base64.b64encode(content).decode("ascii")
            mounts.append({"name": "files", "mountPath": target, "subPath": key, "readOnly": True})
        if total > _MAX_FILES_BYTES:
            raise ExecutorError(f"mounted files total {total} bytes; the limit is {_MAX_FILES_BYTES}")
        body: dict[str, Any] = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": name, "labels": self._labels(run.run_id), "ownerReferences": [owner]},
            "data": data,
        }
        if binary:
            body["binaryData"] = binary
        self.apis.core.create_namespaced_config_map(self.namespace, body)
        created.append(("configmap", name))
        return name, mounts

    def _job(
        self,
        run: RunSpec,
        job_name: str,
        shard: int,
        attempt: int,
        owner: dict[str, Any],
        secret_name: str | None,
        files: tuple[str, list[dict[str, Any]]] | None,
    ) -> dict[str, Any]:
        labels = self._labels(run.run_id, **{LABEL_SHARD: str(shard), LABEL_ATTEMPT: str(attempt)})
        env = [{"name": k, "value": v} for k, v in sorted(run.env.items())]
        env.append({"name": "SHARD_INDEX", "value": str(shard)})
        env += [
            {"name": k, "valueFrom": {"secretKeyRef": {"name": secret_name, "key": k}}}
            for k in sorted(run.secret_env)
        ]
        volumes: list[dict[str, Any]] = [{"name": "tmp", "emptyDir": {}}]
        mounts: list[dict[str, Any]] = [{"name": "tmp", "mountPath": "/tmp"}]
        if files is not None:
            volumes.append({"name": "files", "configMap": {"name": files[0]}})
            mounts += files[1]
        pod_spec: dict[str, Any] = {
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [
                {
                    "name": CONTAINER_NAME,
                    "image": run.image,
                    "imagePullPolicy": "IfNotPresent",
                    "env": env,
                    "resources": self._resources(run),
                    "securityContext": {
                        "allowPrivilegeEscalation": False,
                        "capabilities": {"drop": ["ALL"]},
                    },
                    "volumeMounts": mounts,
                }
            ],
            "volumes": volumes,
        }
        if self._image_pull_secret:
            pod_spec["imagePullSecrets"] = [{"name": self._image_pull_secret}]
        spec: dict[str, Any] = {
            # retries are the scheduler's (a new attempt per failed shard)
            "backoffLimit": 0,
            "template": {"metadata": {"labels": labels}, "spec": pod_spec},
        }
        if self._active_deadline:
            spec["activeDeadlineSeconds"] = self._active_deadline
        return {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name, "labels": labels, "ownerReferences": [owner]},
            "spec": spec,
        }

    @staticmethod
    def _resources(run: RunSpec) -> dict[str, Any]:
        limits: dict[str, str] = {}
        if run.cpus:
            limits["cpu"] = f"{max(1, round(run.cpus * 1000))}m"
        if run.memory:
            limits["memory"] = memory_quantity(run.memory)
        return {"requests": dict(limits), "limits": limits} if limits else {}

    def _rollback(self, created: list[tuple[str, str]]) -> None:
        for kind, name in reversed(created):
            try:
                if kind == "job":
                    self.apis.batch.delete_namespaced_job(
                        name, self.namespace, propagation_policy="Background"
                    )
                elif kind == "secret":
                    self.apis.core.delete_namespaced_secret(name, self.namespace)
                else:
                    self.apis.core.delete_namespaced_config_map(
                        name, self.namespace, propagation_policy="Background"
                    )
            except Exception as e:
                logger.debug("rollback of %s %s failed: %s", kind, name, e)

    # --- observation -----------------------------------------------------

    def _selector(self, handle: RunHandle) -> str:
        return f"{LABEL_RUN_ID}={handle['run_id']}"

    def _jobs(self, handle: RunHandle) -> dict[str, dict[str, Any]]:
        result = self.apis.batch.list_namespaced_job(
            self.namespace, label_selector=self._selector(handle)
        )
        return {j["metadata"]["name"]: j for j in self._to_dict(result).get("items") or []}

    def _pods_by_job(self, handle: RunHandle) -> dict[str, dict[str, Any]]:
        """Job name -> its newest pod."""
        result = self.apis.core.list_namespaced_pod(
            self.namespace, label_selector=self._selector(handle)
        )
        pods: dict[str, dict[str, Any]] = {}
        for pod in self._to_dict(result).get("items") or []:
            labels = pod["metadata"].get("labels") or {}
            job_name = labels.get("batch.kubernetes.io/job-name") or labels.get("job-name")
            current = pods.get(job_name) if job_name else None
            if job_name and (
                current is None
                or (_epoch(pod["metadata"].get("creationTimestamp")) or 0)
                > (_epoch(current["metadata"].get("creationTimestamp")) or 0)
            ):
                pods[job_name] = pod
        return pods

    def status(self, handle: RunHandle) -> RuntimeStatus:
        if not handle.get("run_id"):
            return RuntimeStatus()
        try:
            jobs = self._jobs(handle)
            pods = self._pods_by_job(handle)
        except Exception as e:
            return RuntimeStatus(error=f"kubernetes unavailable: {e}")
        shards = [
            self._shard_runtime(int(shard_key), attempt, job_name, jobs.get(job_name), pods.get(job_name))
            for shard_key, job_names in handle.get("attempts", {}).items()
            for attempt, job_name in enumerate(job_names)
        ]
        return RuntimeStatus(shards=shards)

    @staticmethod
    def _shard_runtime(
        shard: int,
        attempt: int,
        job_name: str,
        job: dict[str, Any] | None,
        pod: dict[str, Any] | None,
    ) -> ShardRuntime:
        def runtime(state: str, name: str | None = job_name, **kwargs: Any) -> ShardRuntime:
            return ShardRuntime(shard, attempt, job_name, name, state, **kwargs)

        if job is None:
            return runtime("missing", None)
        if pod is None:
            # suspended (cancelled or stopped), or failed with its pods
            # removed (e.g. DeadlineExceeded)
            if job.get("spec", {}).get("suspend"):
                annotations = job["metadata"].get("annotations") or {}
                return runtime("exited", error=annotations.get(ANNOTATION_STOP_REASON) or "suspended")
            failed = next(
                (
                    c for c in (job.get("status") or {}).get("conditions") or []
                    if c.get("type") == "Failed" and c.get("status") == "True"
                ),
                None,
            )
            if failed is not None:
                return runtime("exited", error=_reason(failed) or "job failed")
            return runtime("created")

        pod_name = pod["metadata"]["name"]
        pod_status = pod.get("status") or {}
        container = next(
            (c for c in pod_status.get("containerStatuses") or [] if c.get("name") == CONTAINER_NAME),
            {},
        )
        state = container.get("state") or {}
        if state.get("terminated"):
            terminated = state["terminated"]
            reason = terminated.get("reason")
            return runtime(
                "exited",
                pod_name,
                exit_code=terminated.get("exitCode"),
                oom_killed=reason == "OOMKilled",
                started_at=terminated.get("startedAt"),
                finished_at=terminated.get("finishedAt"),
                error=None if reason in (None, "Completed", "Error") else _reason(terminated),
            )
        if state.get("running"):
            return runtime("running", pod_name, started_at=state["running"].get("startedAt"))
        if pod_status.get("phase") == "Failed":
            # evicted or rejected before the container ran
            return runtime("exited", pod_name, error=_reason(pod_status) or "pod failed")
        waiting = state.get("waiting") or {}
        if waiting.get("reason") in _FATAL_WAITING:
            return runtime("exited", pod_name, error=_reason(waiting))
        error = None if waiting.get("reason") in (None, *_STARTING) else _reason(waiting)
        for condition in pod_status.get("conditions") or []:
            if condition.get("type") == "PodScheduled" and condition.get("status") == "False":
                error = _reason(condition)
        return runtime("created", pod_name, error=error)

    def _latest_job(self, handle: RunHandle, shard: int) -> str | None:
        attempts = handle.get("attempts", {}).get(str(shard), [])
        return attempts[-1] if attempts else None

    def logs(
        self, handle: RunHandle, shard: int = 0, follow: bool = True, tail: int | None = None
    ) -> Iterator[str]:
        job_name = self._latest_job(handle, shard)
        if job_name is None:
            raise ExecutorError(f"no job for shard {shard}")
        try:
            pod = self._pods_by_job(handle).get(job_name)
        except Exception as e:
            raise ExecutorError(f"kubernetes unavailable: {e}") from e
        if pod is None:
            raise ExecutorError(f"no pod for shard {shard} (not started yet, or removed)")
        kwargs: dict[str, Any] = {
            "container": CONTAINER_NAME,
            "follow": follow,
            "_preload_content": False,
        }
        if tail is not None:
            kwargs["tail_lines"] = tail
        try:
            response = self.apis.core.read_namespaced_pod_log(
                pod["metadata"]["name"], self.namespace, **kwargs
            )
        except Exception as e:
            raise ExecutorError(f"cannot read logs of shard {shard}: {e}") from e
        try:
            buffer = ""
            for chunk in response.stream(amt=None):
                buffer += chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else chunk
                *lines, buffer = buffer.split("\n")
                yield from (line for line in lines if line)
            if buffer:
                yield buffer
        finally:
            # a client that stops reading (SSE disconnect) must not leave the
            # follow stream open
            response.close()
            response.release_conn()

    def telemetry(self, handle: RunHandle) -> list[ResourceSample]:
        try:
            pods = self._pods_by_job(handle)
            result = self.apis.custom.list_namespaced_custom_object(
                "metrics.k8s.io", "v1beta1", self.namespace, "pods",
                label_selector=self._selector(handle),
            )
        except Exception as e:
            # metrics-server is optional
            logger.debug("pod metrics unavailable: %s", e)
            return []
        usage_by_pod = {
            item["metadata"]["name"]: next(
                (c.get("usage") or {} for c in item.get("containers") or [] if c.get("name") == CONTAINER_NAME),
                {},
            )
            for item in self._to_dict(result).get("items") or []
        }
        samples: list[ResourceSample] = []
        for shard_key in handle.get("attempts", {}):
            shard = int(shard_key)
            pod = pods.get(self._latest_job(handle, shard) or "")
            if pod is None or pod["metadata"]["name"] not in usage_by_pod:
                continue
            usage = usage_by_pod[pod["metadata"]["name"]]
            cores = parse_quantity(usage.get("cpu"))
            memory = parse_quantity(usage.get("memory"))
            container = next(
                (c for c in pod.get("spec", {}).get("containers") or [] if c.get("name") == CONTAINER_NAME),
                {},
            )
            limit = parse_quantity((container.get("resources") or {}).get("limits", {}).get("memory"))
            samples.append(
                ResourceSample(
                    shard_index=shard,
                    timestamp=time.time(),
                    cpu_percent=cores * 100 if cores is not None else None,
                    memory_bytes=int(memory) if memory is not None else None,
                    memory_limit_bytes=int(limit) if limit is not None else None,
                )
            )
        return samples

    def events(self, handle: RunHandle, since: float) -> list[RuntimeEvent]:
        # job name -> shard; pods are named "<job>-<suffix>"
        shards = {
            job_name: int(shard_key)
            for shard_key, job_names in handle.get("attempts", {}).items()
            for job_name in job_names
        }
        if not shards:
            return []
        try:
            result = self.apis.core.list_namespaced_event(self.namespace)
        except Exception as e:
            logger.debug("events failed: %s", e)
            return []
        out: list[RuntimeEvent] = []
        for event in self._to_dict(result).get("items") or []:
            involved = event.get("involvedObject") or {}
            name = involved.get("name") or ""
            job_name = name if name in shards else name.rsplit("-", 1)[0]
            if job_name not in shards:
                continue
            timestamp = (
                _epoch(event.get("lastTimestamp"))
                or _epoch(event.get("eventTime"))
                or _epoch(event.get("metadata", {}).get("creationTimestamp"))
                or 0.0
            )
            if timestamp < since:
                continue
            out.append(
                RuntimeEvent(
                    timestamp=timestamp,
                    shard_index=shards[job_name],
                    type=involved.get("kind", ""),
                    action=event.get("reason", ""),
                    message=f"{name}: {event.get('message', '')}".strip(),
                )
            )
        return sorted(out, key=lambda e: e.timestamp)

    # --- control ---------------------------------------------------------

    def _suspend(self, job_name: str, reason: str) -> None:
        """Suspend a Job: its pods get SIGTERM and are removed, the Job stays."""
        try:
            self.apis.batch.patch_namespaced_job(
                job_name,
                self.namespace,
                {"metadata": {"annotations": {ANNOTATION_STOP_REASON: reason}}, "spec": {"suspend": True}},
            )
        except Exception as e:
            logger.warning("failed to suspend %s: %s", job_name, e)

    def cancel(self, handle: RunHandle) -> None:
        try:
            jobs = self._jobs(handle)
        except Exception as e:
            raise ExecutorError(f"kubernetes unavailable: {e}") from e
        for shard_key in handle.get("attempts", {}):
            job = jobs.get(self._latest_job(handle, int(shard_key)) or "")
            if job is not None and not _job_finished(job) and not job.get("spec", {}).get("suspend"):
                self._suspend(job["metadata"]["name"], "cancelled")

    def _stop_stragglers(self, handle: RunHandle) -> None:
        """Suspend unfinished Jobs of an ended run (a pod stuck pulling its
        image retries forever), recording why the shard failed."""
        try:
            jobs = self._jobs(handle)
            runtime = {s.container_id: s for s in self.status(handle).shards}
        except Exception as e:
            logger.debug("cannot check jobs of %s: %s", handle.get("run_id"), e)
            return
        for name, job in jobs.items():
            if _job_finished(job) or job.get("spec", {}).get("suspend"):
                continue
            shard = runtime.get(name)
            self._suspend(name, (shard.error if shard else None) or "stopped: run ended")

    def cleanup(self, handle: RunHandle, remove_containers: bool = False) -> None:
        for name in handle.get("secrets", []):
            try:
                self.apis.core.delete_namespaced_secret(name, self.namespace)
            except Exception as e:
                if not _not_found(e):
                    logger.debug("secret %s not removed: %s", name, e)
        handle["secrets"] = []
        if not handle.get("run_id"):
            return
        if not remove_containers:
            self._stop_stragglers(handle)
            return
        anchor = handle.get("anchor")
        try:
            if anchor is None:
                raise LookupError("no anchor")
            # Jobs, pods, Secrets and ConfigMaps are garbage-collected with it
            self.apis.core.delete_namespaced_config_map(
                anchor["name"], self.namespace, propagation_policy="Background"
            )
        except Exception as e:
            if not (_not_found(e) or isinstance(e, LookupError)):
                logger.warning("failed to delete run %s: %s", handle["run_id"], e)
            # anchor already gone: delete the jobs directly
            for job_names in handle.get("attempts", {}).values():
                for job_name in job_names:
                    try:
                        self.apis.batch.delete_namespaced_job(
                            job_name, self.namespace, propagation_policy="Background"
                        )
                    except Exception as job_error:
                        if not _not_found(job_error):
                            logger.warning("failed to delete %s: %s", job_name, job_error)


__all__ = ["KubernetesExecutor", "KubeApis", "parse_quantity", "memory_quantity"]
