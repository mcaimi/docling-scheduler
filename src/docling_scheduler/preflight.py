#!/usr/bin/env python
"""Pre-flight validation and rendering of a run request.

Produces everything needed to launch (resolved settings, container env and
secrets) plus a redacted, user-facing report: errors, warnings, document
count, shard plan, concurrency and a manifest preview.
"""

import math
import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from botocore.exceptions import BotoCoreError, ClientError

from .config import SchedulerConfig
from .discovery import list_documents, list_s3_documents, s3_source_uri, shard_plan
from .executors.base import Executor, ExecutorError
from .models import RunRequest, RunTarget
from .settings_io import (
    CONTAINER_DATA_DIR,
    CONTAINER_S3_CA_BUNDLE,
    merge_settings,
    non_secret_settings,
    render_environment,
    rewrite_loopback,
    rewrite_loopback_url,
)

# Placeholder the agent uses for API keys that are not set. The contract
# omits sensitive defaults, so an unset key means the container gets this.
_PLACEHOLDER_KEY = "your-api-key-here"
_IMAGE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,254}$")
_MEMORY_RE = re.compile(r"^\d+(\.\d+)?[bkmg]?$", re.IGNORECASE)
_POSITIVE_INT_KEYS = (
    "max_concurrency",
    "vector_store_batch_size",
    "vector_store_max_batch_workers",
    "vector_store_retry_attempts",
    "vector_store_circuit_breaker_threshold",
)
# Filled in at launch, shown as placeholders in previews.
LAUNCH_PLACEHOLDER = "<assigned at launch>"


def check_mlflow_http(uri: str, timeout: float = 3.0) -> str | None:
    """``None`` if the MLflow server answers ``/health``, else an error message."""
    try:
        with urllib.request.urlopen(f"{uri.rstrip('/')}/health", timeout=timeout) as response:
            if response.status == 200:
                return None
            return f"MLflow health check returned HTTP {response.status}"
    except (urllib.error.URLError, OSError, ValueError) as e:
        return f"MLflow server not reachable at {uri}: {e}"


@dataclass
class Preflight:
    errors: list[dict[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    document_count: int = 0
    shard_plan: list[dict[str, Any]] = field(default_factory=list)
    concurrency: dict[str, Any] = field(default_factory=dict)
    preview: dict[str, Any] = field(default_factory=dict)
    # launch inputs (never serialised)
    image: str = ""
    host_folder: str = ""
    file_mounts: dict[str, str] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    secret_env: dict[str, str] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.errors

    def error(self, field_name: str, message: str) -> None:
        self.errors.append({"field": field_name, "message": message})

    def public(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": self.errors,
            "warnings": self.warnings,
            "document_count": self.document_count,
            "shard_plan": self.shard_plan,
            "concurrency": self.concurrency,
            "preview": self.preview,
        }


def _missing_key(value: Any) -> bool:
    return not value or value == _PLACEHOLDER_KEY


def _image_allowed(image: str, allowlist: list[str]) -> bool:
    for prefix in allowlist:
        if image == prefix or (
            image.startswith(prefix) and image[len(prefix)] in ":@"
        ):
            return True
    return False


def _check_host_path(
    raw: str, config: SchedulerConfig, report: Preflight, field_name: str, *, directory: bool
) -> Path | None:
    """Validate a host path to bind-mount: absolute, existing, readable and
    (after resolving symlinks) under an allowed mount root."""
    noun = "folder" if directory else "file"
    if any(c in raw for c in "\x00\n\r"):
        report.error(field_name, "invalid characters in path")
        return None
    path = Path(os.path.expanduser(raw))
    if not path.is_absolute():
        report.error(field_name, "must be an absolute path")
        return None
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError):
        report.error(field_name, f"{noun} does not exist")
        return None
    if directory and not resolved.is_dir():
        report.error(field_name, "not a directory")
        return None
    if not directory and not resolved.is_file():
        report.error(field_name, "not a file")
        return None
    roots = [Path(os.path.expanduser(r)).resolve() for r in config.allowed_mount_roots]
    if not any(resolved == root or resolved.is_relative_to(root) for root in roots):
        report.error(
            field_name,
            "outside the allowed mount roots (SCHEDULER_ALLOWED_MOUNT_ROOTS)",
        )
        return None
    if not os.access(resolved, os.R_OK | (os.X_OK if directory else 0)):
        report.error(field_name, f"{noun} is not readable")
        return None
    return resolved


def _local_documents(target: RunTarget, config: SchedulerConfig, report: Preflight) -> list[str] | None:
    if not target.host_folder:
        report.error("target.host_folder", "required with source_backend=local")
        return None
    folder = _check_host_path(
        target.host_folder, config, report, "target.host_folder", directory=True
    )
    if folder is None:
        return None
    report.host_folder = str(folder)
    paths = list_documents(folder)
    if not paths:
        report.error("target.host_folder", "no supported documents in folder")
    return paths


def _s3_documents(
    settings: dict[str, Any],
    target: RunTarget,
    config: SchedulerConfig,
    report: Preflight,
    list_s3: Callable[[dict[str, Any]], list[str]],
) -> list[str] | None:
    """List the bucket from this host (catches a wrong bucket, endpoint or
    credentials before any container starts) and mount the CA bundle."""
    if target.host_folder:
        report.warnings.append("target.host_folder is not used with source_backend=s3")
    if not settings.get("s3_access_key_id"):
        report.warnings.append(
            "s3_access_key_id is not set: containers fall back to boto3's default "
            "credential chain, which doesn't include this host's ~/.aws or AWS_PROFILE"
        )
    ca_ok = True
    if settings.get("s3_ca_bundle"):
        bundle = _check_host_path(
            settings["s3_ca_bundle"], config, report, "settings.s3_ca_bundle", directory=False
        )
        ca_ok = bundle is not None
        if bundle is not None:
            settings["s3_ca_bundle"] = str(bundle)
            report.file_mounts[CONTAINER_S3_CA_BUNDLE] = str(bundle)
    if not settings.get("s3_bucket"):
        report.error("settings.s3_bucket", "required with source_backend=s3")
        return None
    if not ca_ok:
        return None
    try:
        paths = list_s3(settings)
    except (BotoCoreError, ClientError, ValueError) as e:
        report.error("settings.s3_bucket", f"cannot list {s3_source_uri(settings)}: {e}")
        return None
    if not paths:
        report.error("settings.s3_prefix", f"no supported documents under {s3_source_uri(settings)}")
    return paths


def _check_settings(settings: dict[str, Any], shards: int, report: Preflight) -> None:
    if settings.get("chunking_strategy") == "fixed":
        size, overlap = settings.get("chunk_size"), settings.get("chunk_overlap")
        if size is not None and size <= 0:
            report.error("settings.chunk_size", "must be > 0")
        elif size is not None and overlap is not None and not 0 <= overlap < size:
            report.error("settings.chunk_overlap", "must be >= 0 and smaller than chunk_size")
    for key in _POSITIVE_INT_KEYS:
        value = settings.get(key)
        if value is not None and value < 1:
            report.error(f"settings.{key}", "must be >= 1")

    backend = settings.get("vector_store_backend")
    if backend == "chroma" and not settings.get("chroma_host"):
        report.error(
            "settings.chroma_host",
            "embedded Chroma would write inside the container and be lost; "
            "set chroma_host to a Chroma server",
        )
    if backend == "milvus":
        uri = str(settings.get("milvus_uri") or "")
        if not uri.startswith(("http://", "https://", "tcp://", "grpc://")):
            report.error(
                "settings.milvus_uri",
                "must be a Milvus server URL (Milvus Lite files are lost with the container)",
            )
    if backend in ("chroma", "milvus") and shards > 1:
        report.warnings.append(
            f"{shards} shards will write to one {backend} instance concurrently"
        )

    if settings.get("mlflow_langchain_autolog"):
        report.warnings.append(
            "mlflow_langchain_autolog records whole documents in spans; traces whose "
            "payload exceeds MLflow limits are dropped and the document view will miss them"
        )
    if _missing_key(settings.get("embedding_api_key")):
        report.warnings.append("embedding_api_key is not set; embeddings will fail")
    if settings.get("user_prompt") and _missing_key(settings.get("api_key")):
        report.warnings.append(
            "user_prompt is set but api_key is not set; "
            "prompt extraction will fail and defaults will be used"
        )


def run_preflight(
    request: RunRequest,
    *,
    config: SchedulerConfig,
    defaults: dict[str, Any],
    executor: Executor,
    check_mlflow: Callable[[str], str | None] = check_mlflow_http,
    list_s3: Callable[[dict[str, Any]], list[str]] = list_s3_documents,
) -> Preflight:
    report = Preflight()
    target = request.target

    # --- target --------------------------------------------------------
    if target.type != executor.name:
        report.error("target.type", f"this scheduler runs the {executor.name} executor")
    image = (target.image or config.default_image).strip()
    if not _IMAGE_RE.match(image):
        report.error("target.image", "invalid image reference")
    elif not _image_allowed(image, config.image_allowlist):
        report.error("target.image", "image not in the allowlist (SCHEDULER_IMAGE_ALLOWLIST)")
    report.image = image

    if target.shards > config.max_shards:
        report.error("target.shards", f"at most {config.max_shards} shards")
    if target.memory and not _MEMORY_RE.match(target.memory):
        report.error("target.memory", "expected a size such as 512m or 4g")

    # --- settings ------------------------------------------------------
    settings, errors, explicit = merge_settings(defaults, request.settings)
    for key, message in errors.items():
        report.error(f"settings.{key}", message)
    _check_settings(settings, target.shards, report)

    # --- documents -----------------------------------------------------
    s3 = settings.get("source_backend") == "s3"
    if s3:
        paths = _s3_documents(settings, target, config, report, list_s3)
    else:
        paths = _local_documents(target, config, report)
    if paths is not None:
        report.document_count = len(paths)
        report.shard_plan = shard_plan(paths, target.shards)
        if paths and target.shards > len(paths):
            report.warnings.append(
                f"{target.shards} shards for {len(paths)} documents: some shards will be empty"
            )
    if report.file_mounts:
        # containers see the mounted copy, not the host path
        settings["s3_ca_bundle"] = CONTAINER_S3_CA_BUNDLE

    # a container sees the host's CPU count, not its --cpus limit
    if "max_concurrency" not in explicit and target.cpus:
        settings["max_concurrency"] = max(1, math.floor(target.cpus))
        explicit.add("max_concurrency")
    per_shard = settings.get("max_concurrency") if "max_concurrency" in explicit else None
    report.concurrency = {
        "shards": target.shards,
        "max_concurrency_per_shard": per_shard,
        "total_concurrent_documents": per_shard * target.shards if per_shard else None,
        "vector_store_write_workers": (
            per_shard * target.shards * settings.get("vector_store_max_batch_workers", 1)
            if per_shard and settings.get("vector_store_backend") not in (None, "none")
            else None
        ),
    }
    total = report.concurrency["total_concurrent_documents"]
    if per_shard is None:
        report.warnings.append(
            "max_concurrency not set: each container uses the host CPU count; "
            "set max_concurrency or a CPU limit"
        )
    elif total is not None and total > config.concurrency_warning_threshold:
        report.warnings.append(
            f"{total} documents in flight at once may hit embedding API rate limits"
        )

    # --- endpoints -----------------------------------------------------
    settings, rewritten = rewrite_loopback(settings)
    if rewritten:
        report.warnings.append(
            "loopback endpoints rewritten to host.containers.internal: " + ", ".join(rewritten)
        )
    tracking_uri = config.mlflow_tracking_uri
    if not tracking_uri.startswith(("http://", "https://")):
        report.error(
            "mlflow",
            "containers need an MLflow tracking server; a local file/sqlite store is not reachable",
        )
        container_tracking_uri = tracking_uri
    else:
        container_tracking_uri = rewrite_loopback_url(tracking_uri) or tracking_uri
        if (problem := check_mlflow(tracking_uri)) is not None:
            report.error("mlflow", problem)

    # --- runtime -------------------------------------------------------
    availability = executor.available()
    if not availability.get("available"):
        report.error("target", f"{executor.name} unavailable: {availability.get('error')}")
    elif report.ok:
        try:
            if not executor.image_exists(image):
                report.warnings.append(f"image {image} not present locally; it will be pulled")
        except ExecutorError as e:
            report.error("target.image", str(e))

    # --- rendering -----------------------------------------------------
    managed = {
        "ENABLE_TRACING": "1",
        "LOG_FORMAT": "json",
        "MLFLOW_TRACKING_URI": container_tracking_uri,
        "SHARD_COUNT": str(target.shards),
        "SCHEDULER_EXECUTOR": executor.name,
    }
    if not s3:
        managed["FOLDER_PATH"] = CONTAINER_DATA_DIR
    env, secret_env = render_environment(settings, explicit_keys=explicit, managed=managed)
    report.settings, report.env, report.secret_env = settings, env, secret_env
    report.preview = {
        "executor": executor.name,
        "image": image,
        "shards": target.shards,
        "source": s3_source_uri(settings) if s3 else report.host_folder,
        "mount": (
            None if s3
            else {"source": report.host_folder, "target": CONTAINER_DATA_DIR, "read_only": True}
        ),
        "file_mounts": [
            {"source": host_path, "target": path, "read_only": True}
            for path, host_path in sorted(report.file_mounts.items())
        ],
        "resources": {"cpus": target.cpus, "memory": target.memory},
        "env": {
            **dict(sorted(env.items())),
            "SHARD_INDEX": "<0..shards-1>",
            "MLFLOW_PARENT_RUN_ID": LAUNCH_PLACEHOLDER,
            "MLFLOW_SESSION_ID": LAUNCH_PLACEHOLDER,
            "SCHEDULER_RUN_ID": LAUNCH_PLACEHOLDER,
        },
        "secret_env": {name: "<secret>" for name in sorted(secret_env)},
        "settings": non_secret_settings(settings),
    }
    return report


__all__ = ["Preflight", "run_preflight", "check_mlflow_http", "LAUNCH_PLACEHOLDER"]
