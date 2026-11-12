#!/usr/bin/env python
"""Scheduler process configuration (``SCHEDULER_*`` environment variables)."""

import os
import secrets
from dataclasses import dataclass, field
from pathlib import Path


def _list_env(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return [item.strip() for item in raw.split(",") if item.strip()]


def _default_db_path() -> Path:
    base = os.getenv("XDG_DATA_HOME") or os.path.join(Path.home(), ".local", "share")
    return Path(base) / "docling-agent" / "scheduler.db"


def _optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    return int(raw) if raw and raw.strip() else None


@dataclass
class SchedulerConfig:
    # Container runtime: "podman" (local) or "kubernetes" (remote cluster).
    executor: str = "podman"
    # MLflow server the scheduler monitors; containers reach the same server
    # (localhost is rewritten to host.containers.internal for Podman).
    mlflow_tracking_uri: str = "http://localhost:5000"
    # Tracking URI rendered into containers when they reach MLflow by another
    # address than the scheduler does (e.g. an in-cluster Service).
    container_mlflow_tracking_uri: str | None = None
    # Base URL of the MLflow UI for deep links (defaults to the tracking URI
    # when that is an http(s) URL).
    mlflow_ui_uri: str | None = None
    db_path: Path = field(default_factory=_default_db_path)
    # .env used for form defaults and for secrets the operator doesn't type in.
    envfile: str | None = None
    default_image: str = "localhost/docling-agent:latest"
    # Image references must start with one of these prefixes.
    image_allowlist: list[str] = field(
        default_factory=lambda: ["localhost/docling-agent", "docling-agent"]
    )
    # Host folders mounted into containers must live under one of these.
    allowed_mount_roots: list[str] = field(default_factory=lambda: [str(Path.home())])
    podman_url: str | None = None
    # SELinux relabel option for the read-only document mount ("z" = shared,
    # so all shard containers can read it; "" disables relabeling).
    podman_relabel: str = "z"
    # Kubernetes executor: kubeconfig file and context (defaults: the usual
    # kubeconfig lookup, then in-cluster config), namespace for run objects,
    # optional image pull secret and per-shard deadline in seconds.
    k8s_kubeconfig: str | None = None
    k8s_context: str | None = None
    k8s_namespace: str | None = None
    k8s_image_pull_secret: str | None = None
    k8s_active_deadline: int | None = None
    max_shards: int = 32
    # Pre-flight warns when shards x max_concurrency exceeds this.
    concurrency_warning_threshold: int = 32
    token: str = field(default_factory=lambda: secrets.token_urlsafe(32))
    allowed_hosts: list[str] = field(default_factory=lambda: ["127.0.0.1", "localhost"])
    # Extra browser origins allowed to call the API (e.g. the Vite dev server).
    allowed_origins: list[str] = field(default_factory=list)
    # Seconds between background reconciliation passes (0 disables).
    reconcile_interval: float = 10.0
    # Built web UI (docling-ui's dist/) served at /; None or a missing
    # directory disables it.
    web_dir: Path | None = None

    @property
    def effective_mlflow_ui_uri(self) -> str | None:
        if self.mlflow_ui_uri:
            return self.mlflow_ui_uri.rstrip("/")
        if self.mlflow_tracking_uri.startswith(("http://", "https://")):
            return self.mlflow_tracking_uri.rstrip("/")
        return None

    @classmethod
    def from_env(cls) -> "SchedulerConfig":
        defaults = cls()
        envfile = os.getenv("SCHEDULER_ENVFILE")
        if envfile is None and Path(".env").exists():
            envfile = ".env"
        return cls(
            executor=(os.getenv("SCHEDULER_EXECUTOR") or defaults.executor).strip().lower(),
            mlflow_tracking_uri=os.getenv("SCHEDULER_MLFLOW_TRACKING_URI")
            or os.getenv("MLFLOW_TRACKING_URI")
            or defaults.mlflow_tracking_uri,
            container_mlflow_tracking_uri=os.getenv("SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI")
            or None,
            mlflow_ui_uri=os.getenv("SCHEDULER_MLFLOW_UI_URI") or None,
            db_path=Path(os.getenv("SCHEDULER_DB_PATH") or defaults.db_path),
            envfile=envfile or None,
            default_image=os.getenv("SCHEDULER_DEFAULT_IMAGE") or defaults.default_image,
            image_allowlist=_list_env("SCHEDULER_IMAGE_ALLOWLIST", defaults.image_allowlist),
            allowed_mount_roots=_list_env(
                "SCHEDULER_ALLOWED_MOUNT_ROOTS", defaults.allowed_mount_roots
            ),
            podman_url=os.getenv("SCHEDULER_PODMAN_URL") or None,
            podman_relabel=os.getenv("SCHEDULER_PODMAN_RELABEL", defaults.podman_relabel),
            k8s_kubeconfig=os.getenv("SCHEDULER_K8S_KUBECONFIG") or None,
            k8s_context=os.getenv("SCHEDULER_K8S_CONTEXT") or None,
            k8s_namespace=os.getenv("SCHEDULER_K8S_NAMESPACE") or None,
            k8s_image_pull_secret=os.getenv("SCHEDULER_K8S_IMAGE_PULL_SECRET") or None,
            k8s_active_deadline=_optional_int("SCHEDULER_K8S_ACTIVE_DEADLINE"),
            max_shards=int(os.getenv("SCHEDULER_MAX_SHARDS") or defaults.max_shards),
            concurrency_warning_threshold=int(
                os.getenv("SCHEDULER_CONCURRENCY_WARNING")
                or defaults.concurrency_warning_threshold
            ),
            token=os.getenv("SCHEDULER_TOKEN") or defaults.token,
            allowed_hosts=_list_env("SCHEDULER_ALLOWED_HOSTS", defaults.allowed_hosts),
            allowed_origins=_list_env("SCHEDULER_ALLOWED_ORIGINS", []),
            reconcile_interval=float(
                os.getenv("SCHEDULER_RECONCILE_INTERVAL") or defaults.reconcile_interval
            ),
            web_dir=Path(os.environ["SCHEDULER_WEB_DIR"]) if os.getenv("SCHEDULER_WEB_DIR") else None,
        )


__all__ = ["SchedulerConfig"]
