"""Container executors."""

from ..config import SchedulerConfig
from .base import Executor, ExecutorCapabilities, ExecutorError, RunHandle
from .kubernetes import KubernetesExecutor
from .podman import PodmanExecutor

EXECUTOR_NAMES = (PodmanExecutor.name, KubernetesExecutor.name)


def create_executor(config: SchedulerConfig) -> Executor:
    """The executor selected by ``SCHEDULER_EXECUTOR``.

    Raises ``ValueError`` for an unknown executor or incomplete configuration.
    """
    if config.executor == PodmanExecutor.name:
        return PodmanExecutor(base_url=config.podman_url, relabel=config.podman_relabel)
    if config.executor == KubernetesExecutor.name:
        if not config.k8s_namespace:
            raise ValueError("SCHEDULER_K8S_NAMESPACE is required with SCHEDULER_EXECUTOR=kubernetes")
        return KubernetesExecutor(
            namespace=config.k8s_namespace,
            kubeconfig=config.k8s_kubeconfig,
            context=config.k8s_context,
            image_pull_secret=config.k8s_image_pull_secret,
            active_deadline=config.k8s_active_deadline,
        )
    raise ValueError(
        f"unknown SCHEDULER_EXECUTOR {config.executor!r} (expected one of {', '.join(EXECUTOR_NAMES)})"
    )


__all__ = [
    "Executor",
    "ExecutorCapabilities",
    "ExecutorError",
    "RunHandle",
    "PodmanExecutor",
    "KubernetesExecutor",
    "EXECUTOR_NAMES",
    "create_executor",
]
