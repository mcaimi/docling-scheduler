#!/usr/bin/env python
"""Executor selection from the scheduler configuration."""

import pytest

from docling_scheduler.config import SchedulerConfig
from docling_scheduler.executors import KubernetesExecutor, PodmanExecutor, create_executor


def test_podman_is_the_default():
    assert isinstance(create_executor(SchedulerConfig()), PodmanExecutor)


def test_kubernetes_needs_a_namespace():
    with pytest.raises(ValueError, match="SCHEDULER_K8S_NAMESPACE"):
        create_executor(SchedulerConfig(executor="kubernetes"))
    executor = create_executor(SchedulerConfig(executor="kubernetes", k8s_namespace="docling"))
    assert isinstance(executor, KubernetesExecutor) and executor.namespace == "docling"


def test_unknown_executor():
    with pytest.raises(ValueError, match="unknown SCHEDULER_EXECUTOR"):
        create_executor(SchedulerConfig(executor="nomad"))


def test_kubernetes_settings_from_env(monkeypatch):
    monkeypatch.setenv("SCHEDULER_EXECUTOR", " Kubernetes ")
    monkeypatch.setenv("SCHEDULER_K8S_NAMESPACE", "docling")
    monkeypatch.setenv("SCHEDULER_K8S_CONTEXT", "prod")
    monkeypatch.setenv("SCHEDULER_K8S_ACTIVE_DEADLINE", "7200")
    monkeypatch.setenv("SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI", "http://mlflow.svc:5000")
    config = SchedulerConfig.from_env()
    assert config.executor == "kubernetes"
    assert (config.k8s_namespace, config.k8s_context, config.k8s_active_deadline) == ("docling", "prod", 7200)
    assert config.k8s_kubeconfig is None and config.k8s_image_pull_secret is None
    assert config.container_mlflow_tracking_uri == "http://mlflow.svc:5000"
