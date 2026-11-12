#!/usr/bin/env python
"""Pre-flight validation of run requests."""

import pytest
from botocore.exceptions import ClientError

from docling_scheduler.config import SchedulerConfig
from docling_scheduler.executors.kubernetes import KubernetesExecutor
from docling_scheduler.models import PodmanTarget, RunRequest, RunTarget
from docling_scheduler.preflight import run_preflight
from docling_scheduler.settings_io import load_defaults
from docling_scheduler.contract import SETTINGS_SPEC

from .fakes import FakeExecutor


@pytest.fixture
def docs(tmp_path):
    folder = tmp_path / "docs"
    folder.mkdir()
    for i in range(3):
        (folder / f"d{i}.md").write_text("# x")
    return folder


@pytest.fixture
def env(monkeypatch, tmp_path):
    for spec in SETTINGS_SPEC:
        for key in (spec.env, *spec.env_fallbacks):
            monkeypatch.delenv(key, raising=False)
    return load_defaults(None)


@pytest.fixture
def config(tmp_path):
    return SchedulerConfig(allowed_mount_roots=[str(tmp_path)], db_path=tmp_path / "db")


def check(request, config, defaults, executor=None, mlflow_error=None, list_s3=None):
    return run_preflight(
        request,
        config=config,
        defaults={**defaults, "embedding_api_key": "sk-real"},
        executor=executor or FakeExecutor(),
        check_mlflow=lambda uri: mlflow_error,
        list_s3=list_s3 or (lambda settings: []),
    )


def request(folder, settings=None, **target):
    host_folder = str(folder) if folder is not None else None
    return RunRequest(settings=settings or {}, target=PodmanTarget(host_folder=host_folder, **target))


def fields(report):
    return {e["field"] for e in report.errors}


def test_valid_request_renders_launch_inputs(docs, config, env):
    report = check(request(docs, {"chunk_size": 400}, shards=2, cpus=2.5), config, env)
    assert report.ok, report.errors
    assert report.document_count == 3
    assert [p["document_count"] for p in report.shard_plan] == [2, 1]
    assert report.env["FOLDER_PATH"] == "/data/in"
    assert report.env["ENABLE_TRACING"] == "1"
    assert report.env["LOG_FORMAT"] == "json"
    assert report.env["SHARD_COUNT"] == "2"
    assert report.env["CHUNK_SIZE"] == "400"
    assert report.env["MAX_CONCURRENCY"] == "2"  # derived from the CPU limit
    assert report.env["MLFLOW_TRACKING_URI"] == "http://host.containers.internal:5000"
    assert report.secret_env["EMBEDDING_API_KEY"] == "sk-real"
    assert report.concurrency["total_concurrent_documents"] == 4
    public = report.public()
    assert "sk-real" not in str(public)
    assert public["preview"]["secret_env"]["EMBEDDING_API_KEY"] == "<secret>"
    assert "embedding_api_key" not in public["preview"]["settings"]


def test_folder_rules(tmp_path, docs, config, env):
    outside = tmp_path.parent
    assert "target.host_folder" in fields(check(request(outside), config, env))
    assert "target.host_folder" in fields(check(request(tmp_path / "nope"), config, env))
    assert "target.host_folder" in fields(check(request("relative/path"), config, env))
    empty = tmp_path / "empty"
    empty.mkdir()
    assert "target.host_folder" in fields(check(request(empty), config, env))
    link = tmp_path / "escape"
    link.symlink_to(outside)
    assert "target.host_folder" in fields(check(request(link), config, env))


def test_image_allowlist(docs, config, env):
    assert "target.image" in fields(check(request(docs, image="quay.io/evil/image:1"), config, env))
    assert "target.image" in fields(check(request(docs, image="localhost/docling-agent-evil"), config, env))
    assert "target.image" in fields(check(request(docs, image="-v /:/host"), config, env))
    assert check(request(docs, image="localhost/docling-agent:v2"), config, env).ok


def test_settings_errors(docs, config, env):
    report = check(request(docs, {"chunk_size": 100, "chunk_overlap": 100, "folder_path": "/"}), config, env)
    assert {"settings.chunk_overlap", "settings.folder_path"} <= fields(report)


def test_vector_store_rules(docs, config, env):
    assert "settings.chroma_host" in fields(
        check(request(docs, {"vector_store_backend": "chroma"}), config, env)
    )
    assert "settings.milvus_uri" in fields(
        check(request(docs, {"vector_store_backend": "milvus", "milvus_uri": "./milvus.db"}), config, env)
    )
    report = check(
        request(docs, {"vector_store_backend": "chroma", "chroma_host": "localhost"}, shards=2), config, env
    )
    assert report.ok
    assert report.env["CHROMA_HOST"] == "host.containers.internal"
    assert any("rewritten" in w for w in report.warnings)
    assert any("concurrently" in w for w in report.warnings)


def test_mlflow_must_be_reachable_server(docs, config, env, tmp_path):
    assert "mlflow" in fields(check(request(docs), config, env, mlflow_error="down"))
    sqlite_config = SchedulerConfig(
        allowed_mount_roots=[str(tmp_path)], mlflow_tracking_uri="sqlite:///x.db"
    )
    assert "mlflow" in fields(check(request(docs), sqlite_config, env))


def test_runtime_checks(docs, config, env):
    executor = FakeExecutor()
    executor.available_result = {"available": False, "error": "no socket"}
    assert "target" in fields(check(request(docs), config, env, executor))
    executor = FakeExecutor()
    executor.images = set()
    report = check(request(docs), config, env, executor)
    assert report.ok and any("will be pulled" in w for w in report.warnings)


def test_concurrency_warnings(docs, config, env):
    report = check(request(docs), config, env)
    assert any("max_concurrency not set" in w for w in report.warnings)
    report = check(request(docs, {"max_concurrency": 20}, shards=3), config, env)
    assert any("rate limits" in w for w in report.warnings)


def test_limits(docs, config, env):
    assert "target.shards" in fields(check(request(docs, shards=33), config, env))
    assert "target.memory" in fields(check(request(docs, memory="lots"), config, env))


def test_embedded_stores_rejected(docs, config, env):
    assert "settings.chroma_host" in fields(
        check(request(docs, {"vector_store_backend": "chroma", "chroma_persist_directory": "/x"}), config, env)
    )
    assert "settings.milvus_uri" in fields(
        check(request(docs, {"vector_store_backend": "milvus", "milvus_uri": "./milvus.db"}), config, env)
    )


def test_unset_api_keys_warn(docs, config, env):
    report = run_preflight(
        request(docs, {"user_prompt": "use hybrid chunking"}),
        config=config, defaults=env, executor=FakeExecutor(), check_mlflow=lambda uri: None,
    )
    assert any("embedding_api_key is not set" in w for w in report.warnings)
    assert any("api_key is not set" in w and "user_prompt" in w for w in report.warnings)


class OtherExecutor(FakeExecutor):
    name = "kubernetes"


def test_target_type_must_match_executor(docs, config, env):
    assert "target.type" in fields(check(request(docs), config, env, OtherExecutor()))


S3 = {
    "source_backend": "s3",
    "s3_bucket": "docs",
    "s3_prefix": "in",
    "s3_endpoint_url": "http://localhost:9000",
    "s3_access_key_id": "AKIA-x",
    "s3_secret_access_key": "s3-secret",
}
S3_PATHS = ["s3://docs/in/a.pdf", "s3://docs/in/b.pdf", "s3://docs/in/c.md"]


class Lister:
    def __init__(self, paths=S3_PATHS, error=None):
        self.paths, self.error, self.seen = paths, error, []

    def __call__(self, settings):
        self.seen.append(dict(settings))
        if self.error:
            raise self.error
        return self.paths


def test_s3_source_needs_no_host_folder(config, env):
    lister = Lister()
    report = check(request(None, S3, shards=2), config, env, list_s3=lister)
    assert report.ok, report.errors
    assert report.document_count == 3
    assert [p["documents"] for p in report.shard_plan] == [["a.pdf", "c.md"], ["b.pdf"]]
    # listed from the host as configured, rendered for the container
    assert lister.seen[0]["s3_endpoint_url"] == "http://localhost:9000"
    assert report.env["S3_ENDPOINT_URL"] == "http://host.containers.internal:9000"
    assert report.env["SOURCE_BACKEND"] == "s3"
    assert report.env["S3_BUCKET"] == "docs"
    assert "FOLDER_PATH" not in report.env
    assert report.secret_env["S3_SECRET_ACCESS_KEY"] == "s3-secret"
    assert report.host_folder == "" and report.file_mounts == {}
    public = report.public()
    assert "s3-secret" not in str(public) and "AKIA-x" not in str(public)
    assert public["preview"]["source"] == "s3://docs/in/"
    assert public["preview"]["mount"] is None


def test_s3_source_errors(docs, config, env):
    assert "settings.s3_bucket" in fields(
        check(request(None, {**S3, "s3_bucket": ""}), config, env, list_s3=Lister())
    )
    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "denied"}}, "ListObjectsV2")
    report = check(request(None, S3), config, env, list_s3=Lister(error=denied))
    assert "settings.s3_bucket" in fields(report)
    assert any("AccessDenied" in e["message"] for e in report.errors)
    assert "settings.s3_prefix" in fields(check(request(None, S3), config, env, list_s3=Lister([])))
    # a stray host folder is ignored, not mounted
    report = check(request(docs, S3), config, env, list_s3=Lister())
    assert report.ok and report.host_folder == ""
    assert any("host_folder is not used" in w for w in report.warnings)


def test_local_source_requires_host_folder(config, env):
    assert "target.host_folder" in fields(check(request(None), config, env))


def test_s3_without_credentials_warns(config, env):
    settings = {k: v for k, v in S3.items() if not k.startswith("s3_access") and "secret" not in k}
    report = check(request(None, settings), config, env, list_s3=Lister())
    assert report.ok
    assert any("s3_access_key_id is not set" in w for w in report.warnings)


def test_s3_ca_bundle_is_mounted(tmp_path, config, env):
    bundle = tmp_path / "ca.pem"
    bundle.write_text("-----BEGIN CERTIFICATE-----")
    lister = Lister()
    report = check(request(None, {**S3, "s3_ca_bundle": str(bundle)}), config, env, list_s3=lister)
    assert report.ok, report.errors
    assert lister.seen[0]["s3_ca_bundle"] == str(bundle.resolve())
    assert report.file_mounts == {"/data/s3-ca-bundle.pem": str(bundle.resolve())}
    assert report.env["S3_CA_BUNDLE"] == "/data/s3-ca-bundle.pem"

    outside = check(request(None, {**S3, "s3_ca_bundle": "/etc/hosts"}), config, env, list_s3=Lister())
    assert "settings.s3_ca_bundle" in fields(outside)
    assert "settings.s3_ca_bundle" in fields(
        check(request(None, {**S3, "s3_ca_bundle": str(tmp_path)}), config, env, list_s3=Lister())
    )


class KubeExecutor(FakeExecutor):
    name = "kubernetes"
    capabilities = KubernetesExecutor.capabilities


REMOTE_S3 = {**S3, "s3_endpoint_url": "https://s3.example.com"}


def kube_request(settings=None, folder=None, **target):
    target = {"type": "kubernetes", "image": "quay.io/acme/docling-agent:1.0", **target}
    return RunRequest(
        settings=settings if settings is not None else REMOTE_S3,
        target=RunTarget(host_folder=str(folder) if folder else None, **target),
    )


@pytest.fixture
def kube_config(tmp_path):
    return SchedulerConfig(
        executor="kubernetes",
        allowed_mount_roots=[str(tmp_path)],
        db_path=tmp_path / "db",
        image_allowlist=["quay.io/acme/docling-agent"],
        mlflow_tracking_uri="https://mlflow.apps.example.com",
        container_mlflow_tracking_uri="http://mlflow.mlflow.svc:5000",
    )


def test_kubernetes_s3_run(kube_config, env):
    executor = KubeExecutor()
    executor.images = set()  # never consulted: the cluster pulls the image
    report = check(kube_request(shards=2), kube_config, env, executor, list_s3=Lister())
    assert report.ok, report.errors
    assert report.document_count == 3
    assert report.env["S3_ENDPOINT_URL"] == "https://s3.example.com"
    assert report.env["MLFLOW_TRACKING_URI"] == "http://mlflow.mlflow.svc:5000"
    assert report.env["SCHEDULER_EXECUTOR"] == "kubernetes"
    assert "FOLDER_PATH" not in report.env
    assert any("pulled by kubernetes" in w for w in report.warnings)
    assert any("must be able to reach them" in w for w in report.warnings)


def test_kubernetes_rejects_local_sources(kube_config, env, docs):
    report = check(kube_request({}, folder=docs), kube_config, env, KubeExecutor())
    assert "settings.source_backend" in fields(report)
    assert report.host_folder == "" and report.preview["mount"] is None


def test_kubernetes_rejects_loopback_endpoints(kube_config, env):
    settings = {**S3, "vector_store_backend": "chroma", "chroma_host": "localhost"}
    report = check(kube_request(settings), kube_config, env, KubeExecutor(), list_s3=Lister())
    assert {"settings.s3_endpoint_url", "settings.chroma_host"} <= fields(report)
    assert not any("rewritten" in w for w in report.warnings)


def test_kubernetes_mlflow_uri_must_be_reachable_from_the_cluster(kube_config, env, tmp_path):
    loopback = SchedulerConfig(
        executor="kubernetes", db_path=tmp_path / "db", image_allowlist=["quay.io/acme/docling-agent"],
        mlflow_tracking_uri="http://localhost:5000",
    )
    report = check(kube_request(), loopback, env, KubeExecutor(), list_s3=Lister())
    assert "mlflow" in fields(report)
    assert any("SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI" in e["message"] for e in report.errors)
    # a port-forwarded MLflow for the scheduler, a Service URL for the pods
    loopback.container_mlflow_tracking_uri = "http://mlflow.mlflow.svc:5000"
    assert check(kube_request(), loopback, env, KubeExecutor(), list_s3=Lister()).ok


def test_kubernetes_rejects_localhost_images(kube_config, env):
    kube_config.image_allowlist = ["localhost/docling-agent"]
    report = check(
        kube_request(image="localhost/docling-agent:latest"), kube_config, env, KubeExecutor(),
        list_s3=Lister(),
    )
    assert "target.image" in fields(report)


def test_podman_uses_container_mlflow_uri_with_rewrite(docs, config, env):
    config.container_mlflow_tracking_uri = "http://127.0.0.1:5001"
    report = check(request(docs), config, env)
    assert report.env["MLFLOW_TRACKING_URI"] == "http://host.containers.internal:5001"
