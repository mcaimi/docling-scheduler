#!/usr/bin/env python
"""KubernetesExecutor against an in-memory cluster."""

import pytest

from docling_scheduler.executors.base import ExecutorError
from docling_scheduler.executors.kubernetes import (
    ANNOTATION_STOP_REASON,
    KubernetesExecutor,
    memory_quantity,
    parse_quantity,
)
from docling_scheduler.models import RunSpec

from .fakes import FakeKubeCluster


@pytest.fixture
def cluster():
    return FakeKubeCluster()


@pytest.fixture
def executor(cluster):
    return KubernetesExecutor(
        namespace="docling", image_pull_secret="registry-creds", active_deadline=3600,
        api_factory=cluster.apis,
    )


def spec(**overrides):
    values = dict(
        run_id="run1",
        image="quay.io/acme/docling-agent:1.0",
        shard_count=2,
        shards=[0, 1],
        env={"SOURCE_BACKEND": "s3", "SHARD_COUNT": "2"},
        secret_env={"EMBEDDING_API_KEY": "sk-secret", "S3_SECRET_ACCESS_KEY": "s3-secret"},
        cpus=1.5,
        memory="2g",
    )
    values.update(overrides)
    return RunSpec(**values)


def job_of(cluster, handle, shard, attempt=-1):
    return cluster.objects["jobs"][handle["attempts"][str(shard)][attempt]]


def container_of(job):
    return job["spec"]["template"]["spec"]["containers"][0]


RUNNING = {"running": {"startedAt": "2026-01-01T00:00:05Z"}}


def terminated(code, reason="Error", **extra):
    return {"terminated": {"exitCode": code, "reason": reason, "startedAt": "t0", "finishedAt": "t1", **extra}}


def test_submit_creates_jobs_owned_by_the_run_anchor(executor, cluster):
    handle = executor.submit(spec())
    assert set(cluster.namespaces) == {"docling"}
    anchor = cluster.objects["configmaps"]["docling-run-run1"]
    assert handle["anchor"] == {"name": "docling-run-run1", "uid": anchor["metadata"]["uid"]}
    assert handle["attempts"] == {"0": ["docling-agent-run1-s0-a0"], "1": ["docling-agent-run1-s1-a0"]}

    (secret_name,) = handle["secrets"]
    secret = cluster.objects["secrets"][secret_name]
    assert secret["stringData"] == {"EMBEDDING_API_KEY": "sk-secret", "S3_SECRET_ACCESS_KEY": "s3-secret"}

    owner = {"apiVersion": "v1", "kind": "ConfigMap", "name": "docling-run-run1", "uid": anchor["metadata"]["uid"]}
    job = job_of(cluster, handle, 1)
    assert job["metadata"]["ownerReferences"] == [owner]
    assert secret["metadata"]["ownerReferences"] == [owner]
    labels = job["spec"]["template"]["metadata"]["labels"]
    assert labels["docling-agent/run-id"] == "run1"
    assert labels["docling-agent/shard"] == "1"
    assert labels["docling-agent/attempt"] == "0"
    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == 3600

    pod_spec = job["spec"]["template"]["spec"]
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["securityContext"] == {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}}
    assert pod_spec["imagePullSecrets"] == [{"name": "registry-creds"}]
    container = container_of(job)
    assert container["image"] == "quay.io/acme/docling-agent:1.0"
    assert container["securityContext"] == {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}}
    assert container["resources"] == {
        "requests": {"cpu": "1500m", "memory": "2Gi"},
        "limits": {"cpu": "1500m", "memory": "2Gi"},
    }
    env = {e["name"]: e for e in container["env"]}
    assert env["SHARD_INDEX"] == {"name": "SHARD_INDEX", "value": "1"}
    assert env["SOURCE_BACKEND"]["value"] == "s3"
    # secrets only by reference: never as values in the Job
    assert env["EMBEDDING_API_KEY"] == {
        "name": "EMBEDDING_API_KEY",
        "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "EMBEDDING_API_KEY"}},
    }
    assert "sk-secret" not in str(job) and "s3-secret" not in str(job)


def test_file_mounts_are_copied_into_a_configmap(executor, cluster, tmp_path):
    bundle = tmp_path / "ca.pem"
    bundle.write_text("-----BEGIN CERTIFICATE-----")
    handle = executor.submit(spec(file_mounts={"/data/s3-ca-bundle.pem": str(bundle)}))
    files = [c for name, c in cluster.objects["configmaps"].items() if "-files-" in name]
    assert len(files) == 1
    assert files[0]["data"] == {"file-0": "-----BEGIN CERTIFICATE-----"}
    pod_spec = job_of(cluster, handle, 0)["spec"]["template"]["spec"]
    assert {"name": "files", "configMap": {"name": files[0]["metadata"]["name"]}} in pod_spec["volumes"]
    assert {"name": "files", "mountPath": "/data/s3-ca-bundle.pem", "subPath": "file-0",
            "readOnly": True} in container_of(job_of(cluster, handle, 0))["volumeMounts"]


def test_failed_submit_rolls_back(executor, cluster):
    cluster.fail_job_create_after = 1
    with pytest.raises(ExecutorError):
        executor.submit(spec())
    assert all(not items for items in cluster.objects.values())


def test_retry_appends_attempts_and_recreates_a_missing_anchor(executor, cluster):
    handle = executor.submit(spec())
    executor.cleanup(handle)
    retried = executor.submit(spec(shards=[1]), handle=handle)
    assert retried["attempts"]["1"] == ["docling-agent-run1-s1-a0", "docling-agent-run1-s1-a1"]
    assert retried["anchor"] == handle["anchor"]
    assert len(retried["secrets"]) == 1

    # the anchor was deleted out of band: jobs must not hang off a dead owner
    del cluster.objects["configmaps"]["docling-run-run1"]
    again = executor.submit(spec(shards=[1]), handle=retried)
    anchor_uid = cluster.objects["configmaps"]["docling-run-run1"]["metadata"]["uid"]
    assert again["anchor"]["uid"] == anchor_uid != handle["anchor"]["uid"]
    assert job_of(cluster, again, 1)["metadata"]["ownerReferences"][0]["uid"] == anchor_uid


def test_status_maps_pod_states(executor, cluster):
    handle = executor.submit(spec(shard_count=1, shards=[0]))
    job_name = handle["attempts"]["0"][0]

    def shard():
        (runtime,) = executor.status(handle).shards
        return runtime

    assert shard().state == "created"
    cluster.add_pod(job_name, phase="Pending", conditions=[
        {"type": "PodScheduled", "status": "False", "reason": "Unschedulable", "message": "0/3 nodes"}])
    assert (shard().state, shard().error) == ("created", "Unschedulable: 0/3 nodes")

    cluster.objects["pods"].clear()
    cluster.add_pod(job_name, {"waiting": {"reason": "ContainerCreating"}}, phase="Pending")
    assert (shard().state, shard().error) == ("created", None)

    cluster.objects["pods"].clear()
    cluster.add_pod(job_name, {"waiting": {"reason": "ImagePullBackOff", "message": "not found"}}, phase="Pending")
    runtime = shard()
    assert runtime.terminal and runtime.exit_code is None
    assert runtime.error == "ImagePullBackOff: not found"

    cluster.objects["pods"].clear()
    pod = cluster.add_pod(job_name, RUNNING)
    runtime = shard()
    assert (runtime.state, runtime.container_id, runtime.name) == ("running", job_name, pod["metadata"]["name"])
    assert runtime.started_at == "2026-01-01T00:00:05Z"

    cluster.objects["pods"].clear()
    cluster.add_pod(job_name, terminated(0, "Completed"), phase="Succeeded")
    runtime = shard()
    assert (runtime.state, runtime.exit_code, runtime.error) == ("exited", 0, None)
    assert runtime.finished_at == "t1"

    cluster.objects["pods"].clear()
    cluster.add_pod(job_name, terminated(137, "OOMKilled"), phase="Failed")
    runtime = shard()
    assert runtime.exit_code == 137 and runtime.oom_killed

    cluster.objects["pods"].clear()
    cluster.add_pod(job_name, phase="Failed", reason="Evicted", message="low on memory")
    runtime = shard()
    assert runtime.terminal and runtime.error == "Evicted: low on memory"

    # pods removed by the Job controller after a deadline
    cluster.objects["pods"].clear()
    cluster.objects["jobs"][job_name]["status"] = {"conditions": [
        {"type": "Failed", "status": "True", "reason": "DeadlineExceeded", "message": "too long"}]}
    assert (shard().state, shard().error) == ("exited", "DeadlineExceeded: too long")

    del cluster.objects["jobs"][job_name]
    assert shard().state == "missing"


def test_status_uses_the_newest_pod_and_reports_api_errors(executor, cluster):
    handle = executor.submit(spec(shard_count=1, shards=[0]))
    job_name = handle["attempts"]["0"][0]
    cluster.add_pod(job_name, terminated(1), created="2026-01-01T00:00:00Z")
    cluster.add_pod(job_name, RUNNING, created="2026-01-01T00:05:00Z")
    assert executor.status(handle).shards[0].state == "running"
    cluster.down = True
    status = executor.status(handle)
    assert status.error and "unavailable" in status.error


def test_cancel_suspends_unfinished_jobs(executor, cluster):
    handle = executor.submit(spec())
    cluster.add_pod(handle["attempts"]["0"][0], RUNNING)
    cluster.add_pod(handle["attempts"]["1"][0], terminated(0, "Completed"), phase="Succeeded")
    cluster.objects["jobs"][handle["attempts"]["1"][0]]["status"] = {
        "conditions": [{"type": "Complete", "status": "True"}]}

    executor.cancel(handle)
    assert [name for name, _ in cluster.patches] == [handle["attempts"]["0"][0]]
    job = job_of(cluster, handle, 0)
    assert job["spec"]["suspend"] is True
    assert job["metadata"]["annotations"][ANNOTATION_STOP_REASON] == "cancelled"
    runtime = executor.status(handle).latest_by_shard()
    assert (runtime[0].state, runtime[0].error) == ("exited", "cancelled")
    assert (runtime[1].state, runtime[1].exit_code) == ("exited", 0)


def test_cleanup_removes_secrets_and_stops_stuck_pods(executor, cluster):
    handle = executor.submit(spec())
    stuck = handle["attempts"]["0"][0]
    cluster.add_pod(stuck, {"waiting": {"reason": "ErrImagePull", "message": "denied"}}, phase="Pending")
    cluster.add_pod(handle["attempts"]["1"][0], terminated(1), phase="Failed")
    cluster.objects["jobs"][handle["attempts"]["1"][0]]["status"] = {
        "conditions": [{"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}]}

    executor.cleanup(handle)
    assert handle["secrets"] == [] and cluster.objects["secrets"] == {}
    # the stuck shard keeps its failure reason after its pod is gone
    assert [name for name, _ in cluster.patches] == [stuck]
    runtime = executor.status(handle).latest_by_shard()
    assert (runtime[0].state, runtime[0].error) == ("exited", "ErrImagePull: denied")
    assert runtime[1].exit_code == 1  # finished pods (and their logs) are kept
    assert len(cluster.objects["jobs"]) == 2


def test_cleanup_with_containers_deletes_the_whole_run(executor, cluster, tmp_path):
    bundle = tmp_path / "ca.pem"
    bundle.write_text("pem")
    handle = executor.submit(spec(file_mounts={"/data/s3-ca-bundle.pem": str(bundle)}))
    cluster.add_pod(handle["attempts"]["0"][0], terminated(0, "Completed"), phase="Succeeded")
    executor.cleanup(handle, remove_containers=True)
    assert all(not items for items in cluster.objects.values())


def test_cleanup_without_anchor_deletes_jobs(executor, cluster):
    handle = executor.submit(spec())
    del cluster.objects["configmaps"]["docling-run-run1"]
    executor.cleanup(handle, remove_containers=True)
    assert cluster.objects["jobs"] == {}


def test_logs_stream_lines_and_close_the_response(executor, cluster):
    handle = executor.submit(spec())
    pod = cluster.add_pod(handle["attempts"]["1"][0], RUNNING)
    cluster.log_chunks = [b'{"level": "INFO"', b', "msg": "a"}\npartial', b" line\n", b"tail"]
    lines = list(executor.logs(handle, shard=1, follow=True, tail=50))
    assert lines == ['{"level": "INFO", "msg": "a"}', "partial line", "tail"]
    name, kwargs = cluster.log_calls[0]
    assert name == pod["metadata"]["name"]
    assert kwargs == {"container": "agent", "follow": True, "_preload_content": False, "tail_lines": 50}
    assert cluster.log_responses[0].closed and cluster.log_responses[0].released

    # a client that disconnects early still releases the stream
    cluster.log_chunks = [b"one\ntwo\n"]
    stream = executor.logs(handle, shard=1)
    assert next(stream) == "one"
    stream.close()
    assert cluster.log_responses[1].closed


def test_logs_errors(executor, cluster):
    handle = executor.submit(spec())
    with pytest.raises(ExecutorError, match="no pod"):
        list(executor.logs(handle, shard=0))
    with pytest.raises(ExecutorError, match="no job"):
        list(executor.logs(handle, shard=5))


def test_telemetry_from_pod_metrics(executor, cluster):
    handle = executor.submit(spec())
    pod = cluster.add_pod(handle["attempts"]["0"][0], RUNNING)
    assert executor.telemetry(handle) == []  # no metrics-server
    cluster.metrics = [{
        "metadata": {"name": pod["metadata"]["name"]},
        "containers": [{"name": "agent", "usage": {"cpu": "250000000n", "memory": "512Mi"}}],
    }]
    (sample,) = executor.telemetry(handle)
    assert sample.shard_index == 0
    assert sample.cpu_percent == pytest.approx(25.0)
    assert sample.memory_bytes == 512 * 2**20
    assert sample.memory_limit_bytes == 2 * 2**30


def test_events_are_matched_to_shards(executor, cluster):
    handle = executor.submit(spec())
    job0 = handle["attempts"]["0"][0]
    cluster.events = [
        {"involvedObject": {"kind": "Pod", "name": f"{job0}-abcde"}, "reason": "Pulled",
         "message": "image pulled", "lastTimestamp": "2026-01-01T00:00:10Z"},
        {"involvedObject": {"kind": "Job", "name": job0}, "reason": "SuccessfulCreate",
         "message": "created pod", "lastTimestamp": "2026-01-01T00:00:01Z"},
        {"involvedObject": {"kind": "Pod", "name": "someone-else-xyz"}, "reason": "Pulled",
         "lastTimestamp": "2026-01-01T00:00:02Z"},
        {"involvedObject": {"kind": "Job", "name": job0}, "reason": "Old",
         "lastTimestamp": "2020-01-01T00:00:00Z"},
    ]
    events = executor.events(handle, since=1735689600)  # 2025-01-01
    assert [(e.action, e.shard_index, e.type) for e in events] == [
        ("SuccessfulCreate", 0, "Job"), ("Pulled", 0, "Pod")]
    assert events[1].message == f"{job0}-abcde: image pulled"


def test_available_checks_permissions(executor, cluster):
    assert executor.available() == {"available": True, "version": "v1.31.2", "namespace": "docling"}
    cluster.denied = {("create", "jobs"), ("get", "pods/log")}
    result = executor.available()
    assert not result["available"]
    assert "create jobs" in result["error"] and "get pods/log" in result["error"]


def test_capabilities_and_images(executor):
    assert executor.capabilities.host_gateway is None
    assert not executor.capabilities.local_source
    assert not executor.capabilities.local_images
    assert executor.image_exists("quay.io/acme/docling-agent:1.0")


@pytest.mark.parametrize(
    "value, expected",
    [("250m", 0.25), ("1.5", 1.5), ("512Mi", 512 * 2**20), ("1e3", 1000.0), ("2G", 2e9),
     ("100n", 1e-7), ("bogus", None), (None, None), ("5Q", None)],
)
def test_parse_quantity(value, expected):
    assert parse_quantity(value) == (pytest.approx(expected) if expected is not None else None)


@pytest.mark.parametrize(
    "value, expected", [("512m", "512Mi"), ("4g", "4Gi"), ("1.5G", "1.5Gi"), ("100b", "100"), ("2048", "2048")]
)
def test_memory_quantity(value, expected):
    assert memory_quantity(value) == expected
