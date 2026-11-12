#!/usr/bin/env python
"""PodmanExecutor against an in-memory podman client."""

import pytest

from docling_scheduler.executors.base import ExecutorError
from docling_scheduler.executors.podman import PodmanExecutor
from docling_scheduler.models import RunSpec

from .fakes import FakePodmanClient


@pytest.fixture
def client():
    return FakePodmanClient()


@pytest.fixture
def executor(client):
    return PodmanExecutor(client_factory=lambda: client)


def spec(**overrides):
    values = dict(
        run_id="run1",
        image="localhost/docling-agent:latest",
        shard_count=2,
        shards=[0, 1],
        env={"FOLDER_PATH": "/data/in", "SHARD_COUNT": "2"},
        secret_env={"EMBEDDING_API_KEY": "sk-secret"},
        host_folder="/home/u/docs",
        cpus=1.5,
        memory="2g",
    )
    values.update(overrides)
    return RunSpec(**values)


def test_submit_creates_secret_backed_containers(executor, client):
    handle = executor.submit(spec())
    assert handle["run_id"] == "run1"
    assert len(handle["attempts"]["0"]) == 1 and len(handle["attempts"]["1"]) == 1
    (secret_name,) = client.secrets.items
    assert client.secrets.items[secret_name] == b"sk-secret"
    assert handle["secrets"] == [secret_name]

    container = client.containers.get(handle["attempts"]["1"][0])
    kwargs = container.kwargs
    assert container.status == "running"
    assert kwargs["environment"]["SHARD_INDEX"] == "1"
    assert "EMBEDDING_API_KEY" not in kwargs["environment"]
    assert kwargs["secret_env"] == {"EMBEDDING_API_KEY": secret_name}
    assert kwargs["mounts"] == [{"type": "bind", "source": "/home/u/docs", "target": "/data/in",
                                 "read_only": True, "relabel": "z"}]
    assert kwargs["labels"]["docling-agent/run-id"] == "run1"
    assert kwargs["labels"]["docling-agent/shard"] == "1"
    assert kwargs["cap_drop"] == ["ALL"]
    assert kwargs["security_opt"] == ["no-new-privileges"]
    assert kwargs["nano_cpus"] == 1_500_000_000
    assert kwargs["mem_limit"] == "2g"


def test_submit_pulls_missing_image(executor, client):
    executor.submit(spec(image="localhost/docling-agent:v2"))
    assert client.images.pulled == ["localhost/docling-agent:v2"]


def test_failed_submit_rolls_back(executor, client):
    client.fail_start = True
    with pytest.raises(ExecutorError):
        executor.submit(spec())
    assert client.secrets.items == {}
    assert client.containers.items == {}


def test_retry_appends_attempt(executor, client):
    handle = executor.submit(spec())
    client.containers.get(handle["attempts"]["1"][0]).status = "exited"
    handle = executor.submit(spec(shards=[1]), handle=handle)
    assert len(handle["attempts"]["0"]) == 1
    assert len(handle["attempts"]["1"]) == 2
    assert client.containers.get(handle["attempts"]["1"][1]).kwargs["labels"]["docling-agent/attempt"] == "1"


def test_status_maps_container_states(executor, client):
    handle = executor.submit(spec())
    c0 = client.containers.get(handle["attempts"]["0"][0])
    c0.status, c0.exit_code = "exited", 137
    client.containers.get(handle["attempts"]["1"][0]).remove()
    status = executor.status(handle)
    by_shard = status.latest_by_shard()
    assert by_shard[0].state == "exited" and by_shard[0].exit_code == 137 and by_shard[0].oom_killed
    assert by_shard[1].state == "missing"


def test_logs_split_lines_across_chunks(executor, client):
    handle = executor.submit(spec())
    container = client.containers.get(handle["attempts"]["0"][0])
    container.log_chunks = [b'{"msg": "a"}\n{"ms', b'g": "b"}\n', b"tail"]
    lines = list(executor.logs(handle, shard=0, follow=False, tail=10))
    assert lines == ['{"msg": "a"}', '{"msg": "b"}', "tail"]
    assert client.log_kwargs["tail"] == 10 and client.log_kwargs["follow"] is False


def test_logs_unknown_shard(executor):
    handle = executor.submit(spec())
    with pytest.raises(ExecutorError):
        list(executor.logs(handle, shard=5))


def test_telemetry_and_events(executor, client):
    handle = executor.submit(spec())
    samples = executor.telemetry(handle)
    assert {s.shard_index for s in samples} == {0, 1}
    assert samples[0].cpu_percent == 12.5 and samples[0].memory_limit_bytes == 4096

    client.events.events = [{
        "Type": "container", "Action": "died", "timeNano": 2_000_000_000,
        "Actor": {"Attributes": {"name": "docling-agent-run1-s0-a0",
                                 "docling-agent/shard": "0", "containerExitCode": "137"}},
    }]
    (event,) = executor.events(handle, since=1.0)
    assert event.action == "died" and event.shard_index == 0 and event.timestamp == 2.0
    assert "exit=137" in event.message
    assert client.events.calls[0]["filters"] == {"label": ["docling-agent/run-id=run1"]}


def test_cancel_and_cleanup(executor, client):
    handle = executor.submit(spec())
    executor.cancel(handle)
    assert all(c.status == "exited" for c in client.containers.items.values())
    executor.cleanup(handle)
    assert client.secrets.items == {} and handle["secrets"] == []
    assert len(client.containers.items) == 2  # kept for logs
    executor.cleanup(handle, remove_containers=True)
    assert client.containers.items == {}


def test_available(executor):
    assert executor.available() == {"available": True, "version": "5.0.0"}
    broken = PodmanExecutor(client_factory=lambda: (_ for _ in ()).throw(OSError("no socket")))
    result = broken.available()
    assert result["available"] is False and "no socket" in result["error"]


def test_s3_source_mounts_only_files(executor, client):
    handle = executor.submit(
        spec(host_folder=None, file_mounts={"/data/s3-ca-bundle.pem": "/home/u/ca.pem"})
    )
    kwargs = client.containers.get(handle["attempts"]["0"][0]).kwargs
    assert kwargs["mounts"] == [{"type": "bind", "source": "/home/u/ca.pem",
                                 "target": "/data/s3-ca-bundle.pem", "read_only": True,
                                 "relabel": "z"}]
