#!/usr/bin/env python
"""In-memory stand-ins for the podman-py client and executors."""

import itertools

from docling_scheduler.models import RuntimeStatus, ShardRuntime

_ids = itertools.count(1)


class FakeContainer:
    def __init__(self, client, image, **kwargs):
        self.client = client
        self.id = f"cid{next(_ids)}"
        self.name = kwargs.get("name")
        self.image = image
        self.kwargs = kwargs
        self.labels = kwargs.get("labels", {})
        self.status = "created"
        self.exit_code = 0
        self.removed = False
        self.log_chunks: list[bytes] = []
        self.stats_payload = {"Stats": [{"CPU": 12.5, "MemUsage": 1024, "MemLimit": 4096}]}

    @property
    def attrs(self):
        return {"State": {"Status": self.status, "ExitCode": self.exit_code,
                          "OOMKilled": self.exit_code == 137, "StartedAt": "t0",
                          "FinishedAt": "t1"}}

    def reload(self):
        pass

    def start(self):
        if self.client.fail_start:
            raise RuntimeError("start failed")
        self.status = "running"

    def stop(self, timeout=None):
        self.status = "exited"
        self.exit_code = 143

    def remove(self, force=False):
        self.removed = True
        self.client.containers.items.pop(self.id, None)

    def logs(self, **kwargs):
        self.client.log_kwargs = kwargs
        return iter(self.log_chunks)

    def stats(self, **kwargs):
        return self.stats_payload


class FakeContainers:
    def __init__(self, client):
        self.client = client
        self.items: dict[str, FakeContainer] = {}

    def create(self, image, **kwargs):
        container = FakeContainer(self.client, image, **kwargs)
        self.items[container.id] = container
        return container

    def get(self, container_id):
        from podman.errors import NotFound

        if container_id not in self.items:
            raise NotFound("gone")
        return self.items[container_id]


class FakeSecrets:
    def __init__(self):
        self.items: dict[str, bytes] = {}

    def create(self, name, data):
        self.items[name] = data

    def remove(self, name):
        self.items.pop(name)


class FakeImages:
    def __init__(self):
        self.present = {"localhost/docling-agent:latest"}
        self.pulled: list[str] = []

    def exists(self, image):
        return image in self.present

    def pull(self, image):
        self.pulled.append(image)
        self.present.add(image)


class FakeEvents:
    def __init__(self):
        self.events: list[dict] = []
        self.calls: list[dict] = []

    def list(self, **kwargs):
        self.calls.append(kwargs)
        return iter(self.events)


class FakePodmanClient:
    def __init__(self):
        self.containers = FakeContainers(self)
        self.secrets = FakeSecrets()
        self.images = FakeImages()
        self.events = FakeEvents()
        self.fail_start = False
        self.log_kwargs = None

    def ping(self):
        return True

    def version(self):
        return {"Version": "5.0.0"}


class FakeExecutor:
    """Executor double: shards are 'containers' whose state tests set directly."""

    name = "podman"

    def __init__(self):
        self.available_result = {"available": True}
        self.images = {"localhost/docling-agent:latest"}
        self.submitted = []
        self.states: dict[tuple[int, int], tuple[str, int | None]] = {}
        self.log_lines: list[str] = []
        self.cancelled = []
        self.cleaned = []

    def available(self):
        return self.available_result

    def image_exists(self, image):
        return image in self.images

    def submit(self, run, handle=None):
        self.submitted.append(run)
        handle = {"run_id": run.run_id, "attempts": {k: list(v) for k, v in (handle or {}).get("attempts", {}).items()}, "secrets": ["s"]}
        for shard in run.shards:
            attempts = handle["attempts"].setdefault(str(shard), [])
            self.states[(shard, len(attempts))] = ("running", None)
            attempts.append(f"c{shard}-{len(attempts)}")
        return handle

    def status(self, handle):
        shards = []
        for key, ids in handle.get("attempts", {}).items():
            for attempt, cid in enumerate(ids):
                state, code = self.states[(int(key), attempt)]
                shards.append(ShardRuntime(int(key), attempt, cid, cid, state, code))
        return RuntimeStatus(shards)

    def finish(self, shard, exit_code, attempt=None):
        if attempt is None:
            attempt = max(a for s, a in self.states if s == shard)
        self.states[(shard, attempt)] = ("exited", exit_code)

    def logs(self, handle, shard=0, follow=True, tail=None):
        return iter(self.log_lines)

    def telemetry(self, handle):
        return []

    def events(self, handle, since):
        return []

    def cancel(self, handle):
        self.cancelled.append(handle["run_id"])
        for (shard, attempt), (state, _) in list(self.states.items()):
            if state == "running":
                self.states[(shard, attempt)] = ("exited", 143)

    def cleanup(self, handle, remove_containers=False):
        self.cleaned.append((handle["run_id"], remove_containers))
        handle["secrets"] = []


class FakeS3Client:
    """boto3 S3 client double: ``list_objects_v2`` over a fixed key set."""

    def __init__(self, keys, page_size=2):
        self.keys = sorted(keys)
        self.page_size = page_size
        self.calls: list[dict] = []

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix="", Delimiter=None):
        self.calls.append({"Bucket": Bucket, "Prefix": Prefix, "Delimiter": Delimiter})
        keys = [k for k in self.keys if k.startswith(Prefix)]
        if Delimiter:
            keys = [k for k in keys if Delimiter not in k[len(Prefix):]]
        for i in range(0, len(keys), self.page_size):
            yield {"Contents": [{"Key": k} for k in keys[i:i + self.page_size]]}
