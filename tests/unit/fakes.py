#!/usr/bin/env python
"""In-memory stand-ins for the podman-py client and executors."""

import itertools

from docling_scheduler.executors.podman import PodmanExecutor
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
    capabilities = PodmanExecutor.capabilities

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


class FakeApiError(Exception):
    """Stands in for ``kubernetes.client.ApiException`` (only ``status`` is read)."""

    def __init__(self, status, reason=""):
        super().__init__(f"({status}) {reason}")
        self.status = status


def _matches(obj, label_selector):
    key, _, value = (label_selector or "").partition("=")
    return not key or (obj["metadata"].get("labels") or {}).get(key) == value


class FakeLogResponse:
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False
        self.released = False

    def stream(self, amt=None):
        yield from self.chunks

    def close(self):
        self.closed = True

    def release_conn(self):
        self.released = True


class FakeKubeCore:
    def __init__(self, cluster):
        self.cluster = cluster

    def create_namespaced_config_map(self, namespace, body):
        return self.cluster.create("configmaps", namespace, body)

    def read_namespaced_config_map(self, name, namespace):
        return self.cluster.get("configmaps", name)

    def delete_namespaced_config_map(self, name, namespace, propagation_policy=None):
        self.cluster.delete("configmaps", name)

    def create_namespaced_secret(self, namespace, body):
        return self.cluster.create("secrets", namespace, body)

    def delete_namespaced_secret(self, name, namespace):
        self.cluster.delete("secrets", name)

    def list_namespaced_pod(self, namespace, label_selector=None):
        self.cluster.check_up()
        pods = self.cluster.objects["pods"].values()
        return {"items": [p for p in pods if _matches(p, label_selector)]}

    def read_namespaced_pod_log(self, name, namespace, **kwargs):
        self.cluster.log_calls.append((name, kwargs))
        response = FakeLogResponse(self.cluster.log_chunks)
        self.cluster.log_responses.append(response)
        return response

    def list_namespaced_event(self, namespace):
        return {"items": self.cluster.events}


class FakeKubeBatch:
    def __init__(self, cluster):
        self.cluster = cluster

    def create_namespaced_job(self, namespace, body):
        if self.cluster.fail_job_create_after is not None:
            if self.cluster.fail_job_create_after == 0:
                raise FakeApiError(422, "invalid job")
            self.cluster.fail_job_create_after -= 1
        return self.cluster.create("jobs", namespace, body)

    def list_namespaced_job(self, namespace, label_selector=None):
        self.cluster.check_up()
        jobs = self.cluster.objects["jobs"].values()
        return {"items": [j for j in jobs if _matches(j, label_selector)]}

    def patch_namespaced_job(self, name, namespace, body):
        job = self.cluster.get("jobs", name)
        annotations = body.get("metadata", {}).get("annotations", {})
        job["metadata"].setdefault("annotations", {}).update(annotations)
        job["spec"].update(body.get("spec", {}))
        if job["spec"].get("suspend"):
            # the Job controller deletes the active pods of a suspended Job
            self.cluster.delete_pods_of(name)
        self.cluster.patches.append((name, body))

    def delete_namespaced_job(self, name, namespace, propagation_policy=None):
        self.cluster.delete("jobs", name)


class FakeKubeCustom:
    def __init__(self, cluster):
        self.cluster = cluster

    def list_namespaced_custom_object(self, group, version, namespace, plural, label_selector=None):
        if self.cluster.metrics is None:
            raise FakeApiError(404, "metrics.k8s.io not served")
        return {"items": self.cluster.metrics}


class FakeKubeVersion:
    def get_code(self):
        return {"gitVersion": "v1.31.2"}


class FakeKubeAuthz:
    def __init__(self, cluster):
        self.cluster = cluster

    def create_self_subject_access_review(self, body):
        attrs = body["spec"]["resourceAttributes"]
        resource = attrs["resource"] + (f"/{attrs['subresource']}" if attrs["subresource"] else "")
        return {"status": {"allowed": (attrs["verb"], resource) not in self.cluster.denied}}


class FakeKubeCluster:
    """In-memory namespace: objects keyed by kind and name, with owner
    garbage collection and a hand-driven 'Job controller' (``add_pod``)."""

    def __init__(self):
        self.objects = {"configmaps": {}, "secrets": {}, "jobs": {}, "pods": {}}
        self.namespaces: list[str] = []
        self.denied: set[tuple[str, str]] = set()
        self.fail_job_create_after: int | None = None
        self.down = False
        self.metrics: list[dict] | None = None
        self.events: list[dict] = []
        self.log_chunks: list[bytes] = []
        self.log_calls: list = []
        self.log_responses: list[FakeLogResponse] = []
        self.patches: list = []
        self._uids = itertools.count(1)

    def apis(self):
        from docling_scheduler.executors.kubernetes import KubeApis

        return KubeApis(
            client=None,
            core=FakeKubeCore(self),
            batch=FakeKubeBatch(self),
            custom=FakeKubeCustom(self),
            version=FakeKubeVersion(),
            authz=FakeKubeAuthz(self),
        )

    def check_up(self):
        if self.down:
            raise FakeApiError(503, "unavailable")

    def create(self, kind, namespace, body):
        name = body["metadata"]["name"]
        if name in self.objects[kind]:
            raise FakeApiError(409, "already exists")
        self.namespaces.append(namespace)
        obj = {**body, "metadata": {**body["metadata"], "uid": f"uid-{next(self._uids)}"}}
        self.objects[kind][name] = obj
        return obj

    def get(self, kind, name):
        if name not in self.objects[kind]:
            raise FakeApiError(404, "not found")
        return self.objects[kind][name]

    def delete(self, kind, name):
        obj = self.objects[kind].pop(name, None)
        if obj is None:
            raise FakeApiError(404, "not found")
        if kind == "jobs":
            self.delete_pods_of(name)
        uid = obj["metadata"]["uid"]
        for dependent_kind, items in self.objects.items():
            for dependent in list(items.values()):
                owners = dependent["metadata"].get("ownerReferences") or []
                if any(o["uid"] == uid for o in owners):
                    self.delete(dependent_kind, dependent["metadata"]["name"])

    def delete_pods_of(self, job_name):
        for name, pod in list(self.objects["pods"].items()):
            if pod["metadata"]["labels"].get("batch.kubernetes.io/job-name") == job_name:
                del self.objects["pods"][name]

    def add_pod(self, job_name, state=None, phase="Running", conditions=None, created="2026-01-01T00:00:00Z", **status):
        """Create the Job's pod with an ``agent`` container in ``state``."""
        job = self.objects["jobs"][job_name]
        template = job["spec"]["template"]
        name = f"{job_name}-{len(self.objects['pods'])}x"
        pod = {
            "metadata": {
                "name": name,
                "creationTimestamp": created,
                "labels": {**template["metadata"]["labels"], "batch.kubernetes.io/job-name": job_name},
            },
            "spec": template["spec"],
            "status": {
                "phase": phase,
                "conditions": conditions or [],
                "containerStatuses": [{"name": "agent", "state": state}] if state else [],
                **status,
            },
        }
        self.objects["pods"][name] = pod
        return pod
