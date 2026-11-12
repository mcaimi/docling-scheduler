#!/usr/bin/env python
"""Podman executor: one container per shard, via the Podman REST API.

Uses ``podman-py`` against the user's socket (no shelling out, so no
argument-injection surface). Sensitive settings become per-run Podman secrets
exposed as environment variables, removed on cleanup.
"""

import logging
import time
import uuid
from typing import Any, Callable, Iterator

from ..models import ResourceSample, RunSpec, RuntimeEvent, RuntimeStatus, ShardRuntime
from ..settings_io import CONTAINER_DATA_DIR
from .base import (
    LABEL_ATTEMPT,
    LABEL_MANAGED,
    LABEL_RUN_ID,
    LABEL_SHARD,
    ExecutorError,
    RunHandle,
)

logger = logging.getLogger(__name__)


def _default_client_factory(base_url: str | None) -> Callable[[], Any]:
    def factory():
        from podman import PodmanClient

        return PodmanClient(base_url=base_url) if base_url else PodmanClient()

    return factory


def _parse_size(value: Any) -> int | None:
    """Podman stats sizes are ints (bytes) in libpod responses."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class PodmanExecutor:
    name = "podman"

    def __init__(
        self,
        base_url: str | None = None,
        relabel: str = "z",
        client_factory: Callable[[], Any] | None = None,
    ):
        self._client_factory = client_factory or _default_client_factory(base_url)
        self._relabel = relabel
        self._client: Any = None

    @property
    def client(self) -> Any:
        if self._client is None:
            self._client = self._client_factory()
        return self._client

    # --- discovery -------------------------------------------------------

    def available(self) -> dict[str, Any]:
        try:
            client = self.client
            if not client.ping():
                return {"available": False, "error": "podman service did not answer ping"}
            version = client.version()
            return {"available": True, "version": version.get("Version")}
        except Exception as e:
            self._client = None
            return {"available": False, "error": str(e)}

    def image_exists(self, image: str) -> bool:
        try:
            return bool(self.client.images.exists(image))
        except Exception as e:
            raise ExecutorError(f"cannot query images: {e}") from e

    # --- lifecycle -------------------------------------------------------

    def submit(self, run: RunSpec, handle: RunHandle | None = None) -> RunHandle:
        handle = {
            "run_id": run.run_id,
            "secrets": list((handle or {}).get("secrets", [])),
            "attempts": {k: list(v) for k, v in (handle or {}).get("attempts", {}).items()},
        }
        client = self.client
        try:
            if not client.images.exists(run.image):
                logger.info("Pulling image %s", run.image)
                client.images.pull(run.image)
        except Exception as e:
            raise ExecutorError(f"image {run.image} unavailable: {e}") from e

        # one secret set per submission (secrets are removed when a run ends,
        # so a retry creates fresh ones)
        token = uuid.uuid4().hex[:8]
        secret_env: dict[str, str] = {}
        try:
            for env_name, value in run.secret_env.items():
                secret_name = f"docling-agent-{run.run_id}-{token}-{env_name.lower()}"
                client.secrets.create(secret_name, value.encode())
                handle["secrets"].append(secret_name)
                secret_env[env_name] = secret_name

            for shard in run.shards:
                attempts = handle["attempts"].setdefault(str(shard), [])
                attempt = len(attempts)
                container = client.containers.create(
                    run.image,
                    name=f"docling-agent-{run.run_id}-s{shard}-a{attempt}",
                    environment={**run.env, "SHARD_INDEX": str(shard)},
                    secret_env=secret_env,
                    labels={
                        LABEL_RUN_ID: run.run_id,
                        LABEL_SHARD: str(shard),
                        LABEL_ATTEMPT: str(attempt),
                        LABEL_MANAGED: "true",
                    },
                    mounts=self._mounts(run),
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges"],
                    **self._resources(run),
                )
                attempts.append(container.id)
                container.start()
        except Exception as e:
            # leave nothing half-created behind: stop/remove what this call made
            self._rollback(handle, run.shards, token)
            raise ExecutorError(f"failed to start containers: {e}") from e
        return handle

    def _mounts(self, run: RunSpec) -> list[dict[str, Any]]:
        """Read-only binds: the document folder (local source only) and files."""
        binds = [(run.host_folder, CONTAINER_DATA_DIR)] if run.host_folder else []
        binds += [(source, target) for target, source in sorted(run.file_mounts.items())]
        return [self._bind(source, target) for source, target in binds]

    def _bind(self, source: str, target: str) -> dict[str, Any]:
        mount: dict[str, Any] = {
            "type": "bind",
            "source": source,
            "target": target,
            "read_only": True,
        }
        if self._relabel:
            mount["relabel"] = self._relabel
        return mount

    @staticmethod
    def _resources(run: RunSpec) -> dict[str, Any]:
        resources: dict[str, Any] = {}
        if run.cpus:
            resources["nano_cpus"] = int(run.cpus * 1e9)
        if run.memory:
            resources["mem_limit"] = run.memory
        return resources

    def _rollback(self, handle: RunHandle, shards: list[int], token: str) -> None:
        for name in [s for s in handle["secrets"] if f"-{token}-" in s]:
            try:
                self.client.secrets.remove(name)
            except Exception:
                pass
            handle["secrets"].remove(name)
        for shard in shards:
            attempts = handle["attempts"].get(str(shard), [])
            container_id = attempts[-1] if attempts else None
            if container_id is None:
                continue
            try:
                self.client.containers.get(container_id).remove(force=True)
            except Exception:
                pass

    # --- observation -----------------------------------------------------

    def _container(self, container_id: str) -> Any | None:
        from podman.errors import NotFound

        try:
            container = self.client.containers.get(container_id)
            container.reload()
            return container
        except NotFound:
            return None

    def status(self, handle: RunHandle) -> RuntimeStatus:
        shards: list[ShardRuntime] = []
        try:
            for shard_key, container_ids in handle.get("attempts", {}).items():
                for attempt, container_id in enumerate(container_ids):
                    shards.append(self._shard_runtime(int(shard_key), attempt, container_id))
        except Exception as e:
            return RuntimeStatus(shards=shards, error=f"podman unavailable: {e}")
        return RuntimeStatus(shards=shards)

    def _shard_runtime(self, shard: int, attempt: int, container_id: str) -> ShardRuntime:
        container = self._container(container_id)
        if container is None:
            return ShardRuntime(shard, attempt, container_id, None, "missing")
        state = container.attrs.get("State", {})
        status = (state.get("Status") or "unknown").lower()
        if status in ("stopped", "dead"):
            status = "exited"
        elif status in ("configured", "initialized"):
            status = "created"
        exit_code = state.get("ExitCode") if status == "exited" else None
        return ShardRuntime(
            shard_index=shard,
            attempt=attempt,
            container_id=container_id,
            name=container.name,
            state=status,
            exit_code=exit_code,
            oom_killed=bool(state.get("OOMKilled")),
            started_at=state.get("StartedAt"),
            finished_at=state.get("FinishedAt") if status == "exited" else None,
            error=state.get("Error") or None,
        )

    def _latest_container_id(self, handle: RunHandle, shard: int) -> str | None:
        attempts = handle.get("attempts", {}).get(str(shard), [])
        return attempts[-1] if attempts else None

    def logs(
        self, handle: RunHandle, shard: int = 0, follow: bool = True, tail: int | None = None
    ) -> Iterator[str]:
        container_id = self._latest_container_id(handle, shard)
        if container_id is None:
            raise ExecutorError(f"no container for shard {shard}")
        container = self._container(container_id)
        if container is None:
            raise ExecutorError(f"container for shard {shard} was removed")
        kwargs: dict[str, Any] = {"stream": True, "follow": follow, "stdout": True, "stderr": True}
        if tail is not None:
            kwargs["tail"] = tail
        buffer = ""
        for chunk in container.logs(**kwargs):
            buffer += chunk.decode("utf-8", errors="replace") if isinstance(chunk, bytes) else chunk
            *lines, buffer = buffer.split("\n")
            yield from (line for line in lines if line)
        if buffer:
            yield buffer

    def telemetry(self, handle: RunHandle) -> list[ResourceSample]:
        samples: list[ResourceSample] = []
        for shard_key in handle.get("attempts", {}):
            shard = int(shard_key)
            container_id = self._latest_container_id(handle, shard)
            container = self._container(container_id) if container_id else None
            if container is None or container.attrs.get("State", {}).get("Status") != "running":
                continue
            try:
                raw = container.stats(stream=False, decode=True)
            except Exception as e:
                logger.debug("stats failed for %s: %s", container_id, e)
                continue
            stats = (raw.get("Stats") or [{}])[0] if isinstance(raw, dict) else {}
            cpu = stats.get("CPU")
            samples.append(
                ResourceSample(
                    shard_index=shard,
                    timestamp=time.time(),
                    cpu_percent=float(cpu) if cpu is not None else None,
                    memory_bytes=_parse_size(stats.get("MemUsage")),
                    memory_limit_bytes=_parse_size(stats.get("MemLimit")),
                )
            )
        return samples

    def events(self, handle: RunHandle, since: float) -> list[RuntimeEvent]:
        run_id = handle.get("run_id")
        if not run_id:
            return []
        out: list[RuntimeEvent] = []
        try:
            for event in self.client.events.list(
                since=int(since),
                until=int(time.time()),
                filters={"label": [f"{LABEL_RUN_ID}={run_id}"]},
                decode=True,
            ):
                attributes = event.get("Actor", {}).get("Attributes", {})
                shard = attributes.get(LABEL_SHARD)
                message = attributes.get("name", "")
                if "containerExitCode" in attributes:
                    message += f" exit={attributes['containerExitCode']}"
                time_nano = event.get("timeNano")
                out.append(
                    RuntimeEvent(
                        timestamp=time_nano / 1e9 if time_nano else float(event.get("time", 0)),
                        shard_index=int(shard) if shard is not None else None,
                        type=event.get("Type", ""),
                        action=event.get("Action") or event.get("status", ""),
                        message=message.strip(),
                    )
                )
        except Exception as e:
            logger.debug("events failed: %s", e)
        return out

    def cancel(self, handle: RunHandle) -> None:
        for shard_key in handle.get("attempts", {}):
            container_id = self._latest_container_id(handle, int(shard_key))
            container = self._container(container_id) if container_id else None
            if container is not None and container.attrs.get("State", {}).get("Status") in (
                "running",
                "created",
                "configured",
            ):
                try:
                    container.stop(timeout=10)
                except Exception as e:
                    logger.warning("failed to stop %s: %s", container_id, e)

    def cleanup(self, handle: RunHandle, remove_containers: bool = False) -> None:
        for name in handle.get("secrets", []):
            try:
                self.client.secrets.remove(name)
            except Exception as e:
                logger.debug("secret %s not removed: %s", name, e)
        handle["secrets"] = []
        if not remove_containers:
            return
        for container_ids in handle.get("attempts", {}).values():
            for container_id in container_ids:
                container = self._container(container_id)
                if container is not None:
                    try:
                        container.remove(force=True)
                    except Exception as e:
                        logger.warning("failed to remove %s: %s", container_id, e)


__all__ = ["PodmanExecutor"]
