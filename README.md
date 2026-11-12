# docling-scheduler

Local scheduler for [docling-agent](https://github.com/mcaimi/docling-agent) runs. It starts one Podman container per shard from the agent image and monitors the runs through MLflow (a tracking server is required) and the container runtime. It can also serve the [docling-ui](https://github.com/mcaimi/docling-ui) web UI.

The scheduler never imports agent code. It relies only on the agent's published interface:

- the agent **image**, configured purely through environment variables
- the **settings contract** `src/docling_scheduler/settings-schema.json`, vendored from the agent: every setting's env var, type, default, sensitivity, choices and UI dependencies, plus the document globs
- the agent's **MLflow** conventions: nested child runs per shard, `step = docs_completed` progress metrics, per-document traces, and the `run_summary.json` / `error_reports.json` artifacts
- the agent's **exit codes**

## Setup

```bash
uv sync --extra dev
# build the agent image (in the docling-agent repo)
podman build -t localhost/docling-agent:latest -f Containerfile .
# optional: build the UI (in the docling-ui repo)
npm ci && npm run build

SCHEDULER_MLFLOW_TRACKING_URI=http://localhost:5000 \
SCHEDULER_WEB_DIR=../docling-ui/dist \
  uv run python -m docling_scheduler --port 8765
# open the printed URL: http://localhost:8765/#token=...
```

Without `SCHEDULER_WEB_DIR`, only the API is served. The API binds to loopback only: it can start containers.

Every `/api` call needs `Authorization: Bearer <token>`:

```bash
curl -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"settings": {"chunk_size": 800}, "target": {"host_folder": "/home/me/pdfs", "shards": 2, "cpus": 2}}' \
  http://127.0.0.1:8765/api/runs/validate
```

### Documents from S3

With `source_backend=s3`, the agent reads documents from `s3_bucket`/`s3_prefix` instead of a mounted folder. Omit `target.host_folder` in that case:

```bash
curl -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"settings": {"source_backend": "s3", "s3_bucket": "docs", "s3_prefix": "incoming/",
                    "s3_endpoint_url": "http://localhost:9000", "s3_addressing_style": "path"},
       "target": {"shards": 2, "cpus": 2}}' \
  http://127.0.0.1:8765/api/runs/validate
```

- Pre-flight lists the bucket from the scheduler host, using the agent's rules, to count documents and plan shards. A wrong bucket, endpoint or credentials fails validation instead of failing the containers.
- Put credentials in `.env` (`S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` / `S3_SESSION_TOKEN`, or the `AWS_*` equivalents). They are passed as Podman secrets, and only to S3 runs. Containers don't see the host's `~/.aws` or `AWS_PROFILE`.
- A loopback `s3_endpoint_url` (for example, a local MinIO) is rewritten to `host.containers.internal`. A host `s3_ca_bundle` must be under `SCHEDULER_ALLOWED_MOUNT_ROOTS` and is mounted read-only into the containers.

## Configuration

Form defaults and secrets come from `.env` (or `SCHEDULER_ENVFILE`), using the agent's variable names. Secrets are passed to containers as Podman secrets and are never returned by the API.

| Variable | Default |
|---|---|
| `SCHEDULER_MLFLOW_TRACKING_URI` (fallback `MLFLOW_TRACKING_URI`) | `http://localhost:5000` |
| `SCHEDULER_MLFLOW_UI_URI` | tracking URI |
| `SCHEDULER_DEFAULT_IMAGE` | `localhost/docling-agent:latest` |
| `SCHEDULER_IMAGE_ALLOWLIST` | `localhost/docling-agent,docling-agent` |
| `SCHEDULER_ALLOWED_MOUNT_ROOTS` | your home directory |
| `SCHEDULER_PODMAN_URL` / `SCHEDULER_PODMAN_RELABEL` | podman-py default / `z` |
| `SCHEDULER_MAX_SHARDS` / `SCHEDULER_CONCURRENCY_WARNING` | `32` / `32` |
| `SCHEDULER_DB_PATH` | `$XDG_DATA_HOME/docling-agent/scheduler.db` |
| `SCHEDULER_TOKEN` | random per start |
| `SCHEDULER_ALLOWED_HOSTS` / `SCHEDULER_ALLOWED_ORIGINS` | `127.0.0.1,localhost` / none (add `http://localhost:5173` for the UI dev server) |
| `SCHEDULER_RECONCILE_INTERVAL` | `10` seconds |
| `SCHEDULER_WEB_DIR` | unset (API only) |

## Updating the settings contract

When the agent's settings change, refresh the vendored copy from a docling-agent checkout:

```bash
(cd ../docling-agent && uv run python -m src.docling_agent --print-settings-schema) \
  > src/docling_scheduler/settings-schema.json
```

The scheduler refuses to start if the contract's `schema_version` is one it doesn't support.

## Testing

```bash
uv run pytest
```

The unit tests use fake executors and a mocked Podman client. Nothing here runs the agent: test the scheduler and agent together by launching real runs under Podman.
