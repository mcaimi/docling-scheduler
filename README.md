# docling-scheduler

Local scheduler for [docling-agent](https://github.com/mcaimi/docling-agent) runs. It starts one container per shard from the agent image, under local Podman or as Jobs on a remote Kubernetes/OpenShift cluster, and monitors the runs through MLflow (a tracking server is required) and the container runtime. It can also serve the [docling-ui](https://github.com/mcaimi/docling-ui) web UI.

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

Without `SCHEDULER_WEB_DIR`, only the API is served. The API binds to loopback by default, because it can start containers. `--host 0.0.0.0` (or any other address) listens more widely; then the bearer token and the `SCHEDULER_ALLOWED_HOSTS` / `SCHEDULER_ALLOWED_ORIGINS` checks are its only guard.

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

### Running on Kubernetes / OpenShift

With `SCHEDULER_EXECUTOR=kubernetes`, the scheduler still runs on your machine, but each shard becomes a Job in a namespace on a remote cluster. It talks to the API server through the official `kubernetes` Python client. Install it with `uv sync --extra k8s`.

1. **Install the namespace resources** (once, as a cluster or namespace admin). `deploy/` is a Kustomize tree: a base with the scheduler's ServiceAccount, a least-privilege Role and a LimitRange, plus optional components for a ResourceQuota, an ingress-deny NetworkPolicy and a long-lived token. Pick the overlay for your cluster and change `namespace:` in it if needed:

   ```bash
   oc apply -k deploy/overlays/openshift        # OpenShift (restricted-v2 SCC, nothing extra granted)
   kubectl apply -k deploy/overlays/kubernetes  # vanilla Kubernetes (Pod Security "restricted")
   ```

2. **Give the scheduler credentials.** By default it uses your current kubeconfig context, which needs the Role's permissions in the namespace. A dedicated kubeconfig for the `docling-scheduler` ServiceAccount is safer:

   ```bash
   TOKEN=$(oc create token docling-scheduler -n docling --duration=8h)   # or: kubectl create token ...
   oc login --server=https://api.cluster.example.com:6443 --token="$TOKEN" --kubeconfig=./docling.kubeconfig
   ```

   On vanilla Kubernetes, build the same file with `kubectl config set-cluster`, `set-credentials --token` and `set-context`, using `--kubeconfig=./docling.kubeconfig`.

   For unattended use, enable the `service-account-token` component and build the kubeconfig from that Secret's token instead.

3. **Push the agent image to a registry** that the cluster can pull from. Allow it with `SCHEDULER_IMAGE_ALLOWLIST`, and if the registry is private, create a pull secret in the namespace and set `SCHEDULER_K8S_IMAGE_PULL_SECRET`. `localhost/` images are rejected. The image must work under an arbitrary non-root UID (OpenShift assigns one), so writable paths must be group-0 writable. `/tmp` is an `emptyDir`.

4. **Run the scheduler:**

   ```bash
   SCHEDULER_EXECUTOR=kubernetes \
   SCHEDULER_K8S_NAMESPACE=docling \
   SCHEDULER_K8S_KUBECONFIG=./docling.kubeconfig \
   SCHEDULER_IMAGE_ALLOWLIST=quay.io/acme/docling-agent \
   SCHEDULER_DEFAULT_IMAGE=quay.io/acme/docling-agent:latest \
   SCHEDULER_MLFLOW_TRACKING_URI=https://mlflow.apps.cluster.example.com \
   SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI=http://mlflow.mlflow.svc:5000 \
     uv run python -m docling_scheduler
   ```

   Requests use `"target": {"type": "kubernetes", ...}`. `/api/targets` reports which executor is active and its capabilities (`host_gateway`, `local_source`, `local_images`).

How runs behave on a cluster:

- **S3 only.** Nothing on your machine is mounted, so `source_backend=local` fails pre-flight. Pre-flight still lists the bucket from your machine to plan the shards.
- **No loopback endpoints.** MLflow, S3, embedding, LLM and vector store endpoints must be reachable from the cluster. Pre-flight rejects any `localhost` endpoint that the run would use, instead of rewriting it. If you reach MLflow through a port-forward or Route but pods should use an in-cluster Service, set `SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI`.
- **Run objects.** Each run gets an anchor ConfigMap `docling-run-<id>` that owns its Jobs, Secret and file ConfigMap. Deleting a run deletes the anchor, and the cluster garbage-collects the rest. Sensitive settings go into a per-submission Secret, referenced with `secretKeyRef` and deleted when the run ends. A host `s3_ca_bundle` is copied into a ConfigMap.
- **Pods.** Pods run non-root with every capability dropped, no privilege escalation, `RuntimeDefault` seccomp and no service account token. `target.cpus` / `target.memory` become requests and limits (`4g` → `4Gi`), and the LimitRange covers runs that don't set them. Each Job has `backoffLimit: 0`; "retry failed shards" adds a new Job per shard. Set `SCHEDULER_K8S_ACTIVE_DEADLINE` to cap a shard's runtime.
- **Stuck pods.** A pod that can never start (`ImagePullBackOff`, `CreateContainerConfigError`, …) fails its shard, so the run can be retried instead of staying pending.
- **Cancel** suspends the Jobs. Pods get SIGTERM and are removed, so their logs go away unless the cluster ships logs elsewhere. Logs of finished pods remain available until the run is deleted.
- **Telemetry** needs metrics-server, and is empty without it. Events are read from the namespace, so use a namespace dedicated to these runs.

### Running the scheduler in the cluster

The `Containerfile` builds an image with the scheduler and a built docling-ui (cloned from `UI_REPO` at `UI_REF`, `main` by default). It runs as an arbitrary non-root UID, so it fits OpenShift `restricted-v2` and the restricted Pod Security Standard:

```bash
podman build -t quay.io/you/docling-scheduler:latest -f Containerfile .
podman push quay.io/you/docling-scheduler:latest
```

`entrypoint.sh` reads every `SCHEDULER_*` variable and applies container defaults: inside a pod, the executor is `kubernetes` and the namespace is the pod's own (in-cluster config, `docling-scheduler` service account). The UI is served from `/opt/app-root/ui`, the registry is kept at `/data/scheduler.db`, and an agent `.env` mounted at `/etc/docling-scheduler/agent.env` is picked up as `SCHEDULER_ENVFILE`.

The image listens on `0.0.0.0:8765` (`SCHEDULER_HOST` / `SCHEDULER_PORT`). To deploy it, uncomment `components/scheduler` in your overlay, plus `components/scheduler-route` on OpenShift, or `components/scheduler-fsgroup` and `components/scheduler-ingress` on vanilla Kubernetes. Then set the hostname in the Route or Ingress, and set the same hostname in `SCHEDULER_ALLOWED_HOSTS` and `SCHEDULER_ALLOWED_ORIGINS` (`https://<hostname>`) in `deploy/components/scheduler/scheduler.env`. Finally, set the image:

```bash
cd deploy/overlays/openshift
kustomize edit set image docling-scheduler=quay.io/you/docling-scheduler:latest
oc create secret generic docling-scheduler-auth -n docling --from-literal=SCHEDULER_TOKEN=$(openssl rand -base64 32)  # optional: stable token
oc create secret generic docling-scheduler-agent -n docling --from-file=agent.env=.env                                # optional
oc apply -k .
# open https://<hostname>/#token=<SCHEDULER_TOKEN>  (without a stable token: oc -n docling logs deploy/docling-scheduler)
```

The scheduler sits behind a ClusterIP Service (`docling-scheduler:8765`). The Route terminates TLS at the router. The Ingress expects a `docling-scheduler-tls` certificate Secret, since the bearer token must not travel in clear text. Without either, `oc -n docling port-forward svc/docling-scheduler 8765` still works, on local port 8765 (the Origin check expects it).

## Configuration

Form defaults and secrets come from `.env` (or `SCHEDULER_ENVFILE`), using the agent's variable names. Secrets are passed to containers as Podman secrets or Kubernetes Secrets, and are never returned by the API.

| Variable | Default |
|---|---|
| `SCHEDULER_EXECUTOR` | `podman` (or `kubernetes`) |
| `SCHEDULER_MLFLOW_TRACKING_URI` (fallback `MLFLOW_TRACKING_URI`) | `http://localhost:5000` |
| `SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI` | tracking URI (loopback rewritten for Podman) |
| `SCHEDULER_MLFLOW_UI_URI` | tracking URI |
| `SCHEDULER_DEFAULT_IMAGE` | `localhost/docling-agent:latest` |
| `SCHEDULER_IMAGE_ALLOWLIST` | `localhost/docling-agent,docling-agent` |
| `SCHEDULER_ALLOWED_MOUNT_ROOTS` | your home directory |
| `SCHEDULER_PODMAN_URL` / `SCHEDULER_PODMAN_RELABEL` | podman-py default / `z` |
| `SCHEDULER_K8S_NAMESPACE` | required with `kubernetes` |
| `SCHEDULER_K8S_KUBECONFIG` / `SCHEDULER_K8S_CONTEXT` | standard kubeconfig lookup, then in-cluster config / current context |
| `SCHEDULER_K8S_IMAGE_PULL_SECRET` | none |
| `SCHEDULER_K8S_ACTIVE_DEADLINE` | none (seconds per shard) |
| `SCHEDULER_MAX_SHARDS` / `SCHEDULER_CONCURRENCY_WARNING` | `32` / `32` |
| `SCHEDULER_DB_PATH` | `$XDG_DATA_HOME/docling-agent/scheduler.db` |
| `SCHEDULER_TOKEN` | random per start |
| `SCHEDULER_ALLOWED_HOSTS` / `SCHEDULER_ALLOWED_ORIGINS` | `127.0.0.1,localhost` / none (add `http://localhost:5173` for the UI dev server, `https://<hostname>` behind a Route or Ingress) |
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

The unit tests use fake executors, a mocked Podman client and an in-memory Kubernetes API. Nothing here runs the agent: test the scheduler and agent together by launching real runs under Podman or on a cluster. Check the manifests with `kustomize build deploy/overlays/openshift`.
