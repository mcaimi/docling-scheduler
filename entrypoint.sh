#!/bin/sh
# Container entrypoint: fills in container defaults for every SCHEDULER_*
# variable, then runs the scheduler. Everything is configured through the
# environment (a ConfigMap/Secret with envFrom on Kubernetes); arguments are
# passed through to `python -m docling_scheduler`.
set -eu

SA_DIR=/var/run/secrets/kubernetes.io/serviceaccount

# Sets $1 to $2 when $1 is unset or empty; exports it only when non-empty, so
# the scheduler's own defaults apply to anything left blank.
default() {
    eval "_value=\${$1:-}"
    [ -n "$_value" ] || _value=$2
    if [ -n "$_value" ]; then
        export "$1=$_value"
    else
        unset "$1"
    fi
}

# --- listener (all interfaces: loopback is unreachable from outside a container)
default SCHEDULER_HOST 0.0.0.0
default SCHEDULER_PORT 8765

# --- runtime -------------------------------------------------------------------
# In a pod: Jobs go to the pod's own namespace, through its service account.
if [ -r "$SA_DIR/namespace" ]; then
    default SCHEDULER_EXECUTOR kubernetes
    default SCHEDULER_K8S_NAMESPACE "$(cat "$SA_DIR/namespace")"
else
    default SCHEDULER_EXECUTOR podman
    default SCHEDULER_K8S_NAMESPACE ""
fi
default SCHEDULER_K8S_KUBECONFIG ""          # unset: in-cluster config
default SCHEDULER_K8S_CONTEXT ""
default SCHEDULER_K8S_IMAGE_PULL_SECRET ""
default SCHEDULER_K8S_ACTIVE_DEADLINE ""     # seconds per shard
default SCHEDULER_PODMAN_URL ""              # e.g. unix:///run/podman/podman.sock
default SCHEDULER_PODMAN_RELABEL z

# --- agent image -------------------------------------------------------------
default SCHEDULER_DEFAULT_IMAGE ""           # scheduler default: localhost/docling-agent:latest
default SCHEDULER_IMAGE_ALLOWLIST ""         # comma-separated prefixes
default SCHEDULER_ALLOWED_MOUNT_ROOTS ""     # Podman host folders only

# --- MLflow --------------------------------------------------------------------
default SCHEDULER_MLFLOW_TRACKING_URI "${MLFLOW_TRACKING_URI:-}"
default SCHEDULER_CONTAINER_MLFLOW_TRACKING_URI ""
default SCHEDULER_MLFLOW_UI_URI ""

# --- state, settings and UI ------------------------------------------------------
default SCHEDULER_DB_PATH /data/scheduler.db
# agent .env (form defaults and secrets), mounted from a Secret when present
if [ -r /etc/docling-scheduler/agent.env ]; then
    default SCHEDULER_ENVFILE /etc/docling-scheduler/agent.env
else
    default SCHEDULER_ENVFILE ""
fi
default SCHEDULER_WEB_DIR /opt/app-root/ui

# --- limits and API ------------------------------------------------------------
default SCHEDULER_MAX_SHARDS ""
default SCHEDULER_CONCURRENCY_WARNING ""
default SCHEDULER_RECONCILE_INTERVAL ""
default SCHEDULER_TOKEN ""                   # unset: random per start, printed below
default SCHEDULER_ALLOWED_HOSTS ""           # Host headers: add the Route/Ingress hostname
default SCHEDULER_ALLOWED_ORIGINS ""         # browser origins: add https://<hostname>
default LOG_LEVEL INFO

mkdir -p "$(dirname "$SCHEDULER_DB_PATH")"

exec python -m docling_scheduler --host "$SCHEDULER_HOST" --port "$SCHEDULER_PORT" "$@"
