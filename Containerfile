# docling-scheduler image: the scheduler API plus a built docling-ui.
#
#   podman build -t docling-scheduler:latest -f Containerfile .
#   podman build --build-arg UI_REF=v0.1.0 ...        # pin the UI
#
# Runs as an arbitrary non-root UID in group 0 (OpenShift restricted-v2, Pod
# Security "restricted"). Configuration is environment only: see entrypoint.sh.

ARG UBI_TAG=latest

# --- 1. web UI ---------------------------------------------------------------
FROM registry.access.redhat.com/ubi9/nodejs-22:latest AS ui

ARG UI_REPO=https://github.com/mcaimi/docling-ui.git
ARG UI_REF=main

USER 0
RUN command -v git >/dev/null || dnf install -y --setopt=install_weak_deps=0 git-core
WORKDIR /build
RUN git clone --depth 1 --branch "${UI_REF}" "${UI_REPO}" docling-ui
WORKDIR /build/docling-ui
RUN npm ci --no-audit --no-fund && npm run build

# --- 2. scheduler --------------------------------------------------------------
FROM registry.access.redhat.com/ubi9/ubi-minimal:${UBI_TAG} AS build

RUN microdnf install -y --setopt=install_weak_deps=0 python3.12 && microdnf clean all
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_PYTHON=/usr/bin/python3.12 \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/app-root/venv

WORKDIR /build
# dependencies first, so source edits don't invalidate this layer
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra k8s --no-install-project
COPY README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --extra k8s --no-editable

# --- 3. runtime ----------------------------------------------------------------
FROM registry.access.redhat.com/ubi9/ubi-minimal:${UBI_TAG}

LABEL org.opencontainers.image.title="docling-scheduler" \
      org.opencontainers.image.description="Launches and monitors containerised docling-agent runs" \
      org.opencontainers.image.source="https://github.com/mcaimi/docling-scheduler"

# same interpreter path as the build stage: the venv links to it
RUN microdnf install -y --setopt=install_weak_deps=0 python3.12 && microdnf clean all

COPY --from=build /opt/app-root/venv /opt/app-root/venv
COPY --from=ui /build/docling-ui/dist /opt/app-root/ui
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# arbitrary UID at runtime, always in group 0: give the group the owner's rights
RUN mkdir -p /opt/app-root/home /data \
    && chown -R 1001:0 /opt/app-root /data \
    && chmod -R g=u /opt/app-root /data \
    && chmod 0755 /usr/local/bin/entrypoint.sh

ENV PATH=/opt/app-root/venv/bin:${PATH} \
    HOME=/opt/app-root/home \
    PYTHONUNBUFFERED=1 \
    SCHEDULER_WEB_DIR=/opt/app-root/ui \
    SCHEDULER_DB_PATH=/data/scheduler.db

WORKDIR /opt/app-root/home
VOLUME ["/data"]
EXPOSE 8765
USER 1001

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
