#!/usr/bin/env python
"""HTTP API of the scheduler (FastAPI).

Security model: the server binds to loopback by default (``--host`` can
widen it, e.g. behind a Kubernetes Service) and holds the power to start
containers, so every ``/api`` route except ``/api/health`` requires the
startup token (``Authorization: Bearer <token>``; the log stream also accepts
``?token=`` because ``EventSource`` cannot set headers). Requests must carry
an allowed ``Host`` header (DNS-rebinding defence), and state-changing
requests from a browser must come from an allowed ``Origin``. Secrets are
never returned.
"""

import asyncio
import hmac
import json
import logging
import threading
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .executors.base import ExecutorError
from .models import RunRequest
from .service import InvalidState, PreflightFailed, RunNotFound, SchedulerService

logger = logging.getLogger(__name__)

_PUBLIC_PATHS = {"/api/health"}
_QUERY_TOKEN_SUFFIX = "/logs"
_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
_SECURITY_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "Referrer-Policy": "no-referrer",
    "X-Frame-Options": "DENY",
    # charts set inline style attributes; scripts are same-origin only
    "Content-Security-Policy": (
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'"
    ),
}


def _allowed_origins(service: SchedulerService, port: int | None) -> set[str]:
    origins = set(service.config.allowed_origins)
    if port:
        for host in service.config.allowed_hosts:
            origins.add(f"http://{host}:{port}")
    return origins


def create_app(service: SchedulerService, *, port: int | None = None) -> FastAPI:
    config = service.config
    origins = _allowed_origins(service, port)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        stop = threading.Event()
        thread = None
        if config.reconcile_interval > 0:

            def loop():
                while not stop.wait(config.reconcile_interval):
                    service.reconcile_all()

            thread = threading.Thread(target=loop, name="scheduler-reconciler", daemon=True)
            thread.start()
        yield
        stop.set()
        if thread is not None:
            thread.join(timeout=5)

    app = FastAPI(title="Docling agent scheduler", lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.middleware("http")
    async def guard(request: Request, call_next):
        path = request.url.path
        if path.startswith("/api") and path not in _PUBLIC_PATHS:
            origin = request.headers.get("origin")
            if request.method not in _SAFE_METHODS and origin is not None and origin not in origins:
                return JSONResponse({"detail": "origin not allowed"}, status_code=403)
            supplied = None
            auth = request.headers.get("authorization", "")
            if auth.lower().startswith("bearer "):
                supplied = auth[7:].strip()
            elif path.endswith(_QUERY_TOKEN_SUFFIX):
                supplied = request.query_params.get("token")
            if not supplied or not hmac.compare_digest(supplied, config.token):
                return JSONResponse(
                    {"detail": "missing or invalid token"},
                    status_code=401,
                    headers=_SECURITY_HEADERS,
                )
        response = await call_next(request)
        for name, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(name, value)
        return response

    # added last = outermost: reject bad Host headers before anything else
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=config.allowed_hosts)

    def not_found(run_id: str) -> HTTPException:
        return HTTPException(status_code=404, detail=f"run {run_id} not found")

    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/settings/schema")
    def settings_schema() -> list[dict[str, Any]]:
        return service.settings_schema()

    @app.get("/api/settings/defaults")
    def settings_defaults() -> dict[str, Any]:
        return service.settings_defaults()

    @app.get("/api/targets")
    def targets() -> dict[str, Any]:
        return service.targets()

    @app.post("/api/runs/validate")
    def validate(request: RunRequest) -> dict[str, Any]:
        return service.preflight(request).public()

    @app.post("/api/runs", status_code=201)
    def create_run(request: RunRequest):
        try:
            return service.create_run(request)
        except PreflightFailed as e:
            return JSONResponse(e.report.public(), status_code=400)
        except ExecutorError as e:
            raise HTTPException(status_code=502, detail=str(e)) from e

    @app.get("/api/runs")
    def list_runs(status: str | None = None, limit: int = Query(100, ge=1, le=1000)):
        return service.list_runs(status=status, limit=limit)

    @app.get("/api/runs/{run_id}")
    def get_run(run_id: str):
        try:
            return service.get_run(run_id)
        except RunNotFound:
            raise not_found(run_id) from None

    @app.get("/api/runs/{run_id}/mlflow")
    def run_mlflow(run_id: str):
        try:
            return service.mlflow_overview(run_id)
        except RunNotFound:
            raise not_found(run_id) from None

    @app.get("/api/runs/{run_id}/documents")
    def run_documents(run_id: str, after: int = Query(0, ge=0), generation: str | None = None):
        try:
            return service.documents(run_id, after=after, generation=generation)
        except RunNotFound:
            raise not_found(run_id) from None

    @app.get("/api/runs/{run_id}/errors")
    def run_errors(run_id: str):
        try:
            return service.error_reports(run_id)
        except RunNotFound:
            raise not_found(run_id) from None

    @app.get("/api/runs/{run_id}/runtime")
    def run_runtime(run_id: str):
        try:
            return service.runtime(run_id)
        except RunNotFound:
            raise not_found(run_id) from None

    @app.get("/api/runs/{run_id}/logs")
    async def run_logs(
        run_id: str,
        shard: int = Query(0, ge=0),
        follow: bool = True,
        tail: int | None = Query(None, ge=0),
        level: str | None = None,
        file: str | None = None,
    ):
        try:
            entries = service.logs(
                run_id, shard=shard, follow=follow, tail=tail, level=level, file=file
            )
            # surface "no such shard/container" as an HTTP error, not a broken stream
            first = await asyncio.to_thread(next, entries, None)
        except RunNotFound:
            raise not_found(run_id) from None
        except (ValueError, ExecutorError) as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        def stream():
            try:
                if first is not None:
                    yield f"data: {json.dumps(first, default=str)}\n\n"
                for entry in entries:
                    yield f"data: {json.dumps(entry, default=str)}\n\n"
                yield "event: end\ndata: {}\n\n"
            except ExecutorError as e:
                yield f"event: error\ndata: {json.dumps({'detail': str(e)})}\n\n"

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.post("/api/runs/{run_id}/cancel")
    def cancel(run_id: str):
        try:
            return service.cancel(run_id)
        except RunNotFound:
            raise not_found(run_id) from None
        except InvalidState as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except ExecutorError as e:
            raise HTTPException(status_code=502, detail=str(e)) from e

    @app.post("/api/runs/{run_id}/retry-failed")
    def retry_failed(run_id: str):
        try:
            return service.retry_failed(run_id)
        except RunNotFound:
            raise not_found(run_id) from None
        except InvalidState as e:
            raise HTTPException(status_code=409, detail=str(e)) from e
        except PreflightFailed as e:
            return JSONResponse(e.report.public(), status_code=400)
        except ExecutorError as e:
            raise HTTPException(status_code=502, detail=str(e)) from e

    @app.delete("/api/runs/{run_id}", status_code=204)
    def delete(run_id: str):
        try:
            service.delete(run_id)
        except RunNotFound:
            raise not_found(run_id) from None
        except InvalidState as e:
            raise HTTPException(status_code=409, detail=str(e)) from e

    web_dir = config.web_dir.resolve() if config.web_dir else None
    if web_dir is not None and (web_dir / "index.html").is_file():
        index = web_dir / "index.html"

        # registered last: every non-API GET serves a built asset or the SPA shell
        @app.get("/{path:path}", include_in_schema=False)
        def web(path: str):
            if path == "api" or path.startswith("api/"):
                raise HTTPException(status_code=404, detail="not found")
            candidate = (web_dir / path).resolve()
            if path and candidate.is_file() and candidate.is_relative_to(web_dir):
                # hashed asset names: safe to cache
                return FileResponse(candidate, headers={"Cache-Control": "public, max-age=31536000, immutable"}
                                    if path.startswith("assets/") else None)
            return FileResponse(index, headers={"Cache-Control": "no-cache"})

    return app


__all__ = ["create_app"]
