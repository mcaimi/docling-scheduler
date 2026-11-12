#!/usr/bin/env python
"""Serving the built web UI from the scheduler API."""

import pytest
from fastapi.testclient import TestClient

from docling_scheduler.api import create_app
from docling_scheduler.config import SchedulerConfig
from docling_scheduler.mlflow_monitor import MlflowMonitor
from docling_scheduler.registry import Registry
from docling_scheduler.service import SchedulerService

from .fakes import FakeExecutor


@pytest.fixture
def client(tmp_path):
    web = tmp_path / "dist"
    (web / "assets").mkdir(parents=True)
    (web / "index.html").write_text("<html>app</html>")
    (web / "assets" / "app-123.js").write_text("console.log(1)")
    (tmp_path / "secret.txt").write_text("do not serve")
    config = SchedulerConfig(
        db_path=tmp_path / "s.db", token="tok", web_dir=web,
        allowed_hosts=["testserver"], reconcile_interval=0,
    )
    service = SchedulerService(config, Registry(config.db_path), FakeExecutor(), MlflowMonitor("http://127.0.0.1:1"))
    return TestClient(create_app(service, port=8765))


def test_spa_routes_serve_index_without_token(client):
    for path in ("/", "/runs", "/runs/abc123", "/runs/new"):
        response = client.get(path)
        assert response.status_code == 200, path
        assert response.text == "<html>app</html>"
        assert response.headers["cache-control"] == "no-cache"


def test_assets_are_cached_and_traversal_is_blocked(client):
    asset = client.get("/assets/app-123.js")
    assert asset.text == "console.log(1)"
    assert "immutable" in asset.headers["cache-control"]
    assert client.get("/..%2Fsecret.txt").text == "<html>app</html>"
    assert client.get("/assets/../../secret.txt").text != "do not serve"


def test_api_paths_never_fall_back_to_the_app(client):
    assert client.get("/api/nope").status_code == 401
    assert client.get("/api/nope", headers={"Authorization": "Bearer tok"}).status_code == 404


def test_security_headers(client):
    for path in ("/", "/api/health"):
        headers = client.get(path).headers
        assert headers["x-content-type-options"] == "nosniff"
        assert headers["referrer-policy"] == "no-referrer"
        assert "frame-ancestors 'none'" in headers["content-security-policy"]


def test_no_ui_when_not_built(tmp_path):
    config = SchedulerConfig(db_path=tmp_path / "s.db", token="tok", web_dir=tmp_path / "missing",
                             allowed_hosts=["testserver"], reconcile_interval=0)
    service = SchedulerService(config, Registry(config.db_path), FakeExecutor(), MlflowMonitor("http://127.0.0.1:1"))
    assert TestClient(create_app(service)).get("/").status_code == 404


def test_schema_marks_managed_settings(client):
    schema = client.get("/api/settings/schema", headers={"Authorization": "Bearer tok"}).json()
    managed = {e["key"] for e in schema if e["managed"]}
    assert {"folder_path", "enable_tracing", "mlflow_tracking_uri", "shard_index"} <= managed
    assert "chunk_size" not in managed
