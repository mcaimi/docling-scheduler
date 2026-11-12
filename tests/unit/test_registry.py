#!/usr/bin/env python
"""SQLite run registry."""

import pytest

from docling_scheduler.registry import Registry, RunRecord


@pytest.fixture
def registry(tmp_path):
    reg = Registry(tmp_path / "sub" / "scheduler.db")
    yield reg
    reg.close()


def record(run_id="r1", created_at=1.0):
    return RunRecord(run_id, created_at, "podman", "img", 2, "1", "p" * 32,
                     handle={"attempts": {"0": ["c"]}}, config={"settings": {"a": 1}})


def test_roundtrip(registry):
    registry.create(record())
    got = registry.get("r1")
    assert got.handle == {"attempts": {"0": ["c"]}}
    assert got.config == {"settings": {"a": 1}}
    assert got.status == "SUBMITTING" and not got.final
    assert registry.get("missing") is None


def test_update_and_final(registry):
    registry.create(record())
    registry.update("r1", status="RUNNING", cancelled=True)
    assert registry.get("r1").cancelled
    assert [r.id for r in registry.list_active()] == ["r1"]
    registry.mark_final("r1", "FAILED")
    got = registry.get("r1")
    assert got.final and got.status == "FAILED" and got.finished_at
    assert registry.list_active() == []


def test_update_rejects_unknown_columns(registry):
    registry.create(record())
    with pytest.raises(ValueError):
        registry.update("r1", id="hijack")


def test_query_orders_and_filters(registry):
    registry.create(record("old", 1.0))
    registry.create(record("new", 2.0))
    registry.update("old", status="FAILED")
    assert [r.id for r in registry.query()] == ["new", "old"]
    assert [r.id for r in registry.query(status="FAILED")] == ["old"]
    registry.delete("old")
    assert [r.id for r in registry.query()] == ["new"]


def test_persists_across_instances(tmp_path):
    path = tmp_path / "s.db"
    Registry(path).create(record())
    assert Registry(path).get("r1") is not None
