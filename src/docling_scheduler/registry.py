#!/usr/bin/env python
"""SQLite registry of scheduler runs (metadata only, never secrets).

Live progress is always read from MLflow and the container runtime; the
registry only remembers what was launched where, plus the last merged status
so run lists don't have to query the runtime for every row.
"""

import json
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    created_at REAL NOT NULL,
    target_type TEXT NOT NULL,
    image TEXT NOT NULL,
    shard_count INTEGER NOT NULL,
    mlflow_experiment_id TEXT,
    mlflow_parent_run_id TEXT,
    handle TEXT NOT NULL DEFAULT '{}',
    config TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL,
    final INTEGER NOT NULL DEFAULT 0,
    cancelled INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    finished_at REAL
)
"""

# Merged run statuses (see reconcile.merged_status).
TERMINAL_STATUSES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED", "SUBMIT_FAILED"})


@dataclass
class RunRecord:
    id: str
    created_at: float
    target_type: str
    image: str
    shard_count: int
    mlflow_experiment_id: str | None
    mlflow_parent_run_id: str | None
    handle: dict[str, Any] = field(default_factory=dict)
    # Non-secret settings, target spec, and pre-flight warnings at launch.
    config: dict[str, Any] = field(default_factory=dict)
    status: str = "SUBMITTING"
    # True once status is terminal and cleanup has run; never re-derived.
    final: bool = False
    cancelled: bool = False
    error: str | None = None
    finished_at: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "target_type": self.target_type,
            "image": self.image,
            "shard_count": self.shard_count,
            "mlflow_experiment_id": self.mlflow_experiment_id,
            "mlflow_parent_run_id": self.mlflow_parent_run_id,
            "config": self.config,
            "status": self.status,
            "final": self.final,
            "cancelled": self.cancelled,
            "error": self.error,
            "finished_at": self.finished_at,
        }


class Registry:
    def __init__(self, db_path: str | Path):
        self._path = str(db_path)
        if self._path != ":memory:":
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # one shared connection, serialised by the lock (FastAPI runs sync
        # endpoints in a thread pool; the reconciler has its own thread)
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute(_SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @staticmethod
    def _from_row(row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            id=row["id"],
            created_at=row["created_at"],
            target_type=row["target_type"],
            image=row["image"],
            shard_count=row["shard_count"],
            mlflow_experiment_id=row["mlflow_experiment_id"],
            mlflow_parent_run_id=row["mlflow_parent_run_id"],
            handle=json.loads(row["handle"]),
            config=json.loads(row["config"]),
            status=row["status"],
            final=bool(row["final"]),
            cancelled=bool(row["cancelled"]),
            error=row["error"],
            finished_at=row["finished_at"],
        )

    def create(self, record: RunRecord) -> RunRecord:
        with self._lock:
            self._conn.execute(
                "INSERT INTO runs (id, created_at, target_type, image, shard_count,"
                " mlflow_experiment_id, mlflow_parent_run_id, handle, config, status,"
                " final, cancelled, error, finished_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.id,
                    record.created_at,
                    record.target_type,
                    record.image,
                    record.shard_count,
                    record.mlflow_experiment_id,
                    record.mlflow_parent_run_id,
                    json.dumps(record.handle),
                    json.dumps(record.config),
                    record.status,
                    int(record.final),
                    int(record.cancelled),
                    record.error,
                    record.finished_at,
                ),
            )
            self._conn.commit()
        return record

    def get(self, run_id: str) -> RunRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        return self._from_row(row) if row else None

    def query(self, *, status: str | None = None, limit: int = 100) -> list[RunRecord]:
        query = "SELECT * FROM runs"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [self._from_row(r) for r in rows]

    def list_active(self) -> list[RunRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs WHERE final = 0 ORDER BY created_at"
            ).fetchall()
        return [self._from_row(r) for r in rows]

    _UPDATABLE = {"handle", "status", "final", "cancelled", "error", "finished_at",
                  "mlflow_experiment_id", "mlflow_parent_run_id"}

    def update(self, run_id: str, **fields: Any) -> None:
        unknown = set(fields) - self._UPDATABLE
        if unknown:
            raise ValueError(f"cannot update {sorted(unknown)}")
        values = []
        for key, value in fields.items():
            if key == "handle":
                value = json.dumps(value)
            elif key in ("final", "cancelled"):
                value = int(value)
            values.append(value)
        assignments = ", ".join(f"{k} = ?" for k in fields)
        with self._lock:
            self._conn.execute(
                f"UPDATE runs SET {assignments} WHERE id = ?", (*values, run_id)
            )
            self._conn.commit()

    def mark_final(self, run_id: str, status: str) -> None:
        self.update(run_id, status=status, final=True, finished_at=time.time())

    def delete(self, run_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM runs WHERE id = ?", (run_id,))
            self._conn.commit()


__all__ = ["Registry", "RunRecord", "TERMINAL_STATUSES"]
