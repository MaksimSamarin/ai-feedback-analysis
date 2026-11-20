from contextlib import contextmanager

import pytest

from app import db


class Connection:
    def __init__(self, row):
        self.row = row
        self.queries = []

    def execute(self, sql, params=()):
        self.queries.append((" ".join(sql.split()), params))
        return self

    def fetchone(self):
        return self.row

    def fetchall(self):
        return []


@pytest.mark.parametrize("reason", ["user", "admin", "retention", "paused_ttl"])
def test_delete_preserves_metrics_and_removes_files(monkeypatch, tmp_path, reason):
    row = {"id": "r1", "user_id": 7, "status": "completed", "results_file": "r.xlsx", "raw_file": "r.json"}
    for name in ("r.xlsx", "r.json"):
        (tmp_path / name).write_text("test")
    conn = Connection(row)
    monkeypatch.setattr(db, "RESULTS_DIR", tmp_path)
    monkeypatch.setattr(db, "get_redis_client", lambda: None)
    assert db._soft_delete_report(conn, row, deleted_by=9, reason=reason)
    assert not list(tmp_path.iterdir())
    sql = "\n".join(q[0] for q in conn.queries)
    assert "DELETE FROM report_rows" in sql
    assert "DELETE FROM reports" not in sql
    assert "total_tokens =" not in sql
    assert "summary_json = NULL" in sql
    assert "api_key_encrypted = NULL" in sql
    assert "deleted_at IS NULL" in sql


@pytest.mark.parametrize("status", ["running", "queued", "paused"])
def test_active_report_cannot_be_deleted(status):
    conn = Connection({})
    assert not db._soft_delete_report(conn, {"status": status}, deleted_by=1, reason="user")
    assert not conn.queries


def test_already_deleted_is_idempotent():
    conn = Connection({})
    assert db._soft_delete_report(conn, {"deleted_at": "2026-11-08"}, deleted_by=1, reason="user")
    assert not conn.queries


def test_deleted_lookup_is_hidden(monkeypatch):
    conn = Connection(None)
    @contextmanager
    def get_conn():
        yield conn
    monkeypatch.setattr(db, "get_conn", get_conn)
    db.get_report("r1", 7)
    db.get_report_any("r1")
    db.get_report_by_job_id("r1", 7)
    assert all("deleted_at IS NULL" in sql for sql, _ in conn.queries)
