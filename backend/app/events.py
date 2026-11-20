"""Структурированные события: индексируемый поиск и безопасная хронология отчётов."""
from datetime import datetime, timedelta, timezone
import csv
import io
import json
import logging
import os
import queue
import threading
import time

from fastapi import HTTPException

from app import db
from app.governance import require_management

RETENTION_DAYS = max(1, int(os.getenv("EVENT_RETENTION_DAYS", "30")))
_pending = queue.Queue(maxsize=50000)
_started = False
_start_lock = threading.Lock()


def init_schema(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS service_events (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, event_at TIMESTAMPTZ NOT NULL,
        level TEXT NOT NULL, service TEXT NOT NULL, user_id BIGINT, username TEXT,
        report_id TEXT, job_id TEXT, request_id TEXT, event_type TEXT NOT NULL,
        group_id BIGINT, business BOOLEAN NOT NULL DEFAULT FALSE,
        message TEXT NOT NULL, traceback TEXT)""")
    for name, cols in {
        "time": "event_at DESC,id DESC", "user": "user_id,event_at DESC", "login": "username,event_at DESC",
        "report": "report_id,event_at DESC", "job": "job_id,event_at DESC", "request": "request_id,event_at DESC",
        "scope": "group_id,business,event_at DESC", "kind": "event_type,event_at DESC",
        "service_level": "service,level,event_at DESC",
        "request_service_level": "request_id,service,level,event_at DESC,id DESC",
        "request_service_time": "request_id,service,event_at DESC,id DESC",
    }.items():
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_events_{name} ON service_events({cols})")
    # pg_trgm входит в используемый образ Postgres; пакеты и расширения извне не загружаются.
    conn.execute("SAVEPOINT events_trgm")
    try:
        conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_message ON service_events USING gin(message gin_trgm_ops)")
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT events_trgm")
        logging.getLogger(__name__).warning(
            "Расширение pg_trgm или триграммный индекс недоступны; текстовый поиск работает без ускорения",
            extra={"event_type": "schema.search_fallback"},
        )
    finally:
        conn.execute("RELEASE SAVEPOINT events_trgm")
    conn.execute("ALTER TABLE admin_audit ADD COLUMN IF NOT EXISTS group_id BIGINT")
    conn.execute("ALTER TABLE service_events ADD COLUMN IF NOT EXISTS http_status INTEGER")
    conn.execute("""UPDATE service_events SET http_status=substring(message from 'status=([0-9]{3})')::integer
        WHERE event_type='http.request' AND http_status IS NULL AND message ~ 'status=[0-9]{3}'""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_scope_time ON admin_audit(group_id,created_at DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_audit_actor_time ON admin_audit(actor_user_id,created_at DESC)")
    conn.execute(r"""UPDATE admin_audit a SET group_id = CASE
        WHEN target_type='group' AND target_id ~ '^\d+$' THEN target_id::bigint
        WHEN target_type='member' AND target_id ~ '^\d+:\d+$' THEN split_part(target_id,':',1)::bigint
        WHEN target_type='request' AND target_id ~ '^\d+$' THEN
            (SELECT requested_group_id FROM access_requests WHERE id=a.target_id::bigint)
        WHEN target_type='report' THEN (SELECT access_group_id FROM reports WHERE id=a.target_id LIMIT 1)
        END WHERE group_id IS NULL AND target_type IN ('group','member','request','report')""")


def _identifier(value):
    try:
        return int(value) if value not in {None, "-", ""} else None
    except (TypeError, ValueError):
        return None


def insert_event(conn, item):
    conn.execute("""INSERT INTO service_events(event_at,level,service,user_id,username,report_id,job_id,
        request_id,event_type,group_id,business,message,traceback,http_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        item.get("ts") or db._now_iso(), item.get("level", "INFO"), item.get("service", "backend"),
        _identifier(item.get("user_id")), item.get("username"), item.get("report_id"), item.get("job_id"),
        item.get("request_id"), item.get("event_type") or "application", _identifier(item.get("group_id")),
        bool(item.get("business", False)), item.get("message", ""), item.get("exception"), item.get("status_code"),
    ))


def record_event(conn, event_type, message, report=None, *, actor_id=None):
    from app.logging_utils import current_context, _redact
    row = report or {}
    item = {**current_context(), "event_type": event_type, "message": _redact(message), "business": True,
            "report_id": row.get("id"), "job_id": row.get("job_id"), "group_id": row.get("access_group_id")}
    if actor_id is not None:
        actor = conn.execute("SELECT username FROM users WHERE id=?", (actor_id,)).fetchone()
        item.update(user_id=actor_id, username=actor.get("username") if actor else None)
    elif item.get("user_id") in {None, "-"}:
        item["user_id"] = row.get("user_id")
    insert_event(conn, item)


def record_export(report_id, actor_id):
    with db.get_conn() as conn:
        report = conn.execute("SELECT id,job_id,user_id,access_group_id FROM reports WHERE id=?", (report_id,)).fetchone()
        if report:
            record_event(conn, "report.export", "Выгружен файл отчёта", report, actor_id=actor_id)


def _writer():
    last_cleanup = 0
    while True:
        try:
            batch = [_pending.get(timeout=1)]
        except queue.Empty:
            batch = []
        while len(batch) < 200:
            try:
                batch.append(_pending.get_nowait())
            except queue.Empty:
                break
        while True:
            try:
                with db.get_conn() as conn:
                    for item in batch:
                        insert_event(conn, item)
                    if time.monotonic() - last_cleanup > 60:
                        # Пакетная ретенция не удерживает транзакцию на всём объёме.
                        cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
                        removed = conn.execute("DELETE FROM service_events WHERE id IN (SELECT id FROM service_events WHERE event_at<? LIMIT 10000)", (cutoff,))
                        last_cleanup = 0 if removed.rowcount >= 10000 else time.monotonic()
                break
            except Exception:
                # Отдельный поток сохраняет очередь до окончания миграции/восстановления БД.
                time.sleep(1)
        for _ in batch:
            _pending.task_done()


class DatabaseHandler(logging.Handler):
    def emit(self, record):
        global _started
        try:
            item = json.loads(self.format(record))
            with _start_lock:
                if not _started:
                    threading.Thread(target=_writer, name="structured-log-writer", daemon=True).start()
                    _started = True
            _pending.put_nowait(item)
        except Exception:
            # stdout и ротационные файлы остаются независимыми от индекса поиска.
            pass


def search(user, *, group_id=None, report_id=None, job_id=None, request_id=None, username=None,
           q=None, event_type=None, service=None, levels=None, date_from=None, date_to=None,
           offset=0, limit=100, ascending=False, timeline=False, include_http=False):
    scope = require_management(user)
    if not scope.admin and not timeline:
        raise HTTPException(403, "Системные логи доступны только администратору")
    if group_id is not None:
        scope.require_group(group_id)
    where, args = scope.sql("e.group_id")
    if not include_http:
        where += " AND NOT (e.event_type='http.request' AND COALESCE(e.http_status,500)<400)"
    if not scope.admin or timeline:
        where += " AND e.business"
    for column, value in (("e.group_id", group_id), ("e.report_id", report_id), ("e.job_id", job_id),
                          ("e.request_id", request_id),
                          ("e.event_type", event_type), ("e.service", service)):
        if value and value != "all":
            where += f" AND {column}=?"
            args.append(value)
    levels = levels if levels is not None else 'INFO,WARNING,ERROR,CRITICAL'
    if levels:
        values = [v.upper() for v in levels.split(",") if v.upper() in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}]
        if values:
            where += " AND e.level IN (" + ",".join("?" for _ in values) + ")"
            args.extend(values)
    for value, operator in ((date_from, ">="), (date_to, "<")):
        if value:
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                raise HTTPException(422, "Некорректная дата периода")
            where += f" AND e.event_at{operator}?::timestamptz"
            args.append(value)
    if q:
        where += " AND e.message ILIKE ?"
        args.append("%" + q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
    if username:
        from app.user_profile import scoped_name
        where += " AND (e.username ILIKE ? OR e.user_id IN (SELECT fu.id FROM users fu WHERE ("+scoped_name(scope,'fu')+") ILIKE ?))"
        args.extend(['%'+username+'%']*2)
    with db.get_conn() as conn:
        total = int(conn.execute("SELECT COUNT(*) AS n FROM service_events e WHERE " + where, args).fetchone()["n"])
        columns = "e.*" if scope.admin else "e.id,e.event_at,e.level,e.user_id,e.username,e.report_id,e.job_id,e.event_type,e.message"
        order = "ASC" if ascending else "DESC"
        rows = conn.execute(f"SELECT {columns} FROM service_events e WHERE " + where +
                            f" ORDER BY e.event_at {order},e.id {order} LIMIT ? OFFSET ?", [*args, limit, offset]).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "has_more": offset + len(rows) < total}


def audit_search(user, *, actor_id=None, action=None, target_type=None, target_id=None, author=None, object_query=None,
                 date_from=None, date_to=None, group_id=None, offset=0, limit=100, include_unchanged=False):
    scope = require_management(user)
    if group_id is not None:
        scope.require_group(group_id)
    where, args = scope.sql("a.group_id")
    if not include_unchanged:
        from app.audit_view import changes_predicate
        where += ' AND (' + changes_predicate() + ')'
    for col, val, op in (("a.actor_user_id", actor_id, "="), ("a.action", action, "="),
                         ("a.target_type", target_type, "="), ("a.target_id", target_id, "="),
                         ("a.group_id", group_id, "="), ("a.created_at", date_from, ">="), ("a.created_at", date_to, "<")):
        if val:
            if col == "a.created_at":
                from app.access import utc_bound
                val = utc_bound(val)
            where += f" AND {col}{op}?"; args.append(val)
    joins = """FROM admin_audit a LEFT JOIN users u ON u.id=a.actor_user_id
        LEFT JOIN access_groups g ON g.id=a.group_id
        LEFT JOIN reports r ON a.target_type='report' AND r.id=a.target_id
        LEFT JOIN access_requests ar ON ar.id=CASE WHEN a.target_type='request' AND a.target_id ~ '^[0-9]+$' THEN a.target_id::bigint END
        LEFT JOIN users tu ON tu.id=CASE
            WHEN a.target_type='member' AND a.target_id ~ '^[0-9]+:[0-9]+$' THEN split_part(a.target_id,':',2)::bigint
            WHEN a.target_type='user' AND a.target_id ~ '^[0-9]+$' THEN a.target_id::bigint
            WHEN a.target_type='request' THEN ar.user_id END"""
    from app.user_profile import scoped_name
    actor_name=scoped_name(scope,'u')
    target_name=scoped_name(scope,'tu')
    if not scope.admin:
        actor_name="CASE WHEN a.target_type='request' AND ar.user_id=u.id THEN u.full_name ELSE ("+actor_name+") END"
        target_name="CASE WHEN a.target_type='request' THEN tu.full_name ELSE ("+target_name+") END"
    if author:
        where += ' AND (u.username ILIKE ? OR ('+actor_name+') ILIKE ?)'; args.extend(['%'+author+'%']*2)
    if object_query:
        where += " AND concat_ws(' ',g.name,r.name,tu.username,("+target_name+")) ILIKE ?"; args.append('%'+object_query+'%')
    with db.get_conn() as conn:
        total = int(conn.execute("SELECT COUNT(*) AS n " + joins + " WHERE " + where, args).fetchone()["n"])
        rows = conn.execute("SELECT a.*,u.username AS actor_username,"+actor_name+" AS actor_full_name,g.name AS group_name,r.name AS report_name,tu.username AS target_username,"+target_name+" AS target_full_name " + joins + " WHERE " + where +
            " ORDER BY a.created_at DESC,a.id DESC LIMIT ? OFFSET ?", [*args, limit, offset]).fetchall()
    from app.audit_view import describe
    rows = [describe(row) for row in rows]
    return {"rows": rows, "total": total, "has_more": offset + len(rows) < total}


def csv_export(rows):
    result = io.StringIO()
    writer = csv.writer(result)
    columns = ["event_at", "level", "service", "username", "report_id", "job_id", "request_id", "event_type", "message", "traceback"]
    writer.writerow(columns)
    for row in rows:
        writer.writerow([("'" + str(row.get(c, ""))) if str(row.get(c, "")).startswith(("=", "+", "-", "@"))
                         else row.get(c, "") for c in columns])
    return "\ufeff" + result.getvalue()
