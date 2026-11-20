"""Права доступа, календарные лимиты и аудит изменений."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import HTTPException

from app import db
from app.config import sso_admin_role

MOSCOW = ZoneInfo("Europe/Moscow")
LIMIT_USAGE_CACHE_TTL_SEC = max(1, min(10, int(os.getenv("LIMIT_USAGE_CACHE_TTL_SEC", "2"))))


def init_schema(conn):
    conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations(name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)")
    conn.execute("""CREATE TABLE IF NOT EXISTS access_groups (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name TEXT NOT NULL UNIQUE,
        description TEXT NOT NULL DEFAULT '', token_limit BIGINT CHECK(token_limit >= 0),
        limit_period TEXT NOT NULL DEFAULT 'month' CHECK(limit_period IN ('month','none')),
        is_active BOOLEAN NOT NULL DEFAULT TRUE, is_system BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL, created_by BIGINT)""")
    conn.execute("""CREATE TABLE IF NOT EXISTS access_group_members (
        group_id BIGINT NOT NULL REFERENCES access_groups(id), user_id BIGINT NOT NULL REFERENCES users(id),
        role_in_group TEXT NOT NULL DEFAULT 'member' CHECK(role_in_group IN ('member','manager')),
        limit_mode TEXT NOT NULL DEFAULT 'group' CHECK(limit_mode IN ('group','personal')),
        personal_token_limit BIGINT CHECK(personal_token_limit >= 0), added_at TEXT NOT NULL, added_by BIGINT,
        PRIMARY KEY(group_id,user_id), CHECK(limit_mode <> 'personal' OR personal_token_limit IS NOT NULL))""")
    conn.execute("""CREATE TABLE IF NOT EXISTS access_requests (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id),
        requested_group_id BIGINT REFERENCES access_groups(id), purpose TEXT NOT NULL CHECK(length(trim(purpose)) > 0),
        instructions_ack BOOLEAN NOT NULL CHECK(instructions_ack),
        status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','approved','rejected','canceled')),
        decided_by BIGINT, decided_at TEXT, decision_comment TEXT, created_at TEXT NOT NULL)""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_access_request_pending ON access_requests(user_id) WHERE status='pending'")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_access_members_user ON access_group_members(user_id,group_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_group_created ON reports(access_group_id,created_at)")
    conn.execute("""CREATE TABLE IF NOT EXISTS admin_audit (
        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, actor_user_id BIGINT,
        action TEXT NOT NULL, target_type TEXT NOT NULL, target_id TEXT NOT NULL,
        before_json TEXT, after_json TEXT, created_at TEXT NOT NULL)""")
    if not conn.execute("SELECT name FROM schema_migrations WHERE name='access_groups_v1'").fetchone():
        now = db._now_iso()
        guest = conn.execute("""INSERT INTO access_groups(name,is_system,created_at,updated_at)
            VALUES ('Гость',TRUE,?,?) ON CONFLICT(name) DO UPDATE SET is_system=TRUE RETURNING id""", (now, now)).fetchone()
        gid = guest["id"]
        conn.execute("""INSERT INTO access_group_members(group_id,user_id,added_at)
            SELECT ?,id,? FROM users ON CONFLICT DO NOTHING""", (gid, now))
        conn.execute("UPDATE reports SET access_group_id=? WHERE access_group_id IS NULL", (gid,))
        conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES ('access_groups_v1',?)", (now,))


def audit(conn, actor_id, action, target_type, target_id, before=None, after=None):
    from app.audit_view import has_changes
    if not has_changes(before, after):
        return
    source = after or before or {}
    group_id = int(target_id) if target_type == "group" else int(str(target_id).split(":")[0]) if target_type == "member" else (
        source.get("requested_group_id") if target_type == "request" else source.get("access_group_id"))
    conn.execute("""INSERT INTO admin_audit(actor_user_id,action,target_type,target_id,before_json,after_json,created_at,group_id)
        VALUES (?,?,?,?,?,?,?,?)""", (actor_id, action, target_type, str(target_id),
        json.dumps(before, ensure_ascii=False, default=str) if before is not None else None,
        json.dumps(after, ensure_ascii=False, default=str) if after is not None else None, db._now_iso(), group_id))


def actor(user):
    current = db.get_user_by_id(int(user["id"]))
    if not current:
        raise HTTPException(401, "Сессия недействительна")
    return current


def role_from_env(user):
    return sso_admin_role(user.get("sso_provider"), user.get("sso_subject"), user.get("sso_login_name"))


def has_access(user):
    with db.get_conn() as conn:
        row = conn.execute("""SELECT u.role, EXISTS(SELECT 1 FROM access_group_members m
            JOIN access_groups g ON g.id=m.group_id WHERE m.user_id=u.id AND g.is_active) AS member
            FROM users u WHERE u.id=?""", (user["id"],)).fetchone()
    return bool(row and (row["role"] == "admin" or row["member"]))


def require_group_manager(group_id, user, conn=None):
    if conn is None:
        with db.get_conn() as connection:
            return require_group_manager(group_id, user, connection)
    role = conn.execute("SELECT role FROM users WHERE id=?", (user["id"],)).fetchone()
    if role and role["role"] == "admin":
        return "admin"
    row = conn.execute("""SELECT m.role_in_group FROM access_group_members m JOIN access_groups g ON g.id=m.group_id
        WHERE m.group_id=? AND m.user_id=? AND g.is_active""", (group_id, user["id"])).fetchone()
    if not row or row["role_in_group"] != "manager":
        raise HTTPException(403, {"code": "forbidden_group", "message": "Нет прав управления этой группой"})
    return "manager"


def period_bounds(period, now=None):
    if period == "none":
        return None, None
    local = (now or datetime.now(timezone.utc)).astimezone(MOSCOW)
    start = local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    return start.astimezone(timezone.utc).isoformat(), end.astimezone(timezone.utc).isoformat()


def utc_bound(value):
    """Внешняя граница периода: дата/наивное время трактуются по Москве."""
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=MOSCOW)
        return moment.astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        raise HTTPException(422, "Некорректная дата периода")


def previous_bounds(period, now=None):
    start, _ = period_bounds(period, now)
    if start is None:
        return None, None
    from datetime import timedelta
    return period_bounds(period, datetime.fromisoformat(start) - timedelta(seconds=1))


def usage_for(conn, group, user_id=None, *, previous=False, report_created_at=None, cached=False):
    now = datetime.fromisoformat(report_created_at) if report_created_at else None
    start, end = previous_bounds(group["limit_period"], now) if previous else period_bounds(group["limit_period"], now)
    client = db.get_redis_client() if cached else None
    cache_key = None
    if client is not None:
        try:
            version = client.get(f"quota:version:{group['id']}") or b"0"
            cache_key = f"quota:usage:{group['id']}:{version!s}:{start}:{user_id}"
            raw = client.get(cache_key)
            if raw:
                return int(raw), end
        except Exception:
            cache_key = None
    where = "access_group_id=?"
    args = [group["id"]]
    if user_id is not None:
        where += " AND user_id=?"
        args.append(user_id)
    if start:
        # created_at записывается в UTC. Фильтр по ISO-строке использует индекс.
        where += " AND created_at>=? AND created_at<?"
        args.extend([start, end])
    used = int(conn.execute("SELECT COALESCE(SUM(total_tokens),0) AS used FROM reports WHERE " + where, args).fetchone()["used"])
    if cache_key:
        try:
            client.setex(cache_key, LIMIT_USAGE_CACHE_TTL_SEC, str(used))
        except Exception:
            pass
    return used, end


def remaining(group_limit, group_used, mode="group", personal_limit=None, personal_used=0):
    group_remaining = None if group_limit is None else int(group_limit) - group_used
    personal_remaining = None if mode != "personal" else int(personal_limit) - personal_used
    bounded = [v for v in (group_remaining, personal_remaining) if v is not None]
    return {"group_remaining": group_remaining, "personal_remaining": personal_remaining,
            "remaining": min(bounded) if bounded else None}


def validate_member(group, data, before=None, *, manager=False, actor_id=None, target_id=None):
    before = before or {}
    role = data.get("role_in_group", before.get("role_in_group", "member"))
    mode = data.get("limit_mode", before.get("limit_mode", "group"))
    limit = data.get("personal_token_limit", before.get("personal_token_limit"))
    if role not in {"member", "manager"} or mode not in {"group", "personal"}:
        raise HTTPException(422, "Некорректная роль или режим лимита")
    if manager and (actor_id == target_id or before.get("role_in_group") == "manager" or role != "member"):
        raise HTTPException(403, "Руководитель не может изменять себя или других руководителей")
    if mode == "personal":
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 or limit > 9223372036854775807:
            raise HTTPException(422, "Личный лимит должен быть целым неотрицательным числом")
        if group["token_limit"] is not None and limit > group["token_limit"]:
            raise HTTPException(422, "Личный лимит не может превышать лимит группы")
    else:
        limit = None
    return {"role_in_group": role, "limit_mode": mode, "personal_token_limit": limit}


def group_usage(conn, group, user_id=None, member=None):
    used, reset = usage_for(conn, group)
    previous = usage_for(conn, group, previous=True)[0] if group["limit_period"] == "month" else 0
    personal_used, _ = usage_for(conn, group, user_id) if user_id is not None else (0, None)
    member = member or {}
    return {**dict(group), **member, "used": used, "previous_used": previous, "personal_used": personal_used,
            "reset_at": reset, **remaining(group["token_limit"], used, member.get("limit_mode", "group"),
                                          member.get("personal_token_limit"), personal_used)}


def status(user):
    current = actor(user)
    with db.get_conn() as conn:
        rows = conn.execute("""SELECT g.*,m.role_in_group,m.limit_mode,m.personal_token_limit
            FROM access_group_members m JOIN access_groups g ON g.id=m.group_id
            WHERE m.user_id=? AND g.is_active ORDER BY g.name""", (user["id"],)).fetchall()
        groups = [group_usage(conn, row, user["id"], {k: row[k] for k in ("role_in_group", "limit_mode", "personal_token_limit")}) for row in rows]
        requests = conn.execute("""SELECT ar.*,g.name AS requested_group_name FROM access_requests ar
            LEFT JOIN access_groups g ON g.id=ar.requested_group_id
            WHERE ar.user_id=? ORDER BY ar.id DESC LIMIT 20""", (user["id"],)).fetchall()
        last = conn.execute("SELECT access_group_id FROM reports WHERE user_id=? ORDER BY created_at DESC LIMIT 1", (user["id"],)).fetchone()
    return {"has_access": current["role"] == "admin" or bool(groups), "role": current["role"],
            "groups": groups, "requests": requests, "last_group_id": last["access_group_id"] if last else None,
            "default_group_id": default_group(groups, last["access_group_id"] if last else None),
            "is_manager": any(g["role_in_group"] == "manager" for g in groups)}


def default_group(groups, last_id):
    available = [g for g in groups if g.get('remaining') is None or g['remaining'] > 0]
    return next((g['id'] for g in available if g['id'] == last_id), available[0]['id'] if available else None)


def request_groups(user):
    with db.get_conn() as conn:
        return conn.execute('''SELECT g.id,g.name,g.description FROM access_groups g WHERE g.is_active
            AND NOT EXISTS(SELECT 1 FROM access_group_members m WHERE m.group_id=g.id AND m.user_id=?)
            ORDER BY g.name''', (user['id'],)).fetchall()


def pick_group(user, requested=None):
    current = actor(user)
    with db.get_conn() as conn:
        rows = conn.execute("""SELECT g.id FROM access_group_members m JOIN access_groups g ON g.id=m.group_id
            WHERE m.user_id=? AND g.is_active ORDER BY m.added_at""", (user["id"],)).fetchall()
        ids = [r["id"] for r in rows]
        if requested is None:
            last = conn.execute("SELECT access_group_id FROM reports WHERE user_id=? ORDER BY created_at DESC LIMIT 1", (user["id"],)).fetchone()
            requested = last["access_group_id"] if last and last["access_group_id"] in ids else (ids[0] if ids else None)
    if requested not in ids and not (requested is None and current["role"] == "admin"):
        raise HTTPException(403, {"code": "no_access", "message": "Вы не состоите в выбранной активной группе"})
    return requested


class LimitExhausted(HTTPException):
    def __init__(self, kind, used, limit, reset_at, group=None):
        reset_text = (
            "Сброс: " + datetime.fromisoformat(reset_at).astimezone(MOSCOW).strftime("%d.%m.%Y %H:%M МСК")
            if reset_at else "Без автоматического сброса"
        )
        message = f"Исчерпан {kind} лимит токенов: {used} из {limit}. " + reset_text
        if group:
            message = f'Квота «{group["name"]}»: ' + message
        detail = {"code": "limit_exhausted", "message": message,
                  "kind": kind, "used": used, "limit": limit, "reset_at": reset_at}
        if group:
            detail.update(group_id=group['id'], group_name=group['name'])
        super().__init__(409, detail)


def check_limit(user_id, group_id, *, created_at=None, return_remaining=False, fresh=False):
    if group_id is None:
        if actor({"id": user_id})["role"] != "admin":
            raise HTTPException(403, {"code": "no_access", "message": "Не выбрана группа доступа"})
        return None
    with db.get_conn() as conn:
        group = conn.execute("SELECT * FROM access_groups WHERE id=?", (group_id,)).fetchone()
        member = conn.execute("SELECT * FROM access_group_members WHERE group_id=? AND user_id=?", (group_id, user_id)).fetchone()
        if not group or not group["is_active"] or not member:
            raise HTTPException(403, {"code": "no_access", "message": "Доступ к группе отключён"})
        used, reset = usage_for(conn, group, report_created_at=created_at, cached=not fresh)
        personal_used = usage_for(conn, group, user_id, report_created_at=created_at, cached=not fresh)[0] if member["limit_mode"] == "personal" else 0
        limits = remaining(group["token_limit"], used, member["limit_mode"], member["personal_token_limit"], personal_used)
        if limits["group_remaining"] is not None and limits["group_remaining"] <= 0:
            raise LimitExhausted("групповой", used, group["token_limit"], reset, group)
        if limits["personal_remaining"] is not None and limits["personal_remaining"] <= 0:
            raise LimitExhausted("личный", personal_used, member["personal_token_limit"], reset, group)
        return limits["remaining"] if return_remaining else bool(group["token_limit"] is not None or member["limit_mode"] == "personal")


def check_report_limit(row, *, return_remaining=False, fresh=False):
    return check_limit(int(row["user_id"]), row.get("access_group_id"), created_at=row["created_at"], return_remaining=return_remaining, fresh=fresh)



def managed_groups(user):
    current = actor(user)
    with db.get_conn() as conn:
        if current["role"] == "admin":
            rows = conn.execute("""SELECT g.*,(SELECT COUNT(*) FROM access_group_members m WHERE m.group_id=g.id) AS member_count
                FROM access_groups g ORDER BY name""").fetchall()
        else:
            rows = conn.execute("""SELECT g.*,(SELECT COUNT(*) FROM access_group_members a WHERE a.group_id=g.id) AS member_count
                FROM access_groups g JOIN access_group_members m ON m.group_id=g.id
                WHERE m.user_id=? AND m.role_in_group='manager' AND g.is_active ORDER BY g.name""", (user["id"],)).fetchall()
        return [group_usage(conn, g) for g in rows]


def save_group(user, data, group_id=None):
    current = actor(user)
    if current["role"] != "admin":
        raise HTTPException(403, "Требуются права администратора")
    with db.get_conn() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(2026110803)")
        before = conn.execute("SELECT * FROM access_groups WHERE id=? FOR UPDATE", (group_id,)).fetchone() if group_id else None
        if group_id and not before:
            raise HTTPException(404, "Группа не найдена")
        merged = {"name": "", "description": "", "token_limit": None, "limit_period": "month", "is_active": True,
                  **(dict(before) if before else {}), **{k: v for k, v in data.items() if k in {"name", "description", "token_limit", "limit_period", "is_active"}}}
        name = str(merged["name"]).strip()
        limit = merged["token_limit"]
        if not name or len(name) > 120 or merged["limit_period"] not in {"month", "none"}:
            raise HTTPException(422, "Проверьте имя группы и период")
        if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 or limit > 9223372036854775807):
            raise HTTPException(422, "Лимит должен быть целым неотрицательным числом")
        if not isinstance(merged["is_active"], bool):
            raise HTTPException(422, "is_active должен быть логическим значением")
        if before and before["is_system"] and name != before["name"]:
            raise HTTPException(422, "Имя системной группы изменять нельзя")
        duplicate = conn.execute("SELECT id FROM access_groups WHERE name=? AND id<>?", (name, group_id or 0)).fetchone()
        if duplicate:
            raise HTTPException(409, "Группа с таким именем уже существует")
        if limit is not None and group_id:
            too_high = conn.execute("""SELECT user_id FROM access_group_members WHERE group_id=? AND limit_mode='personal'
                AND personal_token_limit>? LIMIT 1""", (group_id, limit)).fetchone()
            if too_high:
                raise HTTPException(422, "Сначала уменьшите личные лимиты участников")
        now = db._now_iso()
        if before:
            row = conn.execute("""UPDATE access_groups SET name=?,description=?,token_limit=?,limit_period=?,is_active=?,updated_at=?
                WHERE id=? RETURNING *""", (name, str(merged["description"]), limit, merged["limit_period"], merged["is_active"], now, group_id)).fetchone()
        else:
            row = conn.execute("""INSERT INTO access_groups(name,description,token_limit,limit_period,is_active,created_at,updated_at,created_by)
                VALUES (?,?,?,?,?,?,?,?) RETURNING *""", (name, str(merged["description"]), limit, merged["limit_period"], merged["is_active"], now, now, user["id"])).fetchone()
        audit(conn, user["id"], "group.update" if before else "group.create", "group", row["id"], before, row)
        return group_usage(conn, row)


def members(user, group_id):
    with db.get_conn() as conn:
        profile_scope = require_group_manager(group_id, user, conn)
        from app.user_profile import columns
        group = conn.execute("SELECT * FROM access_groups WHERE id=?", (group_id,)).fetchone()
        if not group:
            raise HTTPException(404, "Группа не найдена")
        rows = conn.execute("""SELECT m.*,u.username,u.role,""" + columns(profile_scope == "admin") + """ FROM access_group_members m JOIN users u ON u.id=m.user_id
            WHERE m.group_id=? ORDER BY u.username""", (group_id,)).fetchall()
        return [{**dict(m), "used": usage_for(conn, group, m["user_id"])[0]} for m in rows]


def save_member(user, group_id, user_id, data=None, *, remove=False, exact_login=False):
    with db.get_conn() as conn:
        scope = require_group_manager(group_id, user, conn)
        group = conn.execute("SELECT * FROM access_groups WHERE id=? FOR UPDATE", (group_id,)).fetchone()
        if not group:
            raise HTTPException(404, "Группа не найдена")
        before = conn.execute("SELECT * FROM access_group_members WHERE group_id=? AND user_id=? FOR UPDATE", (group_id, user_id)).fetchone()
        if scope != "admin" and not before and not exact_login:
            raise HTTPException(403, "Нового участника можно добавить только по точному логину")
        if not conn.execute("SELECT id FROM users WHERE id=?", (user_id,)).fetchone():
            raise HTTPException(404, "Пользователь не найден")
        values = validate_member(group, data or {}, before, manager=scope != "admin", actor_id=user["id"], target_id=user_id)
        if remove:
            conn.execute("DELETE FROM access_group_members WHERE group_id=? AND user_id=?", (group_id, user_id))
            row = None
        else:
            row = conn.execute("""INSERT INTO access_group_members(group_id,user_id,role_in_group,limit_mode,personal_token_limit,added_at,added_by)
                VALUES (?,?,?,?,?,?,?) ON CONFLICT(group_id,user_id) DO UPDATE SET role_in_group=EXCLUDED.role_in_group,
                limit_mode=EXCLUDED.limit_mode,personal_token_limit=EXCLUDED.personal_token_limit RETURNING *""",
                (group_id, user_id, values["role_in_group"], values["limit_mode"], values["personal_token_limit"], db._now_iso(), user["id"])).fetchone()
        audit(conn, user["id"], "member.remove" if remove else "member.save", "member", f"{group_id}:{user_id}", before, row)
        return row


def search_users(user, query):
    from app.governance import require_management
    scope = require_management(user)
    with db.get_conn() as conn:
        where, args = scope.sql("m.group_id")
        return conn.execute("""SELECT DISTINCT u.id,u.username,u.full_name FROM users u
            LEFT JOIN access_group_members m ON m.user_id=u.id WHERE """ + where +
            " AND (u.username ILIKE ? OR u.full_name ILIKE ?) ORDER BY u.username LIMIT 30", [*args, "%" + query + "%", "%" + query + "%"]).fetchall()


def add_by_login(user, group_id, data):
    # Проверяем scope до поиска: чужая группа не позволяет проверять существование логина.
    require_group_manager(group_id, user)
    login = str(data.get("username") or "").strip()
    if not login or len(login) > 120:
        raise HTTPException(422, "Укажите точный логин")
    with db.get_conn() as conn:
        target = conn.execute("SELECT id FROM users WHERE lower(username)=lower(?)", (login,)).fetchone()
    if not target:
        raise HTTPException(404, "Логин не найден. Пользователю нужно сначала войти в сервис")
    return save_member(user, group_id, target["id"], {"role_in_group": "member"}, exact_login=True)


def save_role(user, target_id, role):
    if actor(user)["role"] != "admin":
        raise HTTPException(403, "Требуются права администратора")
    if role not in {"admin", "user"}:
        raise HTTPException(422, "Неизвестная роль")
    with db.get_conn() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(2026110802)")
        current = conn.execute("SELECT role FROM users WHERE id=?", (user["id"],)).fetchone()
        if not current or current["role"] != "admin":
            raise HTTPException(403, "Требуются права администратора")
        before = conn.execute(
            "SELECT id,username,role,sso_provider,sso_subject,sso_login_name FROM users WHERE id=? FOR UPDATE",
            (target_id,),
        ).fetchone()
        if not before:
            raise HTTPException(404, "Пользователь не найден")
        pinned = role_from_env(before)
        if role == "user":
            if target_id == int(user["id"]):
                raise HTTPException(403, "Самопонижение запрещено")
            if pinned:
                raise HTTPException(403, "Роль закреплена в настройках SSO")
            if before["role"] == "admin" and conn.execute("SELECT COUNT(*) AS n FROM users WHERE role='admin'").fetchone()["n"] <= 1:
                raise HTTPException(403, "Нельзя понизить последнего администратора")
        row = conn.execute("UPDATE users SET role=? WHERE id=? RETURNING id,username,role", (role, target_id)).fetchone()
        audit(conn, user["id"], "user.role", "user", target_id, {key: before[key] for key in row}, row)
        return {**dict(row), "role_from_env": pinned}


def request_access(user, data):
    purpose = str(data.get("purpose") or "").strip()
    group_id = data.get("requested_group_id")
    if not purpose or len(purpose) > 4000 or data.get("instructions_ack") is not True:
        raise HTTPException(422, "Укажите назначение и подтвердите ознакомление с инструкцией")
    with db.get_conn() as conn:
        conn.execute("SELECT id FROM users WHERE id=? FOR UPDATE", (user["id"],))
        if conn.execute("SELECT id FROM access_requests WHERE user_id=? AND status='pending'", (user["id"],)).fetchone():
            raise HTTPException(409, "У вас уже есть ожидающая заявка")
        if group_id is not None and not conn.execute("SELECT id FROM access_groups WHERE id=? AND is_active", (group_id,)).fetchone():
            raise HTTPException(422, "Выберите активную группу")
        if group_id is not None and conn.execute('SELECT 1 FROM access_group_members WHERE group_id=? AND user_id=?', (group_id,user['id'])).fetchone():
            raise HTTPException(409, 'Вы уже состоите в этой группе. Выберите другую группу')
        row = conn.execute("""INSERT INTO access_requests(user_id,requested_group_id,purpose,instructions_ack,created_at)
            VALUES (?,?,?,TRUE,?) RETURNING *""", (user["id"], group_id, purpose, db._now_iso())).fetchone()
        audit(conn, user["id"], "request.create", "request", row["id"], None, row)
        return row


def requests_for(user):
    current = actor(user)
    with db.get_conn() as conn:
        where = "TRUE" if current["role"] == "admin" else """r.requested_group_id IN
            (SELECT m.group_id FROM access_group_members m JOIN access_groups g ON g.id=m.group_id
             WHERE m.user_id=? AND m.role_in_group='manager' AND g.is_active)"""
        from app.user_profile import columns
        return conn.execute("SELECT r.*,u.username,d.username AS decided_username," + columns(current["role"] == "admin") + " FROM access_requests r JOIN users u ON u.id=r.user_id LEFT JOIN users d ON d.id=r.decided_by WHERE NOT u.is_test_fixture AND " + where +
                            " ORDER BY CASE WHEN r.status='pending' THEN 0 ELSE 1 END,r.id DESC LIMIT 1000", () if current["role"] == "admin" else (user["id"],)).fetchall()


def decide_request(user, request_id, data, *, cancel=False):
    with db.get_conn() as conn:
        before = conn.execute("SELECT * FROM access_requests WHERE id=? FOR UPDATE", (request_id,)).fetchone()
        if not before:
            raise HTTPException(404, "Заявка не найдена")
        group_id = before["requested_group_id"]
        if cancel:
            if int(before["user_id"]) != int(user["id"]):
                raise HTTPException(403, "Это чужая заявка")
            decision = "canceled"
        else:
            current = actor(user)
            if current["role"] != "admin":
                if group_id is None:
                    raise HTTPException(403, "Заявка без группы доступна только администратору")
                require_group_manager(group_id, user, conn)
                if data.get("group_id", group_id) != group_id:
                    raise HTTPException(403, "Нельзя перенаправить заявку в другую группу")
            group_id = data.get("group_id", group_id)
            decision = data.get("status")
            if decision not in {"approved", "rejected"}:
                raise HTTPException(422, "Неизвестное решение")
        if before["status"] != "pending":
            raise HTTPException(409, "Заявка уже рассмотрена")
        if decision == "approved":
            group = conn.execute("SELECT id FROM access_groups WHERE id=? AND is_active FOR UPDATE", (group_id,)).fetchone()
            if not group:
                raise HTTPException(422, "Для одобрения выберите активную группу")
            membership = conn.execute("SELECT * FROM access_group_members WHERE group_id=? AND user_id=?", (group_id, before["user_id"])).fetchone()
            row = conn.execute("""INSERT INTO access_group_members(group_id,user_id,added_at,added_by)
                VALUES (?,?,?,?) ON CONFLICT DO NOTHING RETURNING *""", (group_id, before["user_id"], db._now_iso(), user["id"])).fetchone()
            if row:
                audit(conn, user["id"], "member.add", "member", f"{group_id}:{before['user_id']}", membership, row)
        row = conn.execute("""UPDATE access_requests SET status=?,decided_by=?,decided_at=?,decision_comment=?
            WHERE id=? RETURNING *""", (decision, user["id"], db._now_iso(), str(data.get("decision_comment") or "")[:4000], request_id)).fetchone()
        audit(conn, user["id"], "request." + decision, "request", request_id, before, row)
        return row
