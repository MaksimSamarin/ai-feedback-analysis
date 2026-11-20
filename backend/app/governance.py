"""Единый scope управления группами и безопасная выдача метаданных."""
from dataclasses import dataclass

from fastapi import HTTPException

from app import db


@dataclass(frozen=True)
class Scope:
    admin: bool
    groups: tuple[int, ...]

    def require_group(self, group_id):
        if not self.admin and group_id not in self.groups:
            raise HTTPException(403, {"code": "forbidden_group", "message": "Нет доступа к этой группе"})

    def sql(self, column="r.access_group_id"):
        if self.admin:
            return "TRUE", []
        if not self.groups:
            return "FALSE", []
        return column + " IN (" + ",".join("?" for _ in self.groups) + ")", list(self.groups)


def scope_for(user, conn=None):
    if conn is None:
        with db.get_conn() as connection:
            return scope_for(user, connection)
    row = conn.execute("SELECT role FROM users WHERE id=?", (user["id"],)).fetchone()
    if row and row["role"] == "admin":
        return Scope(True, ())
    groups = conn.execute("""SELECT m.group_id FROM access_group_members m
        JOIN access_groups g ON g.id=m.group_id WHERE m.user_id=?
        AND m.role_in_group='manager' AND g.is_active ORDER BY m.group_id""", (user["id"],)).fetchall()
    return Scope(False, tuple(int(r["group_id"]) for r in groups))


def require_management(user):
    scope = scope_for(user)
    if not scope.admin and not scope.groups:
        raise HTTPException(403, "Требуются права руководителя группы")
    return scope


def effective_role(global_role, memberships):
    if global_role == "admin":
        return "admin"
    if any(m["role_in_group"] == "manager" and m.get("is_active", True) for m in memberships):
        return "manager"
    return "user"


def metadata_reports(user, *, group_id=None, user_id=None, report_id=None, status=None,
                     offset=0, limit=50, include_deleted=True, include_test=True,
                     date_from=None, date_to=None, search=None, cursor=None, count_total=True):
    scope = require_management(user)
    if group_id is not None:
        scope.require_group(group_id)
    where, args = scope.sql()
    if group_id == 0:
        where += " AND r.access_group_id IS NULL"
    for column, value in (("r.access_group_id", group_id), ("r.user_id", user_id), ("r.id", report_id)):
        if value is not None:
            if column == "r.access_group_id" and value == 0:
                continue
            where += f" AND {column}=?"
            args.append(value)
    if status=='failed':
        where+=" AND (r.status='failed' OR (r.status='completed' AND r.processed_rows>0 AND r.failed_rows>=r.processed_rows))"
    elif status=='errors':where+=" AND (r.status='failed' OR r.failed_rows>0)"
    elif status:
        where+=' AND r.status=?';args.append(status)
    if not include_deleted:
        where += " AND r.deleted_at IS NULL"
    if not include_test:
        where += " AND r.user_id IN (SELECT id FROM users WHERE NOT is_test_fixture)"
    if search:
        from app.user_profile import scoped_name
        where += " AND (r.user_id IN (SELECT fu.id FROM users fu WHERE fu.username ILIKE ? OR ("+scoped_name(scope,'fu')+") ILIKE ?) OR r.name ILIKE ? OR r.id ILIKE ?)"
        args.extend(["%"+search+"%"]*4)
    from app.access import utc_bound
    for value, operator in ((date_from, ">="), (date_to, "<")):
        if value:
            where += f" AND r.created_at{operator}?"
            args.append(utc_bound(value))
    with db.get_conn() as conn:
        # История группы сохраняется и после исключения участника. Scope задаёт
        # группа списания отчёта; чужой user_id сам по себе не раскрывает данных.
        total = int(conn.execute("SELECT COUNT(*) AS n FROM reports r WHERE " + where, args).fetchone()["n"]) if count_total else None
        if cursor:
            where += ' AND (r.created_at<? OR (r.created_at=? AND r.id>?))'
            args.extend([cursor[0],cursor[0],cursor[1]])
        from app.user_profile import columns
        rows = conn.execute("""SELECT r.id,r.name,r.user_id,u.username,r.access_group_id,g.name AS group_name,
            """+columns(scope.admin,groups=scope.groups)+""",
            r.status,r.provider,r.model,r.analysis_mode,r.total_rows,r.processed_rows,r.groups_total,r.groups_processed,
            r.created_at,r.updated_at,r.finished_at,r.deleted_at,r.prompt_tokens,r.completion_tokens,r.total_tokens,
            r.success_rows,r.failed_rows,
            CASE WHEN r.error_text IS NOT NULL AND r.error_text<>'' THEN 'report_error' END AS error_code
            FROM reports r JOIN users u ON u.id=r.user_id LEFT JOIN access_groups g ON g.id=r.access_group_id
            WHERE """ + where + " ORDER BY r.created_at DESC,r.id LIMIT ?" + (" OFFSET ?" if count_total else ""),
            [*args,limit+1,*([offset] if count_total else [])]).fetchall()
    more = len(rows)>limit
    rows = rows[:limit]
    if report_id is not None and not rows:
        raise HTTPException(404, "Отчёт не найден")
    if not scope.admin:
        for row in rows:
            for column in ("provider", "model", "analysis_mode"):
                row.pop(column, None)
    from app.report_outcome import presentation
    for row in rows:
        row.update(presentation(row["status"], row["processed_rows"], row.get("failed_rows")))
    return {"rows": rows, "total": total, "offset": offset, "limit": limit,
            "has_more": offset + len(rows) < total if count_total else more,
            "next_cursor": [rows[-1]['created_at'],rows[-1]['id']] if rows else None}


def assign_role(actor, target_id, role, group_ids):
    from app.access import audit, role_from_env
    if role not in {"user", "manager", "admin"}:
        raise HTTPException(422, "Неизвестная роль")
    if not isinstance(group_ids, list) or any(isinstance(g, bool) or not isinstance(g, int) for g in group_ids):
        raise HTTPException(422, "Некорректные группы")
    groups = sorted(set(group_ids)) if role == "manager" else []
    if role == "manager" and not groups:
        raise HTTPException(422, "Выберите хотя бы одну группу для руководителя")
    if any(isinstance(g, bool) or not isinstance(g, int) for g in groups):
        raise HTTPException(422, "Некорректные группы")
    with db.get_conn() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(2026110802)")
        if not scope_for(actor, conn).admin:
            raise HTTPException(403, "Требуются права администратора")
        before = conn.execute(
            "SELECT id,username,role,sso_provider,sso_subject,sso_login_name FROM users WHERE id=? FOR UPDATE",
            (target_id,),
        ).fetchone()
        if not before:
            raise HTTPException(404, "Пользователь не найден")
        global_role = "admin" if role == "admin" else "user"
        if global_role != "admin" and before["role"] == "admin":
            if target_id == int(actor["id"]):
                raise HTTPException(403, "Самопонижение запрещено")
            if role_from_env(before):
                raise HTTPException(403, "Роль закреплена в настройках SSO")
            if conn.execute("SELECT COUNT(*) AS n FROM users WHERE role='admin'").fetchone()["n"] <= 1:
                raise HTTPException(403, "Нельзя понизить последнего администратора")
        for gid in groups:
            if not conn.execute("SELECT id FROM access_groups WHERE id=? AND is_active FOR UPDATE", (gid,)).fetchone():
                raise HTTPException(422, "Выберите активные группы")
        conn.execute("UPDATE users SET role=? WHERE id=?", (global_role, target_id))
        if before["role"] != global_role:
            previous = {key: before[key] for key in ("id", "username", "role")}
            audit(conn, actor["id"], "user.role", "user", target_id, previous, {**previous, "role": global_role})
        old = conn.execute("SELECT * FROM access_group_members WHERE user_id=? FOR UPDATE", (target_id,)).fetchall()
        old_by_group = {r["group_id"]: r for r in old}
        for gid in set(old_by_group) | set(groups):
            previous = old_by_group.get(gid)
            desired = "manager" if gid in groups else "member"
            if previous and previous["role_in_group"] == desired:
                continue
            if not previous and desired != "manager":
                continue
            row = conn.execute("""INSERT INTO access_group_members(group_id,user_id,role_in_group,added_at,added_by)
                VALUES (?,?,?,?,?) ON CONFLICT(group_id,user_id) DO UPDATE SET role_in_group=EXCLUDED.role_in_group
                RETURNING *""", (gid, target_id, desired, db._now_iso(), actor["id"])).fetchone()
            audit(conn, actor["id"], "member.role", "member", f"{gid}:{target_id}", previous, row)
    return {"id": target_id, "role": global_role, "effective_role": role, "managed_group_ids": groups}
