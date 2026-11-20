"""Маршруты управления доступом; авторизация задаётся основным приложением."""
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app import access, db, governance, events, reporting, model_catalog, user_profile
from fastapi.responses import Response


class UISettings(BaseModel):
    language: Literal["en", "ru"]


def router(get_current_user, require_admin, require_access):
    api = APIRouter(prefix="/api")

    @api.get("/ui/settings", response_model=UISettings)
    def ui_settings(response: Response):
        response.headers["Cache-Control"] = "no-store"
        return {"language": db.get_ui_language()}

    @api.put("/admin/settings", response_model=UISettings)
    def save_ui_settings(settings: UISettings, user=Depends(require_admin)):
        db.set_ui_language(settings.language)
        return settings

    def management_user(user=Depends(require_access)):
        governance.require_management(user)
        return user

    @api.get("/admin/models")
    def model_list(user=Depends(require_admin)):
        return model_catalog.all_models(fresh=True)

    @api.post("/admin/models")
    def model_create(data: dict, user=Depends(require_admin)):
        return model_catalog.save(user,data)

    @api.post("/admin/models/batch")
    async def model_batch(data: dict, user=Depends(require_admin)):
        return await model_catalog.add_batch(user,data)

    @api.get("/admin/models/catalog")
    async def model_directory(provider: str = "openai", user=Depends(require_admin)):
        return await model_catalog.catalog(provider)

    @api.patch("/admin/models/{model_id}")
    def model_update(model_id: int, data: dict, user=Depends(require_admin)):
        return model_catalog.save(user,data,model_id)

    @api.delete("/admin/models/{model_id}")
    def model_delete(model_id: int, user=Depends(require_admin)):
        return model_catalog.delete(user,model_id)

    @api.post("/admin/models/{model_id}/check")
    async def model_check(model_id: int, user=Depends(require_admin)):
        return await model_catalog.check(user,model_id)

    @api.get("/access/status")
    def status(user=Depends(get_current_user)):
        return access.status(user)

    @api.get("/access/rules")
    def rules(language: Literal["en", "ru"] = "ru", user=Depends(get_current_user)):
        from app.user_rules import user_rules_content
        try:
            return {"content_md": user_rules_content(language)}
        except OSError:
            raise HTTPException(503, "Правила временно недоступны. Попробуйте позже.")

    @api.get("/access/groups")
    def available_groups(for_request: bool = False, user=Depends(get_current_user)):
        if for_request:
            return access.request_groups(user)
        with db.get_conn() as conn:
            scope = governance.scope_for(user, conn)
            where, args = scope.sql("id") if scope.groups else ("TRUE", [])
            return conn.execute("SELECT id,name FROM access_groups WHERE is_active AND " + where + " ORDER BY name", args).fetchall()

    @api.post("/access/requests")
    def request_access(data: dict, user=Depends(get_current_user)):
        return access.request_access(user, data)

    @api.post("/access/requests/{request_id}/cancel")
    def cancel(request_id: int, user=Depends(get_current_user)):
        return access.decide_request(user, request_id, {}, cancel=True)

    @api.get("/group-admin/groups")
    def groups(user=Depends(management_user)):
        return access.managed_groups(user)

    @api.post("/group-admin/groups")
    def create_group(data: dict, user=Depends(require_admin)):
        return access.save_group(user, data)

    @api.patch("/group-admin/groups/{group_id}")
    def edit_group(group_id: int, data: dict, user=Depends(require_admin)):
        return access.save_group(user, data, group_id)

    @api.delete("/group-admin/groups/{group_id}")
    def disable_group(group_id: int, user=Depends(require_admin)):
        with db.get_conn() as conn:
            group = conn.execute("SELECT is_system FROM access_groups WHERE id=?", (group_id,)).fetchone()
            if not group:
                raise HTTPException(404, "Группа не найдена")
            if group["is_system"]:
                raise HTTPException(403, "Системную группу удалить нельзя")
        return access.save_group(user, {"is_active": False}, group_id)

    @api.get("/group-admin/groups/{group_id}/members")
    def members(group_id: int, user=Depends(management_user)):
        return access.members(user, group_id)

    @api.post("/group-admin/groups/{group_id}/members/by-login")
    def add_member(group_id: int, data: dict, user=Depends(management_user)):
        return access.add_by_login(user, group_id, data)

    @api.get("/group-admin/reports")
    def reports(group_id: int | None = None, user_id: int | None = None, report_id: str | None = None,
                status: str | None = None, offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200),
                date_from: str | None = None, date_to: str | None = None, search: str | None = None,
                include_test: bool = True,
                user=Depends(management_user)):
        return governance.metadata_reports(user, group_id=group_id, user_id=user_id, report_id=report_id,
                                            status=status, offset=offset, limit=limit, date_from=date_from,
                                            date_to=date_to, search=search, include_test=include_test)

    @api.put("/group-admin/groups/{group_id}/members/{user_id}")
    def save_member(group_id: int, user_id: int, data: dict, user=Depends(management_user)):
        return access.save_member(user, group_id, user_id, data)

    @api.delete("/group-admin/groups/{group_id}/members/{user_id}")
    def remove_member(group_id: int, user_id: int, user=Depends(management_user)):
        return access.save_member(user, group_id, user_id, remove=True)

    @api.get("/group-admin/users/search")
    def search(q: str = Query(default="", min_length=2, max_length=120), user=Depends(management_user)):
        return access.search_users(user, q)

    @api.get("/group-admin/users")
    def users(paged: bool = False, include_test: bool = False, offset: int = Query(0,ge=0), limit: int = Query(25,ge=1,le=100),
              role_filter: str | None = None, group_id: int | None = None, search: str | None = None,
              user=Depends(require_admin)):
        where,args='TRUE' if include_test else 'NOT u.is_test_fixture',[]
        manager_sql="""EXISTS(SELECT 1 FROM access_group_members mm JOIN access_groups gg ON gg.id=mm.group_id
            WHERE mm.user_id=u.id AND mm.role_in_group='manager' AND gg.is_active)"""
        if role_filter=='admin':where+=" AND u.role='admin'"
        elif role_filter=='manager':where+=" AND u.role<>'admin' AND "+manager_sql
        elif role_filter=='user':where+=" AND u.role<>'admin' AND NOT "+manager_sql
        if group_id is not None:
            where+=' AND EXISTS(SELECT 1 FROM access_group_members gm WHERE gm.user_id=u.id AND gm.group_id=?)';args.append(group_id)
        if search:where+=' AND (u.username ILIKE ? OR u.full_name ILIKE ?)';args.extend(['%'+search+'%']*2)
        with db.get_conn() as conn:
            total=conn.execute('SELECT COUNT(*) AS n FROM users u WHERE '+where,args).fetchone()['n']
            sso_fields = ("sso_provider", "sso_subject", "sso_login_name")
            rows = conn.execute("""SELECT u.id,u.username,u.role,u.sso_provider,u.sso_subject,u.sso_login_name,""" + user_profile.columns(True) + """,u.profile_synced_at,(SELECT MAX(r.updated_at) FROM reports r WHERE r.user_id=u.id) AS last_activity
                FROM users u WHERE """+where+" ORDER BY u.username LIMIT ? OFFSET ?",[*args,limit if paged else 500,offset if paged else 0]).fetchall()
            result = []
            for row in rows:
                memberships = conn.execute("""SELECT m.*,g.name,g.token_limit,g.limit_period,g.is_active FROM access_group_members m
                    JOIN access_groups g ON g.id=m.group_id WHERE m.user_id=? ORDER BY g.name""", (row["id"],)).fetchall()
                result.append({**{key: value for key, value in row.items() if key not in sso_fields}, "groups": memberships,
                               "effective_role": governance.effective_role(row["role"], memberships),
                               "role_from_env": access.role_from_env(row),
                               "total_tokens": db.get_user_usage(int(row["id"]))["total_tokens"]})
            return {'rows':result,'total':total,'has_more':offset+len(result)<total} if paged else result

    @api.patch("/group-admin/users/{user_id}/role")
    def role(user_id: int, data: dict, user=Depends(require_admin)):
        return access.save_role(user, user_id, data.get("role"))

    @api.patch("/group-admin/users/{user_id}/effective-role")
    def effective_role(user_id: int, data: dict, user=Depends(require_admin)):
        return governance.assign_role(user, user_id, data.get("role"), data.get("group_ids", []))

    @api.get("/group-admin/requests")
    def requests(user=Depends(management_user)):
        return access.requests_for(user)

    @api.post("/group-admin/requests/{request_id}/decision")
    def decide(request_id: int, data: dict, user=Depends(management_user)):
        return access.decide_request(user, request_id, data)

    @api.get("/group-admin/audit")
    def audit(actor_id: int | None = None, action: str | None = None, target_type: str | None = None,
              target_id: str | None = None, date_from: str | None = None, date_to: str | None = None,
              group_id: int | None = None, author: str | None = None, object_query: str | None = None,
              include_unchanged: bool = False,
              offset: int = Query(0, ge=0), limit: int = Query(100, ge=1, le=500),
              user=Depends(management_user)):
        return events.audit_search(user, actor_id=actor_id, action=action, target_type=target_type, target_id=target_id,
                                   date_from=date_from, date_to=date_to, group_id=group_id, offset=offset, limit=limit,
                                   author=author,object_query=object_query,include_unchanged=include_unchanged)

    @api.get("/group-admin/events")
    def timeline(report_id: str | None = None, group_id: int | None = None, username: str | None = None,
                 q: str | None = None, event_type: str | None = None, levels: str | None = None,
                 date_from: str | None = None, date_to: str | None = None, offset: int = Query(0, ge=0),
                 limit: int = Query(100, ge=1, le=500), user=Depends(management_user)):
        return events.search(user, group_id=group_id, report_id=report_id, username=username,
                             q=q, event_type=event_type, levels=levels,
                             date_from=date_from, date_to=date_to, offset=offset, limit=limit, timeline=True, ascending=bool(report_id))

    @api.get("/admin/events")
    def log_search(group_id: int | None = None, report_id: str | None = None, job_id: str | None = None,
                   request_id: str | None = None, username: str | None = None, q: str | None = None,
                   event_type: str | None = None, service: str | None = None, levels: str | None = None,
                   date_from: str | None = None, date_to: str | None = None, offset: int = Query(0, ge=0),
                   limit: int = Query(100, ge=1, le=1000), ascending: bool = False, timeline: bool = False,
                   export: bool = False, include_http: bool = False, user=Depends(require_admin)):
        filters = dict(group_id=group_id, report_id=report_id, job_id=job_id, request_id=request_id, username=username,
                       q=q, event_type=event_type, service=service, levels=levels, date_from=date_from,
                       date_to=date_to, ascending=ascending, timeline=timeline,include_http=include_http)
        if export:
            # Поток по страницам: весь отфильтрованный набор, без ограничения UI.
            from fastapi.responses import StreamingResponse
            def chunks():
                pos = 0
                while True:
                    page = events.search(user, **filters, offset=pos, limit=1000)
                    content = events.csv_export(page["rows"])
                    yield content if pos == 0 else "\n".join(content.split("\n")[1:])
                    pos += len(page["rows"])
                    if not page["has_more"]:
                        break
            return StreamingResponse(chunks(), media_type="text/csv; charset=utf-8",
                                     headers={"Content-Disposition": 'attachment; filename="service-events.csv"'})
        return events.search(user, **filters, offset=offset, limit=limit)

    @api.get("/admin/usage")
    def usage(user_id: int | None = None, group_id: int | None = None, model: str | None = None,
              status: str | None = None, mode: str | None = None, date_from: str | None = None,
              date_to: str | None = None, offset: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=1000),
              user=Depends(require_admin)):
        filters, args = [], []
        for column, value, operator in (("r.user_id", user_id, "="), ("r.access_group_id", group_id, "="),
                ("r.model", model, "="), ("r.status", status, "="), ("r.analysis_mode", mode, "="),
                ("r.created_at", date_from, ">="), ("r.created_at", date_to, "<")):
            if value is not None:
                if column == "r.created_at":
                    value = access.utc_bound(value)
                filters.append(f"{column} {operator} ?")
                args.append(value)
        where = " AND ".join(filters) or "TRUE"
        with db.get_conn() as conn:
            totals = conn.execute("""SELECT COUNT(*) AS reports, COUNT(DISTINCT user_id) AS active_users,
                COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,COALESCE(SUM(completion_tokens),0) AS completion_tokens,
                COALESCE(SUM(total_tokens),0) AS total_tokens FROM reports r WHERE """ + where, args).fetchone()
            rows = conn.execute("""SELECT to_char(r.created_at::timestamptz AT TIME ZONE 'Europe/Moscow','YYYY-MM') AS month,
                r.user_id,u.username,r.access_group_id,g.name AS group_name,r.provider,r.model,r.status,r.analysis_mode,
                COUNT(*) AS reports, SUM(r.prompt_tokens) AS prompt_tokens,SUM(r.completion_tokens) AS completion_tokens,
                SUM(r.total_tokens) AS total_tokens FROM reports r JOIN users u ON u.id=r.user_id
                LEFT JOIN access_groups g ON g.id=r.access_group_id WHERE """ + where + """
                GROUP BY month,r.user_id,u.username,r.access_group_id,g.name,r.provider,r.model,r.status,r.analysis_mode
                ORDER BY month DESC,u.username,r.access_group_id,r.provider,r.model,r.status,r.analysis_mode LIMIT ? OFFSET ?""",
                [*args, limit + 1, offset]).fetchall()
            reliable_since = db.get_reliable_since(conn)
        has_more = len(rows) > limit
        return {"totals": totals, "rows": rows[:limit], "offset": offset, "limit": limit,
                "has_more": has_more, "truncated": has_more,
                "updated_at": db._now_iso(), "reliable_since": reliable_since}

    @api.get("/group-admin/reporting")
    def report_metrics(preset: str = "month", date_from: str | None = None, date_to: str | None = None,
                       group_id: int | None = None, user_id: int | None = None, search: str | None = None,
                       export: str | None = None, user=Depends(management_user)):
        result = reporting.get_report(user, preset=preset, date_from=date_from, date_to=date_to,
                                      group_id=group_id, user_id=user_id, search=search)
        if export:
            if export == "reports":
                def rows():
                    cursor = None
                    while True:
                        page = governance.metadata_reports(user, group_id=group_id, user_id=user_id,
                            date_from=result["period"]["from"], date_to=result["period"]["to"], cursor=cursor,
                            count_total=False, limit=500, search=search)
                        yield from page["rows"]
                        cursor = page['next_cursor']
                        if not page["has_more"]:
                            break
                result["reports"] = rows()
            content = reporting.xlsx_report(result, export, admin=governance.require_management(user).admin)
            period = result["period"]["from"][:10]
            return Response(content, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            headers={"Content-Disposition": f'attachment; filename="reporting-{export}-{period}.xlsx"'})
        return result

    return api
