"""Эталон метрик до SQL-агрегации, только для тестов."""
from collections import defaultdict
from datetime import datetime,timedelta
from app.access import MOSCOW

def _metrics(rows, start, end, now):
    tokens = sum(int(r["total_tokens"] or 0) for r in rows)
    processed = sum(int(r["processed_rows"] or 0) for r in rows)
    error_rows = sum(int(r.get("failed_rows") or 0) for r in rows)
    known_processed = sum(int(r["processed_rows"] or 0) for r in rows if r.get("failed_rows") is not None)
    known_reports = sum(r.get("failed_rows") is not None or r["status"] == "failed" for r in rows)
    error_reports = sum(r["status"] == "failed" or int(r.get("failed_rows") or 0) > 0 for r in rows)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    day_tokens = defaultdict(int)
    for row in rows:
        if row.get("created_at"):
            at = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")).astimezone(MOSCOW)
            if today - timedelta(days=7) <= at < today:
                day_tokens[at.date()] += int(row["total_tokens"] or 0)
    data_days = len(day_tokens)
    forecast = None
    if end <= now:
        forecast = tokens
    elif start <= now < end and data_days >= 3:
        remaining_days = max(0, (end.date() - now.date()).days - 1)
        forecast = tokens + round(sum(day_tokens.values()) / data_days * remaining_days)
    return {"total_tokens": tokens, "reports": len(rows), "processed_rows": processed,
            "active_users": len({r["user_id"] for r in rows if int(r["processed_rows"] or 0) > 0}),
            "error_reports": error_reports, "error_rows": error_rows,
            "error_report_percent": error_reports / known_reports * 100 if known_reports else 0,
            "error_row_percent": error_rows / known_processed * 100 if known_processed else 0,
            "average_tokens": tokens / processed if processed else 0, "forecast": forecast,
            "forecast_data_days": data_days, "forecast_is_estimate": forecast is not None and end > now,
            "limit_stops": sum(int(r.get("limit_pause_count") or 0) for r in rows),
            "unknown_row_metrics": sum(r.get("failed_rows") is None for r in rows)}

def get_report(user, *, preset="month", date_from=None, date_to=None, group_id=None, user_id=None, search=None):
    scope = require_management(user)
    if group_id is not None:
        scope.require_group(group_id)
    now = datetime.now(timezone.utc).astimezone(MOSCOW)
    start, end, prev_start, prev_end = period_bounds(preset, date_from, date_to, now)
    where, args = scope.sql()
    if group_id is not None:
        if group_id == 0:
            where += " AND r.access_group_id IS NULL"
        else:
            where += " AND r.access_group_id=?"; args.append(group_id)
    if user_id is not None:
        where += " AND r.user_id=?"; args.append(user_id)
    if search:
        where += " AND u.username ILIKE ?"; args.append("%" + search + "%")
    with db.get_conn() as conn:
        rows = conn.execute("""SELECT r.id,r.user_id,u.username,r.access_group_id,r.status,r.provider,r.model,
            r.analysis_mode,r.group_by_column,r.created_at,r.updated_at,r.deleted_at,r.total_tokens,r.processed_rows,r.failed_rows,r.limit_pause_count
            FROM reports r JOIN users u ON u.id=r.user_id WHERE """ + where +
            " AND r.created_at>=? AND r.created_at<?", [*args, prev_start.astimezone(timezone.utc).isoformat(),
                                                       end.astimezone(timezone.utc).isoformat()]).fetchall()
        group_where, group_args = scope.sql("g.id")
        if group_id is not None:
            group_where += " AND g.id=?"; group_args.append(group_id)
        groups = conn.execute("""SELECT g.id,g.name,g.token_limit,g.limit_period,g.is_active,
            (SELECT COUNT(*) FROM access_group_members m WHERE m.group_id=g.id) AS member_count
            FROM access_groups g WHERE """ + group_where + " ORDER BY g.name", group_args).fetchall()
        leaders = conn.execute("""SELECT m.group_id,u.username FROM access_group_members m
            JOIN users u ON u.id=m.user_id JOIN access_groups g ON g.id=m.group_id
            WHERE m.role_in_group='manager' AND """ + group_where, group_args).fetchall()
        reliable = db.get_reliable_since(conn)
    current, previous = [], []
    for row in rows:
        at = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")).astimezone(MOSCOW)
        (current if start <= at < end else previous).append(row)
    if scope.admin and (group_id == 0 or (group_id is None and any(r['access_group_id'] is None for r in rows))):
        groups.append({'id':0,'name':'Без группы','token_limit':None,'limit_period':'none','is_active':True,'member_count':0})
    summary = _metrics(current, start, end, now)
    summary["previous"] = _metrics(previous, prev_start, prev_end, now)
    summary["comparison"] = {key: compare(summary[key], summary["previous"][key])
                             for key in ("total_tokens", "reports", "processed_rows", "active_users", "error_reports")}
    limits = [g["token_limit"] for g in groups]
    summary["token_limit"] = sum(limits) if limits and all(v is not None for v in limits) else None
    cap = summary["token_limit"]
    summary["limit_percent"] = None if cap is None else 100 if cap == 0 else summary["total_tokens"] / cap * 100
    by_group = defaultdict(list); by_user = defaultdict(list); by_model = defaultdict(list); by_mode = defaultdict(list)
    daily = defaultdict(int)
    group_names = {g["id"]: g["name"] for g in groups}
    for r in current:
        by_group[r["access_group_id"] or 0].append(r)
        by_user[r["user_id"]].append(r)
        by_model[(r["provider"], r["model"])].append(r)
        execution_mode = "grouped" if r.get("group_by_column") else "rows"
        by_mode[execution_mode].append(r)
        day = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00")).astimezone(MOSCOW).strftime("%Y-%m-%d")
        daily[(day, r["access_group_id"] or 0, r["model"] if scope.admin else None)] += int(r["total_tokens"] or 0)
    group_rows = [{**dict(g), **_metrics(by_group[g["id"]], start, end, now),
                   "leaders": [r["username"] for r in leaders if r["group_id"] == g["id"]],
                   "percent": None if g["token_limit"] is None else 100 if g["token_limit"] == 0 else sum(int(r["total_tokens"] or 0) for r in by_group[g["id"]]) / g["token_limit"] * 100,
                   "top_users": sorted([{ "username": items[0]["username"], "total_tokens": sum(int(r["total_tokens"] or 0) for r in items if (r["access_group_id"] or 0) == g["id"])}
                                         for uid, items in by_user.items() if any((r["access_group_id"] or 0) == g["id"] for r in items)],
                                        key=lambda u: u["total_tokens"], reverse=True)[:10]}
                  for g in groups]
    user_rows = [{"id": uid, "username": items[0]["username"], **_metrics(items, start, end, now),
                  "last_activity": max(r["updated_at"] or r["created_at"] for r in items)} for uid, items in by_user.items()]
    user_rows.sort(key=lambda r: r["total_tokens"], reverse=True)
    result = {"period": {"from": start.isoformat(), "to": end.isoformat(), "previous_from": prev_start.isoformat(), "previous_to": prev_end.isoformat()},
              "summary": summary, "groups": group_rows, "users": user_rows,
              "daily": [{"date": day, "group_id": gid, "group_name": group_names.get(gid, "Без группы"),
                         **({"model": model} if scope.admin else {}), "total_tokens": tokens}
                        for (day, gid, model), tokens in sorted(daily.items(), key=lambda p: (p[0][0], str(p[0][1]), str(p[0][2])))],
              "reliable_since": reliable, "updated_at": db._now_iso()}
    if scope.admin:
        result["models"] = [{"provider": p, "model": m, **_metrics(items, start, end, now)} for (p, m), items in by_model.items()]
        result["modes"] = [{"mode": mode, **_metrics(items, start, end, now)} for mode, items in by_mode.items()]
    return result
