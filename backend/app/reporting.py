"""Отчётность по материализованным метаданным, включая мягко удалённые отчёты."""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO

from fastapi import HTTPException
from openpyxl import Workbook

from app import db
from app.access import MOSCOW, utc_bound
from app.governance import require_management


def shift_month(value, months):
    number = value.year * 12 + value.month - 1 + months
    return value.replace(year=number // 12, month=number % 12 + 1, day=1)


def period_bounds(preset="month", date_from=None, date_to=None, now=None):
    now = (now or datetime.now(timezone.utc)).astimezone(MOSCOW)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if preset == "previous":
        end, start = start, shift_month(start, -1)
    elif preset == "quarter":
        start = start.replace(month=((start.month - 1) // 3) * 3 + 1)
        end = shift_month(start, 3)
    elif preset == "custom":
        try:
            start = datetime.fromisoformat(utc_bound(date_from))
            end = datetime.fromisoformat(utc_bound(date_to))
            start, end = start.astimezone(MOSCOW), end.astimezone(MOSCOW)
        except (TypeError, ValueError):
            raise HTTPException(422, "Укажите начало и исключающую верхнюю границу периода")
        if end <= start or end - start > timedelta(days=3660):
            raise HTTPException(422, "Период должен быть положительным и не больше 10 лет")
    elif preset == "month":
        end = shift_month(start, 1)
    else:
        raise HTTPException(422, "Неизвестный период")
    previous_end = start
    previous_start = (shift_month(start, -3) if preset == "quarter" else shift_month(start, -1)
                      if preset in {"month", "previous"} else start - (end - start))
    return start, end, previous_start, previous_end



def compare(current, previous):
    delta = current - previous
    return {"delta": delta, "percent": delta / previous * 100 if previous else None}


def aggregate_metrics(row, start, end, now):
    """Форматирование одной SQL-агрегации; отдельные отчёты здесь отсутствуют."""
    def number(key): return int(row.get(key) or 0)
    tokens, processed = number('total_tokens'), number('processed_rows')
    data_days = number('forecast_data_days')
    forecast = tokens if end <= now else None
    if start <= now < end and data_days >= 3:
        remaining = max(0, (end.date()-now.date()).days-1)
        forecast = tokens + round(number('recent_tokens') / data_days * remaining)
    return {"total_tokens":tokens,"reports":number('reports'),"processed_rows":processed,
        "active_users":number('active_users'),"error_reports":number('error_reports'),"error_rows":number('error_rows'),
        "error_report_percent":number('error_reports')/number('known_reports')*100 if number('known_reports') else 0,
        "error_row_percent":number('error_rows')/number('known_processed')*100 if number('known_processed') else 0,
        "average_tokens":tokens/processed if processed else 0,"forecast":forecast,"forecast_data_days":data_days,
        "forecast_is_estimate":forecast is not None and end>now,"limit_stops":number('limit_stops'),
        "unknown_row_metrics":number('unknown_row_metrics')}


DAY_SQL = "(r.created_at::timestamptz AT TIME ZONE 'Europe/Moscow')::date"


def aggregate_sql(admin):
    dimensions = [('group','gid'),('user','user_id')]
    if admin: dimensions += [('model','provider,model'),('mode','mode')]
    kind = 'CASE ' + ' '.join("WHEN GROUPING("+cols.split(',')[0]+")=0 THEN '"+name+"'" for name,cols in dimensions) + " ELSE 'summary' END"
    keys = 'gid,user_id' + (',provider,model,mode' if admin else '')
    sets = '(phase),' + ','.join('(phase,'+cols+')' for _,cols in dimensions)
    return """SELECT phase,"""+kind+" AS kind,"+keys+""",
        COALESCE(SUM(total_tokens),0) AS total_tokens,COUNT(*) AS reports,
        COALESCE(SUM(processed_rows),0) AS processed_rows,
        COUNT(DISTINCT user_id) FILTER(WHERE processed_rows>0) AS active_users,
        COUNT(*) FILTER(WHERE status='failed' OR failed_rows>0) AS error_reports,
        COALESCE(SUM(failed_rows),0) AS error_rows,
        COUNT(*) FILTER(WHERE failed_rows IS NOT NULL OR status='failed') AS known_reports,
        COALESCE(SUM(processed_rows) FILTER(WHERE failed_rows IS NOT NULL),0) AS known_processed,
        COUNT(*) FILTER(WHERE failed_rows IS NULL) AS unknown_row_metrics,
        COALESCE(SUM(limit_pause_count),0) AS limit_stops,
        COUNT(DISTINCT day) FILTER(WHERE day>=?::date AND day<?::date) AS forecast_data_days,
        COALESCE(SUM(total_tokens) FILTER(WHERE day>=?::date AND day<?::date),0) AS recent_tokens,
        MAX(COALESCE(updated_at,created_at)) AS last_activity
        FROM base GROUP BY GROUPING SETS ("""+sets+")"


def get_report(user, *, preset="month", date_from=None, date_to=None, group_id=None, user_id=None, search=None):
    scope = require_management(user)
    if group_id is not None: scope.require_group(group_id)
    now = datetime.now(timezone.utc).astimezone(MOSCOW)
    start,end,prev_start,prev_end = period_bounds(preset,date_from,date_to,now)
    where,args = scope.sql()
    if group_id == 0: where += ' AND r.access_group_id IS NULL'
    elif group_id is not None: where += ' AND r.access_group_id=?'; args.append(group_id)
    if user_id is not None: where += ' AND r.user_id=?'; args.append(user_id)
    if search:
        from app.user_profile import scoped_name
        where += ' AND (u.username ILIKE ? OR ('+scoped_name(scope)+') ILIKE ?)'; args.extend(['%'+search+'%']*2)
    # created_at хранится как UTC ISO TEXT; предикат без cast использует индекс периода.
    cte = """WITH base AS (SELECT r.user_id,COALESCE(r.access_group_id,0) AS gid,r.provider,r.model,
        CASE WHEN COALESCE(r.group_by_column,'')<>'' THEN 'grouped' ELSE 'rows' END AS mode,
        r.created_at,r.updated_at,r.total_tokens,r.processed_rows,r.failed_rows,r.status,r.limit_pause_count,
        CASE WHEN r.created_at>=? THEN 1 ELSE 0 END AS phase,"""+DAY_SQL+""" AS day
        FROM reports r JOIN users u ON u.id=r.user_id WHERE """+where+" AND r.created_at>=? AND r.created_at<?) "
    base_args = [start.astimezone(timezone.utc).isoformat(),*args,prev_start.astimezone(timezone.utc).isoformat(),end.astimezone(timezone.utc).isoformat()]
    today = now.date(); recent = today-timedelta(days=7)
    with db.get_conn() as conn:
        from app.user_profile import columns, scoped_name
        aggregates = conn.execute(cte+aggregate_sql(scope.admin),[*base_args,recent.isoformat(),today.isoformat(),recent.isoformat(),today.isoformat()]).fetchall()
        daily = conn.execute(cte+"SELECT day::text AS date,gid,model,SUM(total_tokens) AS total_tokens FROM base WHERE phase=1 GROUP BY day,gid,model ORDER BY day,gid::text,model NULLS FIRST" if scope.admin else
            cte+"SELECT day::text AS date,gid,SUM(total_tokens) AS total_tokens FROM base WHERE phase=1 GROUP BY day,gid ORDER BY day,gid::text",base_args).fetchall()
        tops = conn.execute(cte+""", ranked AS (SELECT gid,user_id,SUM(COALESCE(total_tokens,0)) AS total_tokens,
            row_number() OVER(PARTITION BY gid ORDER BY SUM(COALESCE(total_tokens,0)) DESC,user_id) AS pos
            FROM base WHERE phase=1 GROUP BY gid,user_id)
            SELECT x.gid,u.username,x.total_tokens,"""+scoped_name(scope)+""" AS full_name
            FROM ranked x JOIN users u ON u.id=x.user_id WHERE pos<=10 ORDER BY gid,pos""",base_args).fetchall()
        gw,ga = scope.sql('g.id')
        if group_id is not None: gw+=' AND g.id=?'; ga.append(group_id)
        groups=conn.execute("""SELECT g.id,g.name,g.token_limit,g.limit_period,g.is_active,
            (SELECT COUNT(*) FROM access_group_members m WHERE m.group_id=g.id) AS member_count
            FROM access_groups g WHERE """+gw+' ORDER BY g.name',ga).fetchall()
        leaders=conn.execute("SELECT m.group_id,u.username FROM access_group_members m JOIN users u ON u.id=m.user_id JOIN access_groups g ON g.id=m.group_id WHERE m.role_in_group='manager' AND "+gw+' ORDER BY m.group_id,u.username',ga).fetchall()
        user_ids=sorted({int(r['user_id']) for r in aggregates if r['kind']=='user' and r['phase']==1})
        profiles=conn.execute('SELECT u.id,u.username,'+columns(scope.admin,groups=scope.groups)+' FROM users u WHERE u.id IN ('+','.join('?' for _ in user_ids)+')',user_ids).fetchall() if user_ids else []
        reliable=db.get_reliable_since(conn)
    by_kind = defaultdict(dict)
    for row in aggregates:
        if row['phase']==1:
            key = row.get('gid') if row['kind']=='group' else row.get('user_id') if row['kind']=='user' else (row.get('provider'),row.get('model')) if row['kind']=='model' else row.get('mode')
            by_kind[row['kind']][key]=row
    metric=lambda row:aggregate_metrics(row or {},start,end,now)
    summary=metric(next((r for r in aggregates if r['kind']=='summary' and r['phase']==1),{}))
    summary['previous']=aggregate_metrics(next((r for r in aggregates if r['kind']=='summary' and r['phase']==0),{}),prev_start,prev_end,now)
    summary['comparison']={k:compare(summary[k],summary['previous'][k]) for k in ('total_tokens','reports','processed_rows','active_users','error_reports')}
    if scope.admin and (group_id==0 or (group_id is None and any(r['kind']=='group' and r['gid']==0 for r in aggregates))):
        groups.append(dict(id=0,name='Без группы',token_limit=None,limit_period='none',is_active=True,member_count=0))
    limits=[g['token_limit'] for g in groups]; cap=sum(limits) if limits and all(v is not None for v in limits) else None
    summary.update(token_limit=cap,limit_percent=None if cap is None else 100 if cap==0 else summary['total_tokens']/cap*100)
    top_by_group=defaultdict(list); leader_by_group=defaultdict(list)
    for t in tops: top_by_group[t['gid']].append({'username':t['username'],'total_tokens':int(t['total_tokens']),
        **({'full_name':t['full_name']} if t.get('full_name') else {})})
    for l in leaders: leader_by_group[l['group_id']].append(l['username'])
    group_rows=[]
    for g in groups:
        m=metric(by_kind['group'].get(g['id']))
        group_rows.append({**dict(g),**m,'leaders':leader_by_group[g['id']],'top_users':top_by_group[g['id']],
            'percent':None if g['token_limit'] is None else 100 if g['token_limit']==0 else m['total_tokens']/g['token_limit']*100})
    user_rows=[]
    for profile in profiles:
        r=by_kind['user'][profile['id']]
        # Не добавляем пустые поля профиля к историческим ответам без SSO-профиля.
        user_rows.append({**{k:v for k,v in profile.items() if v is not None},**metric(r),'last_activity':r['last_activity']})
    user_rows.sort(key=lambda r:(-r['total_tokens'],r['id']))
    names={g['id']:g['name'] for g in groups}
    result={'period':{'from':start.isoformat(),'to':end.isoformat(),'previous_from':prev_start.isoformat(),'previous_to':prev_end.isoformat()},
        'summary':summary,'groups':group_rows,'users':user_rows,'daily':[{'date':d['date'],'group_id':d['gid'],
        'group_name':names.get(d['gid'],'Без группы'),**({'model':d['model']} if scope.admin else {}),'total_tokens':int(d['total_tokens'] or 0)} for d in daily],
        'reliable_since':reliable,'updated_at':db._now_iso()}
    if scope.admin:
        result['models']=[{'provider':p,'model':m,**metric(r)} for (p,m),r in sorted(by_kind['model'].items(),key=lambda item:str(item[0]))]
        result['modes']=[{'mode':mode,**metric(r)} for mode,r in sorted(by_kind['mode'].items())]
    return result


def xlsx_report(data, view="business", *, admin=False):
    if view not in {"business", "summary", "groups", "users", "daily", "models", "modes", "reports"}:
        raise HTTPException(422, "Неизвестное представление")
    if view in {"models", "modes"} and view not in data:
        raise HTTPException(403, "Представление доступно только администратору")
    if view == "business":
        start = datetime.fromisoformat(utc_bound(data["period"]["from"])).astimezone(MOSCOW)
        end = datetime.fromisoformat(utc_bound(data["period"]["to"])).astimezone(MOSCOW)
        if start.day != 1 or any((start.hour, start.minute, start.second, start.microsecond)) or end != shift_month(start, 1):
            raise HTTPException(422, "Отчёт для бизнеса формируется за полный календарный месяц по Москве")
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet("Отчёт")
    def safe(v):
        if isinstance(v, (list, dict)):
            v = ", ".join(v) if isinstance(v, list) and all(isinstance(x, str) for x in v) else str(v)
        return "'" + v if isinstance(v, str) and v.startswith(("=", "+", "-", "@")) else v
    if view == "business":
        sheet.append(["Период МСК", data["period"]["from"], data["period"]["to"]])
        sheet.append(["Группа", "Руководители", "Лимит", "Токены", "%", "Прогноз", "Активных", "Отчётов", "Строк", "Остановок по лимиту", "Топ пользователей"])
        for g in data["groups"]:
            sheet.append([safe(v) for v in [g["name"], ", ".join(g["leaders"]), g["token_limit"], g["total_tokens"],
                g["percent"], g["forecast"], g["active_users"], g["reports"], g["processed_rows"], g["limit_stops"],
                "; ".join(f'{u.get("full_name") or u["username"]} ({u["username"]}): {u["total_tokens"]}' for u in g["top_users"]) ]])
        people = workbook.create_sheet("Пользователи")
        keys = ['full_name','username','job_title','company'] + (['employee_id'] if admin else [])
        people.append(['ФИО','Логин','Должность','Компания'] + (['Идентификатор сотрудника'] if admin else []))
        for person in data.get('users',[]): people.append([safe(person.get(k)) for k in keys])
    elif view == "summary":
        sheet.append(["Показатель", "Значение"])
        for key, value in data["summary"].items():
            if not isinstance(value, dict): sheet.append([key, safe(value)])
    else:
        rows = iter(data.get(view, []))
        first = next(rows, None)
        columns = list(dict.fromkeys([*(first or {}),*(('full_name','username','job_title','company') if view in {'users','reports'} else ())]))
        if admin and view in {'users','reports'} and 'employee_id' not in columns: columns.append('employee_id')
        if not admin: columns = [c for c in columns if c != 'employee_id']
        labels = {'full_name':'ФИО','username':'Логин','employee_id':'Идентификатор сотрудника','job_title':'Должность','company':'Компания'}
        sheet.append([labels.get(c,c) for c in columns] or ["Нет данных"])
        if first:
            sheet.append([safe(first.get(c)) for c in columns])
        for row in rows:
            sheet.append([safe(row.get(c)) for c in columns])
    info = workbook.create_sheet("Условия")
    info.append(["Надёжный учёт с", data["reliable_since"] or "дата не определена"])
    info.append(["Удалённые отчёты", "Включены"])
    info.append(["Учёт периода", "По дате создания отчёта, Москва. Верхняя граница исключается."])
    info.append(["Неизвестна детализация строк у отчётов", data["summary"]["unknown_row_metrics"]])
    target = BytesIO(); workbook.save(target)
    return target.getvalue()
