"""SQL-формулы проверяются на локальном движке; PostgreSQL GROUPING SETS разворачиваются только адаптером теста."""
from contextlib import contextmanager
from datetime import datetime, timezone
import ast
import json
from pathlib import Path
import re
import sqlite3
from unittest.mock import Mock
import pytest
from app import db, reporting, governance, reporting_schema
from . import reporting_reference as reference


class Result:
    def __init__(self,rows): self.rows=rows
    def fetchall(self): return self.rows
    def fetchone(self): return self.rows[0] if self.rows else None


def fixture_connection(records):
    sql=sqlite3.connect(':memory:');sql.row_factory=sqlite3.Row
    sql.create_function('day_msk',1,lambda v:datetime.fromisoformat(v.replace('Z','+00:00')).astimezone(reporting.MOSCOW).date().isoformat())
    sql.executescript('''CREATE TABLE users(id INTEGER,username TEXT,full_name TEXT,job_title TEXT,company TEXT,employee_id TEXT);
        INSERT INTO users VALUES (7,'user.test',NULL,NULL,NULL,NULL),(8,'other.test',NULL,NULL,NULL,NULL);
        CREATE TABLE access_groups(id INTEGER,name TEXT,token_limit INTEGER,limit_period TEXT,is_active BOOLEAN);
        INSERT INTO access_groups VALUES (1,'Розница',1000,'month',TRUE),(2,'Поддержка',NULL,'none',TRUE);
        CREATE TABLE access_group_members(group_id INTEGER,user_id INTEGER,role_in_group TEXT);
        INSERT INTO access_group_members VALUES (1,7,'manager'),(2,8,'manager');
        CREATE TABLE reports(id TEXT,user_id INTEGER,access_group_id INTEGER,status TEXT,provider TEXT,model TEXT,
          analysis_mode TEXT,group_by_column TEXT,created_at TEXT,updated_at TEXT,deleted_at TEXT,
          total_tokens INTEGER,processed_rows INTEGER,failed_rows INTEGER,limit_pause_count INTEGER);''')
    columns=[r[1] for r in sql.execute('PRAGMA table_info(reports)')]
    for r in records: sql.execute('INSERT INTO reports VALUES ('+','.join('?' for _ in columns)+')',[r.get(c) for c in columns])
    class Conn:
        queries=[]
        def execute(self,text,args=()):
            self.queries.append((text,args))
            text=text.replace(reporting.DAY_SQL,'day_msk(r.created_at)').replace(' ILIKE ',' LIKE ').replace('?::date','?').replace('day::text','day').replace('gid::text','CAST(gid AS TEXT)')
            if 'GROUPING SETS' in text:
                prefix,fields=text.split('SELECT phase,CASE ',1)
                fields=fields[fields.index('COALESCE(SUM(total_tokens)'):fields.index('FROM base GROUP BY GROUPING SETS')]
                admin='(phase,provider,model)' in text
                keys=['gid','user_id']+(['provider','model','mode'] if admin else [])
                dimensions=[('summary',[]),('group',['gid']),('user',['user_id'])]
                if admin:dimensions += [('model',['provider','model']),('mode',['mode'])]
                rows=[]
                for kind,dims in dimensions:
                    select=','.join(k if k in dims else 'NULL AS '+k for k in keys)
                    q=prefix+"SELECT phase,'"+kind+"' AS kind,"+select+','+fields+' FROM base GROUP BY '+','.join(['phase',*dims])
                    rows.extend(dict(r) for r in sql.execute(q,args))
                return Result(rows)
            return Result([dict(r) for r in sql.execute(text,args)])
    return sql,Conn()


def sample_rows():
    rows=[]
    for i,(date,gid,uid,failed,status,tokens) in enumerate([
        ('2026-11-03T10:00:00+00:00',1,7,0,'completed',30),
        ('2026-11-04T10:00:00+00:00',1,7,2,'completed',40),
        ('2026-11-05T10:00:00+00:00',1,7,None,'failed',50),
        ('2026-11-09T22:00:00+00:00',2,7,0,'completed',900),
        ('2026-10-03T10:00:00+00:00',1,7,None,'completed',100),
        ('2026-11-08T10:00:00+00:00',None,8,0,'completed',80),
    ]):
        rows.append(dict(id=str(i),user_id=uid,access_group_id=gid,status=status,provider='openai',model='test-model',
            analysis_mode='custom',group_by_column='Канал' if i%2 else None,created_at=date,updated_at=date,
            deleted_at=date if i==2 else None,total_tokens=tokens,processed_rows=10,failed_rows=failed,limit_pause_count=1 if i==2 else 0))
    return rows


@pytest.fixture
def report_db(monkeypatch):
    sql,conn=fixture_connection(sample_rows())
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(db,'get_conn',connection)
    monkeypatch.setattr(db,'get_reliable_since',lambda c:None)
    class Fixed(datetime):
        @classmethod
        def now(cls,tz=None): return datetime(2026,11,10,9,tzinfo=timezone.utc).astimezone(tz)
    monkeypatch.setattr(reporting,'datetime',Fixed)
    yield sql,conn
    sql.close()


@pytest.mark.parametrize('scope',[governance.Scope(True,()),governance.Scope(False,(1,))])
@pytest.mark.parametrize('preset,extra',[('month',{}),('previous',{}),('quarter',{}),
    ('custom',{'date_from':'2026-01-01','date_to':'2027-01-01'}),('month',{'group_id':1}),('month',{'group_id':0})])
def test_sql_matches_old_json(report_db,monkeypatch,scope,preset,extra):
    if not scope.admin and extra.get('group_id')==0:
        with pytest.raises(Exception): scope.require_group(0)
        return
    monkeypatch.setattr(reporting,'require_management',lambda u:scope)
    # Старый get_report остаётся только тестовым эталоном; импорт без сервера и БД.
    source=Path(__file__).with_name('reporting_reference.py').read_text(encoding='utf-8')
    reference.require_management=lambda u:scope
    reference.db=db;reference.period_bounds=reporting.period_bounds;reference.compare=reporting.compare
    reference.datetime=reporting.datetime;reference.timezone=timezone
    expected=reference.get_report({'id':7},preset=preset,**extra)
    actual=reporting.get_report({'id':7},preset=preset,**extra)
    for data in (expected,actual):
        data.pop('updated_at')
        for kind in ('users','models','modes'):
            if kind in data:data[kind].sort(key=lambda row:json.dumps(row,sort_keys=True,ensure_ascii=False))
        for g in data['groups']:g['top_users'].sort(key=lambda row:(-row['total_tokens'],row['username']))
    assert actual==expected
    assert not scope.admin and 'models' not in actual or scope.admin


def test_forecast_less_than_three_data_days(report_db,monkeypatch):
    sql,_=report_db
    sql.execute("DELETE FROM reports WHERE created_at<'2026-11-08'")
    monkeypatch.setattr(reporting,'require_management',lambda u:governance.Scope(True,()))
    result=reporting.get_report({'id':7})
    assert result['summary']['forecast'] is None and result['summary']['forecast_data_days']==1


def test_shared_member_scope_and_search(report_db,monkeypatch):
    sql,conn=report_db
    sql.execute("UPDATE users SET full_name='Тестов Тест' WHERE id=7")
    monkeypatch.setattr(reporting,'require_management',lambda u:governance.Scope(False,(1,)))
    r=reporting.get_report({'id':7},search='Тестов',user_id=7)
    assert r['summary']['total_tokens']==120 and r['users'][0]['total_tokens']==120
    assert 'employee_id' not in json.dumps(r) and r['users'][0]['full_name']=='Тестов Тест'
    assert all('report_rows' not in q for q,a in conn.queries)
    assert 'r.access_group_id IN (?)' in conn.queries[0][0]


def test_view_has_only_safe_daily_dimensions():
    conn=Mock();reporting_schema.init_schema(conn)
    text=' '.join(c.args[0] for c in conn.execute.call_args_list)
    assert 'idx_reports_created_at ON reports(created_at)' in text and 'CREATE OR REPLACE VIEW reporting_usage_daily' in text
    for secret in ('employee_id','report_rows','prompt_template','error_text','file_path'):assert secret not in text
    assert "Europe/Moscow" in text and 'deleted_reports' in text and 'full_name' in text


def test_metadata_export_keyset_without_offset_or_count(report_db,monkeypatch):
    sql,conn=report_db
    for key in ('name','total_rows','groups_total','groups_processed','finished_at','prompt_tokens','completion_tokens','success_rows','error_text'):
        sql.execute('ALTER TABLE reports ADD COLUMN '+key+' TEXT')
    monkeypatch.setattr(governance,'require_management',lambda u:governance.Scope(True,()))
    seen=[];cursor=None
    while True:
        page=governance.metadata_reports({'id':7},limit=2,count_total=False,cursor=cursor)
        seen.extend(r['id'] for r in page['rows'])
        if not page['has_more']:break
        cursor=page['next_cursor']
    assert len(seen)==len(set(seen))==6
    assert not any('OFFSET' in q or 'COUNT(*)' in q for q,args in conn.queries)
    assert any('r.created_at<? OR (r.created_at=? AND r.id>?)' in q for q,args in conn.queries)


def test_former_member_name_cannot_be_inferred_by_search(report_db,monkeypatch):
    sql,_=report_db
    sql.execute("UPDATE users SET full_name='Тестов Бывший' WHERE id=7")
    sql.execute('DELETE FROM access_group_members WHERE user_id=7')
    monkeypatch.setattr(reporting,'require_management',lambda u:governance.Scope(False,(1,)))
    data=reporting.get_report({'id':1})
    assert data['summary']['total_tokens']==120 and 'full_name' not in data['users'][0]
    assert reporting.get_report({'id':1},search='Бывший')['summary']['reports']==0
