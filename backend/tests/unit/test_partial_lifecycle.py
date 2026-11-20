from contextlib import contextmanager
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from app import db, events, partial_exports as partial
from app.schemas import UserMeResponse
from .test_model_admission import function
from .test_partial_exports import cache


class Cursor:
    rowcount=1
    def __init__(self,row=None,rows=None):self.row=row;self.rows=rows or []
    def fetchone(self):return self.row
    def fetchall(self):return self.rows


@pytest.fixture
def lifecycle(monkeypatch,cache):
    row,redis,calls,_=cache
    committed=[];invalidated=[];raw=Mock()
    def execute(sql,args=()):
        if 'WITH prev AS' in sql:
            return Cursor(dict(old_status='paused',old_personal_queue=False,user_id=1,new_status=args[1],
                id=row['id'],job_id='test-job',access_group_id=None))
        if "status='paused' RETURNING id" in sql:return Cursor({'id':row['id']})
        return Cursor()
    raw.execute.side_effect=execute
    raw.commit.side_effect=lambda:committed.append(True)
    @contextmanager
    def connection():yield raw
    monkeypatch.setattr(db,'_get_pg_pool',lambda:SimpleNamespace(connection=connection))
    monkeypatch.setattr(events,'record_event',Mock())
    monkeypatch.setattr(db,'_invalidate_usage_cache',Mock())
    import app.queue as queue
    monkeypatch.setattr(queue,'has_running_lease',lambda *args:False)
    real=partial.invalidate
    def invalidate(rid):
        assert committed
        invalidated.append(rid);real(rid)
    monkeypatch.setattr(partial,'invalidate',invalidate)
    return row,redis,invalidated,raw


@pytest.mark.parametrize('transition',['completed','canceled','failed','running','queued','retry','retry_rows','delete','resume','ttl_cancel'])
def test_lifecycle_clears_keys_and_files_after_commit(lifecycle,transition):
    row,redis,invalidated,_=lifecycle
    for fmt in ('xlsx','json'):partial.get_export(row['id'],fmt)
    if transition=='retry':db.reset_report_terminal_state(row['id'])
    elif transition=='retry_rows':db.reset_failed_and_skipped_rows(row['id'])
    elif transition=='ttl_cancel':
        raw=lifecycle[3];original=raw.execute.side_effect
        raw.execute.side_effect=lambda sql,args=():Cursor(rows=[{'id':row['id'],'job_id':'test-job'}]) if 'FOR UPDATE SKIP LOCKED' in sql else original(sql,args)
        assert db.autocancel_paused_reports(ttl_days=1)==1
    elif transition=='resume':db.queue_paused_report(row['id'],1,personal_queue=False)
    elif transition=='delete':
        with db.get_conn() as conn:
            assert db._soft_delete_report(conn,dict(id=row['id'],user_id=1,status='completed'),deleted_by=1,reason='user')
    else:db.update_report_status(report_id=row['id'],status=transition)
    assert invalidated==[row['id']]
    assert not list(partial.directory().iterdir())
    assert all(redis.get(partial.key(row['id'],fmt)) is None for fmt in ('xlsx','json'))


def test_rollback_does_not_invalidate_existing_cache(lifecycle):
    row,redis,invalidated,raw=lifecycle
    artifact=partial.get_export(row['id'],'xlsx')
    raw.commit.side_effect=RuntimeError('test commit failed')
    with pytest.raises(RuntimeError):db.reset_report_terminal_state(row['id'])
    assert not invalidated and artifact.path.exists()


def test_results_revision_tracks_zero_cost_new_row_and_overwrite(monkeypatch):
    sql=sqlite3.connect(':memory:');sql.row_factory=sqlite3.Row
    sql.executescript('''CREATE TABLE reports(id TEXT,results_revision INTEGER DEFAULT 0,user_id INTEGER,
        access_group_id INTEGER,created_at TEXT,updated_at TEXT,prompt_tokens INTEGER DEFAULT 0,
        completion_tokens INTEGER DEFAULT 0,total_tokens INTEGER DEFAULT 0,finished_at TEXT,error_text TEXT,
        total_rows INTEGER DEFAULT 0,processed_rows INTEGER DEFAULT 0,progress_percent REAL,eta_seconds REAL,
        current_step TEXT,summary_json TEXT,success_rows INTEGER DEFAULT 0,failed_rows INTEGER DEFAULT 0,deleted_at TEXT);
        INSERT INTO reports(id,user_id) VALUES ('test-report',1);
        CREATE TABLE report_rows(report_id TEXT,row_number INTEGER,status TEXT,sentiment_label TEXT,negativity_score REAL,
        short_reason TEXT,key_topics_json TEXT,raw_response_json TEXT,custom_json TEXT,error_text TEXT,
        prompt_tokens INTEGER DEFAULT 0,completion_tokens INTEGER DEFAULT 0,total_tokens INTEGER DEFAULT 0,warnings_json TEXT);
        INSERT INTO report_rows(report_id,row_number,status) VALUES ('test-report',1,'pending'),('test-report',2,'pending');''')
    class Conn:
        def execute(self,text,args=()):
            cur=sql.execute(text,args)
            return SimpleNamespace(rowcount=cur.rowcount,fetchone=lambda:dict(r) if (r:=cur.fetchone()) else None)
    @contextmanager
    def connection():yield Conn()
    monkeypatch.setattr(db,'get_conn',connection)
    monkeypatch.setattr(db,'_invalidate_usage_cache',Mock())
    def revision():return dict(sql.execute('SELECT * FROM reports').fetchone())
    old=partial.version(revision())
    for number,text in [(1,'Первый результат'),(2,'Новая строка'),(1,'Заменённый результат')]:
        db.update_report_row_result(report_id='test-report',row_number=number,sentiment_label=None,
            negativity_score=None,short_reason=None,category=None,raw_response=None,error_text=None,custom_data={'Итог':text})
        current=partial.version(revision());assert current!=old;old=current
    db.reset_report_terminal_state('test-report')
    assert partial.version(revision())!=old
    assert sql.execute('SELECT total_tokens FROM reports').fetchone()[0]==0
    db.update_report_progress(report_id='test-report',total_rows=2,processed_rows=2,progress_percent=100,
        eta_seconds=None,current_step='Тестовый прогресс',summary_json='{"success_rows":2,"failed_rows":0}')
    before=partial.version(revision())
    db.update_report_progress(report_id='test-report',total_rows=2,processed_rows=2,progress_percent=100,
        eta_seconds=None,current_step='Другой heartbeat',summary_json='{"success_rows":2,"failed_rows":0}')
    assert partial.version(revision())==before
    sql.close()


@pytest.mark.parametrize('full_name',[None,'Тестов Тест Тестович'])
def test_auth_me_returns_only_own_full_name(full_name):
    get=Mock(return_value={'full_name':full_name,'employee_id':'test-private','job_title':'Тестовая должность'})
    me=function('main.py','me',UserMeResponse=UserMeResponse,get_user_by_id=get)
    value=me({'id':7,'username':'user.test','role':'user'}).model_dump()
    assert value['full_name']==full_name and value['username']=='user.test'
    assert 'employee_id' not in value and 'job_title' not in value


def test_snapshot_uses_one_repeatable_read_connection(monkeypatch):
    conn=Mock();conn.execute.return_value=Cursor(dict(id='test-report',status='paused'))
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(db,'get_conn',connection)
    with partial.snapshot('test-report') as (row,same):assert same is conn and row['id']=='test-report'
    assert conn.execute.call_args_list[0].args==('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY',)
