from contextlib import contextmanager
import json
import re
import sqlite3
from unittest.mock import Mock
import httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException
from fastapi.testclient import TestClient
from app import db, model_catalog as models, user_profile, governance
from app import access, reporting
from app.access_api import router


@pytest.fixture
def empty_catalog_db(monkeypatch):
    sql = sqlite3.connect(':memory:',check_same_thread=False)
    sql.row_factory = sqlite3.Row
    sql.executescript('CREATE TABLE users(id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1); '
        'CREATE TABLE schema_migrations(name TEXT PRIMARY KEY,applied_at TEXT); CREATE TABLE reports(provider TEXT,model TEXT);')
    class Cursor:
        def __init__(self,c): self.c=c
        def fetchone(self):
            r=self.c.fetchone(); return self.row(r) if r else None
        def row(self,r):
            result=dict(r)
            for k in ('is_enabled','is_default','last_check_ok'):
                if k in result and result[k] is not None:result[k]=bool(result[k])
            return result
        def fetchall(self): return [self.row(r) for r in self.c.fetchall()]
    class Conn:
        def execute(self,text,args=()):
            if 'pg_advisory' in text: return Mock()
            text=text.replace('BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY','INTEGER PRIMARY KEY AUTOINCREMENT').replace(' FOR UPDATE','').replace(' ILIKE ',' LIKE ')
            return Cursor(sql.execute(text,args))
    @contextmanager
    def connection():
        try: yield Conn(); sql.commit()
        except Exception: sql.rollback(); raise
    monkeypatch.setattr(db,'get_conn',connection)
    monkeypatch.setattr(models,'audit',Mock())
    monkeypatch.setattr(models, "initial_model_catalog", lambda: [])
    with connection() as conn: models.init_schema(conn)
    models.invalidate()
    yield connection
    models.invalidate(); sql.close()


@pytest.fixture
def catalog_db(empty_catalog_db):
    for provider,model in [('openai','test-fast'),('openai','test-accurate'),('ollama','test-local')]:
        models.save({'id':1},{'provider':provider,'model':model})
    return empty_catalog_db


def test_explicit_catalog_survives_restart(catalog_db,monkeypatch):
    rows=models.all_models(fresh=True)
    assert [r['model'] for r in rows]==['test-fast','test-accurate','test-local']
    assert [r['model'] for r in rows if r['is_default']]==['test-fast']
    monkeypatch.setenv('OPENAI_MODELS','test-new-env')
    with catalog_db() as conn: models.init_schema(conn)
    assert [r['model'] for r in models.all_models(fresh=True)]==[r['model'] for r in rows]


def test_one_default_and_transaction_rollback(catalog_db):
    user={'id':1}
    with pytest.raises(HTTPException) as exc: models.save(user,{'is_enabled':False},1)
    assert exc.value.status_code==409
    models.save(user,{'is_default':True},2)
    models.save(user,{'is_enabled':False},1)
    rows=models.all_models(fresh=True)
    assert sum(r['is_default'] for r in rows)==1 and rows[1]['is_default']
    assert {r['model'] for r in models.public_models('openai')}=={'test-accurate'}
    assert not {'created_by','last_check_message','provider'} & models.public_models('openai')[0].keys()


@pytest.mark.parametrize('resume,status',[(False,422),(True,409)])
@pytest.mark.parametrize('model',['unknown','test-accurate'])
def test_disabled_unknown_rejected(catalog_db,resume,status,model):
    models.save({'id':1},{'is_enabled':False},2)
    with pytest.raises(HTTPException) as exc: models.require_enabled('openai',model,resume=resume)
    assert exc.value.status_code==status and model in exc.value.detail


def test_used_model_cannot_delete_even_when_disabled(catalog_db):
    with catalog_db() as conn: conn.execute('INSERT INTO reports VALUES (?,?)',('openai','test-accurate'))
    with pytest.raises(HTTPException) as exc: models.delete({'id':1},2)
    assert exc.value.status_code==409 and 'отключите' in exc.value.detail
    assert models.delete({'id':1},3)=={'deleted':True}


@pytest.mark.asyncio
async def test_check_redacts_key_and_extra_headers(catalog_db,monkeypatch,caplog):
    monkeypatch.setenv('OPENAI_API_KEY','fictional-check-key')
    monkeypatch.setenv('OPENAI_EXTRA_HEADERS_JSON','{"X-Test":"fictional-header-secret"}')
    real=httpx.AsyncClient
    def respond(request):
        body=json.loads(request.content)
        assert body['max_tokens']<=16 and len(body['messages'])==1
        return httpx.Response(400,json={'error':{'message':'fictional-check-key fictional-header-secret https://provider.invalid/v1 '+('bad '*200)}})
    monkeypatch.setattr(models.httpx,'AsyncClient',lambda **kwargs:real(transport=httpx.MockTransport(respond),**kwargs))
    monkeypatch.setattr(models,'get_provider_base_url',lambda *args:'https://provider.invalid/v1')
    row=await models.check({'id':1},2)
    assert not row['last_check_ok'] and len(row['last_check_message'])<=500
    text=json.dumps(row)+caplog.text+str(models.audit.call_args)
    assert 'fictional-check-key' not in text and 'fictional-header-secret' not in text
    assert 'https://provider.invalid' not in text


def test_provider_address_is_masked_in_service_log(monkeypatch):
    from app.logging_utils import _redact
    monkeypatch.setenv('OPENAI_BASE_URL','https://provider.invalid/v1')
    assert 'provider.invalid' not in _redact('HTTP Request: POST https://provider.invalid/v1/chat/completions')


@pytest.mark.parametrize('role',['user','manager'])
@pytest.mark.parametrize('method,path',[('GET','/api/admin/models'),('POST','/api/admin/models'),
    ('PATCH','/api/admin/models/1'),('DELETE','/api/admin/models/1'),('POST','/api/admin/models/1/check'),
    ('GET','/api/admin/models/catalog?provider=openai')])
def test_models_routes_require_global_admin(role,method,path):
    def current(): return {'id':1,'role':role}
    def admin(user=Depends(current)):
        if user['role']!='admin': raise HTTPException(403,'Требуются права администратора')
        return user
    app=FastAPI();app.include_router(router(current,admin,current))
    with TestClient(app) as client: assert client.request(method,path,json={} if method in {'POST','PATCH'} else None).status_code==403


@pytest.mark.parametrize('claims,name',[
    ({'fullname':' Тестов Тест Тестович ','name':'Другое'},'Тестов Тест Тестович'),
    ({'family_name':'Тестов','given_name':'Тест','middle_name':'Тестович'},'Тестов Тест Тестович'),
    ({'name':'Тестовый Пользователь'},'Тестовый Пользователь'),({},None)])
def test_profile_fallback_and_phone_omitted(claims,name):
    value=user_profile.profile_claims({**claims,'phone_number':'fictional-phone','employeeID':'test-employee'})
    assert value['full_name']==name and value['employee_id']=='test-employee'
    assert 'phone_number' not in value


def test_profile_sync_whitelist_empty_preserves(monkeypatch):
    sql=sqlite3.connect(':memory:')
    sql.execute('CREATE TABLE users(id INTEGER,full_name TEXT,job_title TEXT,company TEXT,employee_id TEXT,profile_synced_at TEXT)')
    sql.execute("INSERT INTO users(id) VALUES (1)")
    @contextmanager
    def connection(): yield sql
    monkeypatch.setattr(db,'get_conn',connection)
    user_profile.sync(1,dict(full_name='Тестов Тест',job_title='Тестовая должность',company='Тестовая компания',employee_id='test-employee',phone_number='fictional'))
    user_profile.sync(1,dict(full_name='',job_title='Новая тестовая должность',company=None))
    row=sql.execute('SELECT * FROM users').fetchone()
    assert row[1:5]==('Тестов Тест','Новая тестовая должность','Тестовая компания','test-employee')
    assert row[5] and 'phone' not in str(row)


def test_profile_columns_do_not_expand_scope():
    assert 'employee_id' in user_profile.columns(True)
    for projection in [user_profile.columns(False),user_profile.columns(False,groups=(1,))]:
        assert 'employee_id' not in projection
    assert 'pm.group_id IN (1)' in user_profile.columns(False,groups=(1,))
    assert 'nm.group_id IN (1)' in user_profile.scoped_name(governance.Scope(False,(1,)))


@pytest.mark.parametrize('admin',[True,False])
def test_actual_member_request_projections(catalog_db,monkeypatch,admin):
    with catalog_db() as conn:
        for column in ('username','role','full_name','job_title','company','employee_id'):
            conn.execute(f'ALTER TABLE users ADD COLUMN {column} TEXT')
        conn.execute('ALTER TABLE users ADD COLUMN is_test_fixture BOOLEAN DEFAULT FALSE')
        conn.execute("UPDATE users SET username='lead.test',role=?,full_name='Тестов Тест',job_title='Тестовая должность',company='Тестовая компания',employee_id='test-employee'",('admin' if admin else 'user',))
        conn.execute('CREATE TABLE access_groups(id INTEGER,is_active BOOLEAN)')
        conn.execute('INSERT INTO access_groups VALUES (1,TRUE)')
        conn.execute('CREATE TABLE access_group_members(group_id INTEGER,user_id INTEGER,role_in_group TEXT)')
        conn.execute("INSERT INTO access_group_members VALUES (1,1,'manager')")
        conn.execute('CREATE TABLE access_requests(id INTEGER,user_id INTEGER,requested_group_id INTEGER,status TEXT,decided_by INTEGER)')
        conn.execute("INSERT INTO access_requests VALUES (1,1,1,'pending',NULL)")
        conn.execute("INSERT INTO users(id,username,role,full_name) VALUES (2,'outside.test','user','Тестов Внешний')")
    monkeypatch.setattr(access,'usage_for',lambda *args:(0,None))
    for response in [access.members({'id':1},1),access.requests_for({'id':1})]:
        assert response[0]['full_name']=='Тестов Тест'
        assert ('employee_id' in response[0])==admin
        assert response[0]['company']=='Тестовая компания'
    found=access.search_users({'id':1},'Тестов')
    assert {r['id'] for r in found}==({1,2} if admin else {1})
    assert 'employee_id' not in str(found)


@pytest.mark.parametrize('admin',[True,False])
def test_xlsx_employee_projection(admin):
    from io import BytesIO
    from openpyxl import load_workbook
    data={'period':{},'reliable_since':None,'summary':{'unknown_row_metrics':0},
        'users':[{'username':'user.test','full_name':'Тестов Тест','employee_id':'test-employee'}]}
    workbook=load_workbook(BytesIO(reporting.xlsx_report(data,'users',admin=admin)),read_only=True)
    cells=str(list(workbook['Отчёт'].values));workbook.close()
    assert ('test-employee' in cells)==admin and ('Идентификатор сотрудника' in cells)==admin
    assert 'ФИО' in cells and 'Логин' in cells


def test_cache_avoids_repeated_db_reads(catalog_db,monkeypatch):
    models.invalidate();real=db.get_conn;calls=[]
    @contextmanager
    def counted():
        calls.append(1)
        with real() as conn:yield conn
    monkeypatch.setattr(db,'get_conn',counted)
    monkeypatch.setattr(models.time,'monotonic',lambda:100)
    for _ in range(100): models.public_models('openai')
    assert len(calls)==1
    monkeypatch.setattr(models.time,'monotonic',lambda:111)
    models.public_models('openai')
    assert len(calls)==2


def test_environment_is_not_a_catalog_source(empty_catalog_db,monkeypatch):
    from app import config
    monkeypatch.setenv('OPENAI_MODELS','test-env-first,test-env-second')
    assert all('models' not in p and 'models_env' not in p for p in config._load_provider_config().values())
    with empty_catalog_db() as conn:
        models.init_schema(conn)
    assert models.all_models(fresh=True)==[]


@pytest.mark.parametrize("claims_override", [
    {}, {"iss": "https://other.invalid"}, {"aud": ["test-client", "other-client"]}, {"nonce": "different-nonce"},
])
def test_sso_checks_signed_identity_and_reads_only_profile_fields(monkeypatch, claims_override):
    import time
    from authlib.jose import JsonWebKey, jwt
    from app import config, sso

    issuer = "https://idp.invalid"
    monkeypatch.setattr(config, "SSO_ISSUER_URL", issuer)
    monkeypatch.setattr(config, "SSO_CLIENT_ID", "test-client")
    monkeypatch.setattr(config, "SSO_TOKEN_AUTH_METHOD", "client_secret_post")
    now = int(time.time())
    claims = {
        "iss": issuer, "sub": "test-sub", "aud": "test-client", "nonce": "test-nonce",
        "iat": now, "exp": now + 300, "fullname": "Тестов Тест Тестович",
        "employeeID": "test-employee", "phone_number": "fictional-phone", **claims_override,
    }
    key = JsonWebKey.generate_key("RSA", 2048, is_private=True, options={"kid": "test-key"})
    id_token = jwt.encode({"alg": "RS256", "kid": "test-key"}, claims, key).decode()
    responses = {
        issuer + "/token": {"id_token": id_token},
        issuer + "/jwks": {"keys": [key.as_dict(is_private=False)]},
    }
    request = Mock(side_effect=lambda method, url, **kwargs: responses[url])
    monkeypatch.setattr(sso, "_request_json", request)
    monkeypatch.setattr(sso, "_get_metadata", lambda: {
        "token_endpoint": issuer + "/token", "jwks_uri": issuer + "/jwks",
        "id_token_signing_alg_values_supported": ["RS256"],
    })
    flow = {"nonce": "test-nonce", "verifier": "test-verifier"}
    if claims_override:
        with pytest.raises(sso.SSOError):
            sso.sso_exchange_code("test-code", flow)
        return
    identity = sso.sso_exchange_code("test-code", flow)
    assert identity["subject"] == "test-sub"
    assert identity["profile"]["full_name"] == claims["fullname"]
    assert identity["profile"]["employee_id"] == "test-employee"
    assert "phone_number" not in identity["profile"]
    assert request.call_count == 2
    assert request.call_args_list[0].kwargs["data"]["code_verifier"] == "test-verifier"


def test_profile_optional_index_failure_does_not_abort(monkeypatch,caplog):
    conn=Mock()
    def execute(sql,args=()):
        if 'DO $$' in sql:raise RuntimeError('index permission denied')
        return Mock()
    conn.execute.side_effect=execute
    user_profile.init_schema(conn)
    statements=[r.args[0] for r in conn.execute.call_args_list]
    assert 'ROLLBACK TO SAVEPOINT profile_trgm' in statements and 'RELEASE SAVEPOINT profile_trgm' in statements
    assert any('users_sso_profile_v2_2_0' in s for s in statements)


@pytest.mark.asyncio
async def test_catalog_marks_added_and_missing(catalog_db,monkeypatch):
    real=httpx.AsyncClient
    transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'data':[{'id':'test-fast'},{'id':'test-new'}]}))
    monkeypatch.setattr(models.httpx,'AsyncClient',lambda **kwargs:real(transport=transport,**kwargs))
    monkeypatch.setattr(models,'get_provider_base_url',lambda *args:'https://provider.invalid/v1')
    result=await models.catalog('openai')
    assert [{k:r[k] for k in ('model','already_added')} for r in result['models']]==[{'model':'test-fast','already_added':True},{'model':'test-new','already_added':False}]
    assert [m['model'] for m in result['missing']]==['test-accurate']
