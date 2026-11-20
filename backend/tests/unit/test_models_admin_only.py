import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import json
import re
import pytest
from fastapi import Depends,FastAPI,HTTPException
from fastapi.testclient import TestClient
from app import config,model_catalog as models
from app.access_api import router
from app.schemas import ModelsResponse,ProvidersResponse,ProviderInfo
from .test_models_profiles import catalog_db,empty_catalog_db
from .test_model_admission import function


def test_empty_migration_seeds_legacy_catalog_once(empty_catalog_db,monkeypatch):
    monkeypatch.setattr(models, "initial_model_catalog", lambda: [("openai", "test-legacy-model")])
    with empty_catalog_db() as conn:
        conn.execute('DELETE FROM schema_migrations WHERE name=?',(models.MIGRATION,))
        models.init_schema(conn);models.init_schema(conn)
        assert conn.execute('SELECT COUNT(*) AS n FROM schema_migrations WHERE name=?',(models.MIGRATION,)).fetchone()['n']==1
    rows = models.all_models(fresh=True)
    assert [(row["model"], row["is_default"]) for row in rows] == [("test-legacy-model", True)]


def test_filled_catalog_without_marker_is_unchanged(catalog_db):
    old=models.all_models(fresh=True)
    with catalog_db() as conn:
        conn.execute('DELETE FROM schema_migrations WHERE name=?',(models.MIGRATION,));models.init_schema(conn)
    assert models.all_models(fresh=True)==old


def test_delete_last_unused_and_restart_stays_empty(catalog_db):
    for mid in [3,2,1]:models.delete({'id':1},mid)
    with catalog_db() as conn:models.init_schema(conn)
    assert models.all_models(fresh=True)==[]


def test_old_config_and_environment_are_ignored(monkeypatch,tmp_path):
    path=tmp_path/'providers.json'
    path.write_text(json.dumps({'providers':{'openai':{'label':'Тест','models':['test-json-default'],
        'models_env':'OPENAI_MODELS','base_url_env':'TEST_BASE_URL'}}}),encoding='utf-8')
    monkeypatch.setenv('AI_PROVIDERS_CONFIG_PATH',str(path));monkeypatch.setenv('OPENAI_MODELS','test-env-secret-value')
    loaded=config._load_provider_config()
    assert loaded['openai']['label']=='Тест' and loaded['openai']['base_url_env']=='TEST_BASE_URL'
    assert 'models' not in loaded['openai'] and 'models_env' not in loaded['openai']


@pytest.mark.asyncio
async def test_empty_start_is_422_before_any_file_or_queue(empty_catalog_db):
    start=function('main.py','start_job')
    with pytest.raises(HTTPException) as exc:
        await start(SimpleNamespace(provider='openai',model='test-any'),{'id':1})
    assert exc.value.status_code==422 and exc.value.detail==models.EMPTY_CATALOG_MESSAGE


def test_empty_public_endpoints_are_valid(empty_catalog_db):
    providers=function('main.py','providers',ProvidersResponse=ProvidersResponse,ProviderInfo=ProviderInfo,
        PROVIDER_CONFIG=config.PROVIDER_CONFIG)
    model_list=function('main.py','models',Query=lambda *args,**kw:None,ModelsResponse=ModelsResponse)
    assert providers().model_dump()=={'providers':[]}
    assert model_list('openai').model_dump()=={'provider':'openai','models':[],'entries':[]}


@pytest.mark.parametrize('identifier',['bge-m3','TEXT-EMBED-3','sdxl-turbo','image-pro','FLUX.1','qwen-VL-7b','perplexity-chat','SONAR','rerank-v2'])
def test_non_chat_catalog_patterns(identifier):
    entry=models.chat_suitability(identifier)
    assert entry['is_chat_candidate'] is False and entry['hidden_reason']


@pytest.mark.parametrize('identifier',['qwen3.8-27b','cotype-pro-2.5','glm-4.6-357b'])
def test_chat_catalog_candidates(identifier):assert models.chat_suitability(identifier)['is_chat_candidate']


@pytest.mark.asyncio
async def test_batch_add_checks_each_and_first_is_default(empty_catalog_db,monkeypatch):
    checked=[]
    async def check(user,mid):
        await asyncio.sleep(0)
        checked.append(mid)
        return next(r for r in models.all_models(fresh=True) if r['id']==mid)
    monkeypatch.setattr(models,'check',check)
    result=await models.add_batch({'id':1},{'provider':'openai','models':['test-first','test-second']})
    assert all(r['created'] for r in result['results']) and set(checked)=={1,2}
    assert [r['model'] for r in models.all_models(fresh=True) if r['is_default']]==['test-first']


@pytest.mark.asyncio
async def test_batch_duplicate_does_not_overwrite_and_check_failure_is_explicit(catalog_db,monkeypatch):
    async def fail(*args):raise HTTPException(503,'test provider unavailable')
    monkeypatch.setattr(models,'check',fail)
    result=await models.add_batch({'id':1},{'provider':'openai','models':['test-fast','test-new']})
    assert not result['results'][0]['created'] and 'уже добавлена' in result['results'][0]['error']
    assert result['results'][1]['created'] and result['results'][1]['error']
    assert models.all_models(fresh=True)[0]['is_default']


@pytest.mark.parametrize('role',['user','manager'])
def test_batch_api_requires_global_admin(role):
    def current():return {'id':1,'role':role}
    def admin(user=Depends(current)):
        if user['role']!='admin':raise HTTPException(403,'Требуется администратор')
        return user
    app=FastAPI();app.include_router(router(current,admin,current))
    with TestClient(app) as client:
        assert client.post('/api/admin/models/batch',json={'provider':'openai','models':['test-model']}).status_code==403


@pytest.mark.parametrize('extension',['xlsx','json'])
def test_final_slice_filename_retains_date_and_time(extension):
    from app.partial_exports import Export
    item=Export(Path('unused'),'2026-11-11T12:30:00+00:00')
    from app.download_utils import sanitize_download_filename as sanitize
    actual=sanitize(None,fallback=item.fallback_name('8eb959a3-test'),extension=extension)
    assert actual==f'report_8eb959a3_срез_11-11_15-30.{extension}'
