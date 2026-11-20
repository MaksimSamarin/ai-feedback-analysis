from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
import json
import os
import threading
import time
from unittest.mock import Mock

import pytest
from fastapi import BackgroundTasks, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from app import db, partial_exports as partial
from .test_model_admission import function

REAL_BUILD=partial.build


class Redis:
    def __init__(self): self.data={};self.expires={};self.mutex=threading.RLock();self.unavailable=False
    def get(self,key):
        with self.mutex:
            if self.unavailable:raise ConnectionError('test Redis unavailable')
            if self.expires.get(key,float('inf'))<=time.monotonic():self.delete(key)
            return self.data.get(key)
    def set(self,key,value,nx=False,ex=None):
        with self.mutex:
            if nx and self.get(key) is not None:return False
            if self.unavailable:raise ConnectionError('test Redis unavailable')
            self.data[key]=str(value)
            if ex is not None:self.expires[key]=time.monotonic()+float(ex)
            return True
    def delete(self,key):
        with self.mutex:
            if self.unavailable:raise ConnectionError('test Redis unavailable')
            result=int(key in self.data);self.data.pop(key,None);self.expires.pop(key,None);return result
    def eval(self,script,numkeys,*args):
        with self.mutex:
            keys=list(args[:numkeys]);values=list(args[numkeys:])
            if script==partial.RELEASE:
                return self.delete(keys[0]) if self.get(keys[0])==values[0] else 0
            if script==partial.RENEW:
                if self.get(keys[0])!=values[0]:return 0
                self.expires[keys[0]]=time.monotonic()+float(values[1]);return 1
            assert script==partial.PUBLISH
            if self.get(keys[0])!=values[0] or self.get(keys[1])!=values[1] or self.get(keys[3])!=values[4]:return [0,'']
            old=self.get(keys[2])
            if old and json.loads(old)['revision']>int(values[2]):return [0,'']
            self.set(keys[2],values[3],ex=int(values[5]));return [1,old or '']


@pytest.fixture
def cache(monkeypatch,tmp_path):
    row=dict(id='test-report',status='paused',results_revision=1,total_rows=2,processed_rows=1,
        success_rows=1,failed_rows=0,groups_total=0,groups_processed=0,provider='openai',model='test-model')
    redis=Redis();calls=[]
    monkeypatch.setattr(partial,'RESULTS_DIR',tmp_path)
    monkeypatch.setattr(partial,'PARTIAL_EXPORT_CACHE_ENABLED',True)
    monkeypatch.setattr(db,'get_redis_client',lambda:redis)
    monkeypatch.setattr(db,'get_report_any',lambda rid:deepcopy({**row,'id':rid}))
    @contextmanager
    def snapshot(rid):yield deepcopy({**row,'id':rid}),Mock()
    monkeypatch.setattr(partial,'snapshot',snapshot)
    def build(report,fmt,path,at,conn):
        calls.append((fmt,report['results_revision']))
        time.sleep(.025)
        path.write_text('snapshot:'+str(report['results_revision']),encoding='utf-8')
    monkeypatch.setattr(partial,'build',build)
    return row,redis,calls,tmp_path


def test_version_ignores_only_status_and_heartbeat(cache):
    row,*_=cache;initial=partial.version(row)
    assert partial.version({**row,'status':'running','updated_at':'later','current_step':'heartbeat'})==initial
    for field in partial.VERSION_FIELDS:
        assert partial.version({**row,field:row[field]+1})!=initial


def test_hit_never_reads_rows_or_builds(cache,monkeypatch):
    row,redis,calls,_=cache
    first=partial.get_export(row['id'],'xlsx')
    spy=Mock(side_effect=AssertionError('cache hit read report_rows'))
    monkeypatch.setattr(db,'iter_report_rows',spy)
    again=partial.get_export(row['id'],'xlsx')
    assert first==again and len(calls)==1
    spy.assert_not_called()
    assert json.loads(redis.get(partial.key(row['id'],'xlsx')))['captured_at']==first.captured_at


def test_five_parallel_requests_one_build(cache):
    row,_,calls,_=cache;barrier=threading.Barrier(5)
    def download(_):barrier.wait();return partial.get_export(row['id'],'xlsx')
    with ThreadPoolExecutor(5) as pool:result=list(pool.map(download,range(5)))
    assert len(calls)==1 and len({r.path for r in result})==1
    assert not list(partial.directory().glob('*.tmp'))


def test_new_result_replaces_old_file_and_owner_admin_share(cache):
    row,redis,calls,_=cache
    old=partial.get_export(row['id'],'xlsx')
    row.update(results_revision=2,processed_rows=2,success_rows=2)
    new=partial.get_export(row['id'],'xlsx')
    assert old.path!=new.path and not old.path.exists() and new.path.exists()
    assert len(list(partial.directory().glob('*.xlsx')))==1 and len(calls)==2
    assert partial.get_export(row['id'],'xlsx').path==new.path


def test_build_failure_releases_only_own_lease(cache,monkeypatch):
    row,redis,_,_=cache
    def fail(*args):args[2].write_text('incomplete');raise RuntimeError('test build failure')
    monkeypatch.setattr(partial,'build',fail)
    with pytest.raises(RuntimeError):partial.get_export(row['id'],'xlsx')
    assert not any(k.startswith('partial_export:lock:') for k in redis.data)
    assert not list(partial.directory().iterdir())
    redis.set('test-lease','new-owner')
    assert redis.eval(partial.RELEASE,1,'test-lease','old-owner')==0 and redis.get('test-lease')=='new-owner'


@pytest.mark.parametrize('mode',['none','unavailable','disabled'])
def test_redis_fallback_is_ephemeral(cache,monkeypatch,mode):
    row,redis,_,root=cache
    if mode=='none':monkeypatch.setattr(db,'get_redis_client',lambda:None)
    elif mode=='disabled':monkeypatch.setattr(partial,'PARTIAL_EXPORT_CACHE_ENABLED',False)
    else:redis.unavailable=True
    value=partial.get_export(row['id'],'json')
    assert value.temporary and value.path.parent==root and value.path.exists()


def test_wait_timeout_has_human_error(cache,monkeypatch):
    row,redis,_,_=cache
    redis.set(f'partial_export:lock:{row["id"]}:xlsx:{partial.version(row)}','another-owner',ex=60)
    monkeypatch.setattr(partial,'PARTIAL_EXPORT_WAIT_SEC',.02)
    with pytest.raises(HTTPException) as exc:partial.get_export(row['id'],'xlsx')
    assert exc.value.status_code==503 and 'через минуту' in exc.value.detail


def test_lost_lease_cannot_publish_or_release_new_owner(cache,monkeypatch):
    row,redis,_,_=cache;original=partial.build
    name=f'partial_export:lock:{row["id"]}:xlsx:{partial.version(row)}'
    def build(*args):original(*args);redis.set(name,'new-owner',ex=60)
    monkeypatch.setattr(partial,'build',build)
    monkeypatch.setattr(partial,'PARTIAL_EXPORT_WAIT_SEC',.05)
    with pytest.raises(HTTPException):partial.get_export(row['id'],'xlsx')
    assert redis.get(name)=='new-owner'
    assert not list(partial.directory().iterdir())


def test_invalidation_of_both_formats_is_scoped(cache):
    row,redis,_,_=cache
    for fmt in ('xlsx','json'):partial.get_export(row['id'],fmt)
    other=partial.get_export(row['id']+'_other','xlsx')
    partial.invalidate(row['id'])
    assert other.path.exists()
    assert all(redis.get(partial.key(row['id'],f)) is None for f in ('xlsx','json'))
    assert list(partial.directory().iterdir())==[other.path]


def test_invalidation_during_build_cannot_publish_old_snapshot(cache,monkeypatch):
    row,redis,calls,_=cache;original=partial.build
    def build(*args):
        original(*args)
        if len(calls)==1:
            partial.invalidate(row['id']);row['results_revision']+=1
    monkeypatch.setattr(partial,'build',build)
    value=partial.get_export(row['id'],'xlsx')
    assert value.path.read_text()=='snapshot:2' and len(calls)==2
    assert len(list(partial.directory().glob('*.xlsx')))==1


def test_completed_during_build_leaves_no_cache(cache,monkeypatch):
    row,redis,_,_=cache;original=partial.build
    def build(*args):
        original(*args);row['status']='completed';partial.invalidate(row['id'])
    monkeypatch.setattr(partial,'build',build)
    with pytest.raises(HTTPException) as exc:partial.get_export(row['id'],'xlsx')
    assert exc.value.status_code==409 and not list(partial.directory().iterdir())


def test_old_builder_cannot_replace_newer_version(cache,monkeypatch):
    row,redis,calls,_=cache;original=partial.build
    started=threading.Event();release=threading.Event()
    def build(report,*args):
        if report['results_revision']==1:
            started.set();assert release.wait(5)
        original(report,*args)
    monkeypatch.setattr(partial,'build',build)
    with ThreadPoolExecutor(2) as pool:
        old=pool.submit(partial.get_export,row['id'],'xlsx')
        assert started.wait(5);row['results_revision']=2
        new=partial.get_export(row['id'],'xlsx');release.set()
        assert old.result(5).path==new.path
    assert len(calls)==2 and len(list(partial.directory().glob('*.xlsx')))==1


def test_cleanup_ttl_orphan_and_recent(cache):
    row,redis,_,_=cache
    expired=partial.get_export(row['id'],'xlsx');orphan=partial.get_export('test-orphan','json')
    recent=partial.get_export('test-recent','xlsx')
    redis.delete(partial.key('test-orphan','json'))
    now=time.time()
    os.utime(expired.path,(now-7*3600,now-7*3600));os.utime(orphan.path,(now-601,now-601))
    assert partial.cleanup()==2
    assert not expired.path.exists() and not orphan.path.exists() and recent.path.exists()
    assert redis.get(partial.key(row['id'],'xlsx')) is None


def test_actual_xlsx_summary_and_raw(cache,monkeypatch):
    row,redis,_,_=cache
    row.update(group_by_column='Канал',groups_processed=1,groups_total=2)
    monkeypatch.setattr(partial,'build',REAL_BUILD)
    rows=[dict(row_number=1,status='done',custom_json='{"Итог":"Тест"}',group_key='A')]
    spy=Mock(side_effect=lambda *a,**kw:iter(rows))
    monkeypatch.setattr(db,'iter_report_rows',spy)
    monkeypatch.setattr(db,'get_report_summary_agg',lambda *a,**kw:dict(total_rows=2,processed_rows=1,success_rows=1,failed_rows=0))
    value=partial.get_export(row['id'],'xlsx')
    from openpyxl import load_workbook
    workbook=load_workbook(value.path,read_only=True)
    summary=dict(workbook['summary'].values)
    assert 'обработано 1 из 2; групп 1 из 2' in summary['Срез'] and 'МСК' in summary['Срез']
    workbook.close()
    count=spy.call_count
    partial.get_export(row['id'],'xlsx');assert spy.call_count==count
    raw=partial.get_export(row['id'],'json');assert json.loads(raw.path.read_text(encoding='utf-8'))
    assert '_срез_' in value.fallback_name(row['id'])


@pytest.mark.parametrize('name',['download_report_partial_xlsx','download_report_partial_raw','admin_download_report_partial_xlsx','admin_download_report_partial_raw'])
def test_unauthorized_report_before_cache(name):
    cache=Mock(side_effect=AssertionError('cache accessed without rights'))
    route=function('main.py',name,BackgroundTasks=BackgroundTasks,Query=Query,FileResponse=FileResponse,
        HTTPException=HTTPException,require_admin=lambda:None,get_report=Mock(return_value=None),get_report_any=Mock(return_value=None),_partial_download=cache)
    with pytest.raises(HTTPException) as exc:route('test-foreign',BackgroundTasks(),None,{'id':7})
    assert exc.value.status_code==404;cache.assert_not_called()


@pytest.mark.asyncio
async def test_open_response_survives_cache_cleanup_and_custom_name(cache):
    row,_,_,_=cache
    helper=function('main.py','_partial_download',FileResponse=FileResponse,StreamingResponse=StreamingResponse,
        HTTPException=HTTPException,os=os,_sanitize_download_filename=lambda name,**kw:(name or kw['fallback'])+'.'+kw['extension'],
        _delete_path_later=lambda path:path.unlink(missing_ok=True))
    background=BackgroundTasks();response=helper(row,'xlsx',background,'test-custom')
    assert 'test-custom.xlsx' in response.headers['content-disposition']
    # На Windows нельзя unlink открытого файла; читаемый fd всё равно сохраняется.
    assert response.headers['x-partial-snapshot']
    partial.invalidate(row['id'])
    content=b''.join([chunk async for chunk in response.body_iterator])
    await background()
    assert content==b'snapshot:1'
    partial.invalidate(row['id'])


@pytest.mark.asyncio
async def test_disconnected_response_cleans_ephemeral_file(cache,monkeypatch):
    row,_,_,root=cache
    monkeypatch.setattr(partial,'PARTIAL_EXPORT_CACHE_ENABLED',False)
    helper=function('main.py','_partial_download',FileResponse=FileResponse,StreamingResponse=StreamingResponse,
        HTTPException=HTTPException,os=os,_sanitize_download_filename=lambda name,**kw:kw['fallback']+'.'+kw['extension'],
        _delete_path_later=lambda path:path.unlink(missing_ok=True))
    response=helper(row,'xlsx',BackgroundTasks(),None)
    assert len(list(root.glob('*.xlsx')))==1
    async def send(message):raise OSError('test disconnected')
    async def receive():return {'type':'http.disconnect'}
    try:await response({'type':'http','asgi':{'spec_version':'2.4'}},receive,send)
    except Exception:pass
    assert not list(root.glob('*.xlsx'))
