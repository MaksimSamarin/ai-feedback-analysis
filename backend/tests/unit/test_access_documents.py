import ast
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock
import uuid

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
import pytest

from app import access, user_rules
from app.access_api import router
from app.logging_utils import (current_context, set_request_id, reset_request_id, set_user_context, reset_user_context)
from app.schemas import StartJobRequest


@pytest.fixture
def document_client(monkeypatch,tmp_path):
    main_file=Path(__file__).resolve().parents[2]/'app/main.py'
    source=ast.parse(main_file.read_text(encoding='utf-8'))
    # Используются реальные middleware и обработчик, без запуска серверного lifespan/БД.
    selected=[]
    for name in ['request_context_middleware','require_access','_parse_release_notes','release_notes']:
        node=next(n for n in source.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==name)
        node.decorator_list=[];selected.append(node)
    user={'id':7,'username':'user.test','role':'user'}
    release_file=tmp_path/'release-notes.md'
    release_file.write_text('## 2.2.0\n\nПроверка заметок.\n',encoding='utf-8')
    def auth(request:Request):
        if request.cookies.get('test-session')!='valid':raise HTTPException(401,'Требуется авторизация')
        return user
    namespace=dict(__file__=str(main_file),Any=Any,Path=Path,Depends=Depends,HTTPException=HTTPException,
        JSONResponse=JSONResponse,datetime=datetime,asyncio=asyncio,uuid=uuid,SESSION_COOKIE_NAME='test-session',
        _parse_token=lambda h:None,_get_session_user_cached=lambda token:user if token=='valid' else None,
        set_request_id=set_request_id,reset_request_id=reset_request_id,current_context=current_context,
        set_user_context=set_user_context,reset_user_context=reset_user_context,access=access,
        _should_log_access_request=lambda *args:False,get_current_user=auth,logger=Mock(),
        RELEASE_NOTES_PATH=release_file)
    exec(compile(ast.fix_missing_locations(ast.Module(body=selected,type_ignores=[])),str(main_file),'exec'),namespace)
    monkeypatch.setattr(access,'has_access',lambda user:False)
    app=FastAPI();app.middleware('http')(namespace['request_context_middleware'])
    app.get('/api/release-notes')(namespace['release_notes'])
    app.include_router(router(auth,auth,namespace['require_access']))
    @app.api_route('/api/reports',methods=['GET','POST'])
    @app.get('/api/admin/stats')
    @app.post('/api/jobs')
    @app.get('/api/unregistered-protected')
    def closed():return {'leak':True}
    with TestClient(app) as client:yield client


@pytest.mark.parametrize('path',['/api/release-notes','/api/access/rules'])
def test_documents_available_authenticated_without_group(document_client,path):
    document_client.cookies.set('test-session','valid')
    response=document_client.get(path)
    assert response.status_code==200,response.text
    if path.endswith('/rules'):assert 'content_md' in response.json() and '20–50' in response.json()['content_md']
    else:assert response.json()['releases']


@pytest.mark.parametrize('path',['/api/release-notes','/api/access/rules'])
def test_documents_still_require_login(document_client,path):
    assert document_client.get(path).status_code==401


@pytest.mark.parametrize('method,path',[('GET','/api/reports'),('POST','/api/reports'),('POST','/api/jobs'),
    ('GET','/api/admin/stats'),('GET','/api/unregistered-protected'),('POST','/api/release-notes')])
def test_other_routes_keep_no_access(document_client,method,path):
    document_client.cookies.set('test-session','valid')
    response=document_client.request(method,path)
    assert response.status_code==403 and response.json()['detail']['code']=='no_access'


@pytest.mark.parametrize('mb,days,expected',[(200,30,'30 дней'),(80,1,'1 дня'),(1,0,'отключена')])
def test_rules_use_actual_settings(monkeypatch,mb,days,expected):
    monkeypatch.setattr(user_rules,'MAX_UPLOAD_BYTES',mb*1024*1024)
    monkeypatch.setattr(user_rules,'PAUSED_REPORT_TTL_DAYS',days)
    text=user_rules.user_rules_content()
    assert f'— {mb} МБ' in text and expected in text
    assert '{{' not in text and text.count('**')==16
    assert 'личный лимит' not in text and 'После пополнения нажмите «Продолжить»' in text
    assert 'может снова расходовать квоту' in text


def test_sample_and_recommended_sizes_fit_request_constraints():
    constraints=StartJobRequest.model_fields['max_reviews'].metadata
    minimum=next(v.ge for v in constraints if hasattr(v,'ge'))
    maximum=next(v.le for v in constraints if hasattr(v,'le'))
    assert minimum<=20<=50<=100000<=maximum==1000000
