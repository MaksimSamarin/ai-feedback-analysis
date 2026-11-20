import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
import pytest
from fastapi import Depends, HTTPException
from app import model_catalog
from app.schemas import StartJobRequest, JobStatus


def function(file,name,**namespace):
    node=next(n for n in ast.parse((Path(__file__).parents[2]/'app'/file).read_text(encoding='utf-8')).body
              if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name==name)
    node.decorator_list=[]
    values=dict(asyncio=asyncio,Depends=Depends,get_current_user=lambda:None,StartJobRequest=StartJobRequest,
                JobStatus=JobStatus,logger=Mock(),**namespace)
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node],type_ignores=[])),file,'exec'),values)
    return values[name]


@pytest.mark.asyncio
@pytest.mark.parametrize('resume',[True,False])
async def test_real_start_resume_handlers_reject_disabled_before_work(monkeypatch,resume):
    monkeypatch.setattr(model_catalog,'all_models',lambda **kwargs:[{'provider':'openai','model':'test-other','is_enabled':True}])
    async def owner(*args):return {'status':'paused','provider':'openai','model':'test-disabled'}
    route=function('main.py','resume_job' if resume else 'start_job',_require_job_owner_async=owner)
    with pytest.raises(HTTPException) as exc:
        await route('test-job',{'id':1}) if resume else await route(SimpleNamespace(provider='openai',model='test-disabled'),{'id':1})
    assert exc.value.status_code==(409 if resume else 422) and 'test-disabled' in exc.value.detail


@pytest.mark.asyncio
@pytest.mark.parametrize('status',['queued','running'])
async def test_worker_pauses_queued_but_running_is_not_interrupted(monkeypatch,status):
    gate=Mock(side_effect=HTTPException(409,'Модель отключена. Создайте отчёт заново'))
    monkeypatch.setattr(model_catalog,'require_enabled',gate)
    update=Mock()
    row={'status':status,'provider':'openai','model':'test-disabled'}
    run=function('worker.py','_run_payload',get_report=lambda *args:row,
        update_report_status=update,claim_running_lease=lambda *args:False,RUNNING_LEASE_TTL_SEC=60)
    await run({'report_id':'test-report','job_id':'test-job','user_id':1})
    if status=='queued':
        gate.assert_called_once()
        assert update.call_args.kwargs['status']=='paused' and 'отключена' in update.call_args.kwargs['current_step']
    else:gate.assert_not_called();update.assert_not_called()
