import asyncio
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock

from fastapi import HTTPException
import pytest

from app import db,events
from app.services import job_manager as module


@pytest.mark.parametrize('code,expected',[('limit_exhausted','report.limit'),('no_access','report.access_paused')])
def test_access_revocation_does_not_count_as_budget_stop(monkeypatch,code,expected):
    @contextmanager
    def connection():yield Mock()
    monkeypatch.setattr(db,'get_conn',connection)
    captured=Mock();monkeypatch.setattr(events,'record_event',captured)
    monkeypatch.setattr(module,'update_report_progress',Mock())
    manager=SimpleNamespace(pause=AsyncMock(),_progress_payload=lambda j:{'progress_percent':0},_build_summary_from_job=lambda j:{})
    job=SimpleNamespace(id='test',report_id='test',user_id=1,access_group_id=2,total=100,processed=0,add_log=Mock())
    async def run():
        with pytest.raises(module.QuotaPause):
            await module.JobManager._pause_for_quota(manager,job,HTTPException(409,{'code':code,'message':'Проверка'}))
    asyncio.run(run())
    assert captured.call_args.args[1]==expected
