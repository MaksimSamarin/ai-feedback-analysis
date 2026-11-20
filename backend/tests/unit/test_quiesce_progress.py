import asyncio
import json
from types import SimpleNamespace

import pytest

from app.services import job_manager as module


@pytest.mark.parametrize('status,quota', [('paused',False),('canceled',False),('paused',True)])
def test_quiesce_flushes_after_inflight_and_preserves_limit_message(monkeypatch,status,quota):
    writes=[]
    monkeypatch.setattr(module,'update_report_progress',lambda **kwargs:writes.append(kwargs))
    async def scenario():
        job=SimpleNamespace(quiesce_event=asyncio.Event(),run_event=asyncio.Event(),status=module.JobStatus.running,
            quota_error=Exception('limit') if quota else None,current_step='Пауза: лимит; Сброс: 01.12.2026 00:00 МСК',
            processed=4,total=100,report_id='test-report',groups_total=0,groups_processed=0)
        async def inflight():
            await asyncio.sleep(0)
            job.processed=7
        job.task=asyncio.create_task(inflight())
        manager=module.JobManager()
        monkeypatch.setattr(manager,'get',lambda _:job)
        monkeypatch.setattr(manager,'_progress_payload',lambda j:{'progress_percent':j.processed})
        monkeypatch.setattr(manager,'_build_summary_from_job',lambda j:{'success_rows':j.processed,'failed_rows':0})
        await manager.quiesce('test-job',status)
        assert writes[0]['processed_rows']==7
        assert json.loads(writes[0]['summary_json'])['success_rows']==7
        if quota: assert '01.12.2026 00:00 МСК' in writes[0]['current_step']
    asyncio.run(scenario())
