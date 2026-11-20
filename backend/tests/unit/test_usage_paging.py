from contextlib import contextmanager
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import access_api


def test_usage_paging_never_silently_truncates_or_truncates_totals(monkeypatch):
    class Connection:
        def execute(self,sql,args=()):
            if 'COUNT(DISTINCT user_id)' in sql:
                return Mock(fetchone=lambda:{'reports':99,'total_tokens':999})
            assert 'LIMIT ? OFFSET ?' in sql and args[-2:]==[3,0]
            return Mock(fetchall=lambda:[{'month':'2026-11','total_tokens':n} for n in (10,20,30)])
    @contextmanager
    def connection():yield Connection()
    monkeypatch.setattr(access_api.db,'get_conn',connection)
    monkeypatch.setattr(access_api.db,'get_reliable_since',lambda c:'2026-11-08T10:00:00+00:00')
    user=lambda:{'id':1,'role':'admin'}
    app=FastAPI();app.include_router(access_api.router(user,user,user))
    result=TestClient(app).get('/api/admin/usage?limit=2').json()
    assert result['has_more'] is True and result['truncated'] is True
    assert len(result['rows'])==2 and result['totals']=={'reports':99,'total_tokens':999}
