from contextlib import contextmanager
from unittest.mock import Mock
import pytest
from fastapi import HTTPException
from app import access, db, governance, reporting
from app.schemas import ReportItem


@pytest.mark.parametrize('groups,last,expected',[
    ([{'id':1,'remaining':20},{'id':2,'remaining':10}],2,2),
    ([{'id':1,'remaining':20},{'id':2,'remaining':0}],2,1),
    ([{'id':1,'remaining':0},{'id':2,'remaining':None}],1,2),
    ([{'id':1,'remaining':0},{'id':2,'remaining':-10}],2,None),
    ([],None,None),
])
def test_default_quota(groups,last,expected):
    assert access.default_group(groups,last)==expected


def test_report_quota_fields_from_join(monkeypatch):
    row=dict(id='r',job_id='r',status='completed',created_at='2026-11-09',access_group_id=2,
             access_group_name='Розница',access_group_active=False,access_group_member=False)
    conn=Mock();conn.execute.return_value.fetchone.return_value=row
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(db,'get_conn',connection)
    result=ReportItem(**db.get_report('r',7)).model_dump()
    assert result['access_group_id']==2 and result['access_group_name']=='Розница'
    assert result['access_group_active'] is False and result['access_group_member'] is False
    query=conn.execute.call_args[0][0]
    assert 'LEFT JOIN access_groups' in query and 'r.user_id = ?' in query and 'r.deleted_at IS NULL' in query
    assert 'access_group_name' in db._REPORTS_LIST_SELECT
    row.update(access_group_id=None,access_group_name=None,access_group_active=None)
    assert ReportItem(**db.get_report('r',7)).access_group_name is None


@pytest.mark.parametrize('own',[True,False])
def test_request_for_additional_group(monkeypatch,own):
    conn=Mock();results=[{'id':7},None,{'id':2},{'group_id':2} if own else None,{'id':11}]
    conn.execute.side_effect=[Mock(fetchone=lambda r=r:r) for r in results]
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(db,'get_conn',connection);monkeypatch.setattr(access,'audit',Mock())
    data={'requested_group_id':2,'purpose':'Нужен доступ для сводки','instructions_ack':True}
    if own:
        with pytest.raises(HTTPException) as exc:access.request_access({'id':7},data)
        assert exc.value.status_code==409
    else:assert access.request_access({'id':7},data)['id']==11


def test_shared_user_foreign_group_still_forbidden(monkeypatch):
    scope=governance.Scope(False,(1,))
    monkeypatch.setattr(reporting,'require_management',lambda _:scope)
    monkeypatch.setattr(governance,'require_management',lambda _:scope)
    for function in [reporting.get_report,governance.metadata_reports]:
        with pytest.raises(HTTPException) as exc:function({'id':8},group_id=2,user_id=7)
        assert exc.value.status_code==403


def test_limit_message_identifies_quota():
    exc=access.LimitExhausted('личный',10,10,'2026-11-30T21:00:00+00:00',{'id':2,'name':'Розница'})
    assert exc.detail['group_id']==2 and 'Квота «Розница»' in exc.detail['message']


def test_shared_user_reporting_counts_only_managed_group(monkeypatch):
    records=[dict(id=str(g),user_id=7,username='user.test',access_group_id=g,status='completed',
        provider='openai',model='mock',analysis_mode='custom',group_by_column=None,
        created_at='2026-11-02T00:00:00+03:00',updated_at='2026-11-02T00:00:00+03:00',
        total_tokens=t,processed_rows=1,failed_rows=0) for g,t in [(1,100),(2,900)]]
    from .test_reporting_sql import fixture_connection
    sql,conn=fixture_connection(records)
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(db,'get_conn',connection);monkeypatch.setattr(db,'get_reliable_since',lambda c:None)
    monkeypatch.setattr(reporting,'require_management',lambda u:governance.Scope(False,(1,)))
    result=reporting.get_report({'id':8},preset='custom',date_from='2026-11-01',date_to='2026-12-01',user_id=7)
    assert result['summary']['total_tokens']==100
    assert result['users'][0]['total_tokens']==100 and {g['id'] for g in result['groups']}=={1}
    sql.close()
