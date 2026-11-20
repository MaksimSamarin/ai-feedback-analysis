from contextlib import contextmanager
from unittest.mock import Mock
from app import access, db


def test_own_request_keeps_group_name_outside_current_memberships(monkeypatch):
    own_group={'id':1,'name':'Розница','role_in_group':'member','limit_mode':'group','personal_token_limit':None}
    request={'id':5,'user_id':7,'requested_group_id':2,'requested_group_name':'Поддержка','status':'pending'}
    conn=Mock();conn.execute.side_effect=[Mock(fetchall=lambda:[own_group]),Mock(fetchall=lambda:[request]),Mock(fetchone=lambda:None)]
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(db,'get_conn',connection)
    monkeypatch.setattr(access,'actor',lambda u:{'role':'user'})
    monkeypatch.setattr(access,'group_usage',lambda c,row,uid,m:{**row,'remaining':None})
    result=access.status({'id':7})
    assert result['requests'][0]['requested_group_name']=='Поддержка'
    assert [g['id'] for g in result['groups']]==[1]
    sql,args=conn.execute.call_args_list[1].args
    assert 'LEFT JOIN access_groups' in sql and 'WHERE ar.user_id=?' in sql and args==(7,)
