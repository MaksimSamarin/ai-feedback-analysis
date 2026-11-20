from unittest.mock import Mock

import pytest

from app import access
from app.audit_view import has_changes


@pytest.mark.parametrize('before,after,expected', [
    ({'name':'Розница','token_limit':100,'updated_at':'old'},
     {'token_limit':100,'name':'Розница','updated_at':'new'},False),
    ({'role_in_group':'manager','added_at':'old'}, {'role_in_group':'manager','added_at':'new'},False),
    ({'token_limit':None},{'token_limit':0},True),
    ({'role_in_group':'member'},{'role_in_group':'manager'},True),
    (None,{'name':'Розница'},True),
    ({'name':'Розница'},None,True),
])
def test_meaningful_audit_changes(before,after,expected):
    assert has_changes(before,after) is expected


def test_unchanged_save_does_not_write_audit():
    conn=Mock()
    access.audit(conn,1,'group.update','group',2,
        {'name':'Розница','updated_at':'old'}, {'name':'Розница','updated_at':'new'})
    conn.execute.assert_not_called()
    access.audit(conn,1,'group.update','group',2,{'token_limit':100},{'token_limit':200})
    conn.execute.assert_called_once()
