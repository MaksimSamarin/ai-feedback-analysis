from unittest.mock import Mock

from fastapi import HTTPException
import pytest

from app import governance


@pytest.mark.parametrize("role,members,expected", [
    ("admin", [], "admin"), ("user", [], "user"),
    ("user", [{"role_in_group": "manager", "is_active": True}], "manager"),
    ("user", [{"role_in_group": "manager", "is_active": False}], "user"),
    ("admin", [{"role_in_group": "manager"}], "admin"),
])
def test_effective_role(role, members, expected):
    assert governance.effective_role(role, members) == expected


@pytest.mark.parametrize("admin,groups,target,allowed", [
    (False, (), 1, False), (False, (1,), 1, True), (False, (1,), 2, False),
    (False, (1,), 0, False),
    (False, (1, 2), 2, True), (True, (), 2, True),
])
def test_scope_matrix(admin, groups, target, allowed):
    scope = governance.Scope(admin, groups)
    if allowed:
        scope.require_group(target)
    else:
        with pytest.raises(HTTPException) as exc:
            scope.require_group(target)
        assert exc.value.status_code == 403
    sql, args = scope.sql()
    assert args == ([] if admin else list(groups))
    if not admin and not groups:
        assert sql == "FALSE"


def test_manager_scope_uses_only_active_assignments():
    conn = Mock()
    conn.execute.side_effect = [Mock(fetchone=lambda: {"role": "user"}),
                                Mock(fetchall=lambda: [{"group_id": 2}, {"group_id": 7}])]
    assert governance.scope_for({"id": 3}, conn).groups == (2, 7)
    query = conn.execute.call_args[0][0]
    assert "g.is_active" in query and "role_in_group='manager'" in query


def test_metadata_foreign_group_rejected_before_query(monkeypatch):
    monkeypatch.setattr(governance, "require_management", lambda u: governance.Scope(False, (1,)))
    with pytest.raises(HTTPException) as exc:
        governance.metadata_reports({"id": 3}, group_id=2)
    assert exc.value.status_code == 403
