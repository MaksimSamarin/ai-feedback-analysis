from datetime import datetime, timezone
from unittest.mock import Mock

from fastapi import HTTPException
import pytest

from app import access


@pytest.mark.parametrize("group_limit,used,mode,personal,own,expected", [
    (None, 400, "group", None, 0, None), (100, 40, "group", None, 0, 60),
    (100, 100, "group", None, 0, 0), (100, 120, "group", None, 0, -20),
    (100, 80, "personal", 50, 10, 20), (100, 10, "personal", 50, 40, 10),
    (None, 100, "personal", 20, 21, -1), (0, 0, "group", None, 0, 0),
])
def test_remaining(group_limit, used, mode, personal, own, expected):
    assert access.remaining(group_limit, used, mode, personal, own)["remaining"] == expected


@pytest.mark.parametrize("date,start,end", [
    ("2026-10-31T20:59:59+00:00", "2026-09-30T21:00:00+00:00", "2026-10-31T21:00:00+00:00"),
    ("2026-10-31T21:00:00+00:00", "2026-10-31T21:00:00+00:00", "2026-11-30T21:00:00+00:00"),
    ("2026-12-31T21:00:00+00:00", "2026-12-31T21:00:00+00:00", "2027-01-31T21:00:00+00:00"),
])
def test_moscow_month(date, start, end):
    assert access.period_bounds("month", datetime.fromisoformat(date)) == (start, end)


def test_all_time_and_previous():
    assert access.period_bounds("none") == (None, None)
    assert access.previous_bounds("month", datetime(2026, 11, 8, tzinfo=timezone.utc)) == (
        "2026-09-30T21:00:00+00:00", "2026-10-31T21:00:00+00:00")


@pytest.mark.parametrize("data", [
    {"limit_mode": "personal"}, {"limit_mode": "personal", "personal_token_limit": -1},
    {"limit_mode": "personal", "personal_token_limit": 101},
    {"limit_mode": "personal", "personal_token_limit": True}, {"limit_mode": "invalid"},
    {"role_in_group": "admin"},
])
def test_invalid_personal_limit(data):
    with pytest.raises(HTTPException):
        access.validate_member({"token_limit": 100}, data)


def test_personal_without_group_limit():
    assert access.validate_member({"token_limit": None}, {"limit_mode": "personal", "personal_token_limit": 1000})["personal_token_limit"] == 1000


@pytest.mark.parametrize("before,data,target", [
    ({}, {}, 1), ({"role_in_group": "manager"}, {}, 2), ({}, {"role_in_group": "manager"}, 2),
])
def test_manager_cannot_change_managers(before, data, target):
    with pytest.raises(HTTPException) as exc:
        access.validate_member({"token_limit": None}, data, before, manager=True, actor_id=1, target_id=target)
    assert exc.value.status_code == 403


def test_manager_can_give_personal_limit():
    assert access.validate_member({"token_limit": 100}, {"limit_mode": "personal", "personal_token_limit": 50},
                                   manager=True, actor_id=1, target_id=2)["limit_mode"] == "personal"


def test_migration_not_repeated():
    queries = []
    class Conn:
        def execute(self, sql, params=()):
            queries.append(sql)
            return Mock(fetchone=lambda: {"name": "access_groups_v1"})
    access.init_schema(Conn())
    access.init_schema(Conn())
    assert not any("INSERT INTO access_group_members" in q for q in queries)
    assert not any("UPDATE reports" in q for q in queries)


def test_limit_error_contract():
    error = access.LimitExhausted("личный", 120, 100, None)
    assert error.status_code == 409 and error.detail["code"] == "limit_exhausted"
    assert "120" in error.detail["message"]


def test_last_admin_cannot_be_demoted(monkeypatch):
    from contextlib import contextmanager
    monkeypatch.setattr(access, "actor", lambda u: {"role": "admin"})
    class Conn:
        def execute(self, sql, params=()):
            if "SELECT role FROM users" in sql:
                row = {"role": "admin"}
            elif "SELECT id,username,role" in sql:
                row = {"id": 2, "username": "admin.other", "role": "admin"}
            elif "COUNT(*)" in sql:
                row = {"n": 1}
            else:
                row = None
            return Mock(fetchone=lambda: row)
    @contextmanager
    def get_conn():
        yield Conn()
    monkeypatch.setattr(access.db, "get_conn", get_conn)
    with pytest.raises(HTTPException) as exc:
        access.save_role({"id": 1}, 2, "user")
    assert "последнего" in exc.value.detail
