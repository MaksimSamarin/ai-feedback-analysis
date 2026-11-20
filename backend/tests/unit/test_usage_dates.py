from unittest.mock import Mock

import pytest

from app import access, db


@pytest.mark.parametrize("marker", [{"applied_at": "2031-01-01T00:10:12+00:00"}, None])
def test_reliable_since_comes_from_migration_marker(marker):
    conn = Mock()
    conn.execute.return_value.fetchone.return_value = marker
    assert db.get_reliable_since(conn) == (marker["applied_at"] if marker else None)
    assert conn.execute.call_args.args[1] == (db.SOFT_DELETE_MIGRATION,)


@pytest.mark.parametrize("reset_at,expected", [
    ("2026-11-30T21:00:00+00:00", "01.12.2026 00:00 МСК"),
    ("2026-12-31T21:00:00+00:00", "01.01.2027 00:00 МСК"),
])
def test_limit_message_reset_is_moscow_time(reset_at, expected):
    error = access.LimitExhausted("групповой", 101, 100, reset_at)
    assert "Сброс: " + expected in error.detail["message"]
    assert reset_at not in error.detail["message"]
    assert error.detail["reset_at"] == reset_at


def test_limit_without_reset_has_no_fabricated_date():
    error = access.LimitExhausted("личный", 101, 100, None)
    assert error.detail["message"].endswith("Без автоматического сброса")
    assert error.detail["reset_at"] is None
