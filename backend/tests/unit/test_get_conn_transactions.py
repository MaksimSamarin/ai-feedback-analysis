"""Юнит-тесты контекстного менеджера `get_conn()` (BUG-08).

Проверяем что:
1. При успешном выполнении тела `with get_conn()` вызывается `commit()`
2. При исключении внутри — вызывается `rollback()` и исходное исключение пробрасывается
3. Если сам `rollback()` падает — исходное исключение всё равно видно (не подменяется
   ошибкой отката)

Все тесты изолированы от реальной БД: `_get_pg_pool` подменяется на MagicMock через
`monkeypatch.setattr`. `psycopg` и Postgres не требуются.

Запуск:
    cd backend && pytest tests/unit/test_get_conn_transactions.py -v
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest


def _install_fake_pool(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Возвращает фейковое сырое соединение, которое проксирует `_ConnProxy`.

    Настраивает fake_pool так, чтобы `pool.connection()` возвращал context manager,
    который отдаёт fake_raw_conn в __enter__ и ничего не делает в __exit__.
    """
    fake_raw_conn = MagicMock(name="raw_conn")
    fake_pool = MagicMock(name="pool")
    # pool.connection() — context manager: mock.__enter__() возвращает raw_conn
    cm = fake_pool.connection.return_value
    cm.__enter__.return_value = fake_raw_conn
    cm.__exit__.return_value = None

    monkeypatch.setattr("app.db._get_pg_pool", lambda: fake_pool)
    return fake_raw_conn


def test_get_conn_commits_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: при нормальном выходе из with-блока должен быть вызван commit."""
    from app.db import get_conn

    raw_conn = _install_fake_pool(monkeypatch)

    with get_conn() as conn:
        # Тело with просто выполняется, исключения нет
        assert conn is not None

    assert raw_conn.commit.called, "commit должен быть вызван при успешном выходе"
    assert not raw_conn.rollback.called, "rollback не должен вызываться в happy path"


def test_get_conn_rolls_back_on_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exception path: при исключении внутри with-блока должен быть вызван rollback,
    исходное исключение должно проброситься наружу."""
    from app.db import get_conn

    raw_conn = _install_fake_pool(monkeypatch)

    with pytest.raises(ValueError, match="boom"):
        with get_conn() as conn:
            raise ValueError("boom")

    assert raw_conn.rollback.called, "rollback должен быть вызван при исключении"
    assert not raw_conn.commit.called, "commit не должен вызываться при исключении"


def test_get_conn_commits_called_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Защита от дубля: commit не должен вызываться дважды (в try + в finally)."""
    from app.db import get_conn

    raw_conn = _install_fake_pool(monkeypatch)

    with get_conn():
        pass

    assert raw_conn.commit.call_count == 1, (
        f"commit должен вызваться один раз, получено {raw_conn.commit.call_count}"
    )


def test_get_conn_rollback_failure_does_not_hide_original_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Если rollback сам упадёт, мы не должны скрыть исходное исключение.

    Пользователь должен увидеть свою `ValueError('real bug')`, а не шум от
    ошибки соединения во время отката.
    """
    from app.db import get_conn

    raw_conn = _install_fake_pool(monkeypatch)
    raw_conn.rollback.side_effect = RuntimeError("rollback failed")

    with pytest.raises(ValueError, match="real bug"):
        with get_conn():
            raise ValueError("real bug")

    # Убедились что rollback пытались — но RuntimeError не протёк наружу
    assert raw_conn.rollback.called


def test_get_conn_commit_failure_triggers_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Если сам commit упал (сеть, дедлок) — должен быть вызван rollback."""
    from app.db import get_conn

    raw_conn = _install_fake_pool(monkeypatch)
    raw_conn.commit.side_effect = RuntimeError("commit network error")

    with pytest.raises(RuntimeError, match="commit network error"):
        with get_conn():
            pass

    assert raw_conn.commit.called
    assert raw_conn.rollback.called, (
        "если commit упал, rollback должен всё равно вызваться"
    )
