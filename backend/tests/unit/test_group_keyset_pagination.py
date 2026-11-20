"""Юнит-тесты на keyset-пагинацию `list_pending_group_keys_batch` (v2.1.1).

Регрессия на критичный баг: producer для group-режима использовал
OFFSET-пагинацию, а воркеры конкурентно переводили группы из
`status='pending'` в `done/error`. На каждой итерации OFFSET пропускал
ровно столько групп, сколько уже вышло из pending — в итоге обработка
останавливалась на ~50% (на 8427 группах терялось ~3800).

После фикса:
- сигнатура `list_pending_group_keys_batch(..., after_first_row=...)`,
  параметр `offset` удалён;
- SQL использует `HAVING MIN(row_number) > ?` вместо `OFFSET ?`;
- producer передаёт `after_first_row = last_group.first_row_number`.

Запуск:
    cd backend && pytest tests/unit/test_group_keyset_pagination.py -v
"""
from __future__ import annotations

import inspect


def test_function_signature_uses_after_first_row() -> None:
    """Сигнатура: параметр `after_first_row=0`, без `offset`."""
    from app.db import list_pending_group_keys_batch

    sig = inspect.signature(list_pending_group_keys_batch)
    params = sig.parameters
    assert "after_first_row" in params, (
        "Keyset-пагинация: должен быть параметр after_first_row"
    )
    assert "offset" not in params, (
        "OFFSET-пагинация удалена — параметр offset не должен оставаться "
        "(старый код терял ~45% групп при конкурентной обработке)"
    )
    assert params["after_first_row"].default == 0


def test_sql_uses_having_keyset_not_offset(monkeypatch) -> None:
    """SQL содержит HAVING MIN(row_number) > ? и не содержит OFFSET."""
    import app.db as db_mod

    captured: list[tuple[str, tuple]] = []

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConn:
        def execute(self, sql: str, params=None):
            captured.append((sql, params or ()))
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    db_mod.list_pending_group_keys_batch("r-1", limit=400, after_first_row=1234)

    assert captured, "execute должен быть вызван хотя бы раз"
    sql, params = captured[0]
    assert "HAVING MIN(row_number) > ?" in sql, (
        "Keyset-фильтр должен быть в HAVING, иначе он бы фильтровал отдельные "
        "строки, а не группы"
    )
    assert "OFFSET" not in sql, "OFFSET-пагинация удалена — баг на конкурентном чтении"
    assert "WHERE report_id = ?" in sql
    assert "AND status = 'pending'" in sql
    assert "ORDER BY MIN(row_number) ASC" in sql, "Порядок обработки должен сохраниться"
    assert params == ("r-1", 1234, 400), (
        "Параметры: (report_id, after_first_row, limit)"
    )


def test_default_after_first_row_is_zero(monkeypatch) -> None:
    """Без явного after_first_row — стартуем с 0 (первый батч)."""
    import app.db as db_mod

    captured: list[tuple] = []

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConn:
        def execute(self, sql: str, params=None):
            captured.append(params or ())
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    db_mod.list_pending_group_keys_batch("r-1")

    assert captured
    _report_id, after, _limit = captured[0]
    assert after == 0, "По умолчанию after_first_row=0 (старт с самой первой группы)"


def test_negative_after_first_row_is_clamped(monkeypatch) -> None:
    """Отрицательный after_first_row безопасно приводится к 0."""
    import app.db as db_mod

    captured: list[tuple] = []

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConn:
        def execute(self, sql: str, params=None):
            captured.append(params or ())
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    db_mod.list_pending_group_keys_batch("r-1", after_first_row=-100)

    assert captured
    _report_id, after, _limit = captured[0]
    assert after == 0, "Отрицательные значения зажимаются в 0"


def test_zero_or_negative_limit_is_clamped_to_one(monkeypatch) -> None:
    """limit<=0 не валит SQL — приводится к 1 (минимальный осмысленный батч)."""
    import app.db as db_mod

    captured: list[tuple] = []

    class FakeResult:
        def fetchall(self):
            return []

    class FakeConn:
        def execute(self, sql: str, params=None):
            captured.append(params or ())
            return FakeResult()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    db_mod.list_pending_group_keys_batch("r-1", limit=0)
    assert captured[0][2] == 1

    db_mod.list_pending_group_keys_batch("r-1", limit=-50)
    assert captured[1][2] == 1


def _job_manager_source() -> str:
    """Читаем исходник job_manager.py напрямую, без import — чтобы тест не
    зависел от установленных рантайм-зависимостей (xlsxwriter и т.п.)."""
    from pathlib import Path

    # tests/unit/<this_file>.py → backend/tests/unit → подняться на 2 уровня
    # до backend/, затем app/services/job_manager.py.
    here = Path(__file__).resolve()
    src_path = here.parent.parent.parent / "app" / "services" / "job_manager.py"
    assert src_path.exists(), f"Не нашли исходник: {src_path}"
    return src_path.read_text(encoding="utf-8")


def test_producer_uses_keyset_after_first_row() -> None:
    """Регрессия на интеграцию в producer (job_manager):
    в group-producer использован параметр after_first_row, а не offset."""
    src = _job_manager_source()
    assert "after_first_row" in src, (
        "Group-producer должен передавать after_first_row — keyset-пагинация"
    )
    assert "offset=group_offset" not in src, (
        "Старый OFFSET-вызов удалён (терял группы при конкурентной обработке)"
    )


def test_keyset_advances_to_last_first_row_in_batch() -> None:
    """Producer обновляет after_first_row значением first_row_number
    последней группы в полученном батче (для следующей итерации)."""
    src = _job_manager_source()
    assert "groups[-1]" in src, "Берём last из batch"
    assert "first_row_number" in src, "Передвигаем курсор по first_row_number"
