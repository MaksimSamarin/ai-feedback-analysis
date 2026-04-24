"""Юнит-тесты на reset_failed_and_skipped_rows и preview-группировку (v2.0.0, итерация 3.2).

Проверяют:
1. `reset_failed_and_skipped_rows` сбрасывает в pending строки с `status='error'`
   и `status='done'` с warning `skipped_large_group`.
2. `build_report_analysis` для группового отчёта возвращает по одному элементу
   на `group_key`, для обычного — построчно (как раньше).

Запуск:
    cd backend && pytest tests/unit/test_retry_reset_rows.py -v
"""
from __future__ import annotations


def test_reset_failed_and_skipped_rows_function_exists() -> None:
    """В db-слое есть функция reset_failed_and_skipped_rows(report_id)."""
    from app.db import reset_failed_and_skipped_rows

    assert callable(reset_failed_and_skipped_rows)


def test_reset_sql_targets_error_and_skipped(monkeypatch) -> None:
    """SQL обновляет строки со status='error' ИЛИ status='done' с warning skipped_large_group."""
    import app.db as db_mod

    calls: list[tuple[str, tuple]] = []

    class FakeCursor:
        rowcount = 3

    class FakeConn:
        def execute(self, sql: str, params=None):
            calls.append((sql, params or ()))
            return FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    n = db_mod.reset_failed_and_skipped_rows("report-1")
    assert n == 3
    assert calls, "Должен быть хотя бы один UPDATE"
    sql, params = calls[0]
    assert "UPDATE report_rows" in sql
    assert "status = 'pending'" in sql
    assert "status = 'error'" in sql, "Должен сбрасывать строки с ошибкой"
    # warnings-фильтр передан ПАРАМЕТРОМ (иначе psycopg ругается на % как плейсхолдер)
    assert "warnings_json LIKE ?" in sql, "LIKE должен быть параметризован"
    assert params == ("report-1", "%skipped_large_group%"), (
        "Должны передаваться report_id и LIKE-шаблон в параметрах"
    )
    # Обязательно чистить analysis/warnings/error — иначе после повтора будет мусор
    assert "custom_json = NULL" in sql
    assert "warnings_json = NULL" in sql
    assert "error_text = NULL" in sql


def test_build_report_analysis_groups_preview_by_group_key(monkeypatch) -> None:
    """Для группового отчёта в preview — по одной строке на group_key."""
    import app.db as db_mod

    monkeypatch.setattr(
        db_mod,
        "get_report_summary_agg",
        lambda report_id: {"total_rows": 3, "success_rows": 3, "failed_rows": 0},
    )

    class FakeReport:
        def get(self, key, default=None):
            return {"group_by_column": "store"}.get(key, default)

    class FakeConn:
        def execute(self, sql: str, params=None):
            class R:
                def fetchone(self):
                    return FakeReport()
            return R()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    # 3 строки по группам: A, A, B — ожидаем 2 в preview (по одной на группу).
    rows_stream = [
        {
            "report_id": "r",
            "row_number": 1,
            "status": "done",
            "custom_json": "{}",
            "group_key": "A",
            "input_json": None,
            "passthrough_json": None,
            "warnings_json": None,
            "error_text": None,
        },
        {
            "report_id": "r",
            "row_number": 2,
            "status": "done",
            "custom_json": "{}",
            "group_key": "A",
            "input_json": None,
            "passthrough_json": None,
            "warnings_json": None,
            "error_text": None,
        },
        {
            "report_id": "r",
            "row_number": 3,
            "status": "done",
            "custom_json": "{}",
            "group_key": "B",
            "input_json": None,
            "passthrough_json": None,
            "warnings_json": None,
            "error_text": None,
        },
    ]
    monkeypatch.setattr(db_mod, "iter_report_rows", lambda report_id, batch_size=100: iter(rows_stream))

    _, preview = db_mod.build_report_analysis("r")
    assert len(preview) == 2, "Ожидалось ровно 2 элемента (по одному на группу)"


def test_build_report_analysis_non_grouped_keeps_row_level(monkeypatch) -> None:
    """Для обычного (негруппового) отчёта preview остаётся построчным."""
    import app.db as db_mod

    monkeypatch.setattr(
        db_mod,
        "get_report_summary_agg",
        lambda report_id: {"total_rows": 3, "success_rows": 3, "failed_rows": 0},
    )

    class FakeReport:
        def get(self, key, default=None):
            return {"group_by_column": None}.get(key, default)

    class FakeConn:
        def execute(self, sql: str, params=None):
            class R:
                def fetchone(self):
                    return FakeReport()
            return R()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    rows_stream = [
        {
            "report_id": "r",
            "row_number": i,
            "status": "done",
            "custom_json": "{}",
            "group_key": None,
            "input_json": None,
            "passthrough_json": None,
            "warnings_json": None,
            "error_text": None,
        }
        for i in range(1, 4)
    ]
    monkeypatch.setattr(db_mod, "iter_report_rows", lambda report_id, batch_size=100: iter(rows_stream))

    _, preview = db_mod.build_report_analysis("r")
    assert len(preview) == 3, "Для негруппового отчёта preview построчный"
