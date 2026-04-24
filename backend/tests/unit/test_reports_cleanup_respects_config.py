"""Регрессионные юниты на BUG-02 (enforce_reports_limit удалена).

После фикса BUG-02 единственный механизм очистки старых отчётов —
фоновый `cleanup_reports_keep_last_for_all_users` в worker'е с настройкой
`REPORT_KEEP_LAST`. В `_run_job` никаких "мгновенных" вызовов очистки
(с прошлым хардкодом `20`) быть не должно.

Запуск:
    cd backend && pytest tests/unit/test_reports_cleanup_respects_config.py -v
"""

from __future__ import annotations

from pathlib import Path

import pytest


def test_enforce_reports_limit_removed() -> None:
    """Функция должна быть полностью удалена из db.py."""
    import app.db as db_module

    assert not hasattr(db_module, "enforce_reports_limit"), (
        "Функция `enforce_reports_limit` вернулась — регрессия BUG-02. "
        "Очистка должна идти только через cleanup_reports_keep_last_for_all_users."
    )

    # Импорт по имени должен падать
    with pytest.raises(ImportError):
        from app.db import enforce_reports_limit  # noqa: F401


def test_job_manager_source_has_no_enforce_calls() -> None:
    """В исходниках `job_manager.py` не должно быть ни вызовов, ни импортов
    `enforce_reports_limit` — иначе снова дублирующий механизм очистки."""
    src_path = Path(__file__).resolve().parents[2] / "app" / "services" / "job_manager.py"
    source = src_path.read_text(encoding="utf-8")

    assert "enforce_reports_limit" not in source, (
        "В job_manager.py снова упоминается enforce_reports_limit — регрессия BUG-02. "
        "Проверьте что очистка идёт только через worker cleanup_loop."
    )
