"""Юнит-тесты для `JobManager._build_summary_from_db`.

Функция делает SQL-агрегацию через `get_report_summary_agg` и возвращает
упрощённую сводку без sentiment-агрегатов (итерация 1 отказа от обязательных
полей: total/processed/success/failed).

Тесты проверяют:
1. Корректные агрегаты при mixed / all-failed / empty отчётах
2. Отсутствие ZeroDivisionError при success_rows=0 (защитная логика)
3. Обработка пустого dict от SQL (несуществующий report_id)
4. Совместимость с Pydantic-моделью `JobSummary`
5. `processed_rows == total_rows` после финализации

Запуск:
    cd backend && pytest tests/unit/test_build_summary_from_db.py -v
"""

from __future__ import annotations

import pytest


def _patch_agg(monkeypatch: pytest.MonkeyPatch, payload: dict) -> None:
    """Подменяет `get_report_summary_agg` в namespace job_manager.

    Важно: патчим там, куда имя ИМПОРТИРОВАНО (`app.services.job_manager`),
    а не в исходнике (`app.db`).
    """
    monkeypatch.setattr(
        "app.services.job_manager.get_report_summary_agg",
        lambda _report_id: payload,
    )


def test_empty_report_returns_zero_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    """Отчёт без строк — все агрегаты нулевые, без ошибок."""
    from app.services.job_manager import JobManager

    _patch_agg(
        monkeypatch,
        {"total_rows": 0, "success_rows": 0, "failed_rows": 0},
    )

    summary = JobManager._build_summary_from_db("empty-report")

    assert summary == {
        "total_rows": 0,
        "processed_rows": 0,
        "success_rows": 0,
        "failed_rows": 0,
    }


def test_all_failed_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """10 строк все failed — success_rows=0, без побочных эффектов."""
    from app.services.job_manager import JobManager

    _patch_agg(
        monkeypatch,
        {"total_rows": 10, "success_rows": 0, "failed_rows": 10},
    )

    summary = JobManager._build_summary_from_db("all-failed-report")

    assert summary["total_rows"] == 10
    assert summary["success_rows"] == 0
    assert summary["failed_rows"] == 10


def test_mixed_success_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    """total=100, success=95, failed=5 — корректно."""
    from app.services.job_manager import JobManager

    _patch_agg(
        monkeypatch,
        {"total_rows": 100, "success_rows": 95, "failed_rows": 5},
    )

    summary = JobManager._build_summary_from_db("mixed-report")

    assert summary["total_rows"] == 100
    assert summary["success_rows"] == 95
    assert summary["failed_rows"] == 5


def test_empty_agg_response_handled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если SQL вернул пустой dict — дефолтные нули, не KeyError."""
    from app.services.job_manager import JobManager

    _patch_agg(monkeypatch, {})

    summary = JobManager._build_summary_from_db("unknown-report")

    assert summary["total_rows"] == 0
    assert summary["success_rows"] == 0
    assert summary["failed_rows"] == 0


def test_result_compatible_with_job_summary_pydantic_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Результат функции используется как `JobSummary(**summary_payload)`.
    Проверяем что Pydantic не падает на любом корректном ответе SQL."""
    from app.schemas import JobSummary
    from app.services.job_manager import JobManager

    _patch_agg(
        monkeypatch,
        {"total_rows": 100, "success_rows": 95, "failed_rows": 5},
    )

    summary = JobManager._build_summary_from_db("rep")

    model = JobSummary(**summary)
    assert model.total_rows == 100
    assert model.processed_rows == 100
    assert model.success_rows == 95
    assert model.failed_rows == 5


def test_processed_rows_equals_total_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    """Для финальной агрегации `processed_rows == total_rows` (все обработаны)."""
    from app.services.job_manager import JobManager

    _patch_agg(
        monkeypatch,
        {"total_rows": 42, "success_rows": 40, "failed_rows": 2},
    )

    summary = JobManager._build_summary_from_db("rep")
    assert summary["processed_rows"] == summary["total_rows"] == 42


def test_no_sentiment_fields_in_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Регрессия: после отказа от обязательных полей в JobSummary НЕТ sentiment-агрегатов."""
    from app.services.job_manager import JobManager

    _patch_agg(
        monkeypatch,
        {"total_rows": 5, "success_rows": 5, "failed_rows": 0},
    )

    summary = JobManager._build_summary_from_db("rep")

    forbidden_keys = {
        "avg_negativity_score",
        "sentiment_counts",
        "sentiment_percentages",
        "negative_count",
        "neutral_count",
        "positive_count",
    }
    assert not (forbidden_keys & set(summary.keys())), (
        f"В summary не должно быть sentiment-полей, найдено: {forbidden_keys & set(summary.keys())}"
    )
