"""Юнит-тесты для `app.schemas` — Pydantic-валидация API-контракта (U6).

Фокус на:
1. Кастомных валидаторах (`@field_validator`) — их Pydantic сам не знает
2. Бизнес-критичных ограничениях (parallelism, analysis_columns, negativity_score)
3. Enum-полях (SentimentLabel, JobStatus) — чтобы несуществующие значения падали

Эти схемы — граница API. Если валидация сломается и пропустит невалидный payload,
дальше по коду могут быть KeyError / IndexError в неожиданных местах.

Запуск:
    cd backend && pytest tests/unit/test_schemas_validation.py -v
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError


def _valid_start_job_payload() -> dict[str, Any]:
    """Минимальный валидный payload для `StartJobRequest`."""
    return {
        "file_id": "file-abc",
        "sheet_name": "Reviews",
        "analysis_columns": ["review_text"],
        "provider": "openai",
        "model": "gpt-4o-mini",
        "prompt_template": "Проанализируй отзыв: {row_json}",
    }


# ─── StartJobRequest ─────────────────────────────────────────────────────────


def test_start_job_accepts_minimal_valid_payload() -> None:
    from app.schemas import StartJobRequest

    req = StartJobRequest(**_valid_start_job_payload())
    assert req.analysis_columns == ["review_text"]
    assert req.parallelism == 3  # default
    assert req.max_reviews == 100  # default


def test_start_job_rejects_empty_analysis_columns() -> None:
    """`analysis_columns` обязана содержать минимум 1 колонку."""
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    payload["analysis_columns"] = []

    with pytest.raises(ValidationError) as exc_info:
        StartJobRequest(**payload)

    # Проверяем что ошибка именно на analysis_columns
    errors = exc_info.value.errors()
    assert any("analysis_columns" in str(err.get("loc", [])) for err in errors), (
        f"Ожидали ошибку валидации на analysis_columns, получили: {errors}"
    )


def test_start_job_rejects_parallelism_above_global_limit() -> None:
    """Кастомный валидатор: parallelism не должен превышать GLOBAL_LLM_PARALLELISM."""
    from app.config import GLOBAL_LLM_PARALLELISM
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    payload["parallelism"] = GLOBAL_LLM_PARALLELISM + 1

    with pytest.raises(ValidationError) as exc_info:
        StartJobRequest(**payload)

    # В тексте ошибки должно быть указано max значение
    error_text = str(exc_info.value)
    assert str(GLOBAL_LLM_PARALLELISM) in error_text, (
        f"Текст ошибки должен содержать лимит {GLOBAL_LLM_PARALLELISM}, "
        f"получили: {error_text}"
    )


def test_start_job_accepts_parallelism_at_boundary() -> None:
    """parallelism ровно `GLOBAL_LLM_PARALLELISM` — валидное граничное значение."""
    from app.config import GLOBAL_LLM_PARALLELISM
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    payload["parallelism"] = GLOBAL_LLM_PARALLELISM

    req = StartJobRequest(**payload)
    assert req.parallelism == GLOBAL_LLM_PARALLELISM


def test_start_job_rejects_parallelism_zero_and_negative() -> None:
    """parallelism >= 1 — нельзя запустить 0 или отрицательное число воркеров."""
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    for bad_value in (0, -1):
        payload["parallelism"] = bad_value
        with pytest.raises(ValidationError):
            StartJobRequest(**payload)


def test_start_job_rejects_temperature_out_of_range() -> None:
    """temperature ∈ [0.0, 2.0] — OpenAI/Ollama не принимают другие значения."""
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    for bad_value in (-0.1, 2.01, 10.0):
        payload["temperature"] = bad_value
        with pytest.raises(ValidationError):
            StartJobRequest(**payload)


def test_start_job_rejects_max_reviews_zero() -> None:
    """max_reviews >= 1 — ноль строк для анализа бессмысленно."""
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    payload["max_reviews"] = 0

    with pytest.raises(ValidationError):
        StartJobRequest(**payload)


def test_start_job_rejects_non_custom_analysis_mode() -> None:
    """analysis_mode сейчас поддерживает только 'custom'.

    Если кто-то передаст старое 'sentiment' — валидатор должен явно отказать,
    а не пропустить и сломаться где-то дальше."""
    from app.schemas import StartJobRequest

    payload = _valid_start_job_payload()
    payload["analysis_mode"] = "sentiment"

    with pytest.raises(ValidationError):
        StartJobRequest(**payload)


# ─── AnalysisOutput ──────────────────────────────────────────────────────────


def test_analysis_output_accepts_valid_payload() -> None:
    from app.schemas import AnalysisOutput, SentimentLabel

    out = AnalysisOutput(
        category="service",
        sentiment_label="negative",
        negativity_score=0.8,
        summary="Клиент жалуется на задержку",
    )
    assert out.sentiment_label is SentimentLabel.negative
    assert out.negativity_score == 0.8


def test_analysis_output_rejects_unknown_sentiment_label() -> None:
    """sentiment_label — enum из 3 значений. Любое другое — ошибка."""
    from app.schemas import AnalysisOutput

    with pytest.raises(ValidationError):
        AnalysisOutput(
            category="service",
            sentiment_label="unknown",  # ← не в enum
            negativity_score=0.5,
            summary="x",
        )


def test_analysis_output_rejects_negativity_score_out_of_range() -> None:
    """negativity_score ∈ [0.0, 1.0]. Это вероятность/нормализованная оценка."""
    from app.schemas import AnalysisOutput

    for bad_score in (-0.1, 1.01, 2.0):
        with pytest.raises(ValidationError):
            AnalysisOutput(
                category="service",
                sentiment_label="negative",
                negativity_score=bad_score,
                summary="x",
            )


def test_analysis_output_rejects_too_long_summary() -> None:
    """summary максимум 240 символов — чтобы помещалось в ячейку итогового xlsx."""
    from app.schemas import AnalysisOutput

    with pytest.raises(ValidationError):
        AnalysisOutput(
            category="service",
            sentiment_label="negative",
            negativity_score=0.5,
            summary="x" * 241,
        )


# ─── JobStatus enum ──────────────────────────────────────────────────────────


def test_job_status_values() -> None:
    """Регрессия: JobStatus содержит ровно 6 значений — если добавится 7-й,
    нужно будет обновить фронт и обработку в worker/job_manager."""
    from app.schemas import JobStatus

    expected = {"queued", "running", "paused", "completed", "failed", "canceled"}
    actual = {status.value for status in JobStatus}
    assert actual == expected, (
        f"JobStatus изменился: было {expected}, стало {actual}. "
        "Проверь что фронт и воркер знают о новом статусе."
    )


# ─── AuthRequest ─────────────────────────────────────────────────────────────


def test_auth_request_rejects_short_username() -> None:
    """username минимум 3 символа — короткие логины трудно различать."""
    from app.schemas import AuthRequest

    with pytest.raises(ValidationError):
        AuthRequest(username="ab", password="secret123")


def test_auth_request_rejects_short_password() -> None:
    """password минимум 6 символов (Pydantic-lvl проверка, независимо от policy)."""
    from app.schemas import AuthRequest

    with pytest.raises(ValidationError):
        AuthRequest(username="valid_user", password="123")


def test_auth_request_accepts_valid() -> None:
    from app.schemas import AuthRequest

    req = AuthRequest(username="user123", password="password1")
    assert req.username == "user123"


