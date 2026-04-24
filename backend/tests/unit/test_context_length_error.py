"""Юнит-тесты на обработку `context_length_exceeded` (BUG-05).

После фикса BUG-05 удалены все статические лимиты длины (`MAX_REVIEW_CHARS`,
`LLM_MAX_PROMPT_CHARS`, `LLM_SKIP_OVERSIZED_REQUESTS`). Вместо них провайдер
сам определяет когда промпт не влез, и мы ловим ошибку через исключение
`ContextLengthExceeded`, чтобы отдать пользователю понятное сообщение.

Запуск:
    cd backend && pytest tests/unit/test_context_length_error.py -v
"""

from __future__ import annotations

import pytest


def test_looks_like_context_exceeded_matches_error_codes() -> None:
    """Детектор должен срабатывать на явные коды ошибок OpenAI-совместимых API."""
    from app.providers.openai_provider import _looks_like_context_exceeded

    assert _looks_like_context_exceeded("context_length_exceeded", "")
    assert _looks_like_context_exceeded("string_above_max_length", "")
    # Регистр не должен влиять
    assert _looks_like_context_exceeded("CONTEXT_LENGTH_EXCEEDED", "")


def test_looks_like_context_exceeded_matches_message_hints() -> None:
    """Если код неизвестен, детектор должен ловить текст сообщения."""
    from app.providers.openai_provider import _looks_like_context_exceeded

    assert _looks_like_context_exceeded(
        "",
        "This model's maximum context length is 8192 tokens",
    )
    assert _looks_like_context_exceeded("", "too many tokens in prompt")
    assert _looks_like_context_exceeded("", "input is too long")
    assert _looks_like_context_exceeded("", "context_length exceeded")


def test_looks_like_context_exceeded_rejects_unrelated_errors() -> None:
    """Обычные ошибки не должны классифицироваться как превышение контекста."""
    from app.providers.openai_provider import _looks_like_context_exceeded

    assert not _looks_like_context_exceeded("invalid_api_key", "The API key is invalid")
    assert not _looks_like_context_exceeded("rate_limit_exceeded", "Rate limit hit")
    assert not _looks_like_context_exceeded("", "Invalid JSON response")
    assert not _looks_like_context_exceeded("", "")


def test_removed_constants_not_in_config() -> None:
    """Регрессия: три константы BUG-05 должны отсутствовать в app.config."""
    import app.config as config

    for name in ("MAX_REVIEW_CHARS", "LLM_MAX_PROMPT_CHARS", "LLM_SKIP_OVERSIZED_REQUESTS"):
        assert not hasattr(config, name), (
            f"Константа {name} вернулась в app.config — регрессия BUG-05. "
            "Лимиты длины не должны задаваться статически."
        )


def test_context_length_exceeded_carries_model_and_message() -> None:
    """Класс исключения должен сохранять model и provider_message —
    чтобы job_manager мог сформировать сообщение пользователю со всеми деталями."""
    from app.providers.base import ContextLengthExceeded

    exc = ContextLengthExceeded(model="gpt-4o-mini", provider_message="8192 tokens limit")
    assert exc.model == "gpt-4o-mini"
    assert exc.provider_message == "8192 tokens limit"
    assert "8192 tokens limit" in str(exc)


def test_context_length_exceeded_is_plain_exception() -> None:
    """Наследуется от Exception, не от HTTPError — чтобы не было путаницы с сетевыми ошибками."""
    from app.providers.base import ContextLengthExceeded

    exc = ContextLengthExceeded(model="test")
    assert isinstance(exc, Exception)


def test_context_length_exceeded_default_message() -> None:
    """Если provider_message пустой, должен быть осмысленный дефолт с именем модели."""
    from app.providers.base import ContextLengthExceeded

    exc = ContextLengthExceeded(model="gpt-3.5-turbo")
    text = str(exc)
    assert "gpt-3.5-turbo" in text
    assert "context" in text.lower()
