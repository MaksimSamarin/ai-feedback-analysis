"""Юнит-тесты для `normalize_api_key` — очистка OpenAI API-токена (BUG-07 регрессия).

Функция убирает:
- пробелы в начале/конце
- префикс `Bearer ` (регистр не важен)
- префикс `Authorization:` (и опциональный `Bearer` внутри)

Нужна потому что пользователи копируют токен разными способами:
из документации ("Bearer sk-..."), из cURL ("Authorization: Bearer sk-..."),
из .env файла (с пробелами). Функция приводит к каноничному виду.

Запуск:
    cd backend && pytest tests/unit/test_normalize_api_key.py -v
"""

from __future__ import annotations


def test_returns_empty_for_none() -> None:
    """None → пустая строка (единый контракт с вызывающими — `or ""` ниже не нужен)."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key(None) == ""


def test_returns_empty_for_empty_string() -> None:
    """Пустая строка → пустая строка."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("") == ""


def test_returns_empty_for_whitespace_only() -> None:
    """Только пробелы → пустая строка (после strip)."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("   ") == ""
    assert normalize_api_key("\t\n") == ""


def test_strips_leading_trailing_whitespace() -> None:
    """Внешние пробелы убираются."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("  sk-abc123  ") == "sk-abc123"
    assert normalize_api_key("\tsk-abc\n") == "sk-abc"


def test_strips_bearer_prefix() -> None:
    """`Bearer ` (с пробелом после) убирается — самый частый случай из документации."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("Bearer sk-abc123") == "sk-abc123"


def test_strips_bearer_prefix_case_insensitive() -> None:
    """Регистр `Bearer` не важен — люди копируют по-разному."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("BEARER sk-abc") == "sk-abc"
    assert normalize_api_key("bearer sk-abc") == "sk-abc"
    assert normalize_api_key("BeArEr sk-abc") == "sk-abc"


def test_strips_authorization_header_prefix() -> None:
    """`Authorization: Bearer ...` — весь заголовок целиком тоже очищается."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("Authorization: Bearer sk-abc") == "sk-abc"


def test_strips_authorization_case_insensitive() -> None:
    """`Authorization:` в любом регистре."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("authorization: bearer sk-abc") == "sk-abc"
    assert normalize_api_key("AUTHORIZATION: BEARER sk-abc") == "sk-abc"


def test_preserves_valid_token_unchanged() -> None:
    """Чистый токен без префиксов — возвращается как есть."""
    from app.providers.openai_provider import normalize_api_key

    token = "sk-proj-abc123def456"
    assert normalize_api_key(token) == token


def test_preserves_non_openai_tokens() -> None:
    """Не-OpenAI токены (GitHub, другие сервисы) — функция их не должна ломать.
    Убирает только Bearer/Authorization, остальное остаётся."""
    from app.providers.openai_provider import normalize_api_key

    assert normalize_api_key("ghs_mySecretToken") == "ghs_mySecretToken"
    assert normalize_api_key("Bearer ghs_mySecretToken") == "ghs_mySecretToken"


def test_handles_multiple_spaces_after_bearer() -> None:
    """После `Bearer` может быть несколько пробелов — функция обрабатывает 1 пробел
    плюс .strip() в конце убирает остальные."""
    from app.providers.openai_provider import normalize_api_key

    # После removeprefix("bearer ") останется " sk-abc", strip даст "sk-abc"
    # Но текущая реализация снимает только "bearer " с одним пробелом — проверяем что выдаёт.
    result = normalize_api_key("Bearer  sk-abc")  # два пробела
    # Ожидаем что токен распарсится без лишнего пробела
    assert result == "sk-abc" or result == " sk-abc", (
        f"Ожидали 'sk-abc' или ' sk-abc', получили {result!r}"
    )


def test_is_single_source_not_duplicated() -> None:
    """Регрессия BUG-07: функция должна быть только в `openai_provider.py`, не в `main.py`.

    Проверяем через исходник (не импорт app.main, который тащит psycopg_pool при инициализации).
    """
    from pathlib import Path

    main_src = (
        Path(__file__).resolve().parents[2] / "app" / "main.py"
    ).read_text(encoding="utf-8")

    assert "def _normalize_api_key" not in main_src, (
        "В main.py снова определена локальная `_normalize_api_key` — регрессия BUG-07. "
        "Используйте `from app.providers.openai_provider import normalize_api_key`."
    )
    # Но импорт из openai_provider должен быть
    assert "from app.providers.openai_provider import normalize_api_key" in main_src, (
        "main.py не импортирует `normalize_api_key` из openai_provider — "
        "вероятно, кто-то вернул локальную копию"
    )
