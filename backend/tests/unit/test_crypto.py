"""Юнит-тесты для `app.crypto_utils` — шифрование API-ключей (U4).

Функции `encrypt_text` / `decrypt_text` используют Fernet поверх ключа, выведенного
из `APP_SECRET`. Критично для хранения user-level OpenAI токенов в БД:
если шифрование ломается — все сохранённые отчёты становятся неработоспособными.

Запуск:
    cd backend && pytest tests/unit/test_crypto.py -v
"""

from __future__ import annotations

import importlib

import pytest


def _reload_crypto(monkeypatch: pytest.MonkeyPatch, secret: str):
    """Перезагружает модуль с заданным `APP_SECRET`.

    Нужно потому что `_secret()` читает env внутри каждого вызова; а Fernet
    инстанцируется лениво через `_fernet()`. Реимпорт гарантирует чистое состояние.
    """
    monkeypatch.setenv("APP_SECRET", secret)
    import app.crypto_utils as mod

    return importlib.reload(mod)


def test_encrypt_decrypt_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Happy path: шифруем → расшифровываем → получаем исходное значение."""
    crypto = _reload_crypto(monkeypatch, "test-secret-abc")
    plaintext = "sk-mysecrettoken1234567890"

    ciphertext = crypto.encrypt_text(plaintext)
    restored = crypto.decrypt_text(ciphertext)

    assert restored == plaintext
    assert ciphertext != plaintext, "шифротекст не должен совпадать с plaintext"
    assert ciphertext.startswith("v2:"), "формат должен начинаться с версии 'v2:'"


def test_encrypt_produces_different_ciphertext_each_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fernet использует случайный IV — два encrypt одного текста должны давать
    РАЗНЫЕ шифротексты. Защита от rainbow-таблиц."""
    crypto = _reload_crypto(monkeypatch, "test-secret-abc")
    plaintext = "secret"

    ciphertext_a = crypto.encrypt_text(plaintext)
    ciphertext_b = crypto.encrypt_text(plaintext)

    assert ciphertext_a != ciphertext_b, (
        "Двойной encrypt одного plaintext дал одинаковый шифротекст — "
        "IV не рандомизируется, уязвимо к анализу"
    )
    # Но оба должны расшифровываться в одно
    assert crypto.decrypt_text(ciphertext_a) == plaintext
    assert crypto.decrypt_text(ciphertext_b) == plaintext


def test_decrypt_with_wrong_key_raises_invalid_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """При смене APP_SECRET старые шифротексты должны давать явную ошибку
    `InvalidToken`, а не молча возвращать мусор.

    Это важно для сценария ротации ключа: воркер должен упасть с понятной
    ошибкой, а не пытаться использовать битый токен для вызова LLM.
    """
    from cryptography.fernet import InvalidToken

    crypto_a = _reload_crypto(monkeypatch, "secret-alpha")
    ciphertext = crypto_a.encrypt_text("my-token")

    # Теперь меняем секрет и пытаемся расшифровать
    crypto_b = _reload_crypto(monkeypatch, "secret-beta")

    with pytest.raises(InvalidToken):
        crypto_b.decrypt_text(ciphertext)


def test_encrypt_empty_string_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пустая строка — валидный вход, должна шифроваться и расшифровываться."""
    crypto = _reload_crypto(monkeypatch, "test-secret")

    ciphertext = crypto.encrypt_text("")
    assert crypto.decrypt_text(ciphertext) == ""


def test_encrypt_unicode_roundtrip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unicode (кириллица + эмоджи) должен сохраняться без потерь."""
    crypto = _reload_crypto(monkeypatch, "test-secret")
    plaintext = "пароль🔑 with emoji 日本語"

    ciphertext = crypto.encrypt_text(plaintext)
    restored = crypto.decrypt_text(ciphertext)

    assert restored == plaintext


def test_decrypt_rejects_unversioned_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Формат без префикса `v2:` не должен расшифровываться — даже если тело корректно.

    Префикс нужен для возможной миграции на v3 в будущем: старый формат
    отчётливо определяется и может быть обработан отдельной логикой."""
    from cryptography.fernet import InvalidToken

    crypto = _reload_crypto(monkeypatch, "test-secret")
    ciphertext = crypto.encrypt_text("x")
    # Убираем версионный префикс
    unversioned = ciphertext.removeprefix("v2:")

    with pytest.raises(InvalidToken):
        crypto.decrypt_text(unversioned)


def test_encrypt_without_app_secret_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Без `APP_SECRET` в env — RuntimeError с понятным сообщением."""
    monkeypatch.delenv("APP_SECRET", raising=False)
    import app.crypto_utils as mod

    crypto = importlib.reload(mod)

    with pytest.raises(RuntimeError, match="APP_SECRET"):
        crypto.encrypt_text("any")
