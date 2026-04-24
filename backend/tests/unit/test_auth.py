"""Юнит-тесты для `app.auth_utils` — хэширование паролей (U5).

Проверяем:
1. Argon2 roundtrip (основной путь)
2. Обратная совместимость с PBKDF2 хэшами (старые пользователи)
3. `password_needs_rehash` — корректно определяет миграцию
4. Политика паролей — длина, буквы, цифры
5. Безопасность — plaintext не протекает в хэш, хэши рандомизированы

Запуск:
    cd backend && pytest tests/unit/test_auth.py -v
"""

from __future__ import annotations

import hashlib
import os

import pytest


def test_argon2_hash_verify_roundtrip() -> None:
    """Happy path: хэшируем → верифицируем — True."""
    from app.auth_utils import hash_password, verify_password

    hashed = hash_password("my-secret-password")

    assert hashed.startswith("$argon2"), (
        f"Ожидали Argon2-хэш, получили {hashed[:20]!r} — пакет argon2 не установлен?"
    )
    assert verify_password("my-secret-password", hashed) is True


def test_verify_rejects_wrong_password() -> None:
    """Неверный пароль для верного хэша — False."""
    from app.auth_utils import hash_password, verify_password

    hashed = hash_password("correct-password")

    assert verify_password("wrong-password", hashed) is False
    assert verify_password("", hashed) is False
    assert verify_password("correct-password ", hashed) is False  # с лишним пробелом


def test_hash_does_not_contain_plaintext() -> None:
    """В хэше не должно быть plaintext пароля — защита от случайного логирования."""
    from app.auth_utils import hash_password

    secret = "my-unique-plaintext-marker-xyz"
    hashed = hash_password(secret)

    assert secret not in hashed, (
        "Plaintext пароль просочился в хэш — хэширование не работает или выдаёт сырые данные"
    )


def test_hash_produces_different_hashes_each_time() -> None:
    """Одинаковые пароли → разные хэши (salt рандомный). Защита от rainbow-таблиц."""
    from app.auth_utils import hash_password

    hash_a = hash_password("same-password")
    hash_b = hash_password("same-password")

    assert hash_a != hash_b, (
        "Двойной hash_password одного пароля дал идентичные хэши — salt не рандомизируется"
    )


def test_legacy_pbkdf2_hash_still_verifies() -> None:
    """Старый формат `pbkdf2:sha256$...` (или `<salt_hex>:<digest_hex>`) должен
    продолжать работать. Это обратная совместимость для пользователей, чьи
    пароли были созданы до миграции на Argon2."""
    from app.auth_utils import verify_password

    password = "legacy-user-pass"
    # Создаём PBKDF2 хэш в том же формате, что хранится в старых записях:
    # salt_hex:digest_hex с 100_000 итераций (дефолт verify_password для legacy)
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
    legacy_hash = f"{salt.hex()}:{digest.hex()}"

    assert verify_password(password, legacy_hash) is True, (
        "Старый PBKDF2 хэш не верифицируется — сломали обратную совместимость, "
        "все пользователи с старыми хэшами не смогут войти"
    )
    assert verify_password("wrong", legacy_hash) is False


def test_password_needs_rehash_true_for_legacy_pbkdf2() -> None:
    """Для PBKDF2 хэша функция должна возвращать True — нужен апгрейд на Argon2."""
    from app.auth_utils import password_needs_rehash

    pbkdf2_hash = "a1b2c3:d4e5f6"  # формат salt_hex:digest_hex
    assert password_needs_rehash(pbkdf2_hash) is True


def test_password_needs_rehash_false_for_fresh_argon2() -> None:
    """Для свежего Argon2-хэша (только что созданного) апгрейд не нужен."""
    from app.auth_utils import hash_password, password_needs_rehash

    fresh = hash_password("some-password")
    assert password_needs_rehash(fresh) is False, (
        "Свежий Argon2-хэш помечен как требующий rehash — странно, или параметры сменились"
    )


def test_validate_password_policy_accepts_valid() -> None:
    """Валидный пароль (8+ символов, буква, цифра) — None (нет ошибки)."""
    from app.auth_utils import validate_password_policy

    assert validate_password_policy("GoodPass1") is None
    assert validate_password_policy("Пароль123") is None  # кириллица тоже ок
    assert validate_password_policy("a1bcdefg") is None  # минимум 8 символов


def test_validate_password_policy_rejects_short_password() -> None:
    """Пароль < 8 символов — возвращается текст ошибки, не None."""
    from app.auth_utils import validate_password_policy

    result = validate_password_policy("a1b")
    assert result is not None
    assert "8" in result  # в ошибке упоминается минимальная длина


def test_validate_password_policy_rejects_no_digit() -> None:
    """Пароль без цифр — ошибка."""
    from app.auth_utils import validate_password_policy

    result = validate_password_policy("OnlyLetters")
    assert result is not None
    assert "цифр" in result.lower()


def test_validate_password_policy_rejects_no_letter() -> None:
    """Пароль без букв — ошибка."""
    from app.auth_utils import validate_password_policy

    result = validate_password_policy("12345678")
    assert result is not None
    assert "букв" in result.lower()
