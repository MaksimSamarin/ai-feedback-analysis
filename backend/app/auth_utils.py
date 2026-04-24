from __future__ import annotations

import hashlib
import hmac
import os
import re
try:
    from argon2 import PasswordHasher
    from argon2.exceptions import InvalidHashError, VerifyMismatchError
except Exception:  # pragma: no cover
    PasswordHasher = None  # type: ignore[assignment]
    InvalidHashError = Exception  # type: ignore[assignment]
    VerifyMismatchError = Exception  # type: ignore[assignment]


_ARGON2_HASHER = PasswordHasher() if PasswordHasher is not None else None


def _is_pbkdf2_hash(value: str) -> bool:
    return ":" in value and not value.startswith("$argon2")


def hash_password(password: str) -> str:
    if _ARGON2_HASHER is not None:
        return _ARGON2_HASHER.hash(password)
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 600_000)
    return f"{salt.hex()}:{digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    if stored.startswith("$argon2") and _ARGON2_HASHER is not None:
        try:
            return bool(_ARGON2_HASHER.verify(stored, password))
        except (VerifyMismatchError, InvalidHashError):
            return False
        except Exception:
            return False
    try:
        salt_hex, digest_hex = stored.split(":", 1)
        salt = bytes.fromhex(salt_hex)
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 100_000)
        return hmac.compare_digest(digest.hex(), digest_hex)
    except Exception:
        return False


def password_needs_rehash(stored: str) -> bool:
    if _is_pbkdf2_hash(stored):
        return True
    if stored.startswith("$argon2") and _ARGON2_HASHER is not None:
        try:
            return bool(_ARGON2_HASHER.check_needs_rehash(stored))
        except Exception:
            return True
    return False


def validate_password_policy(password: str) -> str | None:
    if len(password) < 8:
        return "Пароль должен содержать минимум 8 символов"
    if not re.search(r"[A-Za-zА-Яа-я]", password):
        return "Пароль должен содержать хотя бы одну букву"
    if not re.search(r"\d", password):
        return "Пароль должен содержать хотя бы одну цифру"
    return None
