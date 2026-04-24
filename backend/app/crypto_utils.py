from __future__ import annotations

import base64
import hashlib
import os

from cryptography.fernet import Fernet, InvalidToken


def _secret() -> str:
    secret = os.getenv("APP_SECRET", "").strip()
    if not secret:
        raise RuntimeError("APP_SECRET is not configured")
    return secret


def _key_bytes() -> bytes:
    return hashlib.sha256(_secret().encode("utf-8")).digest()


def _fernet() -> Fernet:
    key = base64.urlsafe_b64encode(_key_bytes())
    return Fernet(key)


def encrypt_text(value: str) -> str:
    token = _fernet().encrypt(value.encode("utf-8")).decode("ascii")
    return f"v2:{token}"


def decrypt_text(value: str) -> str:
    try:
        if not value.startswith("v2:"):
            raise InvalidToken("Unsupported encrypted value format")
        token = value[3:].encode("ascii")
        raw = _fernet().decrypt(token)
        return raw.decode("utf-8")
    except Exception as exc:
        raise InvalidToken("Unable to decrypt value") from exc
