"""Вспомогательные функции для download-эндпоинтов.

Вынесены в отдельный модуль чтобы их можно было юнит-тестировать без
импорта `main.py` (который требует Postgres при импорте).
"""
from __future__ import annotations

import re


_UNSAFE_FILENAME_CHARS = re.compile(r"[\\/:*?\"<>|\x00-\x1f]")


def sanitize_download_filename(raw: str | None, *, fallback: str, extension: str) -> str:
    """Безопасное имя файла для Content-Disposition.

    Убирает слэши, управляющие символы, точки по краям. Если после зачистки
    осталась пустая строка — подставляется fallback. Пользовательское
    расширение отрезается, на выходе гарантированно {stem}.{extension}.
    Длина stem ограничена 100 символами.
    """
    base = (raw or "").strip()
    base = _UNSAFE_FILENAME_CHARS.sub("_", base)
    base = base.strip(". ")
    if not base:
        base = fallback
    stem, _, _ = base.rpartition(".")
    cleaned = stem if stem else base
    cleaned = cleaned[:100] or fallback
    return f"{cleaned}.{extension}"
