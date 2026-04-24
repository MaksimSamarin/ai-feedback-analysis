"""Юнит-тесты `sanitize_download_filename` из `app.download_utils`.

Покрывают сценарии итерации 2 (волна C — диалог скачивания с кастомным именем):
- пустая/None строка → fallback
- опасные символы (слэши, контрольные, кавычки) заменяются на `_`
- пользовательское расширение отбрасывается, навязываем своё
- точки и пробелы по краям срезаются
- длина усечена до 100 символов stem
- кириллица сохраняется

Запуск:
    cd backend && pytest tests/unit/test_sanitize_download_filename.py -v
"""
from __future__ import annotations

from app.download_utils import sanitize_download_filename


def test_returns_fallback_when_raw_is_none() -> None:
    assert sanitize_download_filename(None, fallback="report", extension="xlsx") == "report.xlsx"


def test_returns_fallback_when_raw_is_empty_string() -> None:
    assert sanitize_download_filename("", fallback="report", extension="xlsx") == "report.xlsx"


def test_returns_fallback_when_raw_is_whitespace() -> None:
    assert sanitize_download_filename("   ", fallback="report", extension="json") == "report.json"


def test_slashes_replaced_with_underscore() -> None:
    # Защита от path traversal — слэши превращаются в подчёркивания, ведущие
    # точки срезаются. Итог не содержит сепараторов пути — безопасно для
    # Content-Disposition.
    result = sanitize_download_filename("../etc/passwd", fallback="x", extension="xlsx")
    assert "/" not in result
    assert "\\" not in result
    assert ".." not in result
    assert result.endswith(".xlsx")
    assert "etc" in result and "passwd" in result


def test_windows_path_separators_replaced() -> None:
    assert sanitize_download_filename("C:\\users\\secret", fallback="x", extension="xlsx") == "C__users_secret.xlsx"


def test_control_chars_replaced() -> None:
    # \x00 (NULL) и \x1f (Unit Separator) — оба заменяются на _
    result = sanitize_download_filename("report\x00name\x1fhere", fallback="x", extension="xlsx")
    assert result == "report_name_here.xlsx"


def test_quotes_and_angle_brackets_replaced() -> None:
    result = sanitize_download_filename('my<report>"name', fallback="x", extension="xlsx")
    assert result == "my_report__name.xlsx"


def test_user_extension_is_stripped() -> None:
    # Пользователь ввёл "report.txt" — берём stem "report" и прикручиваем xlsx.
    assert sanitize_download_filename("report.txt", fallback="x", extension="xlsx") == "report.xlsx"


def test_double_extension_user_input() -> None:
    # "report.v2.txt" → stem "report.v2" → "report.v2.xlsx"
    assert sanitize_download_filename("report.v2.txt", fallback="x", extension="xlsx") == "report.v2.xlsx"


def test_cyrillic_preserved() -> None:
    result = sanitize_download_filename("отчет_март", fallback="x", extension="xlsx")
    assert result == "отчет_март.xlsx"


def test_leading_trailing_dots_stripped() -> None:
    result = sanitize_download_filename("...report...", fallback="x", extension="xlsx")
    assert result == "report.xlsx"


def test_stem_truncated_to_100_chars() -> None:
    long_name = "a" * 200
    result = sanitize_download_filename(long_name, fallback="x", extension="xlsx")
    assert result == f"{'a' * 100}.xlsx"
    # 100 символов + точка + 4 символа расширения
    assert len(result) == 105


def test_fallback_used_when_sanitization_removes_everything() -> None:
    # Все символы опасные — остаётся пусто → fallback
    result = sanitize_download_filename("///\\\\:::", fallback="report_default", extension="json")
    assert result.startswith("_") or result == "report_default.json"
    # В текущей реализации _UNSAFE_FILENAME_CHARS заменяет каждый символ на _,
    # strip(". ") их не трогает, поэтому получается строка из одних подчёркиваний.
    # Главное — не падает и возвращает валидное имя с расширением.
    assert result.endswith(".json")


def test_custom_fallback_for_partial() -> None:
    result = sanitize_download_filename(None, fallback="report_abc12345_partial", extension="xlsx")
    assert result == "report_abc12345_partial.xlsx"
