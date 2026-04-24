"""Юнит-тесты для `normalize_review` — парсинг ячейки Excel (U1, пост-BUG-05).

Функция превращает значение ячейки (любого типа) в пару `(text | None, warnings)`:
- `None` или "empty marker" (nan/null/n-a/…) → `(None, ["empty_cell"])`
- Валидный текст → `(стрипнутая_строка, [])`
- Длинный текст **не обрезается** (регрессия BUG-05 — лимитов больше нет)

Запуск:
    cd backend && pytest tests/unit/test_excel_normalize_review.py -v
"""

from __future__ import annotations

import pytest


def test_none_returns_none_with_warning() -> None:
    """None (пустая ячейка Excel) → (None, ['empty_cell'])."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review(None)
    assert text is None
    assert warnings == ["empty_cell"]


def test_empty_string_returns_none_with_warning() -> None:
    """Пустая строка — считается пустой ячейкой."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review("")
    assert text is None
    assert warnings == ["empty_cell"]


def test_whitespace_only_returns_none() -> None:
    """Строка только из пробелов — после strip() пустая, считается empty_cell."""
    from app.services.excel_service import normalize_review

    for value in ("   ", "\t\n", "\r\n  "):
        text, warnings = normalize_review(value)
        assert text is None, f"Ожидали None для whitespace {value!r}, получили {text!r}"
        assert warnings == ["empty_cell"]


@pytest.mark.parametrize("marker", ["nan", "none", "null", "n/a", "na", "-"])
def test_empty_markers_return_none(marker: str) -> None:
    """Типичные маркеры пустоты из pandas/Excel → empty_cell."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review(marker)
    assert text is None
    assert warnings == ["empty_cell"]


@pytest.mark.parametrize("marker", ["NaN", "NULL", "N/A", "None", "NA"])
def test_empty_markers_case_insensitive(marker: str) -> None:
    """Регистр маркера не важен — пользователь мог написать `NULL` капсом."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review(marker)
    assert text is None
    assert warnings == ["empty_cell"]


def test_valid_text_returned_as_is() -> None:
    """Обычный отзыв возвращается без изменений, warnings пустой."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review("нормальный отзыв клиента")
    assert text == "нормальный отзыв клиента"
    assert warnings == []


def test_strips_surrounding_whitespace() -> None:
    """Внешние пробелы/таб/перевод строки — убираются."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review("  текст отзыва  \n")
    assert text == "текст отзыва"
    assert warnings == []


def test_long_text_not_truncated() -> None:
    """Регрессия BUG-05: текст в 50 000 символов не должен обрезаться.

    Раньше была константа MAX_REVIEW_CHARS=6000 и обрезка до 6000.
    Теперь никаких лимитов — модель сама скажет если не влез контекст.
    """
    long_text = "а" * 50_000
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review(long_text)
    assert text == long_text, "Текст обрезан — вернулся MAX_REVIEW_CHARS из BUG-05"
    assert len(text) == 50_000
    # В warnings не должно быть маркеров обрезки
    assert "trimmed_to_max_chars" not in warnings
    assert "truncated" not in warnings


def test_number_converted_to_string() -> None:
    """Числа из Excel — преобразуются в строку, не empty_cell."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review(42)
    assert text == "42"
    assert warnings == []

    text_float, warnings_float = normalize_review(3.14)
    assert text_float == "3.14"
    assert warnings_float == []


def test_zero_is_valid_not_empty() -> None:
    """Число `0` — валидное значение, не должно считаться пустым.

    Если кто-то решит "0 это как None" и добавит его в EMPTY_MARKERS —
    это сломает случаи когда в ячейке реально число 0 (рейтинг, счётчик)."""
    from app.services.excel_service import normalize_review

    text, warnings = normalize_review(0)
    assert text == "0"
    assert warnings == []


def test_unicode_and_emoji_preserved() -> None:
    """Кириллица + эмоджи + мультибайтные символы возвращаются без потерь."""
    from app.services.excel_service import normalize_review

    complex_text = "Сервис 👎 ужасный, 日本語 тоже 🎉"
    text, warnings = normalize_review(complex_text)
    assert text == complex_text
    assert warnings == []
