"""Юнит-тесты для `_row_to_preview` — превращает строку `report_rows` в DTO
для страницы отчёта (итерация 1 отказа от обязательных полей).

Проверяют:
1. Слияние input_json + passthrough_json + custom_json в плоский dict `columns`
2. Развёртка вложенных dict в ключи вида `"parent.child"`
3. Толерантность к строковому JSON (unicode, пробелы, некорректный) — не падаем
4. Сохранение числовых типов (int, float, bool) в columns — критично для xlsx
5. Обработка `warnings` (list, JSON-строка, plain-строка)
6. `error_text` маппится в поле `error` (None если пусто)

Запуск:
    cd backend && pytest tests/unit/test_row_to_preview.py -v
"""

from __future__ import annotations

import json


def test_basic_merge_input_passthrough_custom() -> None:
    """Три источника колонок сливаются: input → passthrough → custom."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "input_json": json.dumps({"отзыв": "Всё отлично"}),
        "passthrough_json": json.dumps({"id": "A-100"}),
        "custom_json": json.dumps({"тональность": "позитив", "рейтинг": 5}),
        "error_text": None,
    }

    preview = _row_to_preview(row)

    assert preview["row_number"] == 1
    assert preview["columns"]["отзыв"] == "Всё отлично"
    assert preview["columns"]["id"] == "A-100"
    assert preview["columns"]["тональность"] == "позитив"
    assert preview["columns"]["рейтинг"] == 5


def test_custom_json_overrides_passthrough() -> None:
    """Если ключ встречается в passthrough и custom — побеждает custom (анализ главнее)."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "input_json": json.dumps({"text": "x"}),
        "passthrough_json": json.dumps({"category": "из файла"}),
        "custom_json": json.dumps({"category": "от модели"}),
    }

    preview = _row_to_preview(row)
    assert preview["columns"]["category"] == "от модели"


def test_nested_dict_in_custom_is_flattened() -> None:
    """Вложенные dict разворачиваются в ключи `parent.child`."""
    from app.db import _row_to_preview

    row = {
        "row_number": 2,
        "custom_json": json.dumps({"оценка": {"качество": 5, "скорость": 3}}),
    }

    preview = _row_to_preview(row)
    assert preview["columns"]["оценка.качество"] == 5
    assert preview["columns"]["оценка.скорость"] == 3


def test_numeric_and_bool_types_preserved() -> None:
    """Числа и булевы значения проходят как есть (int/float/bool, не str).
    Критично для Excel-выгрузки — тип ячейки должен быть Number/Boolean."""
    from app.db import _row_to_preview

    row = {
        "row_number": 3,
        "custom_json": json.dumps(
            {
                "score": 5,
                "rating": 4.75,
                "urgent": True,
                "archived": False,
            }
        ),
    }

    preview = _row_to_preview(row)
    assert preview["columns"]["score"] == 5 and isinstance(preview["columns"]["score"], int)
    assert preview["columns"]["rating"] == 4.75 and isinstance(preview["columns"]["rating"], float)
    assert preview["columns"]["urgent"] is True
    assert preview["columns"]["archived"] is False


def test_invalid_json_in_custom_returns_empty_columns() -> None:
    """Кривой JSON в custom_json — не падаем, возвращаем пустые columns."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "custom_json": "{not json",
        "input_json": json.dumps({"ok": "from_input"}),
    }

    preview = _row_to_preview(row)
    assert preview["columns"] == {"ok": "from_input"}


def test_dict_passed_as_is_without_serialization() -> None:
    """Если поле уже dict (не JSON-строка) — тоже работает."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "custom_json": {"категория": "доставка"},
        "input_json": {"текст": "опоздание"},
    }

    preview = _row_to_preview(row)
    assert preview["columns"]["категория"] == "доставка"
    assert preview["columns"]["текст"] == "опоздание"


def test_warnings_list_preserved() -> None:
    """Список warnings сохраняется как список строк."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "warnings": ["dropped_extra_keys:foo", "retry_happened"],
    }

    preview = _row_to_preview(row)
    assert preview["warnings"] == ["dropped_extra_keys:foo", "retry_happened"]


def test_warnings_json_string_decoded() -> None:
    """Если warnings пришли JSON-строкой — декодируем в список."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "warnings": json.dumps(["a", "b"]),
    }

    preview = _row_to_preview(row)
    assert preview["warnings"] == ["a", "b"]


def test_error_text_maps_to_error_field() -> None:
    """error_text из БД попадает в поле `error` DTO."""
    from app.db import _row_to_preview

    row = {"row_number": 1, "error_text": "Промпт превысил контекст"}
    preview = _row_to_preview(row)
    assert preview["error"] == "Промпт превысил контекст"


def test_empty_error_becomes_none() -> None:
    """Пустая строка / None в error_text → поле `error=None`."""
    from app.db import _row_to_preview

    for empty_value in (None, ""):
        row = {"row_number": 1, "error_text": empty_value}
        assert _row_to_preview(row)["error"] is None


def test_no_core_fields_imposed() -> None:
    """Регрессия на обязательные поля: _row_to_preview НЕ требует
    summary/category/confidence — columns строятся из того что есть."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "custom_json": json.dumps({"моё_поле": "значение", "другое": 42}),
    }

    preview = _row_to_preview(row)
    assert "summary" not in preview["columns"]
    assert "category" not in preview["columns"]
    assert "confidence" not in preview["columns"]
    assert preview["columns"]["моё_поле"] == "значение"
    assert preview["columns"]["другое"] == 42


def test_grouped_mode_keeps_only_group_column_and_analysis() -> None:
    """В групповом режиме из input/passthrough остаётся только колонка группировки.

    Регрессия: раньше в превью для группы показывался «отзыв» одной случайной
    записи — пользователь видел текст конкретного отзыва рядом с агрегатом по
    смене и думал, что LLM его проанализировал именно так.
    """
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "input_json": json.dumps({"отзыв": "Продавец Полина мошенничает"}),
        "passthrough_json": json.dumps(
            {"смена": "Юг_2026-01-03_утро", "оценка": 5, "дата": "2026-01-03"}
        ),
        "custom_json": json.dumps({"число_нарушений": 5, "уровень_риска": "средний"}),
        "group_key": "Юг_2026-01-03_утро",
    }

    preview = _row_to_preview(row, group_by_column="смена")

    assert preview["columns"]["смена"] == "Юг_2026-01-03_утро"
    assert preview["columns"]["число_нарушений"] == 5
    assert preview["columns"]["уровень_риска"] == "средний"
    # Случайные поля строки-представителя не попадают в превью группы.
    assert "отзыв" not in preview["columns"]
    assert "оценка" not in preview["columns"]
    assert "дата" not in preview["columns"]


def test_grouped_mode_falls_back_to_group_key_if_column_value_missing() -> None:
    """Если в input/passthrough значения колонки нет — подставляем group_key."""
    from app.db import _row_to_preview

    row = {
        "row_number": 1,
        "input_json": json.dumps({"отзыв": "x"}),
        "passthrough_json": None,
        "custom_json": json.dumps({"уровень_риска": "низкий"}),
        "group_key": "store-42",
    }

    preview = _row_to_preview(row, group_by_column="магазин")
    assert preview["columns"]["магазин"] == "store-42"
    assert preview["columns"]["уровень_риска"] == "низкий"


def test_override_row_number_applied_for_grouped_mode() -> None:
    """В групповом режиме row_number DTO заменяется на порядковый номер группы.

    Регрессия: иначе в превью/xlsx попадает номер случайной строки-представителя
    (в исходном xlsx — например, 37, 55, 71), который непонятен пользователю.
    """
    from app.db import _row_to_preview

    row = {
        "row_number": 8421,
        "custom_json": json.dumps({"число_нарушений": 3}),
        "passthrough_json": json.dumps({"смена": "A"}),
        "group_key": "A",
    }

    preview = _row_to_preview(row, group_by_column="смена", override_row_number=2)
    assert preview["row_number"] == 2
