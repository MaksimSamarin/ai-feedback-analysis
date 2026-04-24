"""Юнит-тесты для `JobManager._validate_custom_output`.

После итерации 1 (отказ от обязательных полей) валидатор НЕ требует
summary/category/confidence. Работает только по пользовательской схеме:
`expected_json_template` + `output_schema.required`.

Проверяют:
1. Ответ с произвольной схемой (без summary/category/confidence) проходит
2. Отсутствующие core-поля НЕ вызывают `ValueError "В ответе отсутствуют важные поля"`
3. `required` из output_schema по-прежнему валидируется
4. `extra_keys` из ответа модели отбрасываются и попадают в warnings
5. Исходный код job_manager не содержит хардкод `core_required_fields`

Запуск:
    cd backend && pytest tests/unit/test_validate_custom_output_no_core.py -v
"""

from __future__ import annotations

from pathlib import Path


def _make_manager():
    from app.services.job_manager import JobManager

    return JobManager.__new__(JobManager)  # без __init__ — не создаём БД-пула


def test_arbitrary_schema_without_core_fields_passes() -> None:
    """Схема без summary/category/confidence — ответ проходит."""
    manager = _make_manager()

    expected = {
        "тональность": {"type": "enum", "values": ["негатив", "нейтраль", "позитив"]},
        "срочность": {"type": "integer"},
    }
    parsed = {"тональность": "негатив", "срочность": 8}

    result = manager._validate_custom_output(
        parsed,
        output_schema=None,
        expected_json_template=expected,
    )
    assert result == {"тональность": "негатив", "срочность": 8}


def test_missing_summary_category_confidence_not_rejected() -> None:
    """Схема содержит summary — но если ответ его не содержит, НЕ падаем
    с 'В ответе отсутствуют важные поля' (этой хардкод-проверки больше нет)."""
    manager = _make_manager()

    expected = {
        "summary": {"type": "string", "max_length": 240},
        "моё_поле": {"type": "string"},
    }
    parsed = {"моё_поле": "значение"}

    result = manager._validate_custom_output(
        parsed,
        output_schema=None,
        expected_json_template=expected,
    )
    assert result == {"моё_поле": "значение"}


def test_output_schema_required_still_enforced() -> None:
    """`output_schema.required` продолжает работать для пользовательских полей."""
    import pytest

    manager = _make_manager()

    output_schema = {
        "type": "object",
        "properties": {"field_a": {"type": "string"}, "field_b": {"type": "integer"}},
        "required": ["field_a"],
    }
    expected = {"field_a": {"type": "string"}, "field_b": {"type": "integer"}}
    parsed = {"field_b": 5}  # нет field_a

    with pytest.raises(ValueError, match="обязательное поле: field_a"):
        manager._validate_custom_output(
            parsed,
            output_schema=output_schema,
            expected_json_template=expected,
        )


def test_extra_keys_dropped_and_warned() -> None:
    """Лишние ключи из ответа модели отбрасываются, попадают в warnings."""
    manager = _make_manager()

    expected = {"поле": {"type": "string"}}
    parsed = {"поле": "ok", "мусор_1": "x", "мусор_2": "y"}
    warnings: list[str] = []

    result = manager._validate_custom_output(
        parsed,
        output_schema=None,
        expected_json_template=expected,
        warnings=warnings,
    )
    assert result == {"поле": "ok"}
    assert warnings and "dropped_extra_keys" in warnings[0]
    assert "мусор_1" in warnings[0] and "мусор_2" in warnings[0]


def test_non_dict_response_rejected() -> None:
    """Если модель вернула не dict — явная ошибка, не пытаемся спасать."""
    import pytest

    manager = _make_manager()

    for bad in [["list"], "строка", 42, None]:
        with pytest.raises(ValueError, match="Ожидался JSON-объект"):
            manager._validate_custom_output(bad, output_schema=None)


def test_source_has_no_core_required_fields_hardcode() -> None:
    """Регрессия: в исходнике job_manager.py не должно быть хардкода
    `core_required_fields = ("summary", "category", "confidence")`."""
    source = Path(__file__).resolve().parents[2] / "app" / "services" / "job_manager.py"
    content = source.read_text(encoding="utf-8-sig")
    assert "core_required_fields" not in content, (
        "Хардкод core_required_fields должен быть удалён — список обязательных полей "
        "теперь строится из пользовательской схемы."
    )
