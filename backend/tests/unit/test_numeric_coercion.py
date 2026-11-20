"""Числовые поля схемы: оценки вида «2,5» и целые в виде 2.0 из ответа модели."""
from __future__ import annotations

import pytest

from app.services.job_manager import JobManager, _coerce_numbers


def _manager() -> JobManager:
    return JobManager.__new__(JobManager)


def _schema(fields: dict) -> dict:
    return {"type": "object", "properties": fields}


@pytest.mark.parametrize("raw, expected", [
    ("2,5", 2.5), ("2.5", 2.5), (" 2,5 ", 2.5), ("0", 0), ("10", 10), ("-1,25", -1.25),
    ("1 000,5", 1000.5), (2.5, 2.5), (2, 2), ("2,0", 2.0),
])
def test_number_accepts_decimal_comma_and_numeric_strings(raw, expected) -> None:
    result = _coerce_numbers(raw, {"type": "number"})
    assert result == expected and type(result) is type(expected)


@pytest.mark.parametrize("raw, expected", [("5", 5), ("5,0", 5), (5.0, 5), ("2,5", 2.5)])
def test_integer_converts_whole_values_only(raw, expected) -> None:
    result = _coerce_numbers(raw, {"type": "integer"})
    assert result == expected and type(result) is type(expected)


@pytest.mark.parametrize("raw", ["да", "", "2,5 балла", "nan", "inf", True, None, "1e3"])
def test_non_numeric_values_are_left_for_type_check(raw) -> None:
    assert _coerce_numbers(raw, {"type": "number"}) is raw


def test_preset_with_half_points_passes_and_stores_numbers() -> None:
    schema = _schema({
        "1. Спросил имя": {"type": "number", "minimum": 0, "maximum": 2.5},
        "5.Спросил про SIM-карту": {"type": "integer", "minimum": 0, "maximum": 5},
        "Примеры": {"type": "string"},
    })
    result = _manager()._validate_custom_output(
        {"1. Спросил имя": "2,5", "5.Спросил про SIM-карту": "5", "Примеры": "спасибо"}, schema,
        expected_json_template={"1. Спросил имя": {}, "5.Спросил про SIM-карту": {}, "Примеры": {}})
    assert result["1. Спросил имя"] == 2.5 and isinstance(result["1. Спросил имя"], float)
    assert result["5.Спросил про SIM-карту"] == 5 and isinstance(result["5.Спросил про SIM-карту"], int)


def test_limits_still_apply_after_coercion() -> None:
    schema = _schema({"Оценка": {"type": "number", "minimum": 0, "maximum": 2.5}})
    with pytest.raises(ValueError, match="<= 2.5"):
        _manager()._validate_custom_output({"Оценка": "3,5"}, schema)


def test_integer_field_still_rejects_half_points() -> None:
    schema = _schema({"Оценка": {"type": "integer", "minimum": 0, "maximum": 2}})
    with pytest.raises(ValueError, match="целым"):
        _manager()._validate_custom_output({"Оценка": "2,5"}, schema)


def test_nested_arrays_and_objects_are_coerced() -> None:
    schema = _schema({
        "Баллы": {"type": "array", "items": {"type": "number"}},
        "Итог": {"type": "object", "properties": {"Сумма": {"type": "number"}, "Комментарий": {"type": "string"}}},
    })
    result = _manager()._validate_custom_output(
        {"Баллы": ["2,5", 1, "0,5"], "Итог": {"Сумма": "4", "Комментарий": "2,5"}}, schema)
    assert result["Баллы"] == [2.5, 1, 0.5]
    assert result["Итог"] == {"Сумма": 4, "Комментарий": "2,5"}


def test_empty_string_in_required_field_is_a_valid_answer() -> None:
    """«Примеров нет» — пустая строка в обязательном поле не делает строку ошибочной (как в 2.1)."""
    schema = {"type": "object", "properties": {"Примеры": {"type": "string"}, "Оценка": {"type": "number"}},
              "required": ["Примеры", "Оценка"]}
    result = _manager()._validate_custom_output({"Примеры": "", "Оценка": "0"}, schema)
    assert result == {"Примеры": "", "Оценка": 0}


def test_missing_or_null_required_field_is_still_an_error() -> None:
    schema = {"type": "object", "properties": {"Примеры": {"type": "string"}}, "required": ["Примеры"]}
    for parsed in ({}, {"Примеры": None}):
        with pytest.raises(ValueError, match="обязательное поле"):
            _manager()._validate_custom_output(dict(parsed), schema)
