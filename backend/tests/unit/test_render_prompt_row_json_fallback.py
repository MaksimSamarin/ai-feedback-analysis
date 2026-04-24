"""Юнит-тесты для подкапотной логики `_render_prompt` (v2.0.0, итерация 3.2).

Проверяют:
1. Если в пользовательском шаблоне есть `{row_json}` — он заменяется на input_json.
2. Если шаблон НЕ содержит `{row_json}` — данные строки автоматически дописываются
   блоком «Данные строки:\\n<input_json>». Пользователь не обязан знать о переменной.
3. Переменная `{row_number}` больше НЕ подставляется (мёртвый код удалён).
4. Инструкция про enum сформулирована под реальную структуру EXPECTED_JSON
   (`type: enum` + массив `values`), а не под JSON Schema-шный ключ `enum`.

Запуск:
    cd backend && pytest tests/unit/test_render_prompt_row_json_fallback.py -v
"""

from __future__ import annotations


def _make_manager():
    from app.services.job_manager import JobManager

    return JobManager.__new__(JobManager)


def _render(**kwargs) -> str:
    defaults = dict(
        prompt_template="Анализируй отзыв.",
        review_text="отзыв",
        input_json='{"text": "опоздал"}',
        analysis_mode="custom",
        expected_json_template={"категория": {"type": "string"}},
    )
    defaults.update(kwargs)
    return _make_manager()._render_prompt(**defaults)


def test_row_json_placeholder_replaced_when_present() -> None:
    """Если в шаблоне есть {row_json} — он заменяется на input_json."""
    prompt = _render(
        prompt_template="Данные: {row_json}",
        input_json='{"id": 42}',
    )
    assert "{row_json}" not in prompt
    assert '{"id": 42}' in prompt


def test_row_json_appended_as_fallback_when_missing() -> None:
    """Если в шаблоне нет {row_json} — блок «Данные строки:» дописывается автоматически."""
    prompt = _render(
        prompt_template="Ты аналитик. Выдели основную тему обращения.",
        input_json='{"text": "не работает приложение"}',
    )
    assert "Данные строки:" in prompt
    assert '"text": "не работает приложение"' in prompt


def test_row_json_fallback_uses_empty_object_when_input_empty() -> None:
    """Если input_json пустой — в fallback подставляется {}."""
    prompt = _render(
        prompt_template="Ты аналитик.",
        input_json="",
    )
    assert "Данные строки:" in prompt
    assert "{}" in prompt


def test_row_number_placeholder_not_substituted() -> None:
    """{row_number} больше не подставляется (мёртвый код удалён)."""
    prompt = _render(
        prompt_template="Строка {row_number} данные {row_json}",
        input_json='{"x": 1}',
    )
    assert "{row_number}" in prompt, (
        "{row_number} не должен заменяться — подстановка удалена как мёртвый код"
    )


def test_render_prompt_signature_has_no_row_number() -> None:
    """В сигнатуре `_render_prompt` больше нет параметра row_number."""
    import inspect

    from app.services.job_manager import JobManager

    sig = inspect.signature(JobManager._render_prompt)
    assert "row_number" not in sig.parameters, (
        "Параметр row_number удалён — не должен быть в сигнатуре"
    )


def test_enum_instruction_matches_expected_json_format() -> None:
    """Инструкция про enum сформулирована под наш формат схемы (type: enum, values)."""
    prompt = _render()
    # Правильная формулировка — говорит про ключи схемы как они есть в EXPECTED_JSON
    assert "`type: enum`" in prompt
    assert "`values`" in prompt
    # Старая (некорректная) формулировка не должна вернуться
    assert "Если в схеме указан enum, верни одно допустимое значение из списка" not in prompt


def test_fallback_preserves_expected_json_block_order() -> None:
    """Fallback-блок с данными должен идти ДО EXPECTED_JSON (данные → формат)."""
    prompt = _render(
        prompt_template="Ты аналитик.",
        input_json='{"x": 1}',
    )
    idx_data = prompt.find("Данные строки:")
    idx_expected = prompt.find("EXPECTED_JSON")
    assert idx_data > -1 and idx_expected > -1
    assert idx_data < idx_expected, "Данные строки должны быть ДО EXPECTED_JSON"
