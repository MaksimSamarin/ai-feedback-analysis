"""Юнит-тесты для `_render_prompt` с retry_feedback.

Итерация 1.2: при повторных попытках LLM мы передаём модели обратную связь —
текст ошибки валидатора и её предыдущий (невалидный) ответ. Это резко повышает
шанс успешного исправления на 2-3 попытке.

Проверяют:
1. Без retry_feedback промпт не содержит feedback-блока (первая попытка).
2. С retry_feedback в промпт добавляется причина ошибки и предыдущий ответ.
3. Слишком длинный предыдущий ответ обрезается до 500 символов.
4. Если previous_response — dict, он сериализуется через JSON.
5. Feedback корректно работает и при наличии EXPECTED_JSON в промпте.

Запуск:
    cd backend && pytest tests/unit/test_render_prompt_retry_feedback.py -v
"""

from __future__ import annotations


def _make_manager():
    from app.services.job_manager import JobManager

    return JobManager.__new__(JobManager)


def _render(**kwargs) -> str:
    defaults = dict(
        prompt_template="Твой промпт с {row_json}",
        review_text="отзыв",
        input_json='{"text": "опоздал"}',
        analysis_mode="custom",
        expected_json_template={"категория": {"type": "string"}},
    )
    defaults.update(kwargs)
    return _make_manager()._render_prompt(**defaults)


def test_no_feedback_on_first_attempt() -> None:
    """Без retry_feedback в промпте нет блока с обратной связью."""
    prompt = _render(retry_feedback=None)
    assert "Предыдущий ответ не прошёл проверку" not in prompt
    assert "EXPECTED_JSON" in prompt


def test_feedback_block_appended_when_provided() -> None:
    """Если retry_feedback передан — в промпт добавляется блок с причиной и прошлым ответом."""
    feedback = {
        "error": "Значение поля 'категория' не входит в допустимые",
        "previous_response": {"категория": "высокий"},
    }
    prompt = _render(retry_feedback=feedback)
    assert "Предыдущий ответ не прошёл проверку" in prompt
    assert "Значение поля 'категория' не входит в допустимые" in prompt
    assert '"категория": "высокий"' in prompt


def test_previous_response_dict_serialized_as_json() -> None:
    """Предыдущий ответ в виде dict сериализуется в JSON с кириллицей без экранирования."""
    feedback = {
        "error": "ошибка",
        "previous_response": {"поле": "значение", "число": 42},
    }
    prompt = _render(retry_feedback=feedback)
    # ensure_ascii=False — кириллица не экранируется
    assert '"поле": "значение"' in prompt
    assert '"число": 42' in prompt


def test_previous_response_truncated_to_500_chars() -> None:
    """Слишком длинный предыдущий ответ обрезается до 500 символов + троеточие."""
    long_text = "x" * 2000
    feedback = {
        "error": "ошибка",
        "previous_response": long_text,
    }
    prompt = _render(retry_feedback=feedback)
    # В промпт попадает урезанная версия (500 символов + многоточие)
    assert "xxx…" in prompt or "xxx..." in prompt
    # Полная строка в 2000 символов в промпте не должна быть
    assert long_text not in prompt


def test_feedback_after_expected_json_block() -> None:
    """Блок feedback идёт ПОСЛЕ EXPECTED_JSON — чтобы модель сначала увидела схему,
    а потом понял свою ошибку относительно неё."""
    feedback = {"error": "ошибка", "previous_response": {"x": 1}}
    prompt = _render(retry_feedback=feedback)
    idx_expected = prompt.find("EXPECTED_JSON")
    idx_feedback = prompt.find("Предыдущий ответ не прошёл проверку")
    assert idx_expected < idx_feedback, "EXPECTED_JSON должен быть выше feedback-блока"


def test_feedback_without_previous_response_only_has_error() -> None:
    """Если previous_response=None — в промпте только причина ошибки, без блока 'Прошлый ответ'."""
    feedback = {"error": "ответ модели не JSON", "previous_response": None}
    prompt = _render(retry_feedback=feedback)
    assert "ответ модели не JSON" in prompt
    assert "Прошлый ответ" not in prompt


def test_feedback_mentions_what_model_should_do() -> None:
    """В feedback-блоке явно указано что делать — иначе модель может проигнорировать."""
    feedback = {"error": "что-то", "previous_response": {"a": 1}}
    prompt = _render(retry_feedback=feedback)
    assert "EXPECTED_JSON" in prompt  # есть упоминание схемы
    assert "без повторения ошибок" in prompt or "Исправь" in prompt or "Верни корректный" in prompt
