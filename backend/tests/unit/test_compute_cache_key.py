"""Юнит-тесты для `JobManager._compute_cache_key` (U7).

После рефакторинга логика генерации cache_key вынесена в статический метод
и стала легко тестируемой. Тесты проверяют:
1. Детерминированность — одинаковые входы → одинаковый key
2. Стабильность при перестановке ключей JSON — не должна влиять
3. Чувствительность к каждому входному полю — любое изменение меняет key
4. SHA-256 формат — 64 hex-символа

Кэш — критичный для стоимости компонент (каждый miss = токены).
Регрессия в этой функции = пересчёт всех закэшированных отчётов.

Запуск:
    cd backend && pytest tests/unit/test_compute_cache_key.py -v
"""

from __future__ import annotations

import re


def _compute(**overrides):
    """Хелпер: считает cache_key с дефолтными параметрами и нужными оверрайдами."""
    from app.services.job_manager import JobManager

    params = dict(
        provider_id="openai",
        model="gpt-4o-mini",
        analysis_mode="custom",
        prompt_template="Проанализируй: {row_json}",
        expected_json_template={"summary": {"type": "string"}},
        output_schema=None,
        normalized_review="Пример отзыва",
    )
    params.update(overrides)
    return JobManager._compute_cache_key(**params)


def test_returns_sha256_hex_strings() -> None:
    """Все 5 возвращаемых значений — 64-символьные hex-строки (SHA-256)."""
    cache_key, prompt_hash, review_hash, expected_hash, schema_hash = _compute()
    for name, value in (
        ("cache_key", cache_key),
        ("prompt_hash", prompt_hash),
        ("review_hash", review_hash),
        ("expected_template_hash", expected_hash),
        ("output_schema_hash", schema_hash),
    ):
        assert isinstance(value, str), f"{name} должен быть строкой"
        assert re.fullmatch(r"[0-9a-f]{64}", value), (
            f"{name}={value!r} — ожидали 64-символьную hex-строку SHA-256"
        )


def test_identical_input_produces_identical_key() -> None:
    """Детерминированность: одинаковые параметры → одинаковый cache_key."""
    key_a = _compute()[0]
    key_b = _compute()[0]
    assert key_a == key_b, "cache_key должен быть детерминированным"


def test_json_key_order_does_not_affect_key() -> None:
    """Порядок ключей в `expected_json_template` не должен влиять на хэш —
    в реализации есть `sort_keys=True`."""
    key_a = _compute(expected_json_template={"a": {"type": "string"}, "b": {"type": "number"}})[0]
    key_b = _compute(expected_json_template={"b": {"type": "number"}, "a": {"type": "string"}})[0]
    assert key_a == key_b, (
        "Перестановка ключей в expected_json_template изменила cache_key — "
        "sort_keys=True не работает?"
    )


def test_different_prompt_produces_different_key() -> None:
    """Любое изменение промпта → другой cache_key."""
    key_a = _compute(prompt_template="A")[0]
    key_b = _compute(prompt_template="B")[0]
    assert key_a != key_b


def test_different_review_produces_different_key() -> None:
    """Разные отзывы → разные cache_key (иначе кэш будет давать wrong-hit)."""
    key_a = _compute(normalized_review="отзыв один")[0]
    key_b = _compute(normalized_review="отзыв два")[0]
    assert key_a != key_b


def test_different_provider_produces_different_key() -> None:
    """Разные провайдеры на одном и том же запросе — разные кэш-ключи
    (gpt-4o и llama3 могут вернуть разные ответы на одинаковый промпт)."""
    key_a = _compute(provider_id="openai")[0]
    key_b = _compute(provider_id="ollama")[0]
    assert key_a != key_b


def test_different_model_produces_different_key() -> None:
    """Разные модели у одного провайдера → разные кэш-ключи."""
    key_a = _compute(model="gpt-4o-mini")[0]
    key_b = _compute(model="gpt-4.1-mini")[0]
    assert key_a != key_b


def test_none_and_empty_schemas_produce_same_key() -> None:
    """`None` для expected_json_template и `{}` должны давать одинаковый результат —
    в реализации используется `template or {}`."""
    key_a = _compute(expected_json_template=None)[0]
    key_b = _compute(expected_json_template={})[0]
    assert key_a == key_b, (
        "None и {} должны трактоваться одинаково (через `template or {}`)"
    )


def test_component_hashes_independent_of_unrelated_fields() -> None:
    """`prompt_hash` не должен зависеть от модели/отзыва — это хэш только промпта."""
    _, prompt_hash_a, _, _, _ = _compute(model="gpt-4o", normalized_review="A")
    _, prompt_hash_b, _, _, _ = _compute(model="llama3", normalized_review="B")
    assert prompt_hash_a == prompt_hash_b, (
        "prompt_hash должен зависеть только от prompt_template"
    )
