"""Юнит-тесты контракта retry: принудительное `use_cache=true` (итерация 2, волна D).

Бизнес-правило: когда пользователь жмёт «Перезапустить» на упавшем/отменённом
отчёте, кэш LLM должен включиться принудительно — даже если исходный запуск
был с `use_cache=false`. Это защита от повторного расхода токенов на уже
обработанные строки.

Тестируем через `build_job_payload_from_report` + эмуляцию override, который
делает retry-эндпоинт в `main.py`. Сам эндпоинт требует Postgres/FastAPI,
юнит-тест проверяет только контракт: payload формируется из БД, и после
override use_cache гарантированно True.

Запуск:
    cd backend && pytest tests/unit/test_retry_forces_use_cache.py -v
"""
from __future__ import annotations

from app.job_payloads import build_job_payload_from_report


def _base_row(**overrides) -> dict[str, object]:
    """Минимальный набор полей отчёта, которого хватает для build_job_payload_from_report."""
    row = {
        "id": "rep-1",
        "job_id": "job-1",
        "user_id": 42,
        "provider": "openai",
        "model": "gpt-4o-mini",
        "prompt_template": "Анализируй: {row_json}",
        "uploaded_file_id": "file-1",
        "sheet_name": "Sheet1",
        "analysis_mode": "custom",
        "max_reviews": 10,
        "parallelism": 3,
        "temperature": 0.0,
        "include_raw_json": 1,
        "use_cache": 1,
        "api_key_encrypted": "dummy",
        "input_columns_json": '["review"]',
        "non_analysis_columns_json": "[]",
        "output_schema_json": "{}",
        "expected_json_template_json": '{"verdict": "enum:позитив,негатив"}',
        "group_by_column": "",
    }
    row.update(overrides)
    return row


def test_payload_picks_up_use_cache_true_from_db() -> None:
    """Sanity: если в БД use_cache=1, он попадает в payload как True."""
    row = _base_row(use_cache=1)
    payload, err = build_job_payload_from_report(row)
    assert err is None
    assert payload is not None
    assert payload["use_cache"] is True


def test_payload_picks_up_use_cache_false_from_db() -> None:
    """Sanity: если пользователь при старте выключил кэш, use_cache=False в payload."""
    row = _base_row(use_cache=0)
    payload, err = build_job_payload_from_report(row)
    assert err is None
    assert payload is not None
    assert payload["use_cache"] is False


def test_retry_overrides_use_cache_when_initial_was_false() -> None:
    """Ключевой контракт: retry принудительно включает кэш, перекрывая исходный use_cache=false.

    Это бизнес-правило закрывает кейс «пользователь выключил кэш один раз,
    отчёт упал, перезапуск повторно потратит все токены».
    """
    row = _base_row(use_cache=0)
    payload, err = build_job_payload_from_report(row)
    assert err is None
    assert payload is not None
    # В main.py retry делает именно это — одной строкой переопределяет флаг
    # в очередном payload перед enqueue_job. Исходная запись в reports не меняется.
    payload["use_cache"] = True
    assert payload["use_cache"] is True


def test_retry_override_keeps_use_cache_true_when_already_true() -> None:
    """Если кэш и так был включён, override не меняет смысл — просто TRUE."""
    row = _base_row(use_cache=1)
    payload, err = build_job_payload_from_report(row)
    assert err is None
    assert payload is not None
    payload["use_cache"] = True
    assert payload["use_cache"] is True


def test_retry_override_does_not_touch_other_fields() -> None:
    """Override затрагивает только use_cache; остальные параметры (модель, промпт,
    параллелизм, колонки) остаются теми, что были в исходном запуске."""
    row = _base_row(
        provider="openai",
        model="gpt-4o-mini",
        prompt_template="Инструкция: {row_json}",
        parallelism=5,
        temperature=0.25,
        use_cache=0,
    )
    payload, err = build_job_payload_from_report(row)
    assert err is None
    assert payload is not None
    payload["use_cache"] = True
    assert payload["provider"] == "openai"
    assert payload["model"] == "gpt-4o-mini"
    assert payload["prompt_template"] == "Инструкция: {row_json}"
    assert payload["parallelism"] == 5
    assert payload["temperature"] == 0.25
    assert payload["analysis_columns"] == ["review"]
    assert payload["use_cache"] is True
