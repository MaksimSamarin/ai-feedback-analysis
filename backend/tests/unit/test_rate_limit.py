"""429 от провайдера: ожидание по Retry-After и отдельный счётчик от повторов валидации."""
from __future__ import annotations

from email.utils import format_datetime
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import httpx
import pytest

import app.rate_limit as rate_limit
from app.llm_errors import explain_error
from app.services.job_manager import JobManager

_URL = "http://local.invalid/v1/chat/completions"


def _status_error(status: int = 429, headers: dict | None = None, body: dict | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", _URL)
    response = httpx.Response(status, request=request, headers=headers or {}, json=body or {})
    return httpx.HTTPStatusError(f"Client error '{status}'", request=request, response=response)


@pytest.fixture(autouse=True)
def _clean_cooldown(monkeypatch):
    rate_limit._cooldown_until.clear()
    sleep = AsyncMock()
    monkeypatch.setattr(rate_limit.asyncio, "sleep", sleep)
    yield sleep
    rate_limit._cooldown_until.clear()


def test_retry_after_seconds_and_cap() -> None:
    assert rate_limit.rate_limit_delay(_status_error(headers={"Retry-After": "17"}), 0) == 17
    assert rate_limit.rate_limit_delay(_status_error(headers={"Retry-After": "0"}), 0) == 1
    assert rate_limit.rate_limit_delay(_status_error(headers={"Retry-After": "3600"}), 0) == 60
    assert rate_limit.rate_limit_delay(_status_error(headers={"retry-after-ms": "2500"}), 0) == 2.5


def test_retry_after_http_date() -> None:
    moment = datetime.now(timezone.utc) + timedelta(seconds=30)
    delay = rate_limit.rate_limit_delay(_status_error(headers={"Retry-After": format_datetime(moment, usegmt=True)}), 0)
    assert 25 <= delay <= 30


def test_backoff_grows_without_header_and_stops_at_cap() -> None:
    delays = [rate_limit.rate_limit_delay(_status_error(), hit) for hit in range(6)]
    assert 4 <= delays[0] <= 5 and 8 <= delays[1] <= 10 and 32 <= delays[3] <= 40
    assert all(48 <= d <= 60 for d in delays[4:])


@pytest.mark.parametrize("exc", [
    _status_error(500), _status_error(400), ValueError("Не JSON"),
    _status_error(429, body={"error": {"code": "insufficient_quota", "message": "quota"}}),
])
def test_not_a_rate_limit(exc) -> None:
    assert rate_limit.rate_limit_delay(exc, 0) is None


def _call(provider, *, schema=None, template=None):
    return JobManager()._process_row(
        provider=provider, row={"row_number": 2, "review_text": "Тест", "input_json": '{"Текст": "Тест"}'},
        prompt_template="Классифицируй", model="test-model", api_key="test", analysis_mode="custom",
        output_schema=schema, expected_json_template=template or {"Итог": {"type": "string"}},
        use_cache=False, temperature=0)


@pytest.mark.asyncio
async def test_rate_limit_waits_do_not_spend_validation_retries(monkeypatch, _clean_cooldown) -> None:
    import app.services.job_manager as module
    monkeypatch.setattr(module, "LLM_RETRIES", 0)
    answers = [_status_error(headers={"Retry-After": "20"}), _status_error(headers={"Retry-After": "20"}),
               {"Итог": "Хорошо"}]
    calls = []

    class Provider:
        id = "openai"

        async def analyze(self, *args):
            calls.append(1)
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer, {"usage": {"total_tokens": 5}}

    result = await _call(Provider())
    assert result["error"] is None and result["analysis_json"] == {"Итог": "Хорошо"}
    assert len(calls) == 3
    assert "rate_limited:2x/40s" in result["warnings"]
    waits = [c.args[0] for c in _clean_cooldown.await_args_list]
    assert waits and all(19 <= w <= 20 for w in waits)


@pytest.mark.asyncio
async def test_rate_limit_gives_up_after_its_own_limit(monkeypatch) -> None:
    import app.services.job_manager as module
    monkeypatch.setattr(module, "LLM_RETRIES", 2)
    monkeypatch.setattr(module, "LLM_RATE_LIMIT_RETRIES", 3)
    calls = []

    class Provider:
        id = "openai"

        async def analyze(self, *args):
            calls.append(1)
            raise _status_error()

    result = await _call(Provider())
    # 3 ожидания 429, затем обычные попытки LLM_RETRIES без ожиданий.
    assert len(calls) == 3 + 3
    assert result["raw_response"]["code"] == "provider_error"
    assert "частоту" in result["error"]
    assert any(str(w).startswith("rate_limited:3x/") for w in result["warnings"])


@pytest.mark.asyncio
async def test_validation_feedback_survives_rate_limit(monkeypatch) -> None:
    import app.services.job_manager as module
    monkeypatch.setattr(module, "LLM_RETRIES", 1)
    schema = {"type": "object", "properties": {"Итог": {"type": "string", "enum": ["Да", "Нет"]}}, "required": ["Итог"]}
    answers = [{"Итог": "Наверное"}, _status_error(headers={"Retry-After": "5"}), {"Итог": "Да"}]
    prompts = []

    class Provider:
        id = "openai"

        async def analyze(self, prompt, *args):
            prompts.append(prompt)
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer, {"usage": {"total_tokens": 5}}

    result = await _call(Provider(), schema=schema, template={"Итог": {}})
    assert result["error"] is None and result["analysis_json"] == {"Итог": "Да"}
    assert "Наверное" not in prompts[0] and "Наверное" in prompts[1] and "Наверное" in prompts[2]


@pytest.mark.asyncio
async def test_outside_enum_error_shows_only_that_field_value(monkeypatch) -> None:
    import app.services.job_manager as module
    monkeypatch.setattr(module, "LLM_RETRIES", 0)
    monkeypatch.setattr(module.asyncio, "sleep", AsyncMock())
    schema = {"type": "object", "properties": {
        "Классификация": {"type": "string", "enum": ["Тарифные запросы", "Качество связи"]},
        "Комментарий": {"type": "string"}}}

    class Provider:
        id = "openai"

        async def analyze(self, *args):
            return {"Классификация": "Тарифы", "Комментарий": "длинный текст ответа модели"}, {"usage": {}}

    result = await _call(Provider(), schema=schema, template={"Классификация": {}, "Комментарий": {}})
    assert result["error"] == "Модель вернула в поле «Классификация» значение «Тарифы» — его нет в разрешённом списке."
    assert "длинный текст" not in result["error"] and "Качество связи" not in result["error"]
    assert result["raw_response"]["code"] == "outside_enum"


def test_empty_enum_answer_is_reported_as_empty() -> None:
    from app.llm_errors import EnumMismatch
    result = explain_error(EnumMismatch("Поле 'Тема' должно быть одним из: А|Б", value=" "))
    assert result["code"] == "required_empty" and "оставила поле «Тема» пустым" in result["message"]


def test_outside_enum_long_value_is_truncated() -> None:
    from app.llm_errors import EnumMismatch
    message = explain_error(EnumMismatch("Поле 'Тема' должно быть одним из: А|Б", value="x" * 500))["message"]
    assert "x" * 200 + "…" in message and "x" * 201 not in message
