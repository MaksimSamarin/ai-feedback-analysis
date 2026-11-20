import io
import json
from unittest.mock import AsyncMock

import httpx
from openpyxl import load_workbook
import pytest

from app.llm_errors import explain_error
from app.services.excel_service import export_results_xlsx
from app.services.job_manager import JobManager


@pytest.mark.parametrize("exc,code", [
    (ValueError("Поле 'Оценка' должно быть одним из: Да|Нет"), "outside_enum"),
    (ValueError("В ответе отсутствует обязательное поле: Имя"), "required_empty"),
    (ValueError("Не JSON: unexpected"), "invalid_json"),
    (ValueError("Ответ обрезан: finish_reason=length"), "truncated"),
    (ValueError("maximum context length"), "context_length"), (httpx.ReadTimeout("timed out"), "timeout"),
])
def test_error_mapping(exc, code):
    result = explain_error(exc)
    assert result["code"] == code and result["diagnostic"] == str(exc)


@pytest.mark.parametrize("status", [429, 500])
def test_provider_error_mapping(status):
    req = httpx.Request("POST", "http://local.invalid/v1/chat/completions")
    exc = httpx.HTTPStatusError("Ошибка", request=req, response=httpx.Response(status, request=req))
    assert explain_error(exc)["code"] == "provider_error"


@pytest.mark.parametrize("has_errors", [True, False])
def test_error_sheet_streaming(has_errors):
    rows = [{"row_number": 2, "status": "error" if has_errors else "done", "review_text": "Исходный отзыв",
             "error_text": "Не заполнено поле" if has_errors else None,
             "raw_response_json": json.dumps({"content": "не JSON"})}]
    buffer = io.BytesIO()
    export_results_xlsx(buffer, lambda: iter(rows), {"total_rows": 1})
    wb = load_workbook(io.BytesIO(buffer.getvalue()), read_only=True)
    assert ("Ошибки" in wb.sheetnames) == has_errors
    if has_errors:
        record = list(wb["Ошибки"].values)[1]
        assert record == (2, "Исходный отзыв", "Не заполнено поле", "не JSON")
    wb.close()


@pytest.mark.asyncio
async def test_failed_attempts_keep_usage_and_raw(monkeypatch):
    import app.services.job_manager as module
    from app.providers.base import InvalidLLMResponse
    monkeypatch.setattr(module, "LLM_RETRIES", 1)
    monkeypatch.setattr(module.asyncio, "sleep", AsyncMock())
    class Provider:
        id = "openai"
        async def analyze(self, *args):
            raise InvalidLLMResponse("Не JSON", {"content": "bad", "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}})
    manager = JobManager()
    row = await manager._process_row(provider=Provider(), row={"row_number": 2, "review_text": "Тест", "input_json": '{}'},
        prompt_template="Тест", model="mock-model", api_key="test", analysis_mode="custom", output_schema=None,
        expected_json_template={"Итог": {"type": "string"}}, use_cache=False, temperature=0)
    assert row["_usage_total_tokens"] == 30
    assert row["raw_response"]["content"] == "bad" and row["raw_response"]["code"] == "invalid_json"


@pytest.mark.asyncio
async def test_cache_hit_has_zero_usage(monkeypatch):
    import app.services.job_manager as module
    monkeypatch.setattr(module, "get_cached_analysis", lambda key: {"analysis": {"Итог": "Тест"}, "raw": {"usage": {"total_tokens": 200}}})
    manager = JobManager()
    row = await manager._process_row(provider=type("Provider", (), {"id": "openai"})(), row={"row_number": 2, "review_text": "Тест", "input_json": '{}'},
        prompt_template="Тест", model="mock-model", api_key="test", analysis_mode="custom", output_schema=None,
        expected_json_template={"Итог": {"type": "string"}}, use_cache=True, temperature=0)
    assert row["_usage_total_tokens"] == row["_usage_prompt_tokens"] == row["_usage_completion_tokens"] == 0
    assert "cache_hit" in row["warnings"]


@pytest.mark.asyncio
async def test_retry_stops_when_pending_usage_exhausts_budget(monkeypatch):
    import app.services.job_manager as module
    from app.providers.base import InvalidLLMResponse
    monkeypatch.setattr(module, "LLM_RETRIES", 2)
    calls = []
    class Provider:
        id = "openai"
        async def analyze(self, *args):
            calls.append(1)
            raise InvalidLLMResponse("Не JSON", {"content": "bad", "usage": {"total_tokens": 15}})
    result = await JobManager()._process_row(provider=Provider(), row={"row_number": 2, "review_text": "Тест", "input_json": '{}'},
        prompt_template="Тест", model="mock-model", api_key="test", analysis_mode="custom", output_schema=None,
        expected_json_template={"Итог": {"type": "string"}}, use_cache=False, temperature=0, token_budget=1)
    assert len(calls) == 1 and result["_usage_total_tokens"] == 15


@pytest.mark.asyncio
async def test_ollama_invalid_json_keeps_usage(monkeypatch):
    from app.providers.ollama_provider import OllamaProvider
    from app.providers.base import InvalidLLMResponse
    client_class = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json={"response": "bad", "prompt_eval_count": 11, "eval_count": 4}))
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: client_class(transport=transport))
    with pytest.raises(InvalidLLMResponse) as exc:
        await OllamaProvider().analyze("Тест", "test-model", None, 3, 0)
    assert exc.value.raw["usage"]["total_tokens"] == 15 and exc.value.raw["content"] == "bad"
