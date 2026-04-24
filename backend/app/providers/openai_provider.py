from __future__ import annotations

import json
import os
from typing import Any

import httpx

from app.config import TLS_VERIFY, get_provider_base_url

from .base import ContextLengthExceeded, LLMProvider


_CONTEXT_EXCEEDED_CODES = {"context_length_exceeded", "string_above_max_length"}
_CONTEXT_EXCEEDED_HINTS = (
    "maximum context length",
    "context length",
    "context_length",
    "too many tokens",
    "too long",
)


def _looks_like_context_exceeded(code: str, message: str) -> bool:
    if code.lower() in _CONTEXT_EXCEEDED_CODES:
        return True
    msg_lower = message.lower()
    return any(hint in msg_lower for hint in _CONTEXT_EXCEEDED_HINTS)


def normalize_api_key(value: str | None) -> str:
    """Нормализует OpenAI-совместимый API-ключ.

    Убирает префиксы "Authorization:" и "Bearer", пробелы. Полезно когда пользователь
    копирует токен вместе с HTTP-заголовком. Вызывается и из API-эндпоинтов в main.py,
    и из самого провайдера при отправке запроса.
    """
    if not value:
        return ""
    key = str(value).strip()
    if not key:
        return ""

    lower = key.lower()
    if lower.startswith("authorization:"):
        key = key.split(":", 1)[1].strip()
        lower = key.lower()

    if lower.startswith("bearer "):
        key = key[7:].strip()

    return key


class OpenAIProvider(LLMProvider):
    id = "openai"

    @staticmethod
    def _v1_base(base_url: str) -> str:
        base = (base_url or "").rstrip("/")
        return base if base.endswith("/v1") else f"{base}/v1"

    @staticmethod
    def _json_object_env(name: str) -> dict[str, Any]:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    async def analyze(
        self,
        prompt: str,
        model: str,
        api_key: str | None,
        timeout_sec: int,
        temperature: float,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        effective_api_key = normalize_api_key(api_key) or normalize_api_key(os.getenv("OPENAI_API_KEY"))
        if not effective_api_key:
            raise ValueError("Для провайдера OpenAI требуется API-ключ")

        body = {
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": "Return only strict JSON. No markdown.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": temperature,
            "response_format": {"type": "json_object"},
        }
        body.update(self._json_object_env("OPENAI_EXTRA_BODY_JSON"))

        headers = {
            "Authorization": f"Bearer {effective_api_key}",
            "Content-Type": "application/json",
        }
        extra_headers = self._json_object_env("OPENAI_EXTRA_HEADERS_JSON")
        for key, value in extra_headers.items():
            if not isinstance(key, str) or not key.strip():
                continue
            headers[key] = str(value)

        openai_base_url = get_provider_base_url("openai", "https://api.openai.com")
        endpoint = f"{self._v1_base(openai_base_url)}/chat/completions"

        async with httpx.AsyncClient(timeout=timeout_sec, verify=TLS_VERIFY) as client:
            resp = await client.post(
                endpoint,
                headers=headers,
                json=body,
            )
            if resp.status_code == 400:
                try:
                    err_body = resp.json()
                except Exception:
                    err_body = {}
                err = err_body.get("error") if isinstance(err_body, dict) else None
                if isinstance(err, dict):
                    code = str(err.get("code") or "")
                    msg = str(err.get("message") or "")
                    if _looks_like_context_exceeded(code, msg):
                        raise ContextLengthExceeded(model=model, provider_message=msg)
            resp.raise_for_status()
            payload = resp.json()

        content = payload["choices"][0]["message"]["content"]
        if isinstance(content, list):
            text_parts = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    text_parts.append(part.get("text", ""))
            content = "".join(text_parts)

        data = json.loads(content)
        raw = {
            "provider": self.id,
            "model": model,
            "usage": payload.get("usage"),
            "id": payload.get("id"),
            "created": payload.get("created"),
            "content": content,
            "temperature": temperature,
            "base_url": openai_base_url,
        }
        return data, raw
