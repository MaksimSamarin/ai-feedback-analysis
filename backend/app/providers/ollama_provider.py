from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import get_provider_base_url

from .base import ContextLengthExceeded, LLMProvider


_OLLAMA_CONTEXT_HINTS = (
    "context",
    "too long",
    "exceeds",
    "maximum",
)


class OllamaProvider(LLMProvider):
    id = "ollama"

    async def analyze(
        self,
        prompt: str,
        model: str,
        api_key: str | None,
        timeout_sec: int,
        temperature: float,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        _ = api_key
        body = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "format": "json",
            "options": {"temperature": temperature},
        }

        ollama_base_url = get_provider_base_url("ollama", "http://localhost:11434")

        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.post(
                f"{ollama_base_url}/api/generate",
                json=body,
            )
            if resp.status_code >= 400:
                try:
                    err_body = resp.json()
                except Exception:
                    err_body = {}
                err_msg = ""
                if isinstance(err_body, dict):
                    err_msg = str(err_body.get("error") or "")
                msg_lower = err_msg.lower()
                if err_msg and any(hint in msg_lower for hint in _OLLAMA_CONTEXT_HINTS):
                    raise ContextLengthExceeded(model=model, provider_message=err_msg)
            resp.raise_for_status()
            payload = resp.json()

        content = payload.get("response", "{}")
        data = json.loads(content)
        raw = {
            "provider": self.id,
            "model": model,
            "content": content,
            "done": payload.get("done"),
            "eval_count": payload.get("eval_count"),
            "temperature": temperature,
        }
        return data, raw
