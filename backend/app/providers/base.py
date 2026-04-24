from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class ContextLengthExceeded(Exception):
    """Промпт превысил лимит контекста модели. Ретрай бессмысленен."""

    def __init__(self, model: str, provider_message: str = "") -> None:
        self.model = model
        self.provider_message = provider_message
        super().__init__(provider_message or f"context length exceeded for model {model}")


class LLMProvider(ABC):
    id: str

    @abstractmethod
    async def analyze(
        self,
        prompt: str,
        model: str,
        api_key: str | None,
        timeout_sec: int,
        temperature: float,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Returns (structured_json, raw_response_metadata)."""
