from __future__ import annotations

from .base import LLMProvider
from .ollama_provider import OllamaProvider
from .openai_provider import OpenAIProvider


def build_provider(provider_id: str) -> LLMProvider:
    providers: dict[str, LLMProvider] = {
        "openai": OpenAIProvider(),
        "ollama": OllamaProvider(),
    }
    if provider_id not in providers:
        raise ValueError(f"Unsupported provider: {provider_id}")
    return providers[provider_id]
