from __future__ import annotations

from typing import Any

import httpx

from app.config import EMBEDDING_MODEL, EMBEDDING_PROVIDER, REQUEST_TIMEOUT_SEC, TLS_VERIFY, get_provider_base_url


def _to_float_list(values: Any) -> list[float]:
    if not isinstance(values, list):
        raise ValueError("Embedding payload is not a list")
    out: list[float] = []
    for item in values:
        if isinstance(item, (int, float)):
            out.append(float(item))
    if not out:
        raise ValueError("Empty embedding vector")
    return out


async def build_embedding(text: str, api_key: str | None = None) -> list[float]:
    normalized = " ".join((text or "").split()).strip()
    if not normalized:
        raise ValueError("Cannot build embedding for empty text")

    provider = EMBEDDING_PROVIDER
    timeout = max(5, REQUEST_TIMEOUT_SEC)

    if provider == "openai":
        base_url = get_provider_base_url("openai", "https://api.openai.com")
        base = base_url.rstrip("/")
        v1_base = base if base.endswith("/v1") else f"{base}/v1"
        clean_key = (api_key or "").strip()
        if clean_key.lower().startswith("authorization:"):
            clean_key = clean_key.split(":", 1)[1].strip()
        if clean_key.lower().startswith("bearer "):
            clean_key = clean_key[7:].strip()
        if not clean_key:
            raise ValueError("Embedding provider openai requires api_key")
        headers = {"Authorization": f"Bearer {clean_key}", "Content-Type": "application/json"}
        body = {"model": EMBEDDING_MODEL, "input": normalized}
        async with httpx.AsyncClient(timeout=timeout, verify=TLS_VERIFY) as client:
            resp = await client.post(f"{v1_base}/embeddings", headers=headers, json=body)
            resp.raise_for_status()
            payload = resp.json()
        data = payload.get("data")
        if not isinstance(data, list) or not data:
            raise ValueError("OpenAI embeddings response has no data")
        return _to_float_list((data[0] or {}).get("embedding"))

    if provider == "ollama":
        base_url = get_provider_base_url("ollama", "http://localhost:11434")
        async with httpx.AsyncClient(timeout=timeout) as client:
            # Older Ollama API
            resp = await client.post(
                f"{base_url}/api/embeddings",
                json={"model": EMBEDDING_MODEL, "prompt": normalized},
            )
            if resp.status_code == 404:
                # Newer Ollama API
                resp = await client.post(
                    f"{base_url}/api/embed",
                    json={"model": EMBEDDING_MODEL, "input": normalized},
                )
            resp.raise_for_status()
            payload = resp.json()

        if isinstance(payload.get("embedding"), list):
            return _to_float_list(payload.get("embedding"))
        embeddings = payload.get("embeddings")
        if isinstance(embeddings, list) and embeddings:
            return _to_float_list(embeddings[0])
        raise ValueError("Ollama embeddings response has no vector")

    raise ValueError(f"Unsupported embedding provider: {provider}")
