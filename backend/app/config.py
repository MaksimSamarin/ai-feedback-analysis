from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
RESULTS_DIR = BASE_DIR / "results"

MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "200"))
MAX_UPLOAD_BYTES = max(1, MAX_UPLOAD_SIZE_MB) * 1024 * 1024
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
LLM_RETRIES = int(os.getenv("LLM_RETRIES", "2"))
# v2.0.0 итерация 3.2: хардкод-лимит снят, по умолчанию 100_000 строк в группе.
# Если группа не влезает в контекст модели — ловим honest error «Промпт превысил
# контекст модели …» и возвращаем пользователю. LLM сама разбирается с границами,
# мы лишние лимиты не навязываем. ENV override GROUP_MAX_ROWS оставлен как safety.
GROUP_MAX_ROWS = max(1, int(os.getenv("GROUP_MAX_ROWS", "100000")))
MAX_LLM_CACHE_ROWS = int(os.getenv("MAX_LLM_CACHE_ROWS", "2000000"))
CACHE_MAINTENANCE_INTERVAL_SEC = int(os.getenv("CACHE_MAINTENANCE_INTERVAL_SEC", "300"))
HASH_PARTITIONS = max(1, int(os.getenv("HASH_PARTITIONS", "100")))
REPORT_KEEP_LAST = max(1, int(os.getenv("REPORT_KEEP_LAST", "20")))
REPORT_CLEANUP_INTERVAL_SEC = max(60, int(os.getenv("REPORT_CLEANUP_INTERVAL_SEC", "600")))
UPLOAD_ORPHAN_TTL_HOURS = max(1, int(os.getenv("UPLOAD_ORPHAN_TTL_HOURS", "24")))
REPORT_CLEANUP_ENABLED = os.getenv("REPORT_CLEANUP_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
GLOBAL_LLM_PARALLELISM = int(os.getenv("GLOBAL_LLM_PARALLELISM", "12"))
SEMANTIC_CACHE_ENABLED = os.getenv("SEMANTIC_CACHE_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "ollama").strip().lower()
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text").strip()
SEMANTIC_CACHE_THRESHOLD = float(os.getenv("SEMANTIC_CACHE_THRESHOLD", "0.94"))
SEMANTIC_CACHE_CANDIDATES = int(os.getenv("SEMANTIC_CACHE_CANDIDATES", "200"))
MAX_SEMANTIC_CACHE_ROWS = int(os.getenv("MAX_SEMANTIC_CACHE_ROWS", "500000"))
TLS_VERIFY = os.getenv("TLS_VERIFY", "0").strip().lower() in {"1", "true", "yes"}

DEFAULT_PROMPT = """Ты аналитик отзывов.
Верни только валидный JSON без markdown и без лишнего текста.

Требования:
- summary: 1-2 предложения, не более 240 символов.
- category: выбери одно значение из перечисления.
- confidence: число от 0 до 1; оценивай отдельно для каждой строки и не используй одно и то же значение по умолчанию.
- sentiment_label: negative|neutral|positive.
- negativity_score: число от 0 до 1, где 1 = максимально негативно.
- Промпт определяет смысл заполнения полей.
- EXPECTED_JSON определяет только структуру ответа, типы данных и допустимые значения.
- Не добавляй лишние поля.

Входные данные строки:
{row_json}
"""

_DEFAULT_PROVIDER_CONFIG: dict[str, Any] = {
    "openai": {
        "label": "OpenAI",
        "label_env": "OPENAI_PROVIDER_LABEL",
        "models": ["gpt-4o-mini", "gpt-4.1-mini"],
        "base_url_env": "OPENAI_BASE_URL",
        "base_url_default": "https://api.openai.com",
        "models_env": "OPENAI_MODELS",
    },
    "ollama": {
        "label": "Ollama (внешний хост)",
        "models": ["llama3.1:latest"],
        "base_url_env": "OLLAMA_BASE_URL",
        "base_url_default": "",
        "models_env": "OLLAMA_MODELS",
    },
}


def _load_provider_config() -> dict[str, Any]:
    config_path = Path(
        os.getenv("AI_PROVIDERS_CONFIG_PATH", str(BASE_DIR / "config" / "ai_providers.json"))
    )
    loaded: dict[str, Any] | None = None
    if config_path.exists():
        try:
            raw = json.loads(config_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("providers"), dict):
                loaded = raw["providers"]
        except Exception:
            loaded = None

    providers = loaded if loaded is not None else _DEFAULT_PROVIDER_CONFIG
    normalized: dict[str, Any] = {}
    for provider_id, conf in providers.items():
        if not isinstance(conf, dict):
            continue
        label = str(conf.get("label") or provider_id)
        label_env = conf.get("label_env")
        if isinstance(label_env, str) and label_env.strip():
            label_override = os.getenv(label_env.strip(), "").strip()
            if label_override:
                label = label_override
        models = conf.get("models")
        if not isinstance(models, list):
            models = []
        models = [str(item).strip() for item in models if str(item).strip()]
        models_env = conf.get("models_env")
        if isinstance(models_env, str) and models_env.strip():
            raw_models = os.getenv(models_env.strip(), "").strip()
            if raw_models:
                models = [item.strip() for item in raw_models.split(",") if item.strip()]

        base_url = ""
        base_url_env = conf.get("base_url_env")
        if isinstance(base_url_env, str) and base_url_env.strip():
            base_url = os.getenv(base_url_env.strip(), "").strip()
        if not base_url:
            base_url = str(conf.get("base_url_default") or "").strip()

        # Ollama remains optional: hide it unless both URL and models are set.
        if provider_id == "ollama" and (not base_url or not models):
            continue

        normalized[provider_id] = {
            "label": label,
            "models": models,
            "base_url_env": conf.get("base_url_env"),
            "base_url_default": conf.get("base_url_default"),
            "models_env": conf.get("models_env"),
            "label_env": conf.get("label_env"),
        }
    return normalized


PROVIDER_CONFIG = _load_provider_config()


def get_provider_base_url(provider_id: str, fallback: str = "") -> str:
    conf = PROVIDER_CONFIG.get(provider_id) or {}
    env_name = conf.get("base_url_env")
    if isinstance(env_name, str) and env_name.strip():
        value = os.getenv(env_name.strip(), "").strip()
        if value:
            return value.rstrip("/")

    default_value = conf.get("base_url_default")
    if isinstance(default_value, str) and default_value.strip():
        return default_value.rstrip("/")

    return fallback.rstrip("/")
