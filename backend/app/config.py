from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
UPLOADS_DIR = DATA_DIR / "uploads"
RESULTS_DIR = BASE_DIR / "results"
PARTIAL_EXPORT_CACHE_ENABLED = os.getenv("PARTIAL_EXPORT_CACHE_ENABLED", "1").lower() in {"1", "true", "yes"}
PARTIAL_EXPORT_CACHE_TTL_HOURS = max(1, int(os.getenv("PARTIAL_EXPORT_CACHE_TTL_HOURS", "6")))
PARTIAL_EXPORT_WAIT_SEC = max(1, int(os.getenv("PARTIAL_EXPORT_WAIT_SEC", "300")))

MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "200"))
MAX_UPLOAD_BYTES = max(1, MAX_UPLOAD_SIZE_MB) * 1024 * 1024
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "45"))
LLM_RETRIES = int(os.getenv("LLM_RETRIES", "2"))
# Ответ 429: сколько раз ждать на строку и максимальная пауза за раз. Ожидания
# не расходуют LLM_RETRIES; при умолчаниях строка ждёт лимит до ~3 минут.
LLM_RATE_LIMIT_RETRIES = max(0, int(os.getenv("LLM_RATE_LIMIT_RETRIES", "6")))
LLM_RATE_LIMIT_MAX_WAIT_SEC = max(1.0, float(os.getenv("LLM_RATE_LIMIT_MAX_WAIT_SEC", "60")))
# v2.0.0 итерация 3.2: хардкод-лимит снят, по умолчанию 100_000 строк в группе.
# Если группа не влезает в контекст модели — ловим honest error «Промпт превысил
# контекст модели …» и возвращаем пользователю. LLM сама разбирается с границами,
# мы лишние лимиты не навязываем. ENV override GROUP_MAX_ROWS оставлен как safety.
GROUP_MAX_ROWS = max(1, int(os.getenv("GROUP_MAX_ROWS", "100000")))
MAX_LLM_CACHE_ROWS = int(os.getenv("MAX_LLM_CACHE_ROWS", "2000000"))
CACHE_MAINTENANCE_INTERVAL_SEC = int(os.getenv("CACHE_MAINTENANCE_INTERVAL_SEC", "300"))
HASH_PARTITIONS = max(1, int(os.getenv("HASH_PARTITIONS", "100")))
REPORT_KEEP_LAST = max(1, int(os.getenv("REPORT_KEEP_LAST", "20")))
PAUSED_REPORT_TTL_DAYS = max(0, int(os.getenv("PAUSED_REPORT_TTL_DAYS", "0")))
# REPORT_CLEANUP_INTERVAL_SEC: 0 = выключить очистку, любое >0 = период в секундах
# (минимум 60 — защита от busy-loop, поднимется если задано меньше).
_raw_cleanup_interval = int(os.getenv("REPORT_CLEANUP_INTERVAL_SEC", "600"))
REPORT_CLEANUP_INTERVAL_SEC = 0 if _raw_cleanup_interval <= 0 else max(60, _raw_cleanup_interval)
# UPLOAD_ORPHAN_TTL_HOURS: 0 = не удалять висячие xlsx никогда, любое >0 = TTL в часах
# (минимум 1 — защита от слишком агрессивной очистки).
_raw_orphan_ttl = int(os.getenv("UPLOAD_ORPHAN_TTL_HOURS", "24"))
UPLOAD_ORPHAN_TTL_HOURS = 0 if _raw_orphan_ttl <= 0 else max(1, _raw_orphan_ttl)
GLOBAL_LLM_PARALLELISM = int(os.getenv("GLOBAL_LLM_PARALLELISM", "12"))
# Сколько отчётов одного пользователя могут одновременно занимать воркер
# (running + queued в общей Redis-очереди). Превышение идёт в личную очередь
# юзера (status='queued' + personal_queue=TRUE, не пушится в Redis), и
# промоутится в общую при освобождении слота. paused НЕ занимает slot.
# Минимум: 1.
MAX_ACTIVE_PER_USER = max(1, int(os.getenv("MAX_ACTIVE_PER_USER", "2")))
SEMANTIC_CACHE_ENABLED = os.getenv("SEMANTIC_CACHE_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "ollama").strip().lower()
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text").strip()
SEMANTIC_CACHE_THRESHOLD = float(os.getenv("SEMANTIC_CACHE_THRESHOLD", "0.94"))
SEMANTIC_CACHE_CANDIDATES = int(os.getenv("SEMANTIC_CACHE_CANDIDATES", "200"))
MAX_SEMANTIC_CACHE_ROWS = int(os.getenv("MAX_SEMANTIC_CACHE_ROWS", "500000"))
TLS_VERIFY = os.getenv("TLS_VERIFY", "1").strip().lower() in {"1", "true", "yes"}

# OpenID Connect. По умолчанию SSO отключено.
SSO_ENABLED = os.getenv("SSO_ENABLED", "0").strip().lower() in {"1", "true", "yes"}
SSO_ISSUER_URL = os.getenv("SSO_ISSUER_URL", "").strip()
SSO_CLIENT_ID = os.getenv("SSO_CLIENT_ID", "").strip()
SSO_CLIENT_SECRET = os.getenv("SSO_CLIENT_SECRET", "").strip()
SSO_REDIRECT_URL = os.getenv("SSO_REDIRECT_URL", "").strip()
SSO_ALLOW_HTTP = os.getenv("SSO_ALLOW_HTTP", "0").strip().lower() in {"1", "true", "yes"}
SSO_CA_FILE = os.getenv("SSO_CA_FILE", "").strip()
SSO_TOKEN_AUTH_METHOD = os.getenv("SSO_TOKEN_AUTH_METHOD", "auto").strip()
SSO_STATE_TTL_SEC = 600  # 10 минут на завершение login flow
LOCAL_AUTH_ENABLED = os.getenv("LOCAL_AUTH_ENABLED", "1").strip().lower() in {"1", "true", "yes"}
_registration_setting = os.getenv("LOCAL_REGISTRATION_ENABLED", "").strip().lower()
LOCAL_REGISTRATION_ENABLED = LOCAL_AUTH_ENABLED and (
    _registration_setting in {"1", "true", "yes"} if _registration_setting else not SSO_ENABLED
)

# Список preferred_username из IdP, которым при SSO-логине выдаётся роль admin.
# Совпадение проверяется case-insensitive. Такие роли закреплены при входе;
# остальные назначения администраторов хранятся в БД.
SSO_ADMIN_USERNAMES = tuple(
    u.strip().casefold()
    for u in os.getenv("SSO_ADMIN_USERNAMES", "").split(",")
    if u.strip()
)
SSO_ADMIN_SUBJECTS = frozenset(filter(None, (
    subject.strip() for subject in os.getenv("SSO_ADMIN_SUBJECTS", "").split(",")
)))

def sso_is_configured() -> bool:
    """SSO считается включённым только если все обязательные поля заданы."""
    return SSO_ENABLED and bool(SSO_ISSUER_URL and SSO_CLIENT_ID and SSO_CLIENT_SECRET and SSO_REDIRECT_URL)


def sso_admin_role(provider: str | None, subject: str | None, login_name: str | None) -> bool:
    return bool(sso_is_configured() and provider == SSO_ISSUER_URL and subject and (
        subject in SSO_ADMIN_SUBJECTS or (login_name or "").casefold() in SSO_ADMIN_USERNAMES
    ))

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
        "models_env": "OPENAI_MODELS",
        "base_url_env": "OPENAI_BASE_URL",
        "base_url_default": "https://api.openai.com",

    },
    "ollama": {
        "label": "Ollama (внешний хост)",
        "models": ["llama3.1:latest"],
        "models_env": "OLLAMA_MODELS",
        "base_url_env": "OLLAMA_BASE_URL",
        "base_url_default": "",

    },
}


def _read_provider_config() -> dict[str, Any]:
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

    return loaded if loaded is not None else _DEFAULT_PROVIDER_CONFIG


def _load_provider_config() -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for provider_id, conf in _read_provider_config().items():
        if not isinstance(conf, dict):
            continue
        label = str(conf.get("label") or provider_id)
        label_env = conf.get("label_env")
        if isinstance(label_env, str) and label_env.strip():
            label_override = os.getenv(label_env.strip(), "").strip()
            if label_override:
                label = label_override
        base_url = ""
        base_url_env = conf.get("base_url_env")
        if isinstance(base_url_env, str) and base_url_env.strip():
            base_url = os.getenv(base_url_env.strip(), "").strip()
        if not base_url:
            base_url = str(conf.get("base_url_default") or "").strip()

        if provider_id == "ollama" and not base_url:
            continue

        normalized[provider_id] = {
            "label": label,

            "base_url_env": conf.get("base_url_env"),
            "base_url_default": conf.get("base_url_default"),

            "label_env": conf.get("label_env"),
        }
    return normalized



PROVIDER_CONFIG = _load_provider_config()


def initial_model_catalog() -> list[tuple[str, str]]:
    """Модели из настроек v2.0 для однократной инициализации каталога."""
    catalog = []
    for provider, settings in _read_provider_config().items():
        if provider not in {"openai", "ollama"} or provider not in PROVIDER_CONFIG or not isinstance(settings, dict):
            continue
        configured = settings.get("models_env")
        raw = os.getenv(configured, "").strip() if isinstance(configured, str) else ""
        models = raw.split(",") if raw else settings.get("models", [])
        if isinstance(models, list):
            names = dict.fromkeys(name.strip() for name in models if isinstance(name, str) and name.strip())
            catalog.extend((provider, name) for name in names if len(name) <= 200)
    return catalog


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
