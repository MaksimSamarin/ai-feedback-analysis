from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import BackgroundTasks, Cookie, Depends, FastAPI, File, Header, HTTPException, Query, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse, StreamingResponse

from app import __version__ as APP_VERSION
from app import access
from app.auth_utils import hash_password, password_needs_rehash, validate_password_policy, verify_password
from app import sso, user_profile
from app.providers.openai_provider import normalize_api_key
from app.config import (
    DEFAULT_PROMPT,
    GROUP_MAX_ROWS,
    GLOBAL_LLM_PARALLELISM,
    MAX_ACTIVE_PER_USER,
    MAX_UPLOAD_BYTES,
    MAX_UPLOAD_SIZE_MB,
    PROVIDER_CONFIG,
    RESULTS_DIR,
    LOCAL_AUTH_ENABLED,
    LOCAL_REGISTRATION_ENABLED,
    SSO_REDIRECT_URL,
    SSO_STATE_TTL_SEC,
    TLS_VERIFY,
    UPLOADS_DIR,
    get_provider_base_url,
    sso_is_configured,
)
from app.crypto_utils import encrypt_text
from app.db import (
    admin_runtime_stats,
    add_uploaded_file,
    count_active_slots,
    create_report,
    create_session,
    create_user,
    get_or_create_sso_user,
    update_user_password,
    delete_user_preset,
    delete_report,
    delete_session,
    get_report,
    get_report_any,
    get_report_by_job_id,
    get_session_user,
    get_user_usage,
    get_uploaded_file,
    get_user_by_id,
    get_ui_language,
    get_redis_client,
    get_job_logs_bulk,
    build_report_analysis,
    list_report_rows,
    list_recent_report_failures,
    list_active_reports,
    list_reports_by_user,
    get_user_by_username,
    init_db,
    list_users_admin,
    list_user_presets,
    list_reports,
    reset_failed_and_skipped_rows,
    reset_report_terminal_state,
    set_personal_queue,
    queue_paused_report,
    update_report_progress,
    upsert_user_preset,
    update_report_status,
    update_uploaded_file_inspect,
    iter_report_rows,
)
from app.services.excel_service import export_errors_xlsx
from app.job_payloads import build_job_payload_from_report
from app.job_settings import settings_warnings
from app.queue import (
    clear_job_dedup_marker,
    enqueue_inspect_job,
    enqueue_job,
    get_inspect_queue_position,
    get_job_queue_position,
    has_queued_marker,
    has_running_lease,
    remove_job_from_queue,
)
from app.logging_utils import (
    configure_logging,
    current_context,
    reset_request_id,
    reset_user_context,
    set_request_id,
    set_user_context,
)
from app.schemas import (
    AdminLogItem,
    AuthRequest,
    AuthResponse,
    BulkDeleteRequest,
    AdminLogsResponse,
    AdminFailureItem,
    AdminStatsResponse,
    AdminUserItem,
    AdminUsersResponse,
    FileInspectResponse,
    JobStatus,
    ModelsResponse,
    PresetItem,
    PresetsResponse,
    PresetUpsertRequest,
    ProviderInfo,
    ProvidersResponse,
    ReportItem,
    ReportAnalysisResponse,
    ReportsResponse,
    SheetInfo,
    StartJobRequest,
    UsageResponse,
    UserMeResponse,
    VerifyTokenRequest,
    VerifyTokenResponse,
)
SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "session_token").strip() or "session_token"
SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "0").strip().lower() in {"1", "true", "yes"}
BOOTSTRAP_ADMIN_USERNAME = os.getenv("BOOTSTRAP_ADMIN_USERNAME", "").strip()
BOOTSTRAP_ADMIN_PASSWORD = os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "").strip()
BOOTSTRAP_ADMIN_FORCE_PASSWORD = os.getenv("BOOTSTRAP_ADMIN_FORCE_PASSWORD", "0").strip().lower() in {"1", "true", "yes"}
CORS_ALLOW_ORIGINS = [
    origin.strip()
    for origin in os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:8080,http://127.0.0.1:8080").split(",")
    if origin.strip()
]
APP_LOG_DIR = Path(os.getenv("APP_LOG_DIR", "/app/data/logs")).resolve()
NOISY_ACCESS_LOG_PATH_PREFIXES = (
    "/api/jobs/",
    "/api/reports",
    "/api/auth/usage",
    "/api/health",
)
NOISY_ACCESS_LOG_SLOW_MS = max(50, int(os.getenv("NOISY_ACCESS_LOG_SLOW_MS", "250")))
SESSION_USER_CACHE_TTL_SEC = max(0.0, float(os.getenv("SESSION_USER_CACHE_TTL_SEC", "15")))
SESSION_USER_CACHE_MAX = max(100, int(os.getenv("SESSION_USER_CACHE_MAX", "5000")))
_SESSION_USER_CACHE: dict[str, tuple[float, dict[str, Any] | None]] = {}

app = FastAPI(title="Analyzer API", version=APP_VERSION)


logger = logging.getLogger("review_analyzer.api")


def _decode_uploaded_sheets(raw: object) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def _to_file_inspect_response(row: dict[str, Any]) -> FileInspectResponse:
    sheets_payload = _decode_uploaded_sheets(row.get("inspect_sheets_json"))
    inspect_status = str(row.get("inspect_status") or "ready")
    file_id = str(row.get("id") or "")
    queue_position: int | None = None
    if inspect_status in {"queued", "parsing"} and file_id:
        try:
            queue_position = get_inspect_queue_position(file_id)
        except Exception:
            queue_position = None
    return FileInspectResponse(
        file_id=file_id,
        filename=str(row.get("original_name") or ""),
        sheets=[SheetInfo(**item) for item in sheets_payload],
        suggested_sheet=str(row.get("inspect_suggested_sheet") or "") or None,
        suggested_column=str(row.get("inspect_suggested_column") or "") or None,
        inspect_status=inspect_status,
        inspect_error_text=str(row.get("inspect_error_text") or "") or None,
        queue_position=queue_position,
    )


def _prune_session_cache(now_ts: float) -> None:
    if len(_SESSION_USER_CACHE) <= SESSION_USER_CACHE_MAX:
        return
    expired_tokens = [tok for tok, (exp, _) in _SESSION_USER_CACHE.items() if exp <= now_ts]
    for tok in expired_tokens:
        _SESSION_USER_CACHE.pop(tok, None)
    if len(_SESSION_USER_CACHE) <= SESSION_USER_CACHE_MAX:
        return
    for tok in list(_SESSION_USER_CACHE.keys())[: len(_SESSION_USER_CACHE) - SESSION_USER_CACHE_MAX]:
        _SESSION_USER_CACHE.pop(tok, None)


def _cache_session_user(token: str, user: dict[str, Any] | None) -> None:
    if not token or SESSION_USER_CACHE_TTL_SEC <= 0:
        return
    now_ts = time.monotonic()
    _SESSION_USER_CACHE[token] = (now_ts + SESSION_USER_CACHE_TTL_SEC, user)
    _prune_session_cache(now_ts)


def _invalidate_session_user_cache(token: str | None) -> None:
    if token:
        _SESSION_USER_CACHE.pop(token, None)


def _get_session_user_cached(token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    if SESSION_USER_CACHE_TTL_SEC <= 0:
        return get_session_user(token)
    now_ts = time.monotonic()
    cached = _SESSION_USER_CACHE.get(token)
    if cached and cached[0] > now_ts:
        return cached[1]
    user = get_session_user(token)
    _SESSION_USER_CACHE[token] = (now_ts + SESSION_USER_CACHE_TTL_SEC, user)
    _prune_session_cache(now_ts)
    return user


def _normalize_report_row(row: dict[str, Any]) -> ReportItem:
    summary = None
    output_schema = None
    expected_json_template = None
    input_columns = None
    non_analysis_columns = None
    raw_summary = row.get("summary_json")
    if raw_summary:
        try:
            summary = json.loads(raw_summary)
        except Exception:
            summary = None
    raw_output_schema = row.get("output_schema_json")
    if raw_output_schema:
        try:
            output_schema = json.loads(raw_output_schema)
        except Exception:
            output_schema = None
    raw_input_columns = row.get("input_columns_json")
    if raw_input_columns:
        try:
            parsed_input_columns = json.loads(raw_input_columns)
            if isinstance(parsed_input_columns, list):
                input_columns = [str(item) for item in parsed_input_columns]
        except Exception:
            input_columns = None
    raw_expected_json_template = row.get("expected_json_template_json")
    if raw_expected_json_template:
        try:
            parsed_expected = json.loads(raw_expected_json_template)
            if isinstance(parsed_expected, dict):
                expected_json_template = parsed_expected
        except Exception:
            expected_json_template = None
    raw_non_analysis_columns = row.get("non_analysis_columns_json")
    if raw_non_analysis_columns:
        try:
            parsed_non_analysis_columns = json.loads(raw_non_analysis_columns)
            if isinstance(parsed_non_analysis_columns, list):
                non_analysis_columns = [str(item) for item in parsed_non_analysis_columns]
        except Exception:
            non_analysis_columns = None
    base = dict(row)
    from app.report_outcome import presentation
    base.update(presentation(row.get("status"), row.get("processed_rows"), row.get("failed_rows")))
    base["summary_json"] = summary
    base["output_schema_json"] = output_schema
    base["expected_json_template_json"] = expected_json_template
    base["input_columns_json"] = input_columns
    base["non_analysis_columns_json"] = non_analysis_columns
    # source_filename — имя исходного файла без расширения (если было в JOIN'е list_reports;
    # в get_report/get_report_any JOIN'а нет, поле останется None до посещения списка).
    original_name = str(base.pop("source_original_name", "") or "").strip()
    if original_name:
        base["source_filename"] = Path(original_name).stem or original_name
    else:
        base.setdefault("source_filename", None)
    base["group_total"] = int(base.get("group_total", base.get("groups_total")) or 0)
    base["group_processed"] = int(base.get("group_processed", base.get("groups_processed")) or 0)
    return ReportItem(**base)


def _normalize_report_row_light(row: dict[str, Any]) -> ReportItem:
    # Lightweight serializer for frequent polling endpoints:
    # avoid JSON decode overhead on large report metadata fields.
    # ВАЖНО: summary_json НЕ зануляем — это маленький JSON (success_rows,
    # failed_rows + 2-3 счётчика, ~150 байт), но именно из него фронт
    # обновляет карточки «Результаты». Worker пишет это поле в БД атомарно
    # с processed_rows под одним lock'ом — при polling'е фронт всегда читает
    # согласованный snapshot прогресса и summary.
    base = dict(row)
    raw_summary = base.get("summary_json")
    if raw_summary:
        try:
            parsed = json.loads(raw_summary)
            base["summary_json"] = parsed if isinstance(parsed, dict) else None
        except Exception:
            base["summary_json"] = None
    else:
        base["summary_json"] = None
    base["output_schema_json"] = None
    base["expected_json_template_json"] = None
    base["input_columns_json"] = None
    base["non_analysis_columns_json"] = None
    # «Название отчёта» в таблице — имя загруженного файла без расширения.
    # list_reports() тянет uploaded_files.original_name через LEFT JOIN; тут отрезаем .xlsx/.csv.
    original_name = str(base.pop("source_original_name", "") or "").strip()
    if original_name:
        base["source_filename"] = Path(original_name).stem or original_name
    else:
        base.setdefault("source_filename", None)
    # group_total/group_processed приходят из материализованных колонок
    # reports.groups_total / reports.groups_processed (alias'ы в _REPORTS_LIST_SELECT).
    # Раньше считались LEFT JOIN'ом по report_rows на каждый запрос — теперь
    # worker сам поддерживает счётчики в памяти и пишет атомарно с processed_rows.
    # Для обычных отчётов без группировки оба 0.
    base["group_total"] = int(base.get("group_total", base.get("groups_total")) or 0)
    base["group_processed"] = int(base.get("group_processed", base.get("groups_processed")) or 0)
    return ReportItem(**base)




def _v1_base(base_url: str) -> str:
    base = (base_url or "").rstrip("/")
    return base if base.endswith("/v1") else f"{base}/v1"


def _tail_file_lines(path: Path, limit: int) -> list[str]:
    if limit <= 0 or not path.exists():
        return []
    try:
        # Read from the end of file to avoid full-file scan on every admin refresh.
        with path.open("rb") as fh:
            fh.seek(0, os.SEEK_END)
            file_size = fh.tell()
            if file_size <= 0:
                return []
            block_size = 8192
            pos = file_size
            buf = bytearray()
            newline_count = 0
            while pos > 0 and newline_count <= limit:
                read_size = min(block_size, pos)
                pos -= read_size
                fh.seek(pos)
                chunk = fh.read(read_size)
                if not chunk:
                    break
                buf[:0] = chunk
                newline_count = buf.count(b"\n")
            text = buf.decode("utf-8", errors="replace")
            lines = text.splitlines()
    except Exception:
        return []
    return [line.rstrip("\r\n") for line in lines[-limit:]]


def _read_admin_logs(service: str, limit: int, level: str | None = None, query: str | None = None) -> list[AdminLogItem]:
    service_norm = (service or "all").strip().lower()
    if service_norm not in {"all", "backend", "worker"}:
        raise HTTPException(status_code=400, detail="service должен быть one of: all, backend, worker")
    paths: list[Path]
    if service_norm == "all":
        paths = [APP_LOG_DIR / "backend.log", APP_LOG_DIR / "worker.log"]
    else:
        paths = [APP_LOG_DIR / f"{service_norm}.log"]

    combined_raw: list[str] = []
    for p in paths:
        combined_raw.extend(_tail_file_lines(p, limit))
    if len(combined_raw) > limit:
        combined_raw = combined_raw[-limit:]

    level_norm = (level or "").strip().upper()
    query_norm = (query or "").strip().lower()
    items: list[AdminLogItem] = []
    for line in combined_raw:
        if not line.strip():
            continue
        parsed: dict[str, Any] | None = None
        try:
            maybe = json.loads(line)
            if isinstance(maybe, dict):
                parsed = maybe
        except Exception:
            parsed = None

        ts = str(parsed.get("ts")) if parsed and parsed.get("ts") is not None else None
        lvl = str(parsed.get("level")) if parsed and parsed.get("level") is not None else None
        svc = str(parsed.get("service")) if parsed and parsed.get("service") is not None else None
        logger_name = str(parsed.get("logger")) if parsed and parsed.get("logger") is not None else None
        request_id = str(parsed.get("request_id")) if parsed and parsed.get("request_id") is not None else None
        user_id = str(parsed.get("user_id")) if parsed and parsed.get("user_id") is not None else None
        username = str(parsed.get("username")) if parsed and parsed.get("username") is not None else None
        msg = str(parsed.get("message")) if parsed and parsed.get("message") is not None else line

        if level_norm and lvl and lvl.upper() != level_norm:
            continue
        if query_norm and query_norm not in msg.lower() and query_norm not in line.lower():
            continue

        items.append(
            AdminLogItem(
                ts=ts,
                level=lvl,
                service=svc,
                logger=logger_name,
                request_id=request_id,
                user_id=user_id,
                username=username,
                message=msg,
                raw=line,
            )
        )
    return items[-limit:]




def _normalize_expected_field_schema(field_name: str, schema: Any) -> dict[str, Any]:
    if not isinstance(schema, dict):
        raise ValueError(f"Поле '{field_name}' должно быть объектом схемы, например {{\"type\": \"string\"}}")

    schema_type = str(schema.get("type") or "").strip().lower()
    if not schema_type:
        raise ValueError(f"Поле '{field_name}' должно содержать ключ 'type'")

    if schema_type == "enum":
        values = schema.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError(f"Поле '{field_name}' с type=enum должно содержать непустой массив 'values'")
        normalized_values = [str(item).strip() for item in values if str(item).strip()]
        if len(normalized_values) != len(values) or not normalized_values:
            raise ValueError(f"Поле '{field_name}' с type=enum содержит пустые значения")
        return {"type": "string", "enum": normalized_values}

    if schema_type == "string":
        normalized: dict[str, Any] = {"type": "string"}
        min_length = schema.get("min_length")
        max_length = schema.get("max_length")
        if min_length is not None:
            if not isinstance(min_length, int) or min_length < 0:
                raise ValueError(f"Поле '{field_name}': min_length должен быть целым числом >= 0")
            normalized["min_length"] = min_length
        if max_length is not None:
            if not isinstance(max_length, int) or max_length < 1:
                raise ValueError(f"Поле '{field_name}': max_length должен быть целым числом >= 1")
            normalized["max_length"] = max_length
        if (
            isinstance(normalized.get("min_length"), int)
            and isinstance(normalized.get("max_length"), int)
            and normalized["min_length"] > normalized["max_length"]
        ):
            raise ValueError(f"Поле '{field_name}': min_length не может быть больше max_length")
        return normalized

    if schema_type == "date":
        return {"type": "string", "format": "date"}

    if schema_type == "datetime":
        return {"type": "string", "format": "date-time"}

    if schema_type in {"number", "integer"}:
        normalized = {"type": schema_type}
        min_value = schema.get("min")
        max_value = schema.get("max")
        if min_value is not None:
            if not isinstance(min_value, (int, float)):
                raise ValueError(f"Поле '{field_name}': min должен быть числом")
            normalized["minimum"] = min_value
        if max_value is not None:
            if not isinstance(max_value, (int, float)):
                raise ValueError(f"Поле '{field_name}': max должен быть числом")
            normalized["maximum"] = max_value
        if (
            isinstance(normalized.get("minimum"), (int, float))
            and isinstance(normalized.get("maximum"), (int, float))
            and float(normalized["minimum"]) > float(normalized["maximum"])
        ):
            raise ValueError(f"Поле '{field_name}': min не может быть больше max")
        return normalized

    if schema_type == "boolean":
        return {"type": "boolean"}

    if schema_type == "array":
        items = schema.get("items")
        if not isinstance(items, dict):
            raise ValueError(f"Поле '{field_name}' с type=array должно содержать объект 'items'")
        normalized = {
            "type": "array",
            "items": _normalize_expected_field_schema(f"{field_name}[]", items),
        }
        min_items = schema.get("min_items")
        max_items = schema.get("max_items")
        if min_items is not None:
            if not isinstance(min_items, int) or min_items < 0:
                raise ValueError(f"Поле '{field_name}': min_items должен быть целым числом >= 0")
            normalized["min_items"] = min_items
        if max_items is not None:
            if not isinstance(max_items, int) or max_items < 0:
                raise ValueError(f"Поле '{field_name}': max_items должен быть целым числом >= 0")
            normalized["max_items"] = max_items
        if (
            isinstance(normalized.get("min_items"), int)
            and isinstance(normalized.get("max_items"), int)
            and normalized["min_items"] > normalized["max_items"]
        ):
            raise ValueError(f"Поле '{field_name}': min_items не может быть больше max_items")
        return normalized

    if schema_type == "object":
        normalized = {"type": "object"}
        properties_raw = schema.get("properties")
        if properties_raw is not None:
            if not isinstance(properties_raw, dict) or not properties_raw:
                raise ValueError(f"Поле '{field_name}' с type=object должно содержать непустой объект 'properties'")
            properties = {
                str(child_name): _normalize_expected_field_schema(f"{field_name}.{child_name}", child_schema)
                for child_name, child_schema in properties_raw.items()
            }
            normalized["properties"] = properties
            required_raw = schema.get("required")
            if required_raw is None:
                normalized["required"] = list(properties.keys())
            else:
                if not isinstance(required_raw, list):
                    raise ValueError(f"Поле '{field_name}': required должен быть массивом строк")
                required = [str(item) for item in required_raw]
                unknown_required = [item for item in required if item not in properties]
                if unknown_required:
                    raise ValueError(
                        f"Поле '{field_name}': в required есть неизвестные поля: {', '.join(unknown_required)}"
                    )
                normalized["required"] = required
        return normalized

    raise ValueError(
        f"Поле '{field_name}': неподдерживаемый type='{schema_type}'. "
        "Поддерживаются: string, number, integer, boolean, enum, array, object, date, datetime"
    )


def _build_output_schema_from_expected_json_template(template: dict[str, Any]) -> dict[str, Any]:
    if not template:
        raise ValueError("Ожидаемый JSON не должен быть пустым объектом")
    properties: dict[str, Any] = {}
    for key, value in template.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("Ожидаемый JSON содержит пустой ключ")
        properties[key] = _normalize_expected_field_schema(key, value)
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
    }


def _validate_expected_json_template(template: dict[str, Any]) -> None:
    if not template:
        raise ValueError("Ожидаемый JSON не должен быть пустым объектом")
    # Структурная валидация каждого поля (type, values, min/max и т.д.).
    # Обязательные core-поля (summary/category/confidence) убраны в v2.0.0, итерация 1 —
    # пользователь сам определяет схему под свою задачу.
    _build_output_schema_from_expected_json_template(template)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
configure_logging("backend")
init_db()


def _bootstrap_admin() -> None:
    username = BOOTSTRAP_ADMIN_USERNAME
    password = BOOTSTRAP_ADMIN_PASSWORD
    if not LOCAL_AUTH_ENABLED or not username or not password:
        return
    policy_error = validate_password_policy(password)
    if policy_error:
        logger.warning("Bootstrap admin skipped: invalid password policy (%s)", policy_error)
        return
    existing = get_user_by_username(username)
    if not existing:
        if create_user(username, hash_password(password), role="admin") is not None:
            logger.info("Bootstrap admin created: username=%s", username)
        return
    if str(existing.get("role") or "") != "admin":
        logger.warning("Bootstrap admin user exists but role is not admin: username=%s", username)
    if BOOTSTRAP_ADMIN_FORCE_PASSWORD:
        if update_user_password(username, hash_password(password)):
            logger.info("Bootstrap admin password updated: username=%s", username)


_bootstrap_admin()


def _should_log_access_request(path: str, status_code: int, duration_ms: int) -> bool:
    # Polling-heavy endpoints produce huge volumes of logs and CPU overhead.
    # Keep them only when they are slow or unsuccessful.
    if any(path.startswith(prefix) for prefix in NOISY_ACCESS_LOG_PATH_PREFIXES):
        return status_code >= 400 or duration_ms >= NOISY_ACCESS_LOG_SLOW_MS
    return True


@app.middleware("http")
async def request_context_middleware(request, call_next):
    started = datetime.utcnow()
    request_id = request.headers.get("X-Request-ID", "").strip() or uuid.uuid4().hex[:12]
    req_token = set_request_id(request_id)
    request_id = current_context()["request_id"]
    auth_header = request.headers.get("Authorization")
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    token = _parse_token(auth_header) or session_token
    user_obj = _get_session_user_cached(token) if token else None
    user_tokens = set_user_context(
        user_obj.get("id") if isinstance(user_obj, dict) else None,
        user_obj.get("username") if isinstance(user_obj, dict) else None,
    )
    try:
        path = request.url.path
        if (
            path.startswith("/api/")
            and path != "/api/health"
            and not (path in {"/api/release-notes", "/api/ui/settings"} and request.method == "GET")
            and not path.startswith(("/api/auth/", "/api/access/"))
        ):
            if not user_obj:
                return JSONResponse({"detail": "Требуется авторизация"}, status_code=401)
            try:
                await asyncio.to_thread(require_access, user_obj)
            except HTTPException as exc:
                return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
        response = await call_next(request)
        duration_ms = int((datetime.utcnow() - started).total_seconds() * 1000)
        if _should_log_access_request(request.url.path, int(response.status_code), duration_ms):
            access_log = logger.error if response.status_code >= 500 else logger.warning if response.status_code >= 400 else logger.info
            access_log(
                "HTTP request: method=%s path=%s status=%s duration_ms=%s",
                request.method,
                request.url.path,
                response.status_code,
                duration_ms,
                extra={"event_type": "http.request", "status_code": int(response.status_code), "duration_ms": duration_ms},
            )
        if response.status_code == 200 and "/reports/" in request.url.path and "/download/" in request.url.path:
            from app.events import record_export
            report_id = request.url.path.split("/reports/", 1)[1].split("/", 1)[0]
            await asyncio.to_thread(record_export, report_id, user_obj.get("id") if user_obj else None)
        response.headers["X-Request-ID"] = request_id
        return response
    finally:
        reset_user_context(user_tokens)
        reset_request_id(req_token)


def _parse_token(auth_header: str | None) -> str | None:
    if not auth_header:
        return None
    if auth_header.lower().startswith("bearer "):
        return auth_header.split(" ", 1)[1].strip()
    return auth_header.strip()


def _set_session_cookie(response: Response, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite="lax",
        max_age=30 * 24 * 60 * 60,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")


def get_current_user(
    authorization: str | None = Header(default=None),
    session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> dict:
    token = _parse_token(authorization) or session_token
    if not token:
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    user = _get_session_user_cached(token)
    if not user:
        raise HTTPException(status_code=401, detail="Сессия недействительна")
    return user


def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if str(access.actor(user).get("role") or "user") != "admin":
        raise HTTPException(status_code=403, detail="Требуются права администратора")
    return user


def require_access(user: dict = Depends(get_current_user)) -> dict:
    if not access.has_access(user):
        raise HTTPException(403, {"code": "no_access", "message": "Для работы требуется доступ к активной группе"})
    return user


def require_group_manager(group_id: int, user: dict = Depends(require_access)) -> dict:
    access.require_group_manager(group_id, user)
    return user


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


EXAMPLES_DIR = Path(os.getenv("EXAMPLES_DIR", "/app/examples"))
EXAMPLES_CAP = 5
_ALLOWED_EXAMPLE_EXT = {".xlsx", ".csv"}


def _examples_directory(language: str | None) -> Path:
    return EXAMPLES_DIR / "en" if (language or get_ui_language()) == "en" else EXAMPLES_DIR


def _scan_examples(directory: Path) -> list[dict[str, Any]]:
    if not directory.is_dir():
        return []
    entries: list[dict[str, Any]] = []
    for path in sorted(directory.iterdir(), key=lambda p: p.name):
        if not path.is_file():
            continue
        if path.name.startswith("_") or path.name.startswith("."):
            continue
        if path.suffix.lower() not in _ALLOWED_EXAMPLE_EXT:
            continue
        try:
            size = path.stat().st_size
        except OSError:
            continue
        entries.append({"name": path.name, "size_bytes": int(size)})
        if len(entries) >= EXAMPLES_CAP:
            break
    return entries


@app.get("/api/examples")
def list_examples(language: str | None = None, user: dict = Depends(get_current_user)) -> dict[str, list[dict[str, Any]]]:
    _ = user
    return {"examples": _scan_examples(_examples_directory(language))}


RELEASE_NOTES_PATH = Path(os.getenv("RELEASE_NOTES_PATH", "/app/docs/RELEASE_NOTES.md"))


def _parse_release_notes(raw_md: str) -> list[dict[str, str]]:
    """Режет RELEASE_NOTES.md на секции по заголовкам ## .

    Ожидается структура вида:
        # Заметки о релизах
        ...вступление...
        ---
        ## 2.0.1 — 2026-04-23
        ...контент...
        ---
        ## 2.0.0 — 2026-04-19 (в разработке)
        ...контент...

    Возвращает список `{version, title, content_md}` в порядке появления в файле
    (последняя версия сверху, если файл ведётся хронологически).
    """
    releases: list[dict[str, str]] = []
    lines = raw_md.splitlines()
    current_title: str | None = None
    current_buffer: list[str] = []

    def _flush() -> None:
        if current_title is None:
            return
        content = "\n".join(current_buffer).strip()
        # `---` между секциями ведётся в файле как визуальные разделители — выкидываем.
        content = "\n".join(l for l in content.splitlines() if l.strip() != "---").strip()
        # Версия — первое «слово» в заголовке до ` — ` или пробела.
        head = current_title.strip()
        version_token = head.split("—")[0].strip()
        version = version_token.split()[0] if version_token else head
        releases.append({"version": version, "title": head, "content_md": content})

    for line in lines:
        if line.startswith("## "):
            _flush()
            current_title = line[3:].strip()
            current_buffer = []
        elif current_title is not None:
            current_buffer.append(line)
    _flush()
    return releases


@app.get("/api/release-notes")
def release_notes(language: str = "ru", user: dict = Depends(get_current_user)) -> dict[str, Any]:
    _ = user
    path = RELEASE_NOTES_PATH
    if language == "en" and (english_notes := path.with_name("RELEASE_NOTES_EN.md")).is_file():
        path = english_notes
    if not path.is_file():
        return {"releases": [], "source_missing": True}
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        logger.warning("Failed to read release notes at %s: %s", path, exc)
        return {"releases": [], "source_missing": True}
    return {"releases": _parse_release_notes(raw), "source_missing": False}


@app.get("/api/examples/{name}/download")
def download_example(name: str, language: str | None = None, user: dict = Depends(get_current_user)) -> FileResponse:
    _ = user
    directory = _examples_directory(language)
    allowed = {entry["name"] for entry in _scan_examples(directory)}
    if name not in allowed and language is None:
        directory = EXAMPLES_DIR if directory != EXAMPLES_DIR else EXAMPLES_DIR / "en"
        allowed = {entry["name"] for entry in _scan_examples(directory)}
    if name not in allowed:
        raise HTTPException(status_code=404, detail="Пример не найден")
    path = directory / name
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Файл примера отсутствует")
    suffix = path.suffix.lower()
    media_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if suffix == ".xlsx"
        else "text/csv"
    )
    return FileResponse(path, filename=name, media_type=media_type)


@app.post("/api/auth/register", response_model=AuthResponse)
def register(payload: AuthRequest, response: Response) -> AuthResponse:
    if not LOCAL_REGISTRATION_ENABLED:
        raise HTTPException(status_code=404, detail="Регистрация отключена")
    password_error = validate_password_policy(payload.password)
    if password_error:
        logger.warning(
            "Register failed: username=%s reason=%s",
            payload.username,
            password_error,
            extra={"username": payload.username},
        )
        raise HTTPException(status_code=400, detail=password_error)

    user_id = create_user(payload.username, hash_password(payload.password), join_guest=True)
    if user_id is None:
        raise HTTPException(status_code=400, detail="Пользователь уже существует")
    token = create_session(user_id)
    _cache_session_user(token, {"id": user_id, "username": payload.username, "role": "user"})
    _set_session_cookie(response, token)
    logger.info(
        "Register success: user_id=%s username=%s",
        user_id,
        payload.username,
        extra={"user_id": str(user_id), "username": payload.username},
    )
    return AuthResponse(token=token, username=payload.username, role="user")


@app.post("/api/auth/login", response_model=AuthResponse)
def login(payload: AuthRequest, response: Response) -> AuthResponse:
    if not LOCAL_AUTH_ENABLED:
        raise HTTPException(status_code=404, detail="Вход по паролю отключён")
    user = get_user_by_username(payload.username)
    if not user or not verify_password(payload.password, user["password_hash"] or ""):
        logger.warning(
            "Login failed: username=%s reason=invalid_credentials",
            payload.username,
            extra={"username": payload.username},
        )
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    token = create_session(int(user["id"]))
    _cache_session_user(
        token,
        {"id": int(user["id"]), "username": str(user["username"]), "role": str(user.get("role") or "user")},
    )
    _set_session_cookie(response, token)
    logger.info(
        "Login success: user_id=%s username=%s",
        user["id"],
        user["username"],
        extra={"user_id": str(user["id"]), "username": str(user["username"])},
    )
    if password_needs_rehash(str(user.get("password_hash") or "")):
        try:
            update_user_password(str(user["username"]), hash_password(payload.password))
        except Exception:
            logger.warning("Login rehash skipped: user_id=%s username=%s", user["id"], user["username"])
    return AuthResponse(token=token, username=user["username"], role=str(user.get("role") or "user"))


@app.post("/api/auth/logout")
def logout(
    response: Response,
    authorization: str | None = Header(default=None),
    session_token: str | None = Cookie(default=None, alias=SESSION_COOKIE_NAME),
) -> dict[str, str]:
    token = _parse_token(authorization) or session_token
    user_obj = _get_session_user_cached(token) if token else None
    if token:
        delete_session(token)
        _invalidate_session_user_cache(token)
    _clear_session_cookie(response)
    logger.info(
        "Logout: user_id=%s username=%s",
        user_obj.get("id") if isinstance(user_obj, dict) else "-",
        user_obj.get("username") if isinstance(user_obj, dict) else "-",
    )
    return {"status": "ok"}


@app.get("/api/auth/config")
def auth_config() -> dict[str, bool]:
    return {
        "sso_enabled": sso_is_configured(),
        "local_auth_enabled": LOCAL_AUTH_ENABLED,
        "local_registration_enabled": LOCAL_REGISTRATION_ENABLED,
    }


def _sso_redirect(target: str) -> RedirectResponse:
    return RedirectResponse(
        target, status_code=302, headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer"}
    )


@app.get("/api/auth/sso/login")
def sso_login(
    return_to: str | None = Query(default=None, max_length=2048),
    force: bool = Query(default=False),
) -> RedirectResponse:
    if not sso_is_configured():
        raise HTTPException(status_code=404, detail="SSO не настроено")
    try:
        url, browser_token = sso.sso_build_authorize_url(return_to, force)
    except sso.SSOError as exc:
        logger.warning("OIDC login initialization failed: %s", exc)
        raise HTTPException(status_code=502, detail="Не удалось начать вход через SSO") from exc
    redirect = _sso_redirect(url)
    redirect.set_cookie(
        sso.FLOW_COOKIE_NAME, browser_token, max_age=SSO_STATE_TTL_SEC, path=sso.CALLBACK_PATH,
        secure=SESSION_COOKIE_SECURE or SSO_REDIRECT_URL.startswith("https://"), httponly=True, samesite="lax",
    )
    return redirect


@app.get(sso.CALLBACK_PATH)
def sso_callback(
    code: str = Query(default=""),
    state: str = Query(default=""),
    error: str = Query(default=""),
    browser_token: str = Cookie(default="", alias=sso.FLOW_COOKIE_NAME),
) -> RedirectResponse:
    if not sso_is_configured():
        raise HTTPException(status_code=404, detail="SSO не настроено")
    try:
        flow = sso.sso_consume_state(state, browser_token)
        if error or not code:
            raise sso.SSOError("Провайдер не завершил вход")
        identity = sso.sso_exchange_code(code, flow)
        user = get_or_create_sso_user(
            identity["issuer"], identity["subject"], identity["username"], identity["login_name"], identity["email"]
        )
    except sso.SSOError as exc:
        logger.warning("OIDC callback failed: %s", exc)
        return _sso_redirect("/?sso_error=signin_failed")

    user_profile.sync(user["id"], identity["profile"])
    token = create_session(user["id"])
    _cache_session_user(token, user)
    redirect = _sso_redirect(flow["return_to"])
    redirect.delete_cookie(sso.FLOW_COOKIE_NAME, path=sso.CALLBACK_PATH)
    _set_session_cookie(redirect, token)
    logger.info("OIDC login success: user_id=%s", user["id"])
    return redirect


@app.get("/api/auth/me", response_model=UserMeResponse)
def me(user: dict = Depends(get_current_user)) -> UserMeResponse:
    profile = get_user_by_id(int(user["id"])) or {}
    return UserMeResponse(full_name=profile.get("full_name"), id=int(user["id"]), username=str(user["username"]), role=str(user.get("role") or "user"))


@app.get("/api/auth/usage", response_model=UsageResponse)
def usage(user: dict = Depends(get_current_user)) -> UsageResponse:
    state = access.status(user)
    return UsageResponse(**get_user_usage(int(user["id"])), groups=state["groups"], has_access=state["has_access"])


@app.get("/api/admin/users", response_model=AdminUsersResponse)
def admin_users(admin_user: dict = Depends(require_admin)) -> AdminUsersResponse:
    _ = admin_user
    rows = list_users_admin(limit=200)
    items = [
        AdminUserItem(
            id=int(row["id"]),
            username=str(row.get("username") or ""),
            role=str(row.get("role") or "user"),
            created_at=str(row.get("created_at") or ""),
            reports_count=int(row.get("reports_count") or 0),
            last_login_at=str(row["last_login_at"]) if row.get("last_login_at") else None,
        )
        for row in rows
    ]
    return AdminUsersResponse(users=items)


@app.get("/api/admin/users/{target_user_id}/reports", response_model=ReportsResponse)
def admin_user_reports(
    target_user_id: int,
    limit: int = Query(default=50, ge=1, le=500),
    admin_user: dict = Depends(require_admin),
) -> ReportsResponse:
    _ = admin_user
    target = get_user_by_id(target_user_id)
    if not target:
        raise HTTPException(status_code=404, detail="Пользователь не найден")
    rows = list_reports_by_user(target_user_id, limit=limit)
    items = [_normalize_report_row_light(row) for row in rows]
    return ReportsResponse(reports=_enrich_active_list(items))


@app.get("/api/admin/reports/{report_id}/analysis", response_model=ReportAnalysisResponse)
def admin_report_analysis(report_id: str, admin_user: dict = Depends(require_admin)) -> ReportAnalysisResponse:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")

    summary, preview_rows = build_report_analysis(report_id)
    raw_summary = row.get("summary_json")
    if raw_summary:
        try:
            parsed = json.loads(raw_summary)
            if isinstance(parsed, dict):
                summary = parsed
        except Exception:
            pass

    return ReportAnalysisResponse(
        report_id=report_id,
        status=str(row.get("status") or ""),
        summary=summary,
        preview_rows=preview_rows,
    )


@app.post("/api/admin/reports/{report_id}/pause")
def admin_pause_report(report_id: str, admin_user: dict = Depends(require_admin)) -> dict[str, str]:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status in {JobStatus.completed.value, JobStatus.failed.value, JobStatus.canceled.value}:
        return {"status": status}
    if status == JobStatus.paused.value:
        return {"status": JobStatus.paused.value}
    update_report_status(report_id=report_id, status=JobStatus.paused.value)
    logger.info(
        "Admin pause requested: admin_id=%s admin_username=%s report_id=%s",
        admin_user.get("id"),
        admin_user.get("username"),
        report_id,
    )
    return {"status": JobStatus.paused.value}


@app.post("/api/admin/reports/{report_id}/resume")
def admin_resume_report(report_id: str, admin_user: dict = Depends(require_admin)) -> dict[str, str]:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    access.check_report_limit(row)
    status = str(row.get("status") or "")
    if status in {JobStatus.completed.value, JobStatus.failed.value, JobStatus.canceled.value}:
        return {"status": status}
    job_id = str(row.get("job_id") or "")
    if status == JobStatus.running.value:
        if job_id and has_running_lease(job_id):
            return {"status": JobStatus.running.value}
        # Running without lease: treat as orphan and move back to queued for retry.
        status = JobStatus.queued.value
        update_report_status(report_id=report_id, status=JobStatus.queued.value)
        update_report_progress(
            report_id=report_id,
            total_rows=int(row.get("total_rows") or 0),
            processed_rows=int(row.get("processed_rows") or 0),
            progress_percent=float(row.get("progress_percent") or 0.0),
            eta_seconds=row.get("eta_seconds"),
            current_step="В очереди",
        )
    if status == JobStatus.queued.value:
        # Healthy queued/running path.
        if (job_id and has_queued_marker(job_id)) or (job_id and has_running_lease(job_id)):
            return {"status": JobStatus.queued.value}
        # Orphaned queued: rebuild payload and enqueue again.
        payload, err = build_job_payload_from_report(row)
        if err:
            update_report_status(
                report_id=report_id,
                status=JobStatus.failed.value,
                finished_at=datetime.utcnow().isoformat(),
                error_text=err,
            )
            return {"status": JobStatus.failed.value}
        if enqueue_job(payload):
            update_report_status(report_id=report_id, status=JobStatus.queued.value)
            update_report_progress(
                report_id=report_id,
                total_rows=int(row.get("total_rows") or 0),
                processed_rows=int(row.get("processed_rows") or 0),
                progress_percent=float(row.get("progress_percent") or 0.0),
                eta_seconds=row.get("eta_seconds"),
                current_step="В очереди",
            )
            logger.warning(
                "Admin resume repaired orphaned queued job: admin_id=%s admin_username=%s report_id=%s job_id=%s",
                admin_user.get("id"),
                admin_user.get("username"),
                report_id,
                job_id,
            )
            return {"status": JobStatus.queued.value}
        if job_id and has_running_lease(job_id):
            update_report_status(report_id=report_id, status=JobStatus.running.value)
            return {"status": JobStatus.running.value}
        return {"status": JobStatus.queued.value}
    if status != JobStatus.paused.value:
        raise HTTPException(status_code=409, detail=f"Нельзя возобновить задачу со статусом {status}")
    if has_running_lease(job_id):
        raise HTTPException(409, "Завершаются уже отправленные запросы. Повторите возобновление после сохранения результатов.")

    payload, err = build_job_payload_from_report(row)
    if err:
        update_report_status(
            report_id=report_id,
            status=JobStatus.failed.value,
            finished_at=datetime.utcnow().isoformat(),
            error_text=err,
        )
        return {"status": JobStatus.failed.value}

    owner_user_id = int(row["user_id"])
    owner_slot_free = count_active_slots(owner_user_id) < MAX_ACTIVE_PER_USER
    if not queue_paused_report(report_id, owner_user_id, personal_queue=not owner_slot_free):
        raise HTTPException(409, "Состояние отчёта изменилось. Обновите список.")
    if not owner_slot_free:
        return {"status": JobStatus.queued.value}
    clear_job_dedup_marker(job_id)
    queued = enqueue_job(payload)
    if not queued and not has_queued_marker(job_id):
        raise HTTPException(503, "Не удалось поставить отчёт в очередь. Повторите позже.")
    return {"status": JobStatus.queued.value}


@app.post("/api/admin/reports/{report_id}/cancel")
def admin_cancel_report(report_id: str, admin_user: dict = Depends(require_admin)) -> dict[str, str]:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status in {JobStatus.completed.value, JobStatus.failed.value, JobStatus.canceled.value}:
        return {"status": status}
    update_report_status(
        report_id=report_id,
        status=JobStatus.canceled.value,
        finished_at=datetime.utcnow().isoformat(),
        error_text="Отменено администратором",
    )
    job_id = str(row.get("job_id") or "").strip()
    if job_id:
        try:
            remove_job_from_queue(job_id)
        except Exception as exc:
            logger.warning("Failed to remove cancelled job from queue: job_id=%s error=%s", job_id, exc)
    logger.info(
        "Admin cancel requested: admin_id=%s admin_username=%s report_id=%s",
        admin_user.get("id"),
        admin_user.get("username"),
        report_id,
    )
    return {"status": "cancel_requested"}


@app.delete("/api/admin/reports/{report_id}")
def admin_delete_report(report_id: str, admin_user: dict = Depends(require_admin)) -> dict[str, str]:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status in {JobStatus.running.value, JobStatus.queued.value, JobStatus.paused.value}:
        raise HTTPException(status_code=400, detail="Нельзя удалить активный отчет. Сначала нажмите Отмена.")
    target_user_id = int(row.get("user_id") or 0)
    if target_user_id <= 0:
        raise HTTPException(status_code=400, detail="Некорректный owner отчета")
    if not delete_report(report_id, target_user_id, deleted_by=int(admin_user["id"]), reason="admin"):
        raise HTTPException(status_code=409, detail="Завершаются запросы обработки отчёта. Повторите удаление позже.")
    logger.info(
        "Admin report deleted: admin_id=%s admin_username=%s report_id=%s target_user_id=%s",
        admin_user.get("id"),
        admin_user.get("username"),
        report_id,
        target_user_id,
    )
    return {"status": "deleted"}


@app.get("/api/admin/reports/{report_id}/download/xlsx")
def admin_download_report_xlsx(
    report_id: str,
    filename: str | None = Query(default=None),
    admin_user: dict = Depends(require_admin),
) -> FileResponse:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    results_file = row.get("results_file")
    if not results_file:
        raise HTTPException(status_code=404, detail="Файл результатов еще не готов")
    path = RESULTS_DIR / str(results_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Файл результатов не найден на диске")
    download_name = _sanitize_download_filename(filename, fallback=path.stem, extension="xlsx")
    return FileResponse(path, filename=download_name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/admin/reports/{report_id}/download/raw")
def admin_download_report_raw(
    report_id: str,
    filename: str | None = Query(default=None),
    admin_user: dict = Depends(require_admin),
) -> FileResponse:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    raw_file = row.get("raw_file")
    if not raw_file:
        raise HTTPException(status_code=404, detail="Сырой файл еще не готов")
    path = RESULTS_DIR / str(raw_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Сырой файл не найден на диске")
    download_name = _sanitize_download_filename(filename, fallback=path.stem, extension="json")
    return FileResponse(path, filename=download_name, media_type="application/json")


@app.get("/api/admin/reports/{report_id}/download/source")
def admin_download_report_source(
    report_id: str,
    filename: str | None = Query(default=None),
    admin_user: dict = Depends(require_admin),
) -> FileResponse:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    uploaded_file_id = str(row.get("uploaded_file_id") or "")
    user_id = int(row.get("user_id") or 0)
    if not uploaded_file_id or user_id <= 0:
        raise HTTPException(status_code=404, detail="Исходный файл не привязан к отчету")
    source = get_uploaded_file(uploaded_file_id, user_id)
    if not source:
        raise HTTPException(status_code=404, detail="Исходный файл не найден")
    source_path = Path(str(source.get("path") or ""))
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="Исходный файл отсутствует на диске")
    original_name = str(source.get("original_name") or source_path.name)
    original_stem = Path(original_name).stem or "source"
    download_name = _sanitize_download_filename(filename, fallback=original_stem, extension="xlsx")
    return FileResponse(
        source_path,
        filename=download_name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/api/admin/reports/{report_id}/download/partial/xlsx")
def admin_download_report_partial_xlsx(
    report_id: str,
    background: BackgroundTasks,
    filename: str | None = Query(default=None),
    admin_user: dict = Depends(require_admin),
) -> FileResponse:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status not in {JobStatus.running.value, JobStatus.paused.value, JobStatus.queued.value}:
        raise HTTPException(status_code=409, detail="Промежуточная выгрузка доступна только для активных отчётов")
    return _partial_download(row, "xlsx", background, filename)


@app.get("/api/admin/reports/{report_id}/download/partial/raw")
def admin_download_report_partial_raw(
    report_id: str,
    background: BackgroundTasks,
    filename: str | None = Query(default=None),
    admin_user: dict = Depends(require_admin),
) -> FileResponse:
    _ = admin_user
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status not in {JobStatus.running.value, JobStatus.paused.value, JobStatus.queued.value}:
        raise HTTPException(status_code=409, detail="Промежуточная выгрузка доступна только для активных отчётов")
    return _partial_download(row, "json", background, filename)


@app.get("/api/admin/stats", response_model=AdminStatsResponse)
def admin_stats(admin_user: dict = Depends(require_admin)) -> AdminStatsResponse:
    _ = admin_user
    runtime = admin_runtime_stats()
    failures_raw = list_recent_report_failures(limit=30)
    failures = [
        AdminFailureItem(
            report_id=str(item.get("report_id") or ""),
            job_id=str(item.get("job_id") or ""),
            user_id=int(item.get("user_id") or 0),
            username=str(item.get("username") or ""),
            updated_at=str(item["updated_at"]) if item.get("updated_at") else None,
            error_text=str(item["error_text"]) if item.get("error_text") else None,
        )
        for item in failures_raw
    ]
    queue_depth = 0
    client = get_redis_client()
    if client is not None:
        try:
            queue_depth = int(client.llen(os.getenv("REDIS_QUEUE_KEY", "review_analyzer:jobs")) or 0)
        except Exception:
            queue_depth = 0
    presence = {'analysis':0,'inspect':0}
    if client is not None:
        try:
            for key in client.scan_iter(match='review_analyzer:presence:*',count=100):
                mode=client.get(key)
                if isinstance(mode,bytes):mode=mode.decode()
                if mode in presence:presence[mode]+=1
        except Exception:
            pass
    return AdminStatsResponse(
        analysis_workers=presence['analysis'],inspect_workers=presence['inspect'],
        queue_depth=queue_depth,
        queued=int(runtime.get("queued") or 0),
        personal_queued=int(runtime.get("personal_queued") or 0),
        running=int(runtime.get("running") or 0),
        paused=int(runtime.get("paused") or 0),
        failed=int(runtime.get("failed") or 0),
        recent_failures=failures,
    )


@app.get("/api/admin/logs", response_model=AdminLogsResponse)
def admin_logs(
    service: str = Query(default="all"),
    limit: int = Query(default=200, ge=1, le=1000),
    level: str | None = Query(default=None),
    q: str | None = Query(default=None),
    admin_user: dict = Depends(require_admin),
) -> AdminLogsResponse:
    _ = admin_user
    lines = _read_admin_logs(service=service, limit=limit, level=level, query=q)
    return AdminLogsResponse(service=service, lines=lines)


@app.get("/api/default-prompt")
def default_prompt() -> dict[str, str | int]:
    return {
        "prompt_template": DEFAULT_PROMPT,
        "parallelism_max": max(1, GLOBAL_LLM_PARALLELISM),
    }


@app.get("/api/presets", response_model=PresetsResponse)
def list_presets(user: dict = Depends(get_current_user)) -> PresetsResponse:
    rows = list_user_presets(int(user["id"]), limit=100)
    items: list[PresetItem] = []
    for row in rows:
        expected_json_template: dict[str, Any] = {}
        raw_expected = row.get("expected_json_template_json")
        if raw_expected:
            try:
                parsed = json.loads(raw_expected)
                if isinstance(parsed, dict):
                    expected_json_template = parsed
            except Exception:
                expected_json_template = {}
        items.append(
            PresetItem(
                id=str(row["id"]),
                name=str(row["name"]),
                prompt_template=str(row.get("prompt_template") or ""),
                expected_json_template=expected_json_template,
                template_hint=row.get("template_hint"),
                created_at=str(row.get("created_at") or ""),
                updated_at=str(row.get("updated_at") or ""),
            )
        )
    return PresetsResponse(presets=items)


@app.post("/api/presets", response_model=PresetItem)
def save_preset(payload: PresetUpsertRequest, user: dict = Depends(get_current_user)) -> PresetItem:
    if not isinstance(payload.expected_json_template, dict) or not payload.expected_json_template:
        raise HTTPException(status_code=400, detail="expected_json_template должен быть непустым JSON-объектом")
    try:
        _validate_expected_json_template(payload.expected_json_template)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    row = upsert_user_preset(
        user_id=int(user["id"]),
        name=payload.name.strip(),
        prompt_template=payload.prompt_template,
        expected_json_template=payload.expected_json_template,
        template_hint=payload.template_hint,
    )
    expected = payload.expected_json_template
    raw_expected = row.get("expected_json_template_json")
    if raw_expected:
        try:
            parsed = json.loads(raw_expected)
            if isinstance(parsed, dict):
                expected = parsed
        except Exception:
            expected = payload.expected_json_template
    return PresetItem(
        id=str(row["id"]),
        name=str(row["name"]),
        prompt_template=str(row.get("prompt_template") or ""),
        expected_json_template=expected,
        template_hint=row.get("template_hint"),
        created_at=str(row.get("created_at") or ""),
        updated_at=str(row.get("updated_at") or ""),
    )


@app.delete("/api/presets/{preset_id}")
def remove_preset(preset_id: str, user: dict = Depends(get_current_user)) -> dict[str, str]:
    if not delete_user_preset(preset_id, int(user["id"])):
        raise HTTPException(status_code=404, detail="Пресет не найден")
    return {"status": "deleted"}


@app.get("/api/providers", response_model=ProvidersResponse)
def providers() -> ProvidersResponse:
    from app.model_catalog import all_models
    enabled = [r for r in all_models() if r['is_enabled']]
    configured = {r['provider'] for r in enabled}
    items = [ProviderInfo(id=key,label=PROVIDER_CONFIG.get(key,{}).get('label',key)) for key in sorted(configured)]
    primary = next((r['provider'] for r in enabled if r['is_default']), None)
    items.sort(key=lambda item: item.id != primary)
    logger.info("Providers listed: count=%s", len(items))
    return ProvidersResponse(providers=items)


@app.get("/api/models", response_model=ModelsResponse)
def models(provider: str = Query(...)) -> ModelsResponse:
    from app.model_catalog import public_models
    if provider not in {'openai','ollama'}:
        raise HTTPException(404, "Провайдер не найден")
    entries = public_models(provider)
    return ModelsResponse(provider=provider, models=[r['model'] for r in entries], entries=entries)



@app.post("/api/providers/verify-token", response_model=VerifyTokenResponse)
async def verify_provider_token(payload: VerifyTokenRequest, user: dict = Depends(get_current_user)) -> VerifyTokenResponse:
    provider_id = (payload.provider or "").strip()
    logger.info("Verify token requested: provider=%s", provider_id)
    if provider_id not in PROVIDER_CONFIG:
        logger.warning("Verify token failed: provider=%s reason=provider_not_found", provider_id)
        raise HTTPException(status_code=404, detail="Провайдер не найден")

    if provider_id == "ollama":
        base_url = get_provider_base_url("ollama", "http://localhost:11434")
        url = f"{base_url.rstrip('/')}/api/tags"
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url)
            if resp.status_code != 200:
                logger.warning("Verify token failed: provider=%s status=%s", provider_id, resp.status_code)
                return VerifyTokenResponse(
                    ok=False,
                    provider=provider_id,
                    status_code=int(resp.status_code),
                    message=f"Ollama вернул {resp.status_code}",
                    models=[],
                )
            data = resp.json()
            models_raw = data.get("models") if isinstance(data, dict) else None
            models_out: list[str] = []
            if isinstance(models_raw, list):
                for item in models_raw:
                    if isinstance(item, dict):
                        name = item.get("name")
                        if isinstance(name, str) and name.strip():
                            models_out.append(name.strip())
            logger.info("Verify token success: provider=%s models=%s", provider_id, len(models_out))
            return VerifyTokenResponse(ok=True, provider=provider_id, status_code=200, models=models_out[:200])
        except Exception as exc:
            err = str(exc).strip()
            detail = f": {err}" if err else ""
            logger.warning("Verify token failed: provider=%s error=%s", provider_id, type(exc).__name__)
            return VerifyTokenResponse(
                ok=False,
                provider=provider_id,
                status_code=0,
                message=f"Ошибка запроса к Ollama: {type(exc).__name__}{detail}",
                models=[],
            )

    if provider_id == "openai":
        api_key = normalize_api_key(payload.api_key) or normalize_api_key(os.getenv("OPENAI_API_KEY", ""))
        if not api_key:
            logger.warning("Verify token failed: provider=%s reason=missing_api_key", provider_id)
            return VerifyTokenResponse(
                ok=False,
                provider=provider_id,
                status_code=0,
                message="Не задан API-ключ (введите в UI или установите OPENAI_API_KEY)",
                models=[],
            )
        base_url = get_provider_base_url("openai", "https://api.openai.com")
        url = f"{_v1_base(base_url)}/models"
        headers = {"Authorization": f"Bearer {api_key}"}
        try:
            async with httpx.AsyncClient(timeout=15, verify=TLS_VERIFY) as client:
                resp = await client.get(url, headers=headers)
        except Exception as exc:
            err = str(exc).strip()
            detail = f": {err}" if err else ""
            logger.warning("Verify token failed: provider=%s error=%s", provider_id, type(exc).__name__)
            return VerifyTokenResponse(
                ok=False,
                provider=provider_id,
                status_code=0,
                message=f"Ошибка запроса: {type(exc).__name__}{detail}",
                models=[],
            )

        if resp.status_code != 200:
            msg: str | None = None
            try:
                data = resp.json()
                if isinstance(data, dict) and isinstance(data.get("error"), dict):
                    err = data["error"]
                    msg = str(err.get("message") or "") or None
            except Exception:
                msg = None
            logger.warning("Verify token failed: provider=%s status=%s", provider_id, resp.status_code)
            return VerifyTokenResponse(
                ok=False,
                provider=provider_id,
                status_code=int(resp.status_code),
                message=msg or f"HTTP {resp.status_code}",
                models=[],
            )

        models_out: list[str] = []
        try:
            data = resp.json()
            items = data.get("data") if isinstance(data, dict) else None
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        mid = item.get("id")
                        if isinstance(mid, str) and mid.strip():
                            models_out.append(mid.strip())
        except Exception:
            models_out = []

        logger.info("Verify token success: provider=%s models=%s", provider_id, len(models_out))
        return VerifyTokenResponse(ok=True, provider=provider_id, status_code=200, models=models_out[:200])

    return VerifyTokenResponse(
        ok=False,
        provider=provider_id,
        status_code=0,
        message="Проверка токена не реализована для этого провайдера",
        models=[],
    )


@app.post("/api/file/inspect", response_model=FileInspectResponse)
async def file_inspect(file: UploadFile = File(...), user: dict = Depends(get_current_user)) -> FileInspectResponse:
    if not file.filename or Path(file.filename).suffix.lower() not in {".xlsx", ".csv"}:
        logger.warning("File inspect failed: filename=%s reason=unsupported_extension", file.filename)
        raise HTTPException(status_code=400, detail="Поддерживаются файлы XLSX и CSV (UTF-8 или Windows-1251)")

    logger.info("File inspect started: filename=%s", file.filename)
    file_id = str(uuid.uuid4())
    dest = UPLOADS_DIR / f"{file_id}{Path(file.filename).suffix.lower()}"
    written = 0
    chunk_size = 1024 * 1024
    try:
        with dest.open("wb") as out:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    logger.warning(
                        "File inspect failed: filename=%s size_bytes=%s reason=max_upload_exceeded",
                        file.filename,
                        written,
                    )
                    raise HTTPException(
                        status_code=413,
                        detail=f"Файл слишком большой: максимум {MAX_UPLOAD_SIZE_MB} MB",
                    )
                out.write(chunk)
    except HTTPException:
        dest.unlink(missing_ok=True)
        raise
    finally:
        await file.close()

    user_id = int(user["id"])

    await asyncio.to_thread(
        add_uploaded_file,
        file_id,
        user_id,
        file.filename,
        str(dest),
        inspect_status="queued",
        sheets=[],
        suggested_sheet=None,
        suggested_column=None,
        inspect_error_text=None,
    )
    queued = enqueue_inspect_job(
        {
            "kind": "file_inspect",
            "job_id": f"file-inspect:{file_id}",
            "file_id": file_id,
            "user_id": user_id,
        }
    )
    if not queued:
        await asyncio.to_thread(
            update_uploaded_file_inspect,
            file_id,
            user_id,
            inspect_status="error",
            inspect_error_text="Не удалось поставить файл в очередь на обработку",
        )
        raise HTTPException(status_code=503, detail="Не удалось поставить файл в очередь. Повторите позже.")

    logger.info("File inspect queued: file_id=%s filename=%s bytes=%s", file_id, file.filename, written)
    return FileInspectResponse(
        file_id=file_id,
        filename=file.filename,
        sheets=[],
        suggested_sheet=None,
        suggested_column=None,
        inspect_status="queued",
        inspect_error_text=None,
    )


@app.get("/api/file/{file_id}/inspect", response_model=FileInspectResponse)
async def file_inspect_status(file_id: str, user: dict = Depends(get_current_user)) -> FileInspectResponse:
    row = await asyncio.to_thread(get_uploaded_file, file_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Загруженный файл не найден")
    return _to_file_inspect_response(row)


@app.post("/api/jobs")
async def start_job(payload: StartJobRequest, user: dict = Depends(get_current_user)) -> dict[str, str]:
    from app.model_catalog import require_enabled
    await asyncio.to_thread(require_enabled, payload.provider, payload.model)
    access_group_id = await asyncio.to_thread(access.pick_group, user, payload.access_group_id)
    await asyncio.to_thread(access.check_limit, int(user["id"]), access_group_id)
    analysis_columns = [str(col).strip() for col in payload.analysis_columns if str(col).strip()]
    non_analysis_columns = [str(col).strip() for col in (payload.non_analysis_columns or []) if str(col).strip()]
    user_id = int(user["id"])
    logger.info(
        "Job start requested: user_id=%s username=%s provider=%s model=%s file_id=%s sheet=%s analysis_columns=%s max_reviews=%s parallelism=%s",
        user_id,
        user.get("username"),
        payload.provider,
        payload.model,
        payload.file_id,
        payload.sheet_name,
        analysis_columns,
        payload.max_reviews,
        payload.parallelism,
    )
    file_meta = await asyncio.to_thread(get_uploaded_file, payload.file_id, user_id)
    if not file_meta:
        logger.warning("Job start failed: reason=file_not_found file_id=%s", payload.file_id)
        raise HTTPException(status_code=404, detail="Загруженный файл не найден")
    inspect_status = str(file_meta.get("inspect_status") or "ready")
    if inspect_status != "ready":
        if inspect_status == "error":
            detail = str(file_meta.get("inspect_error_text") or "Ошибка обработки файла")
            raise HTTPException(status_code=400, detail=f"Файл не готов: {detail}")
        raise HTTPException(status_code=409, detail="Файл еще подготавливается, дождитесь завершения проверки")

    if payload.analysis_mode == "custom":
        if payload.expected_json_template is None:
            raise HTTPException(status_code=400, detail="Для custom режима заполните Ожидаемый JSON")
        if not isinstance(payload.expected_json_template, dict):
            raise HTTPException(status_code=400, detail="expected_json_template должен быть JSON-объектом")
        try:
            _validate_expected_json_template(payload.expected_json_template)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        effective_output_schema = _build_output_schema_from_expected_json_template(payload.expected_json_template)
    else:
        effective_output_schema = payload.output_schema

    if not analysis_columns:
        logger.warning("Job start failed: reason=no_input_columns")
        raise HTTPException(status_code=400, detail="Нужно выбрать хотя бы одну колонку для анализа")
    group_by_column = (payload.group_by_column or "").strip() or None
    if group_by_column and group_by_column not in analysis_columns:
        logger.warning("Job start failed: reason=group_column_not_in_input group_by_column=%s", group_by_column)
        raise HTTPException(status_code=400, detail="Колонка группировки должна входить в колонки для анализа")
    if group_by_column:
        non_analysis_columns = list(dict.fromkeys([group_by_column, *non_analysis_columns]))
    group_max_rows = GROUP_MAX_ROWS

    try:
        warnings = settings_warnings(payload.expected_json_template, file_meta, payload.sheet_name, analysis_columns, non_analysis_columns)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    if warnings and not payload.settings_confirmed:
        raise HTTPException(409, {"code": "settings_warning", "message": "Поля ответа совпадают с колонками, которые не передаются в обработку или результат.", "fields": warnings})

    effective_api_key = ""
    if payload.provider == "openai":
        effective_api_key = normalize_api_key(payload.api_key) or normalize_api_key(os.getenv("OPENAI_API_KEY", ""))
    if payload.provider == "openai" and not effective_api_key:
        logger.warning("Job start failed: reason=missing_openai_api_key")
        raise HTTPException(status_code=400, detail="Для OpenAI требуется API-токен (в UI или OPENAI_API_KEY)")

    persisted_api_key_encrypted: str | None = None
    queued_api_key_encrypted: str | None = None
    if payload.api_key:
        queued_api_key_encrypted = encrypt_text(payload.api_key)
        if payload.save_api_key_for_resume:
            persisted_api_key_encrypted = queued_api_key_encrypted

    prompt_language = payload.prompt_language or await asyncio.to_thread(get_ui_language)
    job_id = str(uuid.uuid4())
    report_id = job_id

    # Per-user concurrency limit: если у юзера уже MAX_ACTIVE_PER_USER занятых
    # слотов (running + queued в общей очереди, paused НЕ считается),
    # отчёт идёт в личную очередь — status='queued', personal_queue=TRUE,
    # в Redis НЕ пушим. Промоут в общую — автоматически на освобождении слота
    # (см. update_report_status → try_promote_after_slot_freed).
    slot_free = (await asyncio.to_thread(count_active_slots, user_id)) < MAX_ACTIVE_PER_USER

    await asyncio.to_thread(
        create_report,
        report_id=report_id,
        user_id=user_id,
        job_id=job_id,
        status=JobStatus.queued.value,
        provider=payload.provider,
        model=payload.model,
        sheet_name=payload.sheet_name,
        max_reviews=payload.max_reviews,
        parallelism=payload.parallelism,
        temperature=payload.temperature,
        uploaded_file_id=payload.file_id,
        prompt_template=payload.prompt_template,
        prompt_language=prompt_language,
        include_raw_json=payload.include_raw_json,
        api_key_encrypted=persisted_api_key_encrypted,
        analysis_mode=payload.analysis_mode,
        output_schema=effective_output_schema,
        expected_json_template=payload.expected_json_template,
        input_columns=analysis_columns,
        non_analysis_columns=non_analysis_columns,
        group_by_column=group_by_column,
        group_max_rows=group_max_rows,
        use_cache=payload.use_cache,
        personal_queue=not slot_free,
        access_group_id=access_group_id,
        name=(payload.name or "").strip() or Path(str(file_meta.get("original_name") or "Отчёт")).stem[:200],
    )

    if slot_free:
        enqueue_job(
            {
                "job_id": job_id,
                "report_id": report_id,
                "user_id": user_id,
                "file_id": payload.file_id,
                "provider": payload.provider,
                "model": payload.model,
                "prompt_template": payload.prompt_template,
                "prompt_language": prompt_language,
                "sheet_name": payload.sheet_name,
                "analysis_columns": analysis_columns,
                "non_analysis_columns": non_analysis_columns,
                "analysis_mode": payload.analysis_mode,
                "output_schema": effective_output_schema,
                "expected_json_template": payload.expected_json_template,
                "group_by_column": group_by_column,
                "group_max_rows": group_max_rows,
                "max_reviews": payload.max_reviews,
                "parallelism": payload.parallelism,
                "temperature": payload.temperature,
                "include_raw_json": payload.include_raw_json,
                "use_cache": payload.use_cache,
                "api_key_encrypted": queued_api_key_encrypted,
            }
        )
    logger.info(
        "Job started: user_id=%s username=%s report_id=%s provider=%s model=%s max_reviews=%s parallelism=%s temperature=%s personal_queue=%s",
        int(user["id"]),
        user.get("username"),
        report_id,
        payload.provider,
        payload.model,
        payload.max_reviews,
        payload.parallelism,
        payload.temperature,
        not slot_free,
    )
    return {"job_id": job_id, "report_id": report_id}


def _require_job_owner(job_id: str, user_id: int):
    row = get_report_by_job_id(job_id, user_id)
    if not row:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    return row


async def _require_job_owner_async(job_id: str, user_id: int):
    return await asyncio.to_thread(_require_job_owner, job_id, user_id)


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, user: dict = Depends(get_current_user)) -> dict[str, str]:
    row = await _require_job_owner_async(job_id, int(user["id"]))
    status = str(row.get("status") or "")
    if status in {JobStatus.completed.value, JobStatus.failed.value, JobStatus.canceled.value}:
        return {"status": status}
    await asyncio.to_thread(
        update_report_status,
        report_id=str(row["id"]),
        status=JobStatus.canceled.value,
        finished_at=datetime.utcnow().isoformat(),
        error_text="Отменено пользователем",
    )
    # Убираем payload из Redis-очереди: статус уже canceled в БД, держать
    # его в LIST'е бессмысленно — соседние queued-задачи видят эту запись
    # в своих queue_position и показывают лишнюю позицию (IDEA-07 follow-up).
    try:
        await asyncio.to_thread(remove_job_from_queue, job_id)
    except Exception as exc:
        logger.warning("Failed to remove cancelled job from queue: job_id=%s error=%s", job_id, exc)
    logger.info(
        "User cancel requested: user_id=%s username=%s report_id=%s job_id=%s",
        user.get("id"),
        user.get("username"),
        row.get("id"),
        job_id,
    )
    return {"status": "cancel_requested"}


@app.post("/api/jobs/{job_id}/pause")
async def pause_job(job_id: str, user: dict = Depends(get_current_user)) -> dict[str, str]:
    row = await _require_job_owner_async(job_id, int(user["id"]))
    status = str(row.get("status") or "")
    if status in {JobStatus.completed.value, JobStatus.failed.value, JobStatus.canceled.value}:
        return {"status": status}
    if status == JobStatus.paused.value:
        return {"status": JobStatus.paused.value}
    await asyncio.to_thread(update_report_status, report_id=str(row["id"]), status=JobStatus.paused.value)
    logger.info(
        "User pause requested: user_id=%s username=%s report_id=%s job_id=%s",
        user.get("id"),
        user.get("username"),
        row.get("id"),
        job_id,
    )
    return {"status": JobStatus.paused.value}


@app.post("/api/jobs/{job_id}/resume")
async def resume_job(job_id: str, user: dict = Depends(get_current_user)) -> dict[str, str]:
    row = await _require_job_owner_async(job_id, int(user["id"]))
    if row.get('status') in {'queued','paused'}:
        from app.model_catalog import require_enabled
        await asyncio.to_thread(require_enabled, row['provider'], row['model'], resume=True)
    await asyncio.to_thread(access.check_report_limit, row)
    status = str(row.get("status") or "")
    if status in {JobStatus.completed.value, JobStatus.failed.value, JobStatus.canceled.value}:
        return {"status": status}
    if status == JobStatus.running.value:
        if has_running_lease(job_id):
            return {"status": JobStatus.running.value}
        # Running without lease: treat as orphan and move back to queued for retry.
        status = JobStatus.queued.value
        await asyncio.to_thread(update_report_status, report_id=str(row["id"]), status=JobStatus.queued.value)
        await asyncio.to_thread(
            update_report_progress,
            report_id=str(row["id"]),
            total_rows=int(row.get("total_rows") or 0),
            processed_rows=int(row.get("processed_rows") or 0),
            progress_percent=float(row.get("progress_percent") or 0.0),
            eta_seconds=row.get("eta_seconds"),
            current_step="В очереди",
        )
    if status == JobStatus.queued.value:
        # Healthy queued/running path.
        if has_queued_marker(job_id) or has_running_lease(job_id):
            return {"status": JobStatus.queued.value}
        # Orphaned queued: rebuild payload and enqueue again.
        payload, err = build_job_payload_from_report(row)
        if err:
            await asyncio.to_thread(
                update_report_status,
                report_id=str(row["id"]),
                status=JobStatus.failed.value,
                finished_at=datetime.utcnow().isoformat(),
                error_text=err,
            )
            return {"status": JobStatus.failed.value}
        if enqueue_job(payload):
            await asyncio.to_thread(update_report_status, report_id=str(row["id"]), status=JobStatus.queued.value)
            await asyncio.to_thread(
                update_report_progress,
                report_id=str(row["id"]),
                total_rows=int(row.get("total_rows") or 0),
                processed_rows=int(row.get("processed_rows") or 0),
                progress_percent=float(row.get("progress_percent") or 0.0),
                eta_seconds=row.get("eta_seconds"),
                current_step="В очереди",
            )
            logger.warning(
                "User resume repaired orphaned queued job: user_id=%s username=%s report_id=%s job_id=%s",
                user.get("id"),
                user.get("username"),
                row.get("id"),
                job_id,
            )
            return {"status": JobStatus.queued.value}
        if has_running_lease(job_id):
            await asyncio.to_thread(update_report_status, report_id=str(row["id"]), status=JobStatus.running.value)
            return {"status": JobStatus.running.value}
        return {"status": JobStatus.queued.value}
    if status != JobStatus.paused.value:
        raise HTTPException(status_code=409, detail=f"Нельзя возобновить задачу со статусом {status}")
    if await asyncio.to_thread(has_running_lease, job_id):
        raise HTTPException(409, "Завершаются уже отправленные запросы. Повторите возобновление после сохранения результатов.")

    payload, err = build_job_payload_from_report(row)
    if err:
        await asyncio.to_thread(
            update_report_status,
            report_id=str(row["id"]),
            status=JobStatus.failed.value,
            finished_at=datetime.utcnow().isoformat(),
            error_text=err,
        )
        return {"status": JobStatus.failed.value}

    owner_user_id = int(row["user_id"])
    slot_free = (await asyncio.to_thread(count_active_slots, owner_user_id)) < MAX_ACTIVE_PER_USER
    claimed = await asyncio.to_thread(queue_paused_report, str(row["id"]), owner_user_id, personal_queue=not slot_free)
    if not claimed:
        raise HTTPException(409, "Состояние отчёта изменилось. Обновите список.")
    if not slot_free:
        return {"status": JobStatus.queued.value}
    await asyncio.to_thread(clear_job_dedup_marker, job_id)
    queued = await asyncio.to_thread(enqueue_job, payload)
    if not queued and not await asyncio.to_thread(has_queued_marker, job_id):
        raise HTTPException(503, "Не удалось поставить отчёт в очередь. Повторите позже.")
    return {"status": JobStatus.queued.value}


@app.post("/api/jobs/{job_id}/retry")
async def retry_job(job_id: str, user: dict = Depends(get_current_user)) -> dict[str, str]:
    """Перезапуск упавшего/отменённого отчёта с сохранением прогресса.

    Пересобирает payload из записи в БД, принудительно включает кэш
    (чтобы готовые строки не повторно потратили токены) и ставит в
    очередь. Уже обработанные строки пропускаются на уровне БД.
    """
    row = await _require_job_owner_async(job_id, int(user["id"]))
    if row.get('status') in {'queued','paused'}:
        from app.model_catalog import require_enabled
        await asyncio.to_thread(require_enabled, row['provider'], row['model'], resume=True)
    await asyncio.to_thread(access.check_report_limit, row)
    status = str(row.get("status") or "")
    if status == JobStatus.completed.value:
        raise HTTPException(status_code=409, detail="Отчёт уже завершён — перезапуск не нужен")
    if status in {JobStatus.running.value, JobStatus.queued.value, JobStatus.paused.value}:
        raise HTTPException(status_code=409, detail="Отчёт ещё активен — используйте Продолжить/Отмену")

    payload, err = build_job_payload_from_report(row)
    if err:
        raise HTTPException(status_code=400, detail=f"Не удалось пересобрать задачу: {err}")
    # Принудительно включаем кэш на retry — защита от повторного расхода токенов
    # на уже обработанные строки, даже если исходный запуск был с use_cache=false.
    payload["use_cache"] = True

    # Снимаем dedup-маркер на случай краш-сценария (воркер упал между dequeue и
    # delete маркера → маркер сирота → enqueue вернёт False до истечения TTL).
    # На failed/canceled маркер в норме уже снят, лишний DELETE безвреден.
    await asyncio.to_thread(clear_job_dedup_marker, job_id)

    # Per-user concurrency limit: terminal→queued это новое занятие slot'а.
    # Если у юзера уже >=MAX_ACTIVE_PER_USER занятых — кладём в личную очередь,
    # в Redis НЕ пушим. Промоут произойдёт автоматом при освобождении slot'а.
    user_id = int(user["id"])
    slot_free = (await asyncio.to_thread(count_active_slots, user_id)) < MAX_ACTIVE_PER_USER

    if slot_free:
        if not enqueue_job(payload):
            raise HTTPException(status_code=503, detail="Очередь задач недоступна")

    await asyncio.to_thread(reset_report_terminal_state, str(row["id"]))
    # Возвращаем в pending строки с error и с warning skipped_large_group —
    # чтобы retry реально их полечил, а не пропустил как уже обработанные.
    reset_count = await asyncio.to_thread(
        reset_failed_and_skipped_rows, str(row["id"])
    )
    total_rows_val = int(row.get("total_rows") or 0)
    processed_rows_val = max(0, int(row.get("processed_rows") or 0) - reset_count)
    progress_percent_val = (
        float(processed_rows_val) / total_rows_val * 100.0 if total_rows_val > 0 else 0.0
    )
    await asyncio.to_thread(
        update_report_status,
        report_id=str(row["id"]),
        status=JobStatus.queued.value,
        current_step="Перезапуск: в очереди",
    )
    if not slot_free:
        # Помечаем личной очередью — отчёт не в Redis, ждёт promote.
        await asyncio.to_thread(set_personal_queue, str(row["id"]), True)
    await asyncio.to_thread(
        update_report_progress,
        report_id=str(row["id"]),
        total_rows=total_rows_val,
        processed_rows=processed_rows_val,
        progress_percent=progress_percent_val,
        eta_seconds=None,
        current_step="Перезапуск: в очереди",
    )
    logger.info(
        "User retry queued: user_id=%s username=%s report_id=%s job_id=%s previous_status=%s personal_queue=%s",
        user.get("id"),
        user.get("username"),
        row.get("id"),
        job_id,
        status,
        not slot_free,
    )
    return {"status": JobStatus.queued.value}


def _build_queue_position_map(items: list[ReportItem]) -> dict[str, int]:
    """Одним проходом по Redis-очереди строит map {job_id → 0-based позиция}
    для отчётов в статусе `queued`. Позволяет не дёргать LRANGE на каждую строку.
    """
    queued_job_ids = {it.job_id for it in items if it.status == JobStatus.queued.value and it.job_id}
    if not queued_job_ids:
        return {}
    result: dict[str, int] = {}
    try:
        from app.queue import QUEUE_KEY, _redis  # локальный импорт — избегаем циклов при старте модуля
        client = _redis()
        raw_items = client.lrange(QUEUE_KEY, 0, -1) or []
    except Exception:
        return result
    for idx, raw in enumerate(raw_items):
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        jid = str(parsed.get("job_id") or "").strip()
        if jid and jid in queued_job_ids and jid not in result:
            result[jid] = idx
    return result


def _enrich_with_queue_positions(items: list[ReportItem]) -> list[ReportItem]:
    positions = _build_queue_position_map(items)
    if not positions:
        return items
    for item in items:
        if item.job_id in positions:
            item.queue_position = positions[item.job_id]
    return items


def _enrich_with_last_logs(items: list[ReportItem]) -> list[ReportItem]:
    """Подтягивает последние логи из Redis-list job_logs:<id> для каждого отчёта.

    Один pipeline-запрос на все job_id'ы — без N+1 round-trip'ов. Для отчётов
    в терминальных статусах список может быть пустым (TTL Redis-list 24 часа,
    после чего ключ исчезает; завершённые задачи не пишут новых строк).
    """
    job_ids = [it.job_id for it in items if it.job_id]
    if not job_ids:
        return items
    logs_map = get_job_logs_bulk(job_ids, limit=20)
    for item in items:
        item.last_logs = logs_map.get(item.job_id, []) if item.job_id else []
    return items


def _enrich_active_list(items: list[ReportItem]) -> list[ReportItem]:
    return _enrich_with_last_logs(_enrich_with_queue_positions(items))


@app.get("/api/reports", response_model=ReportsResponse)
def reports(user: dict = Depends(get_current_user)) -> ReportsResponse:
    rows = list_reports(int(user["id"]), limit=20)
    items = [_normalize_report_row_light(row) for row in rows]
    return ReportsResponse(reports=_enrich_active_list(items))


@app.get("/api/reports/active", response_model=ReportsResponse)
def reports_active(user: dict = Depends(get_current_user)) -> ReportsResponse:
    rows = list_active_reports(int(user["id"]), limit=20)
    items = [_normalize_report_row_light(row) for row in rows]
    return ReportsResponse(reports=_enrich_active_list(items))


@app.get("/api/reports/{report_id}", response_model=ReportItem)
def report_details(report_id: str, user: dict = Depends(get_current_user)) -> ReportItem:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    return _normalize_report_row(row)


@app.get("/api/reports/{report_id}/analysis", response_model=ReportAnalysisResponse)
def report_analysis(report_id: str, user: dict = Depends(get_current_user)) -> ReportAnalysisResponse:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")

    summary, preview_rows = build_report_analysis(report_id)
    raw_summary = row.get("summary_json")
    if raw_summary:
        try:
            parsed = json.loads(raw_summary)
            if isinstance(parsed, dict):
                summary = parsed
        except Exception:
            pass

    return ReportAnalysisResponse(
        report_id=report_id,
        status=str(row.get("status") or ""),
        summary=summary,
        preview_rows=preview_rows,
    )


@app.delete("/api/reports/{report_id}")
def remove_report(report_id: str, user: dict = Depends(get_current_user)) -> dict[str, str]:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")

    status = str(row.get("status") or "")
    if status in {JobStatus.running.value, JobStatus.queued.value, JobStatus.paused.value}:
        raise HTTPException(status_code=400, detail="Нельзя удалить активный отчет. Сначала нажмите Отмена.")

    reason = "admin" if access.actor(user)["role"] == "admin" else "user"
    if not delete_report(report_id, int(user["id"]), deleted_by=int(user["id"]), reason=reason):
        raise HTTPException(status_code=409, detail="Завершаются запросы обработки отчёта. Повторите удаление позже.")
    return {"status": "deleted"}


from app.download_utils import sanitize_download_filename as _sanitize_download_filename


def _partial_download(row, fmt, background, filename):
    from app.partial_exports import get_export
    # Права уже проверены вызывающим маршрутом. Открываем файл до StreamingResponse:
    # очистка/новая версия может удалить его имя, но открытый fd останется читаемым.
    for attempt in range(2):
        artifact = get_export(str(row['id']), fmt)
        try:
            stream = artifact.path.open('rb')
            break
        except FileNotFoundError:
            if attempt: raise HTTPException(409, 'Срез изменился, повторите скачивание')
    media = 'application/json' if fmt=='json' else 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    name = _sanitize_download_filename(filename, fallback=artifact.fallback_name(str(row['id'])), extension=fmt)
    disposition = FileResponse(artifact.path,filename=name).headers['content-disposition']
    def chunks():
        try:
            while data := stream.read(1024*1024): yield data
        finally: stream.close()
    background.add_task(stream.close)
    if artifact.temporary: background.add_task(_delete_path_later,artifact.path)
    class PartialStreamResponse(StreamingResponse):
        async def __call__(self,scope,receive,send):
            try:
                await super().__call__(scope,receive,send)
            finally:
                stream.close()
                if artifact.temporary: _delete_path_later(artifact.path)
    return PartialStreamResponse(chunks(),media_type=media,background=background,
        headers={'Content-Disposition':disposition,'X-Partial-Snapshot':artifact.captured_at,
                 'Content-Length':str(os.fstat(stream.fileno()).st_size)})


def _delete_path_later(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to cleanup partial download %s: %s", path, exc)






@app.get("/api/reports/{report_id}/download/xlsx")
def download_report_xlsx(
    report_id: str,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    results_file = row.get("results_file")
    if not results_file:
        raise HTTPException(status_code=404, detail="Файл результатов еще не готов")
    path = RESULTS_DIR / results_file
    if not path.exists():
        raise HTTPException(status_code=404, detail="Файл результатов не найден на диске")
    download_name = _sanitize_download_filename(filename, fallback=path.stem, extension="xlsx")
    return FileResponse(path, filename=download_name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/reports/{report_id}/download/raw")
def download_report_raw(
    report_id: str,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    raw_file = row.get("raw_file")
    if not raw_file:
        raise HTTPException(status_code=404, detail="Сырой файл еще не готов")
    path = RESULTS_DIR / raw_file
    if not path.exists():
        raise HTTPException(status_code=404, detail="Сырой файл не найден на диске")
    download_name = _sanitize_download_filename(filename, fallback=path.stem, extension="json")
    return FileResponse(path, filename=download_name, media_type="application/json")


@app.get("/api/reports/{report_id}/download/source")
def download_report_source(
    report_id: str,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    uploaded_file_id = str(row.get("uploaded_file_id") or "")
    user_id = int(user["id"])
    if not uploaded_file_id or user_id <= 0:
        raise HTTPException(status_code=404, detail="Исходный файл не привязан к отчету")
    source = get_uploaded_file(uploaded_file_id, user_id)
    if not source:
        raise HTTPException(status_code=404, detail="Исходный файл не найден")
    source_path = Path(str(source.get("path") or ""))
    if not source_path.exists():
        raise HTTPException(status_code=404, detail="Исходный файл отсутствует на диске")
    original_name = str(source.get("original_name") or source_path.name)
    original_stem = Path(original_name).stem or "source"
    download_name = _sanitize_download_filename(filename, fallback=original_stem, extension="xlsx")
    return FileResponse(
        source_path,
        filename=download_name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/api/reports/{report_id}/download/partial/xlsx")
def download_report_partial_xlsx(
    report_id: str,
    background: BackgroundTasks,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status not in {JobStatus.running.value, JobStatus.paused.value, JobStatus.queued.value}:
        raise HTTPException(
            status_code=409,
            detail="Промежуточная выгрузка доступна только для запущенных или приостановленных отчётов — используйте обычное скачивание",
        )
    return _partial_download(row, "xlsx", background, filename)


@app.get("/api/reports/{report_id}/download/partial/raw")
def download_report_partial_raw(
    report_id: str,
    background: BackgroundTasks,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(status_code=404, detail="Отчет не найден")
    status = str(row.get("status") or "")
    if status not in {JobStatus.running.value, JobStatus.paused.value, JobStatus.queued.value}:
        raise HTTPException(
            status_code=409,
            detail="Промежуточная выгрузка доступна только для запущенных или приостановленных отчётов — используйте обычное скачивание",
        )
    return _partial_download(row, "json", background, filename)


@app.get("/api/jobs/{job_id}/download/xlsx")
def download_xlsx(
    job_id: str,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = _require_job_owner(job_id, int(user["id"]))
    results_file = row.get("results_file")
    if not results_file:
        raise HTTPException(status_code=404, detail="Файл результатов еще не готов")
    path = RESULTS_DIR / str(results_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Файл результатов не найден на диске")
    download_name = _sanitize_download_filename(filename, fallback=path.stem, extension="xlsx")
    return FileResponse(path, filename=download_name, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/jobs/{job_id}/download/raw")
def download_raw(
    job_id: str,
    filename: str | None = Query(default=None),
    user: dict = Depends(get_current_user),
) -> FileResponse:
    row = _require_job_owner(job_id, int(user["id"]))
    raw_file = row.get("raw_file")
    if not raw_file:
        raise HTTPException(status_code=404, detail="Сырой файл еще не готов")
    path = RESULTS_DIR / str(raw_file)
    if not path.exists():
        raise HTTPException(status_code=404, detail="Сырой файл не найден на диске")
    download_name = _sanitize_download_filename(filename, fallback=path.stem, extension="json")
    return FileResponse(path, filename=download_name, media_type="application/json")


def _download_errors(row, background_tasks):
    report_id = str(row["id"])
    path = RESULTS_DIR / f"errors_{uuid.uuid4().hex}.xlsx"
    export_errors_xlsx(path, lambda: iter_report_rows(report_id, errors_only=True))
    background_tasks.add_task(_delete_path_later, path)
    return FileResponse(path, filename="errors.xlsx", media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.get("/api/reports/{report_id}/download/errors/xlsx")
def download_errors(report_id: str, background_tasks: BackgroundTasks, user: dict = Depends(require_access)):
    row = get_report(report_id, int(user["id"]))
    if not row:
        raise HTTPException(404, "Отчёт не найден")
    return _download_errors(row, background_tasks)


@app.get("/api/admin/reports/{report_id}/download/errors/xlsx")
def admin_download_errors(report_id: str, background_tasks: BackgroundTasks, user: dict = Depends(require_admin)):
    row = get_report_any(report_id)
    if not row:
        raise HTTPException(404, "Отчёт не найден")
    return _download_errors(row, background_tasks)


from app.access_api import router as access_router
app.include_router(access_router(get_current_user, require_admin, require_access))


@app.post("/api/jobs/preview-prompt")
def preview_prompt(payload: StartJobRequest, user: dict = Depends(require_access)):
    from app.services.excel_service import iter_sheet_rows
    from app.services.job_manager import job_manager
    from app.providers.openai_provider import OpenAIProvider
    meta = get_uploaded_file(payload.file_id, int(user["id"]))
    if not meta:
        raise HTTPException(404, "Загруженный файл не найден")
    if meta.get("inspect_status") != "ready":
        raise HTTPException(409, "Дождитесь проверки файла")
    try:
        schema = _build_output_schema_from_expected_json_template(payload.expected_json_template or {})
        non_analysis = list(dict.fromkeys([payload.group_by_column, *(payload.non_analysis_columns or [])])) if payload.group_by_column else payload.non_analysis_columns
        warnings = settings_warnings(payload.expected_json_template, meta, payload.sheet_name, payload.analysis_columns, non_analysis)
        if payload.group_by_column and payload.group_by_column not in payload.analysis_columns:
            raise ValueError("Колонка группировки должна входить в колонки для анализа")
        rows = iter_sheet_rows(Path(meta["path"]), sheet_name=payload.sheet_name, analysis_columns=payload.analysis_columns,
                               non_analysis_columns=non_analysis, max_reviews=payload.max_reviews)
        try:
            first = next(rows, None)
            if first is None:
                raise ValueError("В файле нет строк для анализа")
            if payload.group_by_column:
                group_key, _ = job_manager._input_group_key(first, payload.group_by_column)
                grouped = [first]
                for row in rows:
                    if job_manager._input_group_key(row, payload.group_by_column)[0] == group_key:
                        grouped.append(row)
                        if len(grouped) > GROUP_MAX_ROWS:
                            raise ValueError("Первая группа превышает допустимое число строк")
                first = job_manager._merge_group_rows(grouped, group_key, payload.group_by_column)
            prompt = job_manager._render_prompt(prompt_template=payload.prompt_template, review_text=first["review_text"],
                input_json=first["input_json"], analysis_mode=payload.analysis_mode, expected_json_template=payload.expected_json_template,
                language=payload.prompt_language or get_ui_language())
        finally:
            rows.close()
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    messages = OpenAIProvider.request_body(prompt, payload.model, payload.temperature)["messages"] if payload.provider == "openai" else [{"role": "user", "content": prompt}]
    truncated = False
    shown = []
    for message in messages:
        content = str(message.get("content") or "")
        truncated = truncated or len(content) > 50000
        shown.append({"role": message.get("role"), "content": content[:50000], "characters": len(content)})
    return {"messages": shown, "response_schema": schema, "warnings": warnings,
            "truncated": truncated, "will_send": bool(first.get("review_text")), "tokens_spent": 0}


@app.post("/api/reports/bulk_delete")
def bulk_delete(payload: BulkDeleteRequest, user: dict = Depends(require_access)):
    from app.db import bulk_delete_reports
    actor_id = int(user["id"])
    return bulk_delete_reports(actor_id, payload.ids, admin_actor_id=actor_id if access.actor(user)["role"] == "admin" else None)
