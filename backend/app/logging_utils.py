from __future__ import annotations

import contextvars
import json
import logging
import os
import re
import sys
import uuid
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

_request_id: contextvars.ContextVar[str] = contextvars.ContextVar("request_id", default="-")
_user_id: contextvars.ContextVar[str] = contextvars.ContextVar("user_id", default="-")
_username: contextvars.ContextVar[str] = contextvars.ContextVar("username", default="-")
_job_context = contextvars.ContextVar("job_context", default={})
_service_name = "backend"


def current_context():
    return {"service": _service_name, "request_id": _request_id.get(), "user_id": _user_id.get(),
            "username": _username.get(), **_job_context.get()}


def bind_job_context(**values):
    return _job_context.set({**_job_context.get(), **values})


def reset_job_context(token):
    _job_context.reset(token)

_SENSITIVE_PATTERNS = [
    re.compile(r"(?i)(authorization:\s*bearer\s+)[^\s,;]+"),
    re.compile(r"(?i)(api[_-]?key[\"']?\s*[:=]\s*[\"'])[^\"']+([\"'])"),
    re.compile(r"(?i)(password[\"']?\s*[:=]\s*[\"'])[^\"']+([\"'])"),
    re.compile(r"(?i)(token[\"']?\s*[:=]\s*[\"'])[^\"']+([\"'])"),
]


def set_request_id(value: str) -> contextvars.Token[str]:
    safe = str(value)[:128]
    if _redact(safe) != safe or not re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", safe):
        safe = uuid.uuid4().hex
    return _request_id.set(safe)


def reset_request_id(token: contextvars.Token[str]) -> None:
    _request_id.reset(token)


def set_user_context(user_id: str | int | None, username: str | None) -> tuple[contextvars.Token[str], contextvars.Token[str]]:
    uid = str(user_id) if user_id is not None else "-"
    uname = str(username) if username else "-"
    return _user_id.set(uid), _username.set(uname)


def reset_user_context(tokens: tuple[contextvars.Token[str], contextvars.Token[str]]) -> None:
    user_id_token, username_token = tokens
    _user_id.reset(user_id_token)
    _username.reset(username_token)


def _redact(text: str) -> str:
    masked = text
    for pattern in _SENSITIVE_PATTERNS:
        if pattern.groups >= 2:
            masked = pattern.sub(r"\1***\2", masked)
        else:
            masked = pattern.sub(r"\1***", masked)
    masked = re.sub(r'(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+', r'\1***', masked)
    masked = re.sub(r'(?i)((?:api[_-]?key|password|secret|token|cookie|authorization)\s*[:=]\s*)[^\s,;]+', r'\1***', masked)
    masked = re.sub(r'(?is)(["\']?(?:content|prompt|prompt_template|review|отзыв|messages)["\']?\s*[:=]\s*)'
                    r'(?:"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|\[[\s\S]*?\]|[^\n]+)', r'\1"***"', masked)
    for key, value in os.environ.items():
        if re.search(r'(?i)(api_?key|secret|password|access_token|base_url)', key) and len(value) >= 6:
            masked = masked.replace(value, "***")
    return masked


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _redact(record.getMessage()),
            "service": getattr(record, "service", "-"),
            "request_id": getattr(record, "request_id", "-"),
            "user_id": getattr(record, "user_id", "-"),
            "username": getattr(record, "username", "-"),
            "report_id": getattr(record, "report_id", None),
            "job_id": getattr(record, "job_id", None),
            "group_id": getattr(record, "group_id", None),
            "event_type": getattr(record, "event_type", "application"),
        }
        if record.exc_info:
            payload["exception"] = _redact(self.formatException(record.exc_info))
        for key, value in record.__dict__.items():
            if key not in {"status_code", "duration_ms", "row_number", "error_code", "business"} or key in payload:
                continue
            if value is None:
                continue
            payload[key] = _redact(str(value)) if isinstance(value, str) else value
        return json.dumps({k: _redact(v) if isinstance(v, str) else v for k, v in payload.items()}, ensure_ascii=False)


class ContextFilter(logging.Filter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self._service
        for key, value in _job_context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        if not getattr(record, "request_id", None) or getattr(record, "request_id", "-") == "-":
            record.request_id = _request_id.get()
        if not getattr(record, "user_id", None) or getattr(record, "user_id", "-") == "-":
            record.user_id = _user_id.get()
        if not getattr(record, "username", None) or getattr(record, "username", "-") == "-":
            record.username = _username.get()
        return True


def configure_logging(service: str) -> None:
    global _service_name
    _service_name = service
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    try:
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

    level_name = os.getenv("LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, level_name, logging.INFO)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    # Reduce noisy third-party transport logs (httpx/httpcore request lines).
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(ContextFilter(service))
    root.addHandler(handler)

    if os.getenv("STRUCTURED_LOG_DB", "1").lower() in {"1", "true", "yes"}:
        from app.events import DatabaseHandler
        database_handler = DatabaseHandler()
        database_handler.setLevel(level)
        database_handler.setFormatter(JsonFormatter())
        database_handler.addFilter(ContextFilter(service))
        root.addHandler(database_handler)

    log_to_file = os.getenv("LOG_TO_FILE", "1").strip().lower() in {"1", "true", "yes"}
    if not log_to_file:
        return
    log_dir = Path(os.getenv("APP_LOG_DIR", "/app/data/logs")).resolve()
    max_bytes = int(os.getenv("APP_LOG_MAX_BYTES", str(20 * 1024 * 1024)) or (20 * 1024 * 1024))
    backup_count = int(os.getenv("APP_LOG_BACKUP_COUNT", "5") or 5)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            filename=str(log_dir / f"{service}.log"),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(JsonFormatter())
        file_handler.addFilter(ContextFilter(service))
        root.addHandler(file_handler)
    except Exception:
        # Keep stdout logging functional even if file logging init failed.
        pass

