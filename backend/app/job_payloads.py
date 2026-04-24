from __future__ import annotations

import json
import os
from typing import Any

from app.config import GROUP_MAX_ROWS


def _bool_from_db(raw: object, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _json_or_none(raw: object) -> dict[str, Any] | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def _json_list_or_default(raw: object, default: list[str]) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return default
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
    except Exception:
        pass
    return default


def build_job_payload_from_report(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    provider = str(row.get("provider") or "").strip()
    model = str(row.get("model") or "").strip()
    prompt_template = str(row.get("prompt_template") or "").strip()
    uploaded_file_id = str(row.get("uploaded_file_id") or "").strip()
    report_id = str(row.get("id") or "").strip()
    job_id = str(row.get("job_id") or "").strip()
    user_id = int(row.get("user_id") or 0)
    if not (provider and model and prompt_template and uploaded_file_id and report_id and job_id and user_id):
        return None, "Задача не может быть восстановлена: отсутствует контекст запуска"

    api_key_encrypted: str | None = None
    if provider == "openai":
        encrypted = str(row.get("api_key_encrypted") or "").strip()
        if encrypted:
            api_key_encrypted = encrypted
        else:
            env_key = os.getenv("OPENAI_API_KEY", "").strip()
            if not env_key:
                return None, "Задача не может быть восстановлена: для OpenAI не сохранен API-токен"

    input_columns = _json_list_or_default(row.get("input_columns_json"), [])
    non_analysis_columns = _json_list_or_default(row.get("non_analysis_columns_json"), [])
    output_schema = _json_or_none(row.get("output_schema_json"))
    expected_json_template = _json_or_none(row.get("expected_json_template_json"))

    payload: dict[str, Any] = {
        "job_id": job_id,
        "report_id": report_id,
        "user_id": user_id,
        "file_id": uploaded_file_id,
        "provider": provider,
        "model": model,
        "prompt_template": prompt_template,
        "sheet_name": str(row.get("sheet_name") or ""),
        "analysis_columns": input_columns,
        "non_analysis_columns": non_analysis_columns,
        "analysis_mode": str(row.get("analysis_mode") or "custom"),
        "output_schema": output_schema,
        "expected_json_template": expected_json_template,
        "group_by_column": str(row.get("group_by_column") or "").strip() or None,
        "group_max_rows": GROUP_MAX_ROWS,
        "max_reviews": int(row.get("max_reviews") or 100),
        "parallelism": int(row.get("parallelism") or 3),
        "temperature": float(row.get("temperature") or 0.0),
        "include_raw_json": _bool_from_db(row.get("include_raw_json"), True),
        "use_cache": _bool_from_db(row.get("use_cache"), True),
    }
    if api_key_encrypted:
        payload["api_key_encrypted"] = api_key_encrypted
    return payload, None
