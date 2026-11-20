"""Проверка соответствия полей ответа и колонок источника."""
import json


def settings_warnings(expected, file_meta, sheet_name, analysis_columns, non_analysis_columns):
    raw = file_meta.get("inspect_sheets_json") or "[]"
    sheets = json.loads(raw) if isinstance(raw, str) else raw
    sheet = next((s for s in sheets if s["name"] == sheet_name), None)
    if sheet is None:
        raise ValueError("Лист не найден")
    columns = set(sheet.get("columns") or [])
    selected = set(analysis_columns) | set(non_analysis_columns or [])
    if selected - columns:
        raise ValueError("Выбраны неизвестные колонки: " + ", ".join(sorted(selected - columns)))
    return [name for name in (expected or {}) if name in columns and name not in selected]
