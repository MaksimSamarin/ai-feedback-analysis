from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Any, Callable, Iterable, Iterator, Union

import xlsxwriter
from openpyxl import load_workbook
from app.services.csv_input import CsvWorkbook, CSV_SHEET


XlsxTarget = Union[Path, str, IO[bytes]]


RowsFactory = Callable[[], Iterable[dict[str, Any]]]


EMPTY_MARKERS = {"", "nan", "none", "null", "n/a", "na", "-"}
EXCEL_MAX_ROWS = 1048576


def _data_sheet(wb, name, headers, page=1):
    title = name if page == 1 else f"{name[:24]}_{page}"
    sheet = wb.add_worksheet(title)
    for column, header in enumerate(headers):
        sheet.write_string(0, column, str(header))
    return sheet


# Верхняя граница для подсчёта уникальных значений на колонку во время inspect.
# После достижения cap множество освобождается. Размер текстовых значений
# также влияет на память; лимит inspect-worker — 512 MB.
# Переопределяется ENV INSPECT_UNIQUE_CAP.
INSPECT_UNIQUE_CAP = int(os.getenv("INSPECT_UNIQUE_CAP", "100000"))


def inspect_xlsx(path: Path, *, unique_cap: int = INSPECT_UNIQUE_CAP) -> list[dict[str, Any]]:
    # openpyxl в read-only стримит rows без полной загрузки в память. Calamine быстрее,
    # но Linux-binary <=0.7.0 падает в panic ("Option::unwrap on None", src/types/sheet.rs:253)
    # на крупных xlsx с раздутым sheet1.xml — баг воспроизводится в Docker, локально под
    # Windows тот же файл проходит. openpyxl — pure Python, panic'ов не имеет.
    wb = load_workbook(str(path), read_only=True, data_only=True)
    sheets: list[dict[str, Any]] = []

    try:
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            rows = ws.iter_rows(values_only=True)
            header = next(rows, ())
            columns = [str(cell).strip() if cell is not None else "" for cell in header]
            columns = [f"Column {idx + 1}" if not col else col for idx, col in enumerate(columns)]

            unique_buckets: list[set[Any] | None] = [set() for _ in columns]
            total_rows = 0
            for row in rows:
                total_rows += 1
                for idx, cell in enumerate(row):
                    if idx >= len(unique_buckets):
                        continue
                    bucket = unique_buckets[idx]
                    if bucket is None:
                        continue
                    if cell is None:
                        continue
                    if isinstance(cell, str):
                        key: Any = cell.strip()
                        if not key:
                            continue
                    elif isinstance(cell, (int, float, bool)):
                        key = cell
                    else:
                        key = str(cell)
                    bucket.add(key)
                    if len(bucket) > unique_cap:
                        unique_buckets[idx] = None

            unique_counts: dict[str, int | None] = {}
            for idx, col in enumerate(columns):
                bucket = unique_buckets[idx]
                unique_counts[col] = len(bucket) if bucket is not None else None

            sheets.append(
                {
                    "name": ws.title,
                    "columns": columns,
                    "total_rows": total_rows,
                    "unique_counts": unique_counts,
                }
            )
    finally:
        wb.close()

    return sheets


def inspect_file(path: Path, *, unique_cap: int = INSPECT_UNIQUE_CAP) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".xlsx":
        return inspect_xlsx(path, unique_cap=unique_cap)
    if path.suffix.lower() != ".csv":
        raise ValueError("Поддерживаются только XLSX и CSV")
    wb = CsvWorkbook(path)
    try:
        rows = wb.iter_rows()
        columns = next(rows)
        buckets = [set() for _ in columns]
        total = 0
        for row in rows:
            total += 1
            for i, value in enumerate(row):
                bucket = buckets[i]
                if bucket is not None and value.strip():
                    bucket.add(value.strip())
                    if len(bucket) > unique_cap:
                        buckets[i] = None
        return [{"name": CSV_SHEET, "columns": columns, "total_rows": total,
                 "unique_counts": {c: len(b) if b is not None else None for c, b in zip(columns, buckets)}}]
    finally:
        wb.close()


def normalize_review(value: Any) -> tuple[str | None, list[str]]:
    warnings: list[str] = []
    if value is None:
        return None, ["empty_cell"]

    text = str(value).strip()
    if text.lower() in EMPTY_MARKERS:
        return None, ["empty_cell"]

    return text, warnings


def _build_review_text(row_payload: dict[str, str | None]) -> str | None:
    parts = [f"{key}: {value}" for key, value in row_payload.items() if value not in (None, "")]
    if not parts:
        return None
    return "\n".join(parts)


class StreamingWorkbook:
    """Общий интерфейс чтения CSV/XLSX без материализации всего листа."""
    def __init__(self, path):
        self.workbook = load_workbook(str(path), read_only=True, data_only=True)
        self.sheet_names = self.workbook.sheetnames

    def get_sheet_by_name(self, name):
        return self.workbook[name]

    def close(self):
        self.workbook.close()


def iter_sheet_rows(
    path: Path,
    *,
    sheet_name: str,
    analysis_columns: list[str],
    max_reviews: int,
    non_analysis_columns: list[str] | None = None,
) -> Iterator[dict[str, Any]]:
    wb = CsvWorkbook(path) if path.suffix.lower() == ".csv" else StreamingWorkbook(path)
    try:
        if sheet_name not in wb.sheet_names:
            raise ValueError(f"Лист не найден: {sheet_name}")

        ws = wb.get_sheet_by_name(sheet_name)
        rows = ws.iter_rows() if isinstance(wb, CsvWorkbook) else ws.iter_rows(values_only=True)
        header = next(rows, [])
        columns = [str(cell).strip() if cell is not None else "" for cell in header]
        columns = [f"Column {idx + 1}" if not col else col for idx, col in enumerate(columns)]

        selected_analysis_columns = [col for col in analysis_columns if col]
        selected_non_analysis_columns = [col for col in (non_analysis_columns or []) if col]
        if not selected_analysis_columns:
            raise ValueError("Нужно выбрать хотя бы одну колонку для анализа")
        for selected_column in [*selected_analysis_columns, *selected_non_analysis_columns]:
            if selected_column not in columns:
                raise ValueError(f"Колонка не найдена: {selected_column}")

        analysis_indices = {name: columns.index(name) for name in selected_analysis_columns}
        passthrough_indices = {name: columns.index(name) for name in selected_non_analysis_columns}

        yielded = 0
        for row_number, row in enumerate(rows, start=2):
            if yielded >= max_reviews:
                break

            warnings: list[str] = []
            row_payload: dict[str, str | None] = {}
            passthrough_payload: dict[str, str | None] = {}
            for col_name, idx in analysis_indices.items():
                value = row[idx] if idx < len(row) else None
                value_text, value_warnings = normalize_review(value)
                row_payload[col_name] = value_text
                for warning in value_warnings:
                    if warning not in warnings:
                        warnings.append(warning)
            for col_name, idx in passthrough_indices.items():
                value = row[idx] if idx < len(row) else None
                if value is None or isinstance(value, (bool, int)) or isinstance(value, float) and math.isfinite(value):
                    passthrough_payload[col_name] = value
                else:
                    passthrough_payload[col_name] = str(value).strip()

            text = _build_review_text(row_payload)

            yielded += 1
            yield {
                "row_number": row_number,
                "review_text": text,
                "input_json": json.dumps(row_payload, ensure_ascii=False),
                "passthrough_json": json.dumps(passthrough_payload, ensure_ascii=False),
                "warnings": warnings,
            }
    finally:
        wb.close()


def read_sheet_rows(
    path: Path,
    *,
    sheet_name: str,
    analysis_columns: list[str],
    max_reviews: int,
    non_analysis_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    return list(
        iter_sheet_rows(
            path,
            sheet_name=sheet_name,
            analysis_columns=analysis_columns,
            max_reviews=max_reviews,
            non_analysis_columns=non_analysis_columns,
        )
    )


_RESERVED_INPUT_KEYS = frozenset({"row_number", "review_text", "input_json", "warnings", "error"})


def _flatten_dict(data: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in data.items():
        col = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict):
            out.update(_flatten_dict(value, col))
        else:
            out[col] = value
    return out


def _normalize_cell(value: Any) -> Any:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value if item is not None)
    return value


def _parse_json_field(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, dict):
                return decoded
        except Exception:
            return {}
    return {}


def _row_is_materialized(row: dict[str, Any]) -> bool:
    """Строка считается «обработанной» если LLM дал результат или явно упала с ошибкой.

    Нужен для фильтрации pending-строк в выгрузке: в отчёте на 1 млн строк мы
    не должны тащить в xlsx 990k пустых записей, если обработано только 10k.
    Логика устойчива к обоим способам сигнализации:
    - status='done' или 'error' (если поле status передано)
    - иначе — наличие custom_json или error_text
    """
    status = row.get("status")
    if isinstance(status, str):
        if status in {"done", "error"}:
            return True
        if status in {"pending", "in_progress", "queued"}:
            return False
        # unknown status — fallback на данные
    if row.get("custom_json") is not None:
        return True
    if row.get("analysis_json") is not None:
        return True
    if row.get("error_text") or row.get("error"):
        return True
    return False


def _extract_analysis(row: dict[str, Any]) -> dict[str, Any]:
    """Возвращает разобранный JSON с результатом LLM-анализа строки.

    В БД поле называется `custom_json` (TEXT), но в тестах и устаревших местах
    иногда передают уже распарсенный dict под ключом `analysis_json`. Поддерживаем
    оба варианта ради обратной совместимости тестов.
    """
    custom = row.get("custom_json")
    if custom is not None:
        parsed = _parse_json_field(custom)
        if parsed:
            return parsed
    legacy = row.get("analysis_json")
    if isinstance(legacy, dict):
        return legacy
    return _parse_json_field(legacy)


def _collect_column_keys(
    rows: Iterable[dict[str, Any]],
    *,
    group_by_column: str | None = None,
) -> tuple[list[str], list[str]]:
    """Первый проход — собирает уникальные ключи passthrough/dynamic без хранения строк.

    Исходные колонки анализа (input_json) в итоговый xlsx не попадают:
    они уже отражены в `review_text` и дублировать их нет смысла. Если
    пользователю нужны справочные поля из исходника — он явно указывает
    их в `non_analysis_columns` (они идут в passthrough_json).

    Для группового отчёта (`group_by_column` задан) из passthrough остаётся
    только сама колонка группировки: остальные поля относятся к одной
    конкретной строке группы, а выгружается одна строка на группу.
    """
    passthrough_keys: list[str] = []
    seen_passthrough: set[str] = set()
    dynamic_keys: list[str] = []
    seen_dynamic: set[str] = set()

    for row in rows:
        if not _row_is_materialized(row):
            continue
        for key in _parse_json_field(row.get("passthrough_json")).keys():
            key_s = str(key)
            if key_s in _RESERVED_INPUT_KEYS or key_s in seen_passthrough:
                continue
            if group_by_column and key_s != group_by_column:
                continue
            seen_passthrough.add(key_s)
            passthrough_keys.append(key_s)
        analysis = _extract_analysis(row)
        for key in _flatten_dict(analysis or {}).keys():
            if key in seen_dynamic:
                continue
            seen_dynamic.add(key)
            dynamic_keys.append(key)

    return passthrough_keys, [key for key in dynamic_keys if key not in seen_passthrough]


def _write_cell(ws: Any, row: int, col: int, value: Any) -> None:
    """Тип-сохраняющая запись ячейки через xlsxwriter.

    bool проверяется ДО int, иначе True попадёт в write_number (в Python bool ⊂ int).
    None → не пишем (в constant_memory это просто blank-ячейка).
    """
    if value is None:
        return
    if isinstance(value, bool):
        ws.write_boolean(row, col, value)
    elif isinstance(value, (int, float)):
        ws.write_number(row, col, value)
    else:
        ws.write_string(row, col, str(value))


def optional_text(value):
    if value is None or str(value).strip().lower() in {"", "none", "null", "undefined"}:
        return None
    return str(value)


def row_warnings(row):
    values = row.get("warnings") or row.get("warnings_json") or []
    if isinstance(values, str):
        try:
            values = json.loads(values)
        except ValueError:
            values = [values]
    return [text for value in (values or []) if (text := optional_text(value))]


class _ErrorsSheetWriter:
    def __init__(self, wb):
        self.wb = wb
        self.sheet = None
        self.index = 1
        self.page = 0

    def create(self):
        if self.sheet is None or self.index >= EXCEL_MAX_ROWS:
            self.page += 1
            self.index = 1
            self.sheet = _data_sheet(self.wb, "Ошибки", ["Номер строки", "Исходный текст", "Причина", "Сырой ответ (сокращён)"], self.page)
            self.sheet.set_column(1, 3, 60)

    def write(self, row):
        error = optional_text(row.get("error_text")) or optional_text(row.get("error"))
        if not error:
            return
        self.create()
        raw = _parse_json_field(row.get("raw_response_json") or row.get("raw_response"))
        preview = str(raw.get("content") or raw.get("diagnostic") or "")[:2000]
        _write_cell(self.sheet, self.index, 0, row.get("row_number"))
        self.sheet.write_string(self.index, 1, str(row.get("review_text") or "")[:500])
        self.sheet.write_string(self.index, 2, str(error)[:2000])
        self.sheet.write_string(self.index, 3, preview)
        self.index += 1


class _SourceSheetWriter:
    def __init__(self, wb, columns):
        self.wb = wb
        self.page = 1
        self.headers = ["row_number", "group_number", *columns]
        self.sheet = _data_sheet(wb, "Исходные данные", self.headers)
        self.columns = columns
        self.index = 1

    def write(self, row, group_number):
        if self.index >= EXCEL_MAX_ROWS:
            self.page += 1
            self.index = 1
            self.sheet = _data_sheet(self.wb, "Исходные данные", self.headers, self.page)
        data = _parse_json_field(row.get("passthrough_json"))
        values = [row.get("row_number"), group_number, *[data.get(c) for c in self.columns]]
        for column, value in enumerate(values):
            _write_cell(self.sheet, self.index, column, _normalize_cell(value))
        self.index += 1


def export_errors_xlsx(path: XlsxTarget, rows_factory: RowsFactory) -> None:
    target = str(path) if isinstance(path, Path) else path
    wb = xlsxwriter.Workbook(target, {"constant_memory": True})
    try:
        writer = _ErrorsSheetWriter(wb)
        writer.create()
        for row in rows_factory():
            writer.write(row)
    finally:
        wb.close()


def export_results_xlsx(
    path: XlsxTarget,
    rows_factory: RowsFactory,
    summary: dict[str, Any],
    *,
    prompt_example: str | None = None,
    group_by_column: str | None = None,
    source_columns: list[str] | None = None,
) -> None:
    """Стримит результаты отчёта в xlsx через xlsxwriter constant_memory.

    `rows_factory` — функция, возвращающая свежий итератор строк. Вызывается дважды:
    первый раз для сбора заголовков колонок, второй — для записи данных.
    Используется `xlsxwriter` с `{"constant_memory": True}` — каждая строка
    сериализуется в файл сразу после записи и из памяти высвобождается.
    На сотнях тысяч строк даёт в 2-3 раза меньше CPU-времени, чем openpyxl
    write_only (см. замер на 285k строк: openpyxl ~66с loop append → xlsxwriter ~22с).

    Для групповых отчётов (`group_by_column` задан) выгружается ОДНА строка на группу,
    и из passthrough остаётся только сама колонка группировки — остальные поля
    (отзыв, оценка, дата конкретной записи) относятся к одной случайной строке группы
    и в агрегате по группе смысла не несут.
    При source_columns дополнительные справочные значения сохраняются для каждой
    строки на листе «Исходные данные» со связью с номером группы.

    `path` — путь к файлу либо file-like объект (BytesIO). Path удобен в проде,
    BytesIO нужен в юнит-тестах чтобы не плодить временные файлы.
    """
    passthrough_keys, dynamic_keys = _collect_column_keys(
        rows_factory(), group_by_column=group_by_column
    )

    workbook_target: Any = str(path) if isinstance(path, Path) else path
    wb = xlsxwriter.Workbook(workbook_target, {"constant_memory": True})

    is_grouped = bool(group_by_column)
    first_column_header = "group_number" if is_grouped else "row_number"
    headers = [
        first_column_header,
        *passthrough_keys,
        *dynamic_keys,
        "warnings",
        "error",
    ]
    page = 1
    ws = _data_sheet(wb, "results", headers)

    seen_group_keys: dict[str, int] = {}
    group_counter = 0
    out_row = 1
    errors_writer = _ErrorsSheetWriter(wb)
    source_keys = list(dict.fromkeys([group_by_column, *(source_columns or [])])) if group_by_column else []
    source_writer = _SourceSheetWriter(wb, source_keys) if any(k != group_by_column for k in source_keys) else None
    for row in rows_factory():
        errors_writer.write(row)
        if not _row_is_materialized(row):
            continue
        group_key = row.get("group_key")
        if group_key is not None and str(group_key) != "":
            gk = str(group_key)
            if gk in seen_group_keys:
                if source_writer:
                    source_writer.write(row, seen_group_keys[gk])
                continue
        parsed_passthrough = _parse_json_field(row.get("passthrough_json"))
        analysis = _extract_analysis(row)
        flat_analysis = _flatten_dict(analysis or {})

        if is_grouped:
            group_counter += 1
            first_column_value: Any = group_counter
        else:
            first_column_value = row.get("row_number")
        if group_key is not None and str(group_key) != "":
            seen_group_keys[str(group_key)] = group_counter
        if source_writer:
            source_writer.write(row, group_counter)

        cells = [
            first_column_value,
            *[_normalize_cell(parsed_passthrough.get(key)) for key in passthrough_keys],
            *[_normalize_cell(flat_analysis.get(key)) for key in dynamic_keys],
            ", ".join(row_warnings(row)),
            optional_text(row.get("error_text")) or optional_text(row.get("error")),
        ]
        if out_row >= EXCEL_MAX_ROWS:
            page += 1
            out_row = 1
            ws = _data_sheet(wb, "results", headers, page)
        for col_idx, value in enumerate(cells):
            _write_cell(ws, out_row, col_idx, value)
        out_row += 1

    summary_ws = wb.add_worksheet("summary")
    summary_ws.write_string(0, 0, "metric")
    summary_ws.write_string(0, 1, "value")
    for sum_row_idx, (key, value) in enumerate(summary.items(), start=1):
        summary_ws.write_string(sum_row_idx, 0, str(key))
        if isinstance(value, (dict, list)):
            summary_ws.write_string(
                sum_row_idx, 1, json.dumps(value, ensure_ascii=False)
            )
        else:
            _write_cell(summary_ws, sum_row_idx, 1, value)

    if prompt_example:
        prompts_ws = wb.add_worksheet("prompt_example")
        prompts_ws.write_string(0, 0, "name")
        prompts_ws.write_string(0, 1, "value")
        prompts_ws.write_string(1, 0, "prompt")
        prompts_ws.write_string(1, 1, prompt_example)

    wb.close()


def export_raw_json(
    path: Path,
    *,
    rows_factory: RowsFactory,
    model: str,
    provider: str,
    prompt_template: str,
    app_version: str,
) -> None:
    """Стримит сырой JSON отчёта на диск без загрузки всех строк в память (см. BUG-14)."""
    metadata = {
        "provider": provider,
        "model": model,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "prompt_template": prompt_template,
        "app_version": app_version,
    }
    with path.open("w", encoding="utf-8") as fh:
        fh.write("{\n  \"metadata\": ")
        json.dump(metadata, fh, ensure_ascii=False, indent=2)
        fh.write(",\n  \"rows\": [")
        first = True
        for row in rows_factory():
            row_copy = dict(row)
            row_copy.pop("input_json", None)
            if not first:
                fh.write(",")
            first = False
            fh.write("\n    ")
            json.dump(row_copy, fh, ensure_ascii=False)
        fh.write("\n  ]\n}\n")
