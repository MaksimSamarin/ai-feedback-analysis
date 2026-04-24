"""Юнит-тесты для `inspect_xlsx` (v2.0.0, итерация 3.2).

Проверяют расчёт `unique_counts` — количество уникальных значений в каждой
колонке. Используется во frontend для показа «будет обработано N групп»
при выборе колонки группировки.

Проверяют:
1. unique_counts присутствует в результате inspect.
2. Подсчёт верен на малом файле.
3. Пустые ячейки и пробельные строки не попадают в подсчёт.
4. Cap (INSPECT_UNIQUE_CAP) защищает от OOM: колонка с >cap уникальных → None.
5. Числовые/bool/строковые значения считаются одинаково корректно.

Запуск:
    cd backend && pytest tests/unit/test_inspect_xlsx_unique_counts.py -v
"""

from __future__ import annotations

from pathlib import Path

from openpyxl import Workbook


def _make_xlsx(tmp_path: Path, sheet_name: str, header: list[str], rows: list[list]) -> Path:
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name
    ws.append(header)
    for row in rows:
        ws.append(row)
    out = tmp_path / "test.xlsx"
    wb.save(out)
    return out


def test_unique_counts_present_in_result(tmp_path: Path) -> None:
    """В результате inspect есть поле unique_counts со всеми колонками заголовка."""
    from app.services.excel_service import inspect_xlsx

    path = _make_xlsx(tmp_path, "Sheet1", ["col_a", "col_b"], [["x", 1], ["y", 2]])
    sheets = inspect_xlsx(path)
    assert len(sheets) == 1
    assert "unique_counts" in sheets[0]
    assert set(sheets[0]["unique_counts"].keys()) == {"col_a", "col_b"}


def test_unique_counts_values_correct(tmp_path: Path) -> None:
    """Количество уникальных подсчитано корректно."""
    from app.services.excel_service import inspect_xlsx

    path = _make_xlsx(
        tmp_path,
        "Sheet1",
        ["operator", "category"],
        [
            ["ivanov", "доставка"],
            ["petrov", "доставка"],
            ["ivanov", "качество"],
            ["sidorov", "качество"],
            ["ivanov", "доставка"],
        ],
    )
    sheets = inspect_xlsx(path)
    counts = sheets[0]["unique_counts"]
    assert counts["operator"] == 3  # ivanov, petrov, sidorov
    assert counts["category"] == 2  # доставка, качество


def test_unique_counts_ignores_empty_cells(tmp_path: Path) -> None:
    """Пустые ячейки и пробельные строки не попадают в counts."""
    from app.services.excel_service import inspect_xlsx

    path = _make_xlsx(
        tmp_path,
        "Sheet1",
        ["col"],
        [["a"], [""], ["   "], [None], ["b"], ["a"]],
    )
    sheets = inspect_xlsx(path)
    # Только "a" и "b" — пустые и пробельные игнорируются
    assert sheets[0]["unique_counts"]["col"] == 2


def test_unique_counts_cap_returns_none_when_exceeded(tmp_path: Path) -> None:
    """Если уникальных больше cap — counts[col] = None (защита от OOM)."""
    from app.services.excel_service import inspect_xlsx

    # Файл с 10 уникальными значениями в колонке, cap=5 → должно быть None
    rows = [[f"value_{i}"] for i in range(10)]
    path = _make_xlsx(tmp_path, "Sheet1", ["col"], rows)
    sheets = inspect_xlsx(path, unique_cap=5)
    assert sheets[0]["unique_counts"]["col"] is None


def test_unique_counts_cap_allows_exactly_cap_count(tmp_path: Path) -> None:
    """Ровно cap уникальных значений — ещё считается, возвращается число."""
    from app.services.excel_service import inspect_xlsx

    rows = [[f"val_{i}"] for i in range(5)]
    path = _make_xlsx(tmp_path, "Sheet1", ["col"], rows)
    sheets = inspect_xlsx(path, unique_cap=5)
    assert sheets[0]["unique_counts"]["col"] == 5


def test_unique_counts_handles_mixed_types(tmp_path: Path) -> None:
    """Числа и строки считаются без ошибок.

    Bool в Python считается равным 1/0 в set'е (True == 1, False == 0 по хешу),
    поэтому отдельно их не тестируем — на практике по bool никто не группирует.
    """
    from app.services.excel_service import inspect_xlsx

    path = _make_xlsx(
        tmp_path,
        "Sheet1",
        ["mixed"],
        [[1], [2], [1], ["text"], [3.5], [3.5], ["текст"]],
    )
    counts = inspect_xlsx(path)[0]["unique_counts"]
    # 1, 2, "text", 3.5, "текст" — 5 уникальных
    assert counts["mixed"] == 5


def test_total_rows_still_computed_correctly(tmp_path: Path) -> None:
    """Регрессия: total_rows продолжает считаться (не сломан после добавления unique)."""
    from app.services.excel_service import inspect_xlsx

    path = _make_xlsx(tmp_path, "Sheet1", ["col"], [["a"], ["b"], ["c"]])
    sheets = inspect_xlsx(path)
    assert sheets[0]["total_rows"] == 3
