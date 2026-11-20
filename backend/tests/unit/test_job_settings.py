import json
import io
import pytest
from openpyxl import load_workbook, Workbook
from app.job_settings import settings_warnings
from app.services.job_manager import JobManager
from app.services.excel_service import export_results_xlsx, iter_sheet_rows


def test_omitted_source_fields_warn():
    meta = {"inspect_sheets_json": json.dumps([{"name": "Данные", "columns": ["Текст", "Телефон клиента", "Имя агента"]}])}
    expected = {"Телефон клиента": {}, "Имя агента": {}, "Итог": {}}
    assert settings_warnings(expected, meta, "Данные", ["Текст"], []) == ["Телефон клиента", "Имя агента"]
    assert settings_warnings(expected, meta, "Данные", ["Текст", "Имя агента"], ["Телефон клиента"]) == []


def test_group_preview_uses_worker_payload():
    rows = [{"row_number": 2, "review_text": "Текст: Первый", "input_json": '{"Текст":"Первый","Группа":"A"}'},
            {"row_number": 4, "review_text": "Текст: Второй", "input_json": '{"Текст":"Второй","Группа":"A"}'}]
    manager = JobManager()
    merged = manager._merge_group_rows(rows, "A", "Группа")
    prompt = manager._render_prompt(prompt_template="Данные: {row_json}", review_text=merged["review_text"], input_json=merged["input_json"], analysis_mode="custom", expected_json_template={"Итог": "Текст"})
    assert json.loads(merged["input_json"])["group_size"] == 2
    assert merged["input_json"] in prompt
    assert manager._input_group_key(rows[0], "Группа") == ("A", False)


@pytest.mark.parametrize("source_columns,expected_sheet", [(None, False), (["Группа", "Код"], True)])
def test_grouped_source_columns_are_lossless(source_columns, expected_sheet):
    rows = [{"row_number": i + 2, "status": "done", "group_key": group,
             "passthrough_json": json.dumps({"Группа": group, "Код": code}), "analysis_json": {"Итог": "Тест"}}
            for i, (group, code) in enumerate([("A", "001"), ("B", "002"), ("A", "003")])]
    target = io.BytesIO()
    calls = []
    def factory():
        calls.append(1)
        return iter(rows)
    export_results_xlsx(target, factory, {"total_rows": 3}, group_by_column="Группа", source_columns=source_columns)
    assert len(calls) == 2
    wb = load_workbook(io.BytesIO(target.getvalue()), read_only=True)
    assert len(list(wb["results"].values)) == 3
    assert ("Исходные данные" in wb.sheetnames) == expected_sheet
    if expected_sheet:
        assert list(wb["Исходные данные"].values) == [
            ("row_number", "group_number", "Группа", "Код"), (2, 1, "A", "001"), (3, 2, "B", "002"), (4, 1, "A", "003")]
    wb.close()


def test_numeric_xlsx_passthrough_keeps_native_type(tmp_path):
    path = tmp_path / "native.xlsx"
    wb = Workbook()
    wb.active.append(["Текст", "Номер", "Флаг"])
    wb.active.append(["Отзыв", 123.0, True])
    wb.save(path)
    wb.close()
    row = next(iter_sheet_rows(path, sheet_name="Sheet", analysis_columns=["Текст"], non_analysis_columns=["Номер", "Флаг"], max_reviews=1))
    values = json.loads(row["passthrough_json"])
    assert values["Номер"] == 123 and isinstance(values["Номер"], (int, float))
    assert values["Флаг"] is True
