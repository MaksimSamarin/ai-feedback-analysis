import io
import json
from openpyxl import load_workbook
import pytest
from pydantic import ValidationError

from app import db
from app.schemas import StartJobRequest, BulkDeleteRequest
from app.services.excel_service import export_results_xlsx


def test_integral_float_has_no_decimal_suffix():
    target = io.BytesIO()
    rows = [{"row_number": 2, "status": "done", "analysis_json": {"Целое": 123.0, "Дробное": 123.45}}]
    export_results_xlsx(target, lambda: iter(rows), {"total_rows": 1})
    wb = load_workbook(io.BytesIO(target.getvalue()), read_only=True)
    values = list(wb["results"].values)
    whole = values[1][values[0].index("Целое")]
    fraction = values[1][values[0].index("Дробное")]
    assert whole == 123 and isinstance(whole, int)
    assert fraction == 123.45
    wb.close()


def test_name_validation():
    data = dict(file_id="file", sheet_name="Данные", analysis_columns=["Текст"], provider="openai",
                model="mock-model", prompt_template="Тест")
    assert StartJobRequest(**data, name="Отчёт за ноябрь").name == "Отчёт за ноябрь"
    with pytest.raises(ValidationError):
        StartJobRequest(**data, name="a" * 201)
    with pytest.raises(ValidationError):
        BulkDeleteRequest(ids=[])


def test_bulk_deletion_partial_failures(monkeypatch):
    records = {"done": {"status": "completed"}, "active": {"status": "running"}, "broken": {"status": "failed"}}
    calls = []
    def lookup(rid, uid):
        assert uid == 5
        return records.get(rid)
    def delete(rid, uid):
        calls.append((rid, uid))
        if rid == "broken":
            raise OSError("Недоступен диск")
        return True
    monkeypatch.setattr(db, "get_report", lookup)
    monkeypatch.setattr(db, "delete_report", delete)
    result = db.bulk_delete_reports(5, ["done", "active", "foreign", "broken", "done"])
    assert result["deleted"] == 1 and result["failed"] == 3
    assert calls == [("done", 5), ("broken", 5)]
    assert [r["code"] for r in result["results"] if r["status"] == "error"] == ["active_report", "not_found", "delete_failed"]
