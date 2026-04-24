"""Юнит-тесты формы Pydantic-моделей после итерации 1.

Проверяют регрессию: `JobSummary`, `PreviewRow`, `ReportAnalysisResponse` —
не содержат выпиленных полей и умеют собираться на корректных данных.

Запуск:
    cd backend && pytest tests/unit/test_schema_shapes.py -v
"""

from __future__ import annotations

import pytest


def test_job_summary_has_only_four_technical_fields() -> None:
    """JobSummary после итерации 1 — только total/processed/success/failed."""
    from app.schemas import JobSummary

    fields = set(JobSummary.model_fields.keys())
    assert fields == {"total_rows", "processed_rows", "success_rows", "failed_rows"}


def test_job_summary_rejects_missing_required_fields() -> None:
    """Все 4 поля обязательны — без них Pydantic должен упасть."""
    from app.schemas import JobSummary

    with pytest.raises(Exception):
        JobSummary(total_rows=1)  # не хватает processed_rows / success_rows / failed_rows


def test_job_summary_accepts_valid_payload() -> None:
    from app.schemas import JobSummary

    model = JobSummary(total_rows=10, processed_rows=10, success_rows=9, failed_rows=1)
    assert model.total_rows == 10
    assert model.success_rows == 9


def test_preview_row_exists_and_has_expected_fields() -> None:
    """PreviewRow — новая модель с row_number/columns/warnings/error."""
    from app.schemas import PreviewRow

    fields = set(PreviewRow.model_fields.keys())
    assert fields == {"row_number", "columns", "warnings", "error"}


def test_preview_row_columns_is_arbitrary_dict() -> None:
    """columns — dict[str, Any] с произвольными ключами (русские, числа, вложенность)."""
    from app.schemas import PreviewRow

    model = PreviewRow(
        row_number=1,
        columns={"тональность": "негатив", "срочность": 9, "флаг": True},
        warnings=[],
        error=None,
    )
    assert model.columns["тональность"] == "негатив"
    assert model.columns["срочность"] == 9
    assert model.columns["флаг"] is True


def test_preview_row_empty_columns_default() -> None:
    """columns по умолчанию — пустой dict, warnings — пустой list."""
    from app.schemas import PreviewRow

    model = PreviewRow(row_number=1)
    assert model.columns == {}
    assert model.warnings == []
    assert model.error is None


def test_report_analysis_response_uses_preview_rows_not_top_negative() -> None:
    """ReportAnalysisResponse.preview_rows существует, top_negative удалено."""
    from app.schemas import ReportAnalysisResponse

    fields = set(ReportAnalysisResponse.model_fields.keys())
    assert "preview_rows" in fields
    assert "top_negative" not in fields


def test_top_negative_item_removed_from_schemas() -> None:
    """TopNegativeItem выпилен из schemas.py."""
    import app.schemas as schemas_module

    assert not hasattr(schemas_module, "TopNegativeItem"), (
        "TopNegativeItem должна быть удалена (заменена на PreviewRow)."
    )


def test_job_result_uses_preview_rows() -> None:
    """JobResult тоже перешёл на preview_rows (в in-memory job state)."""
    from app.schemas import JobResult

    fields = set(JobResult.model_fields.keys())
    assert "preview_rows" in fields
    assert "top_negative" not in fields
