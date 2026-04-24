"""Юнит-тесты для `build_report_analysis` — основной сборщик ответа
на `/api/reports/{id}/analysis` (итерация 1 отказа от обязательных полей).

Проверяют что функция:
1. Возвращает упрощённую сводку (total/processed/success/failed)
2. Не содержит sentiment-полей (регрессия)
3. Превью — первые `preview_limit` строк (по умолчанию 10)
4. Превью динамически строится из того что LLM вернул, без core-полей

Запуск:
    cd backend && pytest tests/unit/test_build_report_analysis.py -v
"""

from __future__ import annotations

import json

import pytest


def _stub_db(
    monkeypatch: pytest.MonkeyPatch,
    *,
    summary: dict,
    rows: list[dict],
    group_by_column: str | None = None,
) -> None:
    """Стабит зависимости build_report_analysis в пространстве app.db."""
    monkeypatch.setattr("app.db.get_report_summary_agg", lambda _rid: summary)

    def _fake_iter(_rid, *, batch_size=2000):
        for row in rows:
            yield row

    monkeypatch.setattr("app.db.iter_report_rows", _fake_iter)

    # build_report_analysis читает reports.group_by_column через get_conn —
    # мокаем чтобы вернуть нужное значение (по умолчанию None, без группировки).
    class _FakeReport:
        def __init__(self, gbc: str | None) -> None:
            self._gbc = gbc

        def get(self, key, default=None):
            return {"group_by_column": self._gbc}.get(key, default)

    class _FakeConn:
        def __init__(self, gbc: str | None) -> None:
            self._gbc = gbc

        def execute(self, sql: str, params=None):
            gbc = self._gbc
            class R:
                def fetchone(self_inner):
                    return _FakeReport(gbc)
            return R()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr("app.db.get_conn", lambda: _FakeConn(group_by_column))


def test_summary_has_four_technical_fields_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Сводка содержит ровно 4 поля: total/processed/success/failed. Ничего больше."""
    from app.db import build_report_analysis

    _stub_db(
        monkeypatch,
        summary={"total_rows": 100, "success_rows": 95, "failed_rows": 5},
        rows=[],
    )

    summary, _ = build_report_analysis("rep-1")
    assert set(summary.keys()) == {"total_rows", "processed_rows", "success_rows", "failed_rows"}
    assert summary["total_rows"] == 100
    assert summary["processed_rows"] == 100
    assert summary["success_rows"] == 95
    assert summary["failed_rows"] == 5


def test_no_sentiment_fields_in_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Регрессия: sentiment_counts/avg_negativity_score/sentiment_percentages не возвращаются."""
    from app.db import build_report_analysis

    _stub_db(
        monkeypatch,
        summary={"total_rows": 5, "success_rows": 5, "failed_rows": 0},
        rows=[],
    )

    summary, _ = build_report_analysis("rep-1")
    forbidden = {
        "avg_negativity_score",
        "sentiment_counts",
        "sentiment_percentages",
        "negative_count",
        "neutral_count",
        "positive_count",
    }
    assert not (forbidden & set(summary.keys()))


def test_preview_limit_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Превью не больше preview_limit, даже если строк больше."""
    from app.db import build_report_analysis

    rows = [{"row_number": i, "custom_json": json.dumps({"field": f"v{i}"})} for i in range(1, 21)]
    _stub_db(
        monkeypatch,
        summary={"total_rows": 20, "success_rows": 20, "failed_rows": 0},
        rows=rows,
    )

    _, preview = build_report_analysis("rep-1", preview_limit=10)
    assert len(preview) == 10
    assert preview[0]["row_number"] == 1
    assert preview[9]["row_number"] == 10


def test_preview_handles_empty_report(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пустой отчёт — пустой preview, без ошибки."""
    from app.db import build_report_analysis

    _stub_db(
        monkeypatch,
        summary={"total_rows": 0, "success_rows": 0, "failed_rows": 0},
        rows=[],
    )

    summary, preview = build_report_analysis("rep-empty")
    assert summary["total_rows"] == 0
    assert preview == []


def test_preview_contains_dynamic_columns_from_arbitrary_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пользовательская схема без core-полей — превью строится из того что есть."""
    from app.db import build_report_analysis

    rows = [
        {
            "row_number": 1,
            "input_json": json.dumps({"сообщение": "опоздание на 2 дня"}),
            "custom_json": json.dumps(
                {"тональность": "негатив", "срочность": 9, "требует_эскалации": True}
            ),
        }
    ]
    _stub_db(
        monkeypatch,
        summary={"total_rows": 1, "success_rows": 1, "failed_rows": 0},
        rows=rows,
    )

    _, preview = build_report_analysis("rep-1")
    columns = preview[0]["columns"]
    assert columns["сообщение"] == "опоздание на 2 дня"
    assert columns["тональность"] == "негатив"
    assert columns["срочность"] == 9
    assert columns["требует_эскалации"] is True
    # Core-поля в превью отсутствуют — схема их не содержала
    assert "summary" not in columns
    assert "category" not in columns
    assert "confidence" not in columns
