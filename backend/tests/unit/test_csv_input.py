import json
from pathlib import Path

import pytest

from app.services import excel_service as excel


@pytest.mark.parametrize("encoding,delimiter", [("utf-8", ","), ("utf-8-sig", ","), ("cp1251", ";"), ("utf-8", "\t")])
def test_csv_encodings_and_pipeline(tmp_path, encoding, delimiter):
    path = tmp_path / "reviews.csv"
    path.write_text(delimiter.join(["Текст", "Код"]) + "\n" + delimiter.join(["Хорошо", "123"]) + "\n", encoding=encoding)
    info = excel.inspect_file(path)[0]
    assert info["columns"] == ["Текст", "Код"] and info["total_rows"] == 1
    rows = excel.read_sheet_rows(path, sheet_name=info["name"], analysis_columns=["Текст"], non_analysis_columns=["Код"], max_reviews=100)
    assert json.loads(rows[0]["input_json"]) == {"Текст": "Хорошо"}
    assert json.loads(rows[0]["passthrough_json"]) == {"Код": "123"}


def test_csv_quotes_and_newlines(tmp_path):
    path = tmp_path / "quoted.csv"
    path.write_text('Текст,Код\n"Первая, часть\nВторая ""часть""",7\n', encoding="utf-8", newline="\n")
    info = excel.inspect_file(path)[0]
    assert info["total_rows"] == 1
    row = next(excel.iter_sheet_rows(path, sheet_name=info["name"], analysis_columns=["Текст"], max_reviews=1))
    assert 'Первая, часть\nВторая "часть"' in row["review_text"]


@pytest.mark.parametrize("content", [b"", b" \n\n", b"A,B\n\x00,2", b'A,B\n"unclosed,2', b"A,A\n1,2", b"A,B\n1,2,3"])
def test_bad_csv_has_readable_error(tmp_path, content):
    path = tmp_path / "bad.csv"
    path.write_bytes(content)
    with pytest.raises(ValueError, match="CSV"):
        excel.inspect_file(path)


def test_dispatch_xlsx_unchanged(monkeypatch):
    seen = []
    monkeypatch.setattr(excel, "inspect_xlsx", lambda p, **kw: seen.append(p) or [])
    assert excel.inspect_file(Path("test.xlsx")) == []
    assert seen == [Path("test.xlsx")]
    with pytest.raises(ValueError):
        excel.inspect_file(Path("test.txt"))


def test_csv_max_rows_and_unique_cap(tmp_path):
    path = tmp_path / "large.csv"
    path.write_text("Текст\n" + "\n".join(str(i) for i in range(10000)), encoding="utf-8")
    info = excel.inspect_file(path, unique_cap=100)[0]
    assert info["total_rows"] == 10000 and info["unique_counts"]["Текст"] is None
    rows = excel.iter_sheet_rows(path, sheet_name=info["name"], analysis_columns=["Текст"], max_reviews=5)
    assert len(list(rows)) == 5


def test_large_csv_field_and_late_binary_data(tmp_path):
    path = tmp_path / "long.csv"
    path.write_text("Текст\n" + "x" * 200000 + "\n", encoding="utf-8")
    assert excel.inspect_file(path)[0]["total_rows"] == 1
    path.write_bytes(b"Text\n" + b"x" * 70000 + b"\x00\n")
    with pytest.raises(ValueError, match="CSV"):
        excel.inspect_file(path)
