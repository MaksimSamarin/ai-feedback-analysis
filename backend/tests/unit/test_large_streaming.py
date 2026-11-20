from openpyxl import Workbook
import pytest

from app.services import excel_service as excel
from app.services.csv_input import CsvWorkbook


def test_analysis_xlsx_uses_read_only_and_stops_early(tmp_path, monkeypatch):
    path = tmp_path / 'input.xlsx'
    workbook = Workbook(write_only=True); sheet = workbook.create_sheet('Данные')
    sheet.append(['Текст', 'id'])
    for i in range(200): sheet.append([f'Отзыв {i} 🙂\nстрока', i])
    workbook.save(path)
    load = excel.load_workbook; observed = []
    def checked(*args, **kwargs):
        observed.append(kwargs)
        return load(*args, **kwargs)
    monkeypatch.setattr(excel, 'load_workbook', checked)
    rows = list(excel.iter_sheet_rows(path, sheet_name='Данные', analysis_columns=['Текст'],
                                      non_analysis_columns=['id'], max_reviews=3))
    assert len(rows) == 3 and 'Отзыв 0' in rows[0]['review_text']
    assert observed == [{'read_only': True, 'data_only': True}]


def test_broken_csv_reports_start_of_record_after_multiline(tmp_path):
    path = tmp_path / 'broken.csv'
    path.write_text('Текст,Оценка\n"первая\nмногострочная",5\n"сломанная,4\nконец,3\n', encoding='utf-8')
    with pytest.raises(ValueError, match='строка 4'):
        list(CsvWorkbook(path).iter_rows())


def test_export_splits_results_and_errors_without_loss(tmp_path, monkeypatch):
    from openpyxl import load_workbook
    monkeypatch.setattr(excel, 'EXCEL_MAX_ROWS', 4)
    rows = [{'row_number': i+2, 'status': 'error', 'custom_json': {'result': 'value'}, 'error_text': 'test error'} for i in range(7)]
    path = tmp_path / 'split.xlsx'
    excel.export_results_xlsx(path, lambda: iter(rows), {'total_rows': 7})
    workbook = load_workbook(path, read_only=True)
    for prefix in ['results', 'Ошибки']:
        sheets = [s for s in workbook if s.title.startswith(prefix)]
        assert len(sheets) == 3 and sum(s.max_row - 1 for s in sheets) == 7
        assert all(s.max_row <= 4 for s in sheets)
    workbook.close()


def test_group_source_splits_and_keeps_group_number(tmp_path, monkeypatch):
    from openpyxl import load_workbook
    monkeypatch.setattr(excel, 'EXCEL_MAX_ROWS', 4)
    rows = [{'row_number': i+2, 'status': 'done', 'group_key': 'A', 'custom_json': {'result': 'value'},
             'passthrough_json': {'Группа': 'A', 'id': i}} for i in range(7)]
    path = tmp_path / 'source.xlsx'
    excel.export_results_xlsx(path, lambda: iter(rows), {'total_rows': 7}, group_by_column='Группа', source_columns=['id'])
    workbook = load_workbook(path, read_only=True)
    assert workbook['results'].max_row == 2
    source = [s for s in workbook if s.title.startswith('Исходные данные')]
    values = [r for s in source for r in list(s.values)[1:]]
    assert len(values) == 7 and all(r[1] == 1 for r in values)
    workbook.close()
