from .reporting_reference import _metrics as reference_metrics
from datetime import datetime
from io import BytesIO

from fastapi import HTTPException
from openpyxl import load_workbook
import pytest

from app import reporting
from app.governance import Scope


@pytest.mark.parametrize("preset,date,start,end,previous", [
    ('month', '2026-12-31T21:00:00+00:00', '2027-01-01', '2027-02-01', '2026-12-01'),
    ('previous', '2026-11-08T00:00:00+00:00', '2026-10-01', '2026-11-01', '2026-09-01'),
    ('quarter', '2026-11-08T00:00:00+00:00', '2026-10-01', '2027-01-01', '2026-07-01'),
])
def test_period_moscow(preset, date, start, end, previous):
    bounds = reporting.period_bounds(preset, now=datetime.fromisoformat(date))
    assert tuple(v.date().isoformat() for v in bounds[:3]) == (start, end, previous)
    assert bounds[0].utcoffset().total_seconds() == 10800


def test_custom_previous_same_duration():
    start, end, previous, _ = reporting.period_bounds('custom', '2026-11-01', '2026-11-08')
    assert end - start == start - previous


@pytest.mark.parametrize('start,end', [(None, None), ('bad', 'bad'), ('2026-11-08', '2026-11-01')])
def test_invalid_custom(start, end):
    with pytest.raises(HTTPException): reporting.period_bounds('custom', start, end)


def test_foreign_group_denied_before_data(monkeypatch):
    monkeypatch.setattr(reporting, 'require_management', lambda u: Scope(False, (1,)))
    with pytest.raises(HTTPException) as exc: reporting.get_report({'id': 3}, group_id=2)
    assert exc.value.status_code == 403


def test_metrics_keep_deleted_and_unknown():
    rows = [dict(total_tokens=100, processed_rows=10, failed_rows=2, status='completed', user_id=1, deleted_at='date'),
            dict(total_tokens=200, processed_rows=20, failed_rows=None, status='failed', user_id=2)]
    start = datetime.fromisoformat('2026-11-01T00:00:00+03:00')
    end = datetime.fromisoformat('2026-12-01T00:00:00+03:00')
    m = reference_metrics(rows, start, end, start + (end-start)/2)
    assert (m['total_tokens'], m['processed_rows'], m['error_reports'], m['error_rows']) == (300, 30, 2, 2)
    assert m['forecast'] is None and m['unknown_row_metrics'] == 1
    assert m['error_row_percent'] == 20 and m['error_report_percent'] == 100


def test_limit_stop_summary_survives_event_retention():
    start=datetime.fromisoformat('2026-11-01T00:00:00+03:00')
    end=reporting.shift_month(start,1)
    row=dict(total_tokens=100,processed_rows=10,failed_rows=0,status='completed',user_id=1,
             deleted_at='2026-11-08',limit_pause_count=3)
    assert reference_metrics([row],start,end,end)['limit_stops']==3


def test_zero_comparison():
    assert reporting.compare(50, 0) == {'delta': 50, 'percent': None}


@pytest.mark.parametrize('value,expected',[
    ('2026-11-01','2026-10-31T21:00:00+00:00'),
    ('2026-11-01T00:00:00+03:00','2026-10-31T21:00:00+00:00'),
    ('2026-10-31T21:00:00Z','2026-10-31T21:00:00+00:00'),
    ('2027-01-01T00:00:00','2026-12-31T21:00:00+00:00'),
])
def test_external_period_boundaries_use_moscow(value,expected):
    assert reporting.utc_bound(value)==expected


def test_invalid_external_boundary():
    with pytest.raises(HTTPException) as exc:reporting.utc_bound('invalid')
    assert exc.value.status_code==422


def test_business_export_requires_calendar_month():
    data={'period':{'from':'2026-10-01T00:00:00+03:00','to':'2027-01-01T00:00:00+03:00'}}
    with pytest.raises(HTTPException) as exc:reporting.xlsx_report(data,'business')
    assert exc.value.status_code==422


def test_xlsx_matches_summary_and_prevents_formula():
    data = {'period': {'from': '2026-11-01', 'to': '2026-12-01'}, 'reliable_since': None,
            'summary': {'total_tokens': 300, 'unknown_row_metrics': 1}, 'users': [{'username': '=1+1', 'total_tokens': 300}]}
    workbook = load_workbook(BytesIO(reporting.xlsx_report(data, 'users')), read_only=True)
    assert list(workbook['Отчёт'].values) == [('Логин', 'total_tokens', 'ФИО', 'Должность', 'Компания'), ("'=1+1", 300)]
    workbook.close()
    with pytest.raises(HTTPException) as exc: reporting.xlsx_report(data, 'models')
    assert exc.value.status_code == 403
