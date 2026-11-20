from .reporting_reference import _metrics as reference_metrics
from contextlib import contextmanager
from datetime import datetime
from io import BytesIO
import csv
import io
from unittest.mock import Mock

import pytest
from openpyxl import load_workbook

from app import audit_view,events,reporting
from app.governance import Scope
from app.report_outcome import presentation
from app.services.excel_service import export_results_xlsx


def test_extension_denied_does_not_abort_schema_or_search(monkeypatch,caplog):
    class Connection:
        aborted=False
        queries=[]
        def execute(self,sql,args=()):
            self.queries.append(sql)
            if sql.startswith('ROLLBACK TO SAVEPOINT'):self.aborted=False
            elif self.aborted:raise RuntimeError('transaction aborted')
            if sql.startswith('CREATE EXTENSION'):
                self.aborted=True;raise PermissionError('permission denied')
            if 'COUNT(*)' in sql:return Mock(fetchone=lambda:{'n':1})
            return Mock(fetchall=lambda:[{'id':1,'message':'проверка'}])
    conn=Connection();events.init_schema(conn)
    assert not conn.aborted and 'без ускорения' in caplog.text
    assert not any('gin_trgm_ops' in sql for sql in conn.queries)
    @contextmanager
    def connection():yield conn
    monkeypatch.setattr(events.db,'get_conn',connection)
    monkeypatch.setattr(events,'require_management',lambda u:Scope(True,()))
    assert events.search({'id':1},q='проверка')['total']==1


@pytest.mark.parametrize('field,item,expected',[
    ('decision_comment',None,'—'),('description',None,'—'),('token_limit',None,'без лимита'),
    ('personal_token_limit',1000,'1 000'),('status','completed','Готов'),('status','failed','Ошибка'),
])
def test_audit_formats_values_by_field(field,item,expected):assert audit_view.value(field,item)==expected


def test_audit_delete_and_members_have_concrete_descriptions():
    row={'action':'report.delete','target_type':'report','report_name':'Доставка','before_json':{'status':'completed'},'after_json':{}}
    result=audit_view.describe(row)
    assert result['object_label']=='Отчёт «Доставка»' and 'Готов → Удалён' in result['description']
    result=audit_view.describe({'action':'member.remove','target_type':'member','target_username':'user.test','group_name':'Розница'})
    assert result['description']=='Исключён user.test'
    result=audit_view.describe({'action':'member.role','target_type':'member','target_username':'user.test',
        'group_name':'Розница','before_json':{'role_in_group':'member'},'after_json':{'role_in_group':'manager'}})
    assert result['description']=='Роль user.test: Пользователь → Руководитель'
    assert result['object_label']=='Участник «user.test» · группа «Розница»'


def metric_row(date,tokens=100):
    return dict(created_at=date,total_tokens=tokens,processed_rows=10,failed_rows=0,status='completed',user_id=1)


def test_forecast_requires_three_complete_data_days():
    now=datetime.fromisoformat('2026-11-08T12:00:00+03:00')
    start,end,*_=reporting.period_bounds(now=now)
    rows=[metric_row('2026-11-06T00:00:00+03:00'),metric_row('2026-11-07T00:00:00+03:00')]
    assert reference_metrics(rows,start,end,now)['forecast'] is None
    rows.append(metric_row('2026-11-05T00:00:00+03:00'))
    result=reference_metrics(rows,start,end,now)
    assert result['forecast']==2500 and result['forecast_is_estimate'] is True


def test_today_and_old_days_do_not_establish_forecast():
    now=datetime.fromisoformat('2026-11-08T12:00:00+03:00');start,end,*_=reporting.period_bounds(now=now)
    rows=[metric_row('2026-10-31T00:00:00+03:00'),metric_row('2026-11-07T00:00:00+03:00'),metric_row(now.isoformat())]
    assert reference_metrics(rows,start,end,now)['forecast'] is None


@pytest.mark.parametrize('processed,failed,label,all_failed',[(40,40,'Ошибка: 40 из 40',True),(40,3,'Готов, ошибок: 3',False),(40,0,'Готов',False)])
def test_completed_outcome_distinguishes_failure(processed,failed,label,all_failed):
    result=presentation('completed',processed,failed)
    assert result['status_label']==label and result['all_rows_failed']==all_failed
    if failed:assert 'время ожидания' in result['retry_hint']


@pytest.mark.parametrize('error',[None,'None','null',''])
def test_success_xlsx_has_blank_error_and_no_none_warnings(error):
    target=BytesIO();rows=[{'status':'done','row_number':2,'custom_json':{'Итог':'успешно'},'error_text':error,
                          'warnings_json':'[null,"empty_cell"]'}]
    export_results_xlsx(target,lambda:iter(rows),{'total_rows':1})
    workbook=load_workbook(BytesIO(target.getvalue()),read_only=True,data_only=True)
    values=list(workbook['results'].values);header=values[0];record=values[1]
    assert record[header.index('error')] is None
    assert record[header.index('warnings')]=='empty_cell' and 'Ошибки' not in workbook.sheetnames
    workbook.close()


def test_csv_null_diagnostics_are_empty_and_formula_is_escaped():
    exported=events.csv_export([{'message':'=1+1','traceback':None,'username':None}])
    record=list(csv.DictReader(io.StringIO(exported.lstrip('\ufeff'))))[0]
    assert record['traceback']==record['username']=='' and record['message']=="'=1+1"


def test_default_log_filter_keeps_http_failures_and_excludes_debug(monkeypatch):
    statements=[]
    class Connection:
        def execute(self,sql,args):
            statements.append((sql,args))
            return Mock(fetchone=lambda:{'n':0},fetchall=lambda:[])
    @contextmanager
    def connection():yield Connection()
    monkeypatch.setattr(events.db,'get_conn',connection)
    monkeypatch.setattr(events,'require_management',lambda u:Scope(True,()))
    events.search({'id':1})
    assert 'COALESCE(e.http_status,500)<400' in statements[0][0]
    assert 'DEBUG' not in statements[0][1] and 'ERROR' in statements[0][1]
    statements.clear();events.search({'id':1},include_http=True,levels='DEBUG')
    assert 'http_status' not in statements[0][0] and 'DEBUG' in statements[0][1]
