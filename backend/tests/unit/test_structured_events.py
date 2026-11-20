import json
import logging
from unittest.mock import Mock

from fastapi import HTTPException
import pytest

from app import events
from app.governance import Scope
from app.logging_utils import (JsonFormatter, ContextFilter, bind_job_context, reset_job_context,
                               set_request_id, reset_request_id, set_user_context, reset_user_context)


@pytest.mark.parametrize("message,secret", [
    ('Authorization: Bearer hidden-test-key', 'hidden-test-key'),
    ('api_key="hidden-test-key"', 'hidden-test-key'), ('token=hidden-test-key', 'hidden-test-key'),
    ('{"content":"скрытый текст отзыва"}', 'скрытый текст отзыва'),
    ('{"prompt_template":"скрытая инструкция"}', 'скрытая инструкция'),
    ("password='hidden-test-key'", 'hidden-test-key'),
])
def test_secrets_and_content_masked(message, secret):
    record = logging.LogRecord('test', logging.ERROR, __file__, 1, message, (), None)
    record.prompt = 'скрытая инструкция'
    record.raw_response = {'content': 'скрытый текст отзыва'}
    value = JsonFormatter().format(record)
    assert secret not in value
    assert 'raw_response' not in value and 'prompt' not in json.loads(value)


def test_exception_and_environment_secret_masked(monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'test-secret-without-key-label')
    try:
        raise ValueError('failure test-secret-without-key-label; content="private-review"')
    except ValueError:
        import sys
        record = logging.LogRecord('test', logging.ERROR, __file__, 1, 'failed', (), sys.exc_info())
    text = JsonFormatter().format(record)
    assert 'test-secret-without-key-label' not in text and 'private-review' not in text
    assert 'ValueError' in text


def test_context_propagates_and_resets():
    trace = set_request_id('test-http-trace')
    user = set_user_context(7, 'test.user')
    job = bind_job_context(report_id='test-report', job_id='test-job', group_id=3, event_type='model.request')
    try:
        record = logging.LogRecord('test', logging.INFO, __file__, 1, 'safe', (), None)
        ContextFilter('inspect-worker').filter(record)
        value = json.loads(JsonFormatter().format(record))
        assert (value['request_id'], value['user_id'], value['username']) == ('test-http-trace', '7', 'test.user')
        assert value['report_id'] == 'test-report' and value['job_id'] == 'test-job'
        assert value['service'] == 'inspect-worker' and value['event_type'] == 'model.request'
    finally:
        reset_job_context(job); reset_user_context(user); reset_request_id(trace)


def test_system_logs_denied_to_manager(monkeypatch):
    monkeypatch.setattr(events, 'require_management', lambda u: Scope(False, (1,)))
    with pytest.raises(HTTPException) as exc:
        events.search({'id': 3})
    assert exc.value.status_code == 403


def test_events_foreign_group_denied(monkeypatch):
    monkeypatch.setattr(events, 'require_management', lambda u: Scope(False, (1,)))
    for fn, kwargs in ((events.search, {'timeline': True}), (events.audit_search, {})):
        with pytest.raises(HTTPException) as exc:
            fn({'id': 3}, group_id=2, **kwargs)
        assert exc.value.status_code == 403


def test_csv_formula_is_text():
    assert "'=cmd" in events.csv_export([{'message': '=cmd'}])


def test_sensitive_request_id_never_enters_context(monkeypatch):
    from app.logging_utils import current_context
    monkeypatch.setenv('APP_SECRET','test-context-secret')
    token=set_request_id('test-context-secret')
    try:
        assert current_context()['request_id'] != 'test-context-secret'
        record=logging.LogRecord('test',logging.INFO,__file__,1,'safe',(),None)
        record.request_id='test-context-secret'
        assert 'test-context-secret' not in JsonFormatter().format(record)
    finally:
        reset_request_id(token)


def test_business_event_uses_originating_service(monkeypatch):
    from app import logging_utils
    monkeypatch.setattr(logging_utils,'_service_name','inspect-worker')
    connection=Mock()
    events.record_event(connection,'file.ready','Проверка файла готова',{'id':'test-report'})
    arguments=connection.execute.call_args[0][1]
    assert arguments[2]=='inspect-worker'
