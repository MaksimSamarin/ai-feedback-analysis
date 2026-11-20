"""Юнит-тесты на инкремент reports.tokens в bulk_update_report_rows_same_result.

Покрытие:
- hotfix/token-counters (BUG-27 fix): в группововом режиме воркер теперь обновляет
  материализованные счётчики reports.{prompt,completion,total}_tokens после записи
  строк. До hotfix этого не было — получали нули в reports, и get_user_usage был
  вынужден агрегировать через SUM по report_rows (что на 9M+ строк зависало).

Связанные:
- update_report_row_result (row-mode) уже имел эту логику.
- test_user_usage.py — теперь читает из reports, не из report_rows.
- test_report_tokens_batch.py — теперь читает из reports по PK.

Запуск:
    cd backend && pytest tests/unit/test_bulk_update_reports_counter.py -v
"""
from __future__ import annotations


class _FakeRow(dict):
    def __getitem__(self, key):
        return super().__getitem__(key)


class _FakeResult:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    """Стаб connection с настраиваемым ответом на SELECT prev tokens.

    captures: список (sql_normalized, params) для всех execute-вызовов.
    prev_row: что вернуть на SELECT prompt_tokens FROM report_rows.
    """

    def __init__(self, captures, prev_row=None):
        self._captures = captures
        self._prev_row = prev_row

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self._captures.append((normalized, tuple(params or ())))
        if normalized.startswith("SELECT prompt_tokens, completion_tokens, total_tokens FROM report_rows"):
            return _FakeResult(self._prev_row)
        return _FakeResult(None)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _patch_conn(monkeypatch, captures, prev_row=None):
    import app.db as db_mod
    monkeypatch.setattr(db_mod, "get_conn", lambda: _FakeConn(captures, prev_row))


def _find_sql(captures, prefix):
    """Найти первый execute, чей SQL начинается с prefix."""
    for sql, params in captures:
        if sql.startswith(prefix):
            return sql, params
    return None, None


def test_group_mode_increments_reports_counter_on_first_write(monkeypatch) -> None:
    """Первая запись группы (prev_tokens = 0) → UPDATE reports SET tokens += полная сумма."""
    import app.db as db_mod

    captures: list = []
    # prev_row=None означает что первая строка группы ещё не имеет токенов.
    _patch_conn(monkeypatch, captures, prev_row=None)

    db_mod.bulk_update_report_rows_same_result(
        report_id="r1",
        row_numbers=[10, 11, 12, 13, 14],
        sentiment_label="positive",
        negativity_score=0.2,
        short_reason="ok",
        category="quality",
        raw_response=None,
        error_text=None,
        custom_data={"x": 1},
        total_prompt_tokens=5000,
        total_completion_tokens=200,
        total_total_tokens=5200,
    )

    sql, params = _find_sql(captures, "UPDATE reports")
    assert sql is not None, "Должен быть UPDATE reports SET tokens += delta"
    assert "prompt_tokens = COALESCE(prompt_tokens, 0) + ?" in sql
    assert "completion_tokens = COALESCE(completion_tokens, 0) + ?" in sql
    assert "total_tokens = COALESCE(total_tokens, 0) + ?" in sql
    assert "WHERE id = ?" in sql
    delta_prompt, delta_completion, delta_total, _now, rid = params
    assert (delta_prompt, delta_completion, delta_total, rid) == (5000, 200, 5200, "r1")


def test_group_mode_retry_uses_delta(monkeypatch) -> None:
    """При retry (prev_tokens != 0) → UPDATE reports SET tokens += (new - prev), не двойной счёт."""
    import app.db as db_mod

    captures: list = []
    _patch_conn(
        monkeypatch,
        captures,
        prev_row=_FakeRow(prompt_tokens=1000, completion_tokens=50, total_tokens=1050),
    )

    db_mod.bulk_update_report_rows_same_result(
        report_id="r1",
        row_numbers=[10, 11],
        sentiment_label="positive",
        negativity_score=0.1,
        short_reason="retry",
        category=None,
        raw_response=None,
        error_text=None,
        custom_data=None,
        total_prompt_tokens=1500,
        total_completion_tokens=80,
        total_total_tokens=1580,
    )

    sql, params = _find_sql(captures, "UPDATE reports")
    assert sql is not None
    delta_prompt, delta_completion, delta_total, _now, rid = params
    assert (delta_prompt, delta_completion, delta_total, rid) == (500, 30, 530, "r1"), \
        "Дельта = new - prev (1500-1000=500, 80-50=30, 1580-1050=530)"


def test_group_mode_zero_tokens_updates_only_export_revision(monkeypatch) -> None:
    """Нулевой расход не меняет биллинг, но результат меняет версию выгрузки."""
    import app.db as db_mod

    captures: list = []
    _patch_conn(monkeypatch, captures, prev_row=None)

    db_mod.bulk_update_report_rows_same_result(
        report_id="r1",
        row_numbers=[10, 11],
        sentiment_label="positive",
        negativity_score=0.1,
        short_reason="cache hit",
        category=None,
        raw_response=None,
        error_text=None,
        custom_data=None,
        total_prompt_tokens=0,
        total_completion_tokens=0,
        total_total_tokens=0,
    )

    update_reports_calls = [c for c in captures if c[0].startswith("UPDATE reports")]
    assert len(update_reports_calls)==1 and 'SET results_revision=results_revision+1' in update_reports_calls[0][0]
    assert 'prompt_tokens' not in update_reports_calls[0][0]


def test_skip_group_updates_only_export_revision(monkeypatch) -> None:
    """Скип меняет экспорт, не меняя токены и не читая прежний расход."""
    import app.db as db_mod

    captures: list = []
    _patch_conn(monkeypatch, captures, prev_row=None)

    db_mod.bulk_update_report_rows_same_result(
        report_id="r1",
        row_numbers=[10, 11],
        sentiment_label=None,
        negativity_score=None,
        short_reason=None,
        category=None,
        raw_response=None,
        error_text="Пропущено: лимит",
        custom_data=None,
        # total_prompt_tokens / completion / total — по умолчанию 0
    )

    update_reports_calls = [c for c in captures if c[0].startswith("UPDATE reports")]
    select_prev_calls = [c for c in captures if c[0].startswith("SELECT prompt_tokens")]
    assert len(update_reports_calls)==1 and 'SET results_revision=results_revision+1' in update_reports_calls[0][0]
    assert 'prompt_tokens' not in update_reports_calls[0][0]
    assert select_prev_calls == [], "skip_group не должен читать prev tokens"


def test_group_mode_empty_row_numbers_does_nothing(monkeypatch) -> None:
    """Пустой список row_numbers → ни одного SQL вызова, функция возвращает None без падений."""
    import app.db as db_mod

    captures: list = []
    _patch_conn(monkeypatch, captures, prev_row=None)

    result = db_mod.bulk_update_report_rows_same_result(
        report_id="r1",
        row_numbers=[],
        sentiment_label=None,
        negativity_score=None,
        short_reason=None,
        category=None,
        raw_response=None,
        error_text=None,
        custom_data=None,
        total_prompt_tokens=100,
        total_completion_tokens=10,
        total_total_tokens=110,
    )

    assert result is None
    assert captures == []
