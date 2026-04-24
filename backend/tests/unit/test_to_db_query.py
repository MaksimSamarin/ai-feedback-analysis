"""Юнит-тесты для `_to_db_query` — замена плейсхолдеров с учётом контекста (BUG-10).

После фикса BUG-10 функция использует regex-парсинг SQL: `?` внутри строковых
литералов, кавычек-идентификаторов и комментариев **не** заменяется — только
отдельные плейсхолдеры параметров.

Запуск:
    cd backend && pytest tests/unit/test_to_db_query.py -v
"""

from __future__ import annotations


def test_single_placeholder_replaced() -> None:
    from app.db import _to_db_query

    assert _to_db_query("SELECT * FROM t WHERE id = ?") == "SELECT * FROM t WHERE id = %s"


def test_multiple_placeholders_replaced() -> None:
    from app.db import _to_db_query

    result = _to_db_query("SELECT * FROM t WHERE a = ? AND b = ? AND c = ?")
    assert result == "SELECT * FROM t WHERE a = %s AND b = %s AND c = %s"


def test_no_placeholders_returns_unchanged() -> None:
    from app.db import _to_db_query

    assert _to_db_query("SELECT COUNT(*) FROM t") == "SELECT COUNT(*) FROM t"


def test_question_mark_in_single_quoted_string_preserved() -> None:
    """Регрессия BUG-10: `?` внутри строкового литерала не должен заменяться."""
    from app.db import _to_db_query

    sql = "SELECT * FROM reports WHERE title LIKE '%wtf?%' AND id = ?"
    result = _to_db_query(sql)
    assert "wtf?" in result, "`?` внутри строки превратился в %s — регрессия BUG-10"
    assert result == "SELECT * FROM reports WHERE title LIKE '%wtf?%' AND id = %s"


def test_question_mark_in_json_literal_preserved() -> None:
    """Регрессия BUG-10: `?` внутри JSON-литерала не должен заменяться."""
    from app.db import _to_db_query

    sql = "SELECT * FROM t WHERE config @> '{\"q\":\"?\"}'::jsonb"
    result = _to_db_query(sql)
    assert "\"?\"" in result, "`?` в JSON-литерале сломался — регрессия BUG-10"


def test_question_mark_in_comment_preserved() -> None:
    """Регрессия BUG-10: `?` в комментарии не должен заменяться."""
    from app.db import _to_db_query

    sql = "-- какой-то вопрос?\nSELECT id FROM t WHERE x = ?"
    result = _to_db_query(sql)
    assert "вопрос?" in result, "`?` в комментарии превратился в %s — регрессия BUG-10"
    # А реальный плейсхолдер всё равно заменён
    assert result.endswith("WHERE x = %s")


def test_question_mark_in_multiline_comment_preserved() -> None:
    """Регрессия BUG-10: `?` в /* ... */ комментарии не должен заменяться."""
    from app.db import _to_db_query

    sql = "/* ok? */ SELECT ?"
    result = _to_db_query(sql)
    assert "ok?" in result
    assert result.endswith("SELECT %s")


def test_question_mark_in_double_quoted_identifier_preserved() -> None:
    """Регрессия BUG-10: `?` в двойных кавычках (Postgres идентификатор) не заменяется."""
    from app.db import _to_db_query

    sql = 'SELECT * FROM "strange?col" WHERE id = ?'
    result = _to_db_query(sql)
    assert '"strange?col"' in result
    assert result == 'SELECT * FROM "strange?col" WHERE id = %s'


def test_escaped_single_quote_in_string() -> None:
    """SQL-escape одинарной кавычки (`''`) не должен ломать парсер."""
    from app.db import _to_db_query

    sql = "INSERT INTO t VALUES ('it''s a test?')"
    result = _to_db_query(sql)
    # `?` внутри строки сохраняется; плейсхолдеров снаружи нет
    assert result == "INSERT INTO t VALUES ('it''s a test?')"


def test_mixed_literal_and_placeholder() -> None:
    """Смесь литерала с `?` и настоящих плейсхолдеров — сложный кейс."""
    from app.db import _to_db_query

    sql = "UPDATE t SET q = 'what?' WHERE id = ? AND note = 'why?' RETURNING id"
    result = _to_db_query(sql)
    # Оба литерала сохранились
    assert "'what?'" in result
    assert "'why?'" in result
    # Плейсхолдер ровно один
    assert result.count("%s") == 1
    # Итоговая строка
    assert result == "UPDATE t SET q = 'what?' WHERE id = %s AND note = 'why?' RETURNING id"
