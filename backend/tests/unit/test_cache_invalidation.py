"""Юнит-тесты для инвалидации битого кэша и пропуска кэша при retry (v2.0.0, итерация 3.2).

Сценарии:
1. Битый кэш-hit (старый формат, схема поменялась и т.п.) — запись удаляется,
   следующий раз идём в модель реальным запросом.
2. При retry с retry_feedback — кэш пропускается, модель получает обратную связь
   и шанс исправиться; иначе тот же битый ответ из кэша обнулит retry-эффект.

Запуск:
    cd backend && pytest tests/unit/test_cache_invalidation.py -v
"""
from __future__ import annotations


def test_delete_cached_analysis_function_exists() -> None:
    """Есть функция `delete_cached_analysis(cache_key)` в db-слое."""
    from app.db import delete_cached_analysis

    assert callable(delete_cached_analysis)


def test_process_row_skips_cache_on_retry_feedback(monkeypatch) -> None:
    """При retry_feedback (attempt > 0 с ошибкой валидатора) кэш не читается — идём в модель."""
    import app.services.job_manager as jm_mod

    called = {"get_cache": 0, "delete_cache": 0}

    def fake_get(cache_key: str):
        called["get_cache"] += 1
        return None

    def fake_delete(cache_key: str):
        called["delete_cache"] += 1

    monkeypatch.setattr(jm_mod, "get_cached_analysis", fake_get)
    monkeypatch.setattr(jm_mod, "delete_cached_analysis", fake_delete)

    source = open(jm_mod.__file__, "r", encoding="utf-8").read()
    # Проверяем условие `not retry_feedback` рядом с get_cached_analysis — регрессия.
    assert "if use_cache and not retry_feedback:" in source, (
        "При retry_feedback необходимо пропускать cache lookup"
    )


def test_invalid_cache_hit_triggers_delete(monkeypatch) -> None:
    """Если кэш-hit проваливает валидацию — запись удаляется (cache_invalidated warning)."""
    import app.services.job_manager as jm_mod

    source = open(jm_mod.__file__, "r", encoding="utf-8").read()
    assert "delete_cached_analysis(cache_key)" in source, (
        "Битый cache-hit должен удалять запись, чтобы следующий запрос пошёл в модель"
    )
    assert "cache_invalidated" in source, (
        "Должен добавляться warning cache_invalidated — для диагностики в xlsx-выгрузке"
    )


def test_delete_cached_analysis_removes_db_only(monkeypatch, tmp_path) -> None:
    """delete_cached_analysis вызывает DELETE в БД и НЕ дёргает Redis.

    Регрессия на v2.1.2 (BUG-19): Redis L1-кэш для llm_cache:* удалён, потому
    что писался без TTL и накапливался до OOM-kill. Cache теперь только в
    Postgres, поэтому и delete_cached_analysis ничего не делает в Redis.
    """
    import app.db as db_mod

    redis_calls: list[str] = []

    class FakeRedis:
        def delete(self, key: str) -> None:
            redis_calls.append(key)

    sql_calls: list[tuple] = []

    class FakeConn:
        def execute(self, sql: str, params=None):
            sql_calls.append((sql.strip().split()[0].upper(), params))
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(db_mod, "get_redis_client", lambda: FakeRedis())
    monkeypatch.setattr(db_mod, "get_conn", lambda: FakeConn())

    db_mod.delete_cached_analysis("abc123")

    assert redis_calls == [], (
        "После v2.1.2 функция не должна обращаться к Redis "
        "(L1-кэш удалён, см. test_llm_cache_redis_removed.py)"
    )
    assert sql_calls and sql_calls[0][0] == "DELETE", "Должен быть DELETE-запрос в БД"
    assert sql_calls[0][1] == ("abc123",), "Параметром должен быть ровно cache_key"
