"""Юнит-тесты на get_user_usage.

Покрытие:
- v2.1.2 (BUG-22): SQL агрегирует из report_rows, а не из dead-fields в reports.
- v2.1.2 hotfix (BUG-26): Redis-кэш с TTL=300 сек, чтобы не гонять тяжёлый
  SUM на каждый поллинг фронта (20 сек × 20 юзеров = 60 SQL/мин при пиковой
  нагрузке, Postgres задыхался). Дополнительно фронт поднял интервал
  USAGE_POLL_MS до 60 сек — в паре падение нагрузки в ~15 раз.

Запуск:
    cd backend && pytest tests/unit/test_user_usage.py -v
"""
from __future__ import annotations

import json


class _FakeRow(dict):
    def __getitem__(self, key):
        return super().__getitem__(key)


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row, captured):
        self._row = row
        self._captured = captured

    def execute(self, sql, params=None):
        self._captured.append((sql, params or ()))
        return _FakeResult(self._row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _patch_conn(monkeypatch, row, captured):
    import app.db as db_mod
    monkeypatch.setattr(db_mod, "get_conn", lambda: _FakeConn(row, captured))


def test_sql_reads_materialized_counters_from_reports(monkeypatch) -> None:
    """hotfix/token-counters: SQL читает материализованные счётчики из reports.

    До hotfix: SUM по report_rows JOIN с reports — на 9M+ строк зависало (BUG-26 повторно
    проявился на проде). После hotfix: SUM по reports (только ~20-30 отчётов юзера) —
    миллисекунды. Воркер сам инкрементит reports.{prompt,completion,total}_tokens при
    каждой записи строк (см. update_report_row_result и bulk_update_report_rows_same_result).
    """
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        _FakeRow(prompt_tokens=12345, completion_tokens=678, total_tokens=13023),
        captured,
    )
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)

    result = db_mod.get_user_usage(42)

    assert captured, "execute должен быть вызван"
    sql, params = captured[0]
    assert "FROM reports" in sql
    assert "FROM report_rows" not in sql, "Не должно быть SUM по report_rows — это и было причиной перегрузки PG"
    assert "JOIN" not in sql, "JOIN не нужен — токены лежат прямо в reports"
    assert "WHERE user_id = ?" in sql
    assert params == (42,)
    assert result == {
        "prompt_tokens": 12345,
        "completion_tokens": 678,
        "total_tokens": 13023,
    }


def test_returns_zeros_when_empty(monkeypatch) -> None:
    """Если у юзера ни одного отчёта — нули, не падает на None."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        _FakeRow(prompt_tokens=None, completion_tokens=None, total_tokens=None),
        captured,
    )
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)

    result = db_mod.get_user_usage(999)
    assert result == {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


# ---------- v2.1.2 hotfix (BUG-26): Redis-кэш ----------


class _FakeRedis:
    """Минимальный Redis-стаб для теста кэша. Хранит {key: (value, ttl)}."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.setex_calls: list[tuple[str, int, str]] = []
        self.get_calls: list[str] = []

    def get(self, key):
        self.get_calls.append(key)
        return self.store.get(key)

    def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl, value))
        self.store[key] = value


def test_cache_miss_runs_sql_and_writes_cache(monkeypatch) -> None:
    """Первый вызов — кэш пуст → идёт SQL → результат пишется в Redis с TTL=300."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        _FakeRow(prompt_tokens=100, completion_tokens=50, total_tokens=150),
        captured,
    )
    fake_redis = _FakeRedis()
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_user_usage(7)

    assert len(captured) == 1, "Cache miss → должен быть ровно 1 SQL"
    assert result == {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
    assert fake_redis.get_calls == ["usage:7"]
    assert len(fake_redis.setex_calls) == 1
    key, ttl, value = fake_redis.setex_calls[0]
    assert key == "usage:7"
    assert ttl == 300, "TTL должен быть 300 сек (5 минут)"
    assert json.loads(value) == result


def test_cache_hit_skips_sql(monkeypatch) -> None:
    """Второй вызов в течение TTL — из кэша, SQL не вызывается вообще."""
    import app.db as db_mod

    captured: list = []
    # Если SQL вызовется — тест упадёт на assert ниже (captured будет непустым).
    _patch_conn(monkeypatch, _FakeRow(prompt_tokens=999, completion_tokens=999, total_tokens=999), captured)

    fake_redis = _FakeRedis()
    fake_redis.store["usage:7"] = json.dumps(
        {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
    )
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_user_usage(7)

    assert result == {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
    assert captured == [], "Cache hit → SQL вообще не должен вызываться"
    assert fake_redis.setex_calls == [], "При cache hit не перезаписываем кэш"


def test_cache_per_user_keys(monkeypatch) -> None:
    """Кэш разделён по user_id — у каждого пользователя свой ключ."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, _FakeRow(prompt_tokens=1, completion_tokens=1, total_tokens=2), captured)

    fake_redis = _FakeRedis()
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    db_mod.get_user_usage(1)
    db_mod.get_user_usage(2)
    db_mod.get_user_usage(3)

    keys_written = [c[0] for c in fake_redis.setex_calls]
    assert keys_written == ["usage:1", "usage:2", "usage:3"]


def test_corrupt_cache_falls_back_to_sql(monkeypatch) -> None:
    """Если в Redis битый JSON — не падаем, считаем SQL заново."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, _FakeRow(prompt_tokens=42, completion_tokens=8, total_tokens=50), captured)

    fake_redis = _FakeRedis()
    fake_redis.store["usage:7"] = "{not-json"
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_user_usage(7)

    assert result == {"prompt_tokens": 42, "completion_tokens": 8, "total_tokens": 50}
    assert len(captured) == 1, "Битый кэш → fallback на SQL"


def test_redis_unavailable_falls_back_to_sql(monkeypatch) -> None:
    """Если Redis недоступен (None) — функция работает напрямую через SQL."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, _FakeRow(prompt_tokens=10, completion_tokens=5, total_tokens=15), captured)
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)

    result = db_mod.get_user_usage(7)

    assert result == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    assert len(captured) == 1


def test_redis_get_exception_falls_back_to_sql(monkeypatch) -> None:
    """Если Redis.get кинул исключение (timeout/connection error) — не валим запрос."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, _FakeRow(prompt_tokens=10, completion_tokens=5, total_tokens=15), captured)

    class BrokenRedis:
        def get(self, key):
            raise OSError("redis down")

        def setex(self, key, ttl, value):
            pass  # noqa: PIE790

    monkeypatch.setattr(db_mod, "get_redis_client", lambda: BrokenRedis())

    result = db_mod.get_user_usage(7)
    assert result == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    assert len(captured) == 1


def test_redis_setex_exception_does_not_break_response(monkeypatch) -> None:
    """Если Redis.setex упал — результат всё равно возвращается, юзер ничего не теряет."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, _FakeRow(prompt_tokens=10, completion_tokens=5, total_tokens=15), captured)

    class WriteFailRedis:
        def get(self, key):
            return None

        def setex(self, key, ttl, value):
            raise OSError("redis OOM on write")

    monkeypatch.setattr(db_mod, "get_redis_client", lambda: WriteFailRedis())

    result = db_mod.get_user_usage(7)
    assert result == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
