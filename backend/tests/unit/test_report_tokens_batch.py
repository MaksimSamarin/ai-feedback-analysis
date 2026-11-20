"""Юнит-тесты на get_report_tokens_batch (v2.1.3).

Покрытие:
- v2.1.3 (новая фича): per-report агрегация токенов из report_rows + Redis-кэш с TTL=300 сек.
  До v2.1.3 в карточке «Мои отчёты» под иконкой ℹ️ показывались поля reports.{prompt,completion,total}_tokens,
  которые никогда не заполнялись (dead fields). Теперь реальные значения агрегируются батчем за один SQL
  и кэшируются per-report в Redis. Поведение идентично эталону get_user_usage (BUG-22/BUG-26).

Запуск:
    cd backend && pytest tests/unit/test_report_tokens_batch.py -v
"""
from __future__ import annotations

import json


class _FakeRow(dict):
    def __getitem__(self, key):
        return super().__getitem__(key)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows, captured):
        self._rows = rows
        self._captured = captured

    def execute(self, sql, params=None):
        self._captured.append((sql, params or ()))
        return _FakeResult(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def _patch_conn(monkeypatch, rows, captured):
    import app.db as db_mod
    monkeypatch.setattr(db_mod, "get_conn", lambda: _FakeConn(rows, captured))


class _FakeRedis:
    """Минимальный Redis-стаб с поддержкой mget/setex."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.setex_calls: list[tuple[str, int, str]] = []
        self.mget_calls: list[list[str]] = []
        self.raise_on_mget = False
        self.raise_on_setex = False

    def mget(self, keys):
        self.mget_calls.append(list(keys))
        if self.raise_on_mget:
            raise RuntimeError("redis down")
        return [self.store.get(k) for k in keys]

    def setex(self, key, ttl, value):
        if self.raise_on_setex:
            raise RuntimeError("redis flaky")
        self.setex_calls.append((key, ttl, value))
        self.store[key] = value


def test_empty_list_returns_empty_dict(monkeypatch) -> None:
    """Пустой список ID → пустой dict, никаких походов в БД и Redis."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, [], captured)
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: _FakeRedis())

    assert db_mod.get_report_tokens_batch([]) == {}
    assert captured == [], "SQL не должен вызываться для пустого списка"


def test_sql_reads_materialized_counters_from_reports(monkeypatch) -> None:
    """hotfix/token-counters: SQL читает материализованные счётчики из reports по PK.

    До hotfix: SUM по report_rows GROUP BY report_id — на параллельных миллион-строчных
    отчётах вешало PG. После hotfix: SELECT по pkey из reports — index seek, миллисекунды.
    Воркер инкрементит reports.tokens при каждой записи строк (row-mode и group-mode).
    """
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [
            _FakeRow(id="r1", prompt_tokens=1000, completion_tokens=50, total_tokens=1050),
            _FakeRow(id="r2", prompt_tokens=2000, completion_tokens=100, total_tokens=2100),
        ],
        captured,
    )
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)

    result = db_mod.get_report_tokens_batch(["r1", "r2"])

    assert captured, "SQL должен быть вызван"
    sql, params = captured[0]
    assert "FROM reports" in sql
    assert "FROM report_rows" not in sql, "Не должно быть SUM по report_rows — это и было причиной перегрузки PG"
    assert "SUM(" not in sql, "SUM не нужен — счётчики уже посчитаны воркером"
    assert "GROUP BY" not in sql, "GROUP BY не нужен — один отчёт = одна строка"
    assert "IN(?,?)" in sql.replace(" ", "").replace("\n", "")
    assert list(params) == ["r1", "r2"]
    assert result == {
        "r1": {"prompt_tokens": 1000, "completion_tokens": 50, "total_tokens": 1050},
        "r2": {"prompt_tokens": 2000, "completion_tokens": 100, "total_tokens": 2100},
    }


def test_returns_zeros_for_reports_without_rows(monkeypatch) -> None:
    """Отчёты, для которых нет ни одной строки в report_rows → возвращаются нули, не пропадают из результата."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [_FakeRow(id="has-rows", prompt_tokens=10, completion_tokens=1, total_tokens=11)],
        captured,
    )
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)

    result = db_mod.get_report_tokens_batch(["has-rows", "empty1", "empty2"])

    assert result == {
        "has-rows": {"prompt_tokens": 10, "completion_tokens": 1, "total_tokens": 11},
        "empty1": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        "empty2": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
    }


def test_cache_miss_runs_sql_and_writes_each_entry_via_setex(monkeypatch) -> None:
    """Холодный кэш: SQL + setex для каждого отчёта (включая нули для отсутствующих строк), TTL=300."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [_FakeRow(id="r1", prompt_tokens=100, completion_tokens=5, total_tokens=105)],
        captured,
    )
    fake_redis = _FakeRedis()
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    db_mod.get_report_tokens_batch(["r1", "r2"])

    assert len(captured) == 1, "Один батч-SQL"
    keys_written = {k for k, _, _ in fake_redis.setex_calls}
    assert keys_written == {"report_tokens:r1", "report_tokens:r2"}
    ttls = {ttl for _, ttl, _ in fake_redis.setex_calls}
    assert ttls == {db_mod.REPORT_TOKENS_CACHE_TTL_SEC}, "TTL=300 для всех ключей"


def test_cache_hit_skips_sql(monkeypatch) -> None:
    """Все ID в кэше → SQL не дёргается, значения берутся из Redis."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(monkeypatch, [], captured)

    fake_redis = _FakeRedis()
    fake_redis.store["report_tokens:r1"] = json.dumps({"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10})
    fake_redis.store["report_tokens:r2"] = json.dumps({"prompt_tokens": 70, "completion_tokens": 30, "total_tokens": 100})
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_report_tokens_batch(["r1", "r2"])

    assert captured == [], "SQL не должен вызываться при полном hit"
    assert result == {
        "r1": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10},
        "r2": {"prompt_tokens": 70, "completion_tokens": 30, "total_tokens": 100},
    }


def test_partial_cache_hit_sql_only_for_misses(monkeypatch) -> None:
    """Частичный hit: r1 из кэша, r2 идёт в SQL. SQL запрашивает только r2."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [_FakeRow(id="r2", prompt_tokens=50, completion_tokens=2, total_tokens=52)],
        captured,
    )

    fake_redis = _FakeRedis()
    fake_redis.store["report_tokens:r1"] = json.dumps({"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6})
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_report_tokens_batch(["r1", "r2"])

    assert len(captured) == 1, "Один SQL, и только для r2"
    _, params = captured[0]
    assert list(params) == ["r2"]
    assert result == {
        "r1": {"prompt_tokens": 5, "completion_tokens": 1, "total_tokens": 6},
        "r2": {"prompt_tokens": 50, "completion_tokens": 2, "total_tokens": 52},
    }


def test_broken_json_in_cache_falls_back_to_sql(monkeypatch) -> None:
    """Битый JSON в Redis → этот ID считается miss → идёт в SQL, не падает."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [_FakeRow(id="r1", prompt_tokens=9, completion_tokens=1, total_tokens=10)],
        captured,
    )

    fake_redis = _FakeRedis()
    fake_redis.store["report_tokens:r1"] = "{not valid json"
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_report_tokens_batch(["r1"])

    assert len(captured) == 1, "SQL отработал на miss"
    assert result == {"r1": {"prompt_tokens": 9, "completion_tokens": 1, "total_tokens": 10}}


def test_no_redis_falls_back_to_sql(monkeypatch) -> None:
    """Redis недоступен (get_redis_client → None) → все ID считаются miss → SQL отрабатывает."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [
            _FakeRow(id="r1", prompt_tokens=1, completion_tokens=0, total_tokens=1),
            _FakeRow(id="r2", prompt_tokens=2, completion_tokens=0, total_tokens=2),
        ],
        captured,
    )
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)

    result = db_mod.get_report_tokens_batch(["r1", "r2"])

    assert len(captured) == 1
    _, params = captured[0]
    assert list(params) == ["r1", "r2"]
    assert result == {
        "r1": {"prompt_tokens": 1, "completion_tokens": 0, "total_tokens": 1},
        "r2": {"prompt_tokens": 2, "completion_tokens": 0, "total_tokens": 2},
    }


def test_redis_mget_exception_falls_back_to_sql(monkeypatch) -> None:
    """Redis.mget кидает исключение → fallback на SQL, ничего не валится."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [_FakeRow(id="r1", prompt_tokens=42, completion_tokens=8, total_tokens=50)],
        captured,
    )

    fake_redis = _FakeRedis()
    fake_redis.raise_on_mget = True
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_report_tokens_batch(["r1"])

    assert result == {"r1": {"prompt_tokens": 42, "completion_tokens": 8, "total_tokens": 50}}


def test_redis_setex_exception_does_not_break_result(monkeypatch) -> None:
    """Redis.setex кидает → результат всё равно возвращается (кэш — это оптимизация, не источник правды)."""
    import app.db as db_mod

    captured: list = []
    _patch_conn(
        monkeypatch,
        [_FakeRow(id="r1", prompt_tokens=11, completion_tokens=2, total_tokens=13)],
        captured,
    )

    fake_redis = _FakeRedis()
    fake_redis.raise_on_setex = True
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake_redis)

    result = db_mod.get_report_tokens_batch(["r1"])

    assert result == {"r1": {"prompt_tokens": 11, "completion_tokens": 2, "total_tokens": 13}}
