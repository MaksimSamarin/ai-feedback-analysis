"""Юнит-тесты для `iter_report_rows` — батчевая пагинация строк отчёта.

Функция введена в рамках фикса BUG-14 (OOM на экспорте больших отчётов):
не грузит весь отчёт в память, читает батчами.

v2.0.0 итерация 3.2: OFFSET/LIMIT заменён на keyset pagination
(`WHERE row_number > last_seen`) — иначе на 100k строк было O(n²) сканов и
100% CPU на PG при открытии/выгрузке больших отчётов.

Тесты проверяют что:
1. Пустой отчёт не ломает цикл (нет бесконечной петли)
2. Батч < LIMIT корректно завершает итерацию
3. Несколько батчей собираются в правильном порядке
4. `ORDER BY row_number` присутствует в SQL (регрессия: без order пагинация даёт
   разные строки в двух проходах — сломается discovery + write в export_results_xlsx)
5. keyset-параметр `row_number > last_seen` растёт от батча к батчу

Запуск:
    cd backend && pytest tests/unit/test_iter_report_rows.py -v
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest


class _FakeConn:
    """Fake conn который отдаёт batches по очереди и логирует SQL/params."""

    def __init__(self, batches: list[list[dict[str, Any]]]) -> None:
        self._batches = list(batches)
        self._idx = 0
        self.calls: list[dict[str, Any]] = []

    def execute(self, sql: str, params: tuple = ()) -> Any:
        self.calls.append({"sql": sql, "params": params})
        batch: list[dict[str, Any]] = (
            self._batches[self._idx] if self._idx < len(self._batches) else []
        )
        self._idx += 1
        cursor = MagicMock()
        cursor.fetchall.return_value = batch
        return cursor


def _install_fake_get_conn(
    monkeypatch: pytest.MonkeyPatch, batches: list[list[dict[str, Any]]]
) -> _FakeConn:
    """Подменяет `app.db.get_conn` на фейк, возвращающий заранее заготовленные батчи."""
    fake = _FakeConn(batches)

    @contextmanager
    def fake_get_conn():
        yield fake

    monkeypatch.setattr("app.db.get_conn", fake_get_conn)
    return fake


def _row(n: int) -> dict[str, Any]:
    return {"row_number": n, "status": "done", "review_text": f"отзыв {n}"}


def test_empty_report_yields_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Пустой отчёт — итератор возвращает 0 элементов, делает ровно 1 SQL-запрос
    (иначе можно было бы крутиться бесконечно на пустой БД)."""
    from app.db import iter_report_rows

    fake = _install_fake_get_conn(monkeypatch, [[]])

    result = list(iter_report_rows("empty-report", batch_size=100))

    assert result == []
    assert len(fake.calls) == 1, (
        f"На пустом отчёте ожидали 1 запрос, получено {len(fake.calls)} "
        "— возможный бесконечный цикл"
    )


def test_single_batch_when_rows_fewer_than_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """100 строк при batch_size=2000 — один запрос, итератор завершается."""
    from app.db import iter_report_rows

    rows = [_row(i) for i in range(1, 101)]
    fake = _install_fake_get_conn(monkeypatch, [rows])

    result = list(iter_report_rows("rep-small", batch_size=2000))

    assert len(result) == 100
    assert result[0]["row_number"] == 1
    assert result[-1]["row_number"] == 100
    assert len(fake.calls) == 1, (
        "При батче меньше лимита должен быть ровно 1 SQL-запрос"
    )


def test_multiple_batches_when_rows_exceed_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """5000 строк при batch_size=2000 — 3 запроса (2000+2000+1000), всё собралось."""
    from app.db import iter_report_rows

    batches = [
        [_row(i) for i in range(1, 2001)],       # 1..2000
        [_row(i) for i in range(2001, 4001)],    # 2001..4000
        [_row(i) for i in range(4001, 5001)],    # 4001..5000 (неполный)
    ]
    fake = _install_fake_get_conn(monkeypatch, batches)

    result = list(iter_report_rows("rep-big", batch_size=2000))

    assert len(result) == 5000
    assert result[0]["row_number"] == 1
    assert result[-1]["row_number"] == 5000
    assert len(fake.calls) == 3, (
        f"Ожидали 3 SQL-запроса (5000/2000 = 2.5 → 3 батча), "
        f"получили {len(fake.calls)}"
    )


def test_keyset_last_row_number_increases_across_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keyset-параметр `row_number > ?` должен расти от батча к батчу:
    первый запрос с -1, затем с последним row_number из предыдущего батча."""
    from app.db import iter_report_rows

    batches = [
        [_row(i) for i in range(1, 101)],       # полный, последний row_number=100
        [_row(i) for i in range(101, 151)],     # неполный — итерация завершится
    ]
    fake = _install_fake_get_conn(monkeypatch, batches)

    list(iter_report_rows("rep-x", batch_size=100))

    # Параметры SQL: (report_id, last_row_number, limit).
    # На первом запросе last_row_number = -1 (ещё не читали), на втором — 100.
    last_seen_values = [call["params"][1] for call in fake.calls]
    assert last_seen_values == [-1, 100], (
        f"Ожидали last_row_number [-1, 100], получили {last_seen_values} — keyset сломан"
    )


def test_exact_multiple_of_batch_size(monkeypatch: pytest.MonkeyPatch) -> None:
    """4000 строк при batch_size=2000 — 2 полных батча + 1 пустой (для проверки конца).

    Это единственный способ понять что данные кончились, когда последний батч ровно
    по размеру. Без пустого «пинг-запроса» можно зависнуть или пропустить данные.
    """
    from app.db import iter_report_rows

    batches = [
        [_row(i) for i in range(1, 2001)],
        [_row(i) for i in range(2001, 4001)],
        [],  # пустой — сигнал что больше нет
    ]
    fake = _install_fake_get_conn(monkeypatch, batches)

    result = list(iter_report_rows("rep-exact", batch_size=2000))

    assert len(result) == 4000
    assert len(fake.calls) == 3, (
        "Ожидали 3 запроса: 2 полных батча + 1 пустой для проверки конца"
    )


def test_yields_rows_as_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Каждый элемент — dict (не Row-объект psycopg). Важно потому что потребители
    делают `row.get(...)` — Row-объект не имеет .get()."""
    from app.db import iter_report_rows

    _install_fake_get_conn(monkeypatch, [[_row(1), _row(2)]])

    for row in iter_report_rows("rep", batch_size=10):
        assert isinstance(row, dict), (
            f"Ожидали dict, получили {type(row).__name__} — downstream код сломается"
        )
        assert "row_number" in row
        assert row.get("status") == "done"


def test_sql_uses_order_by_row_number(monkeypatch: pytest.MonkeyPatch) -> None:
    """Регрессия: SQL-запрос должен содержать `ORDER BY row_number`.

    Без явного ORDER BY два прохода по данным (discovery + write в export_results_xlsx)
    могут выдать строки в разном порядке — заголовки колонок расходятся с данными.
    """
    from app.db import iter_report_rows

    fake = _install_fake_get_conn(monkeypatch, [[_row(1)]])

    list(iter_report_rows("rep", batch_size=10))

    sql = fake.calls[0]["sql"]
    normalized = " ".join(sql.split()).lower()
    assert "order by row_number" in normalized, (
        f"В SQL нет ORDER BY row_number — пагинация непредсказуема. SQL: {sql!r}"
    )


def test_report_id_passed_as_first_param(monkeypatch: pytest.MonkeyPatch) -> None:
    """SQL-параметры: (report_id, last_row_number, limit). Проверяем что report_id на месте."""
    from app.db import iter_report_rows

    fake = _install_fake_get_conn(monkeypatch, [[_row(1)]])

    list(iter_report_rows("my-unique-report-id", batch_size=50))

    params = fake.calls[0]["params"]
    assert params[0] == "my-unique-report-id"
    assert params[1] == -1   # last_row_number на первом запросе
    assert params[2] == 50   # limit = batch_size


def test_custom_batch_size_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    """Кастомный `batch_size` передаётся в LIMIT."""
    from app.db import iter_report_rows

    fake = _install_fake_get_conn(monkeypatch, [[_row(1)]])

    list(iter_report_rows("rep", batch_size=7))

    assert fake.calls[0]["params"][2] == 7


def test_keyset_sql_uses_row_number_filter(monkeypatch: pytest.MonkeyPatch) -> None:
    """SQL содержит `row_number > ?` — без этого на 100k строк деградация до O(n²)
    (старый OFFSET/LIMIT). Регрессионный тест итерации 3.2."""
    from app.db import iter_report_rows

    fake = _install_fake_get_conn(monkeypatch, [[_row(1)]])

    list(iter_report_rows("rep", batch_size=10))

    normalized = " ".join(fake.calls[0]["sql"].split()).lower()
    assert "row_number > ?" in normalized, (
        "Keyset pagination сломан — вернулся OFFSET. На больших отчётах PG уйдёт в 100% CPU."
    )
    assert "offset" not in normalized, (
        "SQL содержит OFFSET — пагинация неэффективна для больших отчётов."
    )
