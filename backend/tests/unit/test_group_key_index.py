"""Регрессионный тест на индекс `idx_report_rows_group_key` (BUG-09).

Юнит-тест проверяет **через исходник** (не реальную БД), что в `init_db` есть
корректный `CREATE INDEX` для режима группировки. Реальный индекс создаётся
Postgres'ом при старте сервиса через `_init_postgres_schema`.

Почему не живой БД-тест:
- Живая БД требует testcontainers (уровень сценарных тестов)
- Source-тест защищает от самой частой регрессии — случайное удаление строки
  в ходе рефакторинга `_init_postgres_schema`

Запуск:
    cd backend && pytest tests/unit/test_group_key_index.py -v
"""

from __future__ import annotations

from pathlib import Path


def _read_db_source() -> str:
    db_path = Path(__file__).resolve().parents[2] / "app" / "db.py"
    return db_path.read_text(encoding="utf-8")


def test_group_key_index_is_created() -> None:
    """В init_db должна быть команда CREATE INDEX для `idx_report_rows_group_key`."""
    source = _read_db_source()

    assert "idx_report_rows_group_key" in source, (
        "Индекс `idx_report_rows_group_key` удалён из init_db — регрессия BUG-09. "
        "Запросы list_pending_group_keys_batch / list_pending_rows_by_group_key "
        "снова будут делать seq scan по партиции."
    )


def test_group_key_index_uses_correct_columns() -> None:
    """Индекс должен включать `(report_id, group_key, status)` — порядок важен."""
    source = _read_db_source()

    # Нормализуем пробелы для матчинга
    normalized = " ".join(source.split()).lower()

    assert "on report_rows (report_id, group_key, status)" in normalized, (
        "Индекс должен быть на (report_id, group_key, status). "
        "Любой другой порядок — потеря производительности на реальных запросах."
    )


def test_group_key_index_is_partial() -> None:
    """Индекс должен быть partial — `WHERE group_key IS NOT NULL`.

    Без `WHERE` индекс будет в 10x больше: он попадут все строки отчётов без группировки.
    Partial экономит место и ускоряет вставки для не-грппированных отчётов.
    """
    source = _read_db_source()
    normalized = " ".join(source.split()).lower()

    # Проверяем связку: имя индекса + partial условие в разумной близости
    idx_pos = normalized.find("idx_report_rows_group_key")
    assert idx_pos >= 0, "индекс отсутствует"

    # В пределах 200 символов после имени должно быть `WHERE group_key IS NOT NULL`
    window = normalized[idx_pos : idx_pos + 300]
    assert "where group_key is not null" in window, (
        "У индекса `idx_report_rows_group_key` пропал partial-фильтр "
        "`WHERE group_key IS NOT NULL` — будет занимать место для всех строк."
    )


def test_group_key_index_uses_if_not_exists() -> None:
    """`CREATE INDEX IF NOT EXISTS` — обязательно, чтобы не падать на уже существующем индексе."""
    source = _read_db_source()
    normalized = " ".join(source.split()).lower()

    assert "create index if not exists idx_report_rows_group_key" in normalized, (
        "Индекс должен создаваться через `IF NOT EXISTS` — иначе второй старт сервиса упадёт"
    )


def test_index_created_on_parent_partitioned_table() -> None:
    """Индекс создаётся на родительскую таблицу `report_rows`, не на партиции.

    Для HASH-партиционированной таблицы Postgres сам размножит индекс на все партиции.
    Если кто-то попробует создать на партиции (`report_rows_p0`, ...) — новые партиции
    останутся без индекса.
    """
    source = _read_db_source()
    # Python-конкатенация строковых литералов оставляет `"..."\n   "..."` в исходнике —
    # убираем кавычки и нормализуем пробелы, чтобы получить плоскую SQL-команду.
    flat = " ".join(source.replace('"', " ").split()).lower()

    # Ищем что идёт сразу после нашего индекса: должно быть `on report_rows`
    marker = "idx_report_rows_group_key"
    idx = flat.find(marker)
    assert idx >= 0, "индекс `idx_report_rows_group_key` отсутствует"

    after = flat[idx + len(marker) : idx + len(marker) + 40]
    assert "on report_rows " in after, (
        f"После имени индекса ожидали `ON report_rows `, получили {after!r} — "
        "возможно создаётся на партицию (report_rows_p0) вместо родителя."
    )
