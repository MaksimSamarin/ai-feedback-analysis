"""Юнит-тесты для `get_job_queue_position` / `get_inspect_queue_position` (IDEA-07).

Стратегия А: позиция в Redis-LIST (`LPOS`-эквивалент через `LRANGE` + JSON-поиск).
Важные инварианты:
- Первая задача в очереди — позиция 0.
- Задача не в очереди (взята воркером или отсутствует) — None.
- Невалидный JSON среди payload'ов не роняет поиск.
- Пустой `job_id` / `file_id` — None без обращения к Redis.

Запуск:
    cd backend && pytest tests/unit/test_queue_position.py -v
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch


def _patch_redis(items: list[str]) -> MagicMock:
    client = MagicMock()
    client.lrange.return_value = items
    return client


def test_first_in_queue_returns_zero() -> None:
    from app.queue import get_job_queue_position

    items = [
        json.dumps({"job_id": "aaa"}, ensure_ascii=False),
        json.dumps({"job_id": "bbb"}, ensure_ascii=False),
    ]
    with patch("app.queue._redis", return_value=_patch_redis(items)):
        assert get_job_queue_position("aaa") == 0


def test_middle_position() -> None:
    from app.queue import get_job_queue_position

    items = [
        json.dumps({"job_id": "first"}, ensure_ascii=False),
        json.dumps({"job_id": "target"}, ensure_ascii=False),
        json.dumps({"job_id": "third"}, ensure_ascii=False),
    ]
    with patch("app.queue._redis", return_value=_patch_redis(items)):
        assert get_job_queue_position("target") == 1


def test_last_position() -> None:
    from app.queue import get_job_queue_position

    items = [
        json.dumps({"job_id": "a"}, ensure_ascii=False),
        json.dumps({"job_id": "b"}, ensure_ascii=False),
        json.dumps({"job_id": "target"}, ensure_ascii=False),
    ]
    with patch("app.queue._redis", return_value=_patch_redis(items)):
        assert get_job_queue_position("target") == 2


def test_not_in_queue_returns_none() -> None:
    """Задача уже взята воркером — в LIST'е её нет, должна вернуться None."""
    from app.queue import get_job_queue_position

    items = [
        json.dumps({"job_id": "aaa"}, ensure_ascii=False),
        json.dumps({"job_id": "bbb"}, ensure_ascii=False),
    ]
    with patch("app.queue._redis", return_value=_patch_redis(items)):
        assert get_job_queue_position("missing") is None


def test_empty_queue_returns_none() -> None:
    from app.queue import get_job_queue_position

    with patch("app.queue._redis", return_value=_patch_redis([])):
        assert get_job_queue_position("any") is None


def test_empty_job_id_returns_none_without_redis_call() -> None:
    """Пустой id не должен дёргать Redis — безопасный fast-path."""
    from app.queue import get_job_queue_position

    client = _patch_redis([])
    with patch("app.queue._redis", return_value=client):
        assert get_job_queue_position("") is None
    client.lrange.assert_not_called()


def test_malformed_payload_skipped() -> None:
    """Мусор (не JSON, не dict) среди payload'ов не должен ронять поиск."""
    from app.queue import get_job_queue_position

    items = [
        "not a json",
        json.dumps(["unexpected list"], ensure_ascii=False),
        json.dumps({"job_id": "target"}, ensure_ascii=False),
    ]
    with patch("app.queue._redis", return_value=_patch_redis(items)):
        # Таргет реально третий, но индекс считается по списку включая мусор.
        assert get_job_queue_position("target") == 2


def test_inspect_queue_uses_file_id() -> None:
    """Inspect-очередь ищет по `file_id`, а не по `job_id`."""
    from app.queue import get_inspect_queue_position

    items = [
        json.dumps({"file_id": "aaa"}, ensure_ascii=False),
        json.dumps({"file_id": "bbb"}, ensure_ascii=False),
    ]
    with patch("app.queue._redis", return_value=_patch_redis(items)):
        assert get_inspect_queue_position("bbb") == 1


def test_redis_failure_returns_none() -> None:
    """Если Redis недоступен / `lrange` падает, функция не пробрасывает исключение."""
    from app.queue import get_job_queue_position

    client = MagicMock()
    client.lrange.side_effect = Exception("redis down")
    with patch("app.queue._redis", return_value=client):
        assert get_job_queue_position("any") is None
